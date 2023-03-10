use crate::errors::BackieError;
use crate::queue::Queue;
use crate::worker::{runnable, ExecuteTaskFn};
use crate::worker::{StateFn, Worker};
use crate::{BackgroundTask, CurrentTask, PgTaskStore, RetentionMode};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub type AppDataFn<AppData> = Arc<dyn Fn(Queue) -> AppData + Send + Sync>;

#[derive(Clone)]
pub struct WorkerPool<AppData>
where
    AppData: Clone + Send + 'static,
{
    /// Storage of tasks.
    queue_store: Arc<PgTaskStore>, // TODO: make this generic/dynamic referenced

    /// Queue used to spawn tasks.
    queue: Queue,

    /// Make possible to load the application data.
    ///
    /// The application data is loaded when the worker pool is started and is passed to the tasks.
    /// The loading function accepts a queue instance in case the application data depends on it. This
    /// is interesting for situations where the application wants to allow tasks to spawn other tasks.
    application_data_fn: StateFn<AppData>,

    /// The types of task the worker pool can execute and the loaders for them.
    task_registry: BTreeMap<String, ExecuteTaskFn<AppData>>,

    /// Number of workers that will be spawned per queue.
    worker_queues: BTreeMap<String, (RetentionMode, u32)>,
}

impl<AppData> WorkerPool<AppData>
where
    AppData: Clone + Send + 'static,
{
    /// Create a new worker pool.
    pub fn new<A>(queue_store: PgTaskStore, application_data_fn: A) -> Self
    where
        A: Fn(Queue) -> AppData + Send + Sync + 'static,
    {
        let queue_store = Arc::new(queue_store);
        let queue = Queue::new(queue_store.clone());
        let application_data_fn = {
            let queue = queue.clone();
            move || application_data_fn(queue.clone())
        };
        Self {
            queue_store,
            queue,
            application_data_fn: Arc::new(application_data_fn),
            task_registry: BTreeMap::new(),
            worker_queues: BTreeMap::new(),
        }
    }

    /// Register a task type with the worker pool.
    pub fn register_task_type<BT>(mut self, num_workers: u32, retention_mode: RetentionMode) -> Self
    where
        BT: BackgroundTask<AppData = AppData>,
    {
        self.worker_queues
            .insert(BT::QUEUE.to_string(), (retention_mode, num_workers));
        self.task_registry
            .insert(BT::TASK_NAME.to_string(), Arc::new(runnable::<BT>));
        self
    }

    pub async fn start<F>(
        self,
        graceful_shutdown: F,
    ) -> Result<(JoinHandle<()>, Queue), BackieError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::watch::channel(());

        // Spawn all individual workers per queue
        for (queue_name, (retention_mode, num_workers)) in self.worker_queues.iter() {
            for idx in 0..*num_workers {
                let mut worker: Worker<AppData> = Worker::new(
                    self.queue_store.clone(),
                    queue_name.clone(),
                    retention_mode.clone(),
                    self.task_registry.clone(),
                    self.application_data_fn.clone(),
                    Some(rx.clone()),
                );
                let worker_name = format!("worker-{queue_name}-{idx}");
                // TODO: grab the join handle for every worker for graceful shutdown
                tokio::spawn(async move {
                    worker.run_tasks().await;
                    log::info!("Worker {} stopped", worker_name);
                });
            }
        }

        Ok((
            tokio::spawn(async move {
                graceful_shutdown.await;
                if let Err(err) = tx.send(()) {
                    log::warn!("Failed to send shutdown signal to worker pool: {}", err);
                } else {
                    log::info!("Worker pool stopped gracefully");
                }
            }),
            self.queue,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
    use diesel_async::AsyncPgConnection;

    #[derive(Clone, Debug)]
    struct ApplicationContext {
        app_name: String,
    }

    impl ApplicationContext {
        fn new() -> Self {
            Self {
                app_name: "Backie".to_string(),
            }
        }

        fn get_app_name(&self) -> String {
            self.app_name.clone()
        }
    }

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct GreetingTask {
        person: String,
    }

    #[async_trait]
    impl BackgroundTask for GreetingTask {
        const TASK_NAME: &'static str = "my_task";

        type AppData = ApplicationContext;

        async fn run(
            &self,
            task_info: CurrentTask,
            app_context: Self::AppData,
        ) -> Result<(), anyhow::Error> {
            println!(
                "[{}] Hello {}! I'm {}.",
                task_info.id(),
                self.person,
                app_context.get_app_name()
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_worker_pool() {
        let my_app_context = ApplicationContext::new();

        let task_store = PgTaskStore::new(pool().await);

        let (join_handle, queue) = WorkerPool::new(task_store, move |_| my_app_context.clone())
            .register_task_type::<GreetingTask>(1, RetentionMode::RemoveDone)
            .start(futures::future::ready(()))
            .await
            .unwrap();

        queue
            .enqueue(GreetingTask {
                person: "Rafael".to_string(),
            })
            .await
            .unwrap();

        join_handle.await.unwrap();
    }

    async fn pool() -> Pool<AsyncPgConnection> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            option_env!("DATABASE_URL").expect("DATABASE_URL must be set"),
        );
        Pool::builder()
            .max_size(1)
            .min_idle(Some(1))
            .build(manager)
            .await
            .unwrap()
    }
}
