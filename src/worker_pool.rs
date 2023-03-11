use crate::errors::BackieError;
use crate::queue::Queue;
use crate::runnable::BackgroundTask;
use crate::store::TaskStore;
use crate::worker::{runnable, ExecuteTaskFn};
use crate::worker::{StateFn, Worker};
use crate::RetentionMode;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct WorkerPool<AppData, S>
where
    AppData: Clone + Send + 'static,
    S: TaskStore,
{
    /// Storage of tasks.
    task_store: Arc<S>,

    /// Queue used to spawn tasks.
    queue: Queue<S>,

    /// Make possible to load the application data.
    ///
    /// The application data is loaded when the worker pool is started and is passed to the tasks.
    /// The loading function accepts a queue instance in case the application data depends on it. This
    /// is interesting for situations where the application wants to allow tasks to spawn other tasks.
    application_data_fn: StateFn<AppData>,

    /// The types of task the worker pool can execute and the loaders for them.
    task_registry: BTreeMap<String, ExecuteTaskFn<AppData>>,

    /// The queue names for the registered tasks.
    queue_tasks: BTreeMap<String, Vec<String>>,

    /// Number of workers that will be spawned per queue.
    worker_queues: BTreeMap<String, (RetentionMode, u32)>,
}

impl<AppData, S> WorkerPool<AppData, S>
where
    AppData: Clone + Send + 'static,
    S: TaskStore,
{
    /// Create a new worker pool.
    pub fn new<A>(task_store: S, application_data_fn: A) -> Self
    where
        A: Fn(Queue<S>) -> AppData + Send + Sync + 'static,
    {
        let queue_store = Arc::new(task_store);
        let queue = Queue::new(queue_store.clone());
        let application_data_fn = {
            let queue = queue.clone();
            move || application_data_fn(queue.clone())
        };
        Self {
            task_store: queue_store,
            queue,
            application_data_fn: Arc::new(application_data_fn),
            task_registry: BTreeMap::new(),
            queue_tasks: BTreeMap::new(),
            worker_queues: BTreeMap::new(),
        }
    }

    /// Register a task type with the worker pool.
    pub fn register_task_type<BT>(mut self) -> Self
    where
        BT: BackgroundTask<AppData = AppData>,
    {
        self.queue_tasks
            .entry(BT::QUEUE.to_string())
            .or_insert_with(Vec::new)
            .push(BT::TASK_NAME.to_string());
        self.task_registry
            .insert(BT::TASK_NAME.to_string(), Arc::new(runnable::<BT>));
        self
    }

    pub fn configure_queue(
        mut self,
        queue_name: impl ToString,
        num_workers: u32,
        retention_mode: RetentionMode,
    ) -> Self {
        self.worker_queues
            .insert(queue_name.to_string(), (retention_mode, num_workers));
        self
    }

    pub async fn start<F>(
        self,
        graceful_shutdown: F,
    ) -> Result<(JoinHandle<()>, Queue<S>), BackieError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Validate that all registered tasks queues are configured
        for (queue_name, tasks_for_queue) in self.queue_tasks.into_iter() {
            if !self.worker_queues.contains_key(&queue_name) {
                return Err(BackieError::QueueNotConfigured(queue_name, tasks_for_queue));
            }
        }

        let (tx, rx) = tokio::sync::watch::channel(());

        // Spawn all individual workers per queue
        for (queue_name, (retention_mode, num_workers)) in self.worker_queues.iter() {
            for idx in 0..*num_workers {
                let mut worker: Worker<AppData, S> = Worker::new(
                    self.task_store.clone(),
                    queue_name.clone(),
                    *retention_mode,
                    self.task_registry.clone(),
                    self.application_data_fn.clone(),
                    Some(rx.clone()),
                );
                let worker_name = format!("worker-{queue_name}-{idx}");
                // TODO: grab the join handle for every worker for graceful shutdown
                tokio::spawn(async move {
                    match worker.run_tasks().await {
                        Ok(()) => log::info!("Worker {worker_name} stopped sucessfully"),
                        Err(err) => log::error!("Worker {worker_name} stopped due to error: {err}"),
                    }
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
    use crate::store::test_store::MemoryTaskStore;
    use crate::store::PgTaskStore;
    use crate::task::CurrentTask;
    use anyhow::Error;
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

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct OtherTask;

    #[async_trait]
    impl BackgroundTask for OtherTask {
        const TASK_NAME: &'static str = "other_task";

        const QUEUE: &'static str = "other_queue";

        type AppData = ApplicationContext;

        async fn run(&self, task: CurrentTask, context: Self::AppData) -> Result<(), Error> {
            println!(
                "[{}] Other task with {}!",
                task.id(),
                context.get_app_name()
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn validate_all_registered_tasks_queues_are_configured() {
        let my_app_context = ApplicationContext::new();

        let result = WorkerPool::new(memory_store().await, move |_| my_app_context.clone())
            .register_task_type::<GreetingTask>()
            .start(futures::future::ready(()))
            .await;

        assert!(matches!(result, Err(BackieError::QueueNotConfigured(..))));
        if let Err(err) = result {
            assert_eq!(
                err.to_string(),
                "Queue \"default\" needs to be configured because of registered tasks: [\"my_task\"]"
            );
        }
    }

    #[tokio::test]
    async fn test_worker_pool_with_task() {
        let my_app_context = ApplicationContext::new();

        let (join_handle, queue) =
            WorkerPool::new(memory_store().await, move |_| my_app_context.clone())
                .register_task_type::<GreetingTask>()
                .configure_queue(GreetingTask::QUEUE, 1, RetentionMode::RemoveDone)
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

    #[tokio::test]
    async fn test_worker_pool_with_multiple_task_types() {
        let my_app_context = ApplicationContext::new();

        let (join_handle, queue) =
            WorkerPool::new(memory_store().await, move |_| my_app_context.clone())
                .register_task_type::<GreetingTask>()
                .register_task_type::<OtherTask>()
                .configure_queue("default", 1, RetentionMode::default())
                .configure_queue("other_queue", 1, RetentionMode::default())
                .start(futures::future::ready(()))
                .await
                .unwrap();

        queue
            .enqueue(GreetingTask {
                person: "Rafael".to_string(),
            })
            .await
            .unwrap();

        queue.enqueue(OtherTask).await.unwrap();

        join_handle.await.unwrap();
    }

    async fn memory_store() -> MemoryTaskStore {
        MemoryTaskStore::default()
    }

    #[tokio::test]
    #[ignore]
    async fn test_worker_pool_with_pg_store() {
        let my_app_context = ApplicationContext::new();

        let (join_handle, _queue) =
            WorkerPool::new(pg_task_store().await, move |_| my_app_context.clone())
                .register_task_type::<GreetingTask>()
                .configure_queue(GreetingTask::QUEUE, 1, RetentionMode::RemoveDone)
                .start(futures::future::ready(()))
                .await
                .unwrap();

        join_handle.await.unwrap();
    }

    async fn pg_task_store() -> PgTaskStore {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            option_env!("DATABASE_URL").expect("DATABASE_URL must be set"),
        );
        let pool = Pool::builder()
            .max_size(1)
            .min_idle(Some(1))
            .build(manager)
            .await
            .unwrap();

        PgTaskStore::new(pool)
    }
}
