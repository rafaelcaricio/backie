use crate::errors::BackieError;
use crate::queue::Queue;
use crate::runnable::BackgroundTask;
use crate::store::TaskStore;
use crate::worker::{runnable, ExecuteTaskFn};
use crate::worker::{StateFn, Worker};
use crate::RetentionMode;
use futures::future::join_all;
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

        let mut worker_handles = Vec::new();

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
                // grabs the join handle for every worker for graceful shutdown
                let join_handle = tokio::spawn(async move {
                    match worker.run_tasks().await {
                        Ok(()) => log::info!("Worker {worker_name} stopped successfully"),
                        Err(err) => log::error!("Worker {worker_name} stopped due to error: {err}"),
                    }
                });
                worker_handles.push(join_handle);
            }
        }

        Ok((
            tokio::spawn(async move {
                graceful_shutdown.await;
                if let Err(err) = tx.send(()) {
                    log::warn!("Failed to send shutdown signal to worker pool: {}", err);
                } else {
                    // Wait for all workers to finish processing
                    let results = join_all(worker_handles)
                        .await
                        .into_iter()
                        .filter(Result::is_err)
                        .map(Result::unwrap_err)
                        .collect::<Vec<_>>();
                    if !results.is_empty() {
                        log::error!("Worker pool stopped with errors: {:?}", results);
                    } else {
                        log::info!("Worker pool stopped gracefully");
                    }
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
    use async_trait::async_trait;
    use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
    use diesel_async::AsyncPgConnection;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Mutex;

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

        async fn run(
            &self,
            task: CurrentTask,
            context: Self::AppData,
        ) -> Result<(), anyhow::Error> {
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

    #[tokio::test]
    async fn test_worker_pool_stop_after_task_execute() {
        #[derive(Clone)]
        struct NotifyFinishedContext {
            /// Used to notify the task ran
            tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
        }

        /// A task that notifies the test that it ran
        #[derive(serde::Serialize, serde::Deserialize)]
        struct NotifyFinished;

        #[async_trait]
        impl BackgroundTask for NotifyFinished {
            const TASK_NAME: &'static str = "notify_finished";

            type AppData = NotifyFinishedContext;

            async fn run(
                &self,
                task: CurrentTask,
                context: Self::AppData,
            ) -> Result<(), anyhow::Error> {
                // Notify the test that the task ran
                match context.tx.lock().await.take() {
                    None => println!("Cannot notify, already done that!"),
                    Some(tx) => {
                        tx.send(()).unwrap();
                        println!("[{}] Notify finished did it's job!", task.id())
                    }
                };
                Ok(())
            }
        }

        let (tx, rx) = tokio::sync::oneshot::channel();

        let my_app_context = NotifyFinishedContext {
            tx: Arc::new(Mutex::new(Some(tx))),
        };

        let (join_handle, queue) =
            WorkerPool::new(memory_store().await, move |_| my_app_context.clone())
                .register_task_type::<NotifyFinished>()
                .configure_queue("default", 1, RetentionMode::default())
                .start(async move {
                    rx.await.unwrap();
                    println!("Worker pool got notified to stop");
                })
                .await
                .unwrap();

        // Notifies the worker pool to stop after the task is executed
        queue.enqueue(NotifyFinished).await.unwrap();

        // This makes sure the task can run multiple times and use the shared context
        queue.enqueue(NotifyFinished).await.unwrap();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_worker_pool_try_to_run_unknown_task() {
        #[derive(Clone)]
        struct NotifyUnknownRanContext {
            /// Notify that application should stop
            should_stop: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,

            /// Used to mark if the unknown task ran
            unknown_task_ran: Arc<AtomicBool>,
        }

        /// A task that notifies the test that it ran
        #[derive(serde::Serialize, serde::Deserialize)]
        struct NotifyStopDuringRun;

        #[async_trait]
        impl BackgroundTask for NotifyStopDuringRun {
            const TASK_NAME: &'static str = "notify_finished";

            type AppData = NotifyUnknownRanContext;

            async fn run(
                &self,
                task: CurrentTask,
                context: Self::AppData,
            ) -> Result<(), anyhow::Error> {
                // Notify the test that the task ran
                match context.should_stop.lock().await.take() {
                    None => println!("Cannot notify, already done that!"),
                    Some(tx) => {
                        tx.send(()).unwrap();
                        println!("[{}] Notify finished did it's job!", task.id())
                    }
                };
                Ok(())
            }
        }

        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct UnknownTask;

        #[async_trait]
        impl BackgroundTask for UnknownTask {
            const TASK_NAME: &'static str = "unknown_task";

            type AppData = NotifyUnknownRanContext;

            async fn run(
                &self,
                task: CurrentTask,
                context: Self::AppData,
            ) -> Result<(), anyhow::Error> {
                println!("[{}] Unknown task ran!", task.id());
                context.unknown_task_ran.store(true, Ordering::Relaxed);
                Ok(())
            }
        }

        let (tx, rx) = tokio::sync::oneshot::channel();

        let my_app_context = NotifyUnknownRanContext {
            should_stop: Arc::new(Mutex::new(Some(tx))),
            unknown_task_ran: Arc::new(AtomicBool::new(false)),
        };

        let task_store = memory_store().await;

        let (join_handle, queue) = WorkerPool::new(task_store, {
            let my_app_context = my_app_context.clone();
            move |_| my_app_context.clone()
        })
        .register_task_type::<NotifyStopDuringRun>()
        .configure_queue("default", 1, RetentionMode::default())
        .start(async move {
            rx.await.unwrap();
            println!("Worker pool got notified to stop");
        })
        .await
        .unwrap();

        // Enqueue a task that is not registered
        queue.enqueue(UnknownTask).await.unwrap();

        // Notifies the worker pool to stop for this test
        queue.enqueue(NotifyStopDuringRun).await.unwrap();

        join_handle.await.unwrap();

        assert!(
            !my_app_context.unknown_task_ran.load(Ordering::Relaxed),
            "Unknown task ran but it is not registered in the worker pool!"
        );
    }

    /// This test will make sure that the worker pool will only stop after all workers are done.
    /// We create a KeepAliveTask that will keep running until we notify it to stop.
    /// We stop the worker pool and make sure that the KeepAliveTask is still running.
    /// Then we notify the KeepAliveTask to stop and make sure that the worker pool stops.
    #[tokio::test]
    async fn tasks_only_stop_running_when_finished() {
        #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        enum PingPongGame {
            Ping,
            Pong,
            StopThisNow,
        }

        #[derive(Clone)]
        struct PlayerContext {
            /// Used to communicate with the running task
            pong_tx: Arc<tokio::sync::mpsc::Sender<PingPongGame>>,
            ping_rx: Arc<Mutex<tokio::sync::mpsc::Receiver<PingPongGame>>>,
        }

        /// Task that will respond to the ping pong game and keep alive as long as we need
        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct KeepAliveTask;

        #[async_trait]
        impl BackgroundTask for KeepAliveTask {
            const TASK_NAME: &'static str = "keep_alive_task";

            type AppData = PlayerContext;

            async fn run(
                &self,
                _task: CurrentTask,
                context: Self::AppData,
            ) -> Result<(), anyhow::Error> {
                loop {
                    let msg = context.ping_rx.lock().await.recv().await.unwrap();
                    match msg {
                        PingPongGame::Ping => {
                            println!("Pong!");
                            context.pong_tx.send(PingPongGame::Pong).await.unwrap();
                        }
                        PingPongGame::Pong => {
                            context.pong_tx.send(PingPongGame::Ping).await.unwrap();
                        }
                        PingPongGame::StopThisNow => {
                            println!("Got stop signal, stopping the ping pong game now!");
                            break;
                        }
                    }
                }
                Ok(())
            }
        }

        let (notify_stop_worker_pool, should_stop) = tokio::sync::oneshot::channel();
        let (pong_tx, mut pong_rx) = tokio::sync::mpsc::channel(1);
        let (ping_tx, ping_rx) = tokio::sync::mpsc::channel(1);

        let player_context = PlayerContext {
            pong_tx: Arc::new(pong_tx),
            ping_rx: Arc::new(Mutex::new(ping_rx)),
        };

        let task_store = memory_store().await;

        let (worker_pool_finished, queue) = WorkerPool::new(task_store, {
            let player_context = player_context.clone();
            move |_| player_context.clone()
        })
        .register_task_type::<KeepAliveTask>()
        .configure_queue("default", 1, RetentionMode::default())
        .start(async move {
            should_stop.await.unwrap();
            println!("Worker pool got notified to stop");
        })
        .await
        .unwrap();

        queue.enqueue(KeepAliveTask).await.unwrap();

        // Make sure task is running
        println!("Ping!");
        ping_tx.send(PingPongGame::Ping).await.unwrap();
        assert_eq!(pong_rx.recv().await.unwrap(), PingPongGame::Pong);

        // Notify to stop the worker pool
        notify_stop_worker_pool.send(()).unwrap();

        // Make sure task is still running
        println!("Ping!");
        ping_tx.send(PingPongGame::Ping).await.unwrap();
        assert_eq!(pong_rx.recv().await.unwrap(), PingPongGame::Pong);

        // is_none() means that the worker pool is still waiting for tasks to finish, which is what we want!
        assert!(
            worker_pool_finished.now_or_never().is_none(),
            "Worker pool finished before task stopped!"
        );

        // Notify to stop the task, which will stop the worker pool
        ping_tx.send(PingPongGame::StopThisNow).await.unwrap();
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
