use crate::errors::BackieError;
use crate::runnable::BackgroundTask;
use crate::store::TaskStore;
use crate::worker::{runnable, ExecuteTaskFn};
use crate::worker::{StateFn, Worker};
use crate::RetentionMode;
use futures::future::join_all;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct WorkerPool<AppData, S>
where
    AppData: Clone + Send + 'static,
    S: TaskStore + Clone,
{
    /// Storage of tasks.
    task_store: S,

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
    worker_queues: BTreeMap<String, QueueConfig>,
}

impl<AppData, S> WorkerPool<AppData, S>
where
    AppData: Clone + Send + 'static,
    S: TaskStore + Clone,
{
    /// Create a new worker pool.
    pub fn new<A>(task_store: S, application_data_fn: A) -> Self
    where
        A: Fn() -> AppData + Send + Sync + 'static,
    {
        Self {
            task_store,
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

    pub fn configure_queue(mut self, config: QueueConfig) -> Self {
        self.worker_queues.insert(config.name.clone(), config);
        self
    }

    pub async fn start<F>(self, graceful_shutdown: F) -> Result<JoinHandle<()>, BackieError>
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
        for (queue_name, queue_config) in self.worker_queues.iter() {
            for idx in 0..queue_config.num_workers {
                let mut worker: Worker<AppData, S> = Worker::new(
                    self.task_store.clone(),
                    queue_config.to_owned(),
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

        Ok(tokio::spawn(async move {
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
        }))
    }
}

/// Configuration for a queue.
///
/// This is used to configure the number of workers, the retention mode, and the pulling interval
/// for a queue.
///
/// # Examples
///
/// Example of configuring a queue with all options:
/// ```
/// # use backie::QueueConfig;
/// # use backie::RetentionMode;
/// # use std::time::Duration;
/// let config = QueueConfig::new("default")
///     .num_workers(5)
///     .retention_mode(RetentionMode::KeepAll)
///     .execution_timeout(Duration::from_secs(60))
///     .pull_interval(Duration::from_secs(1));
/// ```
/// Example of queue configuration with default options:
/// ```
/// # use backie::QueueConfig;
/// let config = QueueConfig::new("default");
/// // Also possible to use the `From` trait:
/// let config: QueueConfig = "default".into();
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueueConfig {
    pub(crate) name: String,
    pub(crate) num_workers: u32,
    pub(crate) retention_mode: RetentionMode,
    pub(crate) execution_timeout: Option<Duration>,
    pub(crate) pull_interval: Duration,
}

impl QueueConfig {
    /// Create a new queue configuration.
    pub fn new(name: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            num_workers: 1,
            retention_mode: RetentionMode::default(),
            execution_timeout: None,
            pull_interval: Duration::from_secs(1),
        }
    }

    /// Set the number of workers for this queue.
    pub fn num_workers(mut self, num_workers: u32) -> Self {
        self.num_workers = num_workers;
        self
    }

    /// Set the retention mode for this queue.
    pub fn retention_mode(mut self, retention_mode: RetentionMode) -> Self {
        self.retention_mode = retention_mode;
        self
    }

    /// Set the execution timeout for this queue.
    ///
    /// This is the maximum time a task can run before it is considered failed and ready to retry.
    /// If this is not set, the task may run indefinitely.
    pub fn execution_timeout(mut self, execution_timeout: Duration) -> Self {
        self.execution_timeout = Some(execution_timeout);
        self
    }

    /// Set the pull interval for this queue.
    ///
    /// This is the interval at which the queue will be checking for new tasks by calling
    /// the backend storage.
    pub fn pull_interval(mut self, pull_interval: Duration) -> Self {
        self.pull_interval = pull_interval;
        self
    }
}

impl<S> From<S> for QueueConfig
where
    S: ToString,
{
    fn from(name: S) -> Self {
        Self::new(name.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_store::{MemoryTask, MemoryTaskStore};
    use crate::store::PgTaskStore;
    use crate::task::CurrentTask;
    use async_trait::async_trait;
    use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
    use diesel_async::AsyncPgConnection;
    use futures::FutureExt;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Mutex;

    #[derive(Clone, Debug)]
    pub struct ApplicationContext {
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

    /// This tests that one can customize the task parameters for the application.
    #[async_trait]
    trait MyAppTask {
        const TASK_NAME: &'static str;
        const QUEUE: &'static str = "default";

        async fn run(
            &self,
            task_info: CurrentTask,
            app_context: ApplicationContext,
        ) -> Result<(), ()>;
    }

    #[async_trait]
    impl<T> BackgroundTask for T
    where
        T: MyAppTask + serde::de::DeserializeOwned + serde::ser::Serialize + Sync + Send + 'static,
    {
        const TASK_NAME: &'static str = T::TASK_NAME;

        const QUEUE: &'static str = T::QUEUE;

        type AppData = ApplicationContext;

        type Error = ();

        async fn run(
            &self,
            task_info: CurrentTask,
            app_context: Self::AppData,
        ) -> Result<(), Self::Error> {
            self.run(task_info, app_context).await
        }
    }

    #[async_trait]
    impl MyAppTask for GreetingTask {
        const TASK_NAME: &'static str = "my_task";

        async fn run(
            &self,
            task_info: CurrentTask,
            app_context: ApplicationContext,
        ) -> Result<(), ()> {
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
        type Error = ();

        async fn run(&self, task: CurrentTask, context: Self::AppData) -> Result<(), Self::Error> {
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

        let result = WorkerPool::new(memory_store(), move || my_app_context.clone())
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

        let task_store = memory_store();

        let join_handle = WorkerPool::new(task_store.clone(), move || my_app_context.clone())
            .register_task_type::<GreetingTask>()
            .configure_queue(<GreetingTask as MyAppTask>::QUEUE.into())
            .start(futures::future::ready(()))
            .await
            .unwrap();

        let task = GreetingTask {
            person: "Rafael".to_string(),
        };
        task.enqueue(&task_store).await.unwrap();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_worker_pool_with_multiple_task_types() {
        let my_app_context = ApplicationContext::new();

        let task_store = memory_store();
        let join_handle = WorkerPool::new(task_store.clone(), move || my_app_context.clone())
            .register_task_type::<GreetingTask>()
            .register_task_type::<OtherTask>()
            .configure_queue("default".into())
            .configure_queue("other_queue".into())
            .start(futures::future::ready(()))
            .await
            .unwrap();

        let task = GreetingTask {
            person: "Rafael".to_string(),
        };
        task.enqueue(&task_store).await.unwrap();

        OtherTask.enqueue(&task_store).await.unwrap();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_worker_pool_stop_after_task_execute() {
        #[derive(Clone)]
        struct NotifyFinishedContext {
            /// Used to notify the task ran
            notify_finished: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
        }

        /// A task that notifies the test that it ran
        #[derive(serde::Serialize, serde::Deserialize)]
        struct NotifyFinished;

        #[async_trait]
        impl BackgroundTask for NotifyFinished {
            const TASK_NAME: &'static str = "notify_finished";

            type AppData = NotifyFinishedContext;

            type Error = ();

            async fn run(&self, task: CurrentTask, context: Self::AppData) -> Result<(), ()> {
                // Notify the test that the task ran
                match context.notify_finished.lock().await.take() {
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
            notify_finished: Arc::new(Mutex::new(Some(tx))),
        };

        let memory_store = memory_store();

        let join_handle = WorkerPool::new(memory_store.clone(), move || my_app_context.clone())
            .register_task_type::<NotifyFinished>()
            .configure_queue("default".into())
            .start(async move {
                rx.await.unwrap();
                println!("Worker pool got notified to stop");
            })
            .await
            .unwrap();

        // Notifies the worker pool to stop after the task is executed
        NotifyFinished.enqueue(&memory_store).await.unwrap();

        // This makes sure the task can run multiple times and use the shared context
        NotifyFinished.enqueue(&memory_store).await.unwrap();

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

            type Error = ();

            async fn run(
                &self,
                task: CurrentTask,
                context: Self::AppData,
            ) -> Result<(), Self::Error> {
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

            type Error = ();

            async fn run(&self, task: CurrentTask, context: Self::AppData) -> Result<(), ()> {
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

        let task_store = memory_store();

        let join_handle = WorkerPool::new(task_store.clone(), {
            let my_app_context = my_app_context.clone();
            move || my_app_context.clone()
        })
        .register_task_type::<NotifyStopDuringRun>()
        .configure_queue("default".into())
        .start(async move {
            rx.await.unwrap();
            println!("Worker pool got notified to stop");
        })
        .await
        .unwrap();

        // Enqueue a task that is not registered
        UnknownTask.enqueue(&task_store).await.unwrap();

        // Notifies the worker pool to stop for this test
        NotifyStopDuringRun.enqueue(&task_store).await.unwrap();

        join_handle.await.unwrap();

        assert!(
            !my_app_context.unknown_task_ran.load(Ordering::Relaxed),
            "Unknown task ran but it is not registered in the worker pool!"
        );
    }

    #[tokio::test]
    async fn task_can_panic_and_not_affect_worker() {
        #[derive(Clone, serde::Serialize, serde::Deserialize)]
        struct BrokenTask;

        #[async_trait]
        impl BackgroundTask for BrokenTask {
            const TASK_NAME: &'static str = "panic_me";
            type AppData = ();
            type Error = ();

            async fn run(&self, _task: CurrentTask, _context: Self::AppData) -> Result<(), ()> {
                panic!("Oh no!");
            }
        }

        let (notify_stop_worker_pool, should_stop) = tokio::sync::oneshot::channel();

        let task_store = memory_store();

        let worker_pool_finished = WorkerPool::new(task_store.clone(), || ())
            .register_task_type::<BrokenTask>()
            .configure_queue("default".into())
            .start(async move {
                should_stop.await.unwrap();
            })
            .await
            .unwrap();

        // Enqueue a task that will panic
        BrokenTask.enqueue(&task_store).await.unwrap();

        notify_stop_worker_pool.send(()).unwrap();
        worker_pool_finished.await.unwrap();

        let raw_task = task_store
            .tasks
            .lock()
            .await
            .first_entry()
            .unwrap()
            .remove();
        assert_eq!(
            serde_json::to_string(&raw_task.error_info.unwrap()).unwrap(),
            "{\"error\":\"Task panicked with: Oh no!\"}"
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

            type Error = ();

            async fn run(
                &self,
                _task: CurrentTask,
                context: Self::AppData,
            ) -> Result<(), Self::Error> {
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

        let task_store = memory_store();

        let worker_pool_finished = WorkerPool::new(task_store.clone(), {
            let player_context = player_context.clone();
            move || player_context.clone()
        })
        .register_task_type::<KeepAliveTask>()
        .configure_queue("default".into())
        .start(async move {
            should_stop.await.unwrap();
            println!("Worker pool got notified to stop");
        })
        .await
        .unwrap();

        KeepAliveTask.enqueue(&task_store).await.unwrap();

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

    fn memory_store() -> MemoryTaskStore {
        MemoryTaskStore::default()
    }

    #[tokio::test]
    #[ignore]
    async fn test_worker_pool_with_pg_store() {
        let my_app_context = ApplicationContext::new();

        let join_handle = WorkerPool::new(pg_task_store().await.unwrap(), move || {
            my_app_context.clone()
        })
        .register_task_type::<GreetingTask>()
        .configure_queue(
            QueueConfig::new(<GreetingTask as MyAppTask>::QUEUE)
                .retention_mode(RetentionMode::RemoveDone),
        )
        .start(futures::future::ready(()))
        .await
        .unwrap();

        join_handle.await.unwrap();
    }

    async fn pg_task_store() -> Result<PgTaskStore, String> {
        let url = option_env!("DATABASE_URL").ok_or_else(|| "DATABASE_URL not set".to_string())?;
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url);
        let pool = Pool::builder()
            .max_size(1)
            .min_idle(Some(1))
            .build(manager)
            .await
            .unwrap();

        Ok(PgTaskStore::new(pool))
    }
}
