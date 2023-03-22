use async_trait::async_trait;
use backie::{BackgroundTask, CurrentTask, QueueConfig, RetentionMode};
use backie::{PgTaskStore, Queue, WorkerPool};
use diesel_async::pg::AsyncPgConnection;
use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

#[derive(Clone, Debug)]
pub struct MyApplicationContext {
    app_name: String,
    notify_finished: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl MyApplicationContext {
    pub fn new(app_name: &str, notify_finished: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            app_name: app_name.to_string(),
            notify_finished: Arc::new(Mutex::new(Some(notify_finished))),
        }
    }

    pub async fn notify_finished(&self) {
        let mut lock = self.notify_finished.lock().await;
        if let Some(sender) = lock.take() {
            sender.send(()).unwrap();
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MyTask {
    pub number: u16,
}

impl MyTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[async_trait]
impl BackgroundTask for MyTask {
    const TASK_NAME: &'static str = "my_task";
    type AppData = MyApplicationContext;
    type Error = anyhow::Error;

    async fn run(&self, task: CurrentTask, ctx: Self::AppData) -> Result<(), Self::Error> {
        log::info!(
            "[{}] Hello from {}! the current number is {}",
            task.id(),
            ctx.app_name,
            self.number
        );
        tokio::time::sleep(Duration::from_secs(3)).await;

        log::info!("[{}] done..", task.id());
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct MyFailingTask {
    pub number: u16,
}

impl MyFailingTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[async_trait]
impl BackgroundTask for MyFailingTask {
    const TASK_NAME: &'static str = "my_failing_task";
    type AppData = MyApplicationContext;
    type Error = anyhow::Error;

    async fn run(&self, task: CurrentTask, _ctx: Self::AppData) -> Result<(), Self::Error> {
        log::info!("[{}] the current number is {}", task.id(), self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        log::info!("[{}] done..", task.id());
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct EmptyTask {
    pub idx: u64,
}

#[async_trait]
impl BackgroundTask for EmptyTask {
    const TASK_NAME: &'static str = "empty_task";
    const QUEUE: &'static str = "loaded_queue";
    type AppData = MyApplicationContext;
    type Error = anyhow::Error;

    async fn run(&self, _task: CurrentTask, _ctx: Self::AppData) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct FinalTask;

#[async_trait]
impl BackgroundTask for FinalTask {
    const TASK_NAME: &'static str = "final_task";
    const QUEUE: &'static str = "loaded_queue";
    type AppData = MyApplicationContext;
    type Error = anyhow::Error;

    async fn run(&self, _task: CurrentTask, ctx: Self::AppData) -> Result<(), Self::Error> {
        ctx.notify_finished().await;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let connection_url = "postgres://postgres:password@localhost/backie";

    log::info!("Starting...");
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(connection_url);
    let pool = Pool::builder()
        .max_size(300)
        .min_idle(Some(1))
        .build(manager)
        .await
        .unwrap();
    log::info!("Pool created ...");

    let (tx, mut rx) = tokio::sync::watch::channel(false);

    let (notify_finished, wait_done) = tokio::sync::oneshot::channel();

    // Some global application context I want to pass to my background tasks
    let my_app_context = MyApplicationContext::new("Backie Example App", notify_finished);

    // queue.enqueue(task1).await.unwrap();
    // queue.enqueue(task2).await.unwrap();
    // queue.enqueue(task3).await.unwrap();

    // Store all task to join them later
    let mut tasks = JoinSet::new();

    for i in 0..1_000 {
        tasks.spawn({
            let pool = pool.clone();
            async move {
                let mut connection = pool.get().await.unwrap();

                let task = EmptyTask { idx: i };
                task.enqueue(&mut connection).await.unwrap();
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        let _ = result?;
    }

    (FinalTask {})
        .enqueue(&mut pool.get().await.unwrap())
        .await
        .unwrap();
    log::info!("Tasks created ...");

    let started = Instant::now();

    // Register the task types I want to use and start the worker pool
    let join_handle = WorkerPool::new(PgTaskStore::new(pool.clone()), move || my_app_context.clone())
        .register_task_type::<MyTask>()
        .register_task_type::<MyFailingTask>()
        .register_task_type::<EmptyTask>()
        .register_task_type::<FinalTask>()
        .configure_queue("default".into())
        .configure_queue(
            QueueConfig::new("loaded_queue")
                .pull_interval(Duration::from_millis(100))
                .retention_mode(RetentionMode::RemoveDone)
                .num_workers(300),
        )
        .start(async move {
            let _ = rx.changed().await;
        })
        .await
        .unwrap();

    log::info!("Workers started ...");

    wait_done.await.unwrap();
    let elapsed = started.elapsed();
    println!("Ran 50k jobs in {} seconds", elapsed.as_secs());

    // Wait for Ctrl+C
    // let _ = tokio::signal::ctrl_c().await;
    log::info!("Stopping ...");
    tx.send(true).unwrap();
    join_handle.await.unwrap();
    log::info!("Workers Stopped!");

    Ok(())
}
