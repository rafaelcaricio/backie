use async_trait::async_trait;
use backie::{BackgroundTask, CurrentTask};
use backie::{PgTaskStore, Queue, WorkerPool};
use diesel_async::pg::AsyncPgConnection;
use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MyApplicationContext {
    app_name: String,
}

impl MyApplicationContext {
    pub fn new(app_name: &str) -> Self {
        Self {
            app_name: app_name.to_string(),
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

    async fn run(&self, task: CurrentTask, ctx: Self::AppData) -> Result<(), anyhow::Error> {
        // let new_task = MyTask::new(self.number + 1);
        // queue
        //     .insert_task(&new_task)
        //     .await
        //     .unwrap();

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

    async fn run(&self, task: CurrentTask, _ctx: Self::AppData) -> Result<(), anyhow::Error> {
        // let new_task = MyFailingTask::new(self.number + 1);
        // queue
        //     .insert_task(&new_task)
        //     .await
        //     .unwrap();

        // task.id();
        // task.keep_alive().await?;
        // task.previous_error();
        // task.retry_count();

        log::info!("[{}] the current number is {}", task.id(), self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        log::info!("[{}] done..", task.id());
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let connection_url = "postgres://postgres:password@localhost/backie";

    log::info!("Starting...");
    let max_pool_size: u32 = 3;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(connection_url);
    let pool = Pool::builder()
        .max_size(max_pool_size)
        .min_idle(Some(1))
        .build(manager)
        .await
        .unwrap();
    log::info!("Pool created ...");

    let task_store = PgTaskStore::new(pool);

    let (tx, mut rx) = tokio::sync::watch::channel(false);

    // Some global application context I want to pass to my background tasks
    let my_app_context = MyApplicationContext::new("Backie Example App");

    // Register the task types I want to use and start the worker pool
    let (join_handle, _queue) =
        WorkerPool::new(task_store.clone(), move |_| my_app_context.clone())
            .register_task_type::<MyTask>()
            .register_task_type::<MyFailingTask>()
            .configure_queue("default".into())
            .start(async move {
                let _ = rx.changed().await;
            })
            .await
            .unwrap();

    log::info!("Workers started ...");

    let task1 = MyTask::new(0);
    let task2 = MyTask::new(20_000);
    let task3 = MyFailingTask::new(50_000);

    let queue = Queue::new(task_store); // or use the `queue` instance returned by the worker pool
    queue.enqueue(task1).await.unwrap();
    queue.enqueue(task2).await.unwrap();
    queue.enqueue(task3).await.unwrap();
    log::info!("Tasks created ...");

    // Wait for Ctrl+C
    let _ = tokio::signal::ctrl_c().await;
    log::info!("Stopping ...");
    tx.send(true).unwrap();
    join_handle.await.unwrap();
    log::info!("Workers Stopped!");
}
