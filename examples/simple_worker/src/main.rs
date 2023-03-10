use backie::{PgTaskStore, RetentionMode, WorkerPool};
use diesel_async::pg::AsyncPgConnection;
use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
use simple_worker::MyApplicationContext;
use simple_worker::MyFailingTask;
use simple_worker::MyTask;

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
    let (join_handle, queue) = WorkerPool::new(task_store, move |_| my_app_context.clone())
        .register_task_type::<MyTask>(1, RetentionMode::RemoveDone)
        .register_task_type::<MyFailingTask>(1, RetentionMode::RemoveDone)
        .start(async move {
            let _ = rx.changed().await;
        })
        .await
        .unwrap();

    log::info!("Workers started ...");

    let task1 = MyTask::new(0);
    let task2 = MyTask::new(20_000);
    let task3 = MyFailingTask::new(50_000);

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
