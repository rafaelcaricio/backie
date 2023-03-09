use simple_worker::MyFailingTask;
use simple_worker::MyTask;
use std::time::Duration;
use diesel_async::pg::AsyncPgConnection;
use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
use backie::{PgAsyncQueue, WorkerPool, Queueable};

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

    let mut queue = PgAsyncQueue::new(pool);

    let (tx, mut rx) = tokio::sync::watch::channel(false);

    let executor_task = tokio::spawn({
        let mut queue = queue.clone();
        async move {
            let mut workers_pool: WorkerPool<PgAsyncQueue> = WorkerPool::builder()
                .number_of_workers(10_u32)
                .queue(queue)
                .build();

            log::info!("Workers starting ...");
            workers_pool.start(async move {
                rx.changed().await;
            }).await;
            log::info!("Workers stopped!");
        }
    });

    let task1 = MyTask::new(0);
    let task2 = MyTask::new(20_000);
    let task3 = MyFailingTask::new(50_000);

    queue
        .create_task(&task1)
        .await
        .unwrap();

    queue
        .create_task(&task2)
        .await
        .unwrap();

    queue
        .create_task(&task3)
        .await
        .unwrap();

    log::info!("Tasks created ...");
    tokio::signal::ctrl_c().await;
    log::info!("Stopping ...");
    tx.send(true).unwrap();
    executor_task.await.unwrap();
    log::info!("Stopped!");
}
