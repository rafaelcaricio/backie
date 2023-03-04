use fang::queue::AsyncQueue;
use fang::queue::AsyncQueueable;
use fang::worker_pool::AsyncWorkerPool;
use fang::runnable::AsyncRunnable;
use simple_async_worker::MyFailingTask;
use simple_async_worker::MyTask;
use std::time::Duration;
use diesel_async::pg::AsyncPgConnection;
use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
use diesel::PgConnection;

#[tokio::main]
async fn main() {
    env_logger::init();

    let connection_url = "postgres://postgres:password@localhost/fang";

    log::info!("Starting...");
    let max_pool_size: u32 = 3;
    let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(connection_url);
    let pool = Pool::builder()
        .max_size(max_pool_size)
        .min_idle(Some(1))
        .build(manager)
        .await
        .unwrap();

    let mut queue = AsyncQueue::builder()
        .pool(pool)
        .build();

    log::info!("Queue connected...");

    let mut workers_pool: AsyncWorkerPool<AsyncQueue> = AsyncWorkerPool::builder()
        .number_of_workers(10_u32)
        .queue(queue.clone())
        .build();

    log::info!("Pool created ...");

    workers_pool.start().await;
    log::info!("Workers started ...");

    let task1 = MyTask::new(0);
    let task2 = MyTask::new(20_000);
    let task3 = MyFailingTask::new(50_000);

    queue
        .insert_task(&task1 as &dyn AsyncRunnable)
        .await
        .unwrap();

    queue
        .insert_task(&task2 as &dyn AsyncRunnable)
        .await
        .unwrap();

    queue
        .insert_task(&task3 as &dyn AsyncRunnable)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(100)).await;
}
