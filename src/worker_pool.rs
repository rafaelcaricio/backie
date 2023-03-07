use crate::queue::Queueable;
use crate::task::TaskType;
use crate::worker::AsyncWorker;
use crate::RetentionMode;
use async_recursion::async_recursion;
use log::error;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder, Clone)]
pub struct AsyncWorkerPool<AQueue>
where
    AQueue: Queueable + Clone + Sync + 'static,
{
    #[builder(setter(into))]
    /// the AsyncWorkerPool uses a queue to control the tasks that will be executed.
    pub queue: AQueue,

    /// retention_mode controls if tasks should be persisted after execution
    #[builder(default, setter(into))]
    pub retention_mode: RetentionMode,

    /// the number of workers of the AsyncWorkerPool.
    #[builder(setter(into))]
    pub number_of_workers: u32,

    /// The type of tasks that will be executed by `AsyncWorkerPool`.
    #[builder(default=None, setter(into))]
    pub task_type: Option<TaskType>,
}

impl<AQueue> AsyncWorkerPool<AQueue>
where
    AQueue: Queueable + Clone + Sync + 'static,
{
    /// Starts the configured number of workers
    /// This is necessary in order to execute tasks.
    pub async fn start(&mut self) {
        for idx in 0..self.number_of_workers {
            let pool = self.clone();
            tokio::spawn(Self::supervise_task(pool, 0, idx));
        }
    }

    #[async_recursion]
    async fn supervise_task(pool: AsyncWorkerPool<AQueue>, restarts: u64, worker_number: u32) {
        let restarts = restarts + 1;

        let inner_pool = pool.clone();

        let join_handle = tokio::spawn(async move {
            let mut worker: AsyncWorker<AQueue> = AsyncWorker::builder()
                .queue(inner_pool.queue.clone())
                .retention_mode(inner_pool.retention_mode)
                .task_type(inner_pool.task_type.clone())
                .build();

            worker.run_tasks().await
        });

        if (join_handle.await).is_err() {
            error!(
                "Worker {} stopped. Restarting. the number of restarts {}",
                worker_number, restarts,
            );
            Self::supervise_task(pool, restarts, worker_number).await;
        }
    }
}
