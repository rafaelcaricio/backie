use crate::errors::BackieError;
use crate::queue::Queueable;
use crate::task::TaskType;
use crate::worker::Worker;
use crate::RetentionMode;
use async_recursion::async_recursion;
use log::error;
use std::future::Future;
use tokio::sync::watch::Receiver;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder, Clone)]
pub struct WorkerPool<AQueue>
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
    #[builder(default, setter(into))]
    pub task_type: Option<TaskType>,
}

// impl<TypedBuilderFields, Q> AsyncWorkerBuilder<TypedBuilderFields, Q>
//     where
//         TypedBuilderFields: Clone,
//         Q: Queueable + Clone + Sync + 'static,
// {
//     pub fn with_graceful_shutdown<F>(self, signal: F) -> Self<TypedBuilderFields, Q>
//         where
//             F: Future<Output = ()>,
//     {
//         self
//     }
// }

impl<AQueue> WorkerPool<AQueue>
where
    AQueue: Queueable + Clone + Sync + 'static,
{
    /// Starts the configured number of workers
    /// This is necessary in order to execute tasks.
    pub async fn start<F>(&mut self, graceful_shutdown: F) -> Result<(), BackieError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::watch::channel(());
        for idx in 0..self.number_of_workers {
            let pool = self.clone();
            // TODO: the worker pool keeps track of the number of workers and spawns new workers as needed.
            //       There should be always a minimum number of workers active waiting for tasks to execute
            //       or for a gracefull shutdown.
            tokio::spawn(Self::supervise_task(pool, rx.clone(), 0, idx));
        }
        graceful_shutdown.await;
        tx.send(())?;
        log::info!("Worker pool stopped gracefully");
        Ok(())
    }

    #[async_recursion]
    async fn supervise_task(
        pool: WorkerPool<AQueue>,
        receiver: Receiver<()>,
        restarts: u64,
        worker_number: u32,
    ) {
        let restarts = restarts + 1;

        let inner_pool = pool.clone();
        let inner_receiver = receiver.clone();

        let join_handle = tokio::spawn(async move {
            let mut worker: Worker<AQueue> = Worker::builder()
                .queue(inner_pool.queue.clone())
                .retention_mode(inner_pool.retention_mode)
                .task_type(inner_pool.task_type.clone())
                .shutdown(inner_receiver)
                .build();

            worker.run_tasks().await
        });

        if (join_handle.await).is_err() {
            error!(
                "Worker {} stopped. Restarting. the number of restarts {}",
                worker_number, restarts,
            );
            Self::supervise_task(pool, receiver, restarts, worker_number).await;
        }
    }
}
