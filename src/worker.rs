use crate::errors::{AsyncQueueError, BackieError};
use crate::runnable::BackgroundTask;
use crate::store::TaskStore;
use crate::task::{CurrentTask, Task, TaskState};
use crate::RetentionMode;
use futures::future::FutureExt;
use futures::select;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;

pub type ExecuteTaskFn<AppData> = Arc<
    dyn Fn(
            CurrentTask,
            serde_json::Value,
            AppData,
        ) -> Pin<Box<dyn Future<Output = Result<(), TaskExecError>> + Send>>
        + Send
        + Sync,
>;

pub type StateFn<AppData> = Arc<dyn Fn() -> AppData + Send + Sync>;

#[derive(Debug, Error)]
pub enum TaskExecError {
    #[error("Task execution failed: {0}")]
    ExecutionFailed(#[from] anyhow::Error),

    #[error("Task deserialization failed: {0}")]
    TaskDeserializationFailed(#[from] serde_json::Error),
}

pub(crate) fn runnable<BT>(
    task_info: CurrentTask,
    payload: serde_json::Value,
    app_context: BT::AppData,
) -> Pin<Box<dyn Future<Output = Result<(), TaskExecError>> + Send>>
where
    BT: BackgroundTask,
{
    Box::pin(async move {
        let background_task: BT = serde_json::from_value(payload)?;
        background_task.run(task_info, app_context).await?;
        Ok(())
    })
}

/// Worker that executes tasks.
pub struct Worker<AppData, S>
where
    AppData: Clone + Send + 'static,
    S: TaskStore,
{
    store: Arc<S>,

    queue_name: String,

    retention_mode: RetentionMode,

    task_registry: BTreeMap<String, ExecuteTaskFn<AppData>>,

    app_data_fn: StateFn<AppData>,

    /// Notification for the worker to stop.
    shutdown: Option<tokio::sync::watch::Receiver<()>>,
}

impl<AppData, S> Worker<AppData, S>
where
    AppData: Clone + Send + 'static,
    S: TaskStore,
{
    pub(crate) fn new(
        store: Arc<S>,
        queue_name: String,
        retention_mode: RetentionMode,
        task_registry: BTreeMap<String, ExecuteTaskFn<AppData>>,
        app_data_fn: StateFn<AppData>,
        shutdown: Option<tokio::sync::watch::Receiver<()>>,
    ) -> Self {
        Self {
            store,
            queue_name,
            retention_mode,
            task_registry,
            app_data_fn,
            shutdown,
        }
    }

    pub(crate) async fn run_tasks(&mut self) -> Result<(), BackieError> {
        loop {
            // Check if has to stop before pulling next task
            if let Some(ref shutdown) = self.shutdown {
                if shutdown.has_changed()? {
                    return Ok(());
                }
            };

            match self.store.pull_next_task(&self.queue_name).await? {
                Some(task) => {
                    self.run(task).await?;
                }
                None => {
                    // Listen to watchable future
                    // All that until a max timeout
                    match &mut self.shutdown {
                        Some(recv) => {
                            // Listen to watchable future
                            // All that until a max timeout
                            select! {
                                _ = recv.changed().fuse() => {
                                    log::info!("Shutting down worker");
                                    return Ok(());
                                }
                                _ = tokio::time::sleep(std::time::Duration::from_secs(1)).fuse() => {}
                            }
                        }
                        None => {
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    };
                }
            };
        }
    }

    // #[cfg(test)]
    // pub async fn run_tasks_until_none(&mut self) -> Result<(), BackieError> {
    //     loop {
    //         match self.store.pull_next_task(self.queue_name.clone()).await? {
    //             Some(task) => {
    //                 let actual_task: Box<dyn BackgroundTask> =
    //                     serde_json::from_value(task.payload.clone()).unwrap();
    //
    //                 // check if task is scheduled or not
    //                 if let Some(CronPattern(_)) = actual_task.cron() {
    //                     // program task
    //                     // self.queue.schedule_task(&*actual_task).await?;
    //                 }
    //                 // run scheduled task
    //                 self.run(task, actual_task).await?;
    //             }
    //             None => {
    //                 return Ok(());
    //             }
    //         };
    //     }
    // }

    async fn run(&self, task: Task) -> Result<(), BackieError> {
        let task_info = CurrentTask::new(&task);
        let runnable_task_caller = self
            .task_registry
            .get(&task.task_name)
            .ok_or_else(|| AsyncQueueError::TaskNotRegistered(task.task_name.clone()))?;

        // TODO: catch panics
        let result: Result<(), TaskExecError> =
            runnable_task_caller(task_info, task.payload.clone(), (self.app_data_fn)()).await;

        match &result {
            Ok(_) => self.finalize_task(task, result).await?,
            Err(error) => {
                if task.retries < task.max_retries {
                    let backoff_seconds = 5; // TODO: runnable_task.backoff(task.retries as u32);

                    log::debug!(
                        "Task {} failed to run and will be retried in {} seconds",
                        task.id,
                        backoff_seconds
                    );
                    let error_message = format!("{}", error);
                    self.store
                        .schedule_task_retry(task.id, backoff_seconds, &error_message)
                        .await?;
                } else {
                    log::debug!("Task {} failed and reached the maximum retries", task.id);
                    self.finalize_task(task, result).await?;
                }
            }
        }
        Ok(())
    }

    async fn finalize_task(
        &self,
        task: Task,
        result: Result<(), TaskExecError>,
    ) -> Result<(), BackieError> {
        match self.retention_mode {
            RetentionMode::KeepAll => match result {
                Ok(_) => {
                    self.store.set_task_state(task.id, TaskState::Done).await?;
                    log::debug!("Task {} done and kept in the database", task.id);
                }
                Err(error) => {
                    log::debug!("Task {} failed and kept in the database", task.id);
                    self.store
                        .set_task_state(task.id, TaskState::Failed(format!("{}", error)))
                        .await?;
                }
            },
            RetentionMode::RemoveAll => {
                log::debug!("Task {} finalized and deleted from the database", task.id);
                self.store.remove_task(task.id).await?;
            }
            RetentionMode::RemoveDone => match result {
                Ok(_) => {
                    log::debug!("Task {} done and deleted from the database", task.id);
                    self.store.remove_task(task.id).await?;
                }
                Err(error) => {
                    log::debug!("Task {} failed and kept in the database", task.id);
                    self.store
                        .set_task_state(task.id, TaskState::Failed(format!("{}", error)))
                        .await?;
                }
            },
        };

        Ok(())
    }
}

#[cfg(test)]
mod async_worker_tests {
    use super::*;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    #[derive(thiserror::Error, Debug)]
    enum TaskError {
        #[error("Something went wrong")]
        SomethingWrong,

        #[error("{0}")]
        Custom(String),
    }

    #[derive(Serialize, Deserialize)]
    struct WorkerAsyncTask {
        pub number: u16,
    }

    #[async_trait]
    impl BackgroundTask for WorkerAsyncTask {
        const TASK_NAME: &'static str = "WorkerAsyncTask";
        type AppData = ();

        async fn run(&self, _: CurrentTask, _: Self::AppData) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct WorkerAsyncTaskSchedule {
        pub number: u16,
    }

    #[async_trait]
    impl BackgroundTask for WorkerAsyncTaskSchedule {
        const TASK_NAME: &'static str = "WorkerAsyncTaskSchedule";
        type AppData = ();

        async fn run(&self, _task: CurrentTask, _data: Self::AppData) -> Result<(), anyhow::Error> {
            Ok(())
        }

        // fn cron(&self) -> Option<Scheduled> {
        //     Some(Scheduled::ScheduleOnce(Utc::now() + Duration::seconds(1)))
        // }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncFailedTask {
        pub number: u16,
    }

    #[async_trait]
    impl BackgroundTask for AsyncFailedTask {
        const TASK_NAME: &'static str = "AsyncFailedTask";
        type AppData = ();

        async fn run(&self, _task: CurrentTask, _data: Self::AppData) -> Result<(), anyhow::Error> {
            let message = format!("number {} is wrong :(", self.number);

            Err(TaskError::Custom(message).into())
        }

        fn max_retries(&self) -> i32 {
            0
        }
    }

    #[derive(Serialize, Deserialize, Clone)]
    struct AsyncRetryTask {}

    #[async_trait]
    impl BackgroundTask for AsyncRetryTask {
        const TASK_NAME: &'static str = "AsyncRetryTask";
        type AppData = ();

        async fn run(&self, _task: CurrentTask, _data: Self::AppData) -> Result<(), anyhow::Error> {
            Err(TaskError::SomethingWrong.into())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskType1 {}

    #[async_trait]
    impl BackgroundTask for AsyncTaskType1 {
        const TASK_NAME: &'static str = "AsyncTaskType1";
        type AppData = ();

        async fn run(&self, _task: CurrentTask, _data: Self::AppData) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskType2 {}

    #[async_trait]
    impl BackgroundTask for AsyncTaskType2 {
        const TASK_NAME: &'static str = "AsyncTaskType2";
        type AppData = ();

        async fn run(&self, _task: CurrentTask, _data: Self::AppData) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }
}
