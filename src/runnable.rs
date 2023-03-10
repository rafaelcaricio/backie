use crate::task::{CurrentTask, TaskHash};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, ser::Serialize};

/// Task that can be executed by the queue.
///
/// The `BackgroundTask` trait is used to define the behaviour of a task. You must implement this
/// trait for all tasks you want to execute.
#[async_trait]
pub trait BackgroundTask: Serialize + DeserializeOwned + Sync + Send + 'static {
    /// Unique name of the task.
    ///
    /// This MUST be unique for the whole application.
    const TASK_NAME: &'static str;

    /// Task queue where this task will be executed.
    ///
    /// Used to define which workers are going to be executing this task. It uses the default
    /// task queue if not changed.
    const QUEUE: &'static str = "default";

    /// Number of retries for tasks.
    ///
    /// By default, it is set to 5.
    const MAX_RETRIES: i32 = 5;

    /// The application data provided to this task at runtime.
    type AppData: Clone + Send + 'static;

    /// Execute the task. This method should define its logic
    async fn run(&self, task: CurrentTask, context: Self::AppData) -> Result<(), anyhow::Error>;

    /// If set to true, no new tasks with the same metadata will be inserted
    /// By default it is set to false.
    fn uniq(&self) -> Option<TaskHash> {
        None
    }

    /// Define the maximum number of retries the task will be retried.
    /// By default the number of retries is 20.
    fn max_retries(&self) -> i32 {
        Self::MAX_RETRIES
    }

    /// Define the backoff mode
    /// By default, it is exponential,  2^(attempt)
    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
