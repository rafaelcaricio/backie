use crate::queue::Queueable;
use crate::task::TaskHash;
use crate::task::TaskType;
use crate::Scheduled;
use async_trait::async_trait;
use std::error::Error;

pub const RETRIES_NUMBER: i32 = 20;

/// Task that can be executed by the queue.
///
/// The `RunnableTask` trait is used to define the behaviour of a task. You must implement this
/// trait for all tasks you want to execute.
#[typetag::serde(tag = "type")]
#[async_trait]
pub trait RunnableTask: Send + Sync {
    /// Execute the task. This method should define its logic
    async fn run(&self, queue: &mut dyn Queueable) -> Result<(), Box<dyn Error + Send + 'static>>;

    /// Define the type of the task.
    /// The `common` task type is used by default
    fn task_type(&self) -> TaskType {
        TaskType::default()
    }

    /// If set to true, no new tasks with the same metadata will be inserted
    /// By default it is set to false.
    fn uniq(&self) -> Option<TaskHash> {
        None
    }

    /// This method defines if a task is periodic or it should be executed once in the future.
    ///
    /// Be careful it works only with the UTC timezone.
    ///
    /// Example:
    ///
    /// ```rust
    /// fn cron(&self) -> Option<Scheduled> {
    ///     let expression = "0/20 * * * Aug-Sep * 2022/1";
    ///     Some(Scheduled::CronPattern(expression.to_string()))
    /// }
    ///```
    /// In order to schedule  a task once, use the `Scheduled::ScheduleOnce` enum variant.
    fn cron(&self) -> Option<Scheduled> {
        None
    }

    /// Define the maximum number of retries the task will be retried.
    /// By default the number of retries is 20.
    fn max_retries(&self) -> i32 {
        RETRIES_NUMBER
    }

    /// Define the backoff mode
    /// By default, it is exponential,  2^(attempt)
    fn backoff(&self, attempt: u32) -> u32 {
        u32::pow(2, attempt)
    }
}
