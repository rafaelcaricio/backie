use crate::task::{CurrentTask, TaskHash};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, ser::Serialize};
use std::fmt::Debug;

/// The [`BackgroundTask`] trait is used to define the behaviour of a task. You must implement this
/// trait for all tasks you want to execute.
///
/// The [`BackgroundTask::TASK_NAME`] attribute must be unique for the whole application. This
/// attribute is critical for reconstructing the task back from the database.
///
/// The [`BackgroundTask::AppData`] can be used to argument the task with application specific
/// contextual information. This is useful for example to pass a database connection pool to the
/// task or other application configuration.
///
/// The [`BackgroundTask::run`] method is the main method of the task. It is executed by the
/// the task queue workers.
///
///
/// # Example
/// ```
/// use async_trait::async_trait;
/// use backie::{BackgroundTask, CurrentTask};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// pub struct MyTask {}
///
/// #[async_trait]
/// impl BackgroundTask for MyTask {
///     const TASK_NAME: &'static str = "my_task_unique_name";
///     type AppData = ();
///     type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
///
///     async fn run(&self, task: CurrentTask, context: Self::AppData) -> Result<(), Self::Error> {
///         // Do something
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait BackgroundTask: Serialize + DeserializeOwned + Sync + Send + 'static {
    /// Unique name of the task.
    ///
    /// This MUST be unique for the whole application.
    const TASK_NAME: &'static str;

    /// Task queue where this task will be executed.
    ///
    /// Used to route to which workers are going to be executing this task. It uses the default
    /// task queue if not changed.
    const QUEUE: &'static str = "default";

    /// Number of retries for tasks.
    ///
    /// By default, it is set to 5.
    const MAX_RETRIES: i32 = 5;

    /// The application data provided to this task at runtime.
    type AppData: Clone + Send + 'static;

    /// An application custom error type.
    type Error: Debug + Send + 'static;

    /// Execute the task. This method should define its logic
    async fn run(&self, task: CurrentTask, context: Self::AppData) -> Result<(), Self::Error>;

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
