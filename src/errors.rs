use thiserror::Error;

/// Library errors
#[derive(Debug, Error)]
pub enum BackieError {
    #[error("Queue processing error: {0}")]
    QueueProcessingError(#[from] AsyncQueueError),

    #[error("Worker Pool shutdown error: {0}")]
    WorkerPoolShutdownError(#[from] tokio::sync::watch::error::SendError<()>),

    #[error("Worker shutdown error: {0}")]
    WorkerShutdownError(#[from] tokio::sync::watch::error::RecvError),
}

/// List of error types that can occur while working with cron schedules.
#[derive(Debug, Error)]
pub enum CronError {
    /// A problem occured during cron schedule parsing.
    #[error(transparent)]
    LibraryError(#[from] cron::error::Error),
    /// [`Scheduled`] enum variant is not provided
    #[error("You have to implement method `cron()` in your Runnable")]
    TaskNotSchedulableError,
    /// The next execution can not be determined using the current [`Scheduled::CronPattern`]
    #[error("No timestamps match with this cron pattern")]
    NoTimestampsError,
}

#[derive(Debug, Error)]
pub enum AsyncQueueError {
    #[error(transparent)]
    PgError(#[from] diesel::result::Error),

    #[error("Task serialization error: {0}")]
    SerdeError(#[from] serde_json::Error),

    #[error(transparent)]
    CronError(#[from] CronError),

    #[error("Task is not in progress, operation not allowed")]
    TaskNotRunning,

    #[error("Task with name {0} is not registered")]
    TaskNotRegistered(String),
}

impl From<cron::error::Error> for AsyncQueueError {
    fn from(error: cron::error::Error) -> Self {
        AsyncQueueError::CronError(CronError::LibraryError(error))
    }
}
