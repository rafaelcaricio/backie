use serde_json::Error as SerdeError;
use std::fmt::Display;
use thiserror::Error;

/// Library errors
#[derive(Debug, Error)]
pub enum BackieError {
    QueueProcessingError(#[from] AsyncQueueError),
    SerializationError(#[from] SerdeError),
    ShutdownError(#[from] tokio::sync::watch::error::SendError<()>),
}

impl Display for BackieError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackieError::QueueProcessingError(error) => {
                write!(f, "Queue processing error: {}", error)
            }
            BackieError::SerializationError(error) => write!(f, "Serialization error: {}", error),
            BackieError::ShutdownError(error) => write!(f, "Shutdown error: {}", error),
        }
    }
}

/// List of error types that can occur while working with cron schedules.
#[derive(Debug, Error)]
pub enum CronError {
    /// A problem occured during cron schedule parsing.
    #[error(transparent)]
    LibraryError(#[from] cron::error::Error),
    /// [`Scheduled`] enum variant is not provided
    #[error("You have to implement method `cron()` in your AsyncRunnable")]
    TaskNotSchedulableError,
    /// The next execution can not be determined using the current [`Scheduled::CronPattern`]
    #[error("No timestamps match with this cron pattern")]
    NoTimestampsError,
}

#[derive(Debug, Error)]
pub enum AsyncQueueError {
    #[error(transparent)]
    PgError(#[from] diesel::result::Error),

    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),

    #[error(transparent)]
    CronError(#[from] CronError),

    #[error("Task is not in progress, operation not allowed")]
    TaskNotRunning,
}

impl From<cron::error::Error> for AsyncQueueError {
    fn from(error: cron::error::Error) -> Self {
        AsyncQueueError::CronError(CronError::LibraryError(error))
    }
}
