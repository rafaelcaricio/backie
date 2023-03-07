use serde_json::Error as SerdeError;
use std::fmt::Display;
use thiserror::Error;

/// Library errors
#[derive(Debug, Clone, Error)]
pub struct BackieError {
    /// A description of an error
    pub description: String,
}

impl Display for BackieError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl From<AsyncQueueError> for BackieError {
    fn from(error: AsyncQueueError) -> Self {
        let message = format!("{error:?}");
        BackieError {
            description: message,
        }
    }
}

impl From<SerdeError> for BackieError {
    fn from(error: SerdeError) -> Self {
        Self::from(AsyncQueueError::SerdeError(error))
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
