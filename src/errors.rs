use serde_json::Error as SerdeError;
use thiserror::Error;

/// An error that can happen during executing of tasks
#[derive(Debug)]
pub struct FrangoError {
    /// A description of an error
    pub description: String,
}

impl From<AsyncQueueError> for FrangoError {
    fn from(error: AsyncQueueError) -> Self {
        let message = format!("{error:?}");
        FrangoError {
            description: message,
        }
    }
}

impl From<SerdeError> for FrangoError {
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
    #[error("Can not perform this operation if task is not uniq, please check its definition in impl AsyncRunnable")]
    TaskNotUniqError,
}

impl From<cron::error::Error> for AsyncQueueError {
    fn from(error: cron::error::Error) -> Self {
        AsyncQueueError::CronError(CronError::LibraryError(error))
    }
}
