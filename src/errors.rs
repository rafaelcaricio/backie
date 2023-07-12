use std::error::Error;

use thiserror::Error;

/// Library errors
#[derive(Debug, Error)]
pub enum BackieError {
    #[error("Queue \"{0}\" needs to be configured because of registered tasks: {1:?}")]
    QueueNotConfigured(String, Vec<String>),

    #[error("Provided task is not serializable to JSON: {0}")]
    NonSerializableTask(#[from] serde_json::Error),

    #[error("Queue processing error: {0}")]
    QueueProcessingError(#[from] AsyncQueueError),

    #[error("Worker Pool shutdown error: {0}")]
    WorkerPoolShutdownError(#[from] tokio::sync::watch::error::SendError<()>),

    #[error("Worker shutdown error: {0}")]
    WorkerShutdownError(#[from] tokio::sync::watch::error::RecvError),
}

#[derive(Debug, Error)]
pub enum AsyncQueueError {
    #[error(transparent)]
    PgError(#[from] diesel::result::Error),

    #[error("Task with name {0} is not registered")]
    TaskNotRegistered(String),

    #[error("Task with name {0} is not serializable to JSON")]
    JsonError(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] Box<dyn Error + Send + Sync>),
}
