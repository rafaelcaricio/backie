//#![warn(missing_docs)]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]
use std::time::Duration;

/// All possible options for retaining tasks in the db after their execution.
///
/// The default mode is [`RetentionMode::RemoveAll`]
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash)]
pub enum RetentionMode {
    /// Keep all tasks
    KeepAll,

    /// Remove all finished tasks independently of their final execution state.
    RemoveAll,

    /// Remove only successfully finished tasks
    RemoveDone,
}

impl Default for RetentionMode {
    fn default() -> Self {
        Self::RemoveDone
    }
}

/// All possible options for backoff between task retries.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Hash, serde::Serialize, serde::Deserialize)]
pub enum BackoffMode {
    /// No backoff, retry immediately
    NoBackoff,

    /// Exponential backoff
    ExponentialBackoff,
}

impl Default for BackoffMode {
    fn default() -> Self {
        Self::ExponentialBackoff
    }
}

impl BackoffMode {
    fn next_attempt(&self, attempt: i32) -> Duration {
        match self {
            Self::NoBackoff => Duration::from_secs(0),
            Self::ExponentialBackoff => {
                Duration::from_secs(2u64.saturating_pow(attempt.saturating_add(1) as u32))
            }
        }
    }
}

pub use runnable::BackgroundTask;
pub use store::{BackgroundTaskExt, TaskStore};
pub use task::{CurrentTask, NewTask, Task, TaskHash, TaskId, TaskState};
pub use worker::Worker;
pub use worker_pool::{QueueConfig, WorkerPool};

#[cfg(feature = "async_postgres")]
pub use store::PgTaskStore;

mod catch_unwind;
pub mod errors;
#[cfg(feature = "async_postgres")]
mod queries;
mod runnable;
#[cfg(feature = "async_postgres")]
mod schema;
mod store;
mod task;
mod worker;
mod worker_pool;
