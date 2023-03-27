//#![warn(missing_docs)]
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

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

pub use runnable::BackgroundTask;
pub use store::{PgTask, PgTaskStore, TaskStore};
pub use task::{CurrentTask, NewTask, Task, TaskId, TaskState};
pub use worker::Worker;
pub use worker_pool::{QueueConfig, WorkerPool};

mod catch_unwind;
pub mod errors;
mod queries;
mod runnable;
mod schema;
mod store;
mod task;
mod worker;
mod worker_pool;
