#![doc = include_str!("../README.md")]

use chrono::{DateTime, Utc};

/// Represents a schedule for scheduled tasks.
///
/// It's used in the [`BackgroundTask::cron`]
#[derive(Debug, Clone)]
pub enum Scheduled {
    /// A cron pattern for a periodic task
    ///
    /// For example, `Scheduled::CronPattern("0/20 * * * * * *")`
    CronPattern(String),
    /// A datetime for a scheduled task that will be executed once
    ///
    /// For example, `Scheduled::ScheduleOnce(chrono::Utc::now() + std::time::Duration::seconds(7i64))`
    ScheduleOnce(DateTime<Utc>),
}

/// All possible options for retaining tasks in the db after their execution.
///
/// The default mode is [`RetentionMode::RemoveAll`]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
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
        Self::RemoveAll
    }
}

pub use runnable::BackgroundTask;
pub use store::{PgTaskStore, TaskStore};
pub use task::CurrentTask;
pub use worker_pool::WorkerPool;

pub mod errors;
mod queries;
pub mod queue;
pub mod runnable;
mod schema;
pub mod store;
pub mod task;
pub mod worker;
pub mod worker_pool;
