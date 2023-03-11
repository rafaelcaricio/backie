use crate::schema::backie_tasks;
use crate::BackgroundTask;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use diesel_derive_newtype::DieselNewType;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::fmt;
use std::fmt::Display;
use std::time::Duration;
use uuid::Uuid;

/// States of a task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskState {
    /// The task is ready to be executed.
    Ready,

    /// The task is running.
    Running,

    /// The task has failed to execute.
    Failed(String),

    /// The task finished successfully.
    Done,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, DieselNewType, Serialize)]
pub struct TaskId(Uuid);

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, DieselNewType, Serialize)]
pub struct TaskHash(Cow<'static, str>);

impl TaskHash {
    pub fn default_for_task<T>(value: &T) -> Result<Self, serde_json::Error>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value)?;
        let mut hasher = Sha256::new();
        hasher.update(serde_json::to_string(&value)?.as_bytes());
        let result = hasher.finalize();
        Ok(TaskHash(Cow::from(hex::encode(result))))
    }
}

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone)]
#[diesel(table_name = backie_tasks)]
pub struct Task {
    /// Unique identifier of the task.
    pub id: TaskId,

    /// Name of the type of task.
    pub task_name: String,

    /// Queue name that the task belongs to.
    pub queue_name: String,

    /// Unique hash is used to identify and avoid duplicate tasks.
    pub uniq_hash: Option<TaskHash>,

    /// Representation of the task.
    pub payload: serde_json::Value,

    /// Max timeout that the task can run for.
    pub timeout_msecs: i64,

    /// Creation time of the task.
    pub created_at: DateTime<Utc>,

    /// Date time when the task is scheduled to run.
    pub scheduled_at: DateTime<Utc>,

    /// Date time when the task is started to run.
    pub running_at: Option<DateTime<Utc>>,

    /// Date time when the task is finished.
    pub done_at: Option<DateTime<Utc>>,

    /// Failure reason, when the task is failed.
    pub error_info: Option<serde_json::Value>,

    /// Number of times a task was retried.
    pub retries: i32,

    /// Maximum number of retries allow for this task before it is maked as failure.
    pub max_retries: i32,
}

impl Task {
    pub fn state(&self) -> TaskState {
        if self.done_at.is_some() {
            if self.error_info.is_some() {
                // TODO: use a proper error type
                TaskState::Failed(self.error_info.clone().unwrap().to_string())
            } else {
                TaskState::Done
            }
        } else if self.running_at.is_some() {
            TaskState::Running
        } else {
            TaskState::Ready
        }
    }
}

#[derive(Insertable, Debug, Eq, PartialEq, Clone)]
#[diesel(table_name = backie_tasks)]
pub struct NewTask {
    task_name: String,
    queue_name: String,
    uniq_hash: Option<TaskHash>,
    payload: serde_json::Value,
    timeout_msecs: i64,
    max_retries: i32,
}

impl NewTask {
    pub(crate) fn new<T>(background_task: T, timeout: Duration) -> Result<Self, serde_json::Error>
    where
        T: BackgroundTask,
    {
        let max_retries = background_task.max_retries();
        let uniq_hash = background_task.uniq();
        let payload = serde_json::to_value(background_task)?;

        Ok(Self {
            task_name: T::TASK_NAME.to_string(),
            queue_name: T::QUEUE.to_string(),
            uniq_hash,
            payload,
            timeout_msecs: timeout.as_millis() as i64,
            max_retries,
        })
    }
}

pub struct CurrentTask {
    id: TaskId,
    retries: i32,
    created_at: DateTime<Utc>,
}

impl CurrentTask {
    pub(crate) fn new(task: &Task) -> Self {
        Self {
            id: task.id.clone(),
            retries: task.retries,
            created_at: task.created_at,
        }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn retry_count(&self) -> i32 {
        self.retries
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
}
