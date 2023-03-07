use std::borrow::Cow;
use std::fmt;
use std::fmt::Display;
use crate::schema::backie_tasks;
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use typed_builder::TypedBuilder;
use uuid::Uuid;
use serde::Serialize;
use diesel_derive_newtype::DieselNewType;
use sha2::{Digest, Sha256};

/// States of a task.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TaskState {
    /// The task is ready to be executed.
    Ready,

    /// The task is running.
    Running,

    /// The task has failed to execute.
    Failed,

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
pub struct TaskType(Cow<'static, str>);

impl Default for TaskType {
    fn default() -> Self {
        Self(Cow::from("default"))
    }
}

impl<S> From<S> for TaskType
where
    S: AsRef<str> + 'static,
{
    fn from(s: S) -> Self {
        TaskType(Cow::from(s.as_ref().to_owned()))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, DieselNewType, Serialize)]
pub struct TaskHash(Cow<'static, str>);

impl TaskHash {
    pub fn default_for_task<T>(value: &T) -> Result<Self, serde_json::Error> where T: Serialize {
        let value = serde_json::to_value(value)?;
        let mut hasher = Sha256::new();
        hasher.update(serde_json::to_string(&value)?.as_bytes());
        let result = hasher.finalize();
        Ok(TaskHash(Cow::from(hex::encode(result))))
    }
}

#[derive(Queryable, Identifiable, Debug, Eq, PartialEq, Clone, TypedBuilder)]
#[diesel(table_name = backie_tasks)]
pub struct Task {
    #[builder(setter(into))]
    pub id: TaskId,

    #[builder(setter(into))]
    pub payload: serde_json::Value,

    #[builder(setter(into))]
    pub error_message: Option<String>,

    #[builder(setter(into))]
    pub task_type: TaskType,

    #[builder(setter(into))]
    pub uniq_hash: Option<TaskHash>,

    #[builder(setter(into))]
    pub retries: i32,

    #[builder(setter(into))]
    pub created_at: DateTime<Utc>,

    #[builder(setter(into))]
    pub running_at: Option<DateTime<Utc>>,

    #[builder(setter(into))]
    pub done_at: Option<DateTime<Utc>>,
}

impl Task {
    pub fn state(&self) -> TaskState {
        if self.done_at.is_some() {
            if self.error_message.is_some() {
                TaskState::Failed
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

#[derive(Insertable, Debug, Eq, PartialEq, Clone, TypedBuilder)]
#[diesel(table_name = backie_tasks)]
pub struct NewTask {
    #[builder(setter(into))]
    payload: serde_json::Value,

    #[builder(setter(into))]
    task_type: TaskType,

    #[builder(setter(into))]
    uniq_hash: Option<TaskHash>,
}

pub struct TaskInfo {
    id: TaskId,
    error_message: Option<String>,
    retries: i32,
    created_at: DateTime<Utc>,
}
