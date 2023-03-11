use crate::errors::AsyncQueueError;
use crate::task::{NewTask, Task, TaskId, TaskState};
use diesel::result::Error::QueryBuilderError;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::AsyncConnection;
use diesel_async::{pg::AsyncPgConnection, pooled_connection::bb8::Pool};

/// An async queue that is used to manipulate tasks, it uses PostgreSQL as storage.
#[derive(Debug, Clone)]
pub struct PgTaskStore {
    pool: Pool<AsyncPgConnection>,
}

impl PgTaskStore {
    pub fn new(pool: Pool<AsyncPgConnection>) -> Self {
        PgTaskStore { pool }
    }
}

#[async_trait::async_trait]
impl TaskStore for PgTaskStore {
    async fn pull_next_task(&self, queue_name: &str) -> Result<Option<Task>, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        connection
            .transaction::<Option<Task>, AsyncQueueError, _>(|conn| {
                async move {
                    let Some(pending_task) = Task::fetch_next_pending(conn, queue_name).await else {
                        return Ok(None);
                    };

                    Task::set_running(conn, pending_task).await.map(Some)
                }
                .scope_boxed()
            })
            .await
    }
    async fn create_task(&self, new_task: NewTask) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::insert(&mut connection, new_task).await
    }
    async fn set_task_state(&self, id: TaskId, state: TaskState) -> Result<(), AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        match state {
            TaskState::Done => Task::set_done(&mut connection, id).await?,
            TaskState::Failed(error_msg) => {
                Task::fail_with_message(&mut connection, id, &error_msg).await?
            }
            _ => return Ok(()),
        };
        Ok(())
    }
    async fn remove_task(&self, id: TaskId) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let result = Task::remove(&mut connection, id).await?;
        Ok(result)
    }
    async fn schedule_task_retry(
        &self,
        id: TaskId,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let task = Task::schedule_retry(&mut connection, id, backoff_seconds, error).await?;
        Ok(task)
    }
}

#[async_trait::async_trait]
pub trait TaskStore {
    async fn pull_next_task(&self, queue_name: &str) -> Result<Option<Task>, AsyncQueueError>;
    async fn create_task(&self, new_task: NewTask) -> Result<Task, AsyncQueueError>;
    async fn set_task_state(&self, id: TaskId, state: TaskState) -> Result<(), AsyncQueueError>;
    async fn remove_task(&self, id: TaskId) -> Result<u64, AsyncQueueError>;
    async fn schedule_task_retry(
        &self,
        id: TaskId,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError>;
}
