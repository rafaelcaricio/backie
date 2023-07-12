use diesel::result::Error::QueryBuilderError;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::AsyncConnection;
use diesel_async::{pg::AsyncPgConnection, pooled_connection::bb8::Pool};
use std::time::Duration;

use crate::errors::AsyncQueueError;
use crate::{BackgroundTask, NewTask, Task, TaskId, TaskState, TaskStore};

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
    type Connection = AsyncPgConnection;

    async fn pull_next_task(
        &self,
        queue_name: &str,
        execution_timeout: Option<Duration>,
        task_names: &[String],
    ) -> Result<Option<Task>, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        connection
            .transaction::<Option<Task>, AsyncQueueError, _>(|conn| {
                async move {
                    let Some(pending_task) = Task::fetch_next_pending(conn, queue_name, execution_timeout, task_names).await else {
                        return Ok(None);
                    };

                    Task::set_running(conn, pending_task).await.map(Some)
                }
                .scope_boxed()
            })
            .await
    }

    async fn set_task_state(&self, id: TaskId, state: TaskState) -> Result<(), AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        match state {
            TaskState::Done => {
                Task::set_done(&mut connection, id).await?;
            }
            TaskState::Failed(error_msg) => {
                Task::fail_with_message(&mut connection, id, &error_msg).await?;
            }
            _ => (),
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

    async fn enqueue<T: BackgroundTask>(
        connection: &mut Self::Connection,
        task: T,
    ) -> Result<(), AsyncQueueError> {
        let new_task = NewTask::new(task)?;
        Task::insert(connection, new_task).await?;
        Ok(())
    }

    async fn schedule_task_retry(
        &self,
        id: TaskId,
        backoff: Duration,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let task = Task::schedule_retry(&mut connection, id, backoff, error).await?;
        Ok(task)
    }
}
