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
    async fn pull_next_task(
        &self,
        queue_name: &str,
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
                    let Some(pending_task) = Task::fetch_next_pending(conn, queue_name, task_names).await else {
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

#[cfg(test)]
pub mod test_store {
    use super::*;
    use itertools::Itertools;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Default, Clone)]
    pub struct MemoryTaskStore {
        pub tasks: Arc<Mutex<BTreeMap<TaskId, Task>>>,
    }

    #[async_trait::async_trait]
    impl TaskStore for MemoryTaskStore {
        async fn pull_next_task(
            &self,
            queue_name: &str,
            task_names: &[String],
        ) -> Result<Option<Task>, AsyncQueueError> {
            let mut tasks = self.tasks.lock().await;
            let mut next_task = None;
            for (_, task) in tasks
                .iter_mut()
                .filter(|(_, task)| task_names.contains(&task.task_name))
                .sorted_by(|a, b| a.1.created_at.cmp(&b.1.created_at))
            {
                if task.queue_name == queue_name && task.state() == TaskState::Ready {
                    task.running_at = Some(chrono::Utc::now());
                    next_task = Some(task.clone());
                    break;
                }
            }
            Ok(next_task)
        }

        async fn create_task(&self, new_task: NewTask) -> Result<Task, AsyncQueueError> {
            let mut tasks = self.tasks.lock().await;
            let task = Task::from(new_task);
            tasks.insert(task.id, task.clone());
            Ok(task)
        }

        async fn set_task_state(
            &self,
            id: TaskId,
            state: TaskState,
        ) -> Result<(), AsyncQueueError> {
            let mut tasks = self.tasks.lock().await;
            let task = tasks.get_mut(&id).unwrap();

            use TaskState::*;
            match state {
                Done => task.done_at = Some(chrono::Utc::now()),
                Failed(error_msg) => {
                    let error_payload = serde_json::json!({
                        "error": error_msg,
                    });
                    task.error_info = Some(error_payload);
                    task.done_at = Some(chrono::Utc::now());
                }
                _ => {}
            }

            Ok(())
        }

        async fn remove_task(&self, id: TaskId) -> Result<u64, AsyncQueueError> {
            let mut tasks = self.tasks.lock().await;
            let res = tasks.remove(&id);
            if res.is_some() {
                Ok(1)
            } else {
                Ok(0)
            }
        }

        async fn schedule_task_retry(
            &self,
            id: TaskId,
            backoff_seconds: u32,
            error: &str,
        ) -> Result<Task, AsyncQueueError> {
            let mut tasks = self.tasks.lock().await;
            let task = tasks.get_mut(&id).unwrap();

            let error_payload = serde_json::json!({
                "error": error,
            });
            task.error_info = Some(error_payload);
            task.running_at = None;
            task.retries += 1;
            task.scheduled_at =
                chrono::Utc::now() + chrono::Duration::seconds(backoff_seconds as i64);

            Ok(task.clone())
        }
    }
}

#[async_trait::async_trait]
pub trait TaskStore: Send + Sync + 'static {
    async fn pull_next_task(
        &self,
        queue_name: &str,
        task_names: &[String],
    ) -> Result<Option<Task>, AsyncQueueError>;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::test_store::MemoryTaskStore;

    #[test]
    fn task_store_trait_is_object_safe() {
        let store = MemoryTaskStore::default();
        let _object = &store as &dyn TaskStore;
    }
}
