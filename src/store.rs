use crate::errors::AsyncQueueError;
use crate::task::{Task, TaskId, TaskState};
use crate::BackgroundTask;
use std::time::Duration;

#[cfg(feature = "async_postgres")]
mod pg_task_store;

#[cfg(feature = "async_postgres")]
pub use self::pg_task_store::*;

/// A trait that is used to enqueue tasks for a given connection type
#[async_trait::async_trait]
pub trait BackgroundTaskExt {
    /// Enqueue a task for execution.
    ///
    /// This method accepts a connection thus enabling the user to use a transaction while
    /// scheduling tasks. This is useful if you want to schedule a task only if some other
    /// condition is met.
    async fn enqueue<S: TaskStore>(
        self,
        connection: &mut S::Connection,
    ) -> Result<(), AsyncQueueError>;
}

#[async_trait::async_trait]
impl<T> BackgroundTaskExt for T
where
    T: BackgroundTask,
{
    async fn enqueue<S: TaskStore>(
        self,
        connection: &mut S::Connection,
    ) -> Result<(), AsyncQueueError> {
        S::enqueue(connection, self).await
    }
}

#[cfg(test)]
pub mod test_store {
    use super::*;
    use crate::NewTask;
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
        type Connection = Self;

        async fn pull_next_task(
            &self,
            queue_name: &str,
            execution_timeout: Option<Duration>,
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
                } else if let Some(execution_timeout) = execution_timeout {
                    if let Some(running_at) = task.running_at {
                        let execution_timeout =
                            chrono::Duration::from_std(execution_timeout).unwrap();
                        if running_at + execution_timeout < chrono::Utc::now() {
                            task.running_at = Some(chrono::Utc::now());
                            next_task = Some(task.clone());
                            break;
                        }
                    }
                }
            }
            Ok(next_task)
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
            backoff: Duration,
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
            task.scheduled_at = chrono::Utc::now()
                + chrono::Duration::from_std(backoff).unwrap_or(chrono::Duration::max_value());

            Ok(task.clone())
        }

        async fn enqueue<T: BackgroundTask>(
            store: &mut Self::Connection,
            task: T,
        ) -> Result<(), AsyncQueueError> {
            let mut tasks = store.tasks.lock().await;
            let new_task = NewTask::new(task)?;
            let task = Task::from(new_task);
            tasks.insert(task.id, task);
            Ok(())
        }
    }
}

#[async_trait::async_trait]
pub trait TaskStore: Send + Sync + 'static {
    type Connection: Send;

    async fn pull_next_task(
        &self,
        queue_name: &str,
        execution_timeout: Option<Duration>,
        task_names: &[String],
    ) -> Result<Option<Task>, AsyncQueueError>;
    async fn set_task_state(&self, id: TaskId, state: TaskState) -> Result<(), AsyncQueueError>;
    async fn remove_task(&self, id: TaskId) -> Result<u64, AsyncQueueError>;
    async fn schedule_task_retry(
        &self,
        id: TaskId,
        backoff: Duration,
        error: &str,
    ) -> Result<Task, AsyncQueueError>;

    async fn enqueue<T: BackgroundTask>(
        conn: &mut Self::Connection,
        task: T,
    ) -> Result<(), AsyncQueueError>
    where
        Self: Sized;
}
