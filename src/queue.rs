use crate::errors::AsyncQueueError;
use crate::runnable::RunnableTask;
use crate::task::{Task, TaskId, TaskType, TaskHash};
use async_trait::async_trait;
use diesel::result::Error::QueryBuilderError;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::AsyncConnection;
use diesel_async::{pg::AsyncPgConnection, pooled_connection::bb8::Pool};

/// This trait defines operations for an asynchronous queue.
/// The trait can be implemented for different storage backends.
/// For now, the trait is only implemented for PostgreSQL. More backends are planned to be implemented in the future.
#[async_trait]
pub trait Queueable: Send {
    /// Pull pending tasks from the queue to execute them.
    ///
    /// This method returns one task of the `task_type` type. If `task_type` is `None` it will try to
    /// fetch a task of the type `common`. The returned task is marked as running and must be executed.
    async fn pull_next_task(&mut self, kind: Option<TaskType>) -> Result<Option<Task>, AsyncQueueError>;

    /// Enqueue a task to the queue, The task will be executed as soon as possible by the worker of the same type
    /// created by an AsyncWorkerPool.
    async fn create_task(&mut self, task: &dyn RunnableTask) -> Result<Task, AsyncQueueError>;

    /// Retrieve a task by its `id`.
    async fn find_task_by_id(&mut self, id: TaskId) -> Result<Task, AsyncQueueError>;

    /// Update the state of a task to failed and set an error_message.
    async fn set_task_failed(&mut self, id: TaskId, error_message: &str) -> Result<Task, AsyncQueueError>;

    /// Update the state of a task to done.
    async fn set_task_done(&mut self, id: TaskId) -> Result<Task, AsyncQueueError>;

    /// Update the state of a task to inform that it's still in progress.
    async fn keep_task_alive(&mut self, id: TaskId) -> Result<(), AsyncQueueError>;

    /// Remove a task by its id.
    async fn remove_task(&mut self, id: TaskId) -> Result<u64, AsyncQueueError>;

    /// The method will remove all tasks from the queue
    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError>;

    /// Remove all tasks that are scheduled in the future.
    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError>;

    /// Remove a task by its metadata (struct fields values)
    async fn remove_task_by_hash(&mut self, task_hash: TaskHash) -> Result<bool, AsyncQueueError>;

    /// Removes all tasks that have the specified `task_type`.
    async fn remove_tasks_type(&mut self, task_type: TaskType) -> Result<u64, AsyncQueueError>;

    async fn schedule_task_retry(
        &mut self,
        id: TaskId,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError>;
}

/// An async queue that can be used to enqueue tasks.
/// It uses a PostgreSQL storage. It must be connected to perform any operation.
/// To connect an `AsyncQueue` to PostgreSQL database call the `connect` method.
/// A Queue can be created with the TypedBuilder.
///
///    ```rust
///         let mut queue = AsyncQueue::builder()
///             .uri("postgres://postgres:postgres@localhost/fang")
///             .max_pool_size(max_pool_size)
///             .build();
///     ```
///
#[derive(Debug, Clone)]
pub struct PgAsyncQueue {
    pool: Pool<AsyncPgConnection>,
}

impl PgAsyncQueue {
    pub fn new(pool: Pool<AsyncPgConnection>) -> Self {
        PgAsyncQueue { pool }
    }
}

#[async_trait]
impl Queueable for PgAsyncQueue {
    async fn pull_next_task(
        &mut self,
        task_type: Option<TaskType>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        connection
            .transaction::<Option<Task>, AsyncQueueError, _>(|conn| {
                async move {
                    let Some(pending_task) = Task::fetch_next_pending(conn, task_type.unwrap_or_default()).await else {
                        return Ok(None);
                    };

                    Task::set_running(conn, pending_task).await.map(|running_task| Some(running_task))
                }
                .scope_boxed()
            })
            .await
    }

    async fn create_task(&mut self, runnable: &dyn RunnableTask) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Ok(Task::insert(&mut connection, runnable).await?)
    }

    async fn find_task_by_id(&mut self, id: TaskId) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::find_by_id(&mut connection, id).await
    }

    async fn set_task_failed(
        &mut self,
        id: TaskId,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::fail_with_message(&mut connection, id, error_message).await
    }

    async fn set_task_done(&mut self, id: TaskId) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::set_done(&mut connection, id).await
    }

    async fn keep_task_alive(&mut self, id: TaskId) -> Result<(), AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        connection
            .transaction::<(), AsyncQueueError, _>(|conn| {
                async move {
                    let task = Task::find_by_id(conn, id).await?;
                    Task::set_running(conn, task).await?;
                    Ok(())
                }.scope_boxed()
            }).await
    }

    async fn remove_task(&mut self, id: TaskId) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let result = Task::remove(&mut connection, id).await?;
        Ok(result)
    }

    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::remove_all(&mut connection).await
    }

    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let result = Task::remove_all_scheduled(&mut connection).await?;
        Ok(result)
    }

    async fn remove_task_by_hash(&mut self, task_hash: TaskHash) -> Result<bool, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::remove_by_hash(&mut connection, task_hash).await
    }

    async fn remove_tasks_type(&mut self, task_type: TaskType) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let result = Task::remove_by_type(&mut connection, task_type).await?;
        Ok(result)
    }

    async fn schedule_task_retry(
        &mut self,
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
mod async_queue_tests {
    use super::*;
    use crate::Scheduled;
    use async_trait::async_trait;
    use chrono::DateTime;
    use chrono::Utc;
    use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
    use diesel_async::AsyncPgConnection;
    use serde::{Deserialize, Serialize};
    use crate::task::TaskState;

    #[derive(Serialize, Deserialize)]
    struct AsyncTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for AsyncTask {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncUniqTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for AsyncUniqTask {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            Ok(())
        }

        fn uniq(&self) -> Option<TaskHash> {
            TaskHash::default_for_task(self).ok()
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskSchedule {
        pub number: u16,
        pub datetime: String,
    }

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for AsyncTaskSchedule {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            Ok(())
        }

        fn cron(&self) -> Option<Scheduled> {
            let datetime = self.datetime.parse::<DateTime<Utc>>().ok()?;
            Some(Scheduled::ScheduleOnce(datetime))
        }
    }

    #[tokio::test]
    async fn insert_task_creates_new_task() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn update_task_state_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();
        let id = task.id;

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let finished_task = test.set_task_done(task.id).await.unwrap();

        assert_eq!(id, finished_task.id);
        assert_eq!(TaskState::Done, finished_task.state());

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn failed_task_query_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();
        let id = task.id;

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let failed_task = test.set_task_failed(task.id, "Some error").await.unwrap();

        assert_eq!(id, failed_task.id);
        assert_eq!(Some("Some error"), failed_task.error_message.as_deref());
        assert_eq!(TaskState::Failed, failed_task.state());

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn remove_all_tasks_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool.into());

        let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.create_task(&AsyncTask { number: 2 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let result = test.remove_all_tasks().await.unwrap();
        assert_eq!(2, result);
    }

    // #[tokio::test]
    // async fn schedule_task_test() {
    //     let pool = pool().await;
    //     let mut test = PgAsyncQueue::new(pool);
    //
    //     let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);
    //
    //     let task = &AsyncTaskSchedule {
    //         number: 1,
    //         datetime: datetime.to_string(),
    //     };
    //
    //     let task = test.schedule_task(task).await.unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //
    //     assert_eq!(Some(1), number);
    //     assert_eq!(Some("AsyncTaskSchedule"), type_task);
    //     assert_eq!(task.scheduled_at, datetime);
    //
    //     test.remove_all_tasks().await.unwrap();
    // }
    //
    // #[tokio::test]
    // async fn remove_all_scheduled_tasks_test() {
    //     let pool = pool().await;
    //     let mut test = PgAsyncQueue::new(pool);
    //
    //     let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);
    //
    //     let task1 = &AsyncTaskSchedule {
    //         number: 1,
    //         datetime: datetime.to_string(),
    //     };
    //
    //     let task2 = &AsyncTaskSchedule {
    //         number: 2,
    //         datetime: datetime.to_string(),
    //     };
    //
    //     test.schedule_task(task1).await.unwrap();
    //     test.schedule_task(task2).await.unwrap();
    //
    //     let number = test.remove_all_scheduled_tasks().await.unwrap();
    //
    //     assert_eq!(2, number);
    //
    //     test.remove_all_tasks().await.unwrap();
    // }

    #[tokio::test]
    async fn pull_next_task_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.create_task(&AsyncTask { number: 2 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.pull_next_task(None).await.unwrap().unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.pull_next_task(None).await.unwrap().unwrap();
        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn remove_tasks_type_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.create_task(&AsyncTask { number: 2 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let result = test.remove_tasks_type(TaskType::from("nonexistentType")).await.unwrap();
        assert_eq!(0, result);

        let result = test.remove_tasks_type(TaskType::default()).await.unwrap();
        assert_eq!(2, result);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn remove_tasks_by_metadata() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let task = test.create_task(&AsyncUniqTask { number: 1 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncUniqTask"), type_task);

        let task = test.create_task(&AsyncUniqTask { number: 2 }).await.unwrap();

        let metadata = task.payload.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncUniqTask"), type_task);

        let result = test.remove_task_by_hash(AsyncUniqTask { number: 0 }.uniq().unwrap())
            .await
            .unwrap();
        assert!(!result, "Should **not** remove task");

        let result = test
            .remove_task_by_hash(AsyncUniqTask { number: 1 }.uniq().unwrap())
            .await
            .unwrap();
        assert!(result, "Should remove task");

        test.remove_all_tasks().await.unwrap();
    }

    async fn pool() -> Pool<AsyncPgConnection> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            "postgres://postgres:password@localhost/backie",
        );
        Pool::builder()
            .max_size(1)
            .min_idle(Some(1))
            .build(manager)
            .await
            .unwrap()
    }
}
