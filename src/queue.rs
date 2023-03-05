use crate::errors::AsyncQueueError;
use crate::errors::CronError;
use crate::fang_task_state::FangTaskState;
use crate::runnable::AsyncRunnable;
use crate::task::Task;
use crate::Scheduled::*;
use async_trait::async_trait;
use chrono::Utc;
use cron::Schedule;
use diesel::result::Error::QueryBuilderError;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::AsyncConnection;
use diesel_async::{pg::AsyncPgConnection, pooled_connection::bb8::Pool};
use std::str::FromStr;
use typed_builder::TypedBuilder;
use uuid::Uuid;

/// This trait defines operations for an asynchronous queue.
/// The trait can be implemented for different storage backends.
/// For now, the trait is only implemented for PostgreSQL. More backends are planned to be implemented in the future.
#[async_trait]
pub trait AsyncQueueable: Send {
    /// This method should retrieve one task of the `task_type` type. If `task_type` is `None` it will try to
    /// fetch a task of the type `common`. After fetching it should update the state of the task to
    /// `FangTaskState::InProgress`.
    ///
    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError>;

    /// Enqueue a task to the queue, The task will be executed as soon as possible by the worker of the same type
    /// created by an AsyncWorkerPool.
    async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError>;

    /// The method will remove all tasks from the queue
    async fn remove_all_tasks(&mut self) -> Result<u64, AsyncQueueError>;

    /// Remove all tasks that are scheduled in the future.
    async fn remove_all_scheduled_tasks(&mut self) -> Result<u64, AsyncQueueError>;

    /// Remove a task by its id.
    async fn remove_task(&mut self, id: Uuid) -> Result<u64, AsyncQueueError>;

    /// Remove a task by its metadata (struct fields values)
    async fn remove_task_by_metadata(
        &mut self,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError>;

    /// Removes all tasks that have the specified `task_type`.
    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError>;

    /// Retrieve a task from storage by its `id`.
    async fn find_task_by_id(&mut self, id: Uuid) -> Result<Task, AsyncQueueError>;

    /// Update the state field of the specified task
    /// See the `FangTaskState` enum for possible states.
    async fn update_task_state(
        &mut self,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError>;

    /// Update the state of a task to `FangTaskState::Failed` and set an error_message.
    async fn fail_task(&mut self, task: Task, error_message: &str)
        -> Result<Task, AsyncQueueError>;

    /// Schedule a task.
    async fn schedule_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError>;

    async fn schedule_retry(
        &mut self,
        task: &Task,
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
#[derive(TypedBuilder, Debug, Clone)]
pub struct PgAsyncQueue {
    pool: Pool<AsyncPgConnection>,
}

#[async_trait]
impl AsyncQueueable for PgAsyncQueue {
    async fn find_task_by_id(&mut self, id: Uuid) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::find_by_id(&mut connection, id).await
    }

    async fn fetch_and_touch_task(
        &mut self,
        task_type: Option<String>,
    ) -> Result<Option<Task>, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        connection
            .transaction::<Option<Task>, AsyncQueueError, _>(|conn| {
                async move {
                    let Some(found_task) = Task::fetch_by_type(conn, task_type).await else {
                        return Ok(None);
                    };

                    match Task::update_state(conn, found_task, FangTaskState::InProgress).await
                    {
                        Ok(updated_task) => Ok(Some(updated_task)),
                        Err(err) => Err(err),
                    }
                }
                .scope_boxed()
            })
            .await
    }

    async fn insert_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Ok(Task::insert(&mut connection, task, Utc::now()).await?)
    }

    async fn schedule_task(&mut self, task: &dyn AsyncRunnable) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let scheduled_at = match task.cron() {
            Some(scheduled) => match scheduled {
                CronPattern(cron_pattern) => {
                    let schedule = Schedule::from_str(&cron_pattern)?;
                    let mut iterator = schedule.upcoming(Utc);
                    iterator
                        .next()
                        .ok_or(AsyncQueueError::CronError(CronError::NoTimestampsError))?
                }
                ScheduleOnce(datetime) => datetime,
            },
            None => {
                return Err(AsyncQueueError::CronError(
                    CronError::TaskNotSchedulableError,
                ));
            }
        };

        Ok(Task::insert(&mut connection, task, scheduled_at).await?)
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

    async fn remove_task(&mut self, id: Uuid) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let result = Task::remove(&mut connection, id).await?;
        Ok(result)
    }

    async fn remove_task_by_metadata(
        &mut self,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError> {
        if task.uniq() {
            let mut connection = self
                .pool
                .get()
                .await
                .map_err(|e| QueryBuilderError(e.into()))?;
            let result = Task::remove_by_metadata(&mut connection, task).await?;
            Ok(result)
        } else {
            Err(AsyncQueueError::TaskNotUniqError)
        }
    }

    async fn remove_tasks_type(&mut self, task_type: &str) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let result = Task::remove_by_type(&mut connection, task_type).await?;
        Ok(result)
    }

    async fn update_task_state(
        &mut self,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let task = Task::update_state(&mut connection, task, state).await?;
        Ok(task)
    }

    async fn fail_task(
        &mut self,
        task: Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let task = Task::fail_with_message(&mut connection, task, error_message).await?;
        Ok(task)
    }

    async fn schedule_retry(
        &mut self,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let task = Task::schedule_retry(&mut connection, task, backoff_seconds, error).await?;
        Ok(task)
    }
}

#[cfg(test)]
mod async_queue_tests {
    use super::*;
    use crate::errors::FrangoError;
    use crate::Scheduled;
    use async_trait::async_trait;
    use chrono::prelude::*;
    use chrono::DateTime;
    use chrono::Duration;
    use chrono::Utc;
    use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
    use diesel_async::AsyncPgConnection;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct AsyncTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FrangoError> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncUniqTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncUniqTask {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FrangoError> {
            Ok(())
        }

        fn uniq(&self) -> bool {
            true
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskSchedule {
        pub number: u16,
        pub datetime: String,
    }

    #[typetag::serde]
    #[async_trait]
    impl AsyncRunnable for AsyncTaskSchedule {
        async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), FrangoError> {
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
        let mut test = PgAsyncQueue::builder().pool(pool).build();

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn update_task_state_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::builder().pool(pool).build();

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();
        let id = task.id;

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let finished_task = test
            .update_task_state(task, FangTaskState::Finished)
            .await
            .unwrap();

        assert_eq!(id, finished_task.id);
        assert_eq!(FangTaskState::Finished, finished_task.state);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn failed_task_query_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::builder().pool(pool).build();

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();
        let id = task.id;

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let failed_task = test.fail_task(task, "Some error").await.unwrap();

        assert_eq!(id, failed_task.id);
        assert_eq!(Some("Some error"), failed_task.error_message.as_deref());
        assert_eq!(FangTaskState::Failed, failed_task.state);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn remove_all_tasks_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::builder().pool(pool.into()).build();

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = insert_task(&mut test, &AsyncTask { number: 2 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let result = test.remove_all_tasks().await.unwrap();
        assert_eq!(2, result);
    }

    #[tokio::test]
    async fn schedule_task_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::builder().pool(pool).build();

        let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);

        let task = &AsyncTaskSchedule {
            number: 1,
            datetime: datetime.to_string(),
        };

        let task = test.schedule_task(task).await.unwrap();

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTaskSchedule"), type_task);
        assert_eq!(task.scheduled_at, datetime);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn remove_all_scheduled_tasks_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::builder().pool(pool).build();

        let datetime = (Utc::now() + Duration::seconds(7)).round_subsecs(0);

        let task1 = &AsyncTaskSchedule {
            number: 1,
            datetime: datetime.to_string(),
        };

        let task2 = &AsyncTaskSchedule {
            number: 2,
            datetime: datetime.to_string(),
        };

        test.schedule_task(task1).await.unwrap();
        test.schedule_task(task2).await.unwrap();

        let number = test.remove_all_scheduled_tasks().await.unwrap();

        assert_eq!(2, number);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn fetch_and_touch_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::builder().pool(pool).build();

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = insert_task(&mut test, &AsyncTask { number: 2 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.fetch_and_touch_task(None).await.unwrap().unwrap();

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = test.fetch_and_touch_task(None).await.unwrap().unwrap();
        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn remove_tasks_type_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::builder().pool(pool).build();

        let task = insert_task(&mut test, &AsyncTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let task = insert_task(&mut test, &AsyncTask { number: 2 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncTask"), type_task);

        let result = test.remove_tasks_type("mytype").await.unwrap();
        assert_eq!(0, result);

        let result = test.remove_tasks_type("common").await.unwrap();
        assert_eq!(2, result);

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn remove_tasks_by_metadata() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::builder().pool(pool).build();

        let task = insert_task(&mut test, &AsyncUniqTask { number: 1 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(1), number);
        assert_eq!(Some("AsyncUniqTask"), type_task);

        let task = insert_task(&mut test, &AsyncUniqTask { number: 2 }).await;

        let metadata = task.metadata.as_object().unwrap();
        let number = metadata["number"].as_u64();
        let type_task = metadata["type"].as_str();

        assert_eq!(Some(2), number);
        assert_eq!(Some("AsyncUniqTask"), type_task);

        let result = test
            .remove_task_by_metadata(&AsyncUniqTask { number: 0 })
            .await
            .unwrap();
        assert_eq!(0, result);

        let result = test
            .remove_task_by_metadata(&AsyncUniqTask { number: 1 })
            .await
            .unwrap();
        assert_eq!(1, result);

        test.remove_all_tasks().await.unwrap();
    }

    async fn insert_task(test: &mut PgAsyncQueue, task: &dyn AsyncRunnable) -> Task {
        test.insert_task(task).await.unwrap()
    }

    async fn pool() -> Pool<AsyncPgConnection> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            "postgres://postgres:password@localhost/fang",
        );
        Pool::builder()
            .max_size(1)
            .min_idle(Some(1))
            .build(manager)
            .await
            .unwrap()
    }
}
