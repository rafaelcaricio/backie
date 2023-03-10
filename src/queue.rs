use crate::errors::AsyncQueueError;
use crate::runnable::BackgroundTask;
use crate::task::{NewTask, Task, TaskHash, TaskId, TaskState};
use diesel::result::Error::QueryBuilderError;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_async::AsyncConnection;
use diesel_async::{pg::AsyncPgConnection, pooled_connection::bb8::Pool};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct Queue {
    task_store: Arc<PgTaskStore>,
}

impl Queue {
    pub(crate) fn new(task_store: Arc<PgTaskStore>) -> Self {
        Queue { task_store }
    }

    pub async fn enqueue<BT>(&self, background_task: BT) -> Result<(), AsyncQueueError>
    where
        BT: BackgroundTask,
    {
        self.task_store
            .create_task(NewTask::new(background_task, Duration::from_secs(10))?)
            .await?;
        Ok(())
    }
}

/// An async queue that is used to manipulate tasks, it uses PostgreSQL as storage.
#[derive(Debug, Clone)]
pub struct PgTaskStore {
    pool: Pool<AsyncPgConnection>,
}

impl PgTaskStore {
    pub fn new(pool: Pool<AsyncPgConnection>) -> Self {
        PgTaskStore { pool }
    }

    pub(crate) async fn pull_next_task(
        &self,
        queue_name: &str,
    ) -> Result<Option<Task>, AsyncQueueError> {
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

    pub(crate) async fn create_task(&self, new_task: NewTask) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::insert(&mut connection, new_task).await
    }

    pub(crate) async fn find_task_by_id(&self, id: TaskId) -> Result<Task, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::find_by_id(&mut connection, id).await
    }

    pub(crate) async fn set_task_state(
        &self,
        id: TaskId,
        state: TaskState,
    ) -> Result<(), AsyncQueueError> {
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

    pub(crate) async fn keep_task_alive(&self, id: TaskId) -> Result<(), AsyncQueueError> {
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
                }
                .scope_boxed()
            })
            .await
    }

    pub(crate) async fn remove_task(&self, id: TaskId) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        let result = Task::remove(&mut connection, id).await?;
        Ok(result)
    }

    pub(crate) async fn remove_all_tasks(&self) -> Result<u64, AsyncQueueError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|e| QueryBuilderError(e.into()))?;
        Task::remove_all(&mut connection).await
    }

    pub(crate) async fn schedule_task_retry(
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
mod async_queue_tests {
    use super::*;
    use crate::CurrentTask;
    use async_trait::async_trait;
    use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
    use diesel_async::AsyncPgConnection;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct AsyncTask {
        pub number: u16,
    }

    #[async_trait]
    impl BackgroundTask for AsyncTask {
        const TASK_NAME: &'static str = "AsyncUniqTask";
        type AppData = ();

        async fn run(&self, _task: CurrentTask, _: Self::AppData) -> Result<(), anyhow::Error> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncUniqTask {
        pub number: u16,
    }

    #[async_trait]
    impl BackgroundTask for AsyncUniqTask {
        const TASK_NAME: &'static str = "AsyncUniqTask";
        type AppData = ();

        async fn run(&self, _task: CurrentTask, _: Self::AppData) -> Result<(), anyhow::Error> {
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

    #[async_trait]
    impl BackgroundTask for AsyncTaskSchedule {
        const TASK_NAME: &'static str = "AsyncUniqTask";
        type AppData = ();

        async fn run(&self, _task: CurrentTask, _: Self::AppData) -> Result<(), anyhow::Error> {
            Ok(())
        }

        // fn cron(&self) -> Option<Scheduled> {
        //     let datetime = self.datetime.parse::<DateTime<Utc>>().ok()?;
        //     Some(Scheduled::ScheduleOnce(datetime))
        // }
    }

    // #[tokio::test]
    // async fn insert_task_creates_new_task() {
    //     let pool = pool().await;
    //     let mut queue = PgTaskStore::new(pool);
    //
    //     let task = queue.create_task(AsyncTask { number: 1 }).await.unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //
    //     assert_eq!(Some(1), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     queue.remove_all_tasks().await.unwrap();
    // }
    //
    // #[tokio::test]
    // async fn update_task_state_test() {
    //     let pool = pool().await;
    //     let mut test = PgTaskStore::new(pool);
    //
    //     let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //     let id = task.id;
    //
    //     assert_eq!(Some(1), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     let finished_task = test.set_task_state(task.id, TaskState::Done).await.unwrap();
    //
    //     assert_eq!(id, finished_task.id);
    //     assert_eq!(TaskState::Done, finished_task.state());
    //
    //     test.remove_all_tasks().await.unwrap();
    // }
    //
    // #[tokio::test]
    // async fn failed_task_query_test() {
    //     let pool = pool().await;
    //     let mut test = PgTaskStore::new(pool);
    //
    //     let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //     let id = task.id;
    //
    //     assert_eq!(Some(1), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     let failed_task = test.set_task_state(task.id, TaskState::Failed("Some error".to_string())).await.unwrap();
    //
    //     assert_eq!(id, failed_task.id);
    //     assert_eq!(Some("Some error"), failed_task.error_message.as_deref());
    //     assert_eq!(TaskState::Failed, failed_task.state());
    //
    //     test.remove_all_tasks().await.unwrap();
    // }
    //
    // #[tokio::test]
    // async fn remove_all_tasks_test() {
    //     let pool = pool().await;
    //     let mut test = PgTaskStore::new(pool);
    //
    //     let task = test.create_task(&AsyncTask { number: 1 }).await.unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //
    //     assert_eq!(Some(1), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     let task = test.create_task(&AsyncTask { number: 2 }).await.unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //
    //     assert_eq!(Some(2), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     let result = test.remove_all_tasks().await.unwrap();
    //     assert_eq!(2, result);
    // }
    //
    // #[tokio::test]
    // async fn pull_next_task_test() {
    //     let pool = pool().await;
    //     let mut queue = PgTaskStore::new(pool);
    //
    //     let task = queue.create_task(&AsyncTask { number: 1 }).await.unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //
    //     assert_eq!(Some(1), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     let task = queue.create_task(&AsyncTask { number: 2 }).await.unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //
    //     assert_eq!(Some(2), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     let task = queue.pull_next_task(None).await.unwrap().unwrap();
    //
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //
    //     assert_eq!(Some(1), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     let task = queue.pull_next_task(None).await.unwrap().unwrap();
    //     let metadata = task.payload.as_object().unwrap();
    //     let number = metadata["number"].as_u64();
    //     let type_task = metadata["type"].as_str();
    //
    //     assert_eq!(Some(2), number);
    //     assert_eq!(Some("AsyncTask"), type_task);
    //
    //     queue.remove_all_tasks().await.unwrap();
    // }

    async fn pool() -> Pool<AsyncPgConnection> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(
            option_env!("DATABASE_URL").expect("DATABASE_URL must be set"),
        );
        Pool::builder()
            .max_size(1)
            .min_idle(Some(1))
            .build(manager)
            .await
            .unwrap()
    }
}
