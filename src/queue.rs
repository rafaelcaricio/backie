use crate::errors::BackieError;
use crate::runnable::BackgroundTask;
use crate::store::{PgTaskStore, TaskStore};
use crate::task::{NewTask, TaskHash};
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

    pub async fn enqueue<BT>(&self, background_task: BT) -> Result<(), BackieError>
    where
        BT: BackgroundTask,
    {
        self.task_store
            .create_task(NewTask::new(background_task, Duration::from_secs(10))?)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod async_queue_tests {
    use super::*;
    use crate::CurrentTask;
    use async_trait::async_trait;
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
}
