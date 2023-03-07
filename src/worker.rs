use std::error::Error;
use crate::errors::BackieError;
use crate::queue::Queueable;
use crate::runnable::RunnableTask;
use crate::task::{Task, TaskType};
use crate::Scheduled::*;
use crate::{RetentionMode};
use log::error;
use typed_builder::TypedBuilder;

/// it executes tasks only of task_type type, it sleeps when there are no tasks in the queue
#[derive(TypedBuilder)]
pub struct AsyncWorker<Q>
where
    Q: Queueable + Clone + Sync + 'static,
{
    #[builder(setter(into))]
    pub queue: Q,

    #[builder(default, setter(into))]
    pub task_type: Option<TaskType>,

    #[builder(default, setter(into))]
    pub retention_mode: RetentionMode,
}

// impl<TypedBuilderFields, Q> AsyncWorkerBuilder<TypedBuilderFields, Q>
//     where
//         TypedBuilderFields: Clone,
//         Q: Queueable + Clone + Sync + 'static,
// {
//     pub fn with_graceful_shutdown<F>(self, signal: F) -> Self<TypedBuilderFields, Q>
//         where
//             F: Future<Output = ()>,
//     {
//         self
//     }
// }

impl<Q> AsyncWorker<Q>
where
    Q: Queueable + Clone + Sync + 'static,
{
    async fn run(&mut self, task: Task, runnable: Box<dyn RunnableTask>) -> Result<(), BackieError> {
        // TODO: catch panics
        let result = runnable.run(&mut self.queue).await;
        match result {
            Ok(_) => self.finalize_task(task, result).await?,
            Err(error) => {
                if task.retries < runnable.max_retries() {
                    let backoff_seconds = runnable.backoff(task.retries as u32);

                    log::debug!(
                        "Task {} failed to run and will be retried in {} seconds",
                        task.id,
                        backoff_seconds
                    );
                    let error_message = format!("{}", error);
                    self.queue
                        .schedule_task_retry(task.id, backoff_seconds, &error_message)
                        .await?;
                } else {
                    log::debug!("Task {} failed and reached the maximum retries", task.id);
                    self.finalize_task(task, Err(error)).await?;
                }
            }
        }
        Ok(())
    }

    async fn finalize_task(
        &mut self,
        task: Task,
        result: Result<(), Box<dyn Error + Send + 'static>>,
    ) -> Result<(), BackieError> {
        match self.retention_mode {
            RetentionMode::KeepAll => match result {
                Ok(_) => {
                    self.queue.set_task_done(task.id).await?;
                    log::debug!("Task {} done and kept in the database", task.id);
                }
                Err(error) => {
                    log::debug!("Task {} failed and kept in the database", task.id);
                    self.queue.set_task_failed(task.id, &format!("{}", error)).await?;
                }
            },
            RetentionMode::RemoveAll => {
                log::debug!("Task {} finalized and deleted from the database", task.id);
                self.queue.remove_task(task.id).await?;
            }
            RetentionMode::RemoveDone => match result {
                Ok(_) => {
                    log::debug!("Task {} done and deleted from the database", task.id);
                    self.queue.remove_task(task.id).await?;
                }
                Err(error) => {
                    log::debug!("Task {} failed and kept in the database", task.id);
                    self.queue.set_task_failed(task.id, &format!("{}", error)).await?;
                }
            },
        };

        Ok(())
    }

    async fn wait(&mut self) {
        // TODO: add a way to stop the worker
        // Listen to postgres pubsub notification
        // Listen to watchable future
        // All that until a max timeout
        //
        // select! {
        //     _ = self.queue.wait_for_task(Some(self.task_type.clone())).fuse() => {},
        //     _ = SleepParams::default().sleep().fuse() => {},
        // }
    }

    pub(crate) async fn run_tasks(&mut self) {
        loop {
            // TODO: check if should stop the worker
            match self
                .queue
                .pull_next_task(self.task_type.clone())
                .await
            {
                Ok(Some(task)) => {
                    let actual_task: Box<dyn RunnableTask> = serde_json::from_value(task.payload.clone()).unwrap();

                    // check if task is scheduled or not
                    if let Some(CronPattern(_)) = actual_task.cron() {
                        // program task
                        //self.queue.schedule_task(&*actual_task).await?;
                    }
                    // run scheduled task
                    // TODO: what do we do if the task fails? it's an internal error, inform the logs
                    let _ = self.run(task, actual_task).await;
                }
                Ok(None) => {
                    self.wait().await;
                }

                Err(error) => {
                    error!("Failed to fetch a task {:?}", error);
                    self.wait().await;
                }
            };
        }
    }

    #[cfg(test)]
    pub async fn run_tasks_until_none(&mut self) -> Result<(), BackieError> {
        loop {
            match self
                .queue
                .pull_next_task(self.task_type.clone())
                .await
            {
                Ok(Some(task)) => {
                    let actual_task: Box<dyn RunnableTask> =
                        serde_json::from_value(task.payload.clone()).unwrap();

                    // check if task is scheduled or not
                    if let Some(CronPattern(_)) = actual_task.cron() {
                        // program task
                        // self.queue.schedule_task(&*actual_task).await?;
                    }
                    self.wait().await;
                    // run scheduled task
                    self.run(task, actual_task).await?;
                }
                Ok(None) => {
                    return Ok(());
                }
                Err(error) => {
                    error!("Failed to fetch a task {:?}", error);

                    self.wait().await;
                }
            };
        }
    }
}

#[cfg(test)]
mod async_worker_tests {
    use super::*;
    use crate::errors::BackieError;
    use crate::queue::Queueable;
    use crate::queue::PgAsyncQueue;
    use crate::worker::Task;
    use crate::RetentionMode;
    use crate::Scheduled;
    use async_trait::async_trait;
    use chrono::Duration;
    use chrono::Utc;
    use diesel_async::pooled_connection::{bb8::Pool, AsyncDieselConnectionManager};
    use diesel_async::AsyncPgConnection;
    use serde::{Deserialize, Serialize};
    use crate::task::TaskState;

    #[derive(Serialize, Deserialize)]
    struct WorkerAsyncTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for WorkerAsyncTask {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize)]
    struct WorkerAsyncTaskSchedule {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for WorkerAsyncTaskSchedule {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            Ok(())
        }
        fn cron(&self) -> Option<Scheduled> {
            Some(Scheduled::ScheduleOnce(Utc::now() + Duration::seconds(1)))
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncFailedTask {
        pub number: u16,
    }

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for AsyncFailedTask {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            let message = format!("number {} is wrong :(", self.number);

            Err(Box::new(BackieError {
                description: message,
            }))
        }

        fn max_retries(&self) -> i32 {
            0
        }
    }

    #[derive(Serialize, Deserialize, Clone)]
    struct AsyncRetryTask {}

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for AsyncRetryTask {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            let message = "Failed".to_string();

            Err(Box::new(BackieError {
                description: message,
            }))
        }

        fn max_retries(&self) -> i32 {
            2
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskType1 {}

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for AsyncTaskType1 {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            Ok(())
        }

        fn task_type(&self) -> TaskType {
            TaskType::from("type1")
        }
    }

    #[derive(Serialize, Deserialize)]
    struct AsyncTaskType2 {}

    #[typetag::serde]
    #[async_trait]
    impl RunnableTask for AsyncTaskType2 {
        async fn run(&self, _queueable: &mut dyn Queueable) -> Result<(), Box<(dyn std::error::Error + Send + 'static)>> {
            Ok(())
        }

        fn task_type(&self) -> TaskType {
            TaskType::from("type2")
        }
    }

    #[tokio::test]
    async fn execute_and_finishes_task() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let actual_task = WorkerAsyncTask { number: 1 };

        let task = insert_task(&mut test, &actual_task).await;
        let id = task.id;

        let mut worker = AsyncWorker::<PgAsyncQueue>::builder()
            .queue(test.clone())
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run(task, Box::new(actual_task)).await.unwrap();
        let task_finished = test.find_task_by_id(id).await.unwrap();
        assert_eq!(id, task_finished.id);
        assert_eq!(TaskState::Done, task_finished.state());

        test.remove_all_tasks().await.unwrap();
    }

    // #[tokio::test]
    // async fn schedule_task_test() {
    //     let pool = pool().await;
    //     let mut test = PgAsyncQueue::new(pool);
    // 
    //     let actual_task = WorkerAsyncTaskSchedule { number: 1 };
    // 
    //     let task = test.schedule_task(&actual_task).await.unwrap();
    // 
    //     let id = task.id;
    // 
    //     let mut worker = AsyncWorker::<PgAsyncQueue>::builder()
    //         .queue(test.clone())
    //         .retention_mode(RetentionMode::KeepAll)
    //         .build();
    // 
    //     worker.run_tasks_until_none().await.unwrap();
    // 
    //     let task = worker.queue.find_task_by_id(id).await.unwrap();
    // 
    //     assert_eq!(id, task.id);
    //     assert_eq!(TaskState::Ready, task.state());
    // 
    //     tokio::time::sleep(core::time::Duration::from_secs(3)).await;
    // 
    //     worker.run_tasks_until_none().await.unwrap();
    // 
    //     let task = test.find_task_by_id(id).await.unwrap();
    //     assert_eq!(id, task.id);
    //     assert_eq!(TaskState::Done, task.state());
    // 
    //     test.remove_all_tasks().await.unwrap();
    // }

    #[tokio::test]
    async fn retries_task_test() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let actual_task = AsyncRetryTask {};

        let task = test.create_task(&actual_task).await.unwrap();

        let id = task.id;

        let mut worker = AsyncWorker::<PgAsyncQueue>::builder()
            .queue(test.clone())
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run_tasks_until_none().await.unwrap();

        let task = worker.queue.find_task_by_id(id).await.unwrap();

        assert_eq!(id, task.id);
        assert_eq!(TaskState::Ready, task.state());
        assert_eq!(1, task.retries);
        assert!(task.error_message.is_some());

        tokio::time::sleep(core::time::Duration::from_secs(5)).await;
        worker.run_tasks_until_none().await.unwrap();

        let task = worker.queue.find_task_by_id(id).await.unwrap();

        assert_eq!(id, task.id);
        assert_eq!(TaskState::Ready, task.state());
        assert_eq!(2, task.retries);

        tokio::time::sleep(core::time::Duration::from_secs(10)).await;
        worker.run_tasks_until_none().await.unwrap();

        let task = test.find_task_by_id(id).await.unwrap();
        assert_eq!(id, task.id);
        assert_eq!(TaskState::Failed, task.state());
        assert_eq!("Failed".to_string(), task.error_message.unwrap());

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn saves_error_for_failed_task() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let failed_task = AsyncFailedTask { number: 1 };

        let task = insert_task(&mut test, &failed_task).await;
        let id = task.id;

        let mut worker = AsyncWorker::<PgAsyncQueue>::builder()
            .queue(test.clone())
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run(task, Box::new(failed_task)).await.unwrap();
        let task_finished = test.find_task_by_id(id).await.unwrap();

        assert_eq!(id, task_finished.id);
        assert_eq!(TaskState::Failed, task_finished.state());
        assert_eq!(
            "number 1 is wrong :(".to_string(),
            task_finished.error_message.unwrap()
        );

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn executes_task_only_of_specific_type() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let task1 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task12 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task2 = insert_task(&mut test, &AsyncTaskType2 {}).await;

        let id1 = task1.id;
        let id12 = task12.id;
        let id2 = task2.id;

        let mut worker = AsyncWorker::<PgAsyncQueue>::builder()
            .queue(test.clone())
            .task_type(TaskType::from("type1"))
            .retention_mode(RetentionMode::KeepAll)
            .build();

        worker.run_tasks_until_none().await.unwrap();
        let task1 = test.find_task_by_id(id1).await.unwrap();
        let task12 = test.find_task_by_id(id12).await.unwrap();
        let task2 = test.find_task_by_id(id2).await.unwrap();

        assert_eq!(id1, task1.id);
        assert_eq!(id12, task12.id);
        assert_eq!(id2, task2.id);
        assert_eq!(TaskState::Done, task1.state());
        assert_eq!(TaskState::Done, task12.state());
        assert_eq!(TaskState::Ready, task2.state());

        test.remove_all_tasks().await.unwrap();
    }

    #[tokio::test]
    async fn remove_when_finished() {
        let pool = pool().await;
        let mut test = PgAsyncQueue::new(pool);

        let task1 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task12 = insert_task(&mut test, &AsyncTaskType1 {}).await;
        let task2 = insert_task(&mut test, &AsyncTaskType2 {}).await;

        let _id1 = task1.id;
        let _id12 = task12.id;
        let id2 = task2.id;

        let mut worker = AsyncWorker::<PgAsyncQueue>::builder()
            .queue(test.clone())
            .task_type(TaskType::from("type1"))
            .build();

        worker.run_tasks_until_none().await.unwrap();
        let task = test
            .pull_next_task(Some(TaskType::from("type1")))
            .await
            .unwrap();
        assert_eq!(None, task);

        let task2 = test
            .pull_next_task(Some(TaskType::from("type2")))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(id2, task2.id);

        test.remove_all_tasks().await.unwrap();
    }

    async fn insert_task(test: &mut PgAsyncQueue, task: &dyn RunnableTask) -> Task {
        test.create_task(task).await.unwrap()
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
