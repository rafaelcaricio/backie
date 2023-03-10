use async_trait::async_trait;
use backie::{BackgroundTask, CurrentTask};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MyApplicationContext {
    app_name: String,
}

impl MyApplicationContext {
    pub fn new(app_name: &str) -> Self {
        Self {
            app_name: app_name.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MyTask {
    pub number: u16,
}

impl MyTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[derive(Serialize, Deserialize)]
pub struct MyFailingTask {
    pub number: u16,
}

impl MyFailingTask {
    pub fn new(number: u16) -> Self {
        Self { number }
    }
}

#[async_trait]
impl BackgroundTask for MyTask {
    const TASK_NAME: &'static str = "my_task";
    type AppData = MyApplicationContext;

    async fn run(&self, task: CurrentTask, ctx: Self::AppData) -> Result<(), anyhow::Error> {
        // let new_task = MyTask::new(self.number + 1);
        // queue
        //     .insert_task(&new_task)
        //     .await
        //     .unwrap();

        log::info!(
            "[{}] Hello from {}! the current number is {}",
            task.id(),
            ctx.app_name,
            self.number
        );
        tokio::time::sleep(Duration::from_secs(3)).await;

        log::info!("[{}] done..", task.id());
        Ok(())
    }
}

#[async_trait]
impl BackgroundTask for MyFailingTask {
    const TASK_NAME: &'static str = "my_failing_task";
    type AppData = MyApplicationContext;

    async fn run(&self, task: CurrentTask, _ctx: Self::AppData) -> Result<(), anyhow::Error> {
        // let new_task = MyFailingTask::new(self.number + 1);
        // queue
        //     .insert_task(&new_task)
        //     .await
        //     .unwrap();

        // task.id();
        // task.keep_alive().await?;
        // task.previous_error();
        // task.retry_count();

        log::info!("[{}] the current number is {}", task.id(), self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        log::info!("[{}] done..", task.id());
        //
        // let b = true;
        //
        // if b {
        //     panic!("Hello!");
        // } else {
        //     Ok(())
        // }
        Ok(())
    }
}
