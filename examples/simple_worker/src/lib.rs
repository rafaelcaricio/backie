use std::time::Duration;
use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use backie::{RunnableTask, Queueable};

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
#[typetag::serde]
impl RunnableTask for MyTask {
    async fn run(&self, _queue: &mut dyn Queueable) -> Result<(), Box<dyn std::error::Error + Send + 'static>> {
        // let new_task = MyTask::new(self.number + 1);
        // queue
        //     .insert_task(&new_task as &dyn AsyncRunnable)
        //     .await
        //     .unwrap();

        log::info!("the current number is {}", self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        log::info!("done..");
        Ok(())
    }
}

#[async_trait]
#[typetag::serde]
impl RunnableTask for MyFailingTask {
    async fn run(&self, _queue: &mut dyn Queueable) -> Result<(), Box<dyn std::error::Error + Send + 'static>> {
        // let new_task = MyFailingTask::new(self.number + 1);
        // queue
        //     .insert_task(&new_task as &dyn AsyncRunnable)
        //     .await
        //     .unwrap();

        log::info!("the current number is {}", self.number);
        tokio::time::sleep(Duration::from_secs(3)).await;

        log::info!("done..");
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
