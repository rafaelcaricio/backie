use crate::errors::BackieError;
use crate::runnable::BackgroundTask;
use crate::store::TaskStore;
use crate::task::NewTask;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct Queue<S>
where
    S: TaskStore + Clone,
{
    task_store: Arc<S>,
}

impl<S> Queue<S>
where
    S: TaskStore + Clone,
{
    pub fn new(task_store: Arc<S>) -> Self {
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
