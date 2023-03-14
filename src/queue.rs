use crate::errors::BackieError;
use crate::runnable::BackgroundTask;
use crate::store::TaskStore;
use crate::task::NewTask;
use std::time::Duration;

pub struct Queue<S>
where
    S: TaskStore,
{
    task_store: S,
}

impl<S> Queue<S>
where
    S: TaskStore,
{
    pub fn new(task_store: S) -> Self {
        Queue { task_store }
    }

    pub async fn enqueue<BT>(&self, background_task: BT) -> Result<(), BackieError>
    where
        BT: BackgroundTask,
    {
        // TODO: Add option to specify the timeout of a task
        self.task_store
            .create_task(NewTask::new(background_task, Duration::from_secs(10))?)
            .await?;
        Ok(())
    }
}

impl<S> Clone for Queue<S>
where
    S: TaskStore + Clone,
{
    fn clone(&self) -> Self {
        Self {
            task_store: self.task_store.clone(),
        }
    }
}
