use crate::errors::AsyncQueueError;
use crate::schema::backie_tasks;
use crate::task::Task;
use crate::task::{NewTask, TaskId};
use chrono::DateTime;
use chrono::Utc;
use diesel::prelude::*;
use diesel::ExpressionMethods;
use diesel_async::{pg::AsyncPgConnection, RunQueryDsl};
use std::time::Duration;

impl Task {
    pub(crate) async fn remove(
        connection: &mut AsyncPgConnection,
        id: TaskId,
    ) -> Result<u64, AsyncQueueError> {
        let query = backie_tasks::table.filter(backie_tasks::id.eq(id));
        Ok(diesel::delete(query).execute(connection).await? as u64)
    }

    pub(crate) async fn fail_with_message(
        connection: &mut AsyncPgConnection,
        id: TaskId,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        let error = serde_json::json!({
            "error": error_message,
        });
        let query = backie_tasks::table.filter(backie_tasks::id.eq(id));
        Ok(diesel::update(query)
            .set((
                backie_tasks::error_info.eq(Some(error)),
                backie_tasks::done_at.eq(Utc::now()),
            ))
            .get_result::<Task>(connection)
            .await?)
    }

    pub(crate) async fn schedule_retry(
        connection: &mut AsyncPgConnection,
        id: TaskId,
        backoff: Duration,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        use crate::schema::backie_tasks::dsl;

        let error = serde_json::json!({
            "error": error_message,
        });

        let task = diesel::update(backie_tasks::table.filter(backie_tasks::id.eq(id)))
            .set((
                backie_tasks::error_info.eq(Some(error)),
                backie_tasks::retries.eq(dsl::retries + 1),
                backie_tasks::scheduled_at.eq(Utc::now()
                    + chrono::Duration::from_std(backoff).unwrap_or(chrono::Duration::max_value())),
                backie_tasks::running_at.eq::<Option<DateTime<Utc>>>(None),
            ))
            .get_result::<Task>(connection)
            .await?;

        Ok(task)
    }

    pub(crate) async fn fetch_next_pending(
        connection: &mut AsyncPgConnection,
        queue_name: &str,
        execution_timeout: Option<Duration>,
        task_names: &[String],
    ) -> Option<Task> {
        if let Some(execution_timeout) = execution_timeout {
            backie_tasks::table
                .filter(backie_tasks::task_name.eq_any(task_names))
                .filter(backie_tasks::scheduled_at.lt(Utc::now())) // skip tasks scheduled for the future
                .order(backie_tasks::created_at.asc()) // get the oldest task first
                .filter(backie_tasks::done_at.is_null()) // and not marked as done
                .filter(backie_tasks::queue_name.eq(queue_name))
                .filter(
                    backie_tasks::running_at
                        .is_null()
                        .or(backie_tasks::running_at.lt(Utc::now()
                            - chrono::Duration::from_std(execution_timeout)
                                .unwrap_or(chrono::Duration::max_value()))),
                ) // that is not marked as running already or expired
                .for_update()
                .skip_locked()
                .limit(1)
                .get_result::<Task>(connection)
                .await
                .ok()
        } else {
            backie_tasks::table
                .filter(backie_tasks::task_name.eq_any(task_names))
                .filter(backie_tasks::scheduled_at.lt(Utc::now())) // skip tasks scheduled for the future
                .order(backie_tasks::created_at.asc()) // get the oldest task first
                .filter(backie_tasks::done_at.is_null()) // and not marked as done
                .filter(backie_tasks::queue_name.eq(queue_name))
                .filter(backie_tasks::running_at.is_null()) // that is not marked as running already
                .for_update()
                .skip_locked()
                .limit(1)
                .get_result::<Task>(connection)
                .await
                .ok()
        }
    }

    pub(crate) async fn set_running(
        connection: &mut AsyncPgConnection,
        task: Task,
    ) -> Result<Task, AsyncQueueError> {
        Ok(diesel::update(&task)
            .set((backie_tasks::running_at.eq(Utc::now()),))
            .get_result::<Task>(connection)
            .await?)
    }

    pub(crate) async fn set_done(
        connection: &mut AsyncPgConnection,
        id: TaskId,
    ) -> Result<Task, AsyncQueueError> {
        Ok(
            diesel::update(backie_tasks::table.filter(backie_tasks::id.eq(id)))
                .set((backie_tasks::done_at.eq(Utc::now()),))
                .get_result::<Task>(connection)
                .await?,
        )
    }

    pub(crate) async fn insert(
        connection: &mut AsyncPgConnection,
        new_task: NewTask,
    ) -> Result<Task, AsyncQueueError> {
        Ok(diesel::insert_into(backie_tasks::table)
            .values(new_task)
            .get_result::<Task>(connection)
            .await?)
    }
}
