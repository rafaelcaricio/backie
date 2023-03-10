use crate::errors::AsyncQueueError;
use crate::schema::backie_tasks;
use crate::task::Task;
use crate::task::{NewTask, TaskHash, TaskId};
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use diesel::prelude::*;
use diesel::ExpressionMethods;
use diesel_async::{pg::AsyncPgConnection, RunQueryDsl};

impl Task {
    pub(crate) async fn remove_all(
        connection: &mut AsyncPgConnection,
    ) -> Result<u64, AsyncQueueError> {
        Ok(diesel::delete(backie_tasks::table)
            .execute(connection)
            .await? as u64)
    }

    pub(crate) async fn remove_all_scheduled(
        connection: &mut AsyncPgConnection,
    ) -> Result<u64, AsyncQueueError> {
        let query = backie_tasks::table.filter(backie_tasks::running_at.is_null());
        Ok(diesel::delete(query).execute(connection).await? as u64)
    }

    pub(crate) async fn remove(
        connection: &mut AsyncPgConnection,
        id: TaskId,
    ) -> Result<u64, AsyncQueueError> {
        let query = backie_tasks::table.filter(backie_tasks::id.eq(id));
        Ok(diesel::delete(query).execute(connection).await? as u64)
    }

    pub(crate) async fn remove_by_hash(
        connection: &mut AsyncPgConnection,
        task_hash: TaskHash,
    ) -> Result<bool, AsyncQueueError> {
        let query = backie_tasks::table.filter(backie_tasks::uniq_hash.eq(task_hash));
        let qty = diesel::delete(query).execute(connection).await?;
        Ok(qty > 0)
    }

    pub(crate) async fn find_by_id(
        connection: &mut AsyncPgConnection,
        id: TaskId,
    ) -> Result<Task, AsyncQueueError> {
        let task = backie_tasks::table
            .filter(backie_tasks::id.eq(id))
            .first::<Task>(connection)
            .await?;
        Ok(task)
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
        backoff_seconds: u32,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        use crate::schema::backie_tasks::dsl;

        let now = Utc::now();
        let scheduled_at = now + Duration::seconds(backoff_seconds as i64);

        let error = serde_json::json!({
            "error": error_message,
        });

        let task = diesel::update(backie_tasks::table.filter(backie_tasks::id.eq(id)))
            .set((
                backie_tasks::error_info.eq(Some(error)),
                backie_tasks::retries.eq(dsl::retries + 1),
                backie_tasks::scheduled_at.eq(scheduled_at),
                backie_tasks::running_at.eq::<Option<DateTime<Utc>>>(None),
            ))
            .get_result::<Task>(connection)
            .await?;

        Ok(task)
    }

    pub(crate) async fn fetch_next_pending(
        connection: &mut AsyncPgConnection,
        queue_name: &str,
    ) -> Option<Task> {
        backie_tasks::table
            .filter(backie_tasks::scheduled_at.lt(Utc::now())) // skip tasks scheduled for the future
            .order(backie_tasks::created_at.asc()) // get the oldest task first
            .filter(backie_tasks::running_at.is_null()) // that is not marked as running already
            .filter(backie_tasks::done_at.is_null()) // and not marked as done
            .filter(backie_tasks::queue_name.eq(queue_name))
            .limit(1)
            .for_update()
            .skip_locked()
            .get_result::<Task>(connection)
            .await
            .ok()
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

    pub(crate) async fn find_by_uniq_hash(
        connection: &mut AsyncPgConnection,
        hash: TaskHash,
    ) -> Option<Task> {
        backie_tasks::table
            .filter(backie_tasks::uniq_hash.eq(hash))
            .first::<Task>(connection)
            .await
            .ok()
    }
}
