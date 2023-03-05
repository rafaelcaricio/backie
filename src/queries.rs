use crate::errors::AsyncQueueError;
use crate::fang_task_state::FangTaskState;
use crate::runnable::AsyncRunnable;
use crate::schema::fang_tasks;
use crate::task::NewTask;
use crate::task::{Task, DEFAULT_TASK_TYPE};
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use diesel::prelude::*;
use diesel::ExpressionMethods;
use diesel_async::{pg::AsyncPgConnection, RunQueryDsl};
use sha2::{Digest, Sha256};
use uuid::Uuid;

impl Task {
    pub async fn remove_all(
        connection: &mut AsyncPgConnection,
    ) -> Result<u64, AsyncQueueError> {
        Ok(diesel::delete(fang_tasks::table)
            .execute(connection)
            .await? as u64)
    }

    pub async fn remove_all_scheduled(
        connection: &mut AsyncPgConnection,
    ) -> Result<u64, AsyncQueueError> {
        let query = fang_tasks::table.filter(fang_tasks::scheduled_at.gt(Utc::now()));
        Ok(diesel::delete(query).execute(connection).await? as u64)
    }

    pub async fn remove(
        connection: &mut AsyncPgConnection,
        id: Uuid,
    ) -> Result<u64, AsyncQueueError> {
        let query = fang_tasks::table.filter(fang_tasks::id.eq(id));
        Ok(diesel::delete(query).execute(connection).await? as u64)
    }

    pub async fn remove_by_metadata(
        connection: &mut AsyncPgConnection,
        task: &dyn AsyncRunnable,
    ) -> Result<u64, AsyncQueueError> {
        let metadata = serde_json::to_value(task)?;

        let uniq_hash = Self::calculate_hash(metadata.to_string());

        let query = fang_tasks::table.filter(fang_tasks::uniq_hash.eq(uniq_hash));

        Ok(diesel::delete(query).execute(connection).await? as u64)
    }

    pub async fn remove_by_type(
        connection: &mut AsyncPgConnection,
        task_type: &str,
    ) -> Result<u64, AsyncQueueError> {
        let query = fang_tasks::table.filter(fang_tasks::task_type.eq(task_type));
        Ok(diesel::delete(query).execute(connection).await? as u64)
    }

    pub async fn find_by_id(
        connection: &mut AsyncPgConnection,
        id: Uuid,
    ) -> Result<Task, AsyncQueueError> {
        let task = fang_tasks::table
            .filter(fang_tasks::id.eq(id))
            .first::<Task>(connection)
            .await?;
        Ok(task)
    }

    pub async fn fail_with_message(
        connection: &mut AsyncPgConnection,
        task: Task,
        error_message: &str,
    ) -> Result<Task, AsyncQueueError> {
        Ok(diesel::update(&task)
            .set((
                fang_tasks::state.eq(FangTaskState::Failed),
                fang_tasks::error_message.eq(error_message),
                fang_tasks::updated_at.eq(Utc::now()),
            ))
            .get_result::<Task>(connection)
            .await?)
    }

    pub async fn schedule_retry(
        connection: &mut AsyncPgConnection,
        task: &Task,
        backoff_seconds: u32,
        error: &str,
    ) -> Result<Task, AsyncQueueError> {
        let now = Utc::now();
        let scheduled_at = now + Duration::seconds(backoff_seconds as i64);

        let task = diesel::update(task)
            .set((
                fang_tasks::state.eq(FangTaskState::Retried),
                fang_tasks::error_message.eq(error),
                fang_tasks::retries.eq(task.retries + 1),
                fang_tasks::scheduled_at.eq(scheduled_at),
                fang_tasks::updated_at.eq(now),
            ))
            .get_result::<Task>(connection)
            .await?;

        Ok(task)
    }

    pub async fn fetch_by_type(
        connection: &mut AsyncPgConnection,
        task_type: Option<String>,
    ) -> Option<Task> {
        fang_tasks::table
            .order(fang_tasks::created_at.asc())
            .order(fang_tasks::scheduled_at.asc())
            .limit(1)
            .filter(fang_tasks::scheduled_at.le(Utc::now()))
            .filter(fang_tasks::state.eq_any(vec![FangTaskState::New, FangTaskState::Retried]))
            .filter(
                fang_tasks::task_type
                    .eq(task_type.unwrap_or_else(|| DEFAULT_TASK_TYPE.to_string())),
            )
            .for_update()
            .skip_locked()
            .get_result::<Task>(connection)
            .await
            .ok()
    }

    pub async fn update_state(
        connection: &mut AsyncPgConnection,
        task: Task,
        state: FangTaskState,
    ) -> Result<Task, AsyncQueueError> {
        let updated_at = Utc::now();
        Ok(diesel::update(&task)
            .set((
                fang_tasks::state.eq(state),
                fang_tasks::updated_at.eq(updated_at),
            ))
            .get_result::<Task>(connection)
            .await?)
    }

    pub async fn insert(
        connection: &mut AsyncPgConnection,
        params: &dyn AsyncRunnable,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Task, AsyncQueueError> {
        if !params.uniq() {
            let new_task = NewTask::builder()
                .scheduled_at(scheduled_at)
                .uniq_hash(None)
                .task_type(params.task_type())
                .metadata(serde_json::to_value(params).unwrap())
                .build();

            Ok(diesel::insert_into(fang_tasks::table)
                .values(new_task)
                .get_result::<Task>(connection)
                .await?)
        } else {
            let metadata = serde_json::to_value(params).unwrap();

            let uniq_hash = Self::calculate_hash(metadata.to_string());

            match Self::find_by_uniq_hash(connection, &uniq_hash).await {
                Some(task) => Ok(task),
                None => {
                    let new_task = NewTask::builder()
                        .scheduled_at(scheduled_at)
                        .uniq_hash(Some(uniq_hash))
                        .task_type(params.task_type())
                        .metadata(serde_json::to_value(params).unwrap())
                        .build();

                    Ok(diesel::insert_into(fang_tasks::table)
                        .values(new_task)
                        .get_result::<Task>(connection)
                        .await?)
                }
            }
        }
    }

    fn calculate_hash(json: String) -> String {
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        let result = hasher.finalize();
        hex::encode(result)
    }

    pub async fn find_by_uniq_hash(
        connection: &mut AsyncPgConnection,
        uniq_hash: &str,
    ) -> Option<Task> {
        fang_tasks::table
            .filter(fang_tasks::uniq_hash.eq(uniq_hash))
            .filter(fang_tasks::state.eq_any(vec![FangTaskState::New, FangTaskState::Retried]))
            .first::<Task>(connection)
            .await
            .ok()
    }
}
