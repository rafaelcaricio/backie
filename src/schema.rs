// @generated automatically by Diesel CLI.

diesel::table! {
    backie_tasks (id) {
        id -> Uuid,
        task_name -> Varchar,
        queue_name -> Varchar,
        uniq_hash -> Nullable<Bpchar>,
        payload -> Jsonb,
        timeout_msecs -> Int8,
        created_at -> Timestamptz,
        scheduled_at -> Timestamptz,
        running_at -> Nullable<Timestamptz>,
        done_at -> Nullable<Timestamptz>,
        error_info -> Nullable<Jsonb>,
        retries -> Int4,
        max_retries -> Int4,
        backoff_mode -> Jsonb,
    }
}
