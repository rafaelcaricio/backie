CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE backie_tasks (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    task_name VARCHAR NOT NULL,
    queue_name VARCHAR DEFAULT 'common' NOT NULL,
    uniq_hash CHAR(64) DEFAULT NULL,
    payload jsonb NOT NULL,
    timeout_msecs INT8 NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    running_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    done_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    error_info jsonb DEFAULT NULL,
    retries INTEGER DEFAULT 0 NOT NULL,
    max_retries INTEGER DEFAULT 0 NOT NULL,
    backoff_mode jsonb NOT NULL
);

--- create uniqueness index
CREATE UNIQUE INDEX backie_tasks_uniq_hash_index ON backie_tasks(uniq_hash) WHERE uniq_hash IS NOT NULL;
