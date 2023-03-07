CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE backie_tasks (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    payload jsonb NOT NULL,
    error_message TEXT DEFAULT NULL,
    task_type VARCHAR DEFAULT 'common' NOT NULL,
    uniq_hash CHAR(64) DEFAULT NULL,
    retries INTEGER DEFAULT 0 NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    running_at TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    done_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

CREATE INDEX backie_tasks_type_index ON backie_tasks(task_type);
CREATE INDEX backie_tasks_created_at_index ON backie_tasks(created_at);
CREATE INDEX backie_tasks_uniq_hash ON backie_tasks(uniq_hash);

--- create uniqueness index
CREATE UNIQUE INDEX backie_tasks_uniq_hash_index ON backie_tasks(uniq_hash) WHERE uniq_hash IS NOT NULL;

CREATE FUNCTION backie_notify_new_tasks() returns trigger as $$
BEGIN
    perform pg_notify('backie::tasks', 'created');
    return new;
END;
$$ language plpgsql;

CREATE TRIGGER backie_notify_workers after insert on backie_tasks for each statement execute procedure backie_notify_new_tasks();
