# Backie 🚲

Async persistent background task processing for Rust applications with Tokio. Queue asynchronous tasks
to be processed by workers. It's designed to be easy to use and horizontally scalable. It uses Postgres as
a storage backend and can also be extended to support other types of storage.

High-level overview of how Backie works:
- Client puts tasks on a queue
- Server starts a multiple workers per queue
- Worker pulls tasks off the queue and starts processing them
- Tasks are processed concurrently by multiple workers

Backie started as a fork of
[fang](https://github.com/ayrat555/fang) crate, but quickly diverged significantly in its implementation.

## Key features

Here are some of the Backie's key features:

- **Guaranteed execution**: at least one execution of a task
- **Async workers**: Workers are started as [Tokio](https://tokio.rs/) tasks
- **Application context**: Tasks can access an shared user-provided application context
- **Single-purpose workers**: Tasks are stored together but workers are configured to execute only tasks of a specific queue
- **Retries**: Tasks are retried with a custom backoff mode
- **Graceful shutdown**: provide a future to gracefully shutdown the workers, on-the-fly tasks are not interrupted
- **Recovery of unfinished tasks**: Tasks that were not finished are retried on the next worker start
- **Unique tasks**: Tasks are not duplicated in the queue if they provide a unique hash

## Other planned features

- Task timeout: Tasks are retried if they are not completed in time
- Scheduling of tasks: Tasks can be scheduled to be executed at a specific time

## Safety

This crate uses `#![forbid(unsafe_code)]` to ensure everything is implemented in 100% safe Rust.

## Minimum supported Rust version

Backie's MSRV is 1.68.

## Installation

1. Add this to your `Cargo.toml`

```toml
[dependencies]
backie = "0.1"
```

If you are not already using, you will also want to include the following dependencies for defining your tasks:

```toml
[dependencies]
async-trait = "0.1"
serde = { version = "1.0", features = ["derive"] }
diesel = { version = "2.0", features = ["postgres", "serde_json", "chrono", "uuid"] }
diesel-async = { version = "0.2", features = ["postgres", "bb8"] }
```

Those dependencies are required to use the `#[async_trait]` and `#[derive(Serialize, Deserialize)]` attributes
in your task definitions and to connect to the Postgres database.

2. Create the `backie_tasks` table in the Postgres database. The migration can be found in [the migrations directory](https://github.com/rafaelcaricio/backie/blob/master/migrations/2023-03-06-151907_create_backie_tasks/up.sql).

## Usage

The [`BackgroundTask`] trait is used to define a task. You must implement this trait for all
tasks you want to execute.

One important thing to note is the use of the attribute [`BackgroundTask::TASK_NAME`] which **must** be unique for 
the whole application. This attribute is critical for reconstructing the task back from the database.

The [`BackgroundTask::AppData`] can be used to argument the task with your application specific contextual information.
This is useful for example to pass a database connection pool to the task or other application configuration.

The [`BackgroundTask::Error`] is the error type that will be returned by the [`BackgroundTask::run`] method. You can
use this to define your own error type for your tasks.

The [`BackgroundTask::run`] method is where you define the behaviour of your background task execution. This method
will be called by the task queue workers.

```rust
use async_trait::async_trait;
use backie::{BackgroundTask, CurrentTask};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MyTask {
    info: String,
}

#[async_trait]
impl BackgroundTask for MyTask {
    const TASK_NAME: &'static str = "my_task_unique_name";
    type AppData = ();
    type Error = ();

    async fn run(&self, task: CurrentTask, context: Self::AppData) -> Result<(), Self::Error> {
        // Do something
        Ok(())
    }
}
```

### Starting workers

First, we need to create a [`TaskStore`] trait instance. This is the object responsible for storing and retrieving
tasks from a database. Backie currently only supports Postgres as a storage backend via the provided
[`PgTaskStore`]. You can implement other storage backends by implementing the [`TaskStore`] trait.

Then, we can use the `task_store` to start a worker pool using the [`WorkerPool`]. The [`WorkerPool`] is responsible
for starting the workers and managing their lifecycle.

A full example of starting a worker pool can be found in the [examples directory](https://github.com/rafaelcaricio/backie/blob/main/examples/simple_worker/src/main.rs).

### Queueing tasks

After stating the workers, we get an instance of [`Queue`] which we can use to enqueue tasks. It is also possible
to directly create a [`Queue`] instance from with a [`TaskStore`] instance.

This will enqueue the task and whenever a worker is available it will start processing. Workers don't need to be
started before enqueuing tasks. Workers don't need to be in the same process as the queue as long as the workers have
access to the same underlying storage system. This enables horizontal scaling of the workers.

## License

This project is licensed under the [MIT license][license].

## Contributing

1. [Fork it!](https://github.com/rafaelcaricio/backie/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Acknowledgements

I would like to thank the authors of the [Fang](https://github.com/ayrat555/fang) and [background_job](https://git.asonix.dog/asonix/background-jobs.git) crates which were the main inspiration for this project.

- Ayrat Badykov ([@ayrat555](https://github.com/ayrat555))
- Pepe Márquez ([@pxp9](https://github.com/pxp9))
- Riley ([asonix](https://github.com/asonix))
