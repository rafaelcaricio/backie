[![Crates.io][s1]][ci] [![docs page][docs-badge]][docs] ![test][ga-test] ![style][ga-style]

# Backie

Async background job processing library with Diesel and Tokio. It's a heavily modified fork of [fang](https://github.com/ayrat555/fang).

## Key Features

 Here are some of the fang's key features:

 - Async workers: Workers are started as `tokio` tasks (async workers)
 - Unique tasks: Tasks are not duplicated in the queue if they are unique
 - Single-purpose workers: Tasks are stored in a single table but workers can be configured to execute only tasks of a specific type
 - Retries: Tasks can be retried with a custom backoff mode

## Differences from Fang crate

- Supports only async processing
- Supports graceful shutdown
- The connection pool for the queue is provided by the user
- Tasks status is calculated based on the database state
- Tasks have a timeout and are retried if they are not completed in time

## Installation

1. Add this to your Cargo.toml

```toml
[dependencies]
backie = "0.10"
```

*Supports rustc 1.67+*

2. Create the `backie_tasks` table in the Postgres database. The migration can be found in [the migrations directory](https://github.com/rafaelcaricio/backie/blob/master/migrations/2023-03-06-151907_create_backie_tasks/up.sql).

## Usage

Every task must implement the `backie::RunnableTask` trait, Backie uses the information provided by the trait to
execute the task.

All implementations of `RunnableTask` must have unique names per project.

```rust
use backie::RunnableTask;
use backie::task::{TaskHash, TaskType};
use backie::queue::AsyncQueueable;
use serde::{Deserialize, Serialize};
use async_trait::async_trait;

#[derive(Serialize, Deserialize)]
#[serde(crate = "fang::serde")]
struct MyTask {
   pub number: u16,
}

#[typetag::serde]
#[async_trait]
impl RunnableTask for MyTask {
   async fn run(&self, _queueable: &mut dyn AsyncQueueable) -> Result<(), Error> {
      Ok(())
   }
   
   // this func is optional
   // Default task_type is common
   fn task_type(&self) -> TaskType {
      "my-task-type".into()
   }

   // If `uniq` is set to true and the task is already in the storage, it won't be inserted again
   // The existing record will be returned for for any insertions operaiton
   fn uniq(&self) -> Option<TaskHash> {
       None
   }

   // the maximum number of retries. Set it to 0 to make it not retriable
   // the default value is 20
   fn max_retries(&self) -> i32 {
      20
   }

   // backoff mode for retries
   fn backoff(&self, attempt: u32) -> u32 {
      u32::pow(2, attempt)
   }
}
```

### Enqueuing a task

To enqueue a task use `AsyncQueueable::create_task`.

For Postgres backend.
```rust
use backie::queue::PgAsyncQueue;

// Create an AsyncQueue
let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new("postgres://postgres:password@localhost/backie");
let pool = Pool::builder()
    .max_size(1)
    .min_idle(Some(1))
    .build(manager)
    .await
    .unwrap();

let mut queue = PgAsyncQueue::new(pool);

// Publish the first example
let task = MyTask { number: 8 };
let task_returned = queue
  .create_task(&task)
  .await
  .unwrap();
```

### Starting workers

Every worker runs in a separate `tokio` task. In case of panic, they are always restarted.
Use `AsyncWorkerPool` to start workers.

```rust
use backie::worker_pool::AsyncWorkerPool;

// Need to create a queue
// Also insert some tasks

let mut pool: AsyncWorkerPool<PgAsyncQueue> = AsyncWorkerPool::builder()
        .number_of_workers(max_pool_size)
        .queue(queue.clone())
         // if you want to run tasks of the specific kind
        .task_type("my_task_type".into())
        .build();

pool.start().await;
```

Check out:

- [Simple Worker Example](https://github.com/rafaelcaricio/backie/tree/master/examples/simple_worker) - simple worker example

### Configuration

Use the `AsyncWorkerPool` builder:
    
 ```rust
 let mut pool: AsyncWorkerPool<PgAsyncQueue> = AsyncWorkerPool::builder()
     .number_of_workers(max_pool_size)
     .queue(queue.clone())
     .build();
 ```

### Configuring the type of workers

### Configuring retention mode

By default, all successfully finished tasks are removed from the DB, failed tasks aren't.

There are three retention modes you can use:

```rust
pub enum RetentionMode {
    KeepAll,        // doesn't remove tasks
    RemoveAll,      // removes all tasks
    RemoveFinished, // default value
}
```

Set retention mode with worker pools `TypeBuilder` in both modules.

## Contributing

1. [Fork it!](https://github.com/ayrat555/fang/fork)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

### Running tests locally
- Install diesel_cli.
```
cargo install diesel_cli
```
- Install docker on your machine.

- Run a Postgres docker container. (See in Makefile.)
```
make db
```

- Run the migrations
```
make diesel
```

- Run tests
```
make tests
```

- Run dirty//long tests, DB must be recreated afterwards.
```
make ignored
```

- Kill the docker container
```
make stop
```

## Thank Fang's authors

I would like to thank the authors of the fang crate which was the inspiration for this project.

- Ayrat Badykov (@ayrat555)
- Pepe Márquez (@pxp9)

[ci]: https://crates.io/crates/backie
[docs]: https://docs.rs/backie/
[ga-test]: https://github.com/rafaelcaricio/backie/actions/workflows/rust.yml/badge.svg
[ga-style]: https://github.com/rafaelcaricio/backie/actions/workflows/style.yml/badge.svg
