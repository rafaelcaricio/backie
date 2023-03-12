use crate::worker::TaskExecError;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

pub(crate) struct CatchUnwindFuture<F: Future + Send + 'static> {
    inner: BoxFuture<'static, F::Output>,
}

impl<F: Future + Send + 'static> CatchUnwindFuture<F> {
    pub fn create(f: F) -> CatchUnwindFuture<F> {
        Self { inner: f.boxed() }
    }
}

impl<F: Future + Send + 'static> Future for CatchUnwindFuture<F> {
    type Output = Result<F::Output, TaskExecError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = &mut self.inner;

        match catch_unwind(move || inner.poll_unpin(cx)) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(cause) => Poll::Ready(Err(cause)),
        }
    }
}

fn catch_unwind<F: FnOnce() -> R, R>(f: F) -> Result<R, TaskExecError> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(res) => Ok(res),
        Err(cause) => match cause.downcast_ref::<&'static str>() {
            None => match cause.downcast_ref::<String>() {
                None => Err(TaskExecError::Panicked(
                    "Sorry, unknown panic message".to_string(),
                )),
                Some(message) => Err(TaskExecError::Panicked(message.to_string())),
            },
            Some(message) => Err(TaskExecError::Panicked(message.to_string())),
        },
    }
}
