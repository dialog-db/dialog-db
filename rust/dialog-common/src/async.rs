use futures::FutureExt;
use std::{future::Future, pin::pin};
use thiserror::Error;

#[cfg(target_arch = "wasm32")]
use std::pin::Pin;

#[cfg(target_arch = "wasm32")]
use tokio::sync::oneshot::channel;

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinSet;

/// Async module errors
#[derive(Error, Debug)]
pub enum DialogAsyncError {
    /// Generic join error
    #[error("Unable to rejoin pending future")]
    JoinError,
}

/// Spawn a future by scheduling it with the local executor. The returned
/// future will be pending until the spawned future completes.
#[cfg(target_arch = "wasm32")]
pub async fn spawn<F>(future: F) -> Result<F::Output, DialogAsyncError>
where
    F: Future + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = channel();

    wasm_bindgen_futures::spawn_local(async move {
        if let Err(_) = tx.send(future.await) {
            // Receiver dropped before spawned task completed
            return;
        }
    });

    // TODO: We should have some accomodation of error outputs here...
    rx.await.map_err(|_| DialogAsyncError::JoinError)
}

/// Spawn a future by scheduling it with the local executor. The returned
/// future will be pending until the spawned future completes.
#[cfg(not(target_arch = "wasm32"))]
pub async fn spawn<F>(future: F) -> Result<F::Output, DialogAsyncError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future)
        .await
        .map_err(|_| DialogAsyncError::JoinError)
}

/// An aggregator of async work that can be used to observe the moment when all
/// the aggregated work is completed. It is similar to tokio's [JoinSet], but is
/// relatively constrained and also works on `wasm32-unknown-unknown`. Unlike
/// [JoinSet], the results can not be observed individually.
///
/// ```rust
/// # use anyhow::Result;
/// # use noosphere_common::TaskQueue;
/// #
/// # #[tokio::main(flavor = "multi_thread")]
/// # async fn main() -> Result<()> {
/// #
/// let mut task_queue = TaskQueue::default();
/// for i in 0..10 {
///     task_queue.spawn(async move {
///         println!("{}", i);
///         Ok(())
///     });
/// }
/// task_queue.join().await?;
/// #
/// #   Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct TaskQueue {
    #[cfg(not(target_arch = "wasm32"))]
    tasks: JoinSet<Result<(), DialogAsyncError>>,

    #[cfg(target_arch = "wasm32")]
    tasks: Vec<SendSyncDoNotApply>,
}

#[cfg(target_arch = "wasm32")]
struct SendSyncDoNotApply(Pin<Box<dyn Future<Output = Result<(), DialogAsyncError>>>>);

#[cfg(target_arch = "wasm32")]
unsafe impl Send for SendSyncDoNotApply {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for SendSyncDoNotApply {}

#[cfg(target_arch = "wasm32")]
impl Future for SendSyncDoNotApply {
    type Output = Result<(), DialogAsyncError>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = unsafe { &mut self.get_unchecked_mut().0 };
        inner.as_mut().poll(cx)
        // let m = Pin::get_mut(self);
        // let f = &mut m.0;
        // f.poll_unpin(cx)
        // let pin = pin!(self.as_mut().0);
        // F::poll(pin, cx)
    }
}

impl TaskQueue {
    #[cfg(not(target_arch = "wasm32"))]
    /// Queue a future to be spawned in the local executor. All queued futures will be polled
    /// to completion before the [TaskQueue] can be joined.
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = Result<(), DialogAsyncError>> + Send + 'static,
    {
        self.tasks.spawn(future);
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Returns a future that finishes when all queued futures have finished.
    pub async fn join(&mut self) -> Result<(), DialogAsyncError> {
        while let Some(result) = self.tasks.join_next().await {
            // trace!("Task completed, {} remaining in queue...", self.tasks.len());
            result.map_err(|_| DialogAsyncError::JoinError)??;
        }
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    /// Queue a future to be spawned in the local executor. All queued futures will be polled
    /// to completion before the [TaskQueue] can be joined.
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = Result<(), DialogAsyncError>> + 'static,
    {
        self.tasks.push(SendSyncDoNotApply(Box::pin(
            async move { spawn(future).await? },
        )));
    }

    #[cfg(target_arch = "wasm32")]
    /// Returns a future that finishes when all queued futures have finished.
    pub async fn join(&mut self) -> Result<(), DialogAsyncError> {
        use futures::future::try_join_all;

        let tasks = std::mem::replace(&mut self.tasks, Vec::new());

        try_join_all(tasks).await?;

        Ok(())
    }

    /// The number of queued tasks
    pub fn count(&self) -> usize {
        self.tasks.len()
    }
}

impl std::fmt::Debug for TaskQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueue")
            .field("tasks", &self.tasks.len())
            .finish()
    }
}
