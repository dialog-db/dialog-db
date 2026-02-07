//! Cross-platform async utilities for task spawning and aggregation.
//!
//! This module provides async primitives that work on both native platforms
//! (using tokio) and WebAssembly (using wasm-bindgen-futures). The main
//! abstractions are:
//!
//! - [`spawn`]: Spawn a future and await its result
//! - [`TaskQueue`]: Aggregate multiple fire-and-forget tasks and join them
//!
//! # Platform Differences
//!
//! On native platforms, tasks are spawned via `tokio::spawn` which requires
//! `Send` bounds. On wasm32, tasks are spawned via `wasm_bindgen_futures::spawn_local`
//! which does not require `Send` (since wasm is single-threaded).
//!
//! # Example
//!
//! ```rust,no_run
//! use dialog_common::r#async::{spawn, TaskQueue, DialogAsyncError};
//!
//! # async fn example() -> Result<(), DialogAsyncError> {
//! // Spawn a single task
//! let result = spawn(async { 42 }).await?;
//!
//! // Aggregate multiple tasks
//! let mut queue = TaskQueue::default();
//! queue.spawn(async { Ok::<(), DialogAsyncError>(()) });
//! queue.spawn(async { Ok::<(), DialogAsyncError>(()) });
//! queue.join().await?;
//! # Ok(())
//! # }
//! ```

use std::future::Future;

#[cfg(target_arch = "wasm32")]
use std::pin::Pin;

#[cfg(target_arch = "wasm32")]
use futures_util::future::try_join_all;
#[cfg(target_arch = "wasm32")]
use tokio::sync::oneshot::channel;

#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinSet;

use thiserror::Error;

use crate::ConditionalSend;

/// Errors that can occur during async task execution.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum DialogAsyncError {
    /// The spawned task failed to rejoin (e.g., task panicked or was cancelled).
    #[error("Unable to rejoin pending future")]
    JoinError,
}

/// Spawns a future on the executor and returns its output.
///
/// This function schedules the given future to run on the async executor and
/// returns a future that resolves to the spawned future's output. The caller
/// can await the result to get the value produced by the spawned task.
///
/// # Platform Behavior
///
/// - **Native (tokio)**: Uses `tokio::spawn`, which runs the task on the
///   tokio runtime's thread pool. Requires `F: Send`.
/// - **WebAssembly**: Uses `wasm_bindgen_futures::spawn_local`, which runs
///   the task on the single-threaded wasm executor. Does not require `Send`.
///
/// # Errors
///
/// Returns [`DialogAsyncError::JoinError`] if:
/// - The spawned task panics
/// - The task is cancelled before completion
/// - (wasm) The receiver is dropped before the task completes
///
/// # Example
///
/// ```rust,no_run
/// use dialog_common::r#async::{spawn, DialogAsyncError};
///
/// # async fn expensive_computation() {}
/// # async fn example() -> Result<(), DialogAsyncError> {
/// let result = spawn(async {
///     expensive_computation().await
/// }).await?;
/// # Ok(())
/// # }
/// ```
pub async fn spawn<F>(future: F) -> Result<F::Output, DialogAsyncError>
where
    F: Future + ConditionalSend + 'static,
    F::Output: Send + 'static,
{
    #[cfg(target_arch = "wasm32")]
    {
        let (tx, rx) = channel();

        wasm_bindgen_futures::spawn_local(async move {
            // Send the result back; ignore error if receiver was dropped
            let _ = tx.send(future.await);
        });

        rx.await.map_err(|_| DialogAsyncError::JoinError)
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
        tokio::spawn(future)
            .await
            .map_err(|_| DialogAsyncError::JoinError)
    }
}

/// An aggregator of async work that can be used to observe the moment when all
/// the aggregated work is completed. It is similar to tokio's [JoinSet], but is
/// relatively constrained and also works on `wasm32-unknown-unknown`. Unlike
/// [JoinSet], the results can not be observed individually.
///
/// ```text
/// let mut task_queue = TaskQueue::default();
/// for i in 0..10 {
///     task_queue.spawn(async move {
///         println!("{}", i);
///         Ok(())
///     });
/// }
/// task_queue.join().await?;
/// ```
#[derive(Default)]
pub struct TaskQueue {
    #[cfg(not(target_arch = "wasm32"))]
    tasks: JoinSet<Result<(), DialogAsyncError>>,

    #[cfg(target_arch = "wasm32")]
    tasks: Vec<SendSyncDoNotApply>,
}

/// Wrapper to make non-Send futures usable in contexts requiring Send+Sync.
///
/// # Safety
///
/// This is safe on wasm32 because:
/// 1. WebAssembly is single-threaded, so Send/Sync are vacuously satisfied
/// 2. The futures are only ever polled from the same thread that created them
/// 3. This wrapper is only compiled on wasm32 targets
///
/// This pattern allows `TaskQueue` to have a uniform API across platforms
/// while respecting that wasm futures don't need Send bounds.
#[cfg(target_arch = "wasm32")]
struct SendSyncDoNotApply(Pin<Box<dyn Future<Output = Result<(), DialogAsyncError>>>>);

#[cfg(target_arch = "wasm32")]
// SAFETY: wasm32 is single-threaded, so Send is vacuously satisfied
unsafe impl Send for SendSyncDoNotApply {}

#[cfg(target_arch = "wasm32")]
// SAFETY: wasm32 is single-threaded, so Sync is vacuously satisfied
unsafe impl Sync for SendSyncDoNotApply {}

#[cfg(target_arch = "wasm32")]
impl Future for SendSyncDoNotApply {
    type Output = Result<(), DialogAsyncError>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: We're not moving the inner future, just getting a mutable reference
        // to poll it. The Pin projection is safe because we maintain the pin invariant.
        let inner = unsafe { &mut self.get_unchecked_mut().0 };
        inner.as_mut().poll(cx)
    }
}

impl TaskQueue {
    /// Queues a future to be executed when [`join`](Self::join) is called.
    ///
    /// The future must return `Result<(), DialogAsyncError>`. All queued futures
    /// will be polled to completion before `join` returns.
    ///
    /// # Platform Differences
    ///
    /// - **Native**: Requires `F: Send + 'static`
    /// - **WebAssembly**: Only requires `F: 'static` (no Send bound)
    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = Result<(), DialogAsyncError>> + ConditionalSend + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.tasks.spawn(future);
        }

        #[cfg(target_arch = "wasm32")]
        {
            self.tasks.push(SendSyncDoNotApply(Box::pin(
                async move { spawn(future).await? },
            )));
        }
    }

    /// Waits for all queued tasks to complete.
    ///
    /// Returns `Ok(())` if all tasks completed successfully, or the first
    /// error encountered. After `join` returns, the queue is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any spawned task returns an error
    /// - Any spawned task panics ([`DialogAsyncError::JoinError`])
    pub async fn join(&mut self) -> Result<(), DialogAsyncError> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            while let Some(result) = self.tasks.join_next().await {
                result.map_err(|_| DialogAsyncError::JoinError)??;
            }
            Ok(())
        }

        #[cfg(target_arch = "wasm32")]
        {
            let tasks = std::mem::take(&mut self.tasks);
            try_join_all(tasks).await?;
            Ok(())
        }
    }

    /// Returns the number of tasks currently queued.
    ///
    /// This count decreases as tasks complete during [`join`](Self::join).
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[dialog_macros::test]
    async fn it_returns_future_output_from_spawn() {
        let result = spawn(async { 42 }).await.unwrap();
        assert_eq!(result, 42);
    }

    #[dialog_macros::test]
    async fn it_propagates_async_result_from_spawn() {
        let result: Result<i32, &str> = spawn(async { Ok(123) }).await.unwrap();
        assert_eq!(result, Ok(123));
    }

    #[dialog_macros::test]
    async fn it_joins_empty_queue() {
        let mut queue = TaskQueue::default();
        assert_eq!(queue.count(), 0);
        queue.join().await.unwrap();
    }

    #[dialog_macros::test]
    async fn it_executes_single_task() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let mut queue = TaskQueue::default();
        queue.spawn(async move {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        assert_eq!(queue.count(), 1);
        queue.join().await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[dialog_macros::test]
    async fn it_executes_multiple_tasks() {
        let counter = Arc::new(AtomicUsize::new(0));

        let mut queue = TaskQueue::default();
        for _ in 0..10 {
            let counter_clone = counter.clone();
            queue.spawn(async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
        }

        assert_eq!(queue.count(), 10);
        queue.join().await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[dialog_macros::test]
    async fn it_propagates_task_error() {
        let mut queue = TaskQueue::default();
        queue.spawn(async { Err(DialogAsyncError::JoinError) });

        let result = queue.join().await;
        assert!(result.is_err());
    }

    #[dialog_macros::test]
    async fn it_is_empty_after_join() {
        let mut queue = TaskQueue::default();
        queue.spawn(async { Ok(()) });
        queue.spawn(async { Ok(()) });

        queue.join().await.unwrap();
        assert_eq!(queue.count(), 0);
    }

    #[dialog_macros::test]
    async fn it_can_be_reused() {
        let counter = Arc::new(AtomicUsize::new(0));

        let mut queue = TaskQueue::default();

        // First batch
        for _ in 0..3 {
            let counter_clone = counter.clone();
            queue.spawn(async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
        }
        queue.join().await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 3);

        // Second batch
        for _ in 0..2 {
            let counter_clone = counter.clone();
            queue.spawn(async move {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
        }
        queue.join().await.unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[dialog_macros::test]
    fn it_shows_count_in_debug() {
        let queue = TaskQueue::default();
        assert!(format!("{:?}", queue).contains("tasks: 0"));
    }
}
