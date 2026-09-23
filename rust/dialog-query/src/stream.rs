pub use async_stream::try_stream;
pub use dialog_common::ConditionalSend;
use futures_core::Stream;
pub use futures_core::{Future, TryStream};
pub use futures_util::{TryStreamExt, stream_select};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::EvaluationError;

/// A fallible stream that is `Send` on native targets and `!Send` on WASM.
///
/// This marker trait adapts the `Send` bound to the compilation target so
/// that the same query evaluation code works in both multi-threaded native
/// runtimes and single-threaded WASM environments.
pub trait SendStream<T>:
    TryStream<Ok = T, Error = EvaluationError, Item = Result<T, EvaluationError>> + ConditionalSend
{
}
impl<S, T> SendStream<T> for S where
    S: TryStream<Ok = T, Error = EvaluationError, Item = Result<T, EvaluationError>>
        + 'static
        + ConditionalSend
{
}

type PinnedSendStream<T> = Pin<Box<dyn SendStream<T>>>;

/// Split a stream into two independent streams that each receive a clone of
/// every item.
///
/// The two halves share the input and drive it themselves: whichever is
/// polled with nothing queued pulls the next item, keeps it, and queues a
/// clone for the other. Nothing is spawned and no channel sits between them,
/// so a fork costs no scheduler round trip. That matters because the query
/// engine forks once per disjunction per evaluation: a nested concept with a
/// rule besides its implicit one forks for every row it is evaluated on, and
/// a task plus two channels each time was most of what a small rule query
/// spent waiting.
///
/// Each half keeps its own waker, so a half waiting on the input is woken
/// when the other pulls an item for it, and a half dropped early wakes the
/// survivor, which then drives the input alone. An input error reaches both
/// halves and ends them.
pub fn fork_stream<S, T>(input: S) -> (PinnedSendStream<T>, PinnedSendStream<T>)
where
    S: SendStream<T> + ConditionalSend + 'static,
    T: Clone + ConditionalSend + 'static,
{
    let fanout = Arc::new(Mutex::new(Fanout {
        input: Some(Box::pin(input)),
        queues: [VecDeque::new(), VecDeque::new()],
        open: [true, true],
        wakers: [None, None],
    }));
    let left = Half {
        fanout: fanout.clone(),
        side: 0,
    };
    let right = Half { fanout, side: 1 };
    (Box::pin(left), Box::pin(right))
}

/// The state two halves of a [`fork_stream`] share.
struct Fanout<T> {
    /// The input, until it ends or fails.
    input: Option<PinnedSendStream<T>>,
    /// Items pulled by one half and not yet taken by the other.
    queues: [VecDeque<Result<T, EvaluationError>>; 2],
    /// Whether each half is still held.
    open: [bool; 2],
    /// The waker of each half last left waiting on the input.
    wakers: [Option<Waker>; 2],
}

/// One half of a [`fork_stream`].
struct Half<T> {
    fanout: Arc<Mutex<Fanout<T>>>,
    side: usize,
}

impl<T: Clone> Stream for Half<T> {
    type Item = Result<T, EvaluationError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let side = self.side;
        let other = 1 - side;
        let mut fanout = self
            .fanout
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        if let Some(item) = fanout.queues[side].pop_front() {
            return Poll::Ready(Some(item));
        }
        let Some(input) = fanout.input.as_mut() else {
            return Poll::Ready(None);
        };

        match input.as_mut().try_poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let failed = item.is_err();
                if fanout.open[other] {
                    fanout.queues[other].push_back(item.clone());
                    if let Some(waker) = fanout.wakers[other].take() {
                        waker.wake();
                    }
                }
                if failed {
                    fanout.input = None;
                }
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                fanout.input = None;
                if let Some(waker) = fanout.wakers[other].take() {
                    waker.wake();
                }
                Poll::Ready(None)
            }
            Poll::Pending => {
                fanout.wakers[side] = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl<T> Drop for Half<T> {
    fn drop(&mut self) {
        let mut fanout = self
            .fanout
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        fanout.open[self.side] = false;
        fanout.queues[self.side].clear();
        // The input may have been left holding this half's waker; wake the
        // survivor so it polls the input and registers its own.
        if let Some(waker) = fanout.wakers[1 - self.side].take() {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::fork_stream;
    use crate::EvaluationError;
    use futures_util::{StreamExt as _, TryStreamExt as _, stream};

    fn items(values: Vec<Result<u32, EvaluationError>>) -> impl super::SendStream<u32> {
        stream::iter(values)
    }

    /// Each half sees every item, whichever order the halves are
    /// drained in -- including one drained fully before the other
    /// starts.
    #[dialog_common::test]
    async fn it_gives_each_half_every_item() -> anyhow::Result<()> {
        let (left, right) = fork_stream(items(vec![Ok(1), Ok(2), Ok(3)]));
        let left: Vec<u32> = left.try_collect().await?;
        let right: Vec<u32> = right.try_collect().await?;
        assert_eq!(left, vec![1, 2, 3]);
        assert_eq!(right, vec![1, 2, 3]);
        Ok(())
    }

    /// Items interleave across the halves without being lost or
    /// repeated.
    #[dialog_common::test]
    async fn it_interleaves_the_halves() -> anyhow::Result<()> {
        let (mut left, mut right) = fork_stream(items(vec![Ok(1), Ok(2), Ok(3)]));
        assert_eq!(left.next().await, Some(Ok(1)));
        assert_eq!(right.next().await, Some(Ok(1)));
        assert_eq!(right.next().await, Some(Ok(2)));
        assert_eq!(left.next().await, Some(Ok(2)));
        assert_eq!(left.next().await, Some(Ok(3)));
        assert_eq!(left.next().await, None);
        assert_eq!(right.next().await, Some(Ok(3)));
        assert_eq!(right.next().await, None);
        Ok(())
    }

    /// Dropping one half leaves the other with every item.
    #[dialog_common::test]
    async fn it_survives_a_dropped_half() -> anyhow::Result<()> {
        let (left, mut right) = fork_stream(items(vec![Ok(1), Ok(2)]));
        assert_eq!(right.next().await, Some(Ok(1)));
        drop(left);
        let rest: Vec<u32> = right.try_collect().await?;
        assert_eq!(rest, vec![2]);
        Ok(())
    }

    /// An input error reaches both halves and ends them.
    #[dialog_common::test]
    async fn it_delivers_an_error_to_both_halves() {
        let error = EvaluationError::Store("broken".into());
        let (mut left, mut right) = fork_stream(items(vec![Ok(1), Err(error.clone()), Ok(2)]));
        assert_eq!(left.next().await, Some(Ok(1)));
        assert_eq!(left.next().await, Some(Err(error.clone())));
        assert_eq!(left.next().await, None);
        assert_eq!(right.next().await, Some(Ok(1)));
        assert_eq!(right.next().await, Some(Err(error)));
        assert_eq!(right.next().await, None);
    }
}
