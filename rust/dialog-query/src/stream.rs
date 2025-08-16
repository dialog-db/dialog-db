use async_stream::try_stream;
use dialog_common::ConditionalSend;
use futures_core::{Future, TryStream};
use futures_util::TryStreamExt;
use tokio::sync::mpsc::unbounded_channel;

use crate::QueryError;

pub trait SendStream<T>:
    TryStream<Ok = T, Error = QueryError, Item = Result<T, QueryError>> + ConditionalSend
{
}
impl<S, T> SendStream<T> for S where
    S: TryStream<Ok = T, Error = QueryError, Item = Result<T, QueryError>>
        + 'static
        + ConditionalSend
{
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}
#[cfg(not(target_arch = "wasm32"))]
fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

pub fn fork_stream<S, T>(input: S) -> (impl SendStream<T>, impl SendStream<T>)
where
    S: SendStream<T> + ConditionalSend + 'static,
    T: Clone + ConditionalSend + 'static,
{
    let (left_tx, mut left_rx) = unbounded_channel();
    let (right_tx, mut right_rx) = unbounded_channel();

    spawn(async move {
        tokio::pin!(input);

        while let Ok(Some(item)) = input.try_next().await {
            if let Err(_error) = left_tx.send(item.clone()) {
                // TODO: Don't silently discard errors here!
                break;
            };
            if let Err(_error) = right_tx.send(item) {
                break;
            };
        }
    });

    let left = try_stream! {
        while let Some(item) = left_rx.recv().await {
                yield item;
        }
    };

    let right = try_stream! {
        while let Some(item) = right_rx.recv().await {
                yield item;
        }
    };

    (left, right)
}
