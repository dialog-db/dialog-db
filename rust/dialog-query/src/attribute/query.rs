/// Base EAV scan query (Cardinality::Many).
pub mod all;
/// Type-erased attribute query dispatching between cardinality variants.
pub mod dynamic;
/// Winner-selecting attribute query (Cardinality::One).
pub mod only;
/// Typed attribute query wrapping a single `Attribute` type.
pub mod typed;

pub use dynamic::DynamicAttributeQuery;
pub use dynamic::DynamicAttributeQuery as AttributeQuery;
pub use typed::StaticAttributeQuery;

use std::collections::VecDeque;
use std::future::poll_fn;
use std::task::Poll;

use async_stream::try_stream;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{ArtifactSelector, Likelihood, Preload, PreloadRequest};
use dialog_capability::Provider;
use dialog_common::{ConditionalSend, ConditionalSync};
use futures_util::StreamExt as _;

use crate::selection::{Match, Selection};

/// How many upstream rows a premise buffers ahead of the one it is
/// probing, so the probes those rows will issue can replicate while the
/// current probe is awaited. Sixteen matches the read-ahead widths the
/// tree layer already uses.
pub(crate) const PROBE_LOOKAHEAD: usize = 256;

/// Wrap a premise's upstream selection with probe pipelining: rows are
/// buffered up to [`PROBE_LOOKAHEAD`] ahead, and each newly buffered
/// row's would-be probe (as `probe` describes it) is offered to the env
/// as a [`Preload`] hint, so on a cold replica the blocks rows
/// `i+1..i+16` will demand replicate while row `i`'s probe is awaited.
///
/// The rows themselves flow through unchanged and in order. Hinting
/// stops permanently the first time the env reports nobody is listening
/// (no plan attached), so an un-staged query pays nothing beyond that
/// first refusal; a `probe` of `None` (a row this premise filters, or a
/// shape not worth preloading) buffers without hinting. A row entering
/// an empty window is never hinted: it is the next row to be probed, so
/// its hint could overlap with nothing and would only duplicate the
/// demand read (a single-row upstream — a query's seed — hints nothing
/// at all).
pub(crate) fn pipelined<'a, Env, M, P>(selection: M, env: &'a Env, probe: P) -> impl Selection + 'a
where
    M: Selection + 'a,
    Env: Provider<Preload> + ConditionalSync,
    P: Fn(&Match) -> Option<ArtifactSelector<Constrained>> + ConditionalSend + 'a,
{
    try_stream! {
        let mut selection = Box::pin(selection);
        let mut window: VecDeque<Match> = VecDeque::with_capacity(PROBE_LOOKAHEAD);
        let mut listening = true;
        let mut exhausted = false;
        loop {
            // Top the window up. The first pull may wait (there is
            // nothing to yield anyway); every further pull is
            // non-blocking, because parking here on a stalled upstream
            // would starve the rows already buffered — and their hints —
            // behind an await they do not need.
            while listening && !exhausted && window.len() < PROBE_LOOKAHEAD {
                let candidate = if window.is_empty() {
                    selection.next().await
                } else {
                    let Some(candidate) = poll_now(&mut selection).await else {
                        break;
                    };
                    candidate
                };
                let Some(candidate) = candidate else {
                    exhausted = true;
                    break;
                };
                let base = candidate?;
                if !window.is_empty()
                    && let Some(selector) = probe(&base)
                {
                    listening = Provider::<Preload>::execute(
                        env,
                        PreloadRequest {
                            selector,
                            likelihood: Likelihood::Likely,
                        },
                    )
                    .await;
                }
                window.push_back(base);
            }
            let base = match window.pop_front() {
                Some(base) => base,
                None => {
                    if exhausted {
                        break;
                    }
                    match selection.next().await {
                        Some(candidate) => candidate?,
                        None => break,
                    }
                }
            };
            yield base;
        }
    }
}

/// One non-blocking pull: `Some(item)` when the stream is ready right
/// now, `None` when it is not (its waker stays registered, so progress
/// resumes on a later poll).
async fn poll_now<S>(stream: &mut S) -> Option<Option<S::Item>>
where
    S: futures_util::Stream + Unpin,
{
    poll_fn(|context| match stream.poll_next_unpin(context) {
        Poll::Ready(item) => Poll::Ready(Some(item)),
        Poll::Pending => Poll::Ready(None),
    })
    .await
}
