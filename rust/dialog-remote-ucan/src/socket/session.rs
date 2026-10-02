//! A service's side of one connection: the watches it began, and how it
//! answers the frames it receives and the changes it delivers.
//!
//! Holds no connection and no clock: whoever keeps the connection hands
//! each frame in and sends what comes back, and hands in each change to a
//! cell with the time it delivers it. The same session serves a loopback
//! server and a service that hibernates between messages, which keeps the
//! watches ([`Session::watches`]) and restores them
//! ([`Session::restore`]).

use std::collections::HashMap;
use std::time::Duration;

use dialog_capability::{Did, Provider};
use dialog_common::ConditionalSync;
use dialog_did_web::Resolve;
use dialog_effects::memory::CellState;
use dialog_ucan_core::revocation::RevocationChecker;
use dialog_ucan_core::{Container, InvocationChain};
use futures_util::future::join_all;

use super::{Reply, Request};
use crate::server::{Access, Answer, Issuance, Payload, Store, Subscription};

/// The command a watch invokes.
const WATCH: [&str; 5] = ["use", "get", "memory", "cell", "watch"];

/// The body a frame the service does not act on is answered with, in the
/// shape a client reads a rejection from.
const UNSUPPORTED: &str =
    r#"{"kind":"Unsupported","reason":"the service does not perform this invocation"}"#;

/// One connection's watches.
#[derive(Debug, Default)]
pub struct Session {
    watches: HashMap<String, Subscription>,
}

impl Session {
    /// A session with no watches.
    pub fn new() -> Self {
        Self::default()
    }

    /// A session holding `watches`, as [`Session::watches`] gave them.
    pub fn restore(watches: impl IntoIterator<Item = Subscription>) -> Self {
        Self {
            watches: watches
                .into_iter()
                .map(|watch| (watch.invocation().to_string(), watch))
                .collect(),
        }
    }

    /// The watches the session holds.
    pub fn watches(&self) -> impl Iterator<Item = &Subscription> {
        self.watches.values()
    }

    /// Answer the frame `bytes` carry: an invocation is answered as a
    /// request would be, a watch with the state of its cell, and a
    /// cancellation with nothing.
    pub async fn receive<P, Resolver, Revocations>(
        &mut self,
        access: &Access<P, Resolver, Revocations>,
        bytes: &[u8],
    ) -> Option<Reply>
    where
        P: Store,
        Resolver: Provider<Resolve> + ConditionalSync,
        Revocations: RevocationChecker + ConditionalSync,
    {
        let request = match Request::decode(bytes) {
            Ok(request) => request,
            Err(error) => return Some(malformed(None, error.to_string())),
        };
        let (container, payload) = match request {
            Request::Cancel { invocation } => {
                self.watches.remove(&invocation);
                return None;
            }
            Request::Invoke { container, payload } => (container, payload),
        };
        let container = match Container::from_bytes(&container) {
            Ok(container) => container,
            Err(error) => return Some(malformed(None, error.to_string())),
        };
        let chain = match InvocationChain::try_from(container.clone()) {
            Ok(chain) => chain,
            Err(error) => return Some(malformed(None, error.to_string())),
        };
        let invocation = chain.invocation.to_cid().to_string();
        let watch = chain.command().0.iter().map(String::as_str).eq(WATCH);
        if watch {
            return Some(match access.subscribe(container).await {
                Ok((subscription, state)) => {
                    self.watches.insert(invocation.clone(), subscription);
                    Reply::State { invocation, state }
                }
                Err(answer) => reply(Some(invocation), answer).await,
            });
        }
        let payload = Payload::Bytes(payload.unwrap_or_default());
        let answer = access.answer(container, payload, Issuance::Required).await;
        Some(reply(Some(invocation), answer).await)
    }

    /// Deliver that the cell `cell` in `space` of `subject` now holds
    /// `state`, at `at` (Unix seconds), to the watches that follow it.
    ///
    /// A watch whose authority was last found to hold more than `interval`
    /// before is checked again first (see [`Access::recheck`]), all of them
    /// at once; one whose authority ends is answered with why, and the
    /// session drops it.
    pub async fn deliver<P, Resolver, Revocations>(
        &mut self,
        access: &Access<P, Resolver, Revocations>,
        change: Change<'_>,
        interval: Duration,
        at: u64,
    ) -> Vec<Reply>
    where
        Resolver: Provider<Resolve> + ConditionalSync,
        Revocations: RevocationChecker + ConditionalSync,
    {
        // Every watch the change reaches is checked at once: a cell many
        // follow is not held up by checking each in turn.
        let checked = join_all(
            self.watches
                .iter_mut()
                .filter(|(_, watch)| watch.follows(change.subject, change.space, change.cell))
                .map(|(invocation, watch)| async move {
                    (
                        invocation.clone(),
                        access.recheck(watch, interval, at).await,
                    )
                }),
        )
        .await;
        let mut replies = Vec::new();
        let mut ended = Vec::new();
        for (invocation, outcome) in checked {
            match outcome {
                Ok(()) => replies.push(Reply::State {
                    invocation,
                    state: change.state.clone(),
                }),
                Err(refusal) => {
                    let response = refusal.into_response();
                    let body = response.body.collect().await.unwrap_or_default();
                    replies.push(Reply::Ended {
                        invocation: invocation.clone(),
                        status: response.status,
                        body,
                    });
                    ended.push(invocation);
                }
            }
        }
        for invocation in ended {
            self.watches.remove(&invocation);
        }
        replies
    }
}

/// A change to a cell: which cell, and what it now holds.
#[derive(Debug, Clone, Copy)]
pub struct Change<'a> {
    /// The subject the cell belongs to.
    pub subject: &'a Did,
    /// The space the cell is in.
    pub space: &'a str,
    /// The cell.
    pub cell: &'a str,
    /// What it now holds.
    pub state: &'a CellState,
}

fn malformed(invocation: Option<String>, detail: String) -> Reply {
    Reply::Answer {
        invocation,
        status: 400,
        version: None,
        body: serde_json::to_vec(&dialog_capability::access::AuthorizeError::Malformed { detail })
            .unwrap_or_default(),
    }
}

async fn reply(invocation: Option<String>, answer: Answer) -> Reply {
    let response = match answer {
        Answer::Performed(response) => response,
        Answer::Refused(refusal) => refusal.into_response(),
        Answer::Unsupported => {
            return Reply::Answer {
                invocation,
                status: 406,
                version: None,
                body: UNSUPPORTED.as_bytes().to_vec(),
            };
        }
    };
    let status = response.status;
    let version = response.version;
    let body = response.body.collect().await.unwrap_or_default();
    Reply::Answer {
        invocation,
        status,
        version,
        body,
    }
}
