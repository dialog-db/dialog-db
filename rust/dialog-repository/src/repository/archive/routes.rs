//! A branch's registration on the environment's routes: where its
//! subject's blocks hydrate from, as it tracks them.
//!
//! A block a replica holds by reference is not in its local archive, and
//! the archive cannot say where it is: a branch can, from what it tracks.
//! Each branch registers on the environment's [`Routes`] when it is
//! opened, so a read that misses locally is answered by the environment
//! (see [`load_or_hydrate`](super::load_or_hydrate)), from the remotes any
//! open branch of the subject tracks, without asking the branch on every
//! read where its remote is.
//!
//! Routes are per subject. Blocks are content-addressed and one catalog
//! holds a repository's, so any remote holding the subject can serve any
//! of its blocks: a miss tries the remotes every open branch of it tracks,
//! in the order they were opened, each branch's in the order it tracks
//! them (its pulls, then its pushes, then the peers it synced with once).
//!
//! What a branch tracks is kept in its tracking cell, which every sync,
//! resolution and refresh may move. Its registration follows the cell:
//! the routes are worked out again from the cell's contents when it is at
//! another version than they were worked out at, so no write to it needs
//! to re-register. They are worked out only on a local miss, which already
//! costs a round trip; a read the local archive answers asks nothing here.

use std::sync::{Arc, Mutex, Weak};

use dialog_capability::{Did, Subject};
use dialog_effects::memory::Version;
pub use dialog_network::{Loan, Route as HydrationRoute, Routes, Routing};

use crate::repository::branch::{Answers, Writer, fallbacks_of};
use crate::repository::memory::Cell;
use crate::{Branch, RemoteFallback, Tracking};

/// A branch's routes as last worked out, with the tracking version and
/// the count of connections they were worked out at.
type Memo = (Option<Version>, usize, Vec<HydrationRoute>);

/// A branch's registration: what it needs to say where its subject's
/// blocks live, and the answer as last worked out.
struct Registered {
    subject: Did,
    /// The branch's writer, shared by every handle of it: while one is
    /// alive the branch is open.
    open: Weak<Writer>,
    host: Subject,
    tracking: Cell<Tracking>,
    answers: Answers,
    /// The routes, and the tracking version and the count of connections
    /// they were worked out at.
    memo: Mutex<Option<Memo>>,
}

impl Registered {
    /// `answer` of the routes as the tracking cell now has them, worked out
    /// again only when the cell or the connections moved since.
    fn with_routes<R>(&self, answer: impl FnOnce(&[HydrationRoute]) -> R) -> R {
        let connections = self
            .answers
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len();
        let mut memo = self
            .memo
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let current = matches!(
            memo.as_ref(),
            Some((at, count, _)) if *count == connections && self.tracking.is_at(at.as_ref())
        );
        if !current {
            let tracked = self.tracking.content().unwrap_or_default();
            let routes: Vec<HydrationRoute> = fallbacks_of(&tracked, &self.host, &self.answers)
                .iter()
                .filter_map(route_of)
                .collect();
            *memo = Some((self.tracking.version(), connections, routes));
        }
        match memo.as_ref() {
            Some((_, _, routes)) => answer(routes),
            None => answer(&[]),
        }
    }
}

impl Routing for Registered {
    fn subject(&self) -> &Did {
        &self.subject
    }

    fn is_open(&self) -> bool {
        self.open.strong_count() > 0
    }

    fn routes(&self) -> Vec<HydrationRoute> {
        self.with_routes(<[HydrationRoute]>::to_vec)
    }

    fn fetches(&self) -> bool {
        self.with_routes(|routes| {
            routes
                .iter()
                .any(|route| matches!(route, HydrationRoute::Remote { .. }))
        })
    }
}

/// Register `branch` on `routes`: while a handle of it is open, a local
/// miss of its subject's blocks hydrates from what it tracks. Registering a
/// branch again (another handle of it, opened again) keeps its place.
pub(crate) fn register(routes: &Routes, branch: &Branch, writer: &Arc<Writer>) {
    let subject = branch.of().clone();
    let key = format!("{subject}:{}", branch.name());
    routes.register(
        key,
        Arc::new(Registered {
            subject,
            open: Arc::downgrade(writer),
            host: branch.subject(),
            tracking: branch.tracking().clone(),
            answers: branch.answers().clone(),
            memo: Mutex::new(None),
        }),
    );
}

/// Where `fallback` hydrates from, as a route: `None` for no remote.
pub(crate) fn route_of(fallback: &RemoteFallback) -> Option<HydrationRoute> {
    match fallback {
        RemoteFallback::None => None,
        RemoteFallback::Remote(remote) => {
            let address = remote.address();
            Some(HydrationRoute::Remote {
                address: address.address,
                subject: address.subject,
            })
        }
        RemoteFallback::Unavailable { remote, reason } => Some(HydrationRoute::Unavailable {
            remote: remote.clone(),
            reason: reason.clone(),
        }),
    }
}

/// Lend `fallback` to `routes` for `subject` until the loan is dropped (see
/// [`Routes::lend`]).
pub(crate) fn lend(routes: &Routes, subject: &Did, fallback: &RemoteFallback) -> Loan {
    routes.lend(subject, route_of(fallback))
}
