//! Telling a watch that the cell it follows may have changed.
//!
//! A peer's store answers what a cell holds and says nothing when that
//! changes, so whoever writes has to say so. [`Changes`] is where they
//! say it. [`Announcing`] wraps a store so that every publish and retract
//! performed through it is announced, whoever performs it: the
//! [`Responder`](crate::serve::Responder) serves through one, and an
//! embedder whose own process writes the same store writes through the
//! responder's [`store`](crate::serve::Responder::store) to be heard.
//!
//! # A change is a flag, not a value
//!
//! A notification carries no state, only that the cell is worth reading
//! again. Each follower holds one flag that any number of notifications
//! set and one read clears, so a cell that changes faster than its
//! watcher keeps up costs the watcher one read of the newest state
//! rather than a queue of stale ones. That read happens after the flag
//! is cleared, so a change that lands during it sets the flag again and
//! is read next: the last state the cell takes is never skipped.
//!
//! No runtime is involved — a follower is woken through its waker — so
//! this works wherever the responder does.

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Poll, Waker};

use dialog_capability::{Capability, Provider};
use dialog_effects::prelude::*;
use dialog_effects::{archive, blob, memory, peer};
use dialog_varsig::did::Did;

/// Where writes to a peer's cells are announced.
#[derive(Debug, Default)]
pub struct Changes {
    /// The followers of each cell, by subject, space and cell.
    followers: Mutex<HashMap<Key, Vec<Weak<Follower>>>>,
}

type Key = (Did, String, String);

#[derive(Debug)]
struct Follower {
    changed: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

/// One watch's interest in one cell. Dropping it stops the following.
#[derive(Debug)]
pub struct Following {
    follower: Arc<Follower>,
    key: Key,
    changes: Weak<Changes>,
}

impl Changes {
    /// Say that the cell `cell` in `space` of `subject` may hold something
    /// new, waking every watch that follows it.
    pub fn notify(&self, subject: &Did, space: &str, cell: &str) {
        let followers = self.followers.lock().unwrap_or_else(|p| p.into_inner());
        let key = (subject.clone(), space.to_string(), cell.to_string());
        for follower in followers.get(&key).into_iter().flatten() {
            let Some(follower) = follower.upgrade() else {
                continue;
            };
            follower.changed.store(true, Ordering::SeqCst);
            if let Some(waker) = follower
                .waker
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take()
            {
                waker.wake();
            }
        }
    }

    /// Start following the cell `cell` in `space` of `subject`.
    pub fn follow(self: &Arc<Self>, subject: Did, space: String, cell: String) -> Following {
        let follower = Arc::new(Follower {
            changed: AtomicBool::new(false),
            waker: Mutex::new(None),
        });
        let key = (subject, space, cell);
        self.followers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .entry(key.clone())
            .or_default()
            .push(Arc::downgrade(&follower));
        Following {
            follower,
            key,
            changes: Arc::downgrade(self),
        }
    }

    /// How many watches are following, across every cell.
    #[cfg(test)]
    pub(crate) fn following(&self) -> usize {
        self.followers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .map(Vec::len)
            .sum()
    }
}

impl Drop for Following {
    /// A watch that ends leaves its cell's followers, and a cell nobody
    /// follows leaves the map, so neither outgrows the live watches.
    fn drop(&mut self) {
        let Some(changes) = self.changes.upgrade() else {
            return;
        };
        let mut followers = changes.followers.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(cell) = followers.get_mut(&self.key) {
            let gone = Arc::downgrade(&self.follower);
            cell.retain(|weak| !weak.ptr_eq(&gone) && weak.strong_count() > 0);
            if cell.is_empty() {
                followers.remove(&self.key);
            }
        }
    }
}

impl Following {
    /// Wait until the cell has been announced as changed since the last
    /// time this returned, clearing the announcement.
    pub async fn changed(&self) {
        poll_fn(|context| {
            // The waker goes in before the flag is read, so a notify that
            // lands between the two either finds the waker or leaves the
            // flag for this read to find.
            *self
                .follower
                .waker
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(context.waker().clone());
            if self.follower.changed.swap(false, Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

/// A store whose publishes and retracts are announced to [`Changes`].
///
/// Every other effect passes straight through.
#[derive(Debug, Clone)]
pub struct Announcing<S> {
    store: S,
    changes: Arc<Changes>,
}

impl<S> Announcing<S> {
    /// Announce the writes to `store` through a [`Changes`] of its own.
    pub fn new(store: S) -> Self {
        Self::with_changes(store, Arc::new(Changes::default()))
    }

    /// Announce the writes to `store` through `changes`.
    pub fn with_changes(store: S, changes: Arc<Changes>) -> Self {
        Self { store, changes }
    }

    /// Where the writes are announced.
    pub fn changes(&self) -> &Arc<Changes> {
        &self.changes
    }

    /// The store underneath. Writes made to it directly are not announced.
    pub fn inner(&self) -> &S {
        &self.store
    }

    /// The store underneath, unwrapped.
    pub fn into_inner(self) -> S {
        self.store
    }
}

#[dialog_capability::provider]
impl<S> Provider<memory::Publish> for Announcing<S>
where
    S: Provider<memory::Publish> + dialog_common::ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<memory::Publish>,
    ) -> Result<memory::Version, memory::MemoryError> {
        let (subject, space, cell) = (
            input.subject().clone(),
            input.space().to_string(),
            input.cell().to_string(),
        );
        let outcome = self.store.execute(input).await;
        if outcome.is_ok() {
            self.changes.notify(&subject, &space, &cell);
        }
        outcome
    }
}

#[dialog_capability::provider]
impl<S> Provider<memory::Retract> for Announcing<S>
where
    S: Provider<memory::Retract> + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Capability<memory::Retract>) -> Result<(), memory::MemoryError> {
        let (subject, space, cell) = (
            input.subject().clone(),
            input.space().to_string(),
            input.cell().to_string(),
        );
        let outcome = self.store.execute(input).await;
        if outcome.is_ok() {
            self.changes.notify(&subject, &space, &cell);
        }
        outcome
    }
}

macro_rules! passes_through {
    ($($effect:ty => $output:ty),+ $(,)?) => {
        $(
            #[dialog_capability::provider]
            impl<S> Provider<$effect> for Announcing<S>
            where
                S: Provider<$effect> + dialog_common::ConditionalSync,
            {
                async fn execute(&self, input: Capability<$effect>) -> $output {
                    self.store.execute(input).await
                }
            }
        )+
    };
}

passes_through!(
    archive::Get => Result<Option<Vec<u8>>, archive::ArchiveError>,
    archive::Put => Result<(), archive::ArchiveError>,
    archive::Import => Result<(), archive::ArchiveError>,
    memory::Resolve => Result<memory::CellState, memory::MemoryError>,
    blob::Read => Result<blob::BlobReader, blob::BlobError>,
    blob::Write => Result<blob::BlobWriter, blob::BlobError>,
    blob::Import => Result<blob::BlobWriter, blob::BlobError>,
    peer::Hello => Result<peer::Greeting, peer::PeerError>,
    peer::Spaces => Result<Vec<peer::Offer>, peer::PeerError>,
);

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::did;

    #[dialog_common::test]
    async fn many_changes_wake_once_and_others_not_at_all() {
        let changes = Arc::new(Changes::default());
        let subject: Did = did!("key:zSubject");
        let following = changes.follow(subject.clone(), "local".into(), "head".into());
        let elsewhere = changes.follow(subject.clone(), "local".into(), "other".into());

        changes.notify(&subject, "local", "head");
        changes.notify(&subject, "local", "head");
        following.changed().await;

        assert!(
            !following.follower.changed.load(Ordering::SeqCst),
            "coalesced into one"
        );
        assert!(
            !elsewhere.follower.changed.load(Ordering::SeqCst),
            "another cell is not woken"
        );
    }

    #[dialog_common::test]
    fn a_dropped_follower_leaves_without_a_notify() {
        let changes = Arc::new(Changes::default());
        let subject: Did = did!("key:zSubject");
        let kept = changes.follow(subject.clone(), "local".into(), "head".into());
        for _ in 0..100 {
            drop(changes.follow(subject.clone(), "local".into(), "head".into()));
            drop(changes.follow(subject.clone(), "local".into(), "other".into()));
        }
        assert_eq!(changes.following(), 1, "only the live watch remains");
        drop(kept);
        assert!(changes.followers.lock().unwrap().is_empty());
    }
}
