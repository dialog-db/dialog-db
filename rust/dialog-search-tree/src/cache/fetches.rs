use std::{collections::HashMap, hash::Hash, sync::Arc};

use parking_lot::Mutex;
use tokio::sync::broadcast;

/// What kind of read took a claim out, which decides who may wait on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// A read someone is awaiting. Its owner polls it for as long as it
    /// is in flight, so a waiter is guaranteed an outcome.
    Demand,
    /// A read-ahead nobody awaits. It advances only while the walk that
    /// queued it is polled, so a waiter outside that walk could wait on an
    /// outcome that only its own polling would ever produce.
    Speculative,
}

/// A claim's outcome channel. A fetch starts [`Outcome::Claimed`] — a bare
/// marker, nothing allocated — and is upgraded to [`Outcome::Awaited`] the
/// moment a second caller arrives and needs a channel to wait on. A fetch
/// nobody waits on never pays for one.
enum Outcome<V> {
    /// Nobody is waiting on it yet.
    Claimed,
    /// Callers are subscribed to its outcome.
    Awaited(broadcast::Sender<Option<V>>),
}

/// A claim in the registry.
///
/// The `id` is what lets a claim be superseded: a demand read that finds a
/// speculative claim replaces it with its own, and the superseded owner's
/// later publish or withdrawal — carrying the old id — touches nothing.
struct Entry<V> {
    id: u64,
    kind: Kind,
    outcome: Outcome<V>,
}

struct Claims<K, V> {
    next_id: u64,
    entries: HashMap<K, Entry<V>>,
}

/// The fetches that are currently in flight for a [`Cache`](super::Cache),
/// keyed exactly like the cache they front.
///
/// A miss that finds no claim for its key takes one out and performs the
/// fetch. A concurrent miss for the same key finds that claim and, when its
/// owner is guaranteed to drive it, waits on its outcome instead of issuing
/// a second fetch of its own. This matters most where a miss is expensive
/// and shared: a cold tree read walks the same upper nodes for every query
/// descending it at that moment.
///
/// The registry only ever holds claims that are still in flight. Publishing
/// an outcome, failing, and dropping a claim all withdraw it, so a key is
/// never left pointing at a fetch that no longer exists.
pub(super) struct Fetches<K, V>
where
    K: Eq + Hash,
{
    claims: Arc<Mutex<Claims<K, V>>>,
}

impl<K, V> Clone for Fetches<K, V>
where
    K: Eq + Hash,
{
    fn clone(&self) -> Self {
        Self {
            claims: self.claims.clone(),
        }
    }
}

impl<K, V> Fetches<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// An empty registry.
    pub fn new() -> Self {
        Self {
            claims: Arc::new(Mutex::new(Claims {
                next_id: 0,
                entries: HashMap::new(),
            })),
        }
    }

    /// Take out a claim of `kind` on `key`, or learn of the claim already in
    /// flight for it.
    pub fn claim(&self, key: &K, kind: Kind) -> Claim<K, V> {
        let mut claims = self.claims.lock();

        match claims.entries.get_mut(key) {
            None => {
                let id = claims.next_id;
                claims.next_id += 1;
                claims.entries.insert(
                    key.clone(),
                    Entry {
                        id,
                        kind,
                        outcome: Outcome::Claimed,
                    },
                );
                Claim::Ours(Fetch {
                    key: key.clone(),
                    id,
                    claims: self.claims.clone(),
                    withdrawn: false,
                })
            }
            Some(entry) => {
                let outcome = match &mut entry.outcome {
                    // First waiter: give the claim its outcome channel.
                    Outcome::Claimed => {
                        let (outcome, receiver) = broadcast::channel(1);
                        entry.outcome = Outcome::Awaited(outcome);
                        receiver
                    }
                    Outcome::Awaited(outcome) => outcome.subscribe(),
                };
                Claim::InFlight {
                    outcome,
                    kind: entry.kind,
                }
            }
        }
    }

    /// Replace whatever claim is on `key` with a demand claim of our own.
    ///
    /// For a demand read that found a speculative claim: it must not wait on
    /// a fetch that may never be driven, so it fetches for itself and takes
    /// the key over. The superseded owner keeps fetching in the background —
    /// nothing can stop it — but its outcome no longer lands on this key.
    /// Whoever was waiting on it is sent back to the start by the dropped
    /// sender, and joins the demand claim instead.
    pub fn supersede(&self, key: &K) -> Fetch<K, V> {
        let mut claims = self.claims.lock();
        let id = claims.next_id;
        claims.next_id += 1;
        claims.entries.insert(
            key.clone(),
            Entry {
                id,
                kind: Kind::Demand,
                outcome: Outcome::Claimed,
            },
        );
        Fetch {
            key: key.clone(),
            id,
            claims: self.claims.clone(),
            withdrawn: false,
        }
    }

    /// The number of fetches currently in flight.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.claims.lock().entries.len()
    }

    /// Whether the in-flight fetch for `key` has waiters (and hence an
    /// allocated outcome channel).
    #[cfg(test)]
    pub fn awaited(&self, key: &K) -> bool {
        matches!(
            self.claims.lock().entries.get(key),
            Some(Entry {
                outcome: Outcome::Awaited(_),
                ..
            })
        )
    }
}

/// The outcome of claiming a key in a [`Fetches`] registry.
pub(super) enum Claim<K, V>
where
    K: Eq + Hash,
{
    /// No fetch was in flight for the key, so this caller performs it.
    Ours(Fetch<K, V>),
    /// A fetch was already in flight; this receiver carries its outcome,
    /// and its kind says whether waiting on it is sound.
    InFlight {
        outcome: broadcast::Receiver<Option<V>>,
        kind: Kind,
    },
}

/// A claim on the fetch for one key, held for as long as that fetch is in
/// flight.
///
/// Dropping the claim without [`publish`](Self::publish)ing withdraws it and
/// closes the outcome channel (if any waiter opened one), which is how a
/// failed or abandoned fetch tells its waiters to start over instead of
/// leaving them attached to a fetch that will never complete.
pub(super) struct Fetch<K, V>
where
    K: Eq + Hash,
{
    key: K,
    id: u64,
    claims: Arc<Mutex<Claims<K, V>>>,
    withdrawn: bool,
}

impl<K, V> Fetch<K, V>
where
    K: Eq + Hash,
{
    /// Withdraw this claim — unless the key has since been taken over by a
    /// newer claim, which is then left exactly as it is.
    fn withdraw(&mut self) -> Option<Outcome<V>> {
        if self.withdrawn {
            return None;
        }
        self.withdrawn = true;
        let mut claims = self.claims.lock();
        match claims.entries.get(&self.key) {
            Some(entry) if entry.id == self.id => {
                claims.entries.remove(&self.key).map(|entry| entry.outcome)
            }
            _ => None,
        }
    }
}

impl<K, V> Fetch<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    /// Hand the fetched value to everyone waiting on this claim.
    ///
    /// The value is only cloned if someone is actually waiting; the common
    /// uncontended fetch publishes to nobody and pays nothing.
    pub fn publish(mut self, value: &Option<V>) {
        // Withdraw first so a caller arriving after the send takes out a fresh
        // claim rather than subscribing to a channel that has already spoken.
        if let Some(Outcome::Awaited(outcome)) = self.withdraw() {
            let _ = outcome.send(value.clone());
        }
    }
}

impl<K, V> Drop for Fetch<K, V>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        // Removing an `Awaited` entry drops its sender, which closes the
        // channel and sends every waiter back to the start.
        drop(self.withdraw());
    }
}
