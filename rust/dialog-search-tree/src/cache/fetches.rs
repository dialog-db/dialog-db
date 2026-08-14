use std::{collections::HashMap, hash::Hash, sync::Arc};

use parking_lot::Mutex;
use tokio::sync::broadcast;

/// A claim's presence in the registry. A fetch starts [`Entry::Claimed`] —
/// a bare marker, nothing allocated — and is upgraded to [`Entry::Awaited`]
/// the moment a second caller arrives and needs a channel to wait on. A
/// fetch nobody waits on never pays for one.
enum Entry<V> {
    /// A fetch is in flight; nobody is waiting on it yet.
    Claimed,
    /// A fetch is in flight and callers are subscribed to its outcome.
    Awaited(broadcast::Sender<Option<V>>),
}

type Claims<K, V> = HashMap<K, Entry<V>>;

/// The fetches that are currently in flight for a [`Cache`](super::Cache),
/// keyed exactly like the cache they front.
///
/// A miss that finds no claim for its key takes one out and performs the
/// fetch. A concurrent miss for the same key finds that claim and waits on its
/// outcome instead of issuing a second fetch of its own. This matters most
/// where a miss is expensive and shared: a cold tree read walks the same upper
/// nodes for every query descending it at that moment.
///
/// The registry only ever holds claims that are still in flight. Publishing an
/// outcome, failing, and dropping a claim all withdraw it, so a key is never
/// left pointing at a fetch that no longer exists.
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
            claims: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Take out the claim on `key`, or subscribe to the claim already in
    /// flight for it.
    pub fn claim(&self, key: &K) -> Claim<K, V> {
        let mut claims = self.claims.lock();

        match claims.get_mut(key) {
            None => {
                claims.insert(key.clone(), Entry::Claimed);
                Claim::Ours(Fetch {
                    key: key.clone(),
                    claims: self.claims.clone(),
                    withdrawn: false,
                })
            }
            Some(entry) => match entry {
                // First waiter: give the claim its outcome channel.
                Entry::Claimed => {
                    let (outcome, receiver) = broadcast::channel(1);
                    *entry = Entry::Awaited(outcome);
                    Claim::InFlight(receiver)
                }
                Entry::Awaited(outcome) => Claim::InFlight(outcome.subscribe()),
            },
        }
    }

    /// The number of fetches currently in flight.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.claims.lock().len()
    }

    /// Whether the in-flight fetch for `key` has waiters (and hence an
    /// allocated outcome channel).
    #[cfg(test)]
    pub fn awaited(&self, key: &K) -> bool {
        matches!(self.claims.lock().get(key), Some(Entry::Awaited(_)))
    }
}

/// The outcome of claiming a key in a [`Fetches`] registry.
pub(super) enum Claim<K, V>
where
    K: Eq + Hash,
{
    /// No fetch was in flight for the key, so this caller performs it.
    Ours(Fetch<K, V>),
    /// A fetch was already in flight; this receiver carries its outcome.
    InFlight(broadcast::Receiver<Option<V>>),
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
    claims: Arc<Mutex<Claims<K, V>>>,
    withdrawn: bool,
}

impl<K, V> Fetch<K, V>
where
    K: Eq + Hash,
{
    fn withdraw(&mut self) -> Option<Entry<V>> {
        if self.withdrawn {
            return None;
        }
        self.withdrawn = true;
        self.claims.lock().remove(&self.key)
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
        if let Some(Entry::Awaited(outcome)) = self.withdraw() {
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
