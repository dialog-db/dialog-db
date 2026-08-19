//! A caching [`Resolve`](crate::Resolve) provider.
//!
//! Caching is a provider concern, not a concern of the [`Resolve`] effect or
//! its caller. [`CachingResolver`] wraps any other `Provider<Resolve>` and
//! memoizes `did -> verifier` with a time-to-live, and briefly remembers a
//! failure so a broken DID does not trigger a fetch on every verification.
//! Because it is just another provider, `Resolve::new(did).perform(&env)` is
//! unchanged whether or not a cache sits underneath.
//!
//! # Why the cache is bounded
//!
//! The key is the DID being resolved, and the authorizer resolves the issuer
//! DID of any submitted invocation. So an unauthenticated party chooses cache
//! keys. Worse, the cheapest keys to supply are the ones that *fail*: a DID
//! whose host is malformed is refused by URL derivation without a single
//! network call, and the refusal is then stored as a negative entry. An
//! unbounded map therefore grows at attacker request for free, which is a
//! memory-exhaustion vector against the server doing the authorizing.
//!
//! [`MAX_ENTRIES`] caps it. On insert into a full map, expired entries are
//! dropped first (they are dead weight and cost nothing to lose); if that does
//! not free room, the entry nearest expiry is evicted, so the survivors are the
//! ones with the most remaining usefulness. Eviction only ever costs a refetch,
//! never correctness.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use web_time::Instant;

use crate::error::ResolveError;
use crate::resolve::Resolve;
use crate::verifier::MultiVerifier;

/// The default time-to-live for a successful resolution.
pub const DEFAULT_TTL: Duration = Duration::from_secs(300);

/// The default time-to-live for a cached failure.
pub const DEFAULT_NEGATIVE_TTL: Duration = Duration::from_secs(30);

/// The most entries a [`CachingResolver`] will hold.
///
/// The keys are attacker-supplied DIDs (see the module docs), so the map must
/// not grow without bound. This is large enough that a real deployment's
/// working set of identities never evicts, and small enough that a hostile
/// caller cannot turn the cache into a memory-exhaustion vector.
pub const MAX_ENTRIES: usize = 4096;

struct Entry {
    result: Result<MultiVerifier, ResolveError>,
    expires_at: Instant,
}

/// Wraps a [`Resolve`](crate::Resolve) provider with an in-memory TTL cache.
pub struct CachingResolver<P> {
    inner: P,
    ttl: Duration,
    negative_ttl: Duration,
    max_entries: usize,
    entries: Mutex<HashMap<String, Entry>>,
}

impl<P> CachingResolver<P> {
    /// Wrap `inner` with the default TTLs.
    #[must_use]
    pub fn new(inner: P) -> Self {
        Self::with_ttls(inner, DEFAULT_TTL, DEFAULT_NEGATIVE_TTL)
    }

    /// Wrap `inner` with explicit success and failure TTLs, and the default
    /// [`MAX_ENTRIES`] bound.
    #[must_use]
    pub fn with_ttls(inner: P, ttl: Duration, negative_ttl: Duration) -> Self {
        Self::with_ttls_and_capacity(inner, ttl, negative_ttl, MAX_ENTRIES)
    }

    /// Wrap `inner` with explicit TTLs and an explicit entry bound.
    ///
    /// `max_entries` is clamped to at least 1: a zero-capacity cache would
    /// evict every entry it just stored, which is a misconfiguration rather
    /// than a useful mode.
    #[must_use]
    pub fn with_ttls_and_capacity(
        inner: P,
        ttl: Duration,
        negative_ttl: Duration,
        max_entries: usize,
    ) -> Self {
        Self {
            inner,
            ttl,
            negative_ttl,
            max_entries: max_entries.max(1),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// How many entries the cache currently holds.
    ///
    /// Never exceeds the configured bound (see [`MAX_ENTRIES`]); exposed so a
    /// caller can observe cache pressure, and so the bound itself is testable.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Is the cache empty?
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn cached(&self, did: &str) -> Option<Result<MultiVerifier, ResolveError>> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        match entries.get(did) {
            Some(entry) if entry.expires_at > Instant::now() => Some(entry.result.clone()),
            Some(_) => {
                entries.remove(did);
                None
            }
            None => None,
        }
    }

    fn store(&self, did: String, result: &Result<MultiVerifier, ResolveError>) {
        let ttl = if result.is_ok() {
            self.ttl
        } else {
            self.negative_ttl
        };
        let now = Instant::now();
        let entry = Entry {
            result: result.clone(),
            expires_at: now + ttl,
        };

        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());

        // Only an insert of a *new* key can grow the map, so overwriting an
        // existing entry never needs to make room.
        if entries.len() >= self.max_entries && !entries.contains_key(&did) {
            // Expired entries are dead weight: drop them before evicting
            // anything still live.
            entries.retain(|_, e| e.expires_at > now);

            // Still full: evict the entry closest to expiring, which is the
            // one with the least remaining value.
            while entries.len() >= self.max_entries {
                let Some(soonest) = entries
                    .iter()
                    .min_by_key(|(_, e)| e.expires_at)
                    .map(|(k, _)| k.clone())
                else {
                    break;
                };
                entries.remove(&soonest);
            }
        }

        entries.insert(did, entry);
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<P> Provider<Resolve> for CachingResolver<P>
where
    P: Provider<Resolve> + ConditionalSync,
{
    async fn execute(&self, input: Resolve) -> Result<MultiVerifier, ResolveError> {
        let key = input.did.as_str().to_string();
        if let Some(hit) = self.cached(&key) {
            return hit;
        }
        let result = self.inner.execute(input).await;
        self.store(key, &result);
        result
    }
}
