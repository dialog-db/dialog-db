//! A caching [`Resolve`](crate::Resolve) provider.
//!
//! Caching is a provider concern, not a concern of the [`Resolve`] effect or
//! its caller. [`CachingResolver`] wraps any other `Provider<Resolve>` and
//! memoizes `did -> verifier` with a time-to-live, and briefly remembers a
//! failure so a broken DID does not trigger a fetch on every verification.
//! Because it is just another provider, `Resolve::new(did).perform(&env)` is
//! unchanged whether or not a cache sits underneath.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_credentials::Verifier;
use web_time::Instant;

use crate::error::ResolveError;
use crate::resolve::Resolve;

/// The default time-to-live for a successful resolution.
pub const DEFAULT_TTL: Duration = Duration::from_secs(300);

/// The default time-to-live for a cached failure.
pub const DEFAULT_NEGATIVE_TTL: Duration = Duration::from_secs(30);

struct Entry {
    result: Result<Verifier, ResolveError>,
    expires_at: Instant,
}

/// Wraps a [`Resolve`](crate::Resolve) provider with an in-memory TTL cache.
pub struct CachingResolver<P> {
    inner: P,
    ttl: Duration,
    negative_ttl: Duration,
    entries: Mutex<HashMap<String, Entry>>,
}

impl<P> CachingResolver<P> {
    /// Wrap `inner` with the default TTLs.
    #[must_use]
    pub fn new(inner: P) -> Self {
        Self::with_ttls(inner, DEFAULT_TTL, DEFAULT_NEGATIVE_TTL)
    }

    /// Wrap `inner` with explicit success and failure TTLs.
    #[must_use]
    pub fn with_ttls(inner: P, ttl: Duration, negative_ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            negative_ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn cached(&self, did: &str) -> Option<Result<Verifier, ResolveError>> {
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

    fn store(&self, did: String, result: &Result<Verifier, ResolveError>) {
        let ttl = if result.is_ok() {
            self.ttl
        } else {
            self.negative_ttl
        };
        let entry = Entry {
            result: result.clone(),
            expires_at: Instant::now() + ttl,
        };
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(did, entry);
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<P> Provider<Resolve> for CachingResolver<P>
where
    P: Provider<Resolve> + ConditionalSync,
{
    async fn execute(&self, input: Resolve) -> Result<Verifier, ResolveError> {
        let key = input.did.as_str().to_string();
        if let Some(hit) = self.cached(&key) {
            return hit;
        }
        let result = self.inner.execute(input).await;
        self.store(key, &result);
        result
    }
}
