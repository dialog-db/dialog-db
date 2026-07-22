//! Cache of redeemed access-service permits.
//!
//! Every remote effect used to POST its UCAN invocation to the access
//! service and receive a fresh presigned URL, even though presigned URLs
//! stay valid for an hour — on a periodically syncing replica the redeem
//! round-trip doubled the cost of every idle poll. A GET permit addresses
//! a stable (endpoint, capability) pair, so it is cached here and reused
//! for [`PERMIT_TTL_SECONDS`]. Mutating permits (PUT/DELETE) can bind
//! payload-specific signing material, so they are never cached.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use chrono::{DateTime, TimeDelta, Utc};
use dialog_remote_s3::Permit;

/// How long a redeemed GET permit is reused before redeeming afresh.
/// Well under the service's hour-long presign validity, so a cached
/// permit is never presented close to its expiry.
pub const PERMIT_TTL_SECONDS: i64 = 300;

/// Cache key: access-service endpoint + the dag-cbor bytes of the
/// capability the permit was redeemed for.
pub type PermitKey = (String, Vec<u8>);

struct Entry {
    permit: Permit,
    expires_at: DateTime<Utc>,
}

/// TTL cache of redeemed GET permits, keyed by [`PermitKey`].
#[derive(Default)]
pub struct PermitCache {
    entries: Mutex<HashMap<PermitKey, Entry>>,
}

impl PermitCache {
    /// An empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// The process-wide cache the providers share.
    pub fn shared() -> &'static PermitCache {
        static CACHE: OnceLock<PermitCache> = OnceLock::new();
        CACHE.get_or_init(PermitCache::new)
    }

    /// The cached permit for `key`, unless it has passed its TTL.
    pub fn lookup(&self, key: &PermitKey, now: DateTime<Utc>) -> Option<Permit> {
        let entries = self.entries.lock().ok()?;
        let entry = entries.get(key)?;
        (now < entry.expires_at).then(|| entry.permit.clone())
    }

    /// Cache `permit` under `key`. Non-GET permits are dropped: a
    /// mutating presign can be payload-specific, so reuse is unsound.
    pub fn store(&self, key: PermitKey, permit: &Permit, now: DateTime<Utc>) {
        if permit.method != "GET" {
            return;
        }
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        // Opportunistic sweep keeps the map bounded by the working set.
        entries.retain(|_, entry| now < entry.expires_at);
        entries.insert(
            key,
            Entry {
                permit: permit.clone(),
                expires_at: now + TimeDelta::seconds(PERMIT_TTL_SECONDS),
            },
        );
    }

    /// Drop the entry for `key`, so the next redeem goes to the service.
    pub fn invalidate(&self, key: &PermitKey) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(key);
        }
    }
}

use dialog_capability::{Capability, Constraint, Effect, ForkInvocation, Provider};
use dialog_remote_s3::{S3, S3Error, S3Invocation};

use crate::site::{UcanAddress, UcanAuthorization, UcanSite};

/// Redeem `authorization` for a permit, reusing a cached GET permit for
/// the same (endpoint, capability) when one is still fresh. Returns the
/// permit together with its cache key so the caller can
/// [`invalidate`](PermitCache::invalidate) on a downstream failure.
pub async fn redeem_cached<Fx>(
    authorization: &UcanAuthorization,
    address: &UcanAddress,
    capability: &Capability<Fx>,
) -> Result<(Permit, PermitKey), S3Error>
where
    Fx: Effect,
    Fx::Of: Constraint,
    Capability<Fx>: serde::Serialize,
{
    let capability_bytes = serde_ipld_dagcbor::to_vec(capability)
        .map_err(|e| S3Error::Authorization(e.to_string()))?;
    let key: PermitKey = (address.endpoint().to_string(), capability_bytes);
    let now = DateTime::<Utc>::from(dialog_common::time::now());
    if let Some(permit) = PermitCache::shared().lookup(&key, now) {
        return Ok((permit, key));
    }
    let permit = authorization.redeem(address).await?;
    PermitCache::shared().store(key.clone(), &permit, now);
    Ok((permit, key))
}

/// Execute a UCAN fork invocation via the cached-redeem path shared by
/// every effect provider in this crate: redeem (or reuse) a permit for
/// the invocation's `(address, capability)`, then invoke it against S3.
///
/// A permit that fails downstream may be stale — revoked or expired
/// server-side even though this process still thinks it has time left
/// on the TTL — so a failed result invalidates the cache entry, forcing
/// the next attempt to redeem afresh instead of retrying the same
/// bad permit.
pub async fn execute_cached<Fx, T, E>(invocation: ForkInvocation<UcanSite, Fx>) -> Result<T, E>
where
    Fx: Effect<Output = Result<T, E>>,
    Fx::Of: Constraint,
    Capability<Fx>: serde::Serialize,
    S3: Provider<S3Invocation<Fx>>,
    E: From<S3Error>,
{
    let (permit, key) = redeem_cached(
        &invocation.authorization,
        &invocation.address,
        &invocation.capability,
    )
    .await?;
    let result = permit.invoke(invocation.capability).perform(&S3).await;
    if result.is_err() {
        // A permit that failed downstream may be stale (revoked or
        // expired server-side); drop it so the next attempt redeems.
        PermitCache::shared().invalidate(&key);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test_configure;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test_configure!(run_in_browser);

    fn get_permit() -> Permit {
        Permit {
            url: "https://bucket.example/key?X-Amz-Signature=abc"
                .parse()
                .unwrap(),
            method: "GET".to_string(),
            headers: vec![],
        }
    }

    fn put_permit() -> Permit {
        Permit {
            method: "PUT".to_string(),
            ..get_permit()
        }
    }

    fn key(endpoint: &str, capability: &[u8]) -> PermitKey {
        (endpoint.to_string(), capability.to_vec())
    }

    #[dialog_common::test]
    fn it_returns_a_cached_permit_before_expiry() {
        let cache = PermitCache::new();
        let now = chrono::Utc::now();
        let k = key("https://access.example/ucan/", b"cap-a");
        cache.store(k.clone(), &get_permit(), now);
        let hit = cache.lookup(&k, now + TimeDelta::seconds(PERMIT_TTL_SECONDS - 1));
        assert_eq!(hit.map(|p| p.method), Some("GET".to_string()));
    }

    #[dialog_common::test]
    fn it_expires_a_permit_after_its_ttl() {
        let cache = PermitCache::new();
        let now = chrono::Utc::now();
        let k = key("https://access.example/ucan/", b"cap-a");
        cache.store(k.clone(), &get_permit(), now);
        assert!(
            cache
                .lookup(&k, now + TimeDelta::seconds(PERMIT_TTL_SECONDS))
                .is_none()
        );
    }

    #[dialog_common::test]
    fn it_keys_permits_by_endpoint_and_capability() {
        let cache = PermitCache::new();
        let now = chrono::Utc::now();
        cache.store(key("https://a.example/", b"cap-a"), &get_permit(), now);
        assert!(
            cache
                .lookup(&key("https://a.example/", b"cap-b"), now)
                .is_none()
        );
        assert!(
            cache
                .lookup(&key("https://b.example/", b"cap-a"), now)
                .is_none()
        );
    }

    #[dialog_common::test]
    fn it_never_stores_a_mutating_permit() {
        let cache = PermitCache::new();
        let now = chrono::Utc::now();
        let k = key("https://access.example/ucan/", b"cap-a");
        cache.store(k.clone(), &put_permit(), now);
        assert!(cache.lookup(&k, now).is_none());
    }

    #[dialog_common::test]
    fn it_invalidates_a_permit_on_demand() {
        let cache = PermitCache::new();
        let now = chrono::Utc::now();
        let k = key("https://access.example/ucan/", b"cap-a");
        cache.store(k.clone(), &get_permit(), now);
        cache.invalidate(&k);
        assert!(cache.lookup(&k, now).is_none());
    }
}
