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
    use dialog_capability::{Principal, Subject, did};
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::archive::{Archive, Catalog, Get};
    use dialog_ucan::UcanInvocation;
    use dialog_ucan_core::{InvocationBuilder, InvocationChain};
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

    // -- invalidate-on-failure coverage --
    //
    // `execute_cached` invalidates a permit's cache entry when the S3
    // request it guards returns `Err`. That branch isn't reachable
    // through the primitives above (`store`/`lookup`/`invalidate`) --
    // it only fires by driving a real `ForkInvocation` through
    // `execute_cached`, which always executes against the real `S3`
    // provider and the process-wide `PermitCache::shared()` (neither is
    // injectable). So this test builds a real invocation and forces the
    // downstream request to fail by pointing the permit at a loopback
    // port nothing listens on -- deterministic and network-free (no
    // live access service or S3 endpoint needed), rather than reaching
    // for a mock HTTP layer.
    //
    // This is the only test in the crate that touches
    // `PermitCache::shared()`; every other test uses `PermitCache::new()`,
    // so there's nothing here to race or leak state into.

    /// A self-signed (issuer == subject, no delegation) UCAN
    /// authorization. Enough to satisfy `ForkInvocation`'s type, but
    /// never actually redeemed here: the cache is pre-seeded so
    /// `execute_cached` never reaches `authorization.redeem`.
    async fn self_authorization(signer: &Ed25519Signer) -> UcanAuthorization {
        let did = signer.did();
        let invocation = InvocationBuilder::new()
            .issuer(signer.clone())
            .audience(&did)
            .subject(&did)
            .command(vec!["archive".to_string(), "get".to_string()])
            .proofs(vec![])
            .try_build()
            .await
            .expect("failed to build self-signed invocation");
        let chain = InvocationChain::new(invocation, std::collections::HashMap::new());
        UcanInvocation {
            chain: Box::new(chain),
            subject: did,
            ability: "/archive/get".to_string(),
        }
        .into()
    }

    /// A permit pointing at a loopback port nothing listens on: the S3
    /// request it guards fails fast with a connection error.
    fn unreachable_permit() -> Permit {
        Permit {
            url: "http://127.0.0.1:1/unreachable".parse().expect("valid url"),
            method: "GET".to_string(),
            headers: vec![],
        }
    }

    #[dialog_common::test]
    async fn it_invalidates_the_cache_entry_after_a_failed_request() {
        let signer = Ed25519Signer::import(&[9u8; 32]).await.unwrap();
        let capability = Subject::from(did!("key:zPermitCacheFailureTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("blobs"))
            .invoke(Get::new([0u8; 32]));
        let capability_bytes = serde_ipld_dagcbor::to_vec(&capability).unwrap();

        // Address is also unreachable, so a redeem attempted after
        // invalidation fails the same deterministic way.
        let address = UcanAddress::new("http://127.0.0.1:1/redeem");
        let cache_key: PermitKey = (address.endpoint().to_string(), capability_bytes);
        let now = chrono::Utc::now();

        // Prime the entry `execute_cached` will look up, so it reuses
        // this permit instead of redeeming.
        PermitCache::shared().store(cache_key.clone(), &unreachable_permit(), now);
        assert!(
            PermitCache::shared().lookup(&cache_key, now).is_some(),
            "precondition: cache should be primed before the call"
        );

        let authorization = self_authorization(&signer).await;
        let invocation =
            ForkInvocation::new(capability.clone(), address.clone(), authorization.clone());

        let result: Result<Option<Vec<u8>>, dialog_effects::archive::ArchiveError> =
            execute_cached(invocation).await;
        assert!(
            result.is_err(),
            "a request against an unreachable permit should fail"
        );
        assert!(
            PermitCache::shared().lookup(&cache_key, now).is_none(),
            "a failed request should invalidate its cache entry"
        );

        // And the invalidation should matter: the next redeem attempt
        // should hit the network (and fail against the same unreachable
        // address) rather than silently serving the stale permit back
        // out of the cache.
        let redeem_result = redeem_cached(&authorization, &address, &capability).await;
        assert!(
            redeem_result.is_err(),
            "after invalidation the next redeem should go to the network, \
             not return the stale cache entry"
        );
    }
}
