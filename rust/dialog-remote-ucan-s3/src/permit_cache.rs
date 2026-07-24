//! Cache of redeemed access-service permits.
//!
//! Every remote effect used to POST its UCAN invocation to the access
//! service and receive a fresh presigned URL, even though presigned URLs
//! stay valid for an hour — on a periodically syncing replica the redeem
//! round-trip doubled the cost of every idle poll.
//!
//! A permit presigns one S3 object, so an entry is keyed by the
//! access-service endpoint and the object path — not by the capability
//! that produced it. That keeps the key payload-free (a `Put` capability
//! carries the whole block) and lets requests that differ only in
//! headers, such as ranged blob reads, share one permit.
//!
//! Only GET permits are reusable: a mutating presign can bind
//! payload-specific signing material. [`PermitKey::cacheable`] is the
//! sole constructor and returns `None` for anything else, so a mutating
//! permit cannot be stored — there is no key to store it under.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use dialog_capability::SiteId;
use dialog_remote_s3::Permit;
use dialog_remote_s3::request::S3Request;
use parking_lot::Mutex;

use crate::site::UcanAddress;

/// How long a redeemed permit is reused before redeeming afresh. Well
/// under the service's hour-long presign validity, so a cached permit is
/// never presented close to its expiry.
pub const PERMIT_TTL: Duration = Duration::from_secs(300);

/// Hard bound on retained entries. Keys are per-object, so a large read
/// sweep would otherwise retain a permit per block for the whole TTL.
const MAX_ENTRIES: usize = 512;

/// Cache key: access-service endpoint plus the S3 object path the permit
/// presigns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PermitKey {
    site: SiteId,
    path: String,
}

impl PermitKey {
    /// The key `request` is cached under at `address`, or `None` when the
    /// request is not cacheable. Only GET permits are reusable: a
    /// mutating presign can bind payload-specific signing material.
    pub fn cacheable(address: &UcanAddress, request: S3Request) -> Option<Self> {
        (request.method == "GET").then(|| Self {
            site: SiteId::from(address.clone()),
            path: request.path,
        })
    }
}

struct Entry {
    permit: Permit,
    expires_at: SystemTime,
}

/// TTL cache of redeemed permits, keyed by [`PermitKey`].
#[derive(Default)]
pub struct PermitCache {
    entries: Mutex<HashMap<PermitKey, Entry>>,
}

impl PermitCache {
    /// The process-wide cache the providers share.
    pub fn shared() -> &'static PermitCache {
        static CACHE: OnceLock<PermitCache> = OnceLock::new();
        CACHE.get_or_init(PermitCache::default)
    }

    /// The cached permit for `key`, unless it has passed its TTL.
    pub fn lookup(&self, key: &PermitKey, now: SystemTime) -> Option<Permit> {
        let entries = self.entries.lock();
        let entry = entries.get(key)?;
        (now < entry.expires_at).then(|| entry.permit.clone())
    }

    /// Cache `permit` under `key`.
    pub fn store(&self, key: PermitKey, permit: &Permit, now: SystemTime) {
        let mut entries = self.entries.lock();
        if entries.len() >= MAX_ENTRIES {
            entries.retain(|_, entry| now < entry.expires_at);
            if entries.len() >= MAX_ENTRIES {
                // Still full of live entries. A miss costs one redeem, so
                // a rare full flush beats growing without bound.
                entries.clear();
            }
        }
        entries.insert(
            key,
            Entry {
                permit: permit.clone(),
                expires_at: now + PERMIT_TTL,
            },
        );
    }

    /// Drop the entry for `key`, so the next redeem goes to the service.
    pub fn invalidate(&self, key: &PermitKey) {
        self.entries.lock().remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Subject, did};
    use dialog_common::Buffer;
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_remote_s3::request::IntoRequest;
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test_configure;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test_configure!(run_in_browser);

    fn permit() -> Permit {
        Permit {
            url: "https://bucket.example/key?X-Amz-Signature=abc"
                .parse()
                .unwrap(),
            method: "GET".to_string(),
            headers: vec![],
        }
    }

    fn address() -> UcanAddress {
        UcanAddress::new("https://access.example/ucan/")
    }

    fn catalog() -> dialog_capability::Capability<Catalog> {
        Subject::from(did!("key:zPermitCacheTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("blocks"))
    }

    fn get_request(digest: [u8; 32]) -> S3Request {
        catalog().invoke(Get::new(digest)).to_request()
    }

    fn key(digest: [u8; 32]) -> PermitKey {
        PermitKey::cacheable(&address(), get_request(digest)).expect("a GET request is cacheable")
    }

    #[dialog_common::test]
    fn it_returns_a_cached_permit_before_expiry() {
        let cache = PermitCache::default();
        let now = dialog_common::time::now();
        cache.store(key([0u8; 32]), &permit(), now);
        let hit = cache.lookup(&key([0u8; 32]), now + PERMIT_TTL - Duration::from_secs(1));
        assert_eq!(hit.map(|p| p.method), Some("GET".to_string()));
    }

    #[dialog_common::test]
    fn it_expires_a_permit_after_its_ttl() {
        let cache = PermitCache::default();
        let now = dialog_common::time::now();
        cache.store(key([0u8; 32]), &permit(), now);
        assert!(cache.lookup(&key([0u8; 32]), now + PERMIT_TTL).is_none());
    }

    #[dialog_common::test]
    fn it_keys_permits_by_endpoint_and_object_path() {
        let cache = PermitCache::default();
        let now = dialog_common::time::now();
        cache.store(key([0u8; 32]), &permit(), now);

        assert!(
            cache.lookup(&key([1u8; 32]), now).is_none(),
            "a different object path is a different entry"
        );
        let elsewhere = PermitKey::cacheable(
            &UcanAddress::new("https://other.example/ucan/"),
            get_request([0u8; 32]),
        )
        .expect("a GET request is cacheable");
        assert!(
            cache.lookup(&elsewhere, now).is_none(),
            "a different access service is a different entry"
        );
    }

    #[dialog_common::test]
    fn it_has_no_cache_key_for_a_mutating_request() {
        let put = catalog().invoke(Put::new(Buffer::from(vec![1, 2, 3])));
        assert!(PermitKey::cacheable(&address(), put.to_request()).is_none());
    }

    #[dialog_common::test]
    fn it_invalidates_a_permit_on_demand() {
        let cache = PermitCache::default();
        let now = dialog_common::time::now();
        cache.store(key([0u8; 32]), &permit(), now);
        cache.invalidate(&key([0u8; 32]));
        assert!(cache.lookup(&key([0u8; 32]), now).is_none());
    }

    #[dialog_common::test]
    fn it_bounds_the_number_of_retained_entries() {
        let cache = PermitCache::default();
        let now = dialog_common::time::now();
        for i in 0..=MAX_ENTRIES {
            let mut digest = [0u8; 32];
            digest[..8].copy_from_slice(&(i as u64).to_le_bytes());
            cache.store(key(digest), &permit(), now);
        }
        assert!(cache.entries.lock().len() <= MAX_ENTRIES);
    }
}
