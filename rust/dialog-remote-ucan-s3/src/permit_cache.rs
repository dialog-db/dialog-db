//! Cache of redeemed access-service permits.
//!
//! Every remote effect used to POST its UCAN invocation to the access
//! service and receive a fresh presigned URL on every request, even
//! though presigned URLs stay valid for an hour. On a periodically
//! syncing replica the redeem round-trip doubled the cost of every idle
//! poll.
//!
//! Each [`UcanSite`](crate::UcanSite) owns one cache, so cached permits
//! are scoped to the operator whose network holds that site and are
//! dropped with it. Nothing outside that operator's session can be
//! served a permit its own authorization did not redeem.
//!
//! An entry is keyed by access-service endpoint, method, and object
//! path. [`PermitKey::cacheable`] is the sole constructor and returns
//! `None` for anything but a plain GET (no query parameters, no
//! precondition), so a permit for any other request shape cannot be
//! stored: there is no key to store it under. Requests that differ only
//! in headers, such as ranged blob reads, share one permit.
//!
//! Retention is a [`SieveCache`] bounded at `MAX_ENTRIES`. At capacity
//! an insert evicts one cold entry instead of flushing the map, so a
//! bulk read sweep cannot wipe the hot entries an idle poll relies on.
//! Entries also lapse [`PERMIT_TTL`] after being stored.

use std::fmt;
use std::time::{Duration, SystemTime};

use dialog_capability::SiteId;
use dialog_remote_s3::request::S3Request;
use dialog_remote_s3::{Permit, Precondition};
use parking_lot::Mutex;
use sieve_cache::SieveCache;

use crate::site::UcanAddress;

/// How long a redeemed permit is reused before redeeming afresh. Well
/// under the service's hour-long presign validity, so a cached permit is
/// never presented close to its expiry.
pub const PERMIT_TTL: Duration = Duration::from_secs(300);

/// Hard bound on retained entries. Keys are per-object, so a large read
/// sweep would otherwise retain a permit per block for the whole TTL.
const MAX_ENTRIES: usize = 512;

/// Cache key: access-service endpoint plus the method and S3 object
/// path the permit presigns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PermitKey {
    site: SiteId,
    method: String,
    path: String,
}

impl PermitKey {
    /// The key `request` is cached under at `address`, or `None` when
    /// the request is not cacheable.
    ///
    /// Only plain GET permits are reusable: a mutating presign can bind
    /// payload-specific signing material, and the presigned signature
    /// covers the query string, so a request carrying parameters or a
    /// precondition fails closed here rather than sharing a permit that
    /// was signed for a different request shape.
    pub fn cacheable(address: &UcanAddress, request: S3Request) -> Option<Self> {
        (request.method == "GET"
            && request.params.is_none()
            && matches!(request.precondition, Precondition::None))
        .then(|| Self {
            site: SiteId::from(address.clone()),
            method: request.method,
            path: request.path,
        })
    }
}

struct Entry {
    permit: Permit,
    expires_at: SystemTime,
}

/// TTL cache of redeemed permits, keyed by [`PermitKey`].
pub struct PermitCache {
    entries: Mutex<SieveCache<PermitKey, Entry>>,
}

impl Default for PermitCache {
    fn default() -> Self {
        Self {
            entries: Mutex::new(SieveCache::new(MAX_ENTRIES).expect("capacity is non-zero")),
        }
    }
}

impl fmt::Debug for PermitCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermitCache")
            .field("entries", &self.entries.lock().len())
            .finish()
    }
}

impl PermitCache {
    /// The cached permit for `key`, unless it has passed its TTL. An
    /// entry found past its TTL is removed on the way out.
    pub fn lookup(&self, key: &PermitKey, now: SystemTime) -> Option<Permit> {
        let mut entries = self.entries.lock();
        match entries.get(key) {
            Some(entry) if now < entry.expires_at => return Some(entry.permit.clone()),
            Some(_) => {}
            None => return None,
        }
        entries.remove(key);
        None
    }

    /// Cache `permit` under `key`. At capacity the sieve evicts one
    /// cold entry to make room; hot entries survive.
    pub fn store(&self, key: PermitKey, permit: &Permit, now: SystemTime) {
        self.entries.lock().insert(
            key,
            Entry {
                permit: permit.clone(),
                expires_at: now + PERMIT_TTL,
            },
        );
    }

    /// Drop the entry for `key`, provided it still holds `permit`. A
    /// concurrent task may have redeemed and stored a fresh permit
    /// under the same key after this one was looked up; that fresh
    /// entry is left alone.
    pub fn invalidate(&self, key: &PermitKey, permit: &Permit) {
        let mut entries = self.entries.lock();
        if entries
            .get(key)
            .is_some_and(|entry| entry.permit.url == permit.url)
        {
            entries.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{MAX_ENTRIES, PERMIT_TTL, PermitCache, PermitKey};
    use dialog_capability::{Capability, Subject, did};
    use dialog_common::{Buffer, time};
    use dialog_effects::Use;
    use dialog_effects::archive::{Archive, Catalog, Get, Put};
    use dialog_remote_s3::request::{IntoRequest, S3Request};
    use dialog_remote_s3::{Permit, Precondition};

    use crate::site::UcanAddress;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test_configure;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test_configure!(run_in_dedicated_worker);

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

    fn catalog() -> Capability<Catalog> {
        Subject::from(did!("key:zPermitCacheTest"))
            .attenuate(Use)
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
        let now = time::now();
        cache.store(key([0u8; 32]), &permit(), now);
        let hit = cache.lookup(&key([0u8; 32]), now + PERMIT_TTL - Duration::from_secs(1));
        assert_eq!(hit.map(|p| p.method), Some("GET".to_string()));
    }

    #[dialog_common::test]
    fn it_expires_a_permit_after_its_ttl() {
        let cache = PermitCache::default();
        let now = time::now();
        cache.store(key([0u8; 32]), &permit(), now);
        assert!(cache.lookup(&key([0u8; 32]), now + PERMIT_TTL).is_none());
    }

    #[dialog_common::test]
    fn it_keys_permits_by_endpoint_and_object_path() {
        let cache = PermitCache::default();
        let now = time::now();
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
    fn it_has_no_cache_key_for_a_parameterized_request() {
        let with_params = S3Request {
            params: Some(vec![("versionId".to_string(), "7".to_string())]),
            ..get_request([0u8; 32])
        };
        assert!(
            PermitKey::cacheable(&address(), with_params).is_none(),
            "the presigned signature covers the query string, so a \
             parameterized request must fail closed"
        );

        let with_precondition = S3Request {
            precondition: Precondition::IfNoneMatch,
            ..get_request([0u8; 32])
        };
        assert!(
            PermitKey::cacheable(&address(), with_precondition).is_none(),
            "a conditional request must fail closed"
        );
    }

    #[dialog_common::test]
    fn it_invalidates_the_entry_holding_the_failed_permit() {
        let cache = PermitCache::default();
        let now = time::now();
        cache.store(key([0u8; 32]), &permit(), now);
        cache.invalidate(&key([0u8; 32]), &permit());
        assert!(cache.lookup(&key([0u8; 32]), now).is_none());
    }

    #[dialog_common::test]
    fn it_retains_a_fresh_permit_when_invalidating_a_stale_one() {
        let cache = PermitCache::default();
        let now = time::now();
        let stale = Permit {
            url: "https://bucket.example/key?X-Amz-Signature=old"
                .parse()
                .unwrap(),
            method: "GET".to_string(),
            headers: vec![],
        };
        // A concurrent task redeemed a fresh permit after `stale` was
        // looked up; the slow failure must not delete it.
        cache.store(key([0u8; 32]), &permit(), now);
        cache.invalidate(&key([0u8; 32]), &stale);
        assert!(
            cache.lookup(&key([0u8; 32]), now).is_some(),
            "invalidation is scoped to the permit that actually failed"
        );
    }

    #[dialog_common::test]
    fn it_bounds_the_number_of_retained_entries() {
        let cache = PermitCache::default();
        let now = time::now();
        for i in 0..=MAX_ENTRIES {
            let mut digest = [0u8; 32];
            digest[..8].copy_from_slice(&(i as u64).to_le_bytes());
            cache.store(key(digest), &permit(), now);
        }
        assert!(cache.entries.lock().len() <= MAX_ENTRIES);
    }

    #[dialog_common::test]
    fn it_evicts_cold_entries_instead_of_flushing_the_cache() {
        let cache = PermitCache::default();
        let now = time::now();
        let hot = key([255u8; 32]);
        cache.store(hot.clone(), &permit(), now);

        // A bulk sweep past capacity while the hot entry keeps being
        // read: the sieve evicts cold entries and the hot one survives,
        // where a full flush would have dropped it at capacity.
        for i in 0..MAX_ENTRIES {
            let mut digest = [0u8; 32];
            digest[..8].copy_from_slice(&(i as u64).to_le_bytes());
            cache.store(key(digest), &permit(), now);
            assert!(
                cache.lookup(&hot, now).is_some(),
                "a hot entry must survive a sweep of cold inserts"
            );
        }
    }
}
