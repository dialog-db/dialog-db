//! `Provider<ForkInvocation<UcanSite, Fx>>` for [`UcanSite`].
//!
//! Every remote effect follows the same two steps: redeem the UCAN
//! authorization at the access service for a presigned permit, then hand
//! the permit to [`S3`] for the actual HTTP request. The access service
//! is responsible for presigning the right object -- `{subject}/{catalog}/{digest}`
//! for the block archive, `{subject}/blob/{digest}` for blobs,
//! `{subject}/{space}/{cell}` for memory cells -- and for choosing the
//! method, so this side is uniform across effects and expressed as one
//! blanket impl rather than one impl per effect.
//!
//! Redeeming is skipped when a fresh permit for the same object is
//! already cached on the site; see [`crate::permit_cache`]. When the
//! service rejects a cached permit (it can lapse server-side under
//! clock skew or configuration), the entry is invalidated and the
//! authorization redeemed afresh for one retry. Semantic failures such
//! as a missing object or a CAS conflict leave the permit cached: it is
//! still good, and dropping it would restore the redeem round-trip this
//! cache exists to remove.

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, ForkInvocation, Provider};
use dialog_common::{ConditionalSend, time};
use dialog_remote_s3::request::{IntoRequest, RequestMethod};
use dialog_remote_s3::{PermitRejection, S3, S3Error, S3Invocation};

use crate::permit_cache::PermitKey;
use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Fx, T, E> Provider<ForkInvocation<UcanSite, Fx>> for UcanSite
where
    Fx: Effect<Output = Result<T, E>> + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: IntoRequest + RequestMethod + Clone + ConditionalSend,
    S3: Provider<S3Invocation<Fx>>,
    T: ConditionalSend,
    E: From<S3Error> + PermitRejection + ConditionalSend,
{
    async fn execute(&self, invocation: ForkInvocation<UcanSite, Fx>) -> Result<T, E> {
        let cache = self.permits();
        let now = time::now();
        // Consult the method before building the request at all: a
        // mutating effect never has a cache key, and its request
        // translation checksums the entire payload.
        let key = (Capability::<Fx>::METHOD == "GET")
            .then(|| invocation.capability.to_request())
            .and_then(|request| PermitKey::cacheable(&invocation.address, request));

        let cached = key.as_ref().and_then(|key| cache.lookup(key, now));
        let from_cache = cached.is_some();
        let permit = match cached {
            Some(permit) => permit,
            None => {
                let permit = invocation.authorization.redeem(&invocation.address).await?;
                if let Some(key) = key.clone() {
                    cache.store(key, &permit, now);
                }
                permit
            }
        };

        // A retry presents the capability a second time, so it is
        // cloned only when a cached permit makes a retry possible.
        let retry = from_cache.then(|| invocation.capability.clone());
        let presented = key.map(|key| (key, permit.clone()));
        let outcome = permit.invoke(invocation.capability).perform(&S3).await;

        match (outcome, presented) {
            (Err(error), Some((key, permit))) if error.is_permit_rejection() => {
                cache.invalidate(&key, &permit);
                match retry {
                    Some(capability) => {
                        let fresh = invocation.authorization.redeem(&invocation.address).await?;
                        cache.store(key, &fresh, now);
                        fresh.invoke(capability).perform(&S3).await
                    }
                    // The rejected permit was redeemed moments ago;
                    // redeeming again would present the same material.
                    None => Err(error),
                }
            }
            (outcome, _) => outcome,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use dialog_capability::{Principal, Subject, did};
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::Use;
    use dialog_effects::archive::{Archive, ArchiveError, Catalog, Get};
    use dialog_remote_s3::Permit;
    use dialog_ucan::UcanInvocation;
    use dialog_ucan_core::{InvocationBuilder, InvocationChain};

    use crate::site::{UcanAddress, UcanAuthorization};

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test_configure;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A self-signed (issuer == subject, no delegation) UCAN
    /// authorization. Enough to satisfy `ForkInvocation`'s type, but
    /// never successfully redeemed below: tests either pre-seed the
    /// cache or point the address at a dead endpoint.
    async fn self_authorization(signer: &Ed25519Signer) -> UcanAuthorization {
        let did = signer.did();
        let invocation = InvocationBuilder::new()
            .issuer(dialog_credentials::Signer::from(signer.clone()))
            .audience(&did)
            .subject(&did)
            .command(vec!["archive".to_string(), "get".to_string()])
            .proofs(vec![])
            .try_build()
            .await
            .expect("failed to build self-signed invocation");
        let chain = InvocationChain::new(invocation, HashMap::new());
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

    /// A transport failure proves nothing about the permit: the entry
    /// must survive so the next attempt skips the redeem round-trip.
    /// (Only a rejection by the service, an HTTP 401/403, invalidates.)
    #[dialog_common::test]
    async fn it_retains_the_permit_after_a_transport_error() {
        let signer = Ed25519Signer::import(&[9u8; 32]).await.unwrap();
        let capability = Subject::from(did!("key:zPermitCacheTransportTest"))
            .attenuate(Use)
            .attenuate(Archive)
            .attenuate(Catalog::new("blobs"))
            .invoke(Get::new([0u8; 32]));

        let site = UcanSite::default();
        let address = UcanAddress::new("http://127.0.0.1:1/redeem");
        let key = PermitKey::cacheable(&address, capability.to_request())
            .expect("a GET request is cacheable");
        let now = time::now();

        // Prime the entry the provider will look up, so it reuses this
        // permit instead of redeeming.
        site.permits()
            .store(key.clone(), &unreachable_permit(), now);

        let invocation =
            ForkInvocation::new(capability, address, self_authorization(&signer).await);
        let result: Result<Option<Vec<u8>>, ArchiveError> = site.execute(invocation).await;

        assert!(
            result.is_err(),
            "a request against an unreachable permit should fail"
        );
        assert!(
            site.permits().lookup(&key, now).is_some(),
            "a transport error must not invalidate the cached permit"
        );
    }

    /// Each site owns its cache: permits redeemed through one site are
    /// invisible to another, so one operator can never ride a permit a
    /// different operator's authorization redeemed.
    #[dialog_common::test]
    fn it_scopes_cached_permits_to_the_site() {
        let capability = Subject::from(did!("key:zPermitCacheScopeTest"))
            .attenuate(Use)
            .attenuate(Archive)
            .attenuate(Catalog::new("blobs"))
            .invoke(Get::new([0u8; 32]));
        let address = UcanAddress::new("http://127.0.0.1:1/redeem");
        let key = PermitKey::cacheable(&address, capability.to_request())
            .expect("a GET request is cacheable");
        let now = time::now();

        let site = UcanSite::default();
        site.permits()
            .store(key.clone(), &unreachable_permit(), now);

        assert!(
            site.clone().permits().lookup(&key, now).is_some(),
            "clones of a site share its cache"
        );
        assert!(
            UcanSite::default().permits().lookup(&key, now).is_none(),
            "a separate site must not see another site's permits"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    mod native {
        use super::*;
        use dialog_common::Blake3Hash;
        use dialog_effects::blob::BlobError;
        use dialog_effects::blob::prelude::{ArchiveBlobExt, BlobExt};
        use dialog_remote_s3::helpers::LocalS3;

        /// The pin for the presence-probe finding: a GET that comes
        /// back 404 is a semantic outcome, not a permit failure. The
        /// cached permit must survive, otherwise every poll for a blob
        /// a peer has not uploaded yet would pay the redeem round-trip
        /// this cache exists to remove.
        #[dialog_common::test]
        async fn it_retains_the_permit_when_the_blob_is_absent() {
            let server = LocalS3::start(&["probe-bucket"])
                .await
                .expect("local S3 starts");

            let signer = Ed25519Signer::import(&[7u8; 32]).await.unwrap();
            let digest = Blake3Hash::hash(b"not uploaded yet");
            let capability = Subject::from(did!("key:zPermitCacheProbeTest"))
                .attenuate(Use)
                .attenuate(Archive)
                .blob()
                .read(digest);

            let site = UcanSite::default();
            let address = UcanAddress::new("http://127.0.0.1:1/redeem");
            let key = PermitKey::cacheable(&address, capability.to_request())
                .expect("a GET request is cacheable");
            let now = time::now();

            // A live permit for an object nobody has uploaded: the
            // server genuinely answers 404.
            let url = format!("{}/probe-bucket/{}", server.endpoint, "missing-object");
            let permit = Permit {
                url: url.parse().expect("valid url"),
                method: "GET".to_string(),
                headers: vec![],
            };
            site.permits().store(key.clone(), &permit, now);

            let invocation =
                ForkInvocation::new(capability, address, self_authorization(&signer).await);
            let result: Result<dialog_effects::blob::BlobReader, BlobError> =
                site.execute(invocation).await;

            assert!(
                matches!(result, Err(BlobError::NotFound(_))),
                "an absent blob should surface as NotFound"
            );
            assert!(
                site.permits().lookup(&key, now).is_some(),
                "a presence probe must not invalidate the cached permit"
            );
        }

        /// A 403 means the service rejected the permit itself: the
        /// entry is invalidated and the provider redeems afresh for a
        /// retry (here the redeem fails too, at the dead endpoint, so
        /// the call errors; what matters is that the entry is gone).
        #[dialog_common::test]
        async fn it_invalidates_a_permit_the_service_rejects() {
            // An auth-enabled server rejects unsigned requests, so a
            // bare object URL yields a genuine 401/403.
            let server = LocalS3::start_with_auth("access-key", "secret-key", &["auth-bucket"])
                .await
                .expect("local S3 starts");

            let signer = Ed25519Signer::import(&[8u8; 32]).await.unwrap();
            let capability = Subject::from(did!("key:zPermitCacheRejectTest"))
                .attenuate(Use)
                .attenuate(Archive)
                .attenuate(Catalog::new("blocks"))
                .invoke(Get::new([2u8; 32]));

            let site = UcanSite::default();
            let address = UcanAddress::new("http://127.0.0.1:1/redeem");
            let key = PermitKey::cacheable(&address, capability.to_request())
                .expect("a GET request is cacheable");
            let now = time::now();

            let url = format!("{}/auth-bucket/some-object", server.endpoint);
            let permit = Permit {
                url: url.parse().expect("valid url"),
                method: "GET".to_string(),
                headers: vec![],
            };
            site.permits().store(key.clone(), &permit, now);

            let invocation =
                ForkInvocation::new(capability, address, self_authorization(&signer).await);
            let result: Result<Option<Vec<u8>>, ArchiveError> = site.execute(invocation).await;

            assert!(
                result.is_err(),
                "a rejected permit whose re-redeem also fails should error"
            );
            assert!(
                site.permits().lookup(&key, now).is_none(),
                "a permit the service rejected must be invalidated"
            );
        }
    }
}
