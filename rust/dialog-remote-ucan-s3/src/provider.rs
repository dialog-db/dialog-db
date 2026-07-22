//! `Provider<ForkInvocation<UcanSite, Fx>>` for [`UcanSite`].
//!
//! Every remote effect follows the same two steps: redeem the UCAN
//! authorization at the access service for a presigned permit, then hand
//! the permit to [`S3`] for the actual HTTP request. The access service
//! is responsible for presigning the right object — `{subject}/{catalog}/{digest}`
//! for the block archive, `{subject}/blob/{digest}` for blobs,
//! `{subject}/{space}/{cell}` for memory cells — and for choosing the
//! method, so this side is uniform across effects and expressed as one
//! blanket impl rather than one impl per effect.
//!
//! Redeeming is skipped when a fresh permit for the same object is
//! already cached; see [`crate::permit_cache`].

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, ForkInvocation, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_remote_s3::request::IntoRequest;
use dialog_remote_s3::{S3, S3Error, S3Invocation};

use crate::permit_cache::{PermitCache, PermitKey};
use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Fx, T, E> Provider<ForkInvocation<UcanSite, Fx>> for UcanSite
where
    Fx: Effect<Output = Result<T, E>> + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: IntoRequest + ConditionalSend + ConditionalSync,
    ForkInvocation<UcanSite, Fx>: ConditionalSend,
    S3: Provider<S3Invocation<Fx>> + ConditionalSync,
    T: ConditionalSend,
    E: From<S3Error> + ConditionalSend,
{
    async fn execute(&self, invocation: ForkInvocation<UcanSite, Fx>) -> Result<T, E> {
        let cache = PermitCache::shared();
        let now = dialog_common::time::now();
        // `None` for a mutating effect, which must redeem every time.
        let key = PermitKey::cacheable(&invocation.address, invocation.capability.to_request());

        let permit = match key.as_ref().and_then(|key| cache.lookup(key, now)) {
            Some(permit) => permit,
            None => {
                let permit = invocation.authorization.redeem(&invocation.address).await?;
                if let Some(key) = key.clone() {
                    cache.store(key, &permit, now);
                }
                permit
            }
        };

        let result = permit.invoke(invocation.capability).perform(&S3).await;
        if let (Err(_), Some(key)) = (&result, &key) {
            // Drop the permit on any downstream failure. It may genuinely
            // be unusable while this process still thinks it has TTL left:
            // the presign can lapse server-side under clock skew, and the
            // delegation it was redeemed against carries its own expiry.
            // For a merely transient error the cost of dropping it is one
            // extra redeem, which beats retrying a dead permit until the
            // TTL runs out.
            cache.invalidate(key);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Principal, Subject, did};
    use dialog_credentials::Ed25519Signer;
    use dialog_effects::archive::{Archive, ArchiveError, Catalog, Get};
    use dialog_remote_s3::Permit;
    use dialog_ucan::UcanInvocation;
    use dialog_ucan_core::{InvocationBuilder, InvocationChain};

    use crate::site::{UcanAddress, UcanAuthorization};

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test_configure;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test_configure!(run_in_browser);

    /// A self-signed (issuer == subject, no delegation) UCAN
    /// authorization. Enough to satisfy `ForkInvocation`'s type, but
    /// never actually redeemed below: the cache is pre-seeded so the
    /// provider never reaches `authorization.redeem`.
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

    /// Invalidate-on-failure only fires by driving a real invocation
    /// through the provider, which always runs against the real `S3` and
    /// the process-wide `PermitCache::shared()` — neither is injectable.
    /// So this primes the shared cache and forces the request to fail by
    /// pointing the permit at a dead loopback port: deterministic and
    /// network-free, without reaching for a mock HTTP layer.
    #[dialog_common::test]
    async fn it_invalidates_the_cache_entry_after_a_failed_request() {
        let signer = Ed25519Signer::import(&[9u8; 32]).await.unwrap();
        let capability = Subject::from(did!("key:zPermitCacheFailureTest"))
            .attenuate(Archive)
            .attenuate(Catalog::new("blobs"))
            .invoke(Get::new([0u8; 32]));

        let address = UcanAddress::new("http://127.0.0.1:1/redeem");
        let key = PermitKey::cacheable(&address, capability.to_request())
            .expect("a GET request is cacheable");
        let now = dialog_common::time::now();

        // Prime the entry the provider will look up, so it reuses this
        // permit instead of redeeming.
        PermitCache::shared().store(key.clone(), &unreachable_permit(), now);
        assert!(
            PermitCache::shared().lookup(&key, now).is_some(),
            "precondition: cache should be primed before the call"
        );

        let invocation =
            ForkInvocation::new(capability, address, self_authorization(&signer).await);
        let result: Result<Option<Vec<u8>>, ArchiveError> = UcanSite.execute(invocation).await;

        assert!(
            result.is_err(),
            "a request against an unreachable permit should fail"
        );
        assert!(
            PermitCache::shared().lookup(&key, now).is_none(),
            "a failed request should invalidate its cache entry"
        );
    }
}
