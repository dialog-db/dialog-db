//! UCAN site configuration -- site type + address type.

use std::sync::Arc;

use dialog_capability::access::{
    Access, Authorization as _, Authorize as AuthorizeEffect, AuthorizeError, FromCapability,
    Protocol,
};
use dialog_capability::{
    Ability, Capability, Constraint, Effect, Fork, ForkInvocation, Provider, Site, SiteAddress,
    SiteFork, SiteId, Subject,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::Rejection;
use dialog_effects::authority::{self, OperatorExt};
use dialog_remote_s3::{Permit, S3Error, http_client};

const MAX_ERROR_BODY_BYTES: usize = 8 * 1024;

/// Read the reason a request was refused.
///
/// The reason travels as itself. There is no code table here and no
/// status codes: an [`AuthorizeError`] built on the other side arrives
/// as the same value, so nothing in this crate has to know a vocabulary
/// of wire names, and adding a reason does not mean teaching two
/// codebases a new string.
///
/// A body that does not parse becomes [`Rejection::Unclassified`] rather
/// than being guessed at. That is also what an older responder gets: it
/// degrades to "something went wrong and we cannot say what", which is
/// true, instead of to a specific reason that might not be.
fn read_rejection(status: u16, body: &[u8]) -> S3Error {
    let bounded = &body[..body.len().min(MAX_ERROR_BODY_BYTES)];

    if let Ok(reason) = serde_json::from_slice::<AuthorizeError>(bounded) {
        return S3Error::Authorization(reason);
    }
    if let Ok(reason) = serde_json::from_slice::<Rejection>(bounded) {
        return S3Error::Rejected(reason);
    }

    S3Error::Rejected(Rejection::Unclassified {
        detail: format!("responder answered {status} with no reason we could read"),
    })
}

use crate::permit_cache::PermitCache;

// Re-export UCAN types for convenience.
pub use dialog_ucan::{Ucan, UcanInvocation};

/// UCAN authorization material for site providers.
///
/// Wraps a [`UcanInvocation`] (signed UCAN chain) that gets sent to the
/// access service to obtain a presigned URL.
#[derive(Debug, Clone)]
pub struct UcanAuthorization(UcanInvocation);

impl UcanAuthorization {
    /// Redeem this authorization at the access service for a presigned URL permit.
    pub async fn redeem(&self, address: &UcanAddress) -> Result<Permit, S3Error> {
        let body = self
            .0
            .to_bytes()
            .map_err(|e| S3Error::Serialization(e.to_string()))?;

        let response = http_client()
            .post(&address.endpoint)
            .header("Content-Type", "application/cbor")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.bytes().await.unwrap_or_default();
            return Err(read_rejection(status.as_u16(), &body));
        }

        let body = response.bytes().await?;

        serde_ipld_dagcbor::from_slice(&body)
            .map_err(|e| S3Error::Serialization(format!("Failed to decode response: {e}")))
    }
}

impl From<UcanInvocation> for UcanAuthorization {
    fn from(invocation: UcanInvocation) -> Self {
        Self(invocation)
    }
}

/// UCAN site address -- wraps the access service endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct UcanAddress {
    /// The access service endpoint URL.
    pub endpoint: String,
}

impl UcanAddress {
    /// Create a new UCAN address with the given endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    /// Get the access service endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl SiteAddress for UcanAddress {
    type Site = UcanSite;
}

impl From<UcanAddress> for SiteId {
    fn from(address: UcanAddress) -> Self {
        address.endpoint.into()
    }
}

/// Site-owned fork wrapper for UCAN.
///
/// Thin newtype around [`Fork<UcanSite, Fx>`] that carries the
/// site-specific [`Authorize`](dialog_capability::SiteFork)
/// impl: fetches session identity from the env, invokes UCAN's
/// `Authorize` on that identity, and bundles the resulting signed
/// delegation into a [`ForkInvocation`].
pub struct UcanFork<Fx: Effect>(Fork<UcanSite, Fx>);

impl<Fx: Effect> From<Fork<UcanSite, Fx>> for UcanFork<Fx> {
    fn from(fork: Fork<UcanSite, Fx>) -> Self {
        Self(fork)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Fx, Env> SiteFork<Env> for UcanFork<Fx>
where
    Fx: Effect + Clone + ConditionalSend + ConditionalSync + 'static,
    Fx::Of: Constraint<Capability: ConditionalSend + ConditionalSync>,
    Capability<Fx>: Ability + ConditionalSend + ConditionalSync,
    Env: Provider<AuthorizeEffect<Ucan>> + Provider<authority::Identify> + ConditionalSync,
{
    type Site = UcanSite;
    type Effect = Fx;

    async fn authorize(self, env: &Env) -> Result<ForkInvocation<UcanSite, Fx>, AuthorizeError> {
        let identity =
            authority::Identify
                .perform(env)
                .await
                .map_err(|e| AuthorizeError::Malformed {
                    detail: e.to_string(),
                })?;
        let profile = identity.profile().clone();
        let operator = identity.did();

        let scope = <Ucan as Protocol>::Access::from_capability(self.0.capability());

        let authorization = Subject::from(profile)
            .attenuate(Access)
            .invoke(AuthorizeEffect::<Ucan>::new(operator, scope))
            .perform(env)
            .await?;

        let invocation = authorization.invoke().await?;
        Ok(self.0.attest(UcanAuthorization::from(invocation)))
    }
}

/// UCAN site configuration for delegated authorization.
///
/// Address info lives in `UcanAddress`. The site owns the cache of
/// redeemed GET permits, so cached permits are scoped to whoever holds
/// this site (one `Network`, hence one `Operator`) and are dropped with
/// it; another operator in the same process has its own site and can
/// never be served a permit this one redeemed. Clones share the cache.
#[derive(Debug, Clone, Default)]
pub struct UcanSite {
    permits: Arc<PermitCache>,
}

impl UcanSite {
    /// The cache of redeemed GET permits shared by clones of this site.
    pub(crate) fn permits(&self) -> &PermitCache {
        &self.permits
    }
}

impl Site for UcanSite {
    type Authorization = UcanAuthorization;
    type Address = UcanAddress;
    type Fork<Fx: Effect> = UcanFork<Fx>;
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_capability::did;

    #[cfg(not(target_arch = "wasm32"))]
    use std::collections::{BTreeMap, HashMap};
    #[cfg(not(target_arch = "wasm32"))]
    use std::sync::Arc;

    #[cfg(not(target_arch = "wasm32"))]
    use dialog_capability::Principal;
    #[cfg(not(target_arch = "wasm32"))]
    use dialog_credentials::Ed25519Signer;
    #[cfg(not(target_arch = "wasm32"))]
    use dialog_ucan_core::subject::Subject as DelegatedSubject;
    #[cfg(not(target_arch = "wasm32"))]
    use dialog_ucan_core::{DelegationBuilder, InvocationBuilder, InvocationChain};
    #[cfg(not(target_arch = "wasm32"))]
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    #[cfg(not(target_arch = "wasm32"))]
    use tokio::net::TcpListener;

    // The reason travels as itself, so what this pins is the round
    // trip: whatever the responder built arrives as the same value.
    #[dialog_common::test]
    async fn it_reads_back_the_reason_the_responder_sent() {
        let sent = AuthorizeError::Revoked {
            subject: did!("key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"),
        };
        let body = serde_json::to_vec(&sent).expect("serializes");

        let S3Error::Authorization(read) = read_rejection(403, &body) else {
            panic!("expected an authorization reason");
        };
        assert_eq!(read, sent, "the reason arrives as the value that was sent");
    }

    #[dialog_common::test]
    async fn it_reads_back_a_rejection() {
        let sent = Rejection::Unavailable {
            reason: "registry is down".into(),
        };
        let body = serde_json::to_vec(&sent).expect("serializes");

        let S3Error::Rejected(read) = read_rejection(503, &body) else {
            panic!("expected a rejection");
        };
        assert_eq!(read, sent);
    }

    // A responder that speaks the old shape, or none at all, degrades to
    // "we cannot say" rather than to a specific reason that might be
    // wrong. Guessing is what the code table used to do.
    #[dialog_common::test]
    async fn it_does_not_guess_at_a_body_it_cannot_read() {
        for body in [
            &b"not json at all"[..],
            br#"{"error":{"code":"CREDENTIAL_REVOKED","message":"old shape"}}"#,
        ] {
            assert!(
                matches!(
                    read_rejection(403, body),
                    S3Error::Rejected(Rejection::Unclassified { .. })
                ),
                "an unreadable body must not become a specific reason"
            );
        }
    }

    #[dialog_common::test]
    async fn it_bounds_the_body_it_reads() {
        let mut oversized = vec![b'x'; MAX_ERROR_BODY_BYTES * 2];
        oversized.extend_from_slice(br#"{"kind":"Revoked","subject":"did:key:zLate"}"#);

        assert!(
            matches!(
                read_rejection(403, &oversized),
                S3Error::Rejected(Rejection::Unclassified { .. })
            ),
            "a reason past the bound must not be read"
        );
    }

    // A responder that has not been updated yet still sends the old
    // `{code, message}` shape. That degrades to "we cannot say why",
    // which is true, rather than to a specific reason guessed from a
    // status code. Worth pinning: it is the behaviour during a rollout
    // where the two sides are not yet in step.
    #[dialog_common::test]
    async fn it_degrades_when_the_responder_speaks_the_old_shape() {
        let old = br#"{"error":{"code":"CREDENTIAL_REVOKED","message":"Credential revoked"}}"#;
        let S3Error::Rejected(Rejection::Unclassified { detail }) = read_rejection(403, old) else {
            panic!("an unreadable body must not become a specific reason");
        };
        assert!(
            !detail.contains("CREDENTIAL_REVOKED"),
            "and must not smuggle the old code back in, got {detail}"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_carries_the_reason_through_redeem() {
        let subject = Ed25519Signer::import(&[41; 32]).await.expect("subject key");
        let operator = Ed25519Signer::import(&[42; 32])
            .await
            .expect("operator key");
        let command = vec!["memory".to_string(), "resolve".to_string()];
        let delegation = DelegationBuilder::new()
            .issuer(subject.clone())
            .audience(&operator)
            .subject(DelegatedSubject::Specific(subject.did()))
            .command(command.clone())
            .try_build()
            .await
            .expect("delegation");
        let delegation_cid = delegation.to_cid();
        let invocation = InvocationBuilder::new()
            .issuer(operator)
            .audience(&subject.did())
            .subject(&subject.did())
            .command(command)
            .arguments(BTreeMap::new())
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("invocation");
        let mut delegations = HashMap::new();
        delegations.insert(delegation_cid, Arc::new(delegation));
        let authorization = UcanAuthorization::from(UcanInvocation {
            chain: Box::new(InvocationChain::new(invocation, delegations)),
            subject: subject.did(),
            ability: "/memory/resolve".to_string(),
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("listener address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("request");
            let mut request = vec![0; 16 * 1024];
            let _ = stream.read(&mut request).await.expect("read request");
            // What a responder sends now: the reason itself.
            let reason = AuthorizeError::Revoked {
                subject: did!("key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"),
            };
            let body = serde_json::to_string(&reason).expect("serializes");
            let response = format!(
                "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        });

        let error = authorization
            .redeem(&UcanAddress::new(format!("http://{address}")))
            .await
            .expect_err("redeem is denied");
        server.await.expect("server task");
        // End to end over a real socket: the reason the responder built
        // is the reason the caller receives.
        let S3Error::Authorization(AuthorizeError::Revoked { subject }) = error else {
            panic!("expected a revoked authorization, got {error:?}");
        };
        assert_eq!(
            subject,
            did!("key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"),
            "the subject is the one the responder named"
        );
    }
}
