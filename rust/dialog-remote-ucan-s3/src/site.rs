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
use dialog_effects::authority::{self, OperatorExt};
use dialog_effects::service::Rejection;
use dialog_remote_s3::{Permit, S3Error};
use dialog_varsig::Did;
use serde::Deserialize;

const MAX_SERVICE_ERROR_BODY_BYTES: usize = 8 * 1024;
const MALFORMED_SERVICE_ERROR_MESSAGE: &str = "Service returned a malformed error response";

#[derive(Deserialize)]
struct ServiceErrorEnvelope {
    error: ServiceErrorBody,
}

#[derive(Deserialize)]
struct ServiceErrorBody {
    code: Option<String>,
    message: Option<String>,
}

/// Classify a non-success response into the reason it denotes.
///
/// This is the only place in the stack that reads a status code. Above
/// it a caller sees *why* its request was not carried out and never how
/// that answer travelled, which is the point: the same reasons arise
/// against a local archive that speaks no HTTP.
///
/// The access-denial codes become real [`AuthorizeError`] values rather
/// than a parallel vocabulary. The DIDs they carry come from the
/// invocation we sent, not from the response: the service says only
/// *that* a proof was refused, and we already know whose proof it was
/// and what it reached for.
///
/// An unrecognized code becomes [`Rejection::Unclassified`]: what lands
/// there is exactly what has no agreed meaning, and guessing would
/// silently change what callers believe the day the service starts
/// saying something new. `SYNC_CONFLICT` is among them here -- redeeming
/// a permit is not a compare-and-swap, and the CAS path raises
/// `VersionMismatch` from its own 412 rather than routing through this.
fn classify_response(status: u16, body: &[u8], subject: &Did, issuer: &Did) -> S3Error {
    let bounded = &body[..body.len().min(MAX_SERVICE_ERROR_BODY_BYTES)];
    let (code, message) = match serde_json::from_slice::<ServiceErrorEnvelope>(bounded) {
        Ok(envelope) => (
            envelope.error.code,
            envelope
                .error
                .message
                .filter(|message| !message.is_empty())
                .unwrap_or_else(|| MALFORMED_SERVICE_ERROR_MESSAGE.to_string()),
        ),
        Err(_) => (None, MALFORMED_SERVICE_ERROR_MESSAGE.to_string()),
    };

    match (code.as_deref(), status) {
        // Authority was withdrawn: re-presenting the same proof cannot
        // help, so this is terminal rather than retryable.
        (Some("CREDENTIAL_REVOKED" | "DEVICE_REVOKED"), _) => AuthorizeError::Revoked {
            subject: subject.clone(),
        }
        .into(),
        // The proof does not reach this subject. Nothing was withdrawn,
        // so a caller holding a broader proof can retry with it.
        (Some("AUDIENCE_MISMATCH" | "SUBJECT_NOT_ALLOWED"), _) => AuthorizeError::InvalidAudience {
            claimed: issuer.clone(),
            authorized: subject.clone(),
        }
        .into(),
        // Refused without naming a reason. `UnprovenSubject` is the
        // honest reading: the service would not say which proof it
        // wanted, only that ours did not satisfy it.
        (None, 401 | 403) => AuthorizeError::UnprovenSubject {
            claimed: issuer.clone(),
            authorized: subject.clone(),
        }
        .into(),
        (Some("REVOCATION_UNAVAILABLE" | "SYNC_UNAVAILABLE"), _) | (_, 503) => {
            Rejection::Unavailable { reason: message }.into()
        }
        _ => Rejection::Unclassified {
            detail: match code {
                Some(code) => format!("{code} ({status}): {message}"),
                None => format!("{status}: {message}"),
            },
        }
        .into(),
    }
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

        let response = reqwest::Client::new()
            .post(&address.endpoint)
            .header("Content-Type", "application/cbor")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.bytes().await.unwrap_or_default();
            return Err(classify_response(
                status.as_u16(),
                &body,
                self.0.subject(),
                self.0.chain().issuer(),
            ));
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

    fn classify(status: u16, code: &str) -> S3Error {
        let body = serde_json::json!({
            "error": { "code": code, "message": "bounded detail" }
        });
        classify_response(
            status,
            body.to_string().as_bytes(),
            &did!("key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"),
            &did!("key:z6MkfQhLHBSFMuR7bQXTQeqe5kYUW51HpfZeaymgy1zkP2jM"),
        )
    }

    // The code table lives here and nowhere else, so this is what pins
    // it. A service-side rename that slips past this test degrades every
    // caller to `Unclassified` silently.
    #[dialog_common::test]
    async fn it_maps_service_codes_to_reasons() {
        assert!(matches!(
            classify(403, "CREDENTIAL_REVOKED"),
            S3Error::Authorization(AuthorizeError::Revoked { .. })
        ));
        assert!(matches!(
            classify(403, "DEVICE_REVOKED"),
            S3Error::Authorization(AuthorizeError::Revoked { .. })
        ));
        assert!(matches!(
            classify(403, "AUDIENCE_MISMATCH"),
            S3Error::Authorization(AuthorizeError::InvalidAudience { .. })
        ));
        assert!(matches!(
            classify(403, "SUBJECT_NOT_ALLOWED"),
            S3Error::Authorization(AuthorizeError::InvalidAudience { .. })
        ));
        assert!(matches!(
            classify(503, "REVOCATION_UNAVAILABLE"),
            S3Error::Rejected(Rejection::Unavailable { .. })
        ));
    }

    // Refused with nothing said about why: the honest reading is that
    // our proof did not satisfy it, not that any particular thing failed.
    #[dialog_common::test]
    async fn it_reads_a_bare_refusal_as_an_unproven_subject() {
        let refused = classify_response(
            403,
            b"not json",
            &did!("key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"),
            &did!("key:z6MkfQhLHBSFMuR7bQXTQeqe5kYUW51HpfZeaymgy1zkP2jM"),
        );
        assert!(matches!(
            refused,
            S3Error::Authorization(AuthorizeError::UnprovenSubject { .. })
        ));
    }

    // An unrecognized code must not be guessed at. Folding it into a
    // named reason would claim knowledge we do not have, and would
    // change meaning the day the service says something new.
    #[dialog_common::test]
    async fn it_does_not_guess_at_codes_it_does_not_know() {
        assert!(matches!(
            classify(500, "INTERNAL_ERROR"),
            S3Error::Rejected(Rejection::Unclassified { .. })
        ));
        assert!(matches!(
            classify(418, "SOMETHING_NEW"),
            S3Error::Rejected(Rejection::Unclassified { .. })
        ));
    }

    #[dialog_common::test]
    async fn it_bounds_and_rejects_malformed_service_responses() {
        let mut malformed = vec![b'x'; MAX_SERVICE_ERROR_BODY_BYTES * 2];
        malformed
            .extend_from_slice(br#"{"error":{"code":"LATE_CODE","message":"must not parse"}}"#);

        let error = classify_response(
            502,
            &malformed,
            &did!("key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"),
            &did!("key:z6MkfQhLHBSFMuR7bQXTQeqe5kYUW51HpfZeaymgy1zkP2jM"),
        );
        let rendered = error.to_string();
        assert!(
            !rendered.contains("LATE_CODE"),
            "a code past the bound must not be read, got {rendered}"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_preserves_a_service_response_through_redeem() {
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
            let body = r#"{"error":{"code":"CREDENTIAL_REVOKED","message":"Credential revoked"}}"#;
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
        // End to end: the service said CREDENTIAL_REVOKED over HTTP and
        // the caller receives the reason, with the subject filled in
        // from the invocation rather than from the response.
        let S3Error::Authorization(AuthorizeError::Revoked { subject }) = error else {
            panic!("expected a revoked authorization, got {error:?}");
        };
        assert_eq!(
            &subject,
            authorization.0.subject(),
            "the subject comes from the invocation we sent"
        );
    }
}
