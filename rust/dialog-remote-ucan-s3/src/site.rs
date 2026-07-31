//! UCAN site configuration -- marker trait + address type.

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
use dialog_effects::service::ServiceResponseError;
use dialog_remote_s3::{Permit, S3Error, http_client};
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

fn service_response_error(status: u16, body: &[u8]) -> ServiceResponseError {
    let bounded = &body[..body.len().min(MAX_SERVICE_ERROR_BODY_BYTES)];
    match serde_json::from_slice::<ServiceErrorEnvelope>(bounded) {
        Ok(envelope) => ServiceResponseError::new(
            status,
            envelope.error.code,
            envelope
                .error
                .message
                .filter(|message| !message.is_empty())
                .unwrap_or_else(|| MALFORMED_SERVICE_ERROR_MESSAGE.to_string()),
        ),
        Err(_) => ServiceResponseError::new(status, None, MALFORMED_SERVICE_ERROR_MESSAGE),
    }
}

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
            .map_err(|e| S3Error::Authorization(e.to_string()))?;

        let response = http_client()
            .post(&address.endpoint)
            .header("Content-Type", "application/cbor")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = response.bytes().await.unwrap_or_default();
            return Err(service_response_error(status.as_u16(), &body).into());
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
        let identity = authority::Identify
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
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
/// A marker type -- no fields. Address info lives in `UcanAddress`.
#[derive(Debug, Clone, Copy, Default)]
pub struct UcanSite;

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

    #[dialog_common::test]
    async fn it_preserves_structured_service_responses() {
        for (status, code) in [
            (403, "CREDENTIAL_REVOKED"),
            (403, "DEVICE_REVOKED"),
            (409, "SYNC_CONFLICT"),
            (503, "REVOCATION_UNAVAILABLE"),
            (500, "INTERNAL_ERROR"),
        ] {
            let body = serde_json::json!({
                "error": {
                    "code": code,
                    "message": "bounded detail"
                }
            });
            let error = service_response_error(status, body.to_string().as_bytes());
            assert_eq!(error.status, status);
            assert_eq!(error.code.as_deref(), Some(code));
            assert_eq!(error.message, "bounded detail");
        }
    }

    #[dialog_common::test]
    async fn it_bounds_and_rejects_malformed_service_responses() {
        let mut malformed = vec![b'x'; MAX_SERVICE_ERROR_BODY_BYTES * 2];
        malformed
            .extend_from_slice(br#"{"error":{"code":"LATE_CODE","message":"must not parse"}}"#);

        let error = service_response_error(502, &malformed);
        assert_eq!(error.status, 502);
        assert_eq!(error.code, None);
        assert_eq!(error.message, MALFORMED_SERVICE_ERROR_MESSAGE);
        assert!(!error.message.contains("LATE_CODE"));
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
        let S3Error::ServiceResponse(error) = error else {
            panic!("expected structured service response");
        };
        assert_eq!(error.status, 403);
        assert_eq!(error.code.as_deref(), Some("CREDENTIAL_REVOKED"));
        assert_eq!(error.message, "Credential revoked");
    }
}
