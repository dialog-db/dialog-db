//! UCAN authorization utilities.
//!
//! Provides the [`CredentialBridge`] adapter for bridging capability-based
//! credential effects into UCAN's `Principal` + `Signer` interface, and
//! helper functions for building UCAN invocation arguments.

use crate::AccessError;
use dialog_capability::{
    DialogCapabilityAuthorizationError, Did, Provider, credential, ucan::Parameters,
};
use dialog_common::ConditionalSync;
use dialog_ucan::promise::Promised;
use dialog_varsig::eddsa::Ed25519Signature;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

use super::InvocationChain;

pub type Args = BTreeMap<String, Promised>;

/// Convert IPLD to Promised (for UCAN invocation arguments).
fn ipld_to_promised(ipld: Ipld) -> Promised {
    match ipld {
        Ipld::Null => Promised::Null,
        Ipld::Bool(b) => Promised::Bool(b),
        Ipld::Integer(i) => Promised::Integer(i),
        Ipld::Float(f) => Promised::Float(f),
        Ipld::String(s) => Promised::String(s),
        Ipld::Bytes(b) => Promised::Bytes(b),
        Ipld::Link(c) => Promised::Link(c),
        Ipld::List(l) => Promised::List(l.into_iter().map(ipld_to_promised).collect()),
        Ipld::Map(m) => Promised::Map(
            m.into_iter()
                .map(|(k, v)| (k, ipld_to_promised(v)))
                .collect(),
        ),
    }
}

/// Convert IPLD Map to BTreeMap<String, Promised> for UCAN invocation.
pub fn parameters_to_args(parameters: Parameters) -> Args {
    parameters
        .into_iter()
        .map(|(k, v)| (k, ipld_to_promised(v)))
        .collect()
}

/// Bridge adapter that wraps credential effects into a UCAN-compatible issuer.
///
/// This type implements `Principal` and `Signer<Ed25519Signature>` by
/// delegating to the credential effects on the environment. It allows
/// the UCAN `InvocationBuilder` to work with capability-based credential
/// operations.
pub(crate) struct CredentialBridge<'a, Env> {
    env: &'a Env,
    subject: Did,
    pub(crate) cached_did: Did,
}

impl<'a, Env> CredentialBridge<'a, Env>
where
    Env: Provider<credential::Identify> + Provider<credential::Sign> + ConditionalSync,
{
    pub(crate) async fn new(
        env: &'a Env,
        subject: Did,
    ) -> Result<CredentialBridge<'a, Env>, DialogCapabilityAuthorizationError> {
        let identify_cap = credential::Subject::from(subject.clone())
            .attenuate(credential::Credential)
            .attenuate(credential::Profile::default())
            .invoke(credential::Identify);

        let detail = <Env as Provider<credential::Identify>>::execute(env, identify_cap)
            .await
            .map_err(|e| DialogCapabilityAuthorizationError::Serialization(e.to_string()))?;
        let did = detail.operator;

        Ok(CredentialBridge {
            env,
            subject,
            cached_did: did,
        })
    }
}

impl<Env> dialog_varsig::Principal for CredentialBridge<'_, Env> {
    fn did(&self) -> Did {
        self.cached_did.clone()
    }
}

impl<Env> dialog_varsig::Signer<Ed25519Signature> for CredentialBridge<'_, Env>
where
    Env: Provider<credential::Sign> + ConditionalSync,
{
    async fn sign(&self, payload: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        let sign_cap = credential::Subject::from(self.subject.clone())
            .attenuate(credential::Credential)
            .attenuate(credential::Profile::default())
            .invoke(credential::Sign::new(payload));

        let bytes = self
            .env
            .execute(sign_cap)
            .await
            .map_err(signature::Error::from_source)?;

        Ed25519Signature::try_from(bytes.as_slice())
    }
}

/// A signed UCAN invocation ready to be sent to the access service.
///
/// Contains the signed invocation chain and metadata needed to POST
/// to the access service endpoint and receive back a presigned URL.
#[derive(Debug, Clone)]
pub struct UcanInvocation {
    pub(crate) endpoint: String,
    pub(crate) chain: Box<InvocationChain<Ed25519Signature>>,
    pub(crate) subject: Did,
    pub(crate) ability: String,
}

impl UcanInvocation {
    /// Get the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Get the ability path.
    pub fn ability(&self) -> &str {
        &self.ability
    }

    /// Get the invocation chain.
    pub fn chain(&self) -> &InvocationChain<Ed25519Signature> {
        &self.chain
    }

    /// POST the signed invocation to the access service and get back
    /// a presigned URL for the S3 operation.
    pub async fn grant(&self) -> Result<crate::AuthorizedRequest, AccessError> {
        let ucan = self.chain.to_bytes()?;

        let response = reqwest::Client::new()
            .post(&self.endpoint)
            .header("Content-Type", "application/cbor")
            .body(ucan)
            .send()
            .await
            .map_err(|e| AccessError::Service(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(AccessError::Service(format!(
                "Access service returned {}: {}",
                status, body
            )));
        }

        let body = response
            .bytes()
            .await
            .map_err(|e| AccessError::Service(e.to_string()))?;

        serde_ipld_dagcbor::from_slice(&body)
            .map_err(|e| AccessError::Service(format!("Failed to decode response: {}", e)))
    }
}
