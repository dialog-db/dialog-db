//! UCAN authorization proof management.
//!
//! This module provides [`UcanAuthorization`], which represents a proof of authority
//! for a specific capability claim using UCAN delegations.

use super::{DelegationChain, InvocationChain};
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use async_trait::async_trait;
use dialog_capability::{
    Authority, Authorization, AuthorizationError, Capability, Did, Effect, Provider,
    ucan::Parameters,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use ed25519_dalek::SigningKey;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;
use ucan::did::{Ed25519Did, Ed25519Signer};
use ucan::invocation::builder::InvocationBuilder;
use ucan::promise::Promised;

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

/// UCAN-based authorization proof for a capability.
///
/// This enum represents authorization in two forms:
/// - `Owned`: The subject is the same as the audience (self-authorization)
/// - `Delegated`: Authority is proven through a UCAN delegation chain
#[derive(Debug, Clone)]
pub enum UcanAuthorization {
    /// Self-authorization where subject == audience.
    Owned {
        endpoint: String,
        /// The subject DID (also the audience).
        subject: Did,
        /// The ability path this authorization permits.
        ability: String,
        /// Constraints of the delegation
        parameters: Parameters,
    },
    /// Authorization through a delegation chain.
    Delegated {
        endpoint: String,
        /// The delegation chain proving authority.
        chain: DelegationChain,
        /// Cached subject DID string.
        subject: Did,
        /// Cached audience DID string.
        audience: Did,
        /// Cached ability path.
        ability: String,
        /// Constraints of the delegation
        parameters: Parameters,
    },
    Invocation {
        endpoint: String,
        chain: InvocationChain,
        subject: Did,
        ability: String,
        parameters: Parameters,
    },
}

impl UcanAuthorization {
    /// Create a self-issued authorization for an owner.
    pub fn owned(
        endpoint: String,
        subject: impl Into<Did>,
        ability: impl Into<String>,
        parameters: Parameters,
    ) -> Self {
        Self::Owned {
            endpoint,
            subject: subject.into(),
            ability: ability.into(),
            parameters,
        }
    }

    /// Create an authorization from a delegation chain.
    pub fn delegated(
        endpoint: String,
        chain: DelegationChain,
        ability: impl Into<String>,
        parameters: Parameters,
    ) -> Self {
        // Pre-compute and cache the DID representations
        let subject: Did = chain.subject().map(|did| did.into()).unwrap_or_default();
        let audience: Did = chain.audience().into();

        Self::Delegated {
            endpoint,
            ability: ability.into(),
            chain,
            subject,
            audience,
            parameters,
        }
    }

    fn endpoint(&self) -> &str {
        match self {
            Self::Owned { endpoint, .. } => endpoint,
            Self::Delegated { endpoint, .. } => endpoint,
            Self::Invocation { endpoint, .. } => endpoint,
        }
    }

    /// Get the delegation chain, if this is a delegated authorization.
    pub fn chain(&self) -> Option<&DelegationChain> {
        match self {
            Self::Owned { .. } => None,
            Self::Delegated { chain, .. } => Some(chain),
            Self::Invocation { .. } => None,
        }
    }

    fn parameters(&self) -> &Parameters {
        match self {
            Self::Owned { parameters, .. } => parameters,
            Self::Delegated { parameters, .. } => parameters,
            Self::Invocation { parameters, .. } => parameters,
        }
    }

    /// Executes authorized effect
    pub async fn grant<C: Effect>(
        &self,
        capability: &Capability<C>,
    ) -> Result<AuthorizedRequest, AccessError> {
        if capability.ability() != self.ability() {
            Err(AccessError::Invocation(format!(
                "Authorization error: {} not authorized",
                capability.ability(),
            )))?;
        }

        // Serialize authorization - must be an Invocation variant after invoke() is called
        let ucan = match self {
            Self::Invocation { chain, .. } => chain.to_bytes()?,
            Self::Delegated { .. } => {
                return Err(AccessError::Invocation(
                    "Authorization not invoked - call invoke() before grant()".into(),
                ));
            }
            Self::Owned { .. } => {
                return Err(AccessError::Invocation(
                    "Owned authorization cannot be granted via UCAN service".into(),
                ));
            }
        };

        // POST to access service
        let response = reqwest::Client::new()
            .post(self.endpoint())
            .header("Content-Type", "application/cbor")
            .body(ucan)
            .send()
            .await
            .map_err(|e| AccessError::Service(e.to_string()))?;

        // Handle response
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(AccessError::Service(format!(
                "Access service returned {}: {}",
                status, body
            )));
        }

        // Decode response as RequestDescriptor
        let body = response
            .bytes()
            .await
            .map_err(|e| AccessError::Service(e.to_string()))?;

        serde_ipld_dagcbor::from_slice(&body)
            .map_err(|e| AccessError::Service(format!("Failed to decode response: {}", e)))
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl Authorization for UcanAuthorization {
    fn subject(&self) -> &Did {
        match self {
            Self::Owned { subject, .. } => subject,
            Self::Delegated { subject, .. } => subject,
            Self::Invocation { subject, .. } => subject,
        }
    }

    fn audience(&self) -> &Did {
        match self {
            Self::Owned { subject, .. } => subject, // For owned, audience == subject
            Self::Delegated { audience, .. } => audience,
            Self::Invocation { subject, .. } => subject,
        }
    }

    fn ability(&self) -> &str {
        match self {
            Self::Owned { ability, .. } => ability,
            Self::Delegated { ability, .. } => ability,
            Self::Invocation { ability, .. } => ability,
        }
    }

    async fn invoke<A: Authority + ConditionalSend + ConditionalSync>(
        &self,
        authority: &A,
    ) -> Result<Self, AuthorizationError> {
        if self.audience() != authority.did() {
            Err(AuthorizationError::NotAudience {
                audience: self.audience().into(),
                issuer: authority.did().into(),
            })
        } else {
            let subject: Ed25519Did = self.subject().parse().map_err(|e| {
                AuthorizationError::Serialization(format!("Invalid subject DID: {:?}", e))
            })?;

            let command: Vec<String> = self
                .ability()
                .trim_start_matches('/')
                .split('/')
                .map(|s| s.to_string())
                .collect();

            let args = parameters_to_args(self.parameters().clone());

            let key = SigningKey::from_bytes(&authority.secret_key_bytes().ok_or(
                AuthorizationError::Serialization("Authority key can not be used".into()),
            )?);
            let issuer = Ed25519Signer::from(key);
            let proofs = self
                .chain()
                .map(|c| c.proof_cids().into())
                .unwrap_or_default();

            let invocation = InvocationBuilder::new()
                .issuer(issuer.clone())
                .audience(subject)
                .subject(subject)
                .command(command)
                .arguments(args)
                .proofs(proofs)
                .try_build(&issuer)
                .await
                .map_err(|e| AuthorizationError::Serialization(format!("{:?}", e)))?;

            let delegations = self
                .chain()
                .map(|c| c.delegations().clone())
                .unwrap_or_default();

            let invocation = InvocationChain::new(invocation, delegations);

            let authorization = Self::Invocation {
                endpoint: self.endpoint().into(),
                chain: invocation,
                subject: self.subject().clone(),
                ability: self.ability().into(),
                parameters: self.parameters().clone(),
            };

            Ok(authorization)
        }
    }
}

/// Blanket implementation provider ability to
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl<Do> Provider<Do> for UcanAuthorization
where
    Do: Effect<Output = Result<AuthorizedRequest, AccessError>> + 'static,
    Capability<Do>: ConditionalSend + S3Request,
{
    async fn execute(
        &mut self,
        capability: Capability<Do>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.grant(&capability).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::ucan::delegation::helpers::{create_delegation, generate_signer};
    use ipld_core::ipld::Ipld;

    #[test]
    fn it_creates_owned_authorization() {
        let auth = UcanAuthorization::owned(
            "https://ucan.tonk.workers.dev".into(),
            "did:key:zTest",
            "/storage/get",
            BTreeMap::default(),
        );

        assert_eq!(auth.subject(), "did:key:zTest");
        assert_eq!(auth.audience(), "did:key:zTest");
        assert_eq!(auth.ability(), "/storage/get");
        assert!(auth.chain().is_none());
        assert_eq!(auth.parameters(), &BTreeMap::default());
    }

    #[dialog_common::test]
    async fn it_creates_delegated_authorization() {
        let subject_signer = generate_signer();
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer();

        let delegation = create_delegation(
            &subject_signer,
            operator_signer.did(),
            subject_did,
            &["storage", "get"],
        )
        .await
        .unwrap();

        let chain = DelegationChain::new(delegation);
        let auth = UcanAuthorization::delegated(
            "https://ucan.tonk.workers.dev".into(),
            chain,
            "/storage/get",
            BTreeMap::from([("key".to_string(), Ipld::Bytes(b"hello".into()))]),
        );

        assert_eq!(auth.subject(), &subject_did.to_string());
        assert_eq!(auth.audience(), &operator_signer.did().to_string());
        assert_eq!(auth.ability(), "/storage/get");
        assert_eq!(
            auth.parameters(),
            &BTreeMap::from([("key".to_string(), Ipld::Bytes(b"hello".into()))])
        );
        assert!(auth.chain().is_some());
    }
}
