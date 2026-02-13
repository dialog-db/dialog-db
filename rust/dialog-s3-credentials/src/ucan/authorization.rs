//! UCAN authorization proof management.
//!
//! This module provides [`UcanAuthorization`], which represents a proof of authority
//! for a specific capability claim using UCAN delegations.

use super::{DelegationChain, InvocationChain};
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use async_trait::async_trait;
use dialog_capability::{
    Authority, Authorization, Capability, DialogCapabilityAuthorizationError, Did, Effect,
    Provider, ucan::Parameters,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_ucan::InvocationBuilder;
use dialog_ucan::promise::Promised;
use dialog_varsig::eddsa::Ed25519Signature;
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

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
        chain: Box<InvocationChain<Ed25519Signature>>,
        subject: Did,
        ability: String,
        parameters: Parameters,
    },
}

impl UcanAuthorization {
    /// Create a self-issued authorization for an owner.
    pub fn owned<S, A>(endpoint: String, subject: S, ability: A, parameters: Parameters) -> Self
    where
        Did: From<S>,
        String: From<A>,
    {
        Self::Owned {
            endpoint,
            subject: subject.into(),
            ability: ability.into(),
            parameters,
        }
    }

    /// Create an authorization from a delegation chain.
    pub fn delegated<A>(
        endpoint: String,
        chain: DelegationChain,
        ability: A,
        parameters: Parameters,
    ) -> Self
    where
        String: From<A>,
    {
        // Pre-compute and cache the DID representations.
        // For powerline delegations (Subject::Any), use the root delegation's
        // issuer as the effective subject, since that's the original authority.
        let subject: Did = chain
            .subject()
            .cloned()
            .unwrap_or_else(|| chain.issuer().clone());
        let audience: Did = chain.audience().clone();

        Self::Delegated {
            endpoint,
            ability: String::from(ability),
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
    type Signature = Ed25519Signature;

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

    async fn invoke<
        A: Authority<Signature = Ed25519Signature> + Clone + ConditionalSend + ConditionalSync,
    >(
        &self,
        authority: &A,
    ) -> Result<Self, DialogCapabilityAuthorizationError> {
        let authority_did = dialog_capability::Principal::did(authority);
        if self.audience() != &authority_did {
            Err(DialogCapabilityAuthorizationError::NotAudience {
                audience: self.audience().clone(),
                issuer: authority_did,
            })
        } else {
            let subject_did = self.subject().clone();

            let command: Vec<String> = self
                .ability()
                .trim_start_matches('/')
                .split('/')
                .map(|s| s.to_string())
                .collect();

            let args = parameters_to_args(self.parameters().clone());

            let proofs = self
                .chain()
                .map(|c| c.proof_cids().into())
                .unwrap_or_default();

            let invocation = InvocationBuilder::new()
                .issuer(authority.clone())
                .audience(&subject_did)
                .subject(&subject_did)
                .command(command)
                .arguments(args)
                .proofs(proofs)
                .try_build()
                .await
                .map_err(|e| {
                    DialogCapabilityAuthorizationError::Serialization(format!("{:?}", e))
                })?;

            let delegations = self
                .chain()
                .map(|c| c.delegations().clone())
                .unwrap_or_default();

            let chain = InvocationChain::new(invocation, delegations);

            let authorization = Self::Invocation {
                endpoint: self.endpoint().into(),
                chain: Box::new(chain),
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
    use dialog_capability::did;
    use dialog_varsig::Principal;
    use ipld_core::ipld::Ipld;

    #[test]
    fn it_creates_owned_authorization() {
        let auth = UcanAuthorization::owned(
            "https://ucan.tonk.workers.dev".into(),
            did!("key:zTest"),
            "/storage/get",
            BTreeMap::default(),
        );

        assert_eq!(auth.subject(), &did!("key:zTest"));
        assert_eq!(auth.audience(), &did!("key:zTest"));
        assert_eq!(auth.ability(), "/storage/get");
        assert!(auth.chain().is_none());
        assert_eq!(auth.parameters(), &BTreeMap::default());
    }

    #[dialog_common::test]
    async fn it_creates_delegated_authorization() {
        let subject_signer = generate_signer().await;
        let subject_did = subject_signer.did();
        let operator_signer = generate_signer().await;

        let delegation = create_delegation(
            &subject_signer,
            &operator_signer,
            &subject_signer,
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

        assert_eq!(auth.subject(), &subject_did);
        assert_eq!(auth.audience(), &operator_signer.did());
        assert_eq!(auth.ability(), "/storage/get");
        assert_eq!(
            auth.parameters(),
            &BTreeMap::from([("key".to_string(), Ipld::Bytes(b"hello".into()))])
        );
        assert!(auth.chain().is_some());
    }

    #[dialog_common::test]
    async fn it_uses_root_issuer_as_subject_for_powerline_delegation() {
        use dialog_ucan::DelegationBuilder;
        use dialog_ucan::subject::Subject;

        let issuer_signer = generate_signer().await;
        let issuer_did = issuer_signer.did();
        let operator_signer = generate_signer().await;
        let operator_did = operator_signer.did();

        // Create a powerline delegation (Subject::Any) from issuer to operator
        let delegation = DelegationBuilder::new()
            .issuer(issuer_signer.clone())
            .audience(&operator_signer)
            .subject(Subject::Any)
            .command(vec!["storage".to_string()])
            .try_build()
            .await
            .unwrap();

        let chain = DelegationChain::new(delegation);
        // Verify the chain has no specific subject
        assert!(
            chain.subject().is_none(),
            "powerline delegation has no specific subject"
        );

        let auth = UcanAuthorization::delegated(
            "https://ucan.tonk.workers.dev".into(),
            chain,
            "/storage/get",
            BTreeMap::default(),
        );

        // The effective subject must be the root issuer, NOT the audience (operator).
        assert_eq!(
            auth.subject(),
            &issuer_did,
            "powerline delegation subject should be the root issuer"
        );
        assert_ne!(
            auth.subject(),
            &operator_did,
            "powerline delegation subject must not be the operator"
        );
        assert_eq!(auth.audience(), &operator_did);
    }
}
