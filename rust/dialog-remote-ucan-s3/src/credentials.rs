//! UCAN-based authorization via external access service.
//!
//! This module provides [`Credentials`], which implements [`credential::Remote`]
//! for delegating authorization to an external access service. The service
//! validates UCAN invocations and returns pre-signed URLs for S3 operations.
//!
//! # Overview
//!
//! The UCAN (User Controlled Authorization Networks) authorization flow works as follows:
//!
//! 1. An operator holds a delegation chain proving authority over a subject (identified by DID)
//! 2. When making S3 requests, the operator creates a UCAN invocation signed with their key
//! 3. The invocation and delegation proofs are sent to an access service
//! 4. The access service validates the chain and returns a pre-signed S3 URL
//! 5. The pre-signed URL is used to perform the actual S3 operation
//!
//! # Example
//!
//! ```rust
//! use dialog_remote_ucan_s3::{Credentials, DelegationChain};
//!
//! // Create credentials with an endpoint and delegation chain
//! // (delegation_chain would be obtained from a UCAN delegation)
//! # fn example(delegation_chain: DelegationChain) {
//! let credentials = Credentials::new(
//!     "https://access.example.com".to_string(),
//!     delegation_chain,
//! );
//! # }
//! ```

use crate::UcanInvocation;
use dialog_capability::{Capability, Constraint, Did, Provider, credential, ucan::parameters};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_ucan::InvocationBuilder;
use dialog_ucan::{DelegationChain, InvocationChain};

/// UCAN-based authorizer that delegates to an external access service.
///
/// Implements [`credential::Remote`] with the following lifecycle:
///
/// 1. `Authorize`: builds and signs a UCAN invocation -> `UcanInvocation`
/// 2. The invocation is POSTed to the access service -> `AuthorizedRequest`
///
/// # Example
///
/// ```rust
/// use dialog_remote_ucan_s3::{Credentials, DelegationChain};
///
/// # fn example(delegation_chain: DelegationChain) {
/// let credentials = Credentials::new(
///     "https://access.example.com".to_string(),
///     delegation_chain,
/// );
/// # }
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Credentials {
    /// The access service URL to POST invocations to.
    endpoint: String,
    /// The delegation chain proving authority from subject to operator.
    delegation: DelegationChain,
    /// Cached DID of the operator (audience of first delegation).
    audience: Did,
}

impl std::hash::Hash for Credentials {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.endpoint.hash(state);
        self.audience.hash(state);
        self.delegation.proof_cids().hash(state);
    }
}

impl Credentials {
    pub fn new(endpoint: String, delegation: DelegationChain) -> Self {
        Self {
            endpoint,
            audience: delegation.audience().clone(),
            delegation,
        }
    }

    /// Returns the operator's DID (audience of the delegation chain).
    pub fn audience(&self) -> &Did {
        &self.audience
    }

    /// Returns the access service URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns the access service URL as a &String reference.
    pub fn endpoint_string(&self) -> &String {
        &self.endpoint
    }

    /// Returns the delegation chain.
    pub fn delegation(&self) -> &DelegationChain {
        &self.delegation
    }
}

/// Build a UCAN invocation from delegation chain, endpoint, and capability.
///
/// This helper builds and signs a UCAN invocation for the given capability.
/// It requires the env to provide `Provider<credential::Identify>` and
/// `Provider<credential::Sign>` for UCAN signing.
pub async fn authorize<C, Env>(
    env: &Env,
    delegation_chain: Option<DelegationChain>,
    endpoint: String,
    capability: Capability<C>,
) -> Result<UcanInvocation, credential::AuthorizeError>
where
    C: Constraint + Clone + ConditionalSend + 'static,
    Capability<C>: ConditionalSend,
    Env: Provider<credential::Identify> + Provider<credential::Sign> + ConditionalSync,
{
    use crate::authorization::{CredentialBridge, parameters_to_args};

    let subject_did = capability.subject().clone();
    let ability = capability.ability();
    let params = parameters(&*capability);

    // Discover the operator's DID via credential::Identify effect.
    let identify_cap = credential::Subject::from(subject_did.clone())
        .attenuate(credential::Credential)
        .attenuate(credential::Profile::default())
        .invoke(credential::Identify);
    let detail = <Env as Provider<credential::Identify>>::execute(env, identify_cap)
        .await
        .map_err(|e| credential::AuthorizeError::Configuration(e.to_string()))?;
    let authority_did = detail.operator;

    // Self-authorization: when subject == authority, no delegation needed.
    let (delegation, proofs) = if subject_did == authority_did {
        (None, vec![])
    } else {
        // Delegated: need a delegation chain.
        let chain = delegation_chain.ok_or_else(|| {
            credential::AuthorizeError::Configuration(format!(
                "No delegation chain for subject '{}' (authority '{}')",
                subject_did, authority_did
            ))
        })?;
        // Verify authority matches the delegation chain audience.
        let chain_audience = chain.audience();
        if &authority_did != chain_audience {
            return Err(credential::AuthorizeError::Configuration(format!(
                "Authority '{}' does not match delegation chain audience '{}'",
                authority_did, chain_audience
            )));
        }
        let proofs = chain.proof_cids().into();
        (Some(chain), proofs)
    };

    // Build and sign the UCAN invocation using credential effects.
    let bridge = CredentialBridge::new(env, subject_did.clone())
        .await
        .map_err(|e| credential::AuthorizeError::Configuration(e.to_string()))?;

    let command: Vec<String> = ability
        .trim_start_matches('/')
        .split('/')
        .map(|s| s.to_string())
        .collect();

    let args = parameters_to_args(params);

    let invocation = InvocationBuilder::new()
        .issuer(bridge)
        .audience(&subject_did)
        .subject(&subject_did)
        .command(command)
        .arguments(args)
        .proofs(proofs)
        .try_build()
        .await
        .map_err(|e| credential::AuthorizeError::Denied(format!("{:?}", e)))?;

    let delegations = delegation
        .map(|c| c.delegations().clone())
        .unwrap_or_default();

    let chain = InvocationChain::new(invocation, delegations);

    Ok(UcanInvocation {
        endpoint,
        chain: Box::new(chain),
        subject: subject_did,
        ability,
    })
}

#[cfg(test)]
#[allow(dead_code)]
pub mod tests {
    use super::*;
    use crate::test_helpers::create_delegation;
    use async_trait::async_trait;
    use dialog_capability::{
        Capability, Constraint, Did, Effect, Policy, Principal, Provider, credential,
    };
    use dialog_common::ConditionalSend;
    use dialog_credentials::Ed25519Signer;
    use dialog_varsig::Signer;
    use dialog_varsig::eddsa::Ed25519Signature;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_service_worker);

    /// Helper to create a test delegation chain from subject to operator.
    pub async fn test_delegation_chain(
        subject_signer: &Ed25519Signer,
        operator: &impl Principal,
        ability: &[&str],
    ) -> DelegationChain {
        let delegation = create_delegation(subject_signer, operator, subject_signer, ability)
            .await
            .expect("Failed to create test delegation");
        DelegationChain::new(delegation)
    }

    #[derive(Clone)]
    pub struct Session {
        credentials: Credentials,
        signer: Ed25519Signer,
    }
    impl Session {
        pub async fn open(credentials: Credentials, secret: &[u8; 32]) -> Self {
            let signer = Ed25519Signer::import(secret)
                .await
                .expect("Failed to import signer");

            Session {
                signer,
                credentials,
            }
        }

        /// Get a reference to the credentials.
        pub fn credentials(&self) -> &Credentials {
            &self.credentials
        }
    }

    impl Principal for Session {
        fn did(&self) -> Did {
            self.signer.did()
        }
    }
    impl Signer<Ed25519Signature> for Session {
        async fn sign(&self, payload: &[u8]) -> Result<Ed25519Signature, signature::Error> {
            self.signer.sign(payload).await
        }
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
    impl Provider<credential::Identify> for Session {
        async fn execute(
            &self,
            _input: Capability<credential::Identify>,
        ) -> Result<credential::Identity, credential::CredentialError> {
            let did = self.signer.did();
            Ok(credential::Identity {
                profile: did.clone(),
                operator: did,
                account: None,
            })
        }
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
    impl Provider<credential::Sign> for Session {
        async fn execute(
            &self,
            input: Capability<credential::Sign>,
        ) -> Result<Vec<u8>, credential::CredentialError> {
            let payload = credential::Sign::of(&input).payload.as_slice();
            let sig: Ed25519Signature = Signer::sign(&self.signer, payload)
                .await
                .map_err(|e| credential::CredentialError::SigningFailed(e.to_string()))?;
            Ok(sig.to_bytes().to_vec())
        }
    }

    /// Session implements Provider<Authorize<C, Local>> for local authorization.
    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
    impl<C> Provider<credential::Authorize<C>> for Session
    where
        C: Effect + Constraint + ConditionalSend + 'static,
        C::Of: Constraint,
        Capability<C>: ConditionalSend,
        credential::Authorize<C>: ConditionalSend + 'static,
    {
        async fn execute(
            &self,
            input: Capability<credential::Authorize<C>>,
        ) -> Result<credential::Authorization<C, credential::Allow>, credential::AuthorizeError>
        {
            let authorize = input.into_inner().constraint;
            Ok(credential::Authorization::new(authorize.capability, ()))
        }
    }
}
