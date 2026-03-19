//! UCAN-based authorization via external access service.
//!
//! This module provides [`Credentials`], which implements [`Provider<storage::*>`]
//! by delegating authorization to an external access service. The service
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
//! use dialog_s3_credentials::ucan::{Credentials, DelegationChain};
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

use super::{DelegationChain, InvocationChain, UcanInvocation};
use crate::capability::{AccessError, AuthorizedRequest};
use async_trait::async_trait;
use dialog_capability::{
    Access, Authorized, Capability, Constraint, Did, Provider, credential, ucan::parameters,
};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_ucan::InvocationBuilder;

/// UCAN-based authorizer that delegates to an external access service.
///
/// This authorizer implements [`Provider<storage::*>`] by:
///
/// 1. Extracting the subject DID from the request URL path (first path segment)
/// 2. Looking up the delegation chain for that subject
/// 3. Building a UCAN invocation signed by the operator
/// 4. Sending the invocation to the access service
/// 5. Returning the pre-signed URL from the service's 307 redirect response
///
/// # Multi-Subject Support
///
/// A single `Credentials` can hold delegations for multiple subjects,
/// allowing access to data across different authorization domains without
/// needing separate authorizer instances.
///
/// # Example
///
/// ```rust
/// use dialog_s3_credentials::ucan::{Credentials, DelegationChain};
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
    /// Order: first delegation's `aud` matches operator, last delegation's `iss` matches subject.
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

    /// Returns the delegation chain.
    pub fn delegation(&self) -> &DelegationChain {
        &self.delegation
    }
}

/// Implement Access trait for Credentials.
///
/// This allows Credentials to authorize capability claims by building
/// fully signed UCAN invocations. The env provides credential effects
/// for identity discovery and signing.
#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
impl<C> Access<C> for Credentials
where
    C: Constraint + Clone + ConditionalSend + 'static,
    Capability<C>: ConditionalSend,
{
    type Authorization = AuthorizedRequest;
    type Error = AccessError;

    async fn authorize<Env>(
        &self,
        capability: Capability<C>,
        env: &Env,
    ) -> Result<Authorized<C, AuthorizedRequest>, Self::Error>
    where
        Env: Provider<credential::Identify> + Provider<credential::Sign> + ConditionalSync,
    {
        use super::authorization::{CredentialBridge, parameters_to_args};

        let credentials = self;
        let subject_did = capability.subject().clone();
        let ability = capability.ability();
        let params = parameters(&capability);

        // Discover the operator's DID via credential::Identify effect.
        let identify_cap = credential::Subject::from(subject_did.clone())
            .attenuate(credential::Credential)
            .invoke(credential::Identify);
        let authority_did: Did =
            <Env as Provider<credential::Identify>>::execute(env, identify_cap)
                .await
                .map_err(|e| AccessError::Configuration(e.to_string()))?;

        // Self-authorization: when subject == authority, no delegation needed.
        let (delegation, proofs) = if subject_did == authority_did {
            (None, vec![])
        } else {
            // Delegated: verify authority matches the delegation chain audience.
            let chain_audience = credentials.delegation.audience();
            if &authority_did != chain_audience {
                return Err(AccessError::Configuration(format!(
                    "Authority '{}' does not match delegation chain audience '{}'",
                    authority_did, chain_audience
                )));
            }
            (
                Some(credentials.delegation.clone()),
                credentials.delegation.proof_cids().into(),
            )
        };

        // Build and sign the UCAN invocation using credential effects.
        let bridge = CredentialBridge::new(env, subject_did.clone())
            .await
            .map_err(|e| AccessError::Invocation(e.to_string()))?;

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
            .map_err(|e| AccessError::Invocation(format!("{:?}", e)))?;

        let delegations = delegation
            .map(|c| c.delegations().clone())
            .unwrap_or_default();

        let chain = InvocationChain::new(invocation, delegations);

        let invocation = UcanInvocation {
            endpoint: credentials.endpoint.clone(),
            chain: Box::new(chain),
            subject: subject_did,
            ability,
        };

        // Presign immediately — POST the UCAN invocation to the access service
        let authorized_request = invocation.grant().await?;

        Ok(Authorized::new(capability, authorized_request))
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::ucan::delegation::helpers::create_delegation;
    use dialog_capability::{Capability, Constraint, Did, Principal, Provider, credential};
    use dialog_common::{ConditionalSend, ConditionalSync};
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
    }

    #[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
    impl<C> Access<C> for Session
    where
        C: Constraint + Clone + ConditionalSend + 'static,
        Capability<C>: ConditionalSend,
    {
        type Authorization = AuthorizedRequest;
        type Error = AccessError;

        async fn authorize<Env>(
            &self,
            capability: Capability<C>,
            env: &Env,
        ) -> Result<Authorized<C, Self::Authorization>, Self::Error>
        where
            Env: Provider<credential::Identify> + Provider<credential::Sign> + ConditionalSync,
        {
            self.credentials.authorize(capability, env).await
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
        ) -> Result<Did, credential::CredentialError> {
            Ok(self.signer.did())
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

}
