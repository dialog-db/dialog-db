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

use super::{DelegationChain, UcanAuthorization};
use crate::capability::{AccessError, AuthorizedRequest, S3Request};
use async_trait::async_trait;
use dialog_capability::{
    Ability, Access, Authorized, Capability, Claim, Did, Effect, Parameters, Provider,
};
use dialog_common::ConditionalSend;

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

impl Credentials {
    pub fn new(endpoint: String, delegation: DelegationChain) -> Self {
        Self {
            endpoint,
            audience: delegation.audience().into(),
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
/// This allows Credentials to find authorization proofs for capability claims
/// by looking up delegation chains for the subject.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Access for Credentials {
    type Authorization = UcanAuthorization;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        // Verify the claim's subject matches our delegation's subject

        // Verify the claim's audience matches the first delegation's audience
        // Per UCAN spec: first delegation's `aud` should match the invoker
        let audience = self.delegation.audience().to_string();
        if claim.audience() != &audience {
            return Err(AccessError::Configuration(format!(
                "Claim audience '{}' does not match delegation chain audience '{}'",
                claim.audience(),
                audience
            )));
        }

        let mut parameters = Parameters::new();
        claim.capability().parametrize(&mut parameters);

        // Return authorization from the delegation chain
        Ok(UcanAuthorization::delegated(
            self.endpoint.clone(),
            self.delegation.clone(),
            claim.capability().ability(),
            parameters,
        ))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Do> Provider<Authorized<Do, UcanAuthorization>> for Credentials
where
    Do: Effect<Output = Result<AuthorizedRequest, AccessError>> + 'static,
    Capability<Do>: ConditionalSend + S3Request,
{
    async fn execute(
        &mut self,
        authorized: Authorized<Do, UcanAuthorization>,
    ) -> Result<AuthorizedRequest, AccessError> {
        authorized
            .authorization()
            .grant(authorized.capability())
            .await
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::capability::archive;
    use crate::ucan::delegation::tests::create_delegation;
    use anyhow;
    use dialog_capability::{Authority, Authorization, Did, Principal, Subject};
    use dialog_common::Blake3Hash;
    use ed25519_dalek::ed25519::signature::SignerMut;
    use ucan::did::{Ed25519Did, Ed25519Signer};
    use ucan::promise::Promised;

    /// Helper to create a test delegation chain from subject to operator.
    pub fn test_delegation_chain(
        subject_signer: &ucan::did::Ed25519Signer,
        operator_did: &Ed25519Did,
        ability: &[&str],
    ) -> DelegationChain {
        let subject_did = subject_signer.did().clone();
        let delegation = create_delegation(subject_signer, operator_did, &subject_did, ability)
            .expect("Failed to create test delegation");
        DelegationChain::new(delegation)
    }

    pub struct Session {
        credentials: Credentials,
        signer: ed25519_dalek::SigningKey,
        did: Did,
    }
    impl Session {
        pub fn new(credentials: Credentials, secret: &[u8; 32]) -> Self {
            let signer = ed25519_dalek::SigningKey::from_bytes(secret);

            Session {
                did: Ed25519Signer::from(signer.clone()).did().into(),
                signer,
                credentials,
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Access for Session {
        type Authorization = UcanAuthorization;
        type Error = AccessError;

        async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
            &self,
            claim: Claim<C>,
        ) -> Result<Self::Authorization, Self::Error> {
            self.credentials.claim(claim).await
        }
    }
    impl Principal for Session {
        fn did(&self) -> &Did {
            &self.did
        }
    }
    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl Authority for Session {
        async fn sign(&mut self, payload: &[u8]) -> Result<Vec<u8>, dialog_capability::SignError> {
            Ok(self.signer.sign(payload).to_vec())
        }
        fn secret_key_bytes(&self) -> Option<[u8; 32]> {
            self.signer.to_bytes().into()
        }
    }

    #[dialog_common::test]
    async fn it_acquires_access() -> anyhow::Result<()> {
        let signer = ed25519_dalek::SigningKey::from_bytes(&[0u8; 32]);
        let operator = Ed25519Signer::from(signer);

        let credentials = Credentials::new(
            "https://access.ucan.com".into(),
            test_delegation_chain(&operator, &operator.did(), &["archive"]),
        );

        let mut session = Session::new(credentials, &[0u8; 32]);

        let read = Subject::from(session.did().to_string())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog {
                catalog: "blobs".into(),
            })
            .invoke(archive::Get {
                digest: Blake3Hash::hash(b"hello"),
            })
            .acquire(&mut session)
            .await?;

        let authorization = read.authorization().invoke(&session)?;

        let ucan = match authorization {
            UcanAuthorization::Invocation { chain, .. } => chain,
            _ => panic!("expected invocation"),
        };

        assert_eq!(ucan.invocation.command().to_string(), "/archive/get");
        assert_eq!(
            ucan.invocation.subject().to_string(),
            session.did().to_string()
        );
        assert_eq!(ucan.verify().await?, ());

        assert_eq!(
            ucan.arguments().get("catalog"),
            Some(&Promised::String("blobs".into()))
        );
        assert_eq!(
            ucan.arguments().get("digest"),
            Some(&Promised::Bytes(
                Blake3Hash::hash(b"hello").as_bytes().into()
            ))
        );

        Ok(())
    }
}
