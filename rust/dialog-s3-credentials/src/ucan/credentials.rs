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
    Ability, Access, Authorized, Capability, Claim, Did, Effect, Provider, ucan::parameters,
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
#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    async_trait::async_trait
)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
impl Access for Credentials {
    type Authorization = UcanAuthorization;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        // Self-authorization: when subject == audience, no delegation needed.
        // The subject is acting on itself, which is inherently authorized.
        if claim.subject() == claim.audience() {
            return Ok(UcanAuthorization::owned(
                self.endpoint.clone(),
                claim.subject().clone(),
                claim.capability().ability(),
                parameters(claim.capability()),
            ));
        }

        // Delegated authorization: verify the claim's audience matches the delegation chain.
        // Per UCAN spec: first delegation's `aud` should match the invoker.
        let audience = self.delegation.audience().to_string();
        if claim.audience() != &audience {
            return Err(AccessError::Configuration(format!(
                "Claim audience '{}' does not match delegation chain audience '{}'",
                claim.audience(),
                audience
            )));
        }

        // Return authorization from the delegation chain
        Ok(UcanAuthorization::delegated(
            self.endpoint.clone(),
            self.delegation.clone(),
            claim.capability().ability(),
            parameters(claim.capability()),
        ))
    }
}

#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), async_trait)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait(?Send))]
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
    use crate::ucan::delegation::helpers::create_delegation;
    use anyhow;
    use dialog_capability::{Authority, Authorization, Did, Principal, Subject};
    use dialog_common::Blake3Hash;
    use ed25519_dalek::ed25519::signature::SignerMut;
    use ucan::did::{Ed25519Did, Ed25519Signer};
    use ucan::promise::Promised;

    /// Helper to create a test delegation chain from subject to operator.
    pub async fn test_delegation_chain(
        subject_signer: &ucan::did::Ed25519Signer,
        operator_did: &Ed25519Did,
        ability: &[&str],
    ) -> DelegationChain {
        let subject_did = subject_signer.did();
        let delegation = create_delegation(subject_signer, operator_did, subject_did, ability)
            .await
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

    #[cfg_attr(
        not(all(target_arch = "wasm32", target_os = "unknown")),
        async_trait::async_trait
    )]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
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
    #[cfg_attr(
        not(all(target_arch = "wasm32", target_os = "unknown")),
        async_trait::async_trait
    )]
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), async_trait::async_trait(?Send))]
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
            test_delegation_chain(&operator, operator.did(), &["archive"]).await,
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

        let authorization = read.authorization().invoke(&session).await?;

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

    #[dialog_common::test]
    async fn it_returns_owned_authorization_for_self_claim() -> anyhow::Result<()> {
        // Create a signer where subject == operator (self-authorization)
        let signer = ed25519_dalek::SigningKey::from_bytes(&[0u8; 32]);
        let operator = Ed25519Signer::from(signer);

        // Create credentials with a delegation (won't be used for self-auth)
        let credentials = Credentials::new(
            "https://access.ucan.com".into(),
            test_delegation_chain(&operator, operator.did(), &["archive"]).await,
        );

        let mut session = Session::new(credentials, &[0u8; 32]);

        // Create a capability where subject == session.did() (self-authorization)
        let capability = Subject::from(session.did().to_string())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog {
                catalog: "blobs".into(),
            })
            .invoke(archive::Get {
                digest: Blake3Hash::hash(b"hello"),
            });

        // Acquire authorization - should return Owned since subject == audience
        let authorized = capability.acquire(&mut session).await?;

        // Verify it's an Owned authorization
        match authorized.authorization() {
            UcanAuthorization::Owned { subject, .. } => {
                assert_eq!(subject, session.did());
            }
            _ => panic!("Expected Owned authorization for self-claim"),
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_allows_self_authorization_with_different_delegation_audience() -> anyhow::Result<()>
    {
        // Create an operator signer for the delegation chain
        let operator_signer = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let operator = Ed25519Signer::from(operator_signer);

        // Create credentials with delegation chain audience = operator_did
        let credentials = Credentials::new(
            "https://access.ucan.com".into(),
            test_delegation_chain(&operator, operator.did(), &["archive"]).await,
        );

        // Create a session with a DIFFERENT key (session.did() != operator_did)
        let mut session = Session::new(credentials, &[2u8; 32]);

        // Verify the DIDs are different
        let session_did = session.did().to_string();
        let operator_did = operator.did().to_string();
        assert_ne!(
            session_did, operator_did,
            "Session DID should differ from operator DID for this test"
        );

        // Create a capability where subject == session.did() (self-authorization case)
        // This means: subject == claim.audience (since acquire sets audience = session.did())
        let capability = Subject::from(session.did().to_string())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog {
                catalog: "blobs".into(),
            })
            .invoke(archive::Get {
                digest: Blake3Hash::hash(b"hello"),
            });

        // This should succeed because subject == audience (self-authorization),
        // but currently fails because claim.audience != delegation.audience
        let result = capability.acquire(&mut session).await;

        // Assert this is an error due to the current implementation order
        // The error message shows it's checking delegation audience before self-auth
        assert!(
            result.is_ok(),
            "Self-authorization should work regardless of delegation chain audience. Error: {:?}",
            result.err()
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_invokes_and_verifies_self_authorization() -> anyhow::Result<()> {
        // Create a signer where subject == operator (self-authorization)
        let signer = ed25519_dalek::SigningKey::from_bytes(&[0u8; 32]);
        let operator = Ed25519Signer::from(signer);

        let credentials = Credentials::new(
            "https://access.ucan.com".into(),
            test_delegation_chain(&operator, operator.did(), &["archive"]).await,
        );

        let mut session = Session::new(credentials, &[0u8; 32]);

        // Create a capability where subject == session.did() (self-authorization)
        let authorized = Subject::from(session.did().to_string())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog {
                catalog: "blobs".into(),
            })
            .invoke(archive::Get {
                digest: Blake3Hash::hash(b"hello"),
            })
            .acquire(&mut session)
            .await?;

        // Invoke the authorization - should create an Invocation with empty proofs
        let authorization = authorized.authorization().invoke(&session).await?;

        let ucan = match authorization {
            UcanAuthorization::Invocation { chain, .. } => chain,
            _ => panic!("Expected Invocation after invoke()"),
        };

        // Verify the invocation properties
        assert_eq!(ucan.invocation.command().to_string(), "/archive/get");
        assert_eq!(
            ucan.invocation.subject().to_string(),
            session.did().to_string()
        );
        assert_eq!(
            ucan.invocation.issuer().to_string(),
            session.did().to_string()
        );
        // Self-invocation should have empty proofs
        assert!(
            ucan.invocation.proofs().is_empty(),
            "Self-invocation should have empty proofs"
        );

        // Verify the chain - self-invocation (issuer == subject) should pass
        assert_eq!(ucan.verify().await?, ());

        Ok(())
    }

    /// WebCrypto-specific tests for browser WASM.
    ///
    /// These tests verify that the UCAN authorization flow works correctly
    /// with WebCrypto-backed signers in browser environments. They exercise
    /// non-extractable key generation, async signing, and signature verification
    /// using the Web Crypto API.
    ///
    /// Run with: `wasm-pack test --headless --chrome rust/dialog-s3-credentials`
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    mod webcrypto_tests {
        use signature::Verifier;
        use ucan::did::Did;
        use ucan::{AsyncDidSigner, WebCryptoEd25519Signer};
        use wasm_bindgen_test::wasm_bindgen_test_configure;

        wasm_bindgen_test_configure!(run_in_service_worker);

        #[dialog_common::test]
        async fn it_generates_webcrypto_signer() {
            let signer = WebCryptoEd25519Signer::generate()
                .await
                .expect("Failed to generate WebCrypto signer");

            let did_str = signer.did().to_string();
            assert!(
                did_str.starts_with("did:key:z"),
                "DID should start with 'did:key:z', got: {}",
                did_str
            );
        }

        #[dialog_common::test]
        async fn it_produces_valid_webcrypto_signature() {
            let signer = WebCryptoEd25519Signer::generate()
                .await
                .expect("Failed to generate signer");
            let msg = b"test message for WebCrypto signing";

            let signature = signer.sign(msg).await.expect("Failed to sign message");

            let verifier = signer.did().verifier();
            verifier
                .verify(msg, &signature)
                .expect("Signature verification failed");
        }

        #[dialog_common::test]
        async fn it_rejects_wrong_message() {
            let signer = WebCryptoEd25519Signer::generate()
                .await
                .expect("Failed to generate signer");
            let msg = b"original message";
            let wrong_msg = b"wrong message";

            let signature = signer.sign(msg).await.expect("Failed to sign message");

            let verifier = signer.did().verifier();
            assert!(
                verifier.verify(wrong_msg, &signature).is_err(),
                "Verification should fail for wrong message"
            );
        }
    }
}
