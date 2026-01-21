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
//! ```ignore
//! use dialog_s3_credentials::ucan::{Credentials, OperatorIdentity, DelegationChain};
//!
//! // Create operator identity from secret key
//! let operator = OperatorIdentity::from_secret(&secret_key);
//!
//! // Build authorizer with delegation for a subject
//! let authorizer = Credentials::builder()
//!     .service_url("https://access.example.com")
//!     .operator(operator)
//!     .delegation("did:key:z6Mk...", delegation_chain)
//!     .build()?;
//! ```

use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;
use ucan::did::Ed25519Did;
use ucan::invocation::builder::InvocationBuilder;
use ucan::promise::Promised;

use super::authority::OperatorIdentity;
use super::authorization::UcanAuthorization;
use super::delegation::DelegationChain;
use super::invocation::InvocationChain;
use crate::access::{AuthorizationError, AuthorizedRequest};
use crate::capability::{archive, memory, storage};
use dialog_common::ConditionalSend;
use dialog_common::capability::{Ability, Access, Authorized, Capability, Provider, ToIpldArgs};

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
fn ipld_args_to_promised(ipld: Ipld) -> BTreeMap<String, Promised> {
    match ipld {
        Ipld::Map(m) => m
            .into_iter()
            .map(|(k, v)| (k, ipld_to_promised(v)))
            .collect(),
        _ => BTreeMap::new(),
    }
}

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
/// ```ignore
/// let authorizer = Credentials::builder()
///     .service_url("https://access.example.com")
///     .operator(operator)
///     .subject("did:key:z6MkSubject...")
///     .delegation(chain)
///     .build()?;
/// ```
#[derive(Debug, Clone)]
pub struct Credentials {
    /// The access service URL to POST invocations to.
    service_url: String,
    /// The operator identity (signs invocations).
    operator: OperatorIdentity,
    /// The subject DID (resource owner).
    subject: String,
    /// The delegation chain proving authority from subject to operator.
    /// Order: first delegation's `aud` matches operator, last delegation's `iss` matches subject.
    delegation: DelegationChain,
    /// HTTP client for making requests.
    client: reqwest::Client,
}

impl Credentials {
    /// Create a new builder for `Credentials`.
    pub fn builder() -> CredentialsBuilder {
        CredentialsBuilder::default()
    }

    /// Returns the access service URL.
    pub fn service_url(&self) -> &str {
        &self.service_url
    }

    /// Returns the operator identity.
    pub fn operator(&self) -> &OperatorIdentity {
        &self.operator
    }

    /// Returns the subject DID (resource owner).
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the delegation chain.
    pub fn delegation(&self) -> &DelegationChain {
        &self.delegation
    }

    /// Authorize a capability via the UCAN access service.
    ///
    /// This method:
    /// 1. Verifies the capability subject matches this credentials' subject
    /// 2. Creates a UCAN invocation for the capability
    /// 3. Builds an InvocationChain (UCAN container)
    /// 4. POSTs it to the access service
    /// 5. Returns the RequestDescriptor from the response
    async fn authorize<C: Ability + ToIpldArgs>(
        &self,
        capability: &C,
    ) -> Result<AuthorizedRequest, AuthorizationError> {
        let capability_subject = capability.subject();

        // 1. Verify the capability subject matches our delegation's subject
        if capability_subject != &self.subject {
            return Err(AuthorizationError::NoDelegation(format!(
                "Capability subject '{}' does not match credentials subject '{}'",
                capability_subject, self.subject
            )));
        }

        // 2. Parse subject DID
        let subject: Ed25519Did = self
            .subject
            .parse()
            .map_err(|e| AuthorizationError::Service(format!("Invalid subject DID: {:?}", e)))?;

        // 3. Get UCAN command path and arguments from capability
        let command_path = capability.command();
        let ucan_command: Vec<String> = command_path
            .trim_start_matches('/')
            .split('/')
            .map(|s| s.to_string())
            .collect();
        let args = ipld_args_to_promised(capability.to_ipld_args());

        // 4. Build invocation
        // - issuer: the operator (who is making the request)
        // - audience: the subject (per UCAN spec: "sub throughout MUST match the aud of the Invocation")
        // - subject: the resource being accessed
        // - proofs: delegation chain from subject to operator
        let invocation = InvocationBuilder::new()
            .issuer(self.operator.signer().clone())
            .audience(subject.clone())
            .subject(subject)
            .command(ucan_command)
            .arguments(args)
            .proofs(self.delegation.proof_cids().to_vec())
            .try_build()
            .map_err(|e| AuthorizationError::Invocation(format!("{:?}", e)))?;

        // 5. Build InvocationChain (UCAN container)
        let chain = InvocationChain::new(invocation, self.delegation.delegations().clone());

        // 6. Serialize to CBOR
        let container_bytes = chain.to_bytes()?;

        // 7. POST to access service
        let response = self
            .client
            .post(&self.service_url)
            .header("Content-Type", "application/cbor")
            .body(container_bytes)
            .send()
            .await
            .map_err(|e| AuthorizationError::Service(e.to_string()))?;

        // 8. Handle response
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(AuthorizationError::Service(format!(
                "Access service returned {}: {}",
                status, body
            )));
        }

        // 9. Decode response as RequestDescriptor
        let response_bytes = response
            .bytes()
            .await
            .map_err(|e| AuthorizationError::Service(e.to_string()))?;

        serde_ipld_dagcbor::from_slice(&response_bytes)
            .map_err(|e| AuthorizationError::Service(format!("Failed to decode response: {}", e)))
    }
}

// Implement Signer trait for Credentials
// This allows ucan::Credentials to be used with StorageClaim, MemoryClaim, etc.
use crate::access::S3Request as S3Claim;
use crate::access::Signer;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Signer for Credentials {
    fn subject(&self) -> &dialog_common::capability::Did {
        &self.subject
    }

    async fn sign<C: S3Claim + Send + Sync + 'static>(
        &self,
        claim: &C,
    ) -> Result<AuthorizedRequest, AuthorizationError> {
        // We need to dispatch to the appropriate authorize call based on the claim type.
        // Since we can't pattern match on type at runtime easily, we use Any.
        use crate::access::storage::{Delete, Get, List, Set, StorageClaim};
        use std::any::Any;

        let claim_any = claim as &dyn Any;

        // Try each StorageClaim variant
        if let Some(c) = claim_any.downcast_ref::<StorageClaim<Get>>() {
            return self.authorize(c).await;
        }
        if let Some(c) = claim_any.downcast_ref::<StorageClaim<Set>>() {
            return self.authorize(c).await;
        }
        if let Some(c) = claim_any.downcast_ref::<StorageClaim<Delete>>() {
            return self.authorize(c).await;
        }
        if let Some(c) = claim_any.downcast_ref::<StorageClaim<List>>() {
            return self.authorize(c).await;
        }

        // Add memory and archive claims as needed...
        Err(AuthorizationError::Service(
            "Unsupported claim type for UCAN authorization".to_string(),
        ))
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
    type Error = AuthorizationError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: dialog_common::capability::Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        // Verify the claim's subject matches our delegation's subject
        if claim.subject() != &self.subject {
            return Err(AuthorizationError::NoDelegation(format!(
                "Claim subject '{}' does not match credentials subject '{}'",
                claim.subject(),
                self.subject
            )));
        }

        // Verify the claim's audience matches the first delegation's audience
        // Per UCAN spec: first delegation's `aud` should match the invoker
        let chain_audience_str = self.delegation.audience().to_string();
        if claim.audience() != &chain_audience_str {
            return Err(AuthorizationError::Configuration(format!(
                "Claim audience '{}' does not match delegation chain audience '{}'",
                claim.audience(),
                chain_audience_str
            )));
        }

        // Return authorization from the delegation chain
        Ok(UcanAuthorization::delegated(self.delegation.clone()))
    }
}

// --- Provider implementations for authorized capabilities ---
//
// These implementations allow UCAN credentials to execute authorized capabilities
// via the access service. Each takes `Authorized<Fx, UcanAuthorization>` where
// `Fx` is the effect type.

// Provider for storage::Get
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<storage::Get, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<storage::Get, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize storage::Get")
    }
}

// Provider for storage::Set
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<storage::Set, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<storage::Set, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize storage::Set")
    }
}

// Provider for storage::Delete
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<storage::Delete, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<storage::Delete, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize storage::Delete")
    }
}

// Provider for storage::List
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<storage::List, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<storage::List, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize storage::List")
    }
}

// Provider for memory::Resolve
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<memory::Resolve, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<memory::Resolve, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize memory::Resolve")
    }
}

// Provider for memory::Publish
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<memory::Publish, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<memory::Publish, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize memory::Publish")
    }
}

// Provider for memory::Retract
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<memory::Retract, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<memory::Retract, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize memory::Retract")
    }
}

// Provider for archive::Get
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<archive::Get, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<archive::Get, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize archive::Get")
    }
}

// Provider for archive::Put
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Authorized<archive::Put, UcanAuthorization>> for Credentials {
    async fn execute(
        &mut self,
        authorized: Authorized<archive::Put, UcanAuthorization>,
    ) -> AuthorizedRequest {
        self.authorize(authorized.capability())
            .await
            .expect("Failed to authorize archive::Put")
    }
}

/// Builder for [`Credentials`].
///
/// Use this to construct a `Credentials` with the required configuration.
///
/// # Required Fields
///
/// - `service_url`: The URL of the access service
/// - `operator`: The operator identity for signing invocations
/// - `subject`: The subject DID (resource owner)
/// - `delegation`: The delegation chain proving authority
#[derive(Default)]
pub struct CredentialsBuilder {
    service_url: Option<String>,
    operator: Option<OperatorIdentity>,
    subject: Option<String>,
    delegation: Option<DelegationChain>,
}

impl CredentialsBuilder {
    /// Set the access service URL.
    ///
    /// This is the base URL of the service that will validate UCAN invocations
    /// and return pre-signed S3 URLs.
    pub fn service_url(mut self, url: impl Into<String>) -> Self {
        self.service_url = Some(url.into());
        self
    }

    /// Set the operator identity for signing invocations.
    ///
    /// The operator is the entity making requests. They must have been
    /// delegated authority by the subject.
    pub fn operator(mut self, operator: OperatorIdentity) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Set the subject DID (resource owner).
    ///
    /// This is the DID that owns the resources being accessed.
    pub fn subject(mut self, subject_did: impl Into<String>) -> Self {
        self.subject = Some(subject_did.into());
        self
    }

    /// Set the delegation chain proving authority from subject to operator.
    ///
    /// The chain order should be: first delegation's `aud` matches operator,
    /// last delegation's `iss` matches subject.
    pub fn delegation(mut self, chain: DelegationChain) -> Self {
        self.delegation = Some(chain);
        self
    }

    /// Build the authorizer.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing or if the HTTP client
    /// cannot be constructed.
    pub fn build(self) -> Result<Credentials, AuthorizationError> {
        let service_url = self
            .service_url
            .ok_or_else(|| AuthorizationError::Configuration("service_url is required".into()))?;

        let operator = self
            .operator
            .ok_or_else(|| AuthorizationError::Configuration("operator is required".into()))?;

        let subject = self
            .subject
            .ok_or_else(|| AuthorizationError::Configuration("subject is required".into()))?;

        let delegation = self
            .delegation
            .ok_or_else(|| AuthorizationError::Configuration("delegation is required".into()))?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| AuthorizationError::Service(e.to_string()))?;

        Ok(Credentials {
            service_url,
            operator,
            subject,
            delegation,
            client,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::delegation::tests::{create_delegation, generate_signer};
    use super::*;

    /// Helper to create a test delegation chain from subject to operator.
    fn test_delegation_chain(
        subject_signer: &ucan::did::Ed25519Signer,
        operator_did: &Ed25519Did,
    ) -> DelegationChain {
        let subject_did = subject_signer.did().clone();
        let delegation = create_delegation(
            subject_signer,
            operator_did,
            &subject_did,
            vec!["storage".to_string()],
        )
        .expect("Failed to create test delegation");
        DelegationChain::new(delegation)
    }

    #[test]
    fn test_builder_missing_service_url() {
        let subject_signer = generate_signer();
        let operator = OperatorIdentity::from_secret(&[0u8; 32]);
        let chain = test_delegation_chain(&subject_signer, &operator.did());

        let result = Credentials::builder()
            .operator(operator)
            .subject(subject_signer.did().to_string())
            .delegation(chain)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("service_url"));
    }

    #[test]
    fn test_builder_missing_operator() {
        let subject_signer = generate_signer();
        let operator = OperatorIdentity::from_secret(&[0u8; 32]);
        let chain = test_delegation_chain(&subject_signer, &operator.did());

        let result = Credentials::builder()
            .service_url("https://example.com")
            .subject(subject_signer.did().to_string())
            .delegation(chain)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("operator"));
    }

    #[test]
    fn test_builder_missing_subject() {
        let subject_signer = generate_signer();
        let operator = OperatorIdentity::from_secret(&[0u8; 32]);
        let chain = test_delegation_chain(&subject_signer, &operator.did());

        let result = Credentials::builder()
            .service_url("https://example.com")
            .operator(operator)
            .delegation(chain)
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("subject"));
    }

    #[test]
    fn test_builder_missing_delegation() {
        let result = Credentials::builder()
            .service_url("https://example.com")
            .operator(OperatorIdentity::from_secret(&[0u8; 32]))
            .subject("did:key:z6MkTest")
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("delegation"));
    }

    #[test]
    fn test_builder_success() {
        let subject_signer = generate_signer();
        let operator = OperatorIdentity::from_secret(&[0u8; 32]);
        let chain = test_delegation_chain(&subject_signer, &operator.did());
        let subject_did = subject_signer.did().to_string();

        let authorizer = Credentials::builder()
            .service_url("https://access.example.com")
            .operator(operator)
            .subject(&subject_did)
            .delegation(chain)
            .build()
            .unwrap();

        assert_eq!(authorizer.service_url(), "https://access.example.com");
        assert_eq!(authorizer.subject(), &subject_did);
    }
}
