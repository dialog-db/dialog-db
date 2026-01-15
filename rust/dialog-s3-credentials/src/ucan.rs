//! UCAN-based authorization via external access service.
//!
//! This module provides [`UcanAuthorizer`], which implements the [`Authorizer`]
//! trait by delegating authorization to an external access service. The service
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
//! use dialog_s3_credentials::ucan::{UcanAuthorizer, OperatorIdentity, DelegationChain};
//!
//! // Create operator identity from secret key
//! let operator = OperatorIdentity::from_secret(&secret_key);
//!
//! // Build authorizer with delegation for a subject
//! let authorizer = UcanAuthorizer::builder()
//!     .service_url("https://access.example.com")
//!     .operator(operator)
//!     .delegation("did:key:z6Mk...", delegation_chain)
//!     .build()?;
//! ```

use crate::{Authorization, AuthorizationError, Authorizer, RequestInfo};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::SigningKey;
use ipld_core::cid::Cid;
use std::collections::BTreeMap;
use std::collections::HashMap;
use ucan::did::{Ed25519Did, Ed25519Signer};
use ucan::invocation::builder::InvocationBuilder;

/// A chain of UCAN delegations proving authority over a subject.
///
/// A delegation chain consists of one or more delegations that together prove
/// that the operator has been granted authority over a subject. Each delegation
/// in the chain grants authority from one party to another, forming a chain
/// from the subject (root authority) to the operator.
#[derive(Debug, Clone)]
pub struct DelegationChain {
    /// Serialized delegation proofs (DAG-CBOR encoded).
    ///
    /// These are the actual delegation tokens, serialized for transmission.
    /// The order should match the chain from subject to operator.
    pub proofs: Vec<Vec<u8>>,

    /// CIDs of the proofs (for inclusion in invocations).
    ///
    /// These content identifiers reference the proofs and are included
    /// in the UCAN invocation to link it to the delegation chain.
    pub proof_cids: Vec<Cid>,
}

impl DelegationChain {
    /// Create a new delegation chain from proofs and their CIDs.
    pub fn new(proofs: Vec<Vec<u8>>, proof_cids: Vec<Cid>) -> Self {
        Self { proofs, proof_cids }
    }

    /// Create from a single delegation proof.
    pub fn single(proof: Vec<u8>, cid: Cid) -> Self {
        Self {
            proofs: vec![proof],
            proof_cids: vec![cid],
        }
    }
}

/// Operator identity for signing UCAN invocations.
///
/// This wraps an Ed25519 signing key and provides methods to create
/// UCAN invocations. The operator is the entity making requests on
/// behalf of a subject (who has delegated authority to them).
#[derive(Clone)]
pub struct OperatorIdentity {
    signer: Ed25519Signer,
}

impl std::fmt::Debug for OperatorIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorIdentity")
            .field("did", &self.did().to_string())
            .finish()
    }
}

impl OperatorIdentity {
    /// Create from an Ed25519 signing key.
    pub fn new(signing_key: SigningKey) -> Self {
        Self {
            signer: Ed25519Signer::new(signing_key),
        }
    }

    /// Create from a 32-byte secret.
    ///
    /// The secret bytes are used directly as the Ed25519 signing key seed.
    pub fn from_secret(secret: &[u8; 32]) -> Self {
        Self::new(SigningKey::from_bytes(secret))
    }

    /// Returns the operator's DID.
    ///
    /// This is the public identity of the operator, formatted as a did:key.
    pub fn did(&self) -> &Ed25519Did {
        self.signer.did()
    }

    /// Returns reference to the inner signer for creating invocations.
    pub(crate) fn signer(&self) -> &Ed25519Signer {
        &self.signer
    }
}

/// UCAN-based authorizer that delegates to an external access service.
///
/// This authorizer implements the [`Authorizer`] trait by:
///
/// 1. Extracting the subject DID from the request URL path (first path segment)
/// 2. Looking up the delegation chain for that subject
/// 3. Building a UCAN invocation signed by the operator
/// 4. Sending the invocation to the configured access service
/// 5. Returning the pre-signed URL from the service's 307 redirect response
///
/// # Multi-Subject Support
///
/// A single `UcanAuthorizer` can hold delegations for multiple subjects,
/// allowing access to data across different authorization domains without
/// needing separate authorizer instances.
///
/// # Example
///
/// ```ignore
/// let authorizer = UcanAuthorizer::builder()
///     .service_url("https://access.example.com")
///     .operator(operator)
///     .delegation("did:key:z6MkSubject1...", chain1)
///     .delegation("did:key:z6MkSubject2...", chain2)
///     .build()?;
/// ```
#[derive(Debug, Clone)]
pub struct UcanAuthorizer {
    service_url: String,
    operator: OperatorIdentity,
    delegations: HashMap<String, DelegationChain>,
    client: reqwest::Client,
}

impl UcanAuthorizer {
    /// Create a new builder for `UcanAuthorizer`.
    pub fn builder() -> UcanAuthorizerBuilder {
        UcanAuthorizerBuilder::default()
    }

    /// Returns the access service URL.
    pub fn service_url(&self) -> &str {
        &self.service_url
    }

    /// Returns the operator identity.
    pub fn operator(&self) -> &OperatorIdentity {
        &self.operator
    }

    /// Returns an iterator over the configured subject DIDs.
    pub fn subjects(&self) -> impl Iterator<Item = &str> {
        self.delegations.keys().map(|s| s.as_str())
    }

    /// Check if a delegation exists for the given subject.
    pub fn has_delegation(&self, subject_did: &str) -> bool {
        self.delegations.contains_key(subject_did)
    }
}

/// Builder for [`UcanAuthorizer`].
///
/// Use this to construct a `UcanAuthorizer` with the required configuration.
///
/// # Required Fields
///
/// - `service_url`: The URL of the access service
/// - `operator`: The operator identity for signing invocations
///
/// # Optional Fields
///
/// - `delegation`: One or more subject DID to delegation chain mappings
#[derive(Default)]
pub struct UcanAuthorizerBuilder {
    service_url: Option<String>,
    operator: Option<OperatorIdentity>,
    delegations: HashMap<String, DelegationChain>,
}

impl UcanAuthorizerBuilder {
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
    /// delegated authority by the subject(s) they wish to access.
    pub fn operator(mut self, operator: OperatorIdentity) -> Self {
        self.operator = Some(operator);
        self
    }

    /// Add a delegation chain for a subject.
    ///
    /// The subject is identified by its DID (e.g., "did:key:z6Mk...").
    /// Multiple delegations can be added for different subjects.
    pub fn delegation(mut self, subject_did: impl Into<String>, chain: DelegationChain) -> Self {
        self.delegations.insert(subject_did.into(), chain);
        self
    }

    /// Build the authorizer.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields are missing or if the HTTP client
    /// cannot be constructed.
    pub fn build(self) -> Result<UcanAuthorizer, AuthorizationError> {
        let service_url = self
            .service_url
            .ok_or_else(|| AuthorizationError::AccessService("service_url is required".into()))?;

        let operator = self
            .operator
            .ok_or_else(|| AuthorizationError::AccessService("operator is required".into()))?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| AuthorizationError::AccessService(e.to_string()))?;

        Ok(UcanAuthorizer {
            service_url,
            operator,
            delegations: self.delegations,
            client,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Authorizer for UcanAuthorizer {
    async fn authorize(&self, request: &RequestInfo) -> Result<Authorization, AuthorizationError> {
        // 1. Extract subject DID from URL path (first segment)
        let subject_did = extract_subject_did(&request.url)?;

        // 2. Look up delegation chain
        let delegation = self.delegations.get(&subject_did).ok_or_else(|| {
            AuthorizationError::AccessService(format!(
                "No delegation chain for subject: {}",
                subject_did
            ))
        })?;

        // 3. Determine command from HTTP method
        let command = method_to_command(request.method)?;

        // 4. Parse subject DID
        let subject: Ed25519Did = subject_did.parse().map_err(|e| {
            AuthorizationError::AccessService(format!("Invalid subject DID: {:?}", e))
        })?;

        // 5. Build and serialize invocation
        let invocation = InvocationBuilder::new()
            .issuer(self.operator.signer().clone())
            .audience(subject)
            .subject(subject)
            .command(command)
            .arguments(BTreeMap::new())
            .proofs(delegation.proof_cids.clone())
            .try_build()
            .map_err(|e| {
                AuthorizationError::AccessService(format!("Failed to build invocation: {:?}", e))
            })?;

        let invocation_bytes = serde_ipld_dagcbor::to_vec(&invocation)
            .map_err(|e| AuthorizationError::AccessService(e.to_string()))?;

        // 6. Build access service request URL
        let access_url = format!("{}{}", self.service_url, request.url.path());

        // 7. Build HTTP request
        let mut req = match request.method {
            "GET" => self.client.get(&access_url),
            "PUT" => self.client.put(&access_url),
            "DELETE" => self.client.delete(&access_url),
            method => {
                return Err(AuthorizationError::AccessService(format!(
                    "Unsupported HTTP method: {}",
                    method
                )));
            }
        };

        // Add Authorization header with invocation
        req = req.header(
            "Authorization",
            format!("Bearer {}", BASE64.encode(&invocation_bytes)),
        );

        // Add X-UCAN-Proofs header with delegation chain
        if !delegation.proofs.is_empty() {
            let proofs_b64: Vec<String> =
                delegation.proofs.iter().map(|p| BASE64.encode(p)).collect();
            req = req.header("X-UCAN-Proofs", proofs_b64.join(","));
        }

        // Add checksum header if present (for PUT requests)
        if let Some(checksum) = &request.checksum {
            req = req.header("X-Checksum-SHA256", checksum.to_string());
        }

        // 8. Send request to access service
        let response = req
            .send()
            .await
            .map_err(|e| AuthorizationError::AccessService(e.to_string()))?;

        // 9. Handle response - expect 307 redirect
        if response.status().as_u16() != 307 {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AuthorizationError::AccessService(format!(
                "Access service returned {}: {}",
                status, body
            )));
        }

        // 10. Extract presigned URL from Location header
        let presigned_url = response
            .headers()
            .get("location")
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| {
                AuthorizationError::AccessService("Missing Location header in redirect".into())
            })?;

        let url = url::Url::parse(presigned_url).map_err(|e| {
            AuthorizationError::AccessService(format!("Invalid presigned URL: {}", e))
        })?;

        // 11. Parse required headers from response
        let mut headers: Vec<(String, String)> = response
            .headers()
            .get("x-required-headers")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        // Add host header
        if let Some(host) = url.host_str() {
            let host_value = match url.port() {
                Some(port) => format!("{}:{}", host, port),
                None => host.to_string(),
            };
            headers.push(("host".to_string(), host_value));
        }

        Ok(Authorization { url, headers })
    }

    fn build_url(&self, path: &str) -> Result<url::Url, AuthorizationError> {
        // Build URL relative to access service
        let base = url::Url::parse(&self.service_url)
            .map_err(|e| AuthorizationError::InvalidEndpoint(e.to_string()))?;
        let mut url = base;
        url.set_path(path);
        Ok(url)
    }

    fn region(&self) -> &str {
        // Region is handled by the access service internally
        "auto"
    }

    fn path_style(&self) -> bool {
        // UCAN authorizer doesn't use path-style vs virtual-hosted distinction
        // The access service URL is used directly
        false
    }
}

/// Extract the subject DID from a URL path.
///
/// Expects the first path segment to be a DID (e.g., "/did:key:z6Mk.../index/key").
fn extract_subject_did(url: &url::Url) -> Result<String, AuthorizationError> {
    let path = url.path();
    let first_segment = path
        .trim_start_matches('/')
        .split('/')
        .next()
        .ok_or_else(|| AuthorizationError::AccessService("URL path is empty".into()))?;

    if !first_segment.starts_with("did:") {
        return Err(AuthorizationError::AccessService(format!(
            "First path segment is not a DID: {}",
            first_segment
        )));
    }

    Ok(first_segment.to_string())
}

/// Convert HTTP method to UCAN command.
fn method_to_command(method: &str) -> Result<Vec<String>, AuthorizationError> {
    match method {
        "GET" => Ok(vec!["http".to_string(), "get".to_string()]),
        "PUT" => Ok(vec!["http".to_string(), "put".to_string()]),
        "DELETE" => Ok(vec!["http".to_string(), "delete".to_string()]),
        other => Err(AuthorizationError::AccessService(format!(
            "Unsupported method for UCAN command: {}",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_subject_did_valid() {
        let url =
            url::Url::parse("https://bucket.s3.amazonaws.com/did:key:z6MkTest/index/key").unwrap();
        let did = extract_subject_did(&url).unwrap();
        assert_eq!(did, "did:key:z6MkTest");
    }

    #[test]
    fn test_extract_subject_did_with_encoded_chars() {
        // DIDs may contain characters that get URL-encoded
        let url = url::Url::parse(
            "https://bucket.s3.amazonaws.com/did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK/index/key",
        )
        .unwrap();
        let did = extract_subject_did(&url).unwrap();
        assert!(did.starts_with("did:key:z6Mk"));
    }

    #[test]
    fn test_extract_subject_did_invalid_not_a_did() {
        let url = url::Url::parse("https://bucket.s3.amazonaws.com/not-a-did/index/key").unwrap();
        let result = extract_subject_did(&url);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not a DID"));
    }

    #[test]
    fn test_extract_subject_did_empty_path() {
        let url = url::Url::parse("https://bucket.s3.amazonaws.com/").unwrap();
        let result = extract_subject_did(&url);
        assert!(result.is_err());
    }

    #[test]
    fn test_method_to_command_get() {
        let cmd = method_to_command("GET").unwrap();
        assert_eq!(cmd, vec!["http".to_string(), "get".to_string()]);
    }

    #[test]
    fn test_method_to_command_put() {
        let cmd = method_to_command("PUT").unwrap();
        assert_eq!(cmd, vec!["http".to_string(), "put".to_string()]);
    }

    #[test]
    fn test_method_to_command_delete() {
        let cmd = method_to_command("DELETE").unwrap();
        assert_eq!(cmd, vec!["http".to_string(), "delete".to_string()]);
    }

    #[test]
    fn test_method_to_command_unsupported() {
        let result = method_to_command("PATCH");
        assert!(result.is_err());
    }

    #[test]
    fn test_operator_identity_from_secret() {
        let secret = [0u8; 32];
        let identity = OperatorIdentity::from_secret(&secret);
        let did_str = identity.did().to_string();
        assert!(did_str.starts_with("did:key:z"));
    }

    #[test]
    fn test_operator_identity_deterministic() {
        let secret = [42u8; 32];
        let identity1 = OperatorIdentity::from_secret(&secret);
        let identity2 = OperatorIdentity::from_secret(&secret);
        assert_eq!(identity1.did().to_string(), identity2.did().to_string());
    }

    #[test]
    fn test_delegation_chain_new() {
        let proofs = vec![vec![1, 2, 3]];
        let cid = Cid::default();
        let chain = DelegationChain::new(proofs.clone(), vec![cid]);
        assert_eq!(chain.proofs, proofs);
        assert_eq!(chain.proof_cids.len(), 1);
    }

    #[test]
    fn test_delegation_chain_single() {
        let proof = vec![1, 2, 3];
        let cid = Cid::default();
        let chain = DelegationChain::single(proof.clone(), cid);
        assert_eq!(chain.proofs.len(), 1);
        assert_eq!(chain.proof_cids.len(), 1);
    }

    #[test]
    fn test_builder_missing_service_url() {
        let result = UcanAuthorizer::builder()
            .operator(OperatorIdentity::from_secret(&[0u8; 32]))
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("service_url"));
    }

    #[test]
    fn test_builder_missing_operator() {
        let result = UcanAuthorizer::builder()
            .service_url("https://example.com")
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("operator"));
    }

    #[test]
    fn test_builder_success() {
        let authorizer = UcanAuthorizer::builder()
            .service_url("https://access.example.com")
            .operator(OperatorIdentity::from_secret(&[0u8; 32]))
            .delegation("did:key:z6MkTest", DelegationChain::new(vec![], vec![]))
            .build()
            .unwrap();

        assert_eq!(authorizer.service_url(), "https://access.example.com");
        assert!(authorizer.has_delegation("did:key:z6MkTest"));
        assert!(!authorizer.has_delegation("did:key:z6MkOther"));
    }

    #[test]
    fn test_builder_multiple_delegations() {
        let authorizer = UcanAuthorizer::builder()
            .service_url("https://access.example.com")
            .operator(OperatorIdentity::from_secret(&[0u8; 32]))
            .delegation("did:key:z6MkOne", DelegationChain::new(vec![], vec![]))
            .delegation("did:key:z6MkTwo", DelegationChain::new(vec![], vec![]))
            .build()
            .unwrap();

        let subjects: Vec<&str> = authorizer.subjects().collect();
        assert_eq!(subjects.len(), 2);
        assert!(authorizer.has_delegation("did:key:z6MkOne"));
        assert!(authorizer.has_delegation("did:key:z6MkTwo"));
    }
}
