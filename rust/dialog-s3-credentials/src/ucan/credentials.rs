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

use async_trait::async_trait;
use dialog_common::Provider;
use ed25519_dalek::SigningKey;
use ipld_core::cid::Cid;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use ucan::Delegation;
use ucan::did::{Ed25519Did, Ed25519Signer};
use ucan::invocation::builder::InvocationBuilder;
use ucan::promise::Promised;

use super::provider::InvocationChain;
use crate::Checksum;
use crate::access::{AuthorizationError, Claim, RequestDescriptor, memory, storage};

/// A chain of UCAN delegations proving authority over a subject.
///
/// A delegation chain consists of one or more delegations that together prove
/// that the operator has been granted authority over a subject. Each delegation
/// in the chain grants authority from one party to another, forming a chain
/// from the subject (root authority) to the operator.
#[derive(Debug, Clone)]
pub struct DelegationChain {
    /// The delegation proofs keyed by CID.
    delegations: HashMap<Cid, Arc<Delegation<Ed25519Did>>>,
    /// The CIDs of the delegation proofs (for reference in invocations).
    proof_cids: Vec<Cid>,
}

impl DelegationChain {
    /// Create a new delegation chain from delegations.
    ///
    /// The CIDs are computed from the delegations.
    pub fn new(delegations: Vec<Delegation<Ed25519Did>>) -> Self {
        let mut map = HashMap::with_capacity(delegations.len());
        let mut cids = Vec::with_capacity(delegations.len());

        for delegation in delegations {
            let cid = delegation.to_cid();
            cids.push(cid);
            map.insert(cid, Arc::new(delegation));
        }

        Self {
            delegations: map,
            proof_cids: cids,
        }
    }

    /// Create a delegation chain with a single delegation.
    pub fn single(delegation: Delegation<Ed25519Did>) -> Self {
        Self::new(vec![delegation])
    }

    /// Create from raw bytes (deserializes each as a Delegation).
    pub fn from_bytes(proof_bytes: Vec<Vec<u8>>) -> Result<Self, AuthorizationError> {
        let mut delegations = Vec::with_capacity(proof_bytes.len());
        for (i, bytes) in proof_bytes.iter().enumerate() {
            let delegation: Delegation<Ed25519Did> = serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|e| {
                    AuthorizationError::Invocation(format!(
                        "failed to decode delegation {}: {}",
                        i, e
                    ))
                })?;
            delegations.push(delegation);
        }
        Ok(Self::new(delegations))
    }

    /// Get the CIDs for use in invocation proofs field.
    pub fn proof_cids(&self) -> &[Cid] {
        &self.proof_cids
    }

    /// Get the delegations map for building InvocationChain.
    pub(crate) fn delegations(&self) -> &HashMap<Cid, Arc<Delegation<Ed25519Did>>> {
        &self.delegations
    }
}

/// Generate a new random Ed25519 signer.
///
/// This is useful for creating space signers in tests or for any use case
/// that needs a randomly generated Ed25519 keypair.
pub fn generate_signer() -> Ed25519Signer {
    let signing_key = SigningKey::generate(&mut rand_core::OsRng);
    Ed25519Signer::new(signing_key)
}

/// Identity of an operator making UCAN invocations.
///
/// The operator is the entity that signs UCAN invocations. They must have
/// been granted authority by the subject(s) they wish to access.
#[derive(Debug, Clone)]
pub struct OperatorIdentity {
    signer: Ed25519Signer,
}

impl OperatorIdentity {
    /// Generate a new random operator identity.
    pub fn generate() -> Self {
        let signing_key = SigningKey::generate(&mut rand_core::OsRng);
        let signer = Ed25519Signer::new(signing_key);
        Self { signer }
    }

    /// Create an operator identity from a 32-byte secret key.
    pub fn from_secret(secret: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(secret);
        let signer = Ed25519Signer::new(signing_key);
        Self { signer }
    }

    /// Returns the DID of this operator.
    pub fn did(&self) -> Ed25519Did {
        self.signer.did().clone()
    }

    /// Returns a reference to the underlying signer.
    pub(crate) fn signer(&self) -> &Ed25519Signer {
        &self.signer
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
///     .delegation("did:key:z6MkSubject1...", chain1)
///     .delegation("did:key:z6MkSubject2...", chain2)
///     .build()?;
/// ```
#[derive(Debug, Clone)]
pub struct Credentials {
    service_url: String,
    operator: OperatorIdentity,
    delegations: HashMap<String, DelegationChain>,
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

    /// Returns an iterator over the configured subject DIDs.
    pub fn subjects(&self) -> impl Iterator<Item = &str> {
        self.delegations.keys().map(|s| s.as_str())
    }

    /// Check if a delegation exists for the given subject.
    pub fn has_delegation(&self, subject_did: &str) -> bool {
        self.delegations.contains_key(subject_did)
    }

    /// Authorize a storage command via the UCAN access service.
    ///
    /// This method:
    /// 1. Looks up the delegation chain for the subject
    /// 2. Creates a UCAN invocation for the command
    /// 3. Builds an InvocationChain (UCAN container)
    /// 4. POSTs it to the access service
    /// 5. Returns the RequestDescriptor from the response
    async fn authorize<C: Claim + IntoUcanArgs>(
        &self,
        subject_did: &str,
        command: &C,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        // 1. Look up delegation chain
        let delegation = self
            .delegations
            .get(subject_did)
            .ok_or_else(|| AuthorizationError::NoDelegation(subject_did.to_string()))?;

        // 2. Parse subject DID
        let subject: Ed25519Did = subject_did
            .parse()
            .map_err(|e| AuthorizationError::Service(format!("Invalid subject DID: {:?}", e)))?;

        // 3. Get UCAN command path and arguments
        let ucan_command = command.ucan_command();
        let args = command.ucan_args();

        // 4. Build invocation
        let invocation = InvocationBuilder::new()
            .issuer(self.operator.signer().clone())
            .audience(subject.clone())
            .subject(subject)
            .command(ucan_command)
            .arguments(args)
            .proofs(delegation.proof_cids().to_vec())
            .try_build()
            .map_err(|e| AuthorizationError::Invocation(format!("{:?}", e)))?;

        // 5. Build InvocationChain (UCAN container)
        let chain = InvocationChain::new(invocation, delegation.delegations().clone());

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

/// Builder for [`Credentials`].
///
/// Use this to construct a `Credentials` with the required configuration.
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
pub struct CredentialsBuilder {
    service_url: Option<String>,
    operator: Option<OperatorIdentity>,
    delegations: HashMap<String, DelegationChain>,
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
    pub fn build(self) -> Result<Credentials, AuthorizationError> {
        let service_url = self
            .service_url
            .ok_or_else(|| AuthorizationError::Configuration("service_url is required".into()))?;

        let operator = self
            .operator
            .ok_or_else(|| AuthorizationError::Configuration("operator is required".into()))?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| AuthorizationError::Service(e.to_string()))?;

        Ok(Credentials {
            service_url,
            operator,
            delegations: self.delegations,
            client,
        })
    }
}

/// Trait for converting a command into UCAN invocation arguments.
pub trait IntoUcanArgs {
    /// Get the UCAN command path (e.g., ["/storage/get"]).
    fn ucan_command(&self) -> Vec<String>;

    /// Get the UCAN invocation arguments as a BTreeMap of Promised values.
    fn ucan_args(&self) -> BTreeMap<String, Promised>;
}

fn string_arg(s: &str) -> Promised {
    Promised::String(s.to_string())
}

fn checksum_to_promised(checksum: &Checksum) -> Promised {
    // Encode checksum as raw bytes - serde_ipld_dagcbor will handle
    // the appropriate CBOR encoding
    Promised::Bytes(checksum.as_bytes().to_vec())
}

impl IntoUcanArgs for storage::Get {
    fn ucan_command(&self) -> Vec<String> {
        vec!["storage".to_string(), "get".to_string()]
    }

    fn ucan_args(&self) -> BTreeMap<String, Promised> {
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), string_arg(&self.store));
        args.insert("key".to_string(), string_arg(&self.key));
        args
    }
}

impl IntoUcanArgs for storage::Set {
    fn ucan_command(&self) -> Vec<String> {
        vec!["storage".to_string(), "set".to_string()]
    }

    fn ucan_args(&self) -> BTreeMap<String, Promised> {
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), string_arg(&self.store));
        args.insert("key".to_string(), string_arg(&self.key));
        args.insert("checksum".to_string(), checksum_to_promised(&self.checksum));
        args
    }
}

impl IntoUcanArgs for storage::Delete {
    fn ucan_command(&self) -> Vec<String> {
        vec!["storage".to_string(), "delete".to_string()]
    }

    fn ucan_args(&self) -> BTreeMap<String, Promised> {
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), string_arg(&self.store));
        args.insert("key".to_string(), string_arg(&self.key));
        args
    }
}

impl IntoUcanArgs for storage::List {
    fn ucan_command(&self) -> Vec<String> {
        vec!["storage".to_string(), "list".to_string()]
    }

    fn ucan_args(&self) -> BTreeMap<String, Promised> {
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), string_arg(&self.store));
        if let Some(token) = &self.continuation_token {
            args.insert("continuation_token".to_string(), string_arg(token));
        }
        args
    }
}

impl IntoUcanArgs for memory::Resolve {
    fn ucan_command(&self) -> Vec<String> {
        vec!["memory".to_string(), "resolve".to_string()]
    }

    fn ucan_args(&self) -> BTreeMap<String, Promised> {
        let mut args = BTreeMap::new();
        args.insert("space".to_string(), string_arg(&self.space));
        args.insert("cell".to_string(), string_arg(&self.cell));
        args
    }
}

impl IntoUcanArgs for memory::Update {
    fn ucan_command(&self) -> Vec<String> {
        vec!["memory".to_string(), "update".to_string()]
    }

    fn ucan_args(&self) -> BTreeMap<String, Promised> {
        let mut args = BTreeMap::new();
        args.insert("space".to_string(), string_arg(&self.space));
        args.insert("cell".to_string(), string_arg(&self.cell));
        if let Some(edition) = &self.when {
            args.insert("when".to_string(), string_arg(edition));
        }
        args.insert("checksum".to_string(), checksum_to_promised(&self.checksum));
        args
    }
}

impl IntoUcanArgs for memory::Delete {
    fn ucan_command(&self) -> Vec<String> {
        vec!["memory".to_string(), "delete".to_string()]
    }

    fn ucan_args(&self) -> BTreeMap<String, Promised> {
        let mut args = BTreeMap::new();
        args.insert("space".to_string(), string_arg(&self.space));
        args.insert("cell".to_string(), string_arg(&self.cell));
        args.insert("when".to_string(), string_arg(&self.when));
        args
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Get> for Credentials {
    async fn execute(&self, effect: storage::Get) -> Result<RequestDescriptor, AuthorizationError> {
        // Subject DID is the store name for UCAN-based access
        self.authorize(&effect.store, &effect).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Set> for Credentials {
    async fn execute(&self, effect: storage::Set) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(&effect.store, &effect).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Delete> for Credentials {
    async fn execute(
        &self,
        effect: storage::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(&effect.store, &effect).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::List> for Credentials {
    async fn execute(
        &self,
        effect: storage::List,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(&effect.store, &effect).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Resolve> for Credentials {
    async fn execute(
        &self,
        effect: memory::Resolve,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(&effect.space, &effect).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Update> for Credentials {
    async fn execute(
        &self,
        effect: memory::Update,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(&effect.space, &effect).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Delete> for Credentials {
    async fn execute(
        &self,
        effect: memory::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(&effect.space, &effect).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_delegation_chain_empty() {
        let chain = DelegationChain::new(vec![]);
        assert_eq!(chain.proof_cids().len(), 0);
        assert_eq!(chain.delegations().len(), 0);
    }

    #[test]
    fn test_builder_missing_service_url() {
        let result = Credentials::builder()
            .operator(OperatorIdentity::from_secret(&[0u8; 32]))
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("service_url"));
    }

    #[test]
    fn test_builder_missing_operator() {
        let result = Credentials::builder()
            .service_url("https://example.com")
            .build();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("operator"));
    }

    #[test]
    fn test_builder_success() {
        let authorizer = Credentials::builder()
            .service_url("https://access.example.com")
            .operator(OperatorIdentity::from_secret(&[0u8; 32]))
            .delegation("did:key:z6MkTest", DelegationChain::new(vec![]))
            .build()
            .unwrap();

        assert_eq!(authorizer.service_url(), "https://access.example.com");
        assert!(authorizer.has_delegation("did:key:z6MkTest"));
        assert!(!authorizer.has_delegation("did:key:z6MkOther"));
    }

    #[test]
    fn test_builder_multiple_delegations() {
        let authorizer = Credentials::builder()
            .service_url("https://access.example.com")
            .operator(OperatorIdentity::from_secret(&[0u8; 32]))
            .delegation("did:key:z6MkOne", DelegationChain::new(vec![]))
            .delegation("did:key:z6MkTwo", DelegationChain::new(vec![]))
            .build()
            .unwrap();

        let subjects: Vec<&str> = authorizer.subjects().collect();
        assert_eq!(subjects.len(), 2);
        assert!(authorizer.has_delegation("did:key:z6MkOne"));
        assert!(authorizer.has_delegation("did:key:z6MkTwo"));
    }

    #[test]
    fn test_storage_get_ucan_command() {
        let cmd = storage::Get::new("index", "hello");
        assert_eq!(cmd.ucan_command(), vec!["storage", "get"]);
    }

    #[test]
    fn test_storage_get_ucan_args() {
        let cmd = storage::Get::new("index", "hello");
        let args = cmd.ucan_args();
        assert_eq!(
            args.get("store"),
            Some(&Promised::String("index".to_string()))
        );
        assert_eq!(
            args.get("key"),
            Some(&Promised::String("hello".to_string()))
        );
    }

    #[test]
    fn test_storage_set_ucan_command() {
        let cmd = storage::Set::new("index", "hello", crate::Hasher::Sha256.checksum(&[1, 2, 3]));
        assert_eq!(cmd.ucan_command(), vec!["storage", "set"]);
    }

    #[test]
    fn test_storage_set_ucan_args() {
        let cmd = storage::Set::new("index", "hello", crate::Hasher::Sha256.checksum(&[1, 2, 3]));
        let args = cmd.ucan_args();
        assert_eq!(
            args.get("store"),
            Some(&Promised::String("index".to_string()))
        );
        assert_eq!(
            args.get("key"),
            Some(&Promised::String("hello".to_string()))
        );
        assert!(args.contains_key("checksum"));
    }

    #[test]
    fn test_memory_resolve_ucan_command() {
        let cmd = memory::Resolve::new("did:key:z6MkTest", "main");
        assert_eq!(cmd.ucan_command(), vec!["memory", "resolve"]);
    }

    #[test]
    fn test_memory_resolve_ucan_args() {
        let cmd = memory::Resolve::new("did:key:z6MkTest", "main");
        let args = cmd.ucan_args();
        assert_eq!(
            args.get("space"),
            Some(&Promised::String("did:key:z6MkTest".to_string()))
        );
        assert_eq!(
            args.get("cell"),
            Some(&Promised::String("main".to_string()))
        );
    }
}
