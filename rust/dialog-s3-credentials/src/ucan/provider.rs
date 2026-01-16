//! UCAN provider for server-side authorization.
//!
//! This module provides [`UcanAuthorizer`], which wraps credentials and handles
//! incoming UCAN invocations to authorize S3 operations.
//!
//! # Overview
//!
//! The UCAN provider sits on the server side and:
//!
//! 1. Receives a UCAN container (invocation + delegation chain)
//! 2. Verifies the invocation and delegation chain
//! 3. Extracts the command and arguments from the invocation
//! 4. Delegates to wrapped credentials to get a presigned URL
//!
//! # Container Format
//!
//! The UCAN container follows the [UCAN Container spec](https://github.com/ucan-wg/container):
//!
//! ```text
//! { "ctn-v1": [token_bytes_0, token_bytes_1, ..., token_bytes_n] }
//! ```
//!
//! Where tokens are DAG-CBOR serialized UCANs, ordered bytewise for determinism.
//! The first token is the invocation, followed by the delegation chain from
//! closest to invoker to root.
//!
//! The delegation chain forms an authority path:
//! ```text
//! Subject (root) → Delegation[n-1] → ... → Delegation[0] → Invocation.issuer
//! ```
//!
//! # Example
//!
//! ```ignore
//! use dialog_s3_credentials::ucan::UcanAuthorizer;
//! use dialog_s3_credentials::s3::Credentials;
//!
//! // Create underlying credentials for S3 access
//! let s3_credentials = Credentials::private(address, access_key, secret_key)?;
//!
//! // Wrap with UCAN authorizer
//! let authorizer = UcanAuthorizer::new(s3_credentials);
//!
//! // Handle incoming UCAN container
//! let result = authorizer.authorize(&container_bytes).await?;
//! ```

use ipld_core::{cid::Cid, ipld::Ipld};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
use ucan::{
    delegation::store::DelegationStore,
    did::Ed25519Did,
    future::Sendable,
    invocation::{CheckFailed, Invocation},
    Delegation,
};

use crate::access::{self, AuthorizationError, Args, RequestDescriptor};

/// UCAN Container version key
const CONTAINER_VERSION: &str = "ctn-v1";

/// In-memory delegation store for verification.
type ProofStore = Arc<Mutex<HashMap<Cid, Arc<Delegation<Ed25519Did>>>>>;

/// An invocation with its delegation chain, parsed from a UCAN container.
///
/// This represents a complete authorization bundle containing:
/// - The invocation (the signed command to execute)
/// - The delegation chain (proofs of authority from subject to invoker)
///
/// The invocation references its proofs by CID, and the delegation chain
/// provides those proofs for verification.
#[derive(Debug, Clone)]
pub struct InvocationChain {
    /// The signed invocation containing the command and arguments.
    pub invocation: Invocation<Ed25519Did>,
    /// The delegation chain as a map keyed by CID for proof lookup.
    delegations: HashMap<Cid, Arc<Delegation<Ed25519Did>>>,
}

impl InvocationChain {
    /// Create a new invocation chain from an invocation and delegations.
    pub fn new(
        invocation: Invocation<Ed25519Did>,
        delegations: HashMap<Cid, Arc<Delegation<Ed25519Did>>>,
    ) -> Self {
        Self {
            invocation,
            delegations,
        }
    }

    /// Verify the invocation chain using rs-ucan's verification.
    ///
    /// This performs complete verification:
    /// 1. Signature verification (issuer signed the invocation)
    /// 2. Proof chain validation (issuer→subject chain via proofs)
    /// 3. Command attenuation checks
    /// 4. Policy predicate evaluation
    ///
    /// The invocation's `proofs` field contains CIDs that reference
    /// delegations in the container. This method builds a store from
    /// those delegations and uses rs-ucan's `Invocation::check` to verify.
    pub async fn verify(&self) -> Result<(), AuthorizationError> {
        // Build delegation store from our map
        let store: ProofStore = Arc::new(Mutex::new(self.delegations.clone()));

        // Use rs-ucan's full verification
        self.invocation
            .check::<Sendable, _, _>(&store)
            .await
            .map_err(|e| map_check_error(e))
    }

    /// Get the command from the invocation.
    pub fn command(&self) -> &ucan::command::Command {
        self.invocation.command()
    }

    /// Get the arguments from the invocation.
    pub fn arguments(&self) -> &BTreeMap<String, ucan::promise::Promised> {
        self.invocation.arguments()
    }

    /// Get the subject (root authority) of the invocation.
    pub fn subject(&self) -> &Ed25519Did {
        self.invocation.subject()
    }

    /// Get the issuer of the invocation.
    pub fn issuer(&self) -> &Ed25519Did {
        self.invocation.issuer()
    }

    /// Get the proof CIDs referenced by the invocation.
    pub fn proofs(&self) -> &Vec<Cid> {
        self.invocation.proofs()
    }
}

/// Map rs-ucan's check error to AuthorizationError.
fn map_check_error<K, D, T, S>(
    err: ucan::invocation::InvocationCheckError<K, D, T, S>,
) -> AuthorizationError
where
    K: ucan::future::FutureKind,
    D: ucan::did::Did,
    T: std::borrow::Borrow<Delegation<D>>,
    S: DelegationStore<K, D, T>,
    S::GetError: std::fmt::Display,
{
    use ucan::invocation::InvocationCheckError;

    match err {
        InvocationCheckError::SignatureVerification(sig_err) => {
            AuthorizationError::Invocation(format!("invalid signature: {}", sig_err))
        }
        InvocationCheckError::StoredCheck(stored_err) => {
            use ucan::invocation::StoredCheckError;
            match stored_err {
                StoredCheckError::GetError(get_err) => {
                    AuthorizationError::Invocation(format!("proof not found: {}", get_err))
                }
                StoredCheckError::CheckFailed(check_err) => map_check_failed(check_err),
            }
        }
    }
}

/// Map rs-ucan's CheckFailed error to AuthorizationError.
fn map_check_failed(err: CheckFailed) -> AuthorizationError {
    match err {
        CheckFailed::InvalidProofIssuerChain => {
            AuthorizationError::Invocation("invalid proof issuer chain".to_string())
        }
        CheckFailed::SubjectNotAllowedByProof => {
            AuthorizationError::Invocation("subject not allowed by proof".to_string())
        }
        CheckFailed::RootProofIssuerIsNotSubject => {
            AuthorizationError::Invocation("root proof issuer is not the subject".to_string())
        }
        CheckFailed::CommandMismatch { expected, found } => AuthorizationError::Invocation(
            format!("command mismatch: expected {:?}, found {:?}", expected, found),
        ),
        CheckFailed::PredicateFailed(predicate) => {
            AuthorizationError::Invocation(format!("predicate failed: {:?}", predicate))
        }
        CheckFailed::PredicateRunError(run_err) => {
            AuthorizationError::Invocation(format!("predicate run error: {}", run_err))
        }
        CheckFailed::WaitingOnPromise(waiting) => {
            AuthorizationError::Invocation(format!("waiting on promise: {:?}", waiting))
        }
    }
}

impl<'de> Deserialize<'de> for InvocationChain {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize as a map with "ctn-v1" key
        let container: BTreeMap<String, Ipld> = Deserialize::deserialize(deserializer)?;

        // Extract the token array under "ctn-v1"
        let tokens_ipld = container.get(CONTAINER_VERSION).ok_or_else(|| {
            serde::de::Error::custom(format!("missing '{}' key", CONTAINER_VERSION))
        })?;

        let Ipld::List(tokens) = tokens_ipld else {
            return Err(serde::de::Error::custom("tokens must be an array"));
        };

        if tokens.is_empty() {
            return Err(serde::de::Error::custom(
                "container must contain at least an invocation",
            ));
        }

        // Extract token bytes
        let mut token_bytes: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
        for (i, token) in tokens.iter().enumerate() {
            let Ipld::Bytes(bytes) = token else {
                return Err(serde::de::Error::custom(format!(
                    "token {} must be bytes",
                    i
                )));
            };
            token_bytes.push(bytes.clone());
        }

        // First token is the invocation - deserialize using rs-ucan's Invocation
        let invocation: Invocation<Ed25519Did> =
            serde_ipld_dagcbor::from_slice(&token_bytes[0]).map_err(|e| {
                serde::de::Error::custom(format!("failed to decode invocation: {}", e))
            })?;

        // Remaining tokens are delegations - build a map keyed by CID
        let mut delegations: HashMap<Cid, Arc<Delegation<Ed25519Did>>> =
            HashMap::with_capacity(token_bytes.len() - 1);
        for (i, bytes) in token_bytes.iter().skip(1).enumerate() {
            let delegation: Delegation<Ed25519Did> =
                serde_ipld_dagcbor::from_slice(bytes).map_err(|e| {
                    serde::de::Error::custom(format!("failed to decode delegation {}: {}", i, e))
                })?;
            let cid = delegation.to_cid();
            delegations.insert(cid, Arc::new(delegation));
        }

        Ok(InvocationChain {
            invocation,
            delegations,
        })
    }
}

impl TryFrom<&[u8]> for InvocationChain {
    type Error = AuthorizationError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        serde_ipld_dagcbor::from_slice(bytes)
            .map_err(|e| AuthorizationError::Invocation(format!("failed to decode container: {}", e)))
    }
}

impl Serialize for InvocationChain {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize invocation to bytes
        let invocation_bytes = serde_ipld_dagcbor::to_vec(&self.invocation)
            .map_err(serde::ser::Error::custom)?;

        // Collect delegation bytes - order by the proof CIDs in the invocation
        let mut token_bytes: Vec<Ipld> = Vec::with_capacity(1 + self.delegations.len());
        token_bytes.push(Ipld::Bytes(invocation_bytes));

        // Add delegations in the order they appear in the invocation's proofs
        for cid in self.invocation.proofs() {
            if let Some(delegation) = self.delegations.get(cid) {
                let delegation_bytes = serde_ipld_dagcbor::to_vec(delegation.as_ref())
                    .map_err(serde::ser::Error::custom)?;
                token_bytes.push(Ipld::Bytes(delegation_bytes));
            }
        }

        // Build container: { "ctn-v1": [token_bytes...] }
        let mut container: BTreeMap<String, Ipld> = BTreeMap::new();
        container.insert(CONTAINER_VERSION.to_string(), Ipld::List(token_bytes));

        container.serialize(serializer)
    }
}

impl InvocationChain {
    /// Serialize to DAG-CBOR bytes (UCAN container format).
    pub fn to_bytes(&self) -> Result<Vec<u8>, AuthorizationError> {
        serde_ipld_dagcbor::to_vec(self)
            .map_err(|e| AuthorizationError::Invocation(format!("failed to encode container: {}", e)))
    }
}

/// UCAN authorizer that wraps credentials and handles UCAN invocations.
///
/// This is the server-side component that:
/// 1. Receives UCAN containers (invocation + delegations)
/// 2. Verifies the delegation chain
/// 3. Extracts commands and constructs effects
/// 4. Delegates to wrapped credentials for presigned URLs
#[derive(Debug, Clone)]
pub struct UcanAuthorizer<C> {
    credentials: C,
}

impl<C: access::AccessProvider> UcanAuthorizer<C> {
    /// Create a new UCAN authorizer wrapping the given credentials.
    pub fn new(credentials: C) -> Self {
        Self { credentials }
    }

    /// Authorize a UCAN container.
    ///
    /// # Arguments
    ///
    /// * `container` - CBOR-encoded UCAN container following the
    ///   [UCAN Container spec](https://github.com/ucan-wg/container):
    ///   `{ "ctn-v1": [invocation_bytes, delegation_0_bytes, ..., delegation_n_bytes] }`
    ///
    /// # Returns
    ///
    /// Returns a `RequestDescriptor` with a presigned URL and headers on success.
    ///
    /// # Verification
    ///
    /// The container is verified using rs-ucan's `syntactic_checks` which:
    /// 1. Verifies the delegation chain from subject to invocation issuer
    /// 2. Checks command prefix authorization at each delegation
    /// 3. Validates policy predicates on each delegation
    pub async fn authorize(
        &self,
        container: &[u8],
    ) -> Result<RequestDescriptor, AuthorizationError> {
        // 1. Parse and verify the invocation chain
        let chain = InvocationChain::try_from(container)?;
        chain.verify().await?;

        // 2. Extract command and arguments
        let command = chain.command();
        let args = chain.arguments();

        // 3. Parse and execute command
        let segments = command.segments();
        let segments_str: Vec<&str> = segments.iter().map(|s| s.as_str()).collect();

        let cmd: access::Do = (&segments_str[..], Args(args)).try_into()?;

        cmd.perform(&self.credentials).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::{archive, memory, storage};
    use std::collections::BTreeMap;
    use ucan::promise::Promised;

    #[test]
    fn test_parse_storage_get() {
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("key".to_string(), Promised::String("hello".to_string()));

        let cmd: storage::Do = (&["get"][..], Args(&args)).try_into().unwrap();
        match cmd {
            storage::Do::Get(effect) => {
                assert_eq!(effect.store, "index");
                assert_eq!(effect.key, "hello");
            }
            _ => panic!("Expected Get command"),
        }
    }

    #[test]
    fn test_parse_storage_get_missing_store() {
        let mut args = BTreeMap::new();
        args.insert("key".to_string(), Promised::String("hello".to_string()));

        let result: Result<storage::Do, _> = (&["get"][..], Args(&args)).try_into();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("store"));
    }

    #[test]
    fn test_parse_memory_resolve() {
        let mut args = BTreeMap::new();
        args.insert("space".to_string(), Promised::String("did:key:z6MkTest".to_string()));
        args.insert("cell".to_string(), Promised::String("main".to_string()));

        let cmd: memory::Do = (&["resolve"][..], Args(&args)).try_into().unwrap();
        match cmd {
            memory::Do::Resolve(effect) => {
                assert_eq!(effect.space, "did:key:z6MkTest");
                assert_eq!(effect.cell, "main");
            }
            _ => panic!("Expected Resolve command"),
        }
    }

    #[test]
    fn test_parse_storage_list_with_continuation() {
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("continuation_token".to_string(), Promised::String("abc123".to_string()));

        let cmd: storage::Do = (&["list"][..], Args(&args)).try_into().unwrap();
        match cmd {
            storage::Do::List(effect) => {
                assert_eq!(effect.store, "index");
                assert_eq!(effect.continuation_token, Some("abc123".to_string()));
            }
            _ => panic!("Expected List command"),
        }
    }

    #[test]
    fn test_parse_storage_list_without_continuation() {
        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));

        let cmd: storage::Do = (&["list"][..], Args(&args)).try_into().unwrap();
        match cmd {
            storage::Do::List(effect) => {
                assert_eq!(effect.store, "index");
                assert_eq!(effect.continuation_token, None);
            }
            _ => panic!("Expected List command"),
        }
    }

    #[test]
    fn test_parse_archive_get() {
        let mut args = BTreeMap::new();
        args.insert("catalog".to_string(), Promised::String("blob".to_string()));
        args.insert("digest".to_string(), Promised::String("abc123".to_string()));

        let cmd: archive::Do = (&["get"][..], Args(&args)).try_into().unwrap();
        match cmd {
            archive::Do::Get(effect) => {
                assert_eq!(effect.catalog, "blob");
                assert_eq!(effect.digest, "abc123");
            }
            _ => panic!("Expected Get command"),
        }
    }

    #[test]
    fn test_parse_archive_list() {
        let mut args = BTreeMap::new();
        args.insert("catalog".to_string(), Promised::String("index".to_string()));

        let cmd: archive::Do = (&["list"][..], Args(&args)).try_into().unwrap();
        match cmd {
            archive::Do::List(effect) => {
                assert_eq!(effect.catalog, "index");
                assert_eq!(effect.continuation_token, None);
            }
            _ => panic!("Expected List command"),
        }
    }

    #[test]
    fn test_invocation_chain_missing_version_key() {
        let mut container_map = BTreeMap::new();
        container_map.insert(
            "wrong-key".to_string(),
            Ipld::List(vec![Ipld::Bytes(vec![0x01])]),
        );

        let container_bytes = serde_ipld_dagcbor::to_vec(&container_map).unwrap();
        let result = InvocationChain::try_from(container_bytes.as_slice());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ctn-v1"));
    }

    #[test]
    fn test_invocation_chain_invalid_token_type() {
        let mut container_map = BTreeMap::new();
        container_map.insert(
            CONTAINER_VERSION.to_string(),
            Ipld::List(vec![Ipld::String("not bytes".to_string())]),
        );

        let container_bytes = serde_ipld_dagcbor::to_vec(&container_map).unwrap();
        let result = InvocationChain::try_from(container_bytes.as_slice());

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be bytes"));
    }

    #[test]
    fn test_invocation_chain_empty_tokens() {
        let mut container_map = BTreeMap::new();
        container_map.insert(CONTAINER_VERSION.to_string(), Ipld::List(vec![]));

        let container_bytes = serde_ipld_dagcbor::to_vec(&container_map).unwrap();
        let result = InvocationChain::try_from(container_bytes.as_slice());

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least an invocation"));
    }
}
