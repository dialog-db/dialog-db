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
//! let s3_credentials = Credentials::private(address, subject, access_key, secret_key)?;
//!
//! // Wrap with UCAN authorizer
//! let authorizer = UcanAuthorizer::new(s3_credentials);
//!
//! // Handle incoming UCAN container
//! let result = authorizer.authorize(&container_bytes).await?;
//! ```

use std::collections::BTreeMap;

use super::invocation::InvocationChain;
use crate::access::{self, AuthorizationError, RequestDescriptor};
use crate::capability::{archive, memory, storage};
use dialog_common::capability::{Capability, Subject};

/// UCAN authorizer that wraps credentials and handles UCAN invocations.
///
/// This is the server-side component that:
/// 1. Receives UCAN containers (invocation + delegations)
/// 2. Verifies the delegation chain
/// 3. Extracts commands and constructs effects
/// 4. Delegates to wrapped credentials for presigned URLs
#[derive(Debug, Clone)]
pub struct UcanAuthorizer<C> {
    signer: C,
}

impl<C: access::Signer + Sync> UcanAuthorizer<C> {
    /// Create a new UCAN authorizer wrapping the given credentials.
    pub fn new(signer: C) -> Self {
        Self { signer }
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

        // 2. Extract command path and arguments
        let command = chain.command();
        let args = chain.arguments();

        // 3. Get subject DID from the invocation
        let subject_did = chain.subject().to_string();

        // 4. Verify the subject matches this signer's subject
        let expected_subject = self.signer.subject();
        if &subject_did != expected_subject {
            return Err(AuthorizationError::NoDelegation(format!(
                "Subject mismatch: invocation subject '{}' does not match credentials subject '{}'",
                subject_did, expected_subject
            )));
        }

        // 5. Dispatch based on command path
        // Command format: ["storage", "get"] or ["memory", "resolve"] etc.
        let command_segments: Vec<&str> = command.0.iter().map(|s| s.as_str()).collect();

        match command_segments.as_slice() {
            // Storage commands
            ["storage", "get"] => {
                let effect = parse_storage_get(args)?;
                let capability = build_storage_capability(&subject_did, args, effect)?;
                self.signer.sign(&capability).await
            }
            ["storage", "set"] => {
                let effect = parse_storage_set(args)?;
                let capability = build_storage_capability(&subject_did, args, effect)?;
                self.signer.sign(&capability).await
            }
            ["storage", "delete"] => {
                let effect = parse_storage_delete(args)?;
                let capability = build_storage_capability(&subject_did, args, effect)?;
                self.signer.sign(&capability).await
            }
            ["storage", "list"] => {
                let effect = parse_storage_list(args)?;
                let capability = build_storage_capability(&subject_did, args, effect)?;
                self.signer.sign(&capability).await
            }

            // Memory commands
            ["memory", "resolve"] => {
                let capability = build_memory_resolve_capability(&subject_did, args)?;
                self.signer.sign(&capability).await
            }
            ["memory", "publish"] => {
                let capability = build_memory_publish_capability(&subject_did, args)?;
                self.signer.sign(&capability).await
            }
            ["memory", "retract"] => {
                let capability = build_memory_retract_capability(&subject_did, args)?;
                self.signer.sign(&capability).await
            }

            // Archive commands
            ["archive", "get"] => {
                let capability = build_archive_get_capability(&subject_did, args)?;
                self.signer.sign(&capability).await
            }
            ["archive", "put"] => {
                let capability = build_archive_put_capability(&subject_did, args)?;
                self.signer.sign(&capability).await
            }

            _ => Err(AuthorizationError::Invocation(format!(
                "Unknown command: {:?}",
                command_segments
            ))),
        }
    }
}

/// Get a string field from arguments.
fn get_string_arg(
    args: &BTreeMap<String, ucan::promise::Promised>,
    key: &str,
) -> Result<String, AuthorizationError> {
    use ucan::promise::Promised;
    match args.get(key) {
        Some(Promised::String(s)) => Ok(s.clone()),
        Some(_) => Err(AuthorizationError::Invocation(format!(
            "Expected string for '{}' argument",
            key
        ))),
        None => Err(AuthorizationError::Invocation(format!(
            "Missing '{}' argument",
            key
        ))),
    }
}

/// Get an optional string field from arguments.
fn get_optional_string_arg(
    args: &BTreeMap<String, ucan::promise::Promised>,
    key: &str,
) -> Result<Option<String>, AuthorizationError> {
    use ucan::promise::Promised;
    match args.get(key) {
        Some(Promised::String(s)) => Ok(Some(s.clone())),
        Some(Promised::Null) => Ok(None),
        Some(_) => Err(AuthorizationError::Invocation(format!(
            "Expected string or null for '{}' argument",
            key
        ))),
        None => Ok(None),
    }
}

/// Get a bytes field from arguments.
fn get_bytes_arg(
    args: &BTreeMap<String, ucan::promise::Promised>,
    key: &str,
) -> Result<Vec<u8>, AuthorizationError> {
    use ucan::promise::Promised;
    match args.get(key) {
        Some(Promised::Bytes(b)) => Ok(b.clone()),
        Some(_) => Err(AuthorizationError::Invocation(format!(
            "Expected bytes for '{}' argument",
            key
        ))),
        None => Err(AuthorizationError::Invocation(format!(
            "Missing '{}' argument",
            key
        ))),
    }
}

// Storage command parsers

fn parse_storage_get(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::access::storage::Get, AuthorizationError> {
    let key = get_bytes_arg(args, "key")?;
    Ok(crate::access::storage::Get::new(key))
}

fn parse_storage_set(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::access::storage::Set, AuthorizationError> {
    let key = get_bytes_arg(args, "key")?;
    let checksum = parse_checksum(args)?;
    Ok(crate::access::storage::Set::new(key, checksum))
}

fn parse_storage_delete(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::access::storage::Delete, AuthorizationError> {
    let key = get_bytes_arg(args, "key")?;
    Ok(crate::access::storage::Delete::new(key))
}

fn parse_storage_list(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::access::storage::List, AuthorizationError> {
    let continuation_token = get_optional_string_arg(args, "continuation_token")?;
    Ok(crate::access::storage::List::new(continuation_token))
}

/// Build a storage capability from subject, args, and effect.
fn build_storage_capability<E>(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
    effect: E,
) -> Result<Capability<E>, AuthorizationError>
where
    E: dialog_common::capability::Effect<Of = storage::Store>,
{
    let store_name = get_string_arg(args, "store")?;
    Ok(Subject::from(subject_did)
        .attenuate(storage::Storage)
        .attenuate(storage::Store::new(store_name))
        .invoke(effect))
}

// Memory command builders

fn build_memory_resolve_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::access::memory::Resolve>, AuthorizationError> {
    let space = get_string_arg(args, "space")?;
    let cell = get_string_arg(args, "cell")?;
    Ok(Subject::from(subject_did)
        .attenuate(memory::Memory)
        .attenuate(memory::Space::new(space))
        .attenuate(memory::Cell::new(cell))
        .invoke(crate::access::memory::Resolve))
}

fn build_memory_publish_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::access::memory::Publish>, AuthorizationError> {
    let space = get_string_arg(args, "space")?;
    let cell = get_string_arg(args, "cell")?;
    let when = get_optional_string_arg(args, "when")?;
    let checksum = parse_checksum(args)?;
    Ok(Subject::from(subject_did)
        .attenuate(memory::Memory)
        .attenuate(memory::Space::new(space))
        .attenuate(memory::Cell::new(cell))
        .invoke(crate::access::memory::Publish { checksum, when }))
}

fn build_memory_retract_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::access::memory::Retract>, AuthorizationError> {
    let space = get_string_arg(args, "space")?;
    let cell = get_string_arg(args, "cell")?;
    let when = get_string_arg(args, "when")?;
    Ok(Subject::from(subject_did)
        .attenuate(memory::Memory)
        .attenuate(memory::Space::new(space))
        .attenuate(memory::Cell::new(cell))
        .invoke(crate::access::memory::Retract::new(when)))
}

// Archive command builders

fn build_archive_get_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::access::archive::Get>, AuthorizationError> {
    let catalog = get_string_arg(args, "catalog")?;
    let digest = get_bytes_arg(args, "digest")?;
    let digest_arr: [u8; 32] = digest.try_into().map_err(|_| {
        AuthorizationError::Invocation("digest must be 32 bytes".to_string())
    })?;
    let digest_hash = dialog_common::Blake3Hash::from(digest_arr);
    Ok(Subject::from(subject_did)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new(catalog))
        .invoke(crate::access::archive::Get::new(digest_hash)))
}

fn build_archive_put_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::access::archive::Put>, AuthorizationError> {
    let catalog = get_string_arg(args, "catalog")?;
    let digest = get_bytes_arg(args, "digest")?;
    let digest_arr: [u8; 32] = digest.try_into().map_err(|_| {
        AuthorizationError::Invocation("digest must be 32 bytes".to_string())
    })?;
    let digest_hash = dialog_common::Blake3Hash::from(digest_arr);
    let checksum = parse_checksum(args)?;
    Ok(Subject::from(subject_did)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new(catalog))
        .invoke(crate::access::archive::Put::new(digest_hash, checksum)))
}

/// Parse checksum from arguments.
fn parse_checksum(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::Checksum, AuthorizationError> {
    use ucan::promise::Promised;

    // Try to get checksum as a map with algorithm and value
    match args.get("checksum") {
        Some(Promised::Map(map)) => {
            let algorithm = match map.get("algorithm") {
                Some(Promised::String(s)) => s.as_str(),
                _ => {
                    return Err(AuthorizationError::Invocation(
                        "checksum.algorithm must be a string".to_string(),
                    ))
                }
            };
            let value = match map.get("value") {
                Some(Promised::Bytes(b)) => b.clone(),
                _ => {
                    return Err(AuthorizationError::Invocation(
                        "checksum.value must be bytes".to_string(),
                    ))
                }
            };

            match algorithm {
                "sha256" => {
                    let arr: [u8; 32] = value.try_into().map_err(|_| {
                        AuthorizationError::Invocation(
                            "sha256 checksum must be 32 bytes".to_string(),
                        )
                    })?;
                    Ok(crate::Checksum::Sha256(arr))
                }
                _ => Err(AuthorizationError::Invocation(format!(
                    "Unknown checksum algorithm: {}",
                    algorithm
                ))),
            }
        }
        Some(_) => Err(AuthorizationError::Invocation(
            "checksum must be a map with algorithm and value".to_string(),
        )),
        None => Err(AuthorizationError::Invocation(
            "Missing checksum argument".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ucan::InvocationChain;
    use std::collections::BTreeMap;
    use ucan::delegation::builder::DelegationBuilder;
    use ucan::delegation::subject::DelegatedSubject;
    use ucan::did::Ed25519Signer;
    use ucan::invocation::builder::InvocationBuilder;
    use ucan::promise::Promised;

    /// Helper to create a test signer
    fn test_signer() -> Ed25519Signer {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[42u8; 32]);
        Ed25519Signer::new(signing_key)
    }

    /// Build a valid UCAN container with invocation and delegation for testing
    fn build_test_container(
        subject_signer: &Ed25519Signer,
        operator_signer: &Ed25519Signer,
        command: Vec<String>,
        args: BTreeMap<String, Promised>,
    ) -> Vec<u8> {
        let subject_did = subject_signer.did().clone();
        let operator_did = operator_signer.did().clone();

        // Create delegation: subject -> operator
        let delegation = DelegationBuilder::new()
            .issuer(subject_signer.clone())
            .audience(operator_did.clone())
            .subject(DelegatedSubject::Specific(subject_did.clone()))
            .command(command.clone())
            .try_build()
            .expect("Failed to build delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation: operator invokes on subject
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(subject_did.clone())
            .subject(subject_did)
            .command(command)
            .arguments(args)
            .proofs(vec![delegation_cid])
            .try_build()
            .expect("Failed to build invocation");

        // Build InvocationChain
        let mut delegations = std::collections::HashMap::new();
        delegations.insert(delegation_cid, std::sync::Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);
        chain.to_bytes().expect("Failed to serialize container")
    }

    #[dialog_common::test]
    async fn test_acquire_fails_for_wrong_subject() {
        use crate::{Address, s3::Credentials};

        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "test-bucket");
        let credentials = Credentials::private(
            address,
            "did:key:zWrongSubject",
            "access-key-id",
            "secret-access-key",
        )
        .unwrap();

        let authorizer = UcanAuthorizer::new(credentials);

        // Build container with different subject
        let subject_signer = test_signer();
        let operator_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let operator_signer = Ed25519Signer::new(operator_key);

        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("key".to_string(), Promised::Bytes(b"test-key".to_vec()));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["storage".to_string(), "get".to_string()],
            args,
        );

        let result = authorizer.authorize(&container).await;
        // Should fail because the subject doesn't match
        assert!(result.is_err());
    }

    #[dialog_common::test]
    async fn test_acquire_perform_storage_get() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();
        let subject_did = subject_signer.did().to_string();

        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "test-bucket");
        let credentials =
            Credentials::private(address, &subject_did, "access-key-id", "secret-access-key")
                .unwrap();

        let authorizer = UcanAuthorizer::new(credentials);

        let operator_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let operator_signer = Ed25519Signer::new(operator_key);

        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("key".to_string(), Promised::Bytes(b"test-key".to_vec()));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["storage".to_string(), "get".to_string()],
            args,
        );

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("test-bucket"));
    }

    #[dialog_common::test]
    async fn test_acquire_perform_storage_set() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();
        let subject_did = subject_signer.did().to_string();

        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "test-bucket");
        let credentials =
            Credentials::private(address, &subject_did, "access-key-id", "secret-access-key")
                .unwrap();

        let authorizer = UcanAuthorizer::new(credentials);

        let operator_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let operator_signer = Ed25519Signer::new(operator_key);

        let mut checksum_map = BTreeMap::new();
        checksum_map.insert(
            "algorithm".to_string(),
            Promised::String("sha256".to_string()),
        );
        checksum_map.insert("value".to_string(), Promised::Bytes([0u8; 32].to_vec()));

        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("key".to_string(), Promised::Bytes(b"test-key".to_vec()));
        args.insert("checksum".to_string(), Promised::Map(checksum_map));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["storage".to_string(), "set".to_string()],
            args,
        );

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "PUT");
    }

    #[dialog_common::test]
    async fn test_acquire_perform_memory_resolve() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();
        let subject_did = subject_signer.did().to_string();

        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "test-bucket");
        let credentials =
            Credentials::private(address, &subject_did, "access-key-id", "secret-access-key")
                .unwrap();

        let authorizer = UcanAuthorizer::new(credentials);

        let operator_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let operator_signer = Ed25519Signer::new(operator_key);

        let mut args = BTreeMap::new();
        args.insert("space".to_string(), Promised::String("test-space".to_string()));
        args.insert("cell".to_string(), Promised::String("test-cell".to_string()));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["memory".to_string(), "resolve".to_string()],
            args,
        );

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
    }

    #[dialog_common::test]
    async fn test_acquire_perform_archive_get() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();
        let subject_did = subject_signer.did().to_string();

        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "test-bucket");
        let credentials =
            Credentials::private(address, &subject_did, "access-key-id", "secret-access-key")
                .unwrap();

        let authorizer = UcanAuthorizer::new(credentials);

        let operator_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let operator_signer = Ed25519Signer::new(operator_key);

        let mut args = BTreeMap::new();
        args.insert("catalog".to_string(), Promised::String("blobs".to_string()));
        args.insert("digest".to_string(), Promised::Bytes([0u8; 32].to_vec()));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["archive".to_string(), "get".to_string()],
            args,
        );

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
    }

    #[dialog_common::test]
    async fn test_acquire_perform_archive_put() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();
        let subject_did = subject_signer.did().to_string();

        let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "test-bucket");
        let credentials =
            Credentials::private(address, &subject_did, "access-key-id", "secret-access-key")
                .unwrap();

        let authorizer = UcanAuthorizer::new(credentials);

        let operator_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let operator_signer = Ed25519Signer::new(operator_key);

        let mut checksum_map = BTreeMap::new();
        checksum_map.insert(
            "algorithm".to_string(),
            Promised::String("sha256".to_string()),
        );
        checksum_map.insert("value".to_string(), Promised::Bytes([0u8; 32].to_vec()));

        let mut args = BTreeMap::new();
        args.insert("catalog".to_string(), Promised::String("blobs".to_string()));
        args.insert("digest".to_string(), Promised::Bytes([0u8; 32].to_vec()));
        args.insert("checksum".to_string(), Promised::Map(checksum_map));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["archive".to_string(), "put".to_string()],
            args,
        );

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "PUT");
    }
}
