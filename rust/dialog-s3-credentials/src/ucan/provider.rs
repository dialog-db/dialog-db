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
//! ```rust,no_run
//! use dialog_s3_credentials::ucan::UcanAuthorizer;
//! use dialog_s3_credentials::s3::Credentials;
//! use dialog_s3_credentials::Address;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create address for S3 bucket
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//!
//! // Create underlying credentials for S3 access
//! let s3_credentials = Credentials::private(
//!     address,
//!     "access-key-id",
//!     "secret-access-key",
//! )?;
//!
//! // Wrap with UCAN authorizer
//! let authorizer = UcanAuthorizer::new(s3_credentials);
//!
//! // Handle incoming UCAN container
//! let container_bytes: Vec<u8> = vec![]; // UCAN container from request
//! let result = authorizer.authorize(&container_bytes).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;

use super::InvocationChain;
use crate::capability::{AccessError, AuthorizedRequest};
use crate::capability::{archive, memory, storage};
use crate::s3::Credentials;
use dialog_capability::{Capability, Subject};

/// UCAN authorizer that wraps credentials and handles UCAN invocations.
///
/// This is the server-side component that:
/// 1. Receives UCAN containers (invocation + delegations)
/// 2. Verifies the delegation chain
/// 3. Extracts commands and constructs effects
/// 4. Delegates to wrapped credentials for presigned URLs
#[derive(Debug, Clone)]
pub struct UcanAuthorizer {
    credentials: Credentials,
}

impl UcanAuthorizer {
    /// Create a new UCAN authorizer wrapping the given credentials.
    pub fn new(credentials: Credentials) -> Self {
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
    pub async fn authorize(&self, container: &[u8]) -> Result<AuthorizedRequest, AccessError> {
        // Parse and verify the invocation chain
        let chain = InvocationChain::try_from(container)?;
        chain.verify().await?;

        // Extract command path and arguments
        let command = chain.command();
        let args = chain.arguments();

        // Get subject DID from the invocation
        let subject_did = chain.subject().to_string();

        // 5. Dispatch based on command path
        // Command format: ["storage", "get"] or ["memory", "resolve"] etc.
        let command_segments: Vec<&str> = command.0.iter().map(|s| s.as_str()).collect();

        match command_segments.as_slice() {
            // Storage commands
            ["storage", "get"] => {
                let effect = parse_storage_get(args)?;
                let capability = build_storage_capability(&subject_did, args, effect)?;
                self.credentials.grant(&capability).await
            }
            ["storage", "set"] => {
                let effect = parse_storage_set(args)?;
                let capability = build_storage_capability(&subject_did, args, effect)?;
                self.credentials.grant(&capability).await
            }
            ["storage", "delete"] => {
                let effect = parse_storage_delete(args)?;
                let capability = build_storage_capability(&subject_did, args, effect)?;
                self.credentials.grant(&capability).await
            }
            ["storage", "list"] => {
                let effect = parse_storage_list(args)?;
                let capability = build_storage_capability(&subject_did, args, effect)?;
                self.credentials.grant(&capability).await
            }

            // Memory commands
            ["memory", "resolve"] => {
                let capability = build_memory_resolve_capability(&subject_did, args)?;
                self.credentials.grant(&capability).await
            }
            ["memory", "publish"] => {
                let capability = build_memory_publish_capability(&subject_did, args)?;
                self.credentials.grant(&capability).await
            }
            ["memory", "retract"] => {
                let capability = build_memory_retract_capability(&subject_did, args)?;
                self.credentials.grant(&capability).await
            }

            // Archive commands
            ["archive", "get"] => {
                let capability = build_archive_get_capability(&subject_did, args)?;
                self.credentials.grant(&capability).await
            }
            ["archive", "put"] => {
                let capability = build_archive_put_capability(&subject_did, args)?;
                self.credentials.grant(&capability).await
            }

            _ => Err(AccessError::Invocation(format!(
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
) -> Result<String, AccessError> {
    use ucan::promise::Promised;
    match args.get(key) {
        Some(Promised::String(s)) => Ok(s.clone()),
        Some(_) => Err(AccessError::Invocation(format!(
            "Expected string for '{}' argument",
            key
        ))),
        None => Err(AccessError::Invocation(format!(
            "Missing '{}' argument",
            key
        ))),
    }
}

/// Get an optional string field from arguments.
fn get_optional_string_arg(
    args: &BTreeMap<String, ucan::promise::Promised>,
    key: &str,
) -> Result<Option<String>, AccessError> {
    use ucan::promise::Promised;
    match args.get(key) {
        Some(Promised::String(s)) => Ok(Some(s.clone())),
        Some(Promised::Null) => Ok(None),
        Some(_) => Err(AccessError::Invocation(format!(
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
) -> Result<Vec<u8>, AccessError> {
    use ucan::promise::Promised;
    match args.get(key) {
        Some(Promised::Bytes(b)) => Ok(b.clone()),
        Some(_) => Err(AccessError::Invocation(format!(
            "Expected bytes for '{}' argument",
            key
        ))),
        None => Err(AccessError::Invocation(format!(
            "Missing '{}' argument",
            key
        ))),
    }
}

// Storage command parsers

fn parse_storage_get(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::capability::storage::Get, AccessError> {
    let key = get_bytes_arg(args, "key")?;
    Ok(crate::capability::storage::Get::new(key))
}

fn parse_storage_set(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::capability::storage::Set, AccessError> {
    let key = get_bytes_arg(args, "key")?;
    let checksum = parse_checksum(args)?;
    Ok(crate::capability::storage::Set::new(key, checksum))
}

fn parse_storage_delete(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::capability::storage::Delete, AccessError> {
    let key = get_bytes_arg(args, "key")?;
    Ok(crate::capability::storage::Delete::new(key))
}

fn parse_storage_list(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::capability::storage::List, AccessError> {
    let continuation_token = get_optional_string_arg(args, "continuation_token")?;
    Ok(crate::capability::storage::List::new(continuation_token))
}

/// Build a storage capability from subject, args, and effect.
fn build_storage_capability<E>(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
    effect: E,
) -> Result<Capability<E>, AccessError>
where
    E: dialog_capability::Effect<Of = storage::Store>,
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
) -> Result<Capability<crate::capability::memory::Resolve>, AccessError> {
    let space = get_string_arg(args, "space")?;
    let cell = get_string_arg(args, "cell")?;
    Ok(Subject::from(subject_did)
        .attenuate(memory::Memory)
        .attenuate(memory::Space::new(space))
        .attenuate(memory::Cell::new(cell))
        .invoke(crate::capability::memory::Resolve))
}

fn build_memory_publish_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::capability::memory::Publish>, AccessError> {
    let space = get_string_arg(args, "space")?;
    let cell = get_string_arg(args, "cell")?;
    let when = get_optional_string_arg(args, "when")?;
    let checksum = parse_checksum(args)?;
    Ok(Subject::from(subject_did)
        .attenuate(memory::Memory)
        .attenuate(memory::Space::new(space))
        .attenuate(memory::Cell::new(cell))
        .invoke(crate::capability::memory::Publish { checksum, when }))
}

fn build_memory_retract_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::capability::memory::Retract>, AccessError> {
    let space = get_string_arg(args, "space")?;
    let cell = get_string_arg(args, "cell")?;
    let when = get_string_arg(args, "when")?;
    Ok(Subject::from(subject_did)
        .attenuate(memory::Memory)
        .attenuate(memory::Space::new(space))
        .attenuate(memory::Cell::new(cell))
        .invoke(crate::capability::memory::Retract::new(when)))
}

// Archive command builders

fn build_archive_get_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::capability::archive::Get>, AccessError> {
    let catalog = get_string_arg(args, "catalog")?;
    let digest = get_bytes_arg(args, "digest")?;
    let digest_arr: [u8; 32] = digest
        .try_into()
        .map_err(|_| AccessError::Invocation("digest must be 32 bytes".to_string()))?;
    let digest_hash = dialog_common::Blake3Hash::from(digest_arr);
    Ok(Subject::from(subject_did)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new(catalog))
        .invoke(crate::capability::archive::Get::new(digest_hash)))
}

fn build_archive_put_capability(
    subject_did: &str,
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<Capability<crate::capability::archive::Put>, AccessError> {
    let catalog = get_string_arg(args, "catalog")?;
    let digest = get_bytes_arg(args, "digest")?;
    let digest_arr: [u8; 32] = digest
        .try_into()
        .map_err(|_| AccessError::Invocation("digest must be 32 bytes".to_string()))?;
    let digest_hash = dialog_common::Blake3Hash::from(digest_arr);
    let checksum = parse_checksum(args)?;
    Ok(Subject::from(subject_did)
        .attenuate(archive::Archive)
        .attenuate(archive::Catalog::new(catalog))
        .invoke(crate::capability::archive::Put::new(digest_hash, checksum)))
}

/// Parse checksum from arguments.
fn parse_checksum(
    args: &BTreeMap<String, ucan::promise::Promised>,
) -> Result<crate::Checksum, AccessError> {
    use ucan::promise::Promised;

    // Try to get checksum as a map with algorithm and value
    match args.get("checksum") {
        Some(Promised::Map(map)) => {
            let algorithm = match map.get("algorithm") {
                Some(Promised::String(s)) => s.as_str(),
                _ => {
                    return Err(AccessError::Invocation(
                        "checksum.algorithm must be a string".to_string(),
                    ));
                }
            };
            let value = match map.get("value") {
                Some(Promised::Bytes(b)) => b.clone(),
                _ => {
                    return Err(AccessError::Invocation(
                        "checksum.value must be bytes".to_string(),
                    ));
                }
            };

            match algorithm {
                "sha256" => {
                    let arr: [u8; 32] = value.try_into().map_err(|_| {
                        AccessError::Invocation("sha256 checksum must be 32 bytes".to_string())
                    })?;
                    Ok(crate::Checksum::Sha256(arr))
                }
                _ => Err(AccessError::Invocation(format!(
                    "Unknown checksum algorithm: {}",
                    algorithm
                ))),
            }
        }
        Some(_) => Err(AccessError::Invocation(
            "checksum must be a map with algorithm and value".to_string(),
        )),
        None => Err(AccessError::Invocation(
            "Missing checksum argument".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ucan::InvocationChain;
    use crate::ucan::credentials::tests::{Session, test_delegation_chain};
    use crate::ucan::{Credentials, UcanAuthorization};
    use crate::{Address, s3};
    use base58::ToBase58;
    use dialog_capability::{Authorization, Principal};
    use dialog_common::Blake3Hash;
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
        let subject_did = *subject_signer.did();
        let operator_did = *operator_signer.did();

        // Create delegation: subject -> operator
        let delegation = DelegationBuilder::new()
            .issuer(subject_signer.clone())
            .audience(operator_did)
            .subject(DelegatedSubject::Specific(subject_did))
            .command(command.clone())
            .try_build()
            .expect("Failed to build delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation: operator invokes on subject
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(subject_did)
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
    async fn it_acquires_and_performs_storage_get() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials =
            Credentials::private(address, "access-key-id", "secret-access-key").unwrap();

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
    async fn it_acquires_and_performs_storage_set() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials =
            Credentials::private(address, "access-key-id", "secret-access-key").unwrap();

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
    async fn it_acquires_and_performs_memory_resolve() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials =
            Credentials::private(address, "access-key-id", "secret-access-key").unwrap();

        let authorizer = UcanAuthorizer::new(credentials);

        let operator_key = ed25519_dalek::SigningKey::from_bytes(&[1u8; 32]);
        let operator_signer = Ed25519Signer::new(operator_key);

        let mut args = BTreeMap::new();
        args.insert(
            "space".to_string(),
            Promised::String("test-space".to_string()),
        );
        args.insert(
            "cell".to_string(),
            Promised::String("test-cell".to_string()),
        );

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
    async fn it_acquires_and_performs_archive_get() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials =
            Credentials::private(address, "access-key-id", "secret-access-key").unwrap();

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
    async fn it_acquires_and_performs_archive_put() {
        use crate::{Address, s3::Credentials};

        let subject_signer = test_signer();

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials =
            Credentials::private(address, "access-key-id", "secret-access-key").unwrap();

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

    #[dialog_common::test]
    async fn it_provides_authorized_requests() -> anyhow::Result<()> {
        let signer = ed25519_dalek::SigningKey::from_bytes(&[0u8; 32]);
        let operator = Ed25519Signer::from(signer);

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials =
            s3::Credentials::private(address, "access-key-id", "secret-access-key").unwrap();

        let provider = UcanAuthorizer::new(credentials);

        let credentials = Credentials::new(
            "https://access.ucan.com".into(),
            test_delegation_chain(&operator, operator.did(), &["archive"]),
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

        let payload = ucan.to_bytes()?;

        let authorization = provider.authorize(&payload).await?;
        assert_eq!(
            authorization.url.path(),
            format!(
                "/{}/blobs/{}",
                operator.did(),
                Blake3Hash::hash(b"hello").as_bytes().to_base58()
            )
        );

        Ok(())
    }
}
