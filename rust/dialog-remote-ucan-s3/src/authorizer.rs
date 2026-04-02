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
//! Subject (root) -> Delegation[n-1] -> ... -> Delegation[0] -> Invocation.issuer
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use dialog_remote_ucan_s3::UcanAuthorizer;
//! use dialog_remote_s3::s3::S3Credentials;
//! use dialog_remote_s3::Address;
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
//! let s3_credentials = S3Credentials::new(
//!     "access-key-id",
//!     "secret-access-key",
//! );
//!
//! // Wrap with UCAN authorizer
//! let authorizer = UcanAuthorizer::new(address.with_credentials(s3_credentials));
//!
//! // Handle incoming UCAN container
//! let container_bytes: Vec<u8> = vec![]; // UCAN container from request
//! let result = authorizer.authorize(&container_bytes).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;

use dialog_capability::{Capability, Constraint, Did, Policy};
use dialog_credentials::Ed25519KeyResolver;
use dialog_effects::{archive, memory, storage};
use dialog_remote_s3::Address;
use dialog_remote_s3::{AccessError, Permit};
use dialog_ucan::InvocationChain;
use dialog_ucan::promise::Promised;
use ipld_core::ipld::Ipld;
use serde::de::DeserializeOwned;

// Generic deserialization from UCAN args

type Args = BTreeMap<String, Promised>;

/// Deserialize a typed struct from UCAN args via IPLD round-trip.
///
/// Converts `Promised` values to IPLD, then uses `ipld_core::serde::from_ipld`
/// to deserialize the target type. Unknown fields are ignored, so this works
/// on the flat args map containing fields from all capability chain layers.
fn deserialize_from_args<T: DeserializeOwned>(args: &Args) -> Result<T, AccessError> {
    let ipld_map: BTreeMap<String, Ipld> = args
        .iter()
        .map(|(k, v)| {
            Ipld::try_from(v)
                .map(|ipld| (k.clone(), ipld))
                .map_err(|e| {
                    AccessError::Invocation(format!("Unresolved promise for '{}': {}", k, e))
                })
        })
        .collect::<Result<_, _>>()?;

    ipld_core::serde::from_ipld(Ipld::Map(ipld_map))
        .map_err(|e| AccessError::Invocation(format!("Failed to deserialize: {}", e)))
}

/// Build a storage capability from UCAN args: `Subject -> Storage -> Store -> Claim`.
fn storage_claim_from_args<C>(subject: &Did, args: &Args) -> Result<Capability<C>, AccessError>
where
    C: Policy<Of = storage::Store> + DeserializeOwned,
    <C as Constraint>::Capability: dialog_capability::Ability,
{
    let store: storage::Store = deserialize_from_args(args)?;
    let claim: C = deserialize_from_args(args)?;
    Ok(dialog_capability::Subject::from(subject.clone())
        .attenuate(storage::Storage)
        .attenuate(store)
        .attenuate(claim))
}

/// Build a memory capability from UCAN args: `Subject -> Memory -> Space -> Cell -> Claim`.
fn memory_claim_from_args<C>(subject: &Did, args: &Args) -> Result<Capability<C>, AccessError>
where
    C: Policy<Of = memory::Cell> + DeserializeOwned,
    <C as Constraint>::Capability: dialog_capability::Ability,
{
    let space: memory::Space = deserialize_from_args(args)?;
    let cell: memory::Cell = deserialize_from_args(args)?;
    let claim: C = deserialize_from_args(args)?;
    Ok(dialog_capability::Subject::from(subject.clone())
        .attenuate(memory::Memory)
        .attenuate(space)
        .attenuate(cell)
        .attenuate(claim))
}

/// Build an archive capability from UCAN args: `Subject -> Archive -> Catalog -> Claim`.
fn archive_claim_from_args<C>(subject: &Did, args: &Args) -> Result<Capability<C>, AccessError>
where
    C: Policy<Of = archive::Catalog> + DeserializeOwned,
    <C as Constraint>::Capability: dialog_capability::Ability,
{
    let catalog: archive::Catalog = deserialize_from_args(args)?;
    let claim: C = deserialize_from_args(args)?;
    Ok(dialog_capability::Subject::from(subject.clone())
        .attenuate(archive::Archive)
        .attenuate(catalog)
        .attenuate(claim))
}

/// Maps an execution effect type to its claim type that can be
/// reconstructed from UCAN args.
///
/// The `Claim` associated type is the authorization-safe representation
/// whose `Capability<Claim>` implements `Access`.
trait FromUcanArgs {
    /// The claim type for this effect (either Self or a generated {Name}Claim).
    type Claim: Constraint;

    /// Reconstruct a capability from UCAN args.
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError>;
}

impl FromUcanArgs for storage::Get {
    type Claim = storage::Get;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        storage_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for storage::Set {
    type Claim = storage::SetClaim;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        storage_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for storage::Delete {
    type Claim = storage::Delete;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        storage_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for storage::List {
    type Claim = storage::List;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        storage_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for memory::Resolve {
    type Claim = memory::Resolve;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        let space: memory::Space = deserialize_from_args(args)?;
        let cell: memory::Cell = deserialize_from_args(args)?;
        Ok(dialog_capability::Subject::from(subject.clone())
            .attenuate(memory::Memory)
            .attenuate(space)
            .attenuate(cell)
            .attenuate(memory::Resolve))
    }
}
impl FromUcanArgs for memory::Publish {
    type Claim = memory::PublishClaim;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        memory_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for memory::Retract {
    type Claim = memory::RetractClaim;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        memory_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for archive::Get {
    type Claim = archive::Get;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        archive_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for archive::Put {
    type Claim = archive::PutClaim;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, AccessError> {
        archive_claim_from_args(subject, args)
    }
}

/// Dispatch UCAN command to the appropriate `FromUcanArgs` handler and authorize
/// the resulting capability directly against the S3 address.
macro_rules! dispatch {
    ($self:expr, $subject:expr, $args:expr, $segments:expr, {
        $( [$seg1:literal, $seg2:literal] => $fx:ty ),+ $(,)?
    }) => {
        match $segments {
            $(
                [$seg1, $seg2] => {
                    let capability = <$fx as FromUcanArgs>::capability_from_args($subject, $args)?;
                    $self.address.authorize(&capability).await
                }
            )+
            _ => Err(AccessError::Invocation(format!("Unknown command: {:?}", $segments)))
        }
    };
}

/// UCAN authorizer that wraps credentials and handles UCAN invocations.
///
/// This is the server-side component that:
/// 1. Receives UCAN containers (invocation + delegations)
/// 2. Verifies the delegation chain
/// 3. Extracts commands and constructs effects
/// 4. Delegates to wrapped credentials for presigned URLs
#[derive(Debug, Clone)]
pub struct UcanAuthorizer {
    address: Address,
}

impl UcanAuthorizer {
    /// Create a new UCAN authorizer wrapping the given address.
    ///
    /// Credentials (if any) should be set on the address via `Address::with_credentials`.
    pub fn new(address: Address) -> Self {
        Self { address }
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
    pub async fn authorize(&self, container: &[u8]) -> Result<Permit, AccessError> {
        // Parse and verify the invocation chain
        let chain = InvocationChain::try_from(container)
            .map_err(|e| AccessError::Invocation(e.to_string()))?;
        chain
            .verify(&Ed25519KeyResolver)
            .await
            .map_err(|e| AccessError::Invocation(e.to_string()))?;

        // Extract command path and arguments
        let command = chain.command();
        let args = chain.arguments();

        // Get subject DID from the invocation
        let subject_did = chain.subject();

        let command_segments: Vec<&str> = command.0.iter().map(|s| s.as_str()).collect();

        dispatch!(self, subject_did, args, command_segments.as_slice(), {
            ["storage", "get"]     => dialog_effects::storage::Get,
            ["storage", "set"]     => dialog_effects::storage::Set,
            ["storage", "delete"]  => dialog_effects::storage::Delete,
            ["storage", "list"]    => dialog_effects::storage::List,
            ["memory", "resolve"]  => dialog_effects::memory::Resolve,
            ["memory", "publish"]  => dialog_effects::memory::Publish,
            ["memory", "retract"]  => dialog_effects::memory::Retract,
            ["archive", "get"]     => dialog_effects::archive::Get,
            ["archive", "put"]     => dialog_effects::archive::Put,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base58::ToBase58;
    use dialog_capability::Principal;
    use dialog_common::Blake3Hash;
    use dialog_credentials::Ed25519Signer;
    use dialog_remote_s3::Address;
    use dialog_remote_s3::s3;
    use dialog_remote_s3::s3::S3Credentials;
    use dialog_ucan::DelegationBuilder;
    use dialog_ucan::InvocationBuilder;
    use dialog_ucan::InvocationChain;
    use dialog_ucan::subject::Subject as DelegatedSubject;
    use std::collections::BTreeMap;

    /// Helper to create a test signer
    async fn test_signer() -> Ed25519Signer {
        Ed25519Signer::import(&[42u8; 32]).await.unwrap()
    }

    /// Build a valid UCAN container with invocation and delegation for testing
    async fn build_test_container(
        subject_signer: &Ed25519Signer,
        operator_signer: &Ed25519Signer,
        command: Vec<String>,
        args: BTreeMap<String, Promised>,
    ) -> Vec<u8> {
        let subject_did = subject_signer.did();

        // Create delegation: subject -> operator
        let delegation = DelegationBuilder::new()
            .issuer(subject_signer.clone())
            .audience(operator_signer)
            .subject(DelegatedSubject::Specific(subject_did.clone()))
            .command(command.clone())
            .try_build()
            .await
            .expect("Failed to build delegation");

        let delegation_cid = delegation.to_cid();

        // Create invocation: operator invokes on subject
        let invocation = InvocationBuilder::new()
            .issuer(operator_signer.clone())
            .audience(&subject_did)
            .subject(&subject_did)
            .command(command)
            .arguments(args)
            .proofs(vec![delegation_cid])
            .try_build()
            .await
            .expect("Failed to build invocation");

        // Build InvocationChain
        let mut delegations = std::collections::HashMap::new();
        delegations.insert(delegation_cid, std::sync::Arc::new(delegation));

        let chain = InvocationChain::new(invocation, delegations);
        chain.to_bytes().expect("Failed to serialize container")
    }

    #[dialog_common::test]
    async fn it_acquires_and_performs_storage_get() {
        let subject_signer = test_signer().await;

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        let operator_signer = Ed25519Signer::import(&[1u8; 32]).await.unwrap();

        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("key".to_string(), Promised::Bytes(b"test-key".to_vec()));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["storage".to_string(), "get".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("test-bucket"));
    }

    #[dialog_common::test]
    async fn it_acquires_and_performs_storage_set() {
        let subject_signer = test_signer().await;

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        let operator_signer = Ed25519Signer::import(&[1u8; 32]).await.unwrap();

        // Multihash format: [code, length, ...digest]
        // SHA-256 code is 0x12, length is 0x20 (32 bytes)
        let mut checksum_bytes = vec![0x12, 0x20];
        checksum_bytes.extend_from_slice(&[0u8; 32]);

        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("key".to_string(), Promised::Bytes(b"test-key".to_vec()));
        args.insert("checksum".to_string(), Promised::Bytes(checksum_bytes));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["storage".to_string(), "set".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "PUT");
    }

    #[dialog_common::test]
    async fn it_acquires_and_performs_memory_resolve() {
        let subject_signer = test_signer().await;

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        let operator_signer = Ed25519Signer::import(&[1u8; 32]).await.unwrap();

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
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok(), "memory/resolve failed: {:?}", result);
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
    }

    #[dialog_common::test]
    async fn it_acquires_and_performs_archive_get() {
        let subject_signer = test_signer().await;

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        let operator_signer = Ed25519Signer::import(&[1u8; 32]).await.unwrap();

        let mut args = BTreeMap::new();
        args.insert("catalog".to_string(), Promised::String("blobs".to_string()));
        args.insert("digest".to_string(), Promised::Bytes([0u8; 32].to_vec()));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["archive".to_string(), "get".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
    }

    #[dialog_common::test]
    async fn it_acquires_and_performs_archive_put() {
        let subject_signer = test_signer().await;

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        let operator_signer = Ed25519Signer::import(&[1u8; 32]).await.unwrap();

        // Multihash format: [code, length, ...digest]
        // SHA-256 code is 0x12, length is 0x20 (32 bytes)
        let mut checksum_bytes = vec![0x12, 0x20];
        checksum_bytes.extend_from_slice(&[0u8; 32]);

        let mut args = BTreeMap::new();
        args.insert("catalog".to_string(), Promised::String("blobs".to_string()));
        args.insert("digest".to_string(), Promised::Bytes([0u8; 32].to_vec()));
        args.insert("checksum".to_string(), Promised::Bytes(checksum_bytes));

        let container = build_test_container(
            &subject_signer,
            &operator_signer,
            vec!["archive".to_string(), "put".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "PUT");
    }

    #[dialog_common::test]
    async fn it_provides_authorized_requests() -> anyhow::Result<()> {
        let operator = Ed25519Signer::import(&[0u8; 32]).await.unwrap();

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = s3::S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        let digest = Blake3Hash::hash(b"hello");
        let args = BTreeMap::from([
            ("catalog".to_string(), Promised::String("blobs".into())),
            (
                "digest".to_string(),
                Promised::Bytes(digest.as_bytes().into()),
            ),
        ]);

        let payload = build_test_container(
            &operator,
            &operator,
            vec!["archive".into(), "get".into()],
            args,
        )
        .await;

        let authorization = authorizer.authorize(&payload).await?;
        assert_eq!(
            authorization.url.path(),
            format!(
                "/{}/blobs/{}",
                operator.did(),
                digest.as_bytes().to_base58()
            )
        );

        Ok(())
    }

    /// Build a self-invocation container (issuer == subject, no delegation).
    /// This is used when a subject acts on itself, which is inherently authorized.
    async fn build_self_invocation_container(
        signer: &Ed25519Signer,
        command: Vec<String>,
        args: BTreeMap<String, Promised>,
    ) -> Vec<u8> {
        let did = signer.did();

        // Self-invocation: issuer == subject, no proofs needed
        let invocation = InvocationBuilder::new()
            .issuer(signer.clone())
            .audience(&did)
            .subject(&did)
            .command(command)
            .arguments(args)
            .proofs(vec![]) // Empty proofs for self-auth
            .try_build()
            .await
            .expect("Failed to build invocation");

        let chain = InvocationChain::new(invocation, std::collections::HashMap::new());
        chain.to_bytes().expect("Failed to serialize container")
    }

    #[dialog_common::test]
    async fn it_authorizes_self_invocation_for_storage_get() {
        let signer = test_signer().await;

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("key".to_string(), Promised::Bytes(b"test-key".to_vec()));

        // Build self-invocation (issuer == subject, no delegation)
        let container = build_self_invocation_container(
            &signer,
            vec!["storage".to_string(), "get".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(
            result.is_ok(),
            "Self-invocation should be authorized: {:?}",
            result
        );

        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("test-bucket"));
    }

    #[dialog_common::test]
    async fn it_authorizes_self_invocation_for_storage_set() {
        let signer = test_signer().await;

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        // Multihash format: [code, length, ...digest]
        // SHA-256 code is 0x12, length is 0x20 (32 bytes)
        let mut checksum_bytes = vec![0x12, 0x20];
        checksum_bytes.extend_from_slice(&[0u8; 32]);

        let mut args = BTreeMap::new();
        args.insert("store".to_string(), Promised::String("index".to_string()));
        args.insert("key".to_string(), Promised::Bytes(b"test-key".to_vec()));
        args.insert("checksum".to_string(), Promised::Bytes(checksum_bytes));

        // Build self-invocation (issuer == subject, no delegation)
        let container = build_self_invocation_container(
            &signer,
            vec!["storage".to_string(), "set".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(
            result.is_ok(),
            "Self-invocation for storage/set should be authorized: {:?}",
            result
        );

        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "PUT");
    }

    #[dialog_common::test]
    async fn it_authorizes_self_invocation_for_archive_get() {
        let signer = test_signer().await;

        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "test-bucket",
        );
        let credentials = S3Credentials::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address.with_credentials(credentials));

        let mut args = BTreeMap::new();
        args.insert("catalog".to_string(), Promised::String("blobs".to_string()));
        args.insert(
            "digest".to_string(),
            Promised::Bytes(Blake3Hash::hash(b"test").as_bytes().to_vec()),
        );

        // Build self-invocation (issuer == subject, no delegation)
        let container = build_self_invocation_container(
            &signer,
            vec!["archive".to_string(), "get".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(
            result.is_ok(),
            "Self-invocation for archive/get should be authorized: {:?}",
            result
        );

        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
    }
}
