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
//! use dialog_remote_s3::{Address, S3Authorization, S3Credential};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let address = Address::builder("https://s3.us-east-1.amazonaws.com")
//!     .region("us-east-1")
//!     .bucket("my-bucket")
//!     .build()?;
//!
//! let auth = S3Authorization::from(S3Credential::new(
//!     "access-key-id",
//!     "secret-access-key",
//! ));
//!
//! let authorizer = UcanAuthorizer::new(address, auth);
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
use dialog_effects::{archive, memory};
use dialog_remote_s3::{Address, Permit, S3Authorization, S3Error};
use dialog_ucan_core::InvocationChain;
use dialog_ucan_core::promise::Promised;
use ipld_core::ipld::Ipld;
use serde::de::DeserializeOwned;

// Generic deserialization from UCAN args

type Args = BTreeMap<String, Promised>;

/// Deserialize a typed struct from UCAN args via IPLD round-trip.
///
/// Converts `Promised` values to IPLD, then uses `ipld_core::serde::from_ipld`
/// to deserialize the target type. Unknown fields are ignored, so this works
/// on the flat args map containing fields from all capability chain layers.
fn deserialize_from_args<T: DeserializeOwned>(args: &Args) -> Result<T, S3Error> {
    let ipld_map: BTreeMap<String, Ipld> = args
        .iter()
        .map(|(k, v)| {
            Ipld::try_from(v)
                .map(|ipld| (k.clone(), ipld))
                .map_err(|e| {
                    S3Error::Authorization(format!("Unresolved promise for '{}': {}", k, e))
                })
        })
        .collect::<Result<_, _>>()?;

    ipld_core::serde::from_ipld(Ipld::Map(ipld_map))
        .map_err(|e| S3Error::Authorization(format!("Failed to deserialize: {}", e)))
}

/// Build a memory capability from UCAN args: `Subject -> Memory -> Space -> Cell -> Claim`.
fn memory_claim_from_args<C>(subject: &Did, args: &Args) -> Result<Capability<C>, S3Error>
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
fn archive_claim_from_args<C>(subject: &Did, args: &Args) -> Result<Capability<C>, S3Error>
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
    fn capability_from_args(subject: &Did, args: &Args)
    -> Result<Capability<Self::Claim>, S3Error>;
}

impl FromUcanArgs for memory::Resolve {
    type Claim = memory::Resolve;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, S3Error> {
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
    ) -> Result<Capability<Self::Claim>, S3Error> {
        memory_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for memory::Retract {
    type Claim = memory::RetractClaim;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, S3Error> {
        memory_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for archive::Get {
    type Claim = archive::Get;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, S3Error> {
        archive_claim_from_args(subject, args)
    }
}
impl FromUcanArgs for archive::Put {
    type Claim = archive::PutClaim;
    fn capability_from_args(
        subject: &Did,
        args: &Args,
    ) -> Result<Capability<Self::Claim>, S3Error> {
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
                    $self.authorization.redeem(&capability, &$self.address).await
                }
            )+
            _ => Err(S3Error::Authorization(format!("Unknown command: {:?}", $segments)))
        }
    };
}

/// UCAN authorizer that wraps credentials and handles UCAN invocations.
///
/// This is the server-side component that:
/// 1. Receives UCAN containers (invocation + delegations)
/// 2. Verifies the delegation chain
/// 3. Extracts commands and constructs effects
/// 4. Delegates to S3 authorization for presigned URLs
#[derive(Debug, Clone)]
pub struct UcanAuthorizer {
    address: Address,
    authorization: S3Authorization,
}

impl UcanAuthorizer {
    /// Create a new UCAN authorizer with the given address and authorization.
    pub fn new(address: Address, authorization: S3Authorization) -> Self {
        Self {
            address,
            authorization,
        }
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
    pub async fn authorize(&self, container: &[u8]) -> Result<Permit, S3Error> {
        // Parse and verify the invocation chain
        let chain = InvocationChain::try_from(container)
            .map_err(|e| S3Error::Authorization(e.to_string()))?;
        chain
            .verify(&Ed25519KeyResolver)
            .await
            .map_err(|e| S3Error::Authorization(e.to_string()))?;

        // Extract command path and arguments
        let command = chain.command();
        let args = chain.arguments();

        // Get subject DID from the invocation
        let subject_did = chain.subject();

        let command_segments: Vec<&str> = command.0.iter().map(|s| s.as_str()).collect();

        dispatch!(self, subject_did, args, command_segments.as_slice(), {
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
    use dialog_remote_s3::s3::S3Credential;
    use dialog_ucan_core::DelegationBuilder;
    use dialog_ucan_core::InvocationBuilder;
    use dialog_ucan_core::InvocationChain;
    use dialog_ucan_core::subject::Subject as DelegatedSubject;
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
    async fn it_acquires_and_performs_memory_resolve() {
        let subject_signer = test_signer().await;

        let address = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("test-bucket")
            .build()
            .unwrap();
        let credentials = S3Credential::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address, S3Authorization::from(credentials));

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

        let address = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("test-bucket")
            .build()
            .unwrap();
        let credentials = S3Credential::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address, S3Authorization::from(credentials));

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

        let address = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("test-bucket")
            .build()
            .unwrap();
        let credentials = S3Credential::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address, S3Authorization::from(credentials));

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

        let address = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("test-bucket")
            .build()
            .unwrap();
        let credentials = s3::S3Credential::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address, S3Authorization::from(credentials));

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
    async fn it_authorizes_self_invocation_for_archive_get() {
        let signer = test_signer().await;

        let address = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("test-bucket")
            .build()
            .unwrap();
        let credentials = S3Credential::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address, S3Authorization::from(credentials));

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

    #[dialog_common::test]
    async fn it_authorizes_self_invocation_for_archive_put() {
        let signer = test_signer().await;

        let address = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("test-bucket")
            .build()
            .unwrap();
        let credentials = S3Credential::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address, S3Authorization::from(credentials));

        // Multihash format: [code, length, ...digest]
        // SHA-256 code is 0x12, length is 0x20 (32 bytes)
        let mut checksum_bytes = vec![0x12, 0x20];
        checksum_bytes.extend_from_slice(&[0xab; 32]);

        let mut args = BTreeMap::new();
        args.insert("catalog".to_string(), Promised::String("blobs".to_string()));
        args.insert("digest".to_string(), Promised::Bytes([0x99; 32].to_vec()));
        args.insert("checksum".to_string(), Promised::Bytes(checksum_bytes));

        let container = build_self_invocation_container(
            &signer,
            vec!["archive".to_string(), "put".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(
            result.is_ok(),
            "Self-invocation for archive/put should be authorized: {:?}",
            result
        );

        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "PUT");
    }

    #[dialog_common::test]
    async fn it_authorizes_self_invocation_for_memory_resolve() {
        let signer = test_signer().await;

        let address = Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("test-bucket")
            .build()
            .unwrap();
        let credentials = S3Credential::new("access-key-id", "secret-access-key");

        let authorizer = UcanAuthorizer::new(address, S3Authorization::from(credentials));

        let mut args = BTreeMap::new();
        args.insert(
            "space".to_string(),
            Promised::String("did:key:zSpace".to_string()),
        );
        args.insert("cell".to_string(), Promised::String("main".to_string()));

        let container = build_self_invocation_container(
            &signer,
            vec!["memory".to_string(), "resolve".to_string()],
            args,
        )
        .await;

        let result = authorizer.authorize(&container).await;
        assert!(
            result.is_ok(),
            "Self-invocation for memory/resolve should be authorized: {:?}",
            result
        );

        let descriptor = result.unwrap();
        assert_eq!(descriptor.method, "GET");
    }
}
