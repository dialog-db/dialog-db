//! This module provides [`Bucket`], a [`StorageBackend`] implementation
//! that allows you to use S3-compatible object storage as a key-value store.
//!
//! # Features
//!
//! - AWS SigV4 presigned URL signing for authorization
//! - Support for public (unsigned) and authenticated access
//! - Automatic key encoding to handle binary and special characters
//! - Checksum verification using SHA-256
//! - Compatible with S3-compatible services (AWS S3, Cloudflare R2)
//!
//! # Examples
//!
//! ## Public Access (No Authentication)
//!
//! For publicly accessible buckets that don't require authentication:
//!
//! ```no_run
//! use dialog_common::{Authority, capability::{Did, Principal}};
//! use dialog_storage::s3::{Address, S3, S3Credentials};
//! use dialog_storage::capability::{storage, Provider, Subject};
//!
//! // Define an issuer type for capability-based access
//! #[derive(Clone)]
//! struct Issuer(String);
//! impl Principal for Issuer {
//!     fn did(&self) -> &Did { &self.0 }
//! }
//! impl Authority for Issuer {
//!     fn sign(&mut self, _: &[u8]) -> Vec<u8> { Vec::new() }
//!     fn secret_key_bytes(&self) -> Option<[u8; 32]> { None }
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create address with endpoint, region, and bucket
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//! let credentials = S3Credentials::public(address)?;
//! let issuer = Issuer("did:key:zMyIssuer".into());
//! let mut bucket = S3::from_s3(credentials, issuer);
//!
//! // Use capability-based access with subject DID as the root
//! let subject = "did:key:zMySubject";
//! Subject::from(subject)
//!     .attenuate(storage::Storage)
//!     .attenuate(storage::Store::new("data"))
//!     .invoke(storage::Set {
//!         key: b"key".to_vec().into(),
//!         value: b"value".to_vec().into(),
//!     })
//!     .perform(&mut bucket)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Authorized Access (Credentials based Authentication)
//!
//! ```no_run
//! use dialog_common::{Authority, capability::{Did, Principal}};
//! use dialog_storage::s3::{Address, S3Credentials, S3};
//! use dialog_storage::capability::{storage, Provider, Subject};
//!
//! # #[derive(Clone)]
//! # struct Issuer(String);
//! # impl Principal for Issuer {
//! #     fn did(&self) -> &Did { &self.0 }
//! # }
//! # impl Authority for Issuer {
//! #     fn sign(&mut self, _: &[u8]) -> Vec<u8> { Vec::new() }
//! #     fn secret_key_bytes(&self) -> Option<[u8; 32]> { None }
//! # }
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//! let credentials = S3Credentials::private(
//!     address,
//!     std::env::var("AWS_ACCESS_KEY_ID")?,
//!     std::env::var("AWS_SECRET_ACCESS_KEY")?,
//! )?;
//!
//! let issuer = Issuer("did:key:zMyIssuer".into());
//! let mut bucket = S3::from_s3(credentials, issuer);
//!
//! // Subject DID identifies whose data we're accessing
//! let subject = "did:key:zMySubject";
//! Subject::from(subject)
//!     .attenuate(storage::Storage)
//!     .attenuate(storage::Store::new("data"))
//!     .invoke(storage::Set {
//!         key: b"key".to_vec().into(),
//!         value: b"value".to_vec().into(),
//!     })
//!     .perform(&mut bucket)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Cloudflare R2
//!
//! ```no_run
//! use dialog_common::{Authority, capability::{Did, Principal}};
//! use dialog_storage::s3::{Address, S3Credentials, S3};
//!
//! # #[derive(Clone)]
//! # struct Issuer(String);
//! # impl Principal for Issuer {
//! #     fn did(&self) -> &Did { &self.0 }
//! # }
//! # impl Authority for Issuer {
//! #     fn sign(&mut self, _: &[u8]) -> Vec<u8> { Vec::new() }
//! #     fn secret_key_bytes(&self) -> Option<[u8; 32]> { None }
//! # }
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // R2 uses "auto" region for signing
//! let address = Address::new(
//!     "https://account-id.r2.cloudflarestorage.com",
//!     "auto",
//!     "my-bucket",
//! );
//! let credentials = S3Credentials::private(
//!     address,
//!     std::env::var("R2_ACCESS_KEY_ID")?,
//!     std::env::var("R2_SECRET_ACCESS_KEY")?,
//! )?;
//!
//! let issuer = Issuer("did:key:zMyIssuer".into());
//! let bucket = S3::from_s3(credentials, issuer);
//! # Ok(())
//! # }
//! ```
//!
//! ## Local Development (MinIO)
//!
//! ```no_run
//! use dialog_common::{Authority, capability::{Did, Principal}};
//! use dialog_storage::s3::{Address, S3Credentials, S3};
//!
//! # #[derive(Clone)]
//! # struct Issuer(String);
//! # impl Principal for Issuer {
//! #     fn did(&self) -> &Did { &self.0 }
//! # }
//! # impl Authority for Issuer {
//! #     fn sign(&mut self, _: &[u8]) -> Vec<u8> { Vec::new() }
//! #     fn secret_key_bytes(&self) -> Option<[u8; 32]> { None }
//! # }
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // IP addresses and localhost automatically use path-style URLs
//! let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
//! let credentials = S3Credentials::private(address, "minioadmin", "minioadmin")?;
//! let issuer = Issuer("did:key:zMyIssuer".into());
//! let bucket = S3::from_s3(credentials, issuer);
//! // path_style is true by default for IP addresses and localhost
//! # Ok(())
//! # }
//! ```
//!
//! # Key Encoding
//!
//! Keys are automatically encoded to be S3-safe. Keys are treated as `/`-delimited
//! paths, and each segment is encoded independently:
//! - Segments containing only safe characters (`a-z`, `A-Z`, `0-9`, `-`, `_`, `.`) are kept as-is
//! - Segments containing unsafe characters or binary data are base58-encoded with a `!` prefix
//! - Path separators (`/`) preserve the S3 key hierarchy

use async_trait::async_trait;
use dialog_common::{
    Authority, Bytes, ConditionalSend, ConditionalSync,
    capability::{Ability, Access, Authorized, Capability, Claim, Did, Effect, Principal, Provider, Subject},
};
use thiserror::Error;

// Re-export core types from dialog-s3-credentials crate
pub use dialog_s3_credentials::{
    AccessError, Address, AuthorizedRequest, Checksum, Credentials, Hasher,
};
// Re-export S3-specific credentials type for direct use
pub use dialog_s3_credentials::s3::Credentials as S3Credentials;
// Use access module types for direct S3 authorization
pub use dialog_s3_credentials::capability::{Precondition, S3Request};

pub use crate::capability::{archive, memory, storage};

/// Extension trait for RequestDescriptor to convert to reqwest RequestBuilder.
pub trait RequestDescriptorExt {
    /// Convert into a reqwest RequestBuilder with the client.
    fn into_request(self, client: &reqwest::Client) -> reqwest::RequestBuilder;
}

impl RequestDescriptorExt for AuthorizedRequest {
    fn into_request(self, client: &reqwest::Client) -> reqwest::RequestBuilder {
        let mut builder = match self.method.as_str() {
            "GET" => client.get(self.url),
            "PUT" => client.put(self.url),
            "DELETE" => client.delete(self.url),
            _ => client.request(
                reqwest::Method::from_bytes(self.method.as_bytes()).unwrap(),
                self.url,
            ),
        };

        for (key, value) in self.headers {
            builder = builder.header(key, value);
        }

        builder
    }
}

mod key;
pub use key::{decode as decode_s3_key, encode as encode_s3_key};

#[cfg(feature = "s3-list")]
mod list;
#[cfg(feature = "s3-list")]
pub use list::ListResult;

#[cfg(feature = "s3-list")]
use crate::StorageSource;
use crate::{DialogStorageError, StorageBackend, StorageSink, TransactionalMemoryBackend};

// Testing helpers module:
// - Address types (S3Address, PublicS3Address, UcanS3Address) are available on all platforms
// - Server implementation is native-only (internal to the helpers module)
#[cfg(any(feature = "helpers", test))]
pub mod helpers;
#[cfg(all(feature = "helpers", not(target_arch = "wasm32")))]
pub use helpers::{LocalS3, PublicS3Settings, S3Settings};
#[cfg(any(feature = "helpers", test))]
pub use helpers::{PublicS3Address, S3Address, Session, UcanS3Address};
#[cfg(all(feature = "helpers", feature = "ucan", not(target_arch = "wasm32")))]
pub use helpers::{Operator, UcanAccessServer, UcanS3Settings};
#[cfg(all(feature = "helpers", feature = "ucan", target_arch = "wasm32"))]
pub use helpers::Operator;

use self::archive::ArchiveError;

/// Errors that can occur when using the S3 storage backend.
#[derive(Error, Debug)]
pub enum S3StorageError {
    /// Failed to authorize the request (signing or credential issues).
    #[error("Authorization error: {0}")]
    AuthorizationError(String),

    /// Transport-level error (connection failed, timeout, network issues).
    #[error("Transport error: {0}")]
    TransportError(String),

    /// Service-level error (S3 returned an error response).
    #[error("Service error: {0}")]
    ServiceError(String),

    /// Error during serialization or deserialization of data.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// CAS edition mismatch (concurrent modification detected).
    #[error("Edition mismatch: expected {expected:?}, got {actual:?}")]
    EditionMismatch {
        /// The expected edition.
        expected: Option<String>,
        /// The actual edition found.
        actual: Option<String>,
    },
}

impl From<S3StorageError> for DialogStorageError {
    fn from(error: S3StorageError) -> Self {
        DialogStorageError::StorageBackend(error.to_string())
    }
}

impl From<reqwest::Error> for S3StorageError {
    fn from(error: reqwest::Error) -> Self {
        S3StorageError::TransportError(error.to_string())
    }
}

impl From<AccessError> for S3StorageError {
    fn from(error: AccessError) -> Self {
        S3StorageError::AuthorizationError(error.to_string())
    }
}

/// Trait for credentials that can authorize S3 operations.
///
/// Implementations can sign claims to produce RequestDescriptors.
pub trait Authorizer: Clone + std::fmt::Debug + Send + Sync {
    /// Get the subject DID (path prefix within the bucket).
    fn subject(&self) -> &str;

    /// Authorize a claim and produce a request descriptor.
    fn authorize<C: S3Request>(&self, claim: &C) -> Result<AuthorizedRequest, AccessError>;
}

trait ArchiveProvider: Provider<archive::AuthorizeGet> + Provider<archive::AuthorizePut> {}
impl<P: Provider<archive::AuthorizeGet> + Provider<archive::AuthorizePut>> ArchiveProvider for P {}

/// S3-backed storage that implements Provider for capability-based operations.
///
/// This type provides access to S3-compatible storage using the capability-based
/// authorization model. It can be used with both direct S3 credentials and
/// S3-compatible storage bucket with capability-based access control.
///
/// This bucket supports both S3 credentials (SigV4 signing) and
/// UCAN-based delegated authorization.
///
/// The `Issuer` type parameter represents the authority that signs requests.
/// For simple S3 usage, this can be any type implementing `Authority`.
/// For UCAN-based access, this would typically be an `Operator` from dialog-artifacts.
#[derive(Debug, Clone)]
pub struct S3<Issuer> {
    credentials: Credentials,
    issuer: Issuer,
}

impl<Issuer> S3<Issuer> {
    /// Create a new S3 with the given credentials and issuer.
    pub fn new(credentials: Credentials, issuer: Issuer) -> Self {
        Self { credentials, issuer }
    }
}

impl<Issuer: Clone> S3<Issuer> {
    /// Create a new S3 from S3 credentials and issuer.
    pub fn from_s3(credentials: dialog_s3_credentials::s3::Credentials, issuer: Issuer) -> Self {
        Self {
            credentials: Credentials::S3(credentials),
            issuer,
        }
    }
}

// Implement Principal for S3 by delegating to the issuer
impl<Issuer: Principal> Principal for S3<Issuer> {
    fn did(&self) -> &Did {
        self.issuer.did()
    }
}

// Implement Authority for S3 by delegating to the issuer
impl<Issuer: Authority> Authority for S3<Issuer> {
    fn sign(&mut self, payload: &[u8]) -> Vec<u8> {
        self.issuer.sign(payload)
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        self.issuer.secret_key_bytes()
    }
}

// Implement Access for S3 by delegating to credentials
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer: ConditionalSend + ConditionalSync> Access for S3<Issuer> {
    type Authorization = dialog_s3_credentials::Authorization;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        self.credentials.claim(claim).await
    }
}

// Implement Provider<Authorized<...>> for S3 by delegating to credentials
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer, Do> Provider<Authorized<Do, dialog_s3_credentials::Authorization>> for S3<Issuer>
where
    Issuer: ConditionalSend + ConditionalSync,
    Do: Effect<Output = Result<AuthorizedRequest, AccessError>> + 'static,
    Capability<Do>: ConditionalSend + S3Request,
{
    async fn execute(
        &mut self,
        authorized: Authorized<Do, dialog_s3_credentials::Authorization>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.credentials.execute(authorized).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<archive::Get> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<archive::Get>,
    ) -> Result<Option<Bytes>, archive::ArchiveError> {
        // Build the authorization capability
        let catalog: &archive::Catalog = input.policy();
        let get: &archive::Get = input.policy();
        let capability = Subject::from(input.subject().to_string())
            .attenuate(archive::Archive)
            .attenuate(catalog.clone())
            .invoke(archive::AuthorizeGet {
                digest: get.digest.clone(),
            });

        // Acquire authorization and perform using self (which implements Access + Authority)
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| ArchiveError::AuthorizationError(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| ArchiveError::ExecutionError(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
            .send()
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        if response.status().is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| ArchiveError::Io(e.to_string()))?;
            Ok(Some(bytes.to_vec().into()))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(archive::ArchiveError::Storage(format!(
                "Failed to get value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<archive::Put> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<archive::Put>,
    ) -> Result<(), archive::ArchiveError> {
        let catalog: &archive::Catalog = input.policy();
        let put: &archive::Put = input.policy();
        let content = put.content.clone();
        let checksum = Hasher::Sha256.checksum(&content);

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(archive::Archive)
            .attenuate(catalog.clone())
            .invoke(archive::AuthorizePut {
                digest: put.digest.clone(),
                checksum,
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| ArchiveError::AuthorizationError(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| ArchiveError::ExecutionError(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client).body(content.to_vec());
        let response = builder
            .send()
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(archive::ArchiveError::Storage(format!(
                "Failed to put value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<memory::Resolve> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<memory::Resolve>,
    ) -> Result<Option<memory::Publication>, memory::MemoryError> {
        // Build the authorization capability
        let space: &memory::Space = input.policy();
        let cell: &memory::Cell = input.policy();
        let capability = Subject::from(input.subject().to_string())
            .attenuate(memory::Memory)
            .attenuate(space.clone())
            .attenuate(cell.clone())
            .invoke(memory::AuthorizeResolve);

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| memory::MemoryError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| memory::MemoryError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
            .send()
            .await
            .map_err(|e| memory::MemoryError::Storage(e.to_string()))?;

        if response.status().is_success() {
            // Extract ETag from response headers as the edition
            let edition = response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim_matches('"').to_string())
                .ok_or_else(|| {
                    memory::MemoryError::Storage("Response missing ETag header".to_string())
                })?;

            let bytes = response
                .bytes()
                .await
                .map_err(|e| memory::MemoryError::Storage(e.to_string()))?;

            Ok(Some(memory::Publication {
                content: bytes.to_vec().into(),
                edition: edition.into_bytes().into(),
            }))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(memory::MemoryError::Storage(format!(
                "Failed to resolve value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<memory::Publish> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<memory::Publish>,
    ) -> Result<Bytes, memory::MemoryError> {
        let space: &memory::Space = input.policy();
        let cell: &memory::Cell = input.policy();
        let publish: &memory::Publish = input.policy();
        let content = publish.content.clone();
        let when = publish.when.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
        let checksum = Hasher::Sha256.checksum(&content);

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(memory::Memory)
            .attenuate(space.clone())
            .attenuate(cell.clone())
            .invoke(memory::AuthorizePublish { checksum, when: when.clone() });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| memory::MemoryError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| memory::MemoryError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client).body(content.to_vec());
        let response = builder
            .send()
            .await
            .map_err(|e| memory::MemoryError::Storage(e.to_string()))?;

        match response.status() {
            status if status.is_success() => {
                // Extract new ETag from response as the new edition
                let new_edition = response
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.trim_matches('"').to_string())
                    .ok_or_else(|| {
                        memory::MemoryError::Storage("Response missing ETag header".to_string())
                    })?;
                Ok(new_edition.into_bytes().into())
            }
            reqwest::StatusCode::PRECONDITION_FAILED => Err(memory::MemoryError::EditionMismatch {
                expected: when,
                actual: None,
            }),
            status => Err(memory::MemoryError::Storage(format!(
                "Failed to publish value: {}",
                status
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<memory::Retract> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<memory::Retract>,
    ) -> Result<(), memory::MemoryError> {
        let space: &memory::Space = input.policy();
        let cell: &memory::Cell = input.policy();
        let retract: &memory::Retract = input.policy();
        let when = String::from_utf8_lossy(&retract.when).to_string();

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(memory::Memory)
            .attenuate(space.clone())
            .attenuate(cell.clone())
            .invoke(memory::AuthorizeRetract { when: when.clone() });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| memory::MemoryError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| memory::MemoryError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
            .send()
            .await
            .map_err(|e| memory::MemoryError::Storage(e.to_string()))?;

        match response.status() {
            status if status.is_success() => Ok(()),
            reqwest::StatusCode::PRECONDITION_FAILED => Err(memory::MemoryError::EditionMismatch {
                expected: Some(when),
                actual: None,
            }),
            status => Err(memory::MemoryError::Storage(format!(
                "Failed to retract value: {}",
                status
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<storage::Get> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<storage::Get>,
    ) -> Result<Option<Bytes>, storage::StorageError> {
        // Build the authorization capability
        let store: &storage::Store = input.policy();
        let get: &storage::Get = input.policy();
        let capability = Subject::from(input.subject().to_string())
            .attenuate(storage::Storage)
            .attenuate(store.clone())
            .invoke(storage::AuthorizeGet {
                key: get.key.clone(),
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| storage::StorageError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
            .send()
            .await
            .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

        if response.status().is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| storage::StorageError::Storage(e.to_string()))?;
            Ok(Some(bytes.to_vec().into()))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(storage::StorageError::Storage(format!(
                "Failed to get value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<storage::Set> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<storage::Set>,
    ) -> Result<(), storage::StorageError> {
        let store: &storage::Store = input.policy();
        let set: &storage::Set = input.policy();
        let value = set.value.clone();
        let checksum = Hasher::Sha256.checksum(&value);

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(storage::Storage)
            .attenuate(store.clone())
            .invoke(storage::AuthorizeSet {
                key: set.key.clone(),
                checksum,
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| storage::StorageError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client).body(value.to_vec());
        let response = builder
            .send()
            .await
            .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(storage::StorageError::Storage(format!(
                "Failed to set value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<storage::Delete> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<storage::Delete>,
    ) -> Result<(), storage::StorageError> {
        // Build the authorization capability
        let store: &storage::Store = input.policy();
        let delete: &storage::Delete = input.policy();
        let capability = Subject::from(input.subject().to_string())
            .attenuate(storage::Storage)
            .attenuate(store.clone())
            .invoke(storage::AuthorizeDelete {
                key: delete.key.clone(),
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| storage::StorageError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
            .send()
            .await
            .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(storage::StorageError::Storage(format!(
                "Failed to delete value: {}",
                response.status()
            )))
        }
    }
}

/// A scoped S3 storage backend implementing `StorageBackend` and `TransactionalMemoryBackend`.
///
/// This is a wrapper around [`S3`] that adds the subject DID and namespace path
/// required for the `StorageBackend` and `TransactionalMemoryBackend` traits.
///
/// # Example
///
/// ```no_run
/// use dialog_common::{Authority, capability::{Did, Principal}};
/// use dialog_storage::s3::{S3, S3Credentials, Address, Bucket};
/// use dialog_storage::StorageBackend;
///
/// // Define an issuer type for capability-based access
/// #[derive(Clone)]
/// struct Issuer(String);
/// impl Principal for Issuer {
///     fn did(&self) -> &Did { &self.0 }
/// }
/// impl Authority for Issuer {
///     fn sign(&mut self, _: &[u8]) -> Vec<u8> { Vec::new() }
///     fn secret_key_bytes(&self) -> Option<[u8; 32]> { None }
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
/// let credentials = S3Credentials::public(address)?;
/// let issuer = Issuer("did:key:zMyIssuer".into());
/// let bucket = S3::from_s3(credentials, issuer);
///
/// // Create a scoped bucket for StorageBackend operations
/// let mut storage = Bucket::new(bucket, "did:key:zMySubject", "my-store");
///
/// // Now you can use StorageBackend methods
/// storage.set(b"key".to_vec(), b"value".to_vec()).await?;
/// let value = storage.get(&b"key".to_vec()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Bucket<Issuer> {
    bucket: S3<Issuer>,
    /// The subject DID (whose data we're accessing)
    subject: Did,
    /// The namespace path (store for StorageBackend, space for TransactionalMemoryBackend)
    path: String,
}

impl<Issuer> Bucket<Issuer> {
    /// Create a new scoped S3 bucket.
    ///
    /// - `bucket`: The underlying S3
    /// - `subject`: The subject DID (whose data we're accessing)
    /// - `path`: The namespace path (store for storage, space for memory)
    pub fn new(bucket: S3<Issuer>, subject: impl Into<Did>, path: impl Into<String>) -> Self {
        Self {
            bucket,
            subject: subject.into(),
            path: path.into(),
        }
    }

    /// Get the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Get the namespace path.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl<Issuer: Clone> Bucket<Issuer> {
    /// Create a new scoped bucket with a different path (nested namespace).
    pub fn at(&self, path: impl Into<String>) -> Self {
        Self {
            bucket: self.bucket.clone(),
            subject: self.subject.clone(),
            path: format!("{}/{}", self.path, path.into()),
        }
    }
}

// Forward Principal trait to the underlying bucket
impl<Issuer: Principal> Principal for Bucket<Issuer> {
    fn did(&self) -> &Did {
        self.bucket.did()
    }
}

// Forward Authority trait to the underlying bucket
impl<Issuer: Authority> Authority for ScopedS3Bucket<Issuer> {
    fn sign(&mut self, payload: &[u8]) -> Vec<u8> {
        self.bucket.sign(payload)
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        self.bucket.secret_key_bytes()
    }
}

// Forward Access trait to the underlying bucket
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer: ConditionalSend + ConditionalSync> Access for Bucket<Issuer> {
    type Authorization = dialog_s3_credentials::Authorization;
    type Error = AccessError;

    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization, Self::Error> {
        self.bucket.claim(claim).await
    }
}

// Forward Provider<Authorized<...>> to the underlying bucket
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer, Do> Provider<Authorized<Do, dialog_s3_credentials::Authorization>>
    for Bucket<Issuer>
where
    Issuer: ConditionalSend + ConditionalSync,
    Do: Effect<Output = Result<AuthorizedRequest, AccessError>> + 'static,
    Capability<Do>: ConditionalSend + S3Request,
{
    async fn execute(
        &mut self,
        authorized: Authorized<Do, dialog_s3_credentials::Authorization>,
    ) -> Result<AuthorizedRequest, AccessError> {
        self.bucket.execute(authorized).await
    }
}

// Implement StorageBackend for Bucket
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> StorageBackend for Bucket<Issuer>
where
    Issuer: Authority + Clone + ConditionalSend + ConditionalSync,
{
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Error = S3StorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        // Build the capability
        let capability: Capability<storage::Set> = Subject::from(self.subject.clone())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new(&self.path))
            .invoke(storage::Set {
                key: key.into(),
                value: value.clone().into(),
            });

        // Execute via Provider
        Provider::<storage::Set>::execute(&mut self.bucket, capability)
            .await
            .map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        // Build the capability
        let capability: Capability<storage::Get> = Subject::from(self.subject.clone())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new(&self.path))
            .invoke(storage::Get {
                key: key.clone().into(),
            });

        // We need a mutable reference for Provider, so clone the bucket
        let mut bucket = self.bucket.clone();
        Provider::<storage::Get>::execute(&mut bucket, capability)
            .await
            .map(|opt| opt.map(|b| b.to_vec()))
            .map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }
}

// Implement TransactionalMemoryBackend for Bucket
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> TransactionalMemoryBackend for Bucket<Issuer>
where
    Issuer: Authority + Clone + ConditionalSend + ConditionalSync,
{
    type Address = Vec<u8>;
    type Value = Vec<u8>;
    type Error = S3StorageError;
    type Edition = String;

    async fn resolve(
        &mut self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        // Encode the address as cell name
        let cell = encode_s3_key(address);

        // Build the capability
        let capability: Capability<memory::Resolve> = Subject::from(self.subject.clone())
            .attenuate(memory::Memory)
            .attenuate(memory::Space::new(&self.path))
            .attenuate(memory::Cell::new(&cell))
            .invoke(memory::Resolve);

        // Execute via Provider
        let result = Provider::<memory::Resolve>::execute(&mut self.bucket, capability)
            .await
            .map_err(|e| S3StorageError::ServiceError(e.to_string()))?;

        Ok(result.map(|pub_| (pub_.content.to_vec(), String::from_utf8_lossy(&pub_.edition).to_string())))
    }

    async fn replace(
        &mut self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        // Encode the address as cell name
        let cell = encode_s3_key(address);

        match content {
            Some(value) => {
                // Publish (create or update)
                let capability: Capability<memory::Publish> = Subject::from(self.subject.clone())
                    .attenuate(memory::Memory)
                    .attenuate(memory::Space::new(&self.path))
                    .attenuate(memory::Cell::new(&cell))
                    .invoke(memory::Publish {
                        content: value.into(),
                        when: edition.map(|e| e.as_bytes().to_vec().into()),
                    });

                let new_edition = Provider::<memory::Publish>::execute(&mut self.bucket, capability)
                    .await
                    .map_err(|e| match e {
                        memory::MemoryError::EditionMismatch { .. } => S3StorageError::EditionMismatch {
                            expected: edition.map(|e| e.to_string()),
                            actual: None,
                        },
                        e => S3StorageError::ServiceError(e.to_string()),
                    })?;

                Ok(Some(String::from_utf8_lossy(&new_edition).to_string()))
            }
            None => {
                // Retract (delete)
                let when = edition
                    .ok_or_else(|| S3StorageError::ServiceError("Edition required for delete".into()))?;

                let capability: Capability<memory::Retract> = Subject::from(self.subject.clone())
                    .attenuate(memory::Memory)
                    .attenuate(memory::Space::new(&self.path))
                    .attenuate(memory::Cell::new(&cell))
                    .invoke(memory::Retract {
                        when: when.as_bytes().to_vec().into(),
                    });

                Provider::<memory::Retract>::execute(&mut self.bucket, capability)
                    .await
                    .map_err(|e| match e {
                        memory::MemoryError::EditionMismatch { .. } => S3StorageError::EditionMismatch {
                            expected: edition.map(|e| e.to_string()),
                            actual: None,
                        },
                        e => S3StorageError::ServiceError(e.to_string()),
                    })?;

                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_s3_credentials::s3::Credentials as S3Credentials;
    #[cfg(all(feature = "helpers", feature = "integration-tests"))]
    use helpers::*;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    fn test_address() -> Address {
        Address::new("https://s3.amazonaws.com", "us-east-1", "bucket")
    }

    fn test_credentials() -> S3Credentials {
        S3Credentials::public(test_address()).unwrap()
    }

    /// Test issuer that implements Authority for S3 direct access testing.
    /// For S3 direct access, signing is a no-op since S3 uses its own SigV4 signing.
    #[derive(Clone)]
    struct TestIssuer {
        did: String,
    }

    impl TestIssuer {
        fn new(did: impl Into<String>) -> Self {
            Self { did: did.into() }
        }
    }

    impl dialog_common::capability::Principal for TestIssuer {
        fn did(&self) -> &dialog_common::capability::Did {
            &self.did
        }
    }

    impl Authority for TestIssuer {
        fn sign(&mut self, _payload: &[u8]) -> Vec<u8> {
            // S3 direct access doesn't need external signing
            Vec::new()
        }

        fn secret_key_bytes(&self) -> Option<[u8; 32]> {
            None
        }
    }

    mod s3bucket_provider_tests {
        use super::*;

        fn create_test_bucket(env: &helpers::PublicS3Address) -> S3Bucket<TestIssuer> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
            S3Bucket::from_s3(s3_creds, TestIssuer::new(TEST_SUBJECT))
        }

        #[dialog_common::test]
        async fn it_performs_storage_get_and_set(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            // Create a storage Set capability
            let key = b"test-provider-key".to_vec();
            let value = b"test-provider-value".to_vec();

            // Execute the set operation using perform()
            Subject::from(TEST_SUBJECT)
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new("test"))
                .invoke(storage::Set {
                    key: key.clone().into(),
                    value: value.clone().into(),
                })
                .perform(&mut bucket)
                .await?;

            // Execute the get operation using perform()
            let result = Subject::from(TEST_SUBJECT)
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new("test"))
                .invoke(storage::Get {
                    key: key.clone().into(),
                })
                .perform(&mut bucket)
                .await?;

            assert_eq!(result, Some(value.into()));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_performs_archive_get_and_put(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            // Create content and compute its digest
            let content = b"test archive content".to_vec();
            let digest = dialog_common::Blake3Hash::hash(&content);

            // Execute the put operation using perform()
            Subject::from(TEST_SUBJECT)
                .attenuate(archive::Archive)
                .attenuate(archive::Catalog::new("test"))
                .invoke(archive::Put {
                    digest: digest.clone(),
                    content: content.clone().into(),
                })
                .perform(&mut bucket)
                .await?;

            // Execute the get operation using perform()
            let result = Subject::from(TEST_SUBJECT)
                .attenuate(archive::Archive)
                .attenuate(archive::Catalog::new("test"))
                .invoke(archive::Get {
                    digest: digest.clone(),
                })
                .perform(&mut bucket)
                .await?;

            assert_eq!(result, Some(content.into()));

            Ok(())
        }
    }

    #[cfg(all(feature = "helpers", feature = "integration-tests", feature = "ucan"))]
    mod ucan_provider_tests {
        use super::*;
        use dialog_common::capability::Subject;
        use dialog_s3_credentials::ucan::{
            Credentials as UcanCredentials, DelegationChain,
            test_helpers::{create_delegation, generate_signer},
        };
        use ucan::did::Ed25519Signer;

        /// Operator wraps a signing key and provides Principal + Authority.
        /// This is compatible with the Operator from dialog-artifacts.
        #[derive(Clone)]
        struct Operator {
            signer: Ed25519Signer,
            did: String,
        }

        impl Operator {
            fn new(signer: Ed25519Signer) -> Self {
                let did = signer.did().to_string();
                Self { signer, did }
            }

            fn generate() -> Self {
                Self::new(generate_signer())
            }
        }

        impl dialog_common::capability::Principal for Operator {
            fn did(&self) -> &dialog_common::capability::Did {
                &self.did
            }
        }

        impl Authority for Operator {
            fn sign(&mut self, payload: &[u8]) -> Vec<u8> {
                use ed25519_dalek::Signer;
                self.signer.signer().sign(payload).to_vec()
            }

            fn secret_key_bytes(&self) -> Option<[u8; 32]> {
                Some(self.signer.signer().to_bytes())
            }
        }

        /// Helper to create a test delegation chain from subject to operator.
        fn create_test_delegation_chain(
            subject_signer: &Ed25519Signer,
            operator_did: &ucan::did::Ed25519Did,
            can: &[&str],
        ) -> DelegationChain {
            let subject_did = subject_signer.did().clone();
            let delegation = create_delegation(subject_signer, operator_did, &subject_did, can)
                .expect("Failed to create test delegation");
            DelegationChain::new(delegation)
        }

        fn create_ucan_bucket(
            env: &helpers::UcanS3Address,
            operator: Operator,
            delegation: DelegationChain,
        ) -> S3Bucket<Operator> {
            let ucan_credentials = UcanCredentials::new(
                env.access_service_url.clone(),
                delegation,
            );
            S3Bucket::new(Credentials::Ucan(ucan_credentials), operator)
        }

        #[dialog_common::test]
        async fn it_performs_archive_get_and_put_with_ucan(
            env: helpers::UcanS3Address,
        ) -> anyhow::Result<()> {
            // Create operator
            let operator = Operator::generate();

            // Create delegation chain: subject delegates to operator
            // For this test, subject and operator are the same
            let delegation = create_test_delegation_chain(
                &operator.signer,
                &operator.signer.did(),
                &["archive"],
            );

            let subject_did = operator.did.clone();

            // Create bucket with UCAN credentials and operator
            let mut bucket = create_ucan_bucket(&env, operator, delegation);

            // Create content and compute its digest
            let content = b"test ucan archive content".to_vec();
            let digest = dialog_common::Blake3Hash::hash(&content);

            // Execute the put operation using perform()
            println!("Subject DID: {}", subject_did);
            println!("Access service URL: {}", env.access_service_url);

            let result = Subject::from(subject_did.clone())
                .attenuate(archive::Archive)
                .attenuate(archive::Catalog::new("blobs"))
                .invoke(archive::Put {
                    digest: digest.clone(),
                    content: content.clone().into(),
                })
                .perform(&mut bucket)
                .await;

            match &result {
                Ok(_) => println!("Put succeeded"),
                Err(e) => println!("Put failed: {:?}", e),
            }
            result?;

            // Execute the get operation using perform()
            let result = Subject::from(subject_did)
                .attenuate(archive::Archive)
                .attenuate(archive::Catalog::new("blobs"))
                .invoke(archive::Get {
                    digest: digest.clone(),
                })
                .perform(&mut bucket)
                .await?;

            assert_eq!(result, Some(content.into()));

            Ok(())
        }
    }

    // NOTE: The following tests are commented out because they use the legacy
    // Bucket API with Authorizer trait which is being replaced by S3Bucket with Provider trait.
    // TODO: Remove or update these tests once the new API is fully working.

    // #[dialog_common::test]
    // fn it_encodes_path_without_prefix() {
    //     // Test path encoding for binary keys
    //     let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(test_credentials()).unwrap();

    //     let path = backend.encode_path(&[1, 2, 3]);
    //     assert_eq!(path, "!Ldp");
    // }

    // #[dialog_common::test]
    // fn it_encodes_path_with_prefix() {
    //     // Path with prefix
    //     let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(test_credentials())
    //         .unwrap()
    //         .at("prefix");

    //     let path = backend.encode_path(&[1, 2, 3]);
    //     assert_eq!(path, "prefix/!Ldp");
    // }

    // #[dialog_common::test]
    // fn it_builds_virtual_hosted_url() {
    //     // Virtual-hosted style: {bucket}.{endpoint}/{key}
    //     let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
    //     let authorizer = Public::new(address, TEST_SUBJECT).unwrap();

    //     // "my-key" is safe ASCII, so it stays as-is (not encoded)
    //     let url = authorizer.build_url("my-key").unwrap();
    //     assert_eq!(url.as_str(), "https://my-bucket.s3.amazonaws.com/my-key");
    // }

    // #[dialog_common::test]
    // fn it_builds_path_style_url() {
    //     // Path-style: {endpoint}/{bucket}/{key}
    //     let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
    //     let authorizer = Public::new(address, TEST_SUBJECT).unwrap();
    //     // localhost defaults to path_style=true

    //     let url = authorizer.build_url("my-key").unwrap();
    //     assert_eq!(url.as_str(), "http://localhost:9000/bucket/my-key");
    // }

    // #[dialog_common::test]
    // fn it_forces_path_style() {
    //     // Force path-style on a non-localhost endpoint
    //     let address = Address::new("https://custom-s3.example.com", "us-east-1", "bucket");
    //     let authorizer = Public::new(address, TEST_SUBJECT)
    //         .unwrap()
    //         .with_path_style(true);

    //     let url = authorizer.build_url("key").unwrap();
    //     assert_eq!(url.as_str(), "https://custom-s3.example.com/bucket/key");
    // }

    // #[dialog_common::test]
    // fn it_forces_virtual_hosted_on_localhost() {
    //     // Force virtual-hosted on localhost (not typical, but supported)
    //     let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
    //     let authorizer = Public::new(address, TEST_SUBJECT)
    //         .unwrap()
    //         .with_path_style(false);

    //     let url = authorizer.build_url("key").unwrap();
    //     assert_eq!(url.as_str(), "http://bucket.localhost:9000/key");
    // }

    // #[dialog_common::test]
    // fn it_builds_r2_url() {
    //     // R2 uses virtual-hosted style by default (non-localhost)
    //     let address = Address::new("https://abc123.r2.cloudflarestorage.com", "auto", "bucket");
    //     let authorizer = Public::new(address, TEST_SUBJECT).unwrap();

    //     let url = authorizer.build_url("my-key").unwrap();
    //     assert_eq!(
    //         url.as_str(),
    //         "https://bucket.abc123.r2.cloudflarestorage.com/my-key"
    //     );
    // }

    // #[dialog_common::test]
    // fn it_nests_at_calls() {
    //     let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(test_credentials())
    //         .unwrap()
    //         .at("data")
    //         .at("v1");

    //     let path = backend.encode_path(b"key");
    //     assert_eq!(path, "data/v1/key");
    // }

    #[dialog_common::test]
    fn it_creates_address() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );

        assert_eq!(address.endpoint(), "https://s3.us-east-1.amazonaws.com");
        assert_eq!(address.region(), "us-east-1");
        assert_eq!(address.bucket(), "my-bucket");
    }

    // #[dialog_common::test]
    // fn it_configures_bucket_with_hasher() {
    //     let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(test_credentials())
    //         .unwrap()
    //         .with_hasher(Hasher::Sha256);

    //     // Hasher should be set (we can't directly inspect it, but the backend should work)
    //     let path = backend.encode_path(b"key");
    //     assert_eq!(path, "key");
    // }

    #[dialog_common::test]
    fn it_converts_errors_to_dialog_error() {
        let error = S3StorageError::TransportError("test".into());
        let dialog_error: DialogStorageError = error.into();
        assert!(dialog_error.to_string().contains("test"));
    }

    // #[dialog_common::test]
    // async fn it_sets_and_gets_values(env: PublicS3Address) -> anyhow::Result<()> {
    //     // Using public access for simplicity. Signed sessions are tested separately.
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("test");

    //     // Test data
    //     let key = b"test-key-1".to_vec();
    //     let value = b"test-value-1".to_vec();

    //     // Set the value
    //     backend.set(key.clone(), value.clone()).await?;

    //     // Get the value back
    //     let retrieved = backend.get(&key).await?;
    //     assert_eq!(retrieved, Some(value));

    //     Ok(())
    // }

    // #[dialog_common::test]
    // async fn it_performs_multiple_operations(env: PublicS3Address) -> anyhow::Result<()> {
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

    //     // Set multiple values
    //     backend.set(b"key1".to_vec(), b"value1".to_vec()).await?;
    //     backend.set(b"key2".to_vec(), b"value2".to_vec()).await?;
    //     backend.set(b"key3".to_vec(), b"value3".to_vec()).await?;

    //     // Verify all values
    //     assert_eq!(
    //         backend.get(&b"key1".to_vec()).await?,
    //         Some(b"value1".to_vec())
    //     );
    //     assert_eq!(
    //         backend.get(&b"key2".to_vec()).await?,
    //         Some(b"value2".to_vec())
    //     );
    //     assert_eq!(
    //         backend.get(&b"key3".to_vec()).await?,
    //         Some(b"value3".to_vec())
    //     );

    //     // Test missing key
    //     assert_eq!(backend.get(&b"nonexistent".to_vec()).await?, None);

    //     Ok(())
    // }

    // #[dialog_common::test]
    // async fn it_handles_large_values(env: PublicS3Address) -> anyhow::Result<()> {
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

    //     // Create a 100KB value
    //     let key = b"large-key".to_vec();
    //     let value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

    //     // Set and retrieve
    //     backend.set(key.clone(), value.clone()).await?;
    //     let retrieved = backend.get(&key).await?;
    //     assert_eq!(retrieved, Some(value));

    //     Ok(())
    // }

    // #[dialog_common::test]
    // async fn it_deletes_values(env: PublicS3Address) -> anyhow::Result<()> {
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

    //     let key = b"delete-test-key".to_vec();
    //     let value = b"delete-test-value".to_vec();

    //     // Set the value
    //     backend.set(key.clone(), value.clone()).await?;

    //     // Verify it exists
    //     assert_eq!(backend.get(&key).await?, Some(value));

    //     // Delete it
    //     backend.delete(&key).await?;

    //     // Verify it's gone
    //     assert_eq!(backend.get(&key).await?, None);

    //     // Delete non-existent key should still succeed (S3 behavior)
    //     backend.delete(&key).await?;

    //     Ok(())
    // }

    // #[cfg(feature = "s3-list")]
    // #[dialog_common::test]
    // async fn it_lists_objects(env: PublicS3Address) -> anyhow::Result<()> {
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("list-test");

    //     // Set multiple values
    //     backend.set(b"key1".to_vec(), b"value1".to_vec()).await?;
    //     backend.set(b"key2".to_vec(), b"value2".to_vec()).await?;
    //     backend.set(b"key3".to_vec(), b"value3".to_vec()).await?;

    //     // List objects
    //     let result = backend.list(None).await?;

    //     assert_eq!(result.keys.len(), 3);
    //     assert!(!result.is_truncated);

    //     // All keys should have the prefix
    //     for key in &result.keys {
    //         assert!(key.starts_with("list-test/"));
    //     }

    //     Ok(())
    // }

    // #[cfg(feature = "s3-list")]
    // #[dialog_common::test]
    // async fn it_lists_empty_for_nonexistent_prefix(env: PublicS3Address) -> anyhow::Result<()> {
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?
    //         .at("nonexistent-prefix-that-does-not-exist");

    //     // List objects with a prefix that has no objects - should return empty list
    //     let result = backend.list(None).await?;

    //     assert!(result.keys.is_empty());
    //     assert!(!result.is_truncated);
    //     assert!(result.next_continuation_token.is_none());

    //     Ok(())
    // }

    // #[cfg(feature = "s3-list")]
    // #[dialog_common::test]
    // async fn it_errors_on_nonexistent_bucket(env: PublicS3Address) -> anyhow::Result<()> {
    //     let address = Address::new(&env.endpoint, "us-east-1", "bucket-that-does-not-exist");
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

    //     // S3 returns 404 NoSuchBucket error when listing a non-existent bucket.
    //     // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_Errors
    //     let result = backend.list(None).await;

    //     assert!(result.is_err());
    //     let err = result.unwrap_err();
    //     assert!(
    //         matches!(err, S3StorageError::ServiceError(ref msg) if msg.contains("NoSuchBucket")),
    //         "Expected NoSuchBucket error for non-existent bucket, got: {:?}",
    //         err
    //     );

    //     Ok(())
    // }

    // #[cfg(feature = "s3-list")]
    // #[dialog_common::test]
    // async fn it_reads_stream(env: PublicS3Address) -> anyhow::Result<()> {
    //     use futures_util::TryStreamExt;

    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("stream-test");

    //     // Set multiple values
    //     backend.set(b"a".to_vec(), b"value-a".to_vec()).await?;
    //     backend.set(b"b".to_vec(), b"value-b".to_vec()).await?;

    //     // Read all items via StorageSource
    //     let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    //     let mut stream = Box::pin(backend.read());

    //     while let Some((key, value)) = stream.try_next().await? {
    //         items.push((key, value));
    //     }

    //     assert_eq!(items.len(), 2);

    //     // Verify the items (order may vary)
    //     let keys: Vec<&[u8]> = items.iter().map(|(k, _)| k.as_slice()).collect();
    //     assert!(keys.contains(&b"a".as_slice()));
    //     assert!(keys.contains(&b"b".as_slice()));

    //     Ok(())
    // }

    // #[dialog_common::test]
    // async fn it_returns_none_for_missing_values(env: PublicS3Address) -> anyhow::Result<()> {
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

    //     // Try to get a key that doesn't exist
    //     let key = b"nonexistent-key".to_vec();
    //     let retrieved = backend.get(&key).await?;

    //     assert_eq!(retrieved, None);

    //     Ok(())
    // }

    // #[dialog_common::test]
    // async fn it_performs_bulk_writes(env: PublicS3Address) -> anyhow::Result<()> {
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("bulk-test");

    //     // Create a source stream with multiple items
    //     use async_stream::try_stream;

    //     let source_stream = try_stream! {
    //         yield (vec![1, 2, 3], vec![4, 5, 6]);
    //         yield (vec![4, 5, 6, 7], vec![8, 9, 10]);
    //         yield (vec![7, 8, 9], vec![10, 11, 12]);
    //     };

    //     // Perform the bulk write
    //     backend.write(source_stream).await?;

    //     // Verify all items were written
    //     assert_eq!(backend.get(&vec![1, 2, 3]).await?, Some(vec![4, 5, 6]));
    //     assert_eq!(backend.get(&vec![4, 5, 6, 7]).await?, Some(vec![8, 9, 10]));
    //     assert_eq!(backend.get(&vec![7, 8, 9]).await?, Some(vec![10, 11, 12]));

    //     Ok(())
    // }

    // #[dialog_common::test]
    // async fn it_integrates_with_memory_backend(env: PublicS3Address) -> anyhow::Result<()> {
    //     use crate::StorageSource;
    //     use futures_util::StreamExt;

    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let mut s3_backend =
    //         Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("memory-integration");

    //     // Create a memory backend with some data
    //     let mut memory_backend = crate::MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();

    //     // Add some data to the memory backend
    //     memory_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
    //     memory_backend.set(vec![4, 5, 6, 7], vec![8, 9, 10]).await?;

    //     // Transfer data from memory backend to S3 backend using drain()
    //     // Map DialogStorageError to S3StorageError for type compatibility
    //     let source_stream = memory_backend
    //         .drain()
    //         .map(|result| result.map_err(|e| S3StorageError::ServiceError(e.to_string())));
    //     s3_backend.write(source_stream).await?;

    //     // Verify all items were transferred to S3
    //     assert_eq!(s3_backend.get(&vec![1, 2, 3]).await?, Some(vec![4, 5, 6]));
    //     assert_eq!(
    //         s3_backend.get(&vec![4, 5, 6, 7]).await?,
    //         Some(vec![8, 9, 10])
    //     );

    //     Ok(())
    // }

    // #[dialog_common::test]
    // async fn it_uses_prefix(env: PublicS3Address) -> anyhow::Result<()> {
    //     // Create two backends with different prefixes
    //     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
    //     let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
    //     let bucket = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;
    //     let mut backend1 = bucket.clone().at("prefix-a");
    //     let mut backend2 = bucket.at("prefix-b");

    //     // Set the same key in both backends
    //     let key = b"shared-key".to_vec();
    //     backend1.set(key.clone(), b"value-a".to_vec()).await?;
    //     backend2.set(key.clone(), b"value-b".to_vec()).await?;

    //     // Each backend should see its own value
    //     assert_eq!(backend1.get(&key).await?, Some(b"value-a".to_vec()));
    //     assert_eq!(backend2.get(&key).await?, Some(b"value-b".to_vec()));

    //     Ok(())
    // }

    // NOTE: All tests below use the legacy Bucket + Authorizer API
    // They are gated behind a never-enabled feature to prevent compilation errors
    // TODO: Update these tests to use S3Bucket + Provider API
    #[cfg(feature = "legacy-bucket-tests")]
    mod legacy_bucket_tests {
        use super::*;

        #[cfg(feature = "s3-list")]
        #[dialog_common::test]
        async fn it_uses_prefix_for_listing(env: PublicS3Address) -> anyhow::Result<()> {
            // Create two backends with different prefixes
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
            let bucket = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;
            let mut backend1 = bucket.clone().at("prefix-a");
            let mut backend2 = bucket.at("prefix-b");

            // Set the same key in both backends
            let key = b"shared-key".to_vec();
            backend1.set(key.clone(), b"value-a".to_vec()).await?;
            backend2.set(key.clone(), b"value-b".to_vec()).await?;

            // Listing should only return keys from respective prefix
            let list1 = backend1.list(None).await?;
            let list2 = backend2.list(None).await?;

            assert!(list1.keys.iter().all(|k| k.starts_with("prefix-a/")));
            assert!(list2.keys.iter().all(|k| k.starts_with("prefix-b/")));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_overwrites_value(env: PublicS3Address) -> anyhow::Result<()> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
            let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        let key = b"overwrite-key".to_vec();

        // Set initial value
        backend.set(key.clone(), b"initial".to_vec()).await?;
        assert_eq!(backend.get(&key).await?, Some(b"initial".to_vec()));

        // Overwrite with new value
        backend.set(key.clone(), b"updated".to_vec()).await?;
        assert_eq!(backend.get(&key).await?, Some(b"updated".to_vec()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_binary_keys(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        // Binary key with non-UTF8 bytes
        let key = vec![0x00, 0xFF, 0x80, 0x7F];
        let value = b"binary-key-value".to_vec();

        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_path_like_keys(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        // Path-like key with slashes
        let key = b"path/to/nested/key".to_vec();
        let value = b"nested-value".to_vec();

        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_encoded_key_segments(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        // Test key with mixed safe and unsafe segments
        // "safe-segment/user@example.com" - first segment is safe, second has @ which is unsafe
        let key_mixed = b"safe-segment/user@example.com".to_vec();
        let value_mixed = b"value-for-mixed-key".to_vec();

        // Verify encoding behavior
        let encoded = encode_s3_key(&key_mixed);
        // Should be "safe-segment/!<base58>" where first part is unchanged and second is encoded
        assert!(
            encoded.starts_with("safe-segment/!"),
            "First segment should be safe, second should be encoded with ! prefix: {}",
            encoded
        );

        // Write and read back
        backend.set(key_mixed.clone(), value_mixed.clone()).await?;
        let retrieved = backend.get(&key_mixed).await?;
        assert_eq!(retrieved, Some(value_mixed));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_multi_segment_mixed_encoding(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        // Test key with multiple segments: safe/unsafe/safe/unsafe pattern
        // "data/file name with spaces/v1/special!chars"
        let key = b"data/file name with spaces/v1/special!chars".to_vec();
        let value = b"value-for-complex-path".to_vec();

        // Verify encoding behavior
        let encoded = encode_s3_key(&key);
        let segments: Vec<&str> = encoded.split('/').collect();
        assert_eq!(segments.len(), 4, "Should have 4 segments");
        assert_eq!(segments[0], "data", "First segment should be safe");
        assert!(
            segments[1].starts_with('!'),
            "Second segment should be encoded (has spaces)"
        );
        assert_eq!(segments[2], "v1", "Third segment should be safe");
        assert!(
            segments[3].starts_with('!'),
            "Fourth segment should be encoded (has !)"
        );

        // Write and read back
        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[dialog_common::test]
    fn it_roundtrips_key_encoding() {
        // Test that encode and decode are inverse operations for valid UTF-8 keys
        // Note: Keys with invalid UTF-8 bytes (like 0xFF, 0x80) will be lossy
        // because encode_s3_key uses String::from_utf8_lossy internally.
        // For pure binary keys, the roundtrip still works via base58 encoding,
        // but the bytes get normalized through UTF-8 replacement characters.
        let test_keys: Vec<Vec<u8>> = vec![
            b"simple-key".to_vec(),
            b"path/to/key".to_vec(),
            b"key with spaces".to_vec(),
            b"key@with!special#chars".to_vec(),
            b"safe/unsafe@mixed/safe2".to_vec(),
        ];

        for key in test_keys {
            let encoded = encode_s3_key(&key);
            let decoded = decode_s3_key(&encoded).unwrap();
            assert_eq!(
                decoded, key,
                "Roundtrip failed for key: {:?}, encoded as: {}",
                key, encoded
            );
        }
    }

    #[dialog_common::test]
    async fn it_works_with_signed_session(env: S3Address) -> anyhow::Result<()> {
        // Create credentials
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let credentials = Credentials::private(
            address,
            "did:key:test",
            &env.access_key_id,
            &env.secret_access_key,
        )?
        .with_path_style(true);

        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?.at("signed-test");

        // Test data
        let key = b"signed-key".to_vec();
        let value = b"signed-value".to_vec();

        // Set the value (uses PUT with presigned URL)
        backend.set(key.clone(), value.clone()).await?;

        // Get the value back (uses GET with presigned URL)
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_with_wrong_secret_key(env: S3Address) -> anyhow::Result<()> {
        // Create credentials with WRONG secret key
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let credentials =
            Credentials::private(address, "did:key:test", &env.access_key_id, "wrong-secret")?
                .with_path_style(true);

        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?;

        // Attempt to set a value - should fail due to signature mismatch
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure with wrong secret key"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_with_wrong_access_key(env: S3Address) -> anyhow::Result<()> {
        // Create credentials with WRONG access key
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let credentials = Credentials::private(
            address,
            "did:key:test",
            "wrong-access-key",
            &env.secret_access_key,
        )?
        .with_path_style(true);

        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?;

        // Attempt to set a value - should fail due to unknown access key
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure with wrong access key"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_unsigned_request_to_auth_server(env: S3Address) -> anyhow::Result<()> {
        // Client uses no credentials but server requires authentication
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let public = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(public)?;

        // Attempt to set a value - should fail because server expects signed requests
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure when sending unsigned request to authenticated server"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_fails_get_with_wrong_credentials(env: S3Address) -> anyhow::Result<()> {
        // First, set a value with correct credentials
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let correct_credentials = Credentials::private(
            address,
            "did:key:test",
            &env.access_key_id,
            &env.secret_access_key,
        )?
        .with_path_style(true);
        let mut correct_backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(correct_credentials)?;

        correct_backend
            .set(b"protected-key".to_vec(), b"secret-value".to_vec())
            .await?;

        // Now try to GET with wrong credentials
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let wrong_credentials =
            Credentials::private(address, "did:key:test", &env.access_key_id, "wrong-secret")?
                .with_path_style(true);
        let wrong_backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(wrong_credentials)?;

        // Attempt to get the value - should fail
        let result = wrong_backend.get(&b"protected-key".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure when getting with wrong credentials"
        );

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[dialog_common::test]
    async fn it_lists_with_signed_session(env: S3Address) -> anyhow::Result<()> {
        // Create credentials
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let credentials = Credentials::private(
            address,
            "did:key:test",
            &env.access_key_id,
            &env.secret_access_key,
        )?
        .with_path_style(true);

        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?.at("signed-list-test");

        // Set multiple values
        backend.set(b"key1".to_vec(), b"value1".to_vec()).await?;
        backend.set(b"key2".to_vec(), b"value2".to_vec()).await?;

        // List objects with signed request
        let result = backend.list(None).await?;

        assert_eq!(result.keys.len(), 2);
        assert!(!result.is_truncated);

        // All keys should have the prefix
        for key in &result.keys {
            assert!(
                key.starts_with("signed-list-test/"),
                "Key {} should start with prefix",
                key
            );
        }

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[dialog_common::test]
    async fn it_reads_stream_with_signed_session(env: S3Address) -> anyhow::Result<()> {
        use futures_util::TryStreamExt;

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let credentials = Credentials::private(
            address,
            "did:key:test",
            &env.access_key_id,
            &env.secret_access_key,
        )?
        .with_path_style(true);

        let mut backend =
            Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?.at("signed-stream-test");

        // Set multiple values
        backend.set(b"a".to_vec(), b"value-a".to_vec()).await?;
        backend.set(b"b".to_vec(), b"value-b".to_vec()).await?;

        // Read all items via StorageSource (uses list internally)
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut stream = Box::pin(backend.read());

        while let Some((key, value)) = stream.try_next().await? {
            items.push((key, value));
        }

        assert_eq!(items.len(), 2);

        // Verify the items (order may vary)
        let keys: Vec<&[u8]> = items.iter().map(|(k, _)| k.as_slice()).collect();
        assert!(keys.contains(&b"a".as_slice()));
        assert!(keys.contains(&b"b".as_slice()));

        Ok(())
    }
    } // end of legacy_bucket_tests module

    // NOTE: These tests are disabled because they use the old Session pattern
    // that directly calls .perform() on authorization. The new pattern uses
    // S3Bucket<Issuer> which wraps the credentials and issuer together.
    // See ucan_provider_tests for the new approach.
    #[cfg(feature = "_disabled_ucan_tests")]
    mod ucan_tests {
        use super::*;
        use async_trait::async_trait;
        use dialog_common::capability::{Ability, Access, Claim, Did, Principal, Subject};
        use dialog_common::{Authority, ConditionalSend};
        use dialog_s3_credentials::capability::{storage, AccessError};
        use dialog_s3_credentials::ucan::{Credentials as UcanCredentials, DelegationChain, UcanAuthorization};
        use ed25519_dalek::ed25519::signature::SignerMut;
        use ucan::delegation::builder::DelegationBuilder;
        use ucan::delegation::subject::DelegatedSubject;
        use ucan::did::{Ed25519Did, Ed25519Signer};

        /// Session combines UCAN credentials with a signer for creating invocations.
        struct Session {
            credentials: UcanCredentials,
            signer: ed25519_dalek::SigningKey,
            did: Did,
        }

        impl Session {
            fn new(credentials: UcanCredentials, secret: &[u8; 32]) -> Self {
                let signer = ed25519_dalek::SigningKey::from_bytes(secret);
                Session {
                    did: Ed25519Signer::from(signer.clone()).did().to_string(),
                    signer,
                    credentials,
                }
            }
        }

        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

        impl Authority for Session {
            fn sign(&mut self, payload: &[u8]) -> Vec<u8> {
                self.signer.sign(payload).to_vec()
            }
            fn secret_key_bytes(&self) -> Option<[u8; 32]> {
                self.signer.to_bytes().into()
            }
        }

        /// Create a delegation chain from subject to operator.
        fn create_delegation_chain(
            subject_signer: &Ed25519Signer,
            operator_did: &Ed25519Did,
            commands: &[&str],
        ) -> DelegationChain {
            let subject_did = subject_signer.did().clone();
            let delegation = DelegationBuilder::new()
                .issuer(subject_signer.clone())
                .audience(operator_did.clone())
                .subject(DelegatedSubject::Specific(subject_did))
                .command(commands.iter().map(|s| s.to_string()).collect())
                .try_build()
                .expect("Failed to build delegation");
            DelegationChain::new(delegation)
        }

        /// Helper to create a UCAN session for testing.
        fn create_ucan_session(access_service_url: &str, seed: &[u8; 32]) -> Session {
            let signer = ed25519_dalek::SigningKey::from_bytes(seed);
            let operator = Ed25519Signer::from(signer.clone());

            // Subject delegates storage/* to operator (same key for simplicity)
            let delegation = create_delegation_chain(&operator, operator.did(), &["storage"]);
            let credentials = UcanCredentials::new(access_service_url.to_string(), delegation);

            Session::new(credentials, seed)
        }

        #[dialog_common::test]
        async fn it_performs_storage_get_and_set_via_ucan(
            env: UcanS3Address,
        ) -> anyhow::Result<()> {
            let mut session = create_ucan_session(&env.access_service_url, &[42u8; 32]);
            let client = reqwest::Client::new();

            let store_name = "test-store";
            let key = b"ucan-test-key".to_vec();
            let value = b"ucan-test-value".to_vec();

            // 1. Set the value using UCAN authorization
            let checksum = Hasher::Sha256.checksum(&value);

            let set_capability = Subject::from(session.did().to_string())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name.to_string()))
                .invoke(storage::Set::new(key.clone(), checksum));

            let authorized = set_capability.acquire(&mut session).await?;
            let request = authorized
                .perform(&mut session)
                .await
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;

            // Execute the presigned PUT request
            let response = request
                .into_request(&client)
                .body(value.clone())
                .send()
                .await?;
            assert!(
                response.status().is_success(),
                "PUT failed: {}",
                response.status()
            );

            // 2. Get the value back using UCAN authorization
            let get_capability = Subject::from(session.did().to_string())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name.to_string()))
                .invoke(storage::Get::new(key.clone()));

            let authorized = get_capability.acquire(&mut session).await?;
            let request = authorized
                .perform(&mut session)
                .await
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;

            // Execute the presigned GET request
            let response = request.into_request(&client).send().await?;
            assert!(
                response.status().is_success(),
                "GET failed: {}",
                response.status()
            );

            let retrieved = response.bytes().await?;
            assert_eq!(retrieved.as_ref(), value.as_slice());

            Ok(())
        }

        #[dialog_common::test]
        async fn it_performs_storage_delete_via_ucan(env: UcanS3Address) -> anyhow::Result<()> {
            let mut session = create_ucan_session(&env.access_service_url, &[43u8; 32]);
            let client = reqwest::Client::new();

            let store_name = "test-store";
            let key = b"ucan-delete-key".to_vec();
            let value = b"value-to-delete".to_vec();

            // 1. First set a value
            let checksum = Hasher::Sha256.checksum(&value);

            let set_capability = Subject::from(session.did().to_string())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name.to_string()))
                .invoke(storage::Set::new(key.clone(), checksum));

            let authorized = set_capability.acquire(&mut session).await?;
            let request = authorized
                .perform(&mut session)
                .await
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
            let response = request
                .into_request(&client)
                .body(value.clone())
                .send()
                .await?;
            assert!(response.status().is_success());

            // 2. Delete the value
            let delete_capability = Subject::from(session.did().to_string())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name.to_string()))
                .invoke(storage::Delete::new(key.clone()));

            let authorized = delete_capability.acquire(&mut session).await?;
            let request = authorized
                .perform(&mut session)
                .await
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
            let response = request.into_request(&client).send().await?;
            assert!(
                response.status().is_success(),
                "DELETE failed: {}",
                response.status()
            );

            // 3. Verify the value is gone
            let get_capability = Subject::from(session.did().to_string())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name.to_string()))
                .invoke(storage::Get::new(key.clone()));

            let authorized = get_capability.acquire(&mut session).await?;
            let request = authorized
                .perform(&mut session)
                .await
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
            let response = request.into_request(&client).send().await?;

            // Should return 404 Not Found
            assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_returns_none_for_nonexistent_key_via_ucan(
            env: UcanS3Address,
        ) -> anyhow::Result<()> {
            let mut session = create_ucan_session(&env.access_service_url, &[44u8; 32]);
            let client = reqwest::Client::new();

            let store_name = "test-store";
            let key = b"nonexistent-ucan-key".to_vec();

            let get_capability = Subject::from(session.did().to_string())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name.to_string()))
                .invoke(storage::Get::new(key.clone()));

            let authorized = get_capability.acquire(&mut session).await?;
            let request = authorized
                .perform(&mut session)
                .await
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
            let response = request.into_request(&client).send().await?;

            // Should return 404 Not Found for nonexistent key
            assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

            Ok(())
        }
    }
}
