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
//! struct Issuer(Did);
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
//! let issuer = Issuer(Did::from("did:key:zMyIssuer"));
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
//! # struct Issuer(Did);
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
//! let issuer = Issuer(Did::from("did:key:zMyIssuer"));
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
//! # struct Issuer(Did);
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
//! let issuer = Issuer(Did::from("did:key:zMyIssuer"));
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
//! # struct Issuer(Did);
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
//! let issuer = Issuer(Did::from("did:key:zMyIssuer"));
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
    capability::{
        Ability, Access, Authorized, Capability, Claim, Did, Effect, Principal, Provider, Subject,
    },
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

use crate::{DialogStorageError, StorageBackend, TransactionalMemoryBackend};

// Testing helpers module:
// - Address types (S3Address, PublicS3Address, UcanS3Address) are available on all platforms
// - Server implementation is native-only (internal to the helpers module)
#[cfg(any(feature = "helpers", test))]
pub mod helpers;
#[cfg(all(feature = "helpers", feature = "ucan", target_arch = "wasm32"))]
pub use helpers::Operator;
#[cfg(all(feature = "helpers", not(target_arch = "wasm32")))]
pub use helpers::{LocalS3, PublicS3Settings, S3Settings};
#[cfg(all(feature = "helpers", feature = "ucan", not(target_arch = "wasm32")))]
pub use helpers::{Operator, UcanAccessServer, UcanS3Settings};
#[cfg(any(feature = "helpers", test))]
pub use helpers::{PublicS3Address, S3Address, Session, UcanS3Address};

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

// Note: ArchiveProvider trait was removed as it was unused. If needed in the future,
// define a trait alias for Provider<archive::AuthorizeGet> + Provider<archive::AuthorizePut>.

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
        Self {
            credentials,
            issuer,
        }
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
        let when = publish
            .when
            .as_ref()
            .map(|b| String::from_utf8_lossy(b).to_string());
        let checksum = Hasher::Sha256.checksum(&content);

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(memory::Memory)
            .attenuate(space.clone())
            .attenuate(cell.clone())
            .invoke(memory::AuthorizePublish {
                checksum,
                when: when.clone(),
            });

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

#[cfg(feature = "s3-list")]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<storage::List> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(
        &mut self,
        input: Capability<storage::List>,
    ) -> Result<storage::ListResult, storage::StorageError> {
        // Build the authorization capability
        let store: &storage::Store = input.policy();
        let list: &storage::List = input.policy();
        let capability = Subject::from(input.subject().to_string())
            .attenuate(storage::Storage)
            .attenuate(store.clone())
            .invoke(storage::AuthorizeList::new(list.continuation_token.clone()));

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
            let body = response
                .text()
                .await
                .map_err(|e| storage::StorageError::Storage(e.to_string()))?;

            // Parse the XML response
            list::parse_list_response(&body)
                .map(|result| storage::ListResult {
                    keys: result.keys,
                    is_truncated: result.is_truncated,
                    next_continuation_token: result.next_continuation_token,
                })
                .map_err(|e| storage::StorageError::Storage(e.to_string()))
        } else {
            Err(storage::StorageError::Storage(format!(
                "Failed to list objects: {}",
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
/// struct Issuer(Did);
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
/// let issuer = Issuer(Did::from("did:key:zMyIssuer"));
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

impl<Issuer> Bucket<Issuer>
where
    Issuer: Authority + Clone + ConditionalSend + ConditionalSync,
{
    /// Delete a value by key.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use dialog_storage::s3::{S3, S3Credentials, Address, Bucket};
    /// # use dialog_storage::StorageBackend;
    /// # use dialog_common::{Authority, capability::{Did, Principal}};
    /// #
    /// # #[derive(Clone)]
    /// # struct Issuer(Did);
    /// # impl Principal for Issuer { fn did(&self) -> &Did { &self.0 } }
    /// # impl Authority for Issuer {
    /// #     fn sign(&mut self, _: &[u8]) -> Vec<u8> { Vec::new() }
    /// #     fn secret_key_bytes(&self) -> Option<[u8; 32]> { None }
    /// # }
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
    /// # let credentials = S3Credentials::public(address)?;
    /// # let issuer = Issuer(Did::from("did:key:zMyIssuer"));
    /// # let s3 = S3::from_s3(credentials, issuer);
    /// # let mut bucket = Bucket::new(s3, "did:key:zSubject", "store");
    /// // First set a value
    /// bucket.set(b"key".to_vec(), b"value".to_vec()).await?;
    ///
    /// // Then delete it
    /// bucket.delete(b"key").await?;
    ///
    /// // Verify it's gone
    /// assert_eq!(bucket.get(&b"key".to_vec()).await?, None);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), S3StorageError> {
        let capability: Capability<storage::Delete> = Subject::from(self.subject.clone())
            .attenuate(storage::Storage)
            .attenuate(storage::Store::new(&self.path))
            .invoke(storage::Delete {
                key: key.to_vec().into(),
            });

        Provider::<storage::Delete>::execute(&mut self.bucket, capability)
            .await
            .map_err(|e| S3StorageError::ServiceError(e.to_string()))
    }
}

// Forward Principal trait to the underlying bucket
impl<Issuer: Principal> Principal for Bucket<Issuer> {
    fn did(&self) -> &Did {
        self.bucket.did()
    }
}

// Forward Authority trait to the underlying bucket
impl<Issuer: Authority> Authority for Bucket<Issuer> {
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
impl<Issuer, Do> Provider<Authorized<Do, dialog_s3_credentials::Authorization>> for Bucket<Issuer>
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

        Ok(result.map(|pub_| {
            (
                pub_.content.to_vec(),
                String::from_utf8_lossy(&pub_.edition).to_string(),
            )
        }))
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

                let new_edition =
                    Provider::<memory::Publish>::execute(&mut self.bucket, capability)
                        .await
                        .map_err(|e| match e {
                            memory::MemoryError::EditionMismatch { .. } => {
                                S3StorageError::EditionMismatch {
                                    expected: edition.map(|e| e.to_string()),
                                    actual: None,
                                }
                            }
                            e => S3StorageError::ServiceError(e.to_string()),
                        })?;

                Ok(Some(String::from_utf8_lossy(&new_edition).to_string()))
            }
            None => {
                // Retract (delete)
                let when = edition.ok_or_else(|| {
                    S3StorageError::ServiceError("Edition required for delete".into())
                })?;

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
                        memory::MemoryError::EditionMismatch { .. } => {
                            S3StorageError::EditionMismatch {
                                expected: edition.map(|e| e.to_string()),
                                actual: None,
                            }
                        }
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

    #[allow(dead_code)]
    fn test_address() -> Address {
        Address::new("https://s3.amazonaws.com", "us-east-1", "bucket")
    }

    #[allow(dead_code)]
    fn test_credentials() -> S3Credentials {
        S3Credentials::public(test_address()).unwrap()
    }

    mod s3bucket_provider_tests {
        use super::*;

        #[allow(dead_code)]
        fn create_test_bucket(env: &helpers::PublicS3Address) -> S3<helpers::Session> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
            S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT))
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

    #[cfg(feature = "ucan")]
    mod ucan_provider_tests {
        use super::*;
        #[allow(unused_imports)]
        use dialog_common::capability::Subject;
        use dialog_s3_credentials::ucan::{
            Credentials as UcanCredentials, DelegationChain, test_helpers::create_delegation,
        };
        use helpers::Operator;

        /// Helper to create a test delegation chain from subject to operator.
        #[allow(dead_code)]
        fn create_test_delegation_chain(
            subject_signer: &ucan::did::Ed25519Signer,
            operator_did: &ucan::did::Ed25519Did,
            can: &[&str],
        ) -> DelegationChain {
            let subject_did = subject_signer.did().clone();
            let delegation = create_delegation(subject_signer, operator_did, &subject_did, can)
                .expect("Failed to create test delegation");
            DelegationChain::new(delegation)
        }

        #[allow(dead_code)]
        fn create_ucan_bucket(
            env: &helpers::UcanS3Address,
            operator: Operator,
            delegation: DelegationChain,
        ) -> S3<Operator> {
            let ucan_credentials = UcanCredentials::new(env.access_service_url.clone(), delegation);
            S3::new(Credentials::Ucan(ucan_credentials), operator)
        }

        #[dialog_common::test]
        async fn it_performs_archive_get_and_put_with_ucan(
            env: helpers::UcanS3Address,
        ) -> anyhow::Result<()> {
            use dialog_common::capability::Principal;
            // Create operator
            let operator = Operator::generate();

            // Create delegation chain: subject delegates to operator
            // For this test, subject and operator are the same
            let delegation = create_test_delegation_chain(
                operator.signer(),
                &operator.signer().did(),
                &["archive"],
            );

            let subject_did = operator.did().to_string();

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

    #[dialog_common::test]
    fn it_converts_errors_to_dialog_error() {
        let error = S3StorageError::TransportError("test".into());
        let dialog_error: DialogStorageError = error.into();
        assert!(dialog_error.to_string().contains("test"));
    }

    /// Tests for `Bucket<Issuer>` which implements `StorageBackend`.
    mod bucket_storage_backend_tests {
        use super::*;

        #[allow(dead_code)]
        fn create_test_bucket(env: &helpers::PublicS3Address) -> Bucket<helpers::Session> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));
            Bucket::new(s3, TEST_SUBJECT, "test-store")
        }

        #[dialog_common::test]
        async fn it_sets_and_gets_value(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            let key = b"storage-key".to_vec();
            let value = b"storage-value".to_vec();

            bucket.set(key.clone(), value.clone()).await?;
            let retrieved = bucket.get(&key).await?;

            assert_eq!(retrieved, Some(value));
            Ok(())
        }

        #[dialog_common::test]
        async fn it_overwrites_value(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            let key = b"overwrite-key".to_vec();

            bucket.set(key.clone(), b"initial".to_vec()).await?;
            assert_eq!(bucket.get(&key).await?, Some(b"initial".to_vec()));

            bucket.set(key.clone(), b"updated".to_vec()).await?;
            assert_eq!(bucket.get(&key).await?, Some(b"updated".to_vec()));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_returns_none_for_nonexistent_key(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            let bucket = create_test_bucket(&env);

            let result = bucket.get(&b"nonexistent-key".to_vec()).await?;
            assert_eq!(result, None);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_handles_binary_keys(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            // Binary key with non-UTF8 bytes
            let key = vec![0x00, 0xFF, 0x80, 0x7F];
            let value = b"binary-key-value".to_vec();

            bucket.set(key.clone(), value.clone()).await?;
            let retrieved = bucket.get(&key).await?;

            assert_eq!(retrieved, Some(value));
            Ok(())
        }

        #[dialog_common::test]
        async fn it_handles_path_like_keys(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            // Path-like key with slashes
            let key = b"path/to/nested/key".to_vec();
            let value = b"nested-value".to_vec();

            bucket.set(key.clone(), value.clone()).await?;
            let retrieved = bucket.get(&key).await?;

            assert_eq!(retrieved, Some(value));
            Ok(())
        }

        #[dialog_common::test]
        async fn it_scopes_to_path(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let bucket = create_test_bucket(&env);

            // Create nested bucket
            let mut nested = bucket.at("nested");

            let key = b"nested-key".to_vec();
            let value = b"nested-value".to_vec();

            nested.set(key.clone(), value.clone()).await?;
            let retrieved = nested.get(&key).await?;

            assert_eq!(retrieved, Some(value));
            Ok(())
        }

        #[dialog_common::test]
        async fn it_performs_multiple_operations(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            // Set multiple values
            bucket.set(b"key1".to_vec(), b"value1".to_vec()).await?;
            bucket.set(b"key2".to_vec(), b"value2".to_vec()).await?;
            bucket.set(b"key3".to_vec(), b"value3".to_vec()).await?;

            // Verify all values
            assert_eq!(
                bucket.get(&b"key1".to_vec()).await?,
                Some(b"value1".to_vec())
            );
            assert_eq!(
                bucket.get(&b"key2".to_vec()).await?,
                Some(b"value2".to_vec())
            );
            assert_eq!(
                bucket.get(&b"key3".to_vec()).await?,
                Some(b"value3".to_vec())
            );

            // Test missing key
            assert_eq!(bucket.get(&b"nonexistent".to_vec()).await?, None);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_handles_large_values(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            // Create a 100KB value
            let key = b"large-key".to_vec();
            let value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

            // Set and retrieve
            bucket.set(key.clone(), value.clone()).await?;
            let retrieved = bucket.get(&key).await?;
            assert_eq!(retrieved, Some(value));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_uses_prefix(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));

            // Create two backends with different paths
            let bucket = Bucket::new(s3.clone(), TEST_SUBJECT, "prefix-a");
            let mut backend1 = bucket;
            let mut backend2 = Bucket::new(s3, TEST_SUBJECT, "prefix-b");

            // Set the same key in both backends
            let key = b"shared-key".to_vec();
            backend1.set(key.clone(), b"value-a".to_vec()).await?;
            backend2.set(key.clone(), b"value-b".to_vec()).await?;

            // Each backend should see its own value
            assert_eq!(backend1.get(&key).await?, Some(b"value-a".to_vec()));
            assert_eq!(backend2.get(&key).await?, Some(b"value-b".to_vec()));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_nests_at_calls(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let bucket = create_test_bucket(&env);

            // Create nested buckets
            let mut nested = bucket.at("data").at("v1");

            let key = b"nested-key".to_vec();
            let value = b"nested-value".to_vec();

            nested.set(key.clone(), value.clone()).await?;
            let retrieved = nested.get(&key).await?;

            assert_eq!(retrieved, Some(value));
            Ok(())
        }

        #[dialog_common::test]
        async fn it_handles_encoded_key_segments(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

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
            bucket.set(key_mixed.clone(), value_mixed.clone()).await?;
            let retrieved = bucket.get(&key_mixed).await?;
            assert_eq!(retrieved, Some(value_mixed));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_handles_multi_segment_mixed_encoding(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            // Test key with multiple segments: safe/unsafe/safe/unsafe pattern
            // "data/file name with spaces/v1/special!chars"
            let key = b"data/file name with spaces/v1/special!chars".to_vec();
            let value = b"value-for-complex-path".to_vec();

            // Verify encoding
            let encoded = encode_s3_key(&key);
            let segments: Vec<&str> = encoded.split('/').collect();
            assert_eq!(segments.len(), 4);
            assert_eq!(segments[0], "data"); // safe
            assert!(segments[1].starts_with('!')); // unsafe (spaces)
            assert_eq!(segments[2], "v1"); // safe
            assert!(segments[3].starts_with('!')); // unsafe (!)

            // Write and read back to verify full roundtrip through S3
            bucket.set(key.clone(), value.clone()).await?;
            let retrieved = bucket.get(&key).await?;
            assert_eq!(retrieved, Some(value));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_deletes_values(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            let key = b"delete-test-key".to_vec();
            let value = b"value-to-delete".to_vec();

            // Set the value
            bucket.set(key.clone(), value.clone()).await?;

            // Verify it exists
            assert_eq!(bucket.get(&key).await?, Some(value));

            // Delete the value
            bucket.delete(&key).await?;

            // Verify it's gone
            assert_eq!(bucket.get(&key).await?, None);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_deletes_nonexistent_key_silently(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env);

            // Delete a key that doesn't exist - should succeed (S3 behavior)
            let result = bucket.delete(b"nonexistent-delete-key").await;
            assert!(result.is_ok(), "Deleting nonexistent key should succeed");

            Ok(())
        }
    }

    /// Key encoding/decoding unit tests (no S3 server needed).
    mod key_encoding_tests {
        use super::*;

        #[dialog_common::test]
        fn it_roundtrips_key_encoding() {
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
        fn it_encodes_mixed_segments() {
            // "safe-segment/user@example.com" - first segment is safe, second has @ which is unsafe
            let key = b"safe-segment/user@example.com".to_vec();
            let encoded = encode_s3_key(&key);

            assert!(
                encoded.starts_with("safe-segment/!"),
                "First segment should be safe, second should be encoded with ! prefix: {}",
                encoded
            );
        }

        #[dialog_common::test]
        fn it_encodes_multi_segment_mixed() {
            // "data/file name with spaces/v1/special!chars"
            let key = b"data/file name with spaces/v1/special!chars".to_vec();
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
        }
    }

    /// Tests for signed (authenticated) S3 sessions using private credentials.
    mod signed_session_tests {
        use super::*;

        #[allow(dead_code)]
        fn create_signed_bucket(env: &helpers::S3Address) -> Bucket<helpers::Session> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds =
                S3Credentials::private(address, &env.access_key_id, &env.secret_access_key)
                    .unwrap()
                    .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));
            Bucket::new(s3, TEST_SUBJECT, "signed-test")
        }

        #[dialog_common::test]
        async fn it_works_with_signed_session(env: helpers::S3Address) -> anyhow::Result<()> {
            let mut bucket = create_signed_bucket(&env);

            let key = b"signed-key".to_vec();
            let value = b"signed-value".to_vec();

            // Set the value (uses PUT with presigned URL)
            bucket.set(key.clone(), value.clone()).await?;

            // Get the value back (uses GET with presigned URL)
            let retrieved = bucket.get(&key).await?;
            assert_eq!(retrieved, Some(value));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_fails_with_wrong_secret_key(env: helpers::S3Address) -> anyhow::Result<()> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds = S3Credentials::private(address, &env.access_key_id, "wrong-secret")
                .unwrap()
                .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));
            let mut bucket = Bucket::new(s3, TEST_SUBJECT, "test");

            // Attempt to set a value - should fail due to signature mismatch
            let result = bucket.set(b"key".to_vec(), b"value".to_vec()).await;

            assert!(
                result.is_err(),
                "Expected authentication failure with wrong secret key"
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_fails_with_wrong_access_key(env: helpers::S3Address) -> anyhow::Result<()> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds =
                S3Credentials::private(address, "wrong-access-key", &env.secret_access_key)
                    .unwrap()
                    .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));
            let mut bucket = Bucket::new(s3, TEST_SUBJECT, "test");

            // Attempt to set a value - should fail due to unknown access key
            let result = bucket.set(b"key".to_vec(), b"value".to_vec()).await;

            assert!(
                result.is_err(),
                "Expected authentication failure with wrong access key"
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_fails_unsigned_request_to_auth_server(
            env: helpers::S3Address,
        ) -> anyhow::Result<()> {
            // Client uses no credentials but server requires authentication
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));
            let mut bucket = Bucket::new(s3, TEST_SUBJECT, "test");

            // Attempt to set a value - should fail because server expects signed requests
            let result = bucket.set(b"key".to_vec(), b"value".to_vec()).await;

            assert!(
                result.is_err(),
                "Expected authentication failure when sending unsigned request to authenticated server"
            );

            Ok(())
        }

        #[dialog_common::test]
        async fn it_fails_get_with_wrong_credentials(
            env: helpers::S3Address,
        ) -> anyhow::Result<()> {
            // First, set a value with correct credentials
            let mut correct_bucket = create_signed_bucket(&env);
            correct_bucket
                .set(b"protected-key".to_vec(), b"secret-value".to_vec())
                .await?;

            // Now try to GET with wrong credentials
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let wrong_creds = S3Credentials::private(address, &env.access_key_id, "wrong-secret")
                .unwrap()
                .with_path_style(true);
            let s3 = S3::from_s3(wrong_creds, helpers::Session::new(TEST_SUBJECT));
            let wrong_bucket = Bucket::new(s3, TEST_SUBJECT, "signed-test");

            // Attempt to get the value - should fail
            let result = wrong_bucket.get(&b"protected-key".to_vec()).await;

            assert!(
                result.is_err(),
                "Expected authentication failure when getting with wrong credentials"
            );

            Ok(())
        }
    }

    /// Tests for UCAN-based storage operations.
    #[cfg(feature = "ucan")]
    mod ucan_storage_tests {
        use super::*;
        #[allow(unused_imports)]
        use dialog_common::capability::{Principal, Subject};
        use dialog_s3_credentials::ucan::{
            Credentials as UcanCredentials, DelegationChain, test_helpers::create_delegation,
        };
        use helpers::Operator;

        #[allow(dead_code)]
        fn create_ucan_bucket(
            env: &helpers::UcanS3Address,
            operator: &Operator,
            can: &[&str],
        ) -> Bucket<Operator> {
            let delegation = {
                let subject_did = operator.signer().did().clone();
                let delegation = create_delegation(
                    operator.signer(),
                    &operator.signer().did(),
                    &subject_did,
                    can,
                )
                .expect("Failed to create test delegation");
                DelegationChain::new(delegation)
            };
            let ucan_credentials = UcanCredentials::new(env.access_service_url.clone(), delegation);
            let s3 = S3::new(Credentials::Ucan(ucan_credentials), operator.clone());
            Bucket::new(s3, operator.did().to_string(), "test-store")
        }

        #[dialog_common::test]
        async fn it_performs_storage_get_and_set_via_ucan(
            env: helpers::UcanS3Address,
        ) -> anyhow::Result<()> {
            let operator = Operator::generate();
            let mut bucket = create_ucan_bucket(&env, &operator, &["storage"]);

            let key = b"ucan-test-key".to_vec();
            let value = b"ucan-test-value".to_vec();

            // Set the value using UCAN authorization
            bucket.set(key.clone(), value.clone()).await?;

            // Get the value back
            let retrieved = bucket.get(&key).await?;
            assert_eq!(retrieved, Some(value));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_performs_storage_delete_via_ucan(
            env: helpers::UcanS3Address,
        ) -> anyhow::Result<()> {
            let operator = Operator::generate();
            let subject_did = operator.did().to_string();

            // Create bucket with storage delegation
            let delegation = {
                let subject_did_clone = operator.signer().did().clone();
                let delegation = create_delegation(
                    operator.signer(),
                    &operator.signer().did(),
                    &subject_did_clone,
                    &["storage"],
                )
                .expect("Failed to create test delegation");
                DelegationChain::new(delegation)
            };
            let ucan_credentials = UcanCredentials::new(env.access_service_url.clone(), delegation);
            let mut bucket = S3::new(Credentials::Ucan(ucan_credentials), operator);

            let store_name = "test-store";
            let key = b"ucan-delete-key".to_vec();
            let value = b"value-to-delete".to_vec();

            // First set a value
            Subject::from(subject_did.clone())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name))
                .invoke(storage::Set {
                    key: key.clone().into(),
                    value: value.clone().into(),
                })
                .perform(&mut bucket)
                .await?;

            // Verify it exists
            let result = Subject::from(subject_did.clone())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name))
                .invoke(storage::Get {
                    key: key.clone().into(),
                })
                .perform(&mut bucket)
                .await?;
            assert_eq!(result, Some(value.into()));

            // Delete the value
            Subject::from(subject_did.clone())
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name))
                .invoke(storage::Delete {
                    key: key.clone().into(),
                })
                .perform(&mut bucket)
                .await?;

            // Verify it's gone
            let result = Subject::from(subject_did)
                .attenuate(storage::Storage)
                .attenuate(storage::Store::new(store_name))
                .invoke(storage::Get {
                    key: key.clone().into(),
                })
                .perform(&mut bucket)
                .await?;
            assert_eq!(result, None);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_returns_none_for_nonexistent_key_via_ucan(
            env: helpers::UcanS3Address,
        ) -> anyhow::Result<()> {
            let operator = Operator::generate();
            let bucket = create_ucan_bucket(&env, &operator, &["storage"]);

            // Try to get a key that doesn't exist
            let result = bucket.get(&b"nonexistent-ucan-key".to_vec()).await?;
            assert_eq!(result, None);

            Ok(())
        }
    }

    /// Tests for S3 list operations.
    #[cfg(feature = "s3-list")]
    mod list_tests {
        use super::*;

        #[allow(dead_code)]
        fn create_test_bucket(
            env: &helpers::PublicS3Address,
            store: &str,
        ) -> Bucket<helpers::Session> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));
            Bucket::new(s3, TEST_SUBJECT, store)
        }

        #[allow(dead_code)]
        fn create_signed_bucket(env: &helpers::S3Address, store: &str) -> Bucket<helpers::Session> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds =
                S3Credentials::private(address, &env.access_key_id, &env.secret_access_key)
                    .unwrap()
                    .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));
            Bucket::new(s3, TEST_SUBJECT, store)
        }

        #[dialog_common::test]
        async fn it_lists_objects(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let mut bucket = create_test_bucket(&env, "list-test");

            // Set multiple values
            bucket.set(b"key1".to_vec(), b"value1".to_vec()).await?;
            bucket.set(b"key2".to_vec(), b"value2".to_vec()).await?;
            bucket.set(b"key3".to_vec(), b"value3".to_vec()).await?;

            // List objects
            let result = bucket.list(None).await?;

            assert_eq!(result.keys.len(), 3);
            assert!(!result.is_truncated);

            // All keys should have the prefix
            for key in &result.keys {
                assert!(
                    key.starts_with(&format!("{}/list-test/", TEST_SUBJECT)),
                    "Key {} should start with prefix",
                    key
                );
            }

            Ok(())
        }

        #[dialog_common::test]
        async fn it_lists_empty_for_nonexistent_prefix(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            let bucket = create_test_bucket(&env, "nonexistent-prefix-that-does-not-exist");

            // List objects with a prefix that has no objects - should return empty list
            let result = bucket.list(None).await?;

            assert!(result.keys.is_empty());
            assert!(!result.is_truncated);
            assert!(result.next_continuation_token.is_none());

            Ok(())
        }

        #[dialog_common::test]
        async fn it_uses_prefix_for_listing(env: helpers::PublicS3Address) -> anyhow::Result<()> {
            let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
            let s3_creds = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));

            // Create two buckets with different prefixes
            let mut bucket_a = Bucket::new(s3.clone(), TEST_SUBJECT, "list-prefix-a");
            let mut bucket_b = Bucket::new(s3, TEST_SUBJECT, "list-prefix-b");

            // Add objects to each bucket
            bucket_a.set(b"key1".to_vec(), b"value-a1".to_vec()).await?;
            bucket_a.set(b"key2".to_vec(), b"value-a2".to_vec()).await?;
            bucket_b.set(b"key1".to_vec(), b"value-b1".to_vec()).await?;

            // List each bucket - they should only see their own objects
            let result_a = bucket_a.list(None).await?;
            let result_b = bucket_b.list(None).await?;

            assert_eq!(result_a.keys.len(), 2);
            assert_eq!(result_b.keys.len(), 1);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_lists_with_signed_session(env: helpers::S3Address) -> anyhow::Result<()> {
            let mut bucket = create_signed_bucket(&env, "signed-list-test");

            // Set multiple values
            bucket.set(b"key1".to_vec(), b"value1".to_vec()).await?;
            bucket.set(b"key2".to_vec(), b"value2".to_vec()).await?;

            // List objects with signed request
            let result = bucket.list(None).await?;

            assert_eq!(result.keys.len(), 2);
            assert!(!result.is_truncated);

            // All keys should have the prefix
            for key in &result.keys {
                assert!(
                    key.starts_with(&format!("{}/signed-list-test/", TEST_SUBJECT)),
                    "Key {} should start with prefix",
                    key
                );
            }

            Ok(())
        }

        #[dialog_common::test]
        async fn it_errors_on_nonexistent_bucket(
            env: helpers::PublicS3Address,
        ) -> anyhow::Result<()> {
            // Create a bucket pointing to a non-existent bucket
            let address = Address::new(&env.endpoint, "us-east-1", "bucket-that-does-not-exist");
            let s3_creds = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
            let s3 = S3::from_s3(s3_creds, helpers::Session::new(TEST_SUBJECT));
            let bucket = Bucket::new(s3, TEST_SUBJECT, "test");

            // S3 returns 404 NoSuchBucket error when listing a non-existent bucket.
            let result = bucket.list(None).await;

            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                err.to_string().contains("NoSuchBucket") || err.to_string().contains("404"),
                "Expected NoSuchBucket error for non-existent bucket, got: {:?}",
                err
            );

            Ok(())
        }
    }

    /// UCAN list tests.
    #[cfg(all(feature = "s3-list", feature = "ucan"))]
    mod ucan_list_tests {
        use super::*;
        use dialog_common::capability::Principal;
        use dialog_s3_credentials::ucan::{
            Credentials as UcanCredentials, DelegationChain, test_helpers::create_delegation,
        };
        use helpers::Operator;

        #[allow(dead_code)]
        fn create_ucan_bucket(
            env: &helpers::UcanS3Address,
            operator: &Operator,
            store: &str,
            can: &[&str],
        ) -> Bucket<Operator> {
            let delegation = {
                let subject_did = operator.signer().did().clone();
                let delegation = create_delegation(
                    operator.signer(),
                    &operator.signer().did(),
                    &subject_did,
                    can,
                )
                .expect("Failed to create test delegation");
                DelegationChain::new(delegation)
            };
            let ucan_credentials = UcanCredentials::new(env.access_service_url.clone(), delegation);
            let s3 = S3::new(Credentials::Ucan(ucan_credentials), operator.clone());
            Bucket::new(s3, operator.did().to_string(), store)
        }

        #[dialog_common::test]
        async fn it_lists_objects_via_ucan(env: helpers::UcanS3Address) -> anyhow::Result<()> {
            let operator = Operator::generate();
            let mut bucket = create_ucan_bucket(&env, &operator, "ucan-list-test", &["storage"]);

            // Set multiple values
            bucket.set(b"key1".to_vec(), b"value1".to_vec()).await?;
            bucket.set(b"key2".to_vec(), b"value2".to_vec()).await?;
            bucket.set(b"key3".to_vec(), b"value3".to_vec()).await?;

            // List objects
            let result = bucket.list(None).await?;

            assert_eq!(result.keys.len(), 3);
            assert!(!result.is_truncated);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_lists_empty_for_nonexistent_prefix_via_ucan(
            env: helpers::UcanS3Address,
        ) -> anyhow::Result<()> {
            let operator = Operator::generate();
            let bucket =
                create_ucan_bucket(&env, &operator, "nonexistent-ucan-prefix", &["storage"]);

            // List objects with a prefix that has no objects - should return empty list
            let result = bucket.list(None).await?;

            assert!(result.keys.is_empty());
            assert!(!result.is_truncated);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_uses_prefix_for_listing_via_ucan(
            env: helpers::UcanS3Address,
        ) -> anyhow::Result<()> {
            let operator = Operator::generate();

            // Create two buckets with different prefixes but same operator
            let mut bucket_a =
                create_ucan_bucket(&env, &operator, "ucan-list-prefix-a", &["storage"]);
            let mut bucket_b =
                create_ucan_bucket(&env, &operator, "ucan-list-prefix-b", &["storage"]);

            // Add objects to each bucket
            bucket_a.set(b"key1".to_vec(), b"value-a1".to_vec()).await?;
            bucket_a.set(b"key2".to_vec(), b"value-a2".to_vec()).await?;
            bucket_b.set(b"key1".to_vec(), b"value-b1".to_vec()).await?;

            // List each bucket - they should only see their own objects
            let result_a = bucket_a.list(None).await?;
            let result_b = bucket_b.list(None).await?;

            assert_eq!(result_a.keys.len(), 2);
            assert_eq!(result_b.keys.len(), 1);

            Ok(())
        }
    }

    /// URL building unit tests (no S3 server needed).
    /// These tests verify that credentials can be constructed with various configurations.
    mod url_building_tests {
        use super::*;

        #[dialog_common::test]
        fn it_creates_virtual_hosted_style_credentials() {
            // Virtual-hosted style: {bucket}.{endpoint}/{key}
            // Non-localhost endpoints default to virtual-hosted style
            let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
            let _credentials = S3Credentials::public(address).unwrap();
        }

        #[dialog_common::test]
        fn it_creates_path_style_credentials_for_localhost() {
            // Path-style: {endpoint}/{bucket}/{key}
            // localhost endpoints default to path style
            let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
            let _credentials = S3Credentials::public(address).unwrap();
        }

        #[dialog_common::test]
        fn it_allows_forcing_path_style() {
            // Force path-style on a non-localhost endpoint
            let address = Address::new("https://custom-s3.example.com", "us-east-1", "bucket");
            let _credentials = S3Credentials::public(address)
                .unwrap()
                .with_path_style(true);
        }

        #[dialog_common::test]
        fn it_allows_forcing_virtual_hosted_on_localhost() {
            // Force virtual-hosted on localhost (not typical, but supported)
            let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
            let _credentials = S3Credentials::public(address)
                .unwrap()
                .with_path_style(false);
        }

        #[dialog_common::test]
        fn it_creates_r2_credentials() {
            // R2 uses virtual-hosted style by default (non-localhost)
            let address = Address::new("https://abc123.r2.cloudflarestorage.com", "auto", "bucket");
            let _credentials = S3Credentials::public(address).unwrap();
        }

        #[dialog_common::test]
        fn it_creates_private_credentials() {
            let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
            let _credentials = S3Credentials::private(address, "access-key", "secret-key").unwrap();
        }
    }
}
