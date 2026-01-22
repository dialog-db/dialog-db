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
//! use dialog_storage::s3::{Address, Bucket, Credentials};
//! use dialog_storage::StorageBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create address with endpoint, region, and bucket
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//! // Subject DID identifies whose data we're accessing (used as path prefix)
//! let subject = "did:key:zMySubject";
//! let credentials = Credentials::public(address, subject)?;
//! let bucket = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?;
//! let mut backend = bucket.at("data");  // Scope to a prefix/directory
//!
//! backend.set(b"key".to_vec(), b"value".to_vec()).await?;
//! let value = backend.get(&b"key".to_vec()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Authorized Access (Credentials based Authentication)
//!
//! ```no_run
//! use dialog_storage::s3::{Address, Credentials, Bucket};
//! use dialog_storage::StorageBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//! // Subject DID identifies whose data we're accessing
//! let subject = "did:key:zMySubject";
//! let credentials = Credentials::private(
//!     address,
//!     subject,
//!     std::env::var("AWS_ACCESS_KEY_ID")?,
//!     std::env::var("AWS_SECRET_ACCESS_KEY")?,
//! )?;
//!
//! let bucket = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?;
//! let mut backend = bucket.at("data");
//!
//! backend.set(b"key".to_vec(), b"value".to_vec()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Cloudflare R2
//!
//! ```no_run
//! use dialog_storage::s3::{Address, Credentials, Bucket};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // R2 uses "auto" region for signing
//! let address = Address::new(
//!     "https://account-id.r2.cloudflarestorage.com",
//!     "auto",
//!     "my-bucket",
//! );
//! let subject = "did:key:zMySubject";
//! let credentials = Credentials::private(
//!     address,
//!     subject,
//!     std::env::var("R2_ACCESS_KEY_ID")?,
//!     std::env::var("R2_SECRET_ACCESS_KEY")?,
//! )?;
//!
//! let bucket = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?;
//! let backend = bucket.at("data");
//! # Ok(())
//! # }
//! ```
//!
//! ## Local Development (MinIO)
//!
//! ```no_run
//! use dialog_storage::s3::{Address, Credentials, Bucket};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // IP addresses and localhost automatically use path-style URLs
//! let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
//! let subject = "did:key:zMySubject";
//! let credentials = Credentials::private(address, subject, "minioadmin", "minioadmin")?;
//! let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?;
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
    Bytes, ConditionalSend, ConditionalSync,
    capability::{
        Access, Authorized, Capability, Constrained, Constraint, Policy, Provider, Subject,
    },
};
use futures_util::{Stream, StreamExt, TryStreamExt};
use std::marker::PhantomData;
use thiserror::Error;

// Re-export core types from dialog-s3-credentials crate
pub use dialog_s3_credentials::{
    Address, AuthorizationError as AccessError, AuthorizedRequest, Checksum, Hasher,
};
// Use access module types for direct S3 authorization
pub use dialog_s3_credentials::{
    access,
    access::{Precondition, S3Request},
};

pub use crate::capability::{archive, memory, storage};

// Re-export s3::Credentials types
pub use dialog_s3_credentials::s3::{Credentials, PrivateCredentials, PublicCredentials};

/// Type alias for backwards compatibility.
pub type Public = PublicCredentials;

// Re-export UCAN types when the feature is enabled
#[cfg(feature = "ucan")]
pub use dialog_s3_credentials::ucan::{
    Credentials as UcanCredentials, CredentialsBuilder as UcanCredentialsBuilder, DelegationChain,
    OperatorIdentity,
};

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
pub use helpers::{PublicS3Address, S3Address, UcanS3Address};
#[cfg(all(feature = "helpers", feature = "ucan", not(target_arch = "wasm32")))]
pub use helpers::{UcanAccessServer, UcanS3Settings};

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

// Implement Authorizer for s3::Credentials
impl Authorizer for Credentials {
    fn subject(&self) -> &str {
        dialog_s3_credentials::s3::Credentials::subject(self).as_str()
    }

    fn authorize<C: S3Request>(&self, claim: &C) -> Result<AuthorizedRequest, AccessError> {
        dialog_s3_credentials::s3::Credentials::authorize(self, claim)
    }
}

// Implement for ucan::Credentials
#[cfg(feature = "ucan")]
impl Authorizer for UcanCredentials {
    fn subject(&self) -> &str {
        dialog_s3_credentials::ucan::Credentials::subject(self)
    }

    fn authorize<C: S3Request>(&self, _claim: &C) -> Result<AuthorizedRequest, AccessError> {
        // TODO: UCAN credentials need async authorization - this needs a different approach
        Err(AccessError::Invocation(
            "UCAN credentials require async authorization".to_string(),
        ))
    }
}

trait ArchiveProvider: Provider<access::archive::Get> + Provider<access::archive::Put> {}
impl<P: Provider<access::archive::Get> + Provider<access::archive::Put>> ArchiveProvider for P {}

pub struct S3Bucket<A: Access, P> {
    provider: P,
    access: A,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<A: Access, P: Provider<Authorized<access::archive::Get, A::Authorization>>>
    Provider<archive::Get> for S3Bucket<A, P>
{
    async fn execute(
        &mut self,
        input: Capability<archive::Get>,
    ) -> Result<Option<Bytes>, archive::ArchiveError> {
        // obtain authorization for access::archive::Get
        let authorize = Subject::from(input.subject())
            .attenuate(access::archive::Archive)
            .attenuate(access::archive::Catalog {
                catalog: archive::Catalog::of(input).catalog,
            })
            .invoke(access::archive::Get {
                digest: archive::Get::of(input).digest,
            })
            .acquire(&mut self.access)
            .await?;

        let authorization = authorize.perform(&mut self.provider).await?;

        let client = reqwest::Client::new();
        let mut builder = authorization.into_request(&client);
        let response = builder.send().await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(S3StorageError::ServiceError(format!(
                "Failed to get value: {}",
                response.status()
            )))
        }
    }
}

/// S3/R2-compatible storage backend.
///
/// The `Bucket` is configured entirely through its credentials, which provides:
/// - The S3 endpoint, region, and bucket
/// - URL building logic
/// - Request signing/authorization via the [`Signer`] trait
///
/// # Example
///
/// ```no_run
/// use dialog_storage::s3::{Bucket, Address, Credentials};
/// use dialog_storage::StorageBackend;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Public access (no credentials)
/// let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
/// let subject = "did:key:zMySubject";  // Subject DID for path prefix
/// let mut storage = Bucket::<Vec<u8>, Vec<u8>, _>::open(Credentials::public(address, subject)?)?;
///
/// // With credentials and prefix
/// let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
/// let credentials = Credentials::private(address, subject, "access_key", "secret_key")?;
/// let mut storage = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?
///     .at("data");  // Scope to a prefix/directory within the bucket
///
/// storage.set(b"key".to_vec(), b"value".to_vec()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Bucket<Key, Value, C>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    C: Authorizer,
{
    /// Optional prefix/directory within the bucket
    path: Option<String>,
    /// Credentials for authorizing requests and building URLs
    credentials: C,
    /// Hasher for computing checksums
    hasher: Hasher,
    /// HTTP client
    client: reqwest::Client,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
}

impl<Key, Value, C> Bucket<Key, Value, C>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    C: Authorizer,
{
    /// Open an S3 storage bucket with the given credentials.
    ///
    /// The credentials provide all configuration including endpoint, region, bucket,
    /// and signing logic. Use:
    /// - [`Credentials::public(address)`](Credentials::public) for public/unsigned access
    /// - [`Credentials::private(address, key, secret)`](Credentials::private) for AWS SigV4 signing
    /// - [`UcanCredentials`] for UCAN-based access via an access service
    ///
    /// By default uses SHA-256 for checksums. Use [`with_hasher`](Self::with_hasher)
    /// to configure a different algorithm.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dialog_storage::s3::{Bucket, Address, Credentials};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Public access (no signing)
    /// let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
    /// let subject = "did:key:zMySubject";
    /// let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(Credentials::public(address, subject)?)?;
    ///
    /// // AWS credentials
    /// let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
    /// let credentials = Credentials::private(address, subject, "minioadmin", "minioadmin")?;
    /// let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(credentials)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(credentials: C) -> Result<Self, S3StorageError> {
        Ok(Self {
            path: None,
            credentials,
            hasher: Hasher::Sha256,
            client: reqwest::Client::new(),
            key_type: PhantomData,
            value_type: PhantomData,
        })
    }

    /// Returns a new `Bucket` scoped to the given prefix/directory.
    ///
    /// All keys will be resolved from this path. Can be chained to create
    /// nested paths.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dialog_storage::s3::{Bucket, Address, Credentials};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
    /// let subject = "did:key:zMySubject";
    /// let bucket = Bucket::<Vec<u8>, Vec<u8>, _>::open(Credentials::public(address, subject)?)?;
    ///
    /// // Scope to "data" directory
    /// let data = bucket.at("data");
    ///
    /// // Scope to nested path "data/v1"
    /// let v1 = bucket.at("data").at("v1");
    /// // or equivalently:
    /// let v1 = bucket.at("data/v1");
    /// # Ok(())
    /// # }
    /// ```
    pub fn at(&self, path: impl Into<String>) -> Self {
        let new_path = path.into();
        let prefix = match &self.path {
            Some(existing) => Some(format!("{}/{}", existing, new_path)),
            None => Some(new_path),
        };

        Self {
            path: prefix,
            credentials: self.credentials.clone(),
            hasher: self.hasher,
            client: self.client.clone(),
            key_type: PhantomData,
            value_type: PhantomData,
        }
    }

    /// Get the prefix/directory path, if any.
    pub fn prefix(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// Set the hasher for computing checksums.
    pub fn with_hasher(mut self, hasher: Hasher) -> Self {
        self.hasher = hasher;
        self
    }

    /// Encode a key and build the full path including any prefix.
    ///
    /// This handles:
    /// - Key encoding (base58 for binary/unsafe characters)
    /// - Prefix prepending
    pub fn encode_path(&self, key: &[u8]) -> String {
        let encoded_key = encode_s3_key(key);
        match &self.path {
            Some(prefix) => format!("{}/{}", prefix, encoded_key),
            None => encoded_key,
        }
    }

    /// Get the prefix path (used for listing operations).
    pub fn prefix_path(&self) -> String {
        self.path.clone().unwrap_or_default()
    }

    /// Send an authorized HTTP request using the Provider trait.
    ///
    /// This is the core method that:
    /// 1. Uses the pre-authorized RequestDescriptor from Provider::execute
    /// 2. Builds the reqwest request with headers via `into_request`
    /// 3. Optionally adds body and precondition headers
    /// 4. Sends the request
    async fn send_request(
        &self,
        descriptor: AuthorizedRequest,
        body: Option<&[u8]>,
        precondition: Precondition,
    ) -> Result<reqwest::Response, S3StorageError> {
        // Build the HTTP request using the pre-authorized descriptor
        let mut builder = descriptor.into_request(&self.client);

        // Add precondition headers for CAS semantics
        match &precondition {
            Precondition::None => {}
            Precondition::IfMatch(etag) => {
                builder = builder.header("If-Match", format!("\"{}\"", etag));
            }
            Precondition::IfNoneMatch => {
                builder = builder.header("If-None-Match", "*");
            }
        }

        // Add body if present
        if let Some(body) = body {
            builder = builder.body(body.to_vec());
        }

        // Send the request
        Ok(builder.send().await?)
    }

    /// Delete an object from S3.
    ///
    /// Note: S3 DELETE always returns 204 No Content, even if the object didn't exist.
    /// This method always returns `Ok(())` on success.
    pub async fn delete(&mut self, key: &Key) -> Result<(), S3StorageError> {
        let subject = self.credentials.subject();
        let store = self.prefix_path();
        let encoded_key = encode_s3_key(key.as_ref());
        let claim = StorageClaim::delete(subject, store, encoded_key.as_bytes());
        let descriptor = self
            .credentials
            .authorize(&claim)
            .map_err(S3StorageError::from)?;

        let response = self
            .send_request(descriptor, None, Precondition::None)
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(S3StorageError::ServiceError(format!(
                "Failed to delete object: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value, C> StorageBackend for Bucket<Key, Value, C>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    C: Authorizer,
{
    type Key = Key;
    type Value = Value;
    type Error = S3StorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let subject = self.credentials.subject();
        let store = self.prefix_path();
        let encoded_key = encode_s3_key(key.as_ref());
        let checksum = self.hasher.checksum(value.as_ref());
        let claim = StorageClaim::set(subject, store, encoded_key.as_bytes(), checksum);
        let descriptor = self
            .credentials
            .authorize(&claim)
            .map_err(S3StorageError::from)?;

        let response = self
            .send_request(descriptor, Some(value.as_ref()), Precondition::None)
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(S3StorageError::ServiceError(format!(
                "Failed to set value: {}",
                response.status()
            )))
        }
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let subject = self.credentials.subject();
        let store = self.prefix_path();
        let encoded_key = encode_s3_key(key.as_ref());
        let claim = StorageClaim::get(subject, store, encoded_key.as_bytes());
        let descriptor = self
            .credentials
            .authorize(&claim)
            .map_err(S3StorageError::from)?;

        let response = self
            .send_request(descriptor, None, Precondition::None)
            .await?;

        if response.status().is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| S3StorageError::TransportError(e.to_string()))?;
            Ok(Some(Value::from(bytes.to_vec())))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(S3StorageError::ServiceError(format!(
                "Failed to get value: {}",
                response.status()
            )))
        }
    }
}

/// Maximum number of concurrent S3 PUT requests when writing.
/// Modern mainstream browsers typically enforce a limit of 6 concurrent
/// requests on HTTP/1.1 which is what S3 is.
const MAX_CONCURRENT_WRITES: usize = 6;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value, C> StorageSink for Bucket<Key, Value, C>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    C: Authorizer,
{
    async fn write<S>(&mut self, source: S) -> Result<(), Self::Error>
    where
        S: Stream<Item = Result<(Self::Key, Self::Value), Self::Error>> + ConditionalSend,
    {
        let storage = self.clone();

        // Map each item to a set operation, then run up to MAX_CONCURRENT_WRITES in parallel
        source
            .map(|result| {
                let mut storage = storage.clone();
                async move {
                    let (key, value) = result?;
                    storage.set(key, value).await
                }
            })
            .buffer_unordered(MAX_CONCURRENT_WRITES)
            .try_collect()
            .await
    }
}

#[cfg(feature = "s3-list")]
impl<Key, Value, C> StorageSource for Bucket<Key, Value, C>
where
    Key: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    C: Authorizer,
{
    fn read(&self) -> impl Stream<Item = Result<(Self::Key, Self::Value), Self::Error>> {
        let storage = self.clone();
        let prefix = self.prefix().map(String::from);
        use async_stream::try_stream;

        try_stream! {
            let mut continuation_token: Option<String> = None;

            loop {
                // Use the S3 ListObjectsV2 API with proper authorization
                let result = storage.list(continuation_token.as_deref()).await?;

                for encoded_key in result.keys {
                    // Strip the prefix from the key if present
                    let key_without_prefix = match &prefix {
                        Some(p) => {
                            let prefix_with_slash = format!("{}/", p);
                            encoded_key.strip_prefix(&prefix_with_slash)
                                .unwrap_or(&encoded_key)
                                .to_string()
                        }
                        None => encoded_key,
                    };

                    // Decode and fetch the value
                    let decoded = decode_s3_key(&key_without_prefix)?;

                    if let Some(value) = storage.get(&Key::from(decoded.clone())).await? {
                        yield (Key::from(decoded), value);
                    }
                }

                // Check if there are more results
                if result.is_truncated {
                    continuation_token = result.next_continuation_token;
                } else {
                    break;
                }
            }
        }
    }

    fn drain(&mut self) -> impl Stream<Item = Result<(Self::Key, Self::Value), Self::Error>> {
        // S3 doesn't support draining, so just read
        self.read()
    }
}

/// Transactional memory backend implementation for S3-compatible storage.
///
/// This implementation provides Compare-And-Swap (CAS) semantics using S3's native
/// conditional request headers, enabling safe concurrent access to objects across
/// multiple processes or replicas.
///
/// # Edition Tracking with ETags
///
/// S3 automatically assigns an [ETag] (entity tag) to each object version. This
/// implementation uses ETags as editions for optimistic concurrency control:
///
/// - **resolve**: Returns the object's current ETag along with its content
/// - **replace**: Uses `If-Match` header to ensure the object hasn't changed since
///   it was read. If the ETag doesn't match, the request fails with 412 Precondition
///   Failed, indicating a concurrent modification.
///
/// # Conditional Operations
///
/// - **Create new**: Uses `If-None-Match: *` to ensure the object doesn't exist
/// - **Update existing**: Uses `If-Match: <etag>` to ensure no concurrent changes
/// - **Delete**: Uses `If-Match: <etag>` for safe deletion (when edition provided)
///
/// [ETag]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value, C> TransactionalMemoryBackend for Bucket<Key, Value, C>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    C: Authorizer + Authorizer,
{
    type Address = Key;
    type Value = Value;
    type Error = S3StorageError;
    type Edition = String;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let subject_did = self.credentials.subject();
        let space = self.path.clone().unwrap_or_default();
        let cell = encode_s3_key(address.as_ref());
        let claim = MemoryClaim::resolve(subject_did, &space, &cell);
        let descriptor = self
            .credentials
            .authorize(&claim)
            .map_err(S3StorageError::from)?;

        let response = self
            .send_request(descriptor, None, Precondition::None)
            .await?;

        if response.status().is_success() {
            // Extract ETag from response headers
            let etag = response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim_matches('"').to_string())
                .ok_or_else(|| {
                    S3StorageError::ServiceError("Response missing ETag header".to_string())
                })?;

            let bytes = response
                .bytes()
                .await
                .map_err(|e| S3StorageError::TransportError(e.to_string()))?;
            Ok(Some((Value::from(bytes.to_vec()), etag)))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(S3StorageError::ServiceError(format!(
                "Failed to resolve value: {}",
                response.status()
            )))
        }
    }

    async fn replace(
        &self,
        address: &Self::Address,
        edition: Option<&Self::Edition>,
        content: Option<Self::Value>,
    ) -> Result<Option<Self::Edition>, Self::Error> {
        let subject_did = self.credentials.subject();
        let space = self.path.clone().unwrap_or_default();
        let cell = encode_s3_key(address.as_ref());
        // Edition is now String (memory::Edition = String)
        let when = edition.cloned();

        match content {
            Some(new_value) => {
                let checksum = self.hasher.checksum(new_value.as_ref());
                let claim =
                    MemoryClaim::publish(subject_did, &space, &cell, checksum, when.clone());
                let descriptor = self
                    .credentials
                    .authorize(&claim)
                    .map_err(S3StorageError::from)?;

                // Convert edition to local Precondition for send_request
                let local_precondition = match edition {
                    Some(etag) => Precondition::IfMatch(etag.clone()),
                    None => Precondition::IfNoneMatch,
                };

                let response = self
                    .send_request(descriptor, Some(new_value.as_ref()), local_precondition)
                    .await?;

                match response.status() {
                    status if status.is_success() => {
                        // Extract new ETag from response
                        let new_etag = response
                            .headers()
                            .get("etag")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.trim_matches('"').to_string())
                            .ok_or_else(|| {
                                S3StorageError::ServiceError(
                                    "Response missing ETag header".to_string(),
                                )
                            })?;
                        Ok(Some(new_etag))
                    }
                    reqwest::StatusCode::PRECONDITION_FAILED => Err(S3StorageError::ServiceError(
                        "CAS condition failed: edition mismatch".to_string(),
                    )),
                    status => Err(S3StorageError::ServiceError(format!(
                        "Failed to replace value: {}",
                        status
                    ))),
                }
            }
            None => {
                // DELETE requires edition (when) - delete with None is a no-op
                let Some(when) = when else {
                    return Ok(None);
                };

                let claim = MemoryClaim::retract(subject_did, &space, &cell, when);
                let descriptor = self
                    .credentials
                    .authorize(&claim)
                    .map_err(S3StorageError::from)?;

                let precondition = match edition {
                    Some(etag) => Precondition::IfMatch(etag.clone()),
                    None => Precondition::None,
                };

                let response = self.send_request(descriptor, None, precondition).await?;

                match response.status() {
                    status if status.is_success() => Ok(None),
                    reqwest::StatusCode::PRECONDITION_FAILED => Err(S3StorageError::ServiceError(
                        "CAS condition failed: edition mismatch".to_string(),
                    )),
                    status => Err(S3StorageError::ServiceError(format!(
                        "Failed to delete value: {}",
                        status
                    ))),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(all(feature = "helpers", feature = "integration-tests"))]
    use helpers::*;

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    fn test_address() -> Address {
        Address::new("https://s3.amazonaws.com", "us-east-1", "bucket")
    }

    fn test_credentials() -> Credentials {
        Credentials::public(test_address(), TEST_SUBJECT).unwrap()
    }

    #[dialog_common::test]
    fn it_encodes_path_without_prefix() {
        // Test path encoding for binary keys
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(test_credentials()).unwrap();

        let path = backend.encode_path(&[1, 2, 3]);
        assert_eq!(path, "!Ldp");
    }

    #[dialog_common::test]
    fn it_encodes_path_with_prefix() {
        // Path with prefix
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(test_credentials())
            .unwrap()
            .at("prefix");

        let path = backend.encode_path(&[1, 2, 3]);
        assert_eq!(path, "prefix/!Ldp");
    }

    #[dialog_common::test]
    fn it_builds_virtual_hosted_url() {
        // Virtual-hosted style: {bucket}.{endpoint}/{key}
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
        let authorizer = Public::new(address, TEST_SUBJECT).unwrap();

        // "my-key" is safe ASCII, so it stays as-is (not encoded)
        let url = authorizer.build_url("my-key").unwrap();
        assert_eq!(url.as_str(), "https://my-bucket.s3.amazonaws.com/my-key");
    }

    #[dialog_common::test]
    fn it_builds_path_style_url() {
        // Path-style: {endpoint}/{bucket}/{key}
        let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
        let authorizer = Public::new(address, TEST_SUBJECT).unwrap();
        // localhost defaults to path_style=true

        let url = authorizer.build_url("my-key").unwrap();
        assert_eq!(url.as_str(), "http://localhost:9000/bucket/my-key");
    }

    #[dialog_common::test]
    fn it_forces_path_style() {
        // Force path-style on a non-localhost endpoint
        let address = Address::new("https://custom-s3.example.com", "us-east-1", "bucket");
        let authorizer = Public::new(address, TEST_SUBJECT)
            .unwrap()
            .with_path_style(true);

        let url = authorizer.build_url("key").unwrap();
        assert_eq!(url.as_str(), "https://custom-s3.example.com/bucket/key");
    }

    #[dialog_common::test]
    fn it_forces_virtual_hosted_on_localhost() {
        // Force virtual-hosted on localhost (not typical, but supported)
        let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
        let authorizer = Public::new(address, TEST_SUBJECT)
            .unwrap()
            .with_path_style(false);

        let url = authorizer.build_url("key").unwrap();
        assert_eq!(url.as_str(), "http://bucket.localhost:9000/key");
    }

    #[dialog_common::test]
    fn it_builds_r2_url() {
        // R2 uses virtual-hosted style by default (non-localhost)
        let address = Address::new("https://abc123.r2.cloudflarestorage.com", "auto", "bucket");
        let authorizer = Public::new(address, TEST_SUBJECT).unwrap();

        let url = authorizer.build_url("my-key").unwrap();
        assert_eq!(
            url.as_str(),
            "https://bucket.abc123.r2.cloudflarestorage.com/my-key"
        );
    }

    #[dialog_common::test]
    fn it_nests_at_calls() {
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(test_credentials())
            .unwrap()
            .at("data")
            .at("v1");

        let path = backend.encode_path(b"key");
        assert_eq!(path, "data/v1/key");
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
    fn it_configures_bucket_with_hasher() {
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(test_credentials())
            .unwrap()
            .with_hasher(Hasher::Sha256);

        // Hasher should be set (we can't directly inspect it, but the backend should work)
        let path = backend.encode_path(b"key");
        assert_eq!(path, "key");
    }

    #[dialog_common::test]
    fn it_converts_errors_to_dialog_error() {
        let error = S3StorageError::TransportError("test".into());
        let dialog_error: DialogStorageError = error.into();
        assert!(dialog_error.to_string().contains("test"));
    }

    #[dialog_common::test]
    async fn it_sets_and_gets_values(env: PublicS3Address) -> anyhow::Result<()> {
        // Using public access for simplicity. Signed sessions are tested separately.
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("test");

        // Test data
        let key = b"test-key-1".to_vec();
        let value = b"test-value-1".to_vec();

        // Set the value
        backend.set(key.clone(), value.clone()).await?;

        // Get the value back
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_multiple_operations(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        // Set multiple values
        backend.set(b"key1".to_vec(), b"value1".to_vec()).await?;
        backend.set(b"key2".to_vec(), b"value2".to_vec()).await?;
        backend.set(b"key3".to_vec(), b"value3".to_vec()).await?;

        // Verify all values
        assert_eq!(
            backend.get(&b"key1".to_vec()).await?,
            Some(b"value1".to_vec())
        );
        assert_eq!(
            backend.get(&b"key2".to_vec()).await?,
            Some(b"value2".to_vec())
        );
        assert_eq!(
            backend.get(&b"key3".to_vec()).await?,
            Some(b"value3".to_vec())
        );

        // Test missing key
        assert_eq!(backend.get(&b"nonexistent".to_vec()).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_large_values(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        // Create a 100KB value
        let key = b"large-key".to_vec();
        let value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        // Set and retrieve
        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_values(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        let key = b"delete-test-key".to_vec();
        let value = b"delete-test-value".to_vec();

        // Set the value
        backend.set(key.clone(), value.clone()).await?;

        // Verify it exists
        assert_eq!(backend.get(&key).await?, Some(value));

        // Delete it
        backend.delete(&key).await?;

        // Verify it's gone
        assert_eq!(backend.get(&key).await?, None);

        // Delete non-existent key should still succeed (S3 behavior)
        backend.delete(&key).await?;

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[dialog_common::test]
    async fn it_lists_objects(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("list-test");

        // Set multiple values
        backend.set(b"key1".to_vec(), b"value1".to_vec()).await?;
        backend.set(b"key2".to_vec(), b"value2".to_vec()).await?;
        backend.set(b"key3".to_vec(), b"value3".to_vec()).await?;

        // List objects
        let result = backend.list(None).await?;

        assert_eq!(result.keys.len(), 3);
        assert!(!result.is_truncated);

        // All keys should have the prefix
        for key in &result.keys {
            assert!(key.starts_with("list-test/"));
        }

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[dialog_common::test]
    async fn it_lists_empty_for_nonexistent_prefix(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?
            .at("nonexistent-prefix-that-does-not-exist");

        // List objects with a prefix that has no objects - should return empty list
        let result = backend.list(None).await?;

        assert!(result.keys.is_empty());
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[dialog_common::test]
    async fn it_errors_on_nonexistent_bucket(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", "bucket-that-does-not-exist");
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        // S3 returns 404 NoSuchBucket error when listing a non-existent bucket.
        // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_Errors
        let result = backend.list(None).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, S3StorageError::ServiceError(ref msg) if msg.contains("NoSuchBucket")),
            "Expected NoSuchBucket error for non-existent bucket, got: {:?}",
            err
        );

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[dialog_common::test]
    async fn it_reads_stream(env: PublicS3Address) -> anyhow::Result<()> {
        use futures_util::TryStreamExt;

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("stream-test");

        // Set multiple values
        backend.set(b"a".to_vec(), b"value-a".to_vec()).await?;
        backend.set(b"b".to_vec(), b"value-b".to_vec()).await?;

        // Read all items via StorageSource
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

    #[dialog_common::test]
    async fn it_returns_none_for_missing_values(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?;

        // Try to get a key that doesn't exist
        let key = b"nonexistent-key".to_vec();
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_performs_bulk_writes(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("bulk-test");

        // Create a source stream with multiple items
        use async_stream::try_stream;

        let source_stream = try_stream! {
            yield (vec![1, 2, 3], vec![4, 5, 6]);
            yield (vec![4, 5, 6, 7], vec![8, 9, 10]);
            yield (vec![7, 8, 9], vec![10, 11, 12]);
        };

        // Perform the bulk write
        backend.write(source_stream).await?;

        // Verify all items were written
        assert_eq!(backend.get(&vec![1, 2, 3]).await?, Some(vec![4, 5, 6]));
        assert_eq!(backend.get(&vec![4, 5, 6, 7]).await?, Some(vec![8, 9, 10]));
        assert_eq!(backend.get(&vec![7, 8, 9]).await?, Some(vec![10, 11, 12]));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_integrates_with_memory_backend(env: PublicS3Address) -> anyhow::Result<()> {
        use crate::StorageSource;
        use futures_util::StreamExt;

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let authorizer = Credentials::public(address, "did:key:test")?.with_path_style(true);
        let mut s3_backend =
            Bucket::<Vec<u8>, Vec<u8>, _>::open(authorizer)?.at("memory-integration");

        // Create a memory backend with some data
        let mut memory_backend = crate::MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();

        // Add some data to the memory backend
        memory_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        memory_backend.set(vec![4, 5, 6, 7], vec![8, 9, 10]).await?;

        // Transfer data from memory backend to S3 backend using drain()
        // Map DialogStorageError to S3StorageError for type compatibility
        let source_stream = memory_backend
            .drain()
            .map(|result| result.map_err(|e| S3StorageError::ServiceError(e.to_string())));
        s3_backend.write(source_stream).await?;

        // Verify all items were transferred to S3
        assert_eq!(s3_backend.get(&vec![1, 2, 3]).await?, Some(vec![4, 5, 6]));
        assert_eq!(
            s3_backend.get(&vec![4, 5, 6, 7]).await?,
            Some(vec![8, 9, 10])
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_uses_prefix(env: PublicS3Address) -> anyhow::Result<()> {
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

        // Each backend should see its own value
        assert_eq!(backend1.get(&key).await?, Some(b"value-a".to_vec()));
        assert_eq!(backend2.get(&key).await?, Some(b"value-b".to_vec()));

        Ok(())
    }

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

    // UCAN-authorized S3 access tests
    #[cfg(feature = "ucan")]
    mod ucan_tests {
        use super::*;
        use dialog_s3_credentials::ucan::{
            Credentials as UcanCredentials, DelegationChain, OperatorIdentity, generate_signer,
        };
        use ucan::delegation::builder::DelegationBuilder;
        use ucan::delegation::subject::DelegatedSubject;
        use ucan::did::Ed25519Did;

        /// Create a test delegation from space to operator for storage commands.
        fn create_storage_delegation(
            space_signer: &ucan::did::Ed25519Signer,
            operator_did: &Ed25519Did,
        ) -> ucan::Delegation<Ed25519Did> {
            DelegationBuilder::new()
                .issuer(space_signer.clone())
                .audience(operator_did.clone())
                .subject(DelegatedSubject::Specific(*space_signer.did()))
                .command(vec!["storage".to_string()])
                .try_build()
                .expect("Failed to build delegation")
        }

        #[dialog_common::test]
        async fn it_sets_and_gets_with_ucan(env: UcanS3Address) -> anyhow::Result<()> {
            // 1. Generate keypairs
            let space_signer = generate_signer();
            let space_did = space_signer.did();

            let operator_identity = OperatorIdentity::generate();
            let operator_did: Ed25519Did = operator_identity
                .did()
                .to_string()
                .parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse DID: {:?}", e))?;

            // 2. Create delegation
            let delegation = create_storage_delegation(&space_signer, &operator_did);
            let delegation_chain = DelegationChain::new(vec![delegation]);

            // 3. The UCAN subject is the space DID (must be valid DID, not a path)
            let subject = space_did.to_string();

            // 4. Create UCAN credentials - delegation keyed by DID
            let credentials = UcanCredentials::builder()
                .service_url(&env.access_service_url)
                .operator(operator_identity)
                .delegation(&subject, delegation_chain)
                .build()?;

            // 5. Open bucket - store name is the DID
            let bucket: Bucket<Vec<u8>, Vec<u8>, _> = Bucket::open(credentials)?;
            let backend = bucket.at(&subject);

            // 6. Test set and get - key can include sub-paths
            let key = b"index/test-key".to_vec();
            let value = b"test-value".to_vec();

            backend.clone().set(key.clone(), value.clone()).await?;
            let retrieved = backend.get(&key).await?;
            assert_eq!(retrieved, Some(value));

            Ok(())
        }

        #[dialog_common::test]
        async fn it_deletes_with_ucan(env: UcanS3Address) -> anyhow::Result<()> {
            // 1. Generate keypairs
            let space_signer = generate_signer();
            let space_did = space_signer.did();

            let operator_identity = OperatorIdentity::generate();
            let operator_did: Ed25519Did = operator_identity
                .did()
                .to_string()
                .parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse DID: {:?}", e))?;

            // 2. Create delegation
            let delegation = create_storage_delegation(&space_signer, &operator_did);
            let delegation_chain = DelegationChain::new(vec![delegation]);

            // 3. Subject is the DID
            let subject = space_did.to_string();

            // 4. Create UCAN credentials
            let credentials = UcanCredentials::builder()
                .service_url(&env.access_service_url)
                .operator(operator_identity)
                .delegation(&subject, delegation_chain)
                .build()?;

            // 5. Open bucket
            let bucket: Bucket<Vec<u8>, Vec<u8>, _> = Bucket::open(credentials)?;
            let mut backend = bucket.at(&subject);

            // 6. Set, verify, delete, verify
            let key = b"index/delete-test-key".to_vec();
            let value = b"delete-test-value".to_vec();

            backend.set(key.clone(), value.clone()).await?;
            assert_eq!(backend.get(&key).await?, Some(value));

            backend.delete(&key).await?;
            assert_eq!(backend.get(&key).await?, None);

            Ok(())
        }

        #[dialog_common::test]
        async fn it_returns_none_for_nonexistent_key_with_ucan(
            env: UcanS3Address,
        ) -> anyhow::Result<()> {
            // 1. Generate keypairs
            let space_signer = generate_signer();
            let space_did = space_signer.did();

            let operator_identity = OperatorIdentity::generate();
            let operator_did: Ed25519Did = operator_identity
                .did()
                .to_string()
                .parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse DID: {:?}", e))?;

            // 2. Create delegation
            let delegation = create_storage_delegation(&space_signer, &operator_did);
            let delegation_chain = DelegationChain::new(vec![delegation]);

            // 3. Subject is the DID
            let subject = space_did.to_string();

            // 4. Create UCAN credentials
            let credentials = UcanCredentials::builder()
                .service_url(&env.access_service_url)
                .operator(operator_identity)
                .delegation(&subject, delegation_chain)
                .build()?;

            // 5. Open bucket
            let bucket: Bucket<Vec<u8>, Vec<u8>, _> = Bucket::open(credentials)?;
            let backend = bucket.at(&subject);

            // 6. Get nonexistent key
            let key = b"index/nonexistent-key".to_vec();
            let result = backend.get(&key).await?;
            assert_eq!(result, None);

            Ok(())
        }
    }
}
