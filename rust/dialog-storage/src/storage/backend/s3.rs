//! S3-compatible storage backend for AWS S3, Cloudflare R2, and other S3-compatible services.
//!
//! This module provides [`Bucket`] providing [`StorageBackend`] implementation
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
//! use dialog_storage::s3::{Address, Bucket};
//! use dialog_storage::StorageBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create address with endpoint, region, and bucket
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//! let bucket = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
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
//! let credentials = Credentials {
//!     access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
//!     secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
//! };
//!
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//! let bucket = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?;
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
//! let credentials = Credentials {
//!     access_key_id: std::env::var("R2_ACCESS_KEY_ID")?,
//!     secret_access_key: std::env::var("R2_SECRET_ACCESS_KEY")?,
//! };
//!
//! // R2 uses "auto" region for signing
//! let address = Address::new(
//!     "https://account-id.r2.cloudflarestorage.com",
//!     "auto",
//!     "my-bucket",
//! );
//! let bucket = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?;
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
//! let credentials = Credentials {
//!     access_key_id: "minioadmin".into(),
//!     secret_access_key: "minioadmin".into(),
//! };
//!
//! // IP addresses and localhost automatically use path-style URLs
//! let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
//! let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?;
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

use std::marker::PhantomData;

#[cfg(feature = "s3-list")]
use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use futures_util::{Stream, StreamExt, TryStreamExt};
use thiserror::Error;

// Re-export core presigning types from s3-presign crate
pub use s3_presign::{
    Acl, Address, Authorization, AuthorizationError, Checksum, Credentials, DEFAULT_EXPIRES,
    Hasher, Invocation, Public, unauthorized,
};

mod key;
pub use key::{decode as decode_s3_key, encode as encode_s3_key};

mod request;
pub use request::{Delete, Get, Precondition, Put, Request};

#[cfg(feature = "s3-list")]
mod list;
#[cfg(feature = "s3-list")]
pub use list::{List, ListResult};

#[cfg(feature = "s3-list")]
use crate::StorageSource;
use crate::{DialogStorageError, StorageBackend, StorageSink, TransactionalMemoryBackend};

// Testing helpers module:
// - Address types (S3Address, PublicS3Address) are available on all platforms
// - Server implementation is native-only (internal to the helpers module)
#[cfg(any(feature = "helpers", test))]
pub mod helpers;
#[cfg(all(feature = "helpers", not(target_arch = "wasm32")))]
pub use helpers::{
    LocalS3, PublicS3Address, PublicS3Settings, S3Address, S3Settings, start, start_public,
};

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

/// S3/R2-compatible storage backend.
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
/// let mut storage = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
///
/// // With credentials and prefix
/// let credentials = Credentials {
///     access_key_id: "...".into(),
///     secret_access_key: "...".into(),
/// };
/// let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
/// let mut storage = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?
///     .at("data");  // Scope to a prefix/directory within the bucket
///
/// storage.set(b"key".to_vec(), b"value".to_vec()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Bucket<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    /// Parsed endpoint URL
    endpoint: url::Url,
    /// AWS region for signing
    region: String,
    /// Bucket name
    bucket: String,
    /// Optional prefix/directory within the bucket
    path: Option<String>,
    /// Use path-style URLs (bucket in path) vs virtual-hosted (bucket in subdomain)
    /// Defaults to true for IP addresses and localhost, false otherwise.
    path_style: bool,
    /// Credentials for authorizing requests (None for public access)
    credentials: Option<Credentials>,
    /// Hasher for computing checksums
    hasher: Hasher,
    /// HTTP client
    pub(crate) client: reqwest::Client,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
}

impl<Key, Value> Bucket<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    /// Open an S3 storage bucket.
    ///
    /// Pass `None` for credentials to use public/unsigned access.
    /// By default uses SHA-256 for checksums. Use [`with_hasher`](Self::with_hasher)
    /// to configure a different algorithm.
    ///
    /// Path-style URLs are automatically enabled for IP addresses and localhost.
    /// Use [`with_path_style`](Self::with_path_style) to override this behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL is invalid.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dialog_storage::s3::{Bucket, Address, Credentials};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // AWS S3
    /// let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
    /// let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
    ///
    /// // Cloudflare R2 (uses "auto" region)
    /// let address = Address::new("https://account.r2.cloudflarestorage.com", "auto", "my-bucket");
    /// let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
    ///
    /// // Local MinIO (path_style is auto-enabled for localhost)
    /// let credentials = Credentials {
    ///     access_key_id: "minioadmin".into(),
    ///     secret_access_key: "minioadmin".into(),
    /// };
    /// let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
    /// let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(
        address: Address,
        credentials: Option<Credentials>,
    ) -> Result<Self, S3StorageError> {
        let endpoint = url::Url::parse(address.endpoint())
            .map_err(|e| S3StorageError::ServiceError(format!("Invalid endpoint URL: {}", e)))?;

        let path_style = Self::is_path_style_default(&endpoint);

        Ok(Self {
            endpoint,
            region: address.region().to_string(),
            bucket: address.bucket().to_string(),
            path: None,
            path_style,
            credentials,
            hasher: Hasher::Sha256,
            client: reqwest::Client::new(),
            key_type: PhantomData,
            value_type: PhantomData,
        })
    }

    /// Returns true if path-style URLs should be used by default for this endpoint.
    ///
    /// Returns true for IP addresses and localhost, since virtual-hosted style
    /// URLs require DNS resolution of `{bucket}.{host}`.
    fn is_path_style_default(endpoint: &url::Url) -> bool {
        use url::Host;
        match endpoint.host() {
            Some(Host::Ipv4(_)) | Some(Host::Ipv6(_)) => true,
            Some(Host::Domain(domain)) => domain == "localhost",
            None => false,
        }
    }

    /// Returns a new `Bucket` scoped to the given prefix/directory.
    ///
    /// All keys will be resolved from this path. Can be chained to create
    /// nested paths.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dialog_storage::s3::{Bucket, Address};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let address = Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
    /// let bucket = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?;
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
            endpoint: self.endpoint.clone(),
            region: self.region.clone(),
            bucket: self.bucket.clone(),
            path: prefix,
            path_style: self.path_style,
            credentials: self.credentials.clone(),
            hasher: self.hasher,
            client: self.client.clone(),
            key_type: PhantomData,
            value_type: PhantomData,
        }
    }

    /// Override the path-style URL setting.
    ///
    /// - `true`: Use path-style URLs (`https://endpoint/bucket/key`)
    /// - `false`: Use virtual-hosted style URLs (`https://bucket.endpoint/key`)
    ///
    /// By default, path-style is enabled for IP addresses and localhost.
    /// Most S3-compatible services (AWS S3, R2, Wasabi) support virtual-hosted style.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dialog_storage::s3::{Bucket, Address};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Force path-style for a custom endpoint that doesn't support virtual-hosted
    /// let address = Address::new("https://custom-s3.example.com", "us-east-1", "my-bucket");
    /// let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
    ///     .with_path_style(true);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = path_style;
        self
    }

    /// Get the prefix/directory path, if any.
    pub fn prefix(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// Get the region for signing.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Set the hasher for computing checksums.
    pub fn with_hasher(mut self, hasher: Hasher) -> Self {
        self.hasher = hasher;
        self
    }

    /// Resolve a key to its full S3 URL.
    ///
    /// This handles:
    /// - Key encoding (base58 for binary/unsafe characters)
    /// - Prefix prepending
    /// - Path-style vs virtual-hosted style URL construction
    pub fn resolve(&self, key: &[u8]) -> Result<url::Url, S3StorageError> {
        let encoded_key = encode_s3_key(key);
        let full_key = match &self.path {
            Some(prefix) => format!("{}/{}", prefix, encoded_key),
            None => encoded_key,
        };

        self.build_url(&full_key)
    }

    /// Build the base URL for the bucket (used for listing operations).
    pub fn base_url(&self) -> Result<url::Url, S3StorageError> {
        self.build_url("")
    }

    /// Build a URL for the given path within the bucket.
    fn build_url(&self, path: &str) -> Result<url::Url, S3StorageError> {
        let url = if self.path_style {
            // Path-style: https://endpoint/bucket/path
            let mut url = self.endpoint.clone();
            let new_path = if path.is_empty() {
                format!("{}/", self.bucket)
            } else {
                format!("{}/{}", self.bucket, path)
            };
            url.set_path(&new_path);
            url
        } else {
            // Virtual-hosted style: https://bucket.endpoint/path
            let host = self
                .endpoint
                .host_str()
                .ok_or_else(|| S3StorageError::ServiceError("Invalid endpoint: no host".into()))?;
            let new_host = format!("{}.{}", self.bucket, host);

            let mut url = self.endpoint.clone();
            url.set_host(Some(&new_host))
                .map_err(|e| S3StorageError::ServiceError(format!("Invalid host: {}", e)))?;

            let new_path = if path.is_empty() { "/" } else { path };
            url.set_path(new_path);
            url
        };

        Ok(url)
    }

    /// Delete an object from S3.
    ///
    /// Note: S3 DELETE always returns 204 No Content, even if the object didn't exist.
    /// This method always returns `Ok(())` on success.
    pub async fn delete(&mut self, key: &Key) -> Result<(), S3StorageError> {
        let url = self.resolve(key.as_ref())?;
        let response = Delete::new(url, self.region()).perform(self).await?;

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
impl<Key, Value> StorageBackend for Bucket<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    type Key = Key;
    type Value = Value;
    type Error = S3StorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let url = self.resolve(key.as_ref())?;
        let response = Put::new(url, value.as_ref(), self.region())
            .with_checksum(&self.hasher)
            .perform(self)
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
        let url = self.resolve(key.as_ref())?;
        let response = Get::new(url, self.region()).perform(self).await?;

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
/// Modern mainstream browsers typically enforce a limit of 6 concurren
/// requests on HTTP/1.1 which is what S3 is.
const MAX_CONCURRENT_WRITES: usize = 6;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageSink for Bucket<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
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
impl<Key, Value> StorageSource for Bucket<Key, Value>
where
    Key: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    fn read(&self) -> impl Stream<Item = Result<(Self::Key, Self::Value), Self::Error>> {
        let storage = self.clone();
        let prefix = self.prefix().map(String::from);

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
impl<Key, Value> TransactionalMemoryBackend for Bucket<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    type Address = Key;
    type Value = Value;
    type Error = S3StorageError;
    type Edition = String;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let url = self.resolve(address.as_ref())?;
        let response = Get::new(url, self.region()).perform(self).await?;

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
        match content {
            Some(new_value) => {
                let url = self.resolve(address.as_ref())?;

                // Build precondition
                let precondition = match edition {
                    Some(etag) => Precondition::IfMatch(etag.clone()),
                    None => Precondition::IfNoneMatch,
                };

                let response = Put::new(url, new_value.as_ref(), self.region())
                    .with_checksum(&self.hasher)
                    .with_precondition(precondition)
                    .perform(self)
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
                // DELETE with If-Match header for CAS
                let url = self.resolve(address.as_ref())?;
                let mut delete_request = Delete::new(url, self.region());

                if let Some(etag) = edition {
                    delete_request = delete_request.with_if_match(etag.clone());
                }

                let response = delete_request.perform(self).await?;

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
    use url::Url;

    #[test]
    fn it_builds_virtual_hosted_url_without_prefix() {
        // Virtual-hosted style: {bucket}.{endpoint}/{key}
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None).unwrap();

        let url = backend.resolve(&[1, 2, 3]).unwrap();
        assert_eq!(url.as_str(), "https://bucket.s3.amazonaws.com/!Ldp");
    }

    #[test]
    fn it_builds_virtual_hosted_url_with_prefix() {
        // Virtual-hosted style with prefix: {bucket}.{endpoint}/{prefix}/{key}
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)
            .unwrap()
            .at("prefix");

        let url = backend.resolve(&[1, 2, 3]).unwrap();
        assert_eq!(url.as_str(), "https://bucket.s3.amazonaws.com/prefix/!Ldp");
    }

    #[test]
    fn it_builds_virtual_hosted_url_with_key() {
        // Virtual-hosted style with text key
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None).unwrap();

        // "my-key" is safe ASCII, so it stays as-is (not encoded)
        let url = backend.resolve(b"my-key").unwrap();
        assert_eq!(url.as_str(), "https://my-bucket.s3.amazonaws.com/my-key");
    }

    #[test]
    fn it_builds_path_style_url() {
        // Path-style: {endpoint}/{bucket}/{key}
        let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None).unwrap();
        // localhost defaults to path_style=true

        let url = backend.resolve(b"my-key").unwrap();
        assert_eq!(url.as_str(), "http://localhost:9000/bucket/my-key");
    }

    #[test]
    fn it_builds_path_style_url_with_prefix() {
        // Path-style with prefix: {endpoint}/{bucket}/{prefix}/{key}
        let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)
            .unwrap()
            .at("prefix");

        let url = backend.resolve(b"my-key").unwrap();
        assert_eq!(url.as_str(), "http://localhost:9000/bucket/prefix/my-key");
    }

    #[test]
    fn it_forces_path_style() {
        // Force path-style on a non-localhost endpoint
        let address = Address::new("https://custom-s3.example.com", "us-east-1", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)
            .unwrap()
            .with_path_style(true);

        let url = backend.resolve(b"key").unwrap();
        assert_eq!(url.as_str(), "https://custom-s3.example.com/bucket/key");
    }

    #[test]
    fn it_forces_virtual_hosted_on_localhost() {
        // Force virtual-hosted on localhost (not typical, but supported)
        let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)
            .unwrap()
            .with_path_style(false);

        let url = backend.resolve(b"key").unwrap();
        assert_eq!(url.as_str(), "http://bucket.localhost:9000/key");
    }

    #[test]
    fn it_builds_r2_url() {
        // R2 uses virtual-hosted style by default (non-localhost)
        let address = Address::new("https://abc123.r2.cloudflarestorage.com", "auto", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None).unwrap();

        let url = backend.resolve(b"my-key").unwrap();
        assert_eq!(
            url.as_str(),
            "https://bucket.abc123.r2.cloudflarestorage.com/my-key"
        );
    }

    #[test]
    fn it_nests_open_calls() {
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)
            .unwrap()
            .at("data")
            .at("v1");

        let url = backend.resolve(b"key").unwrap();
        assert_eq!(url.as_str(), "https://bucket.s3.amazonaws.com/data/v1/key");
    }

    #[test]
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

    #[test]
    fn it_detects_path_style_default() {
        // localhost should use path-style (auto-detected by Bucket::open)
        let localhost = Url::parse("http://localhost:9000").unwrap();
        assert!(Bucket::<Vec<u8>, Vec<u8>>::is_path_style_default(
            &localhost
        ));

        // IPv4 addresses should use path-style
        let ipv4 = Url::parse("http://127.0.0.1:9000").unwrap();
        assert!(Bucket::<Vec<u8>, Vec<u8>>::is_path_style_default(&ipv4));

        let ipv4_other = Url::parse("http://192.168.1.100:9000").unwrap();
        assert!(Bucket::<Vec<u8>, Vec<u8>>::is_path_style_default(
            &ipv4_other
        ));

        // Remote domains should use virtual-hosted
        let remote = Url::parse("https://s3.amazonaws.com").unwrap();
        assert!(!Bucket::<Vec<u8>, Vec<u8>>::is_path_style_default(&remote));
    }

    #[test]
    fn it_generates_signed_urls() {
        let credentials = Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
        };

        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials.clone())).unwrap();

        // Create a PUT request for a key
        let url = backend.resolve(b"test-key").unwrap();
        let request = Put::new(url, b"test-value", "us-east-1");

        // The credentials should be able to sign it
        let authorized = credentials.authorize(&request).unwrap();

        // Should have signing parameters
        assert!(
            authorized
                .url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(authorized.url.as_str().contains("X-Amz-Signature="));
    }

    #[test]
    fn it_authorizes_public_request() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url.clone(), b"test", "us-east-1").with_checksum(&Hasher::Sha256);

        let authorization = unauthorized(&request).unwrap();

        // Public request should not modify the URL
        assert_eq!(authorization.url.path(), url.path());
        assert!(authorization.url.query().is_none());

        // Should have host header
        assert!(authorization.headers.iter().any(|(k, _)| k == "host"));

        // Should have checksum header
        assert!(
            authorization
                .headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[test]
    fn it_configures_bucket_with_hasher() {
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "bucket");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)
            .unwrap()
            .with_hasher(Hasher::Sha256);

        // Hasher should be set (we can't directly inspect it, but the backend should work)
        let url = backend.resolve(b"key").unwrap();
        assert!(url.as_str().contains("bucket"));
    }

    #[test]
    fn it_converts_errors_to_dialog_error() {
        let error = S3StorageError::TransportError("test".into());
        let dialog_error: DialogStorageError = error.into();
        assert!(dialog_error.to_string().contains("test"));
    }
}

// Integration tests that require the test harness (dialog_common::test provider).
// These tests are gated behind the s3-integration-tests feature and require the
// test harness to be ported from the transactional-memory branch.
#[cfg(all(test, feature = "s3-integration-tests"))]
mod integration_tests {
    use super::*;
    use helpers::{PublicS3Address, S3Address};

    #[ignore]
    #[tokio::test]
    async fn it_sets_and_gets_values(env: PublicS3Address) -> anyhow::Result<()> {
        // Using public access for simplicity. Signed sessions are tested separately.
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
            .with_path_style(true)
            .at("test");

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

    #[tokio::test]
    async fn it_performs_multiple_operations(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

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

    #[tokio::test]
    async fn it_handles_large_values(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

        // Create a 100KB value
        let key = b"large-key".to_vec();
        let value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        // Set and retrieve
        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[tokio::test]
    async fn it_deletes_values(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

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
    #[tokio::test]
    async fn it_lists_objects(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
            .with_path_style(true)
            .at("list-test");

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
    #[tokio::test]
    async fn it_lists_empty_for_nonexistent_prefix(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
            .with_path_style(true)
            .at("nonexistent-prefix-that-does-not-exist");

        // List objects with a prefix that has no objects - should return empty list
        let result = backend.list(None).await?;

        assert!(result.keys.is_empty());
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[tokio::test]
    async fn it_errors_on_nonexistent_bucket(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", "bucket-that-does-not-exist");
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

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
    #[tokio::test]
    async fn it_reads_stream(env: PublicS3Address) -> anyhow::Result<()> {
        use futures_util::TryStreamExt;

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
            .with_path_style(true)
            .at("stream-test");

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

    #[tokio::test]
    async fn it_returns_none_for_missing_values(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

        // Try to get a key that doesn't exist
        let key = b"nonexistent-key".to_vec();
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, None);

        Ok(())
    }

    #[tokio::test]
    async fn it_performs_bulk_writes(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
            .with_path_style(true)
            .at("bulk-test");

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

    #[tokio::test]
    async fn it_integrates_with_memory_backend(env: PublicS3Address) -> anyhow::Result<()> {
        use crate::StorageSource;
        use futures_util::StreamExt;

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut s3_backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
            .with_path_style(true)
            .at("memory-integration");

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

    #[tokio::test]
    async fn it_uses_prefix(env: PublicS3Address) -> anyhow::Result<()> {
        // Create two backends with different prefixes
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let bucket = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);
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
    #[tokio::test]
    async fn it_uses_prefix_for_listing(env: PublicS3Address) -> anyhow::Result<()> {
        // Create two backends with different prefixes
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let bucket = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);
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

    #[tokio::test]
    async fn it_overwrites_value(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

        let key = b"overwrite-key".to_vec();

        // Set initial value
        backend.set(key.clone(), b"initial".to_vec()).await?;
        assert_eq!(backend.get(&key).await?, Some(b"initial".to_vec()));

        // Overwrite with new value
        backend.set(key.clone(), b"updated".to_vec()).await?;
        assert_eq!(backend.get(&key).await?, Some(b"updated".to_vec()));

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_binary_keys(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

        // Binary key with non-UTF8 bytes
        let key = vec![0x00, 0xFF, 0x80, 0x7F];
        let value = b"binary-key-value".to_vec();

        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_path_like_keys(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

        // Path-like key with slashes
        let key = b"path/to/nested/key".to_vec();
        let value = b"nested-value".to_vec();

        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_encoded_key_segments(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

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

    #[tokio::test]
    async fn it_handles_multi_segment_mixed_encoding(env: PublicS3Address) -> anyhow::Result<()> {
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

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

    #[test]
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

    #[tokio::test]
    async fn it_works_with_signed_session(env: S3Address) -> anyhow::Result<()> {
        // Create credentials matching the test server
        let credentials = Credentials {
            access_key_id: env.access_key_id.clone(),
            secret_access_key: env.secret_access_key.clone(),
        };

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?
            .with_path_style(true)
            .at("signed-test");

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

    #[tokio::test]
    async fn it_fails_with_wrong_secret_key(env: S3Address) -> anyhow::Result<()> {
        // Create credentials with WRONG secret key
        let credentials = Credentials {
            access_key_id: env.access_key_id.clone(),
            secret_access_key: "wrong-secret".into(),
        };

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend =
            Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?.with_path_style(true);

        // Attempt to set a value - should fail due to signature mismatch
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure with wrong secret key"
        );

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_with_wrong_access_key(env: S3Address) -> anyhow::Result<()> {
        // Create credentials with WRONG access key
        let credentials = Credentials {
            access_key_id: "wrong-access-key".into(),
            secret_access_key: env.secret_access_key.clone(),
        };

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend =
            Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?.with_path_style(true);

        // Attempt to set a value - should fail due to unknown access key
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure with wrong access key"
        );

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_unsigned_request_to_auth_server(env: S3Address) -> anyhow::Result<()> {
        // Client uses no credentials but server requires authentication
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?.with_path_style(true);

        // Attempt to set a value - should fail because server expects signed requests
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure when sending unsigned request to authenticated server"
        );

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_get_with_wrong_credentials(env: S3Address) -> anyhow::Result<()> {
        // First, set a value with correct credentials
        let correct_credentials = Credentials {
            access_key_id: env.access_key_id.clone(),
            secret_access_key: env.secret_access_key.clone(),
        };
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let bucket = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(correct_credentials))?
            .with_path_style(true);
        let mut correct_backend = bucket.clone();

        correct_backend
            .set(b"protected-key".to_vec(), b"secret-value".to_vec())
            .await?;

        // Now try to GET with wrong credentials
        let wrong_credentials = Credentials {
            access_key_id: env.access_key_id.clone(),
            secret_access_key: "wrong-secret".into(),
        };
        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let wrong_backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(wrong_credentials))?
            .with_path_style(true);

        // Attempt to get the value - should fail
        let result = wrong_backend.get(&b"protected-key".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure when getting with wrong credentials"
        );

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[tokio::test]
    async fn it_lists_with_signed_session(env: S3Address) -> anyhow::Result<()> {
        // Create credentials matching the test server
        let credentials = Credentials {
            access_key_id: env.access_key_id.clone(),
            secret_access_key: env.secret_access_key.clone(),
        };

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?
            .with_path_style(true)
            .at("signed-list-test");

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
    #[tokio::test]
    async fn it_reads_stream_with_signed_session(env: S3Address) -> anyhow::Result<()> {
        use futures_util::TryStreamExt;

        let credentials = Credentials {
            access_key_id: env.access_key_id.clone(),
            secret_access_key: env.secret_access_key.clone(),
        };

        let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
        let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?
            .with_path_style(true)
            .at("signed-stream-test");

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
}
