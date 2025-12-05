//! S3-compatible storage backend for AWS S3, Cloudflare R2, and other S3-compatible services.
//!
//! This module provides an [`S3`] storage backend that implements [`StorageBackend`],
//! allowing you to use S3-compatible object storage as a key-value store.
//!
//! # Features
//!
//! - AWS SigV4 presigned URL signing for authorization
//! - Support for public (unsigned) and authenticated access
//! - Automatic key encoding to handle binary and special characters
//! - Checksum verification using SHA-256
//! - Compatible with S3-compatible services
//!
//! # Examples
//!
//! ## Public Access (No Authentication)
//!
//! For publicly accessible buckets that don't require authentication:
//!
//! ```no_run
//! use dialog_storage::s3::{S3, Session};
//! use dialog_storage::StorageBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut backend = S3::<Vec<u8>, Vec<u8>>::open(
//!     "https://s3.amazonaws.com",
//!     "my-bucket",
//!     Session::Public
//! ).with_prefix("data");
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
//! use dialog_storage::s3::{S3, Credentials, Service, Session};
//! use dialog_storage::StorageBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let credentials = Credentials {
//!     access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
//!     secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
//!     session_token: None,
//! };
//!
//! let service = Service::s3("us-east-1");
//! let session = Session::new(&credentials, &service, 3600); // 1 hour expiry
//!
//! let mut backend = S3::<Vec<u8>, Vec<u8>>::open(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "my-bucket",
//!     session
//! ).with_prefix("data");
//!
//! backend.set(b"key".to_vec(), b"value".to_vec()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Cloudflare R2
//!
//! ```no_run
//! use dialog_storage::s3::{S3, Credentials, Service, Session};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let credentials = Credentials {
//!     access_key_id: std::env::var("R2_ACCESS_KEY_ID")?,
//!     secret_access_key: std::env::var("R2_SECRET_ACCESS_KEY")?,
//!     session_token: None,
//! };
//!
//! let service = Service::s3("auto"); // Use R2 "auto" region
//! let session = Session::new(&credentials, &service, 3600);
//!
//! let backend = S3::<Vec<u8>, Vec<u8>>::open(
//!     "https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com",
//!     "my-bucket",
//!     session
//! ).with_prefix("data");
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
use base58::{FromBase58, ToBase58};
use dialog_common::{ConditionalSend, ConditionalSync};
use futures_util::{Stream, StreamExt, TryStreamExt};
use reqwest;
use thiserror::Error;
use url::Url;

mod access;
pub use access::{Acl, Credentials, Invocation, Service, Session, SigningError};

mod checksum;
pub use checksum::{Checksum, Hasher};

#[cfg(feature = "s3-list")]
mod list;
#[cfg(feature = "s3-list")]
pub use list::{List, ListResult};

#[cfg(feature = "s3-list")]
use crate::StorageSource;
use crate::{DialogStorageError, StorageBackend, StorageSink};

/// A PUT request to upload data.
#[derive(Debug)]
pub struct Put {
    url: Url,
    body: Vec<u8>,
    checksum: Option<Checksum>,
    acl: Option<Acl>,
}

impl Put {
    /// Create a new PUT request with the given URL and body.
    ///
    /// Use [`with_checksum`](Self::with_checksum) to add integrity verification.
    pub fn new(url: Url, body: impl AsRef<[u8]>) -> Self {
        Self {
            url,
            body: body.as_ref().to_vec(),
            checksum: None,
            acl: None,
        }
    }

    /// Compute and set the checksum using the given hasher.
    pub fn with_checksum(mut self, hasher: &Hasher) -> Self {
        self.checksum = Some(hasher.checksum(&self.body));
        self
    }

    /// Set the ACL for this request.
    pub fn with_acl(mut self, acl: Acl) -> Self {
        self.acl = Some(acl);
        self
    }
}

impl Invocation for Put {
    fn method(&self) -> &'static str {
        "PUT"
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn checksum(&self) -> Option<&Checksum> {
        self.checksum.as_ref()
    }

    fn acl(&self) -> Option<Acl> {
        self.acl
    }
}
impl Request for Put {
    fn body(&self) -> Option<&[u8]> {
        Some(&self.body)
    }
}

/// A GET request to retrieve data.
#[derive(Debug, Clone)]
pub struct Get {
    url: Url,
}

impl Get {
    /// Create a new GET request for the given URL.
    pub fn new(url: Url) -> Self {
        Self { url }
    }
}

impl Invocation for Get {
    fn method(&self) -> &'static str {
        "GET"
    }

    fn url(&self) -> &Url {
        &self.url
    }
}
impl Request for Get {}

/// A DELETE request to remove an object.
#[derive(Debug, Clone)]
pub struct Delete {
    url: Url,
}

impl Delete {
    /// Create a new DELETE request for the given URL.
    pub fn new(url: Url) -> Self {
        Self { url }
    }
}

impl Invocation for Delete {
    fn method(&self) -> &'static str {
        "DELETE"
    }

    fn url(&self) -> &Url {
        &self.url
    }
}
impl Request for Delete {}

/// S3-safe key encoding that preserves path structure.
///
/// Keys are treated as `/`-delimited paths. Each path component is checked:
/// - If it contains only safe characters (alphanumeric, `-`, `_`, `.`), it's kept as-is
/// - Otherwise, it's base58-encoded and prefixed with `!`
///
/// The `!` character is used as a prefix marker because it's in AWS S3's
/// "safe for use" list and unlikely to appear at the start of path components.
///
/// See [Object key naming guidelines] for more information about S3 key requirements.
///
/// # Examples
///
/// - `remote/main` → `remote/main` (all components safe)
/// - `remote/user@example` → `remote/!<base58>` (@ is unsafe, encode component)
/// - `foo/bar/baz` → `foo/bar/baz` (all safe)
///
/// [Object key naming guidelines]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
pub fn encode_s3_key(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .split('/')
        .map(|component| {
            // Check if component contains only safe characters
            let is_safe = component.bytes().all(|b| {
                matches!(b,
                    b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.'
                )
            });

            if is_safe && !component.is_empty() {
                component.to_string()
            } else {
                // Base58 encode and prefix with !
                format!("!{}", component.as_bytes().to_base58())
            }
        })
        .collect::<Vec<String>>()
        .join("/")
}

/// Decode an S3-encoded key back to bytes.
///
/// Path components starting with `!` are base58-decoded.
/// Other components are used as-is.
pub fn decode_s3_key(encoded: &str) -> Result<Vec<u8>, S3StorageError> {
    // Decode each path component: `!`-prefixed ones are base58, others are plain text
    let components = encoded
        .split('/')
        .map(|component| {
            if let Some(encoded_part) = component.strip_prefix('!') {
                encoded_part.from_base58().map_err(|e| {
                    S3StorageError::SerializationError(format!(
                        "Invalid base58 encoding in component '{}': {:?}",
                        component, e
                    ))
                })
            } else {
                Ok(component.as_bytes().to_vec())
            }
        })
        .collect::<Result<Vec<Vec<u8>>, S3StorageError>>()?;

    // Join decoded components with `/` separator
    Ok(components.join(&b'/'))
}

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

/// Executable S3 request with an optional body.
///
/// This trait extends [`Invocation`] with the request body and the ability to
/// execute the request against an S3 backend. The separation exists because
/// [`Invocation`] lives in the [`access`] module which handles authorization
/// concerns independently and only needs the metadata required for signing
/// (method, URL, checksum, ACL), not the actual payload.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Request: Invocation + Sized {
    /// The request body, if any.
    fn body(&self) -> Option<&[u8]> {
        None
    }

    /// Perform this request against the given S3 backend.
    async fn perform<Key, Value>(
        &self,
        s3: &S3<Key, Value>,
    ) -> Result<reqwest::Response, S3StorageError>
    where
        Key: AsRef<[u8]> + Clone + ConditionalSync,
        Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    {
        let authorized = s3
            .session
            .authorize(self)
            .map_err(|e| S3StorageError::AuthorizationError(e.to_string()))?;

        let mut builder = match self.method() {
            "GET" => s3.client.get(authorized.url),
            "PUT" => s3.client.put(authorized.url),
            "DELETE" => s3.client.delete(authorized.url),
            method => s3.client.request(
                reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                authorized.url,
            ),
        };

        for (key, value) in authorized.headers {
            builder = builder.header(key, value);
        }

        if let Some(body) = self.body() {
            builder = builder.body(body.to_vec());
        }

        Ok(builder.send().await?)
    }
}

/// S3/R2-compatible storage backend.
///
/// # Example
///
/// ```no_run
/// use dialog_storage::s3::{S3, Session, Service, Credentials};
/// use dialog_storage::StorageBackend;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let credentials = Credentials {
///     access_key_id: "...".into(),
///     secret_access_key: "...".into(),
///     session_token: None,
/// };
/// let service = Service::s3("us-east-1");
/// let session = Session::new(&credentials, &service, 86400);
///
/// let mut storage = S3::<Vec<u8>, Vec<u8>>::open(
///     "https://s3.us-east-1.amazonaws.com",
///     "my-bucket",
///     session,
/// );
/// storage.set(b"key".to_vec(), b"value".to_vec()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct S3<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    /// Base endpoint URL (e.g., "https://s3.region.amazonaws.com")
    endpoint: String,
    /// Bucket name
    bucket: String,
    /// Optional prefix for all keys
    prefix: Option<String>,
    /// Session for authorizing requests
    session: Session,
    /// Hasher for computing checksums
    hasher: Hasher,
    /// HTTP client
    client: reqwest::Client,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
}

impl<Key, Value> S3<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    /// Create a new S3 storage backend.
    ///
    /// Use `Session::Public` for unsigned/public access, or create an authorized
    /// session with `Session::new()` for signed requests.
    ///
    /// By default uses SHA-256 for checksums. Use [`with_hasher`](Self::with_hasher)
    /// to configure a different algorithm.
    pub fn open(endpoint: impl Into<String>, bucket: impl Into<String>, session: Session) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            prefix: None,
            session,
            hasher: Hasher::Sha256,
            client: reqwest::Client::new(),
            key_type: PhantomData,
            value_type: PhantomData,
        }
    }

    /// Set the hasher for computing checksums.
    pub fn with_hasher(mut self, hasher: Hasher) -> Self {
        self.hasher = hasher;
        self
    }

    /// Set the key prefix for all keys.
    pub fn with_prefix<Prefix: Into<String>>(mut self, prefix: Prefix) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Build the URL for a given key.
    fn url(&self, key: &[u8]) -> Result<Url, S3StorageError> {
        let encoded = encode_s3_key(key);

        let object_key = match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, encoded),
            None => encoded,
        };

        let base = self.endpoint.trim_end_matches('/');

        Url::parse(&format!("{base}/{}/{object_key}", self.bucket))
            .map_err(|e| S3StorageError::ServiceError(format!("Invalid URL: {}", e)))
    }

    /// Delete an object from S3.
    ///
    /// Note: S3 DELETE always returns 204 No Content, even if the object didn't exist.
    /// This method always returns `Ok(())` on success.
    pub async fn delete(&mut self, key: &Key) -> Result<(), S3StorageError> {
        let url = self.url(key.as_ref())?;
        let response = Delete::new(url).perform(self).await?;

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
impl<Key, Value> StorageBackend for S3<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    type Key = Key;
    type Value = Value;
    type Error = S3StorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let url = self.url(key.as_ref())?;
        let response = Put::new(url, value.as_ref())
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
        let url = self.url(key.as_ref())?;
        let response = Get::new(url).perform(self).await?;

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
impl<Key, Value> StorageSink for S3<Key, Value>
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
impl<Key, Value> StorageSource for S3<Key, Value>
where
    Key: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    fn read(&self) -> impl Stream<Item = Result<(Self::Key, Self::Value), Self::Error>> {
        let storage = self.clone();
        let prefix = self.prefix.clone();

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

#[cfg(test)]
mod unit_tests {
    use super::*;
    use anyhow::Result;
    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_builds_url_without_prefix() -> Result<()> {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com", "bucket", Session::Public);

        let url = backend.url(&[1, 2, 3]).unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/!Ldp");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_builds_url_with_prefix() -> Result<()> {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com", "bucket", Session::Public)
                .with_prefix("prefix");

        let url = backend.url(&[1, 2, 3]).unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/prefix/!Ldp");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_builds_url_with_trailing_slash() -> Result<()> {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com/", "bucket", Session::Public);

        let url = backend.url(&[1, 2, 3]).unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/!Ldp");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_builds_url_with_bucket_only() -> Result<()> {
        let backend = S3::<Vec<u8>, Vec<u8>>::open(
            "https://s3.us-east-1.amazonaws.com",
            "my-bucket",
            Session::Public,
        );

        // "my-key" is safe ASCII, so it stays as-is (not encoded)
        let url = backend.url(b"my-key").unwrap();
        assert_eq!(
            url.as_str(),
            "https://s3.us-east-1.amazonaws.com/my-bucket/my-key"
        );

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_builds_url_with_bucket_and_prefix() -> Result<()> {
        let backend = S3::<Vec<u8>, Vec<u8>>::open(
            "https://s3.us-east-1.amazonaws.com",
            "my-bucket",
            Session::Public,
        )
        .with_prefix("data");

        // "my-key" is safe ASCII, so it stays as-is (not encoded)
        let url = backend.url(b"my-key").unwrap();
        assert_eq!(
            url.as_str(),
            "https://s3.us-east-1.amazonaws.com/my-bucket/data/my-key"
        );

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_generates_signed_urls() -> Result<()> {
        let credentials = Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let service = Service::s3("us-east-1");
        let session = Session::new(&credentials, &service, 86400);

        let backend = S3::<Vec<u8>, Vec<u8>>::open(
            "https://s3.us-east-1.amazonaws.com",
            "my-bucket",
            session.clone(),
        );

        // Create a PUT request for a key
        let url = backend.url(b"test-key").unwrap();
        let request = Put::new(url, b"test-value");

        // The session should be able to sign it
        let authorized = session.authorize(&request).unwrap();

        // Should have signing parameters
        assert!(
            authorized
                .url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(authorized.url.as_str().contains("X-Amz-Signature="));

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_creates_put_request_with_checksum() -> Result<()> {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value").with_checksum(&Hasher::Sha256);

        // Checksum should be present after with_checksum
        assert!(request.checksum().is_some());
        // Checksum should have the correct algorithm name
        assert_eq!(request.checksum().unwrap().name(), "sha256");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_creates_put_request_without_checksum() -> Result<()> {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value");

        // Checksum should be None by default
        assert!(request.checksum().is_none());

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_creates_put_request_with_acl() -> Result<()> {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value").with_acl(Acl::PublicRead);

        assert_eq!(request.acl(), Some(Acl::PublicRead));

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_creates_get_request() -> Result<()> {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Get::new(url.clone());

        assert_eq!(request.method(), "GET");
        assert_eq!(request.url(), &url);
        assert!(request.checksum().is_none());
        assert!(request.acl().is_none());

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_encodes_s3_key_safe_chars() -> Result<()> {
        // Safe characters should pass through unchanged
        assert_eq!(encode_s3_key(b"simple-key"), "simple-key");
        assert_eq!(encode_s3_key(b"with_underscore"), "with_underscore");
        assert_eq!(encode_s3_key(b"with.dot"), "with.dot");
        assert_eq!(encode_s3_key(b"CamelCase123"), "CamelCase123");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_encodes_s3_key_path_structure() -> Result<()> {
        // Path structure should be preserved
        assert_eq!(encode_s3_key(b"path/to/key"), "path/to/key");
        assert_eq!(encode_s3_key(b"a/b/c"), "a/b/c");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_encodes_s3_key_unsafe_chars() -> Result<()> {
        // Unsafe characters trigger base58 encoding with ! prefix
        let encoded = encode_s3_key(b"user@example");
        assert!(encoded.starts_with('!'));

        let encoded = encode_s3_key(b"has space");
        assert!(encoded.starts_with('!'));

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_encodes_s3_key_binary() -> Result<()> {
        // Binary data gets base58 encoded
        let encoded = encode_s3_key(&[0x01, 0x02, 0x03]);
        assert!(encoded.starts_with('!'));

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_decodes_s3_key_safe_chars() -> Result<()> {
        // Safe keys decode to themselves
        assert_eq!(decode_s3_key("simple-key").unwrap(), b"simple-key");
        assert_eq!(decode_s3_key("path/to/key").unwrap(), b"path/to/key");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_roundtrips_encode_decode() -> Result<()> {
        // Roundtrip encoding should preserve original data
        let original = b"test-key";
        let encoded = encode_s3_key(original);
        let decoded = decode_s3_key(&encoded).unwrap();
        assert_eq!(decoded, original);

        // Binary data roundtrip
        let binary = vec![1, 2, 3, 4, 5];
        let encoded = encode_s3_key(&binary);
        let decoded = decode_s3_key(&encoded).unwrap();
        assert_eq!(decoded, binary);

        // Path with mixed components
        let path = b"safe/!encoded/also-safe";
        let encoded = encode_s3_key(path);
        let decoded = decode_s3_key(&encoded).unwrap();
        assert_eq!(decoded, path);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_authorizes_public_session() -> Result<()> {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url.clone(), b"test").with_checksum(&Hasher::Sha256);

        let authorized = Session::Public.authorize(&request).unwrap();

        // Public session should not modify the URL
        assert_eq!(authorized.url.path(), url.path());
        assert!(authorized.url.query().is_none());

        // Should have host header
        assert!(authorized.headers.iter().any(|(k, _)| k == "host"));

        // Should have checksum header
        assert!(
            authorized
                .headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_configures_s3_with_hasher() -> Result<()> {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com", "bucket", Session::Public)
                .with_hasher(Hasher::Sha256);

        // Hasher should be set (we can't directly inspect it, but the backend should work)
        assert!(backend.url(b"key").is_ok());

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_errors_on_invalid_base58() -> Result<()> {
        // Invalid base58 should return an error
        let result = decode_s3_key("!invalid@@base58");
        assert!(result.is_err());

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_converts_errors_to_dialog_error() -> Result<()> {
        let error = S3StorageError::TransportError("test".into());
        let dialog_error: DialogStorageError = error.into();
        assert!(dialog_error.to_string().contains("test"));

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_creates_delete_request() -> Result<()> {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Delete::new(url.clone());

        assert_eq!(request.method(), "DELETE");
        assert_eq!(request.url(), &url);
        assert!(request.checksum().is_none());
        assert!(request.acl().is_none());

        Ok(())
    }
}

/// Local S3 server tests using s3s for end-to-end testing
#[cfg(all(test, not(target_arch = "wasm32")))]
mod local_s3_tests {
    use super::*;

    #[tokio::test]
    async fn it_sets_and_gets_values() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        // Using Session::Public for simplicity. Signed sessions are tested in
        // test_local_s3_with_signed_session using start_with_auth().
        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("test");

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
    async fn it_performs_multiple_operations() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

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
    async fn it_handles_large_values() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

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
    async fn it_deletes_values() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

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
    async fn it_lists_objects() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("list-test");

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
    async fn it_lists_empty_for_nonexistent_prefix() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("nonexistent-prefix-that-does-not-exist");

        // List objects with a prefix that has no objects - should return empty list
        let result = backend.list(None).await?;

        assert!(result.keys.is_empty());
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());

        Ok(())
    }

    #[cfg(feature = "s3-list")]
    #[tokio::test]
    async fn it_errors_on_nonexistent_bucket() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let backend = S3::<Vec<u8>, Vec<u8>>::open(
            service.endpoint(),
            "bucket-that-does-not-exist",
            Session::Public,
        );

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
    async fn it_reads_stream() -> anyhow::Result<()> {
        use futures_util::TryStreamExt;

        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("stream-test");

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
    async fn it_returns_none_for_missing_values() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

        // Try to get a key that doesn't exist
        let key = b"nonexistent-key".to_vec();
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, None);

        Ok(())
    }

    #[tokio::test]
    async fn it_performs_bulk_writes() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("bulk-test");

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
    async fn it_integrates_with_memory_backend() -> anyhow::Result<()> {
        use crate::StorageSource;
        use futures_util::StreamExt;

        let service = test_server::start(&["test-bucket"]).await?;

        let mut s3_backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("memory-integration");

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
    async fn it_uses_prefix() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        // Create two backends with different prefixes
        let mut backend1 =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("prefix-a");

        let mut backend2 =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("prefix-b");

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
    async fn it_uses_prefix_for_listing() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        // Create two backends with different prefixes
        let mut backend1 =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("prefix-a");

        let mut backend2 =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public)
                .with_prefix("prefix-b");

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
    async fn it_overwrites_value() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

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
    async fn it_handles_binary_keys() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

        // Binary key with non-UTF8 bytes
        let key = vec![0x00, 0xFF, 0x80, 0x7F];
        let value = b"binary-key-value".to_vec();

        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_path_like_keys() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

        // Path-like key with slashes
        let key = b"path/to/nested/key".to_vec();
        let value = b"nested-value".to_vec();

        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[tokio::test]
    async fn it_handles_encoded_key_segments() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

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
    async fn it_handles_multi_segment_mixed_encoding() -> anyhow::Result<()> {
        let service = test_server::start(&["test-bucket"]).await?;

        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

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

    #[tokio::test]
    async fn it_roundtrips_key_encoding() -> anyhow::Result<()> {
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
            let decoded = decode_s3_key(&encoded)?;
            assert_eq!(
                decoded, key,
                "Roundtrip failed for key: {:?}, encoded as: {}",
                key, encoded
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn it_works_with_signed_session() -> anyhow::Result<()> {
        let service =
            test_server::start_with_auth("test-access-key", "test-secret-key", &["test-bucket"])
                .await?;

        // Create credentials matching the test server
        let credentials = super::Credentials {
            access_key_id: "test-access-key".into(),
            secret_access_key: "test-secret-key".into(),
            session_token: None,
        };

        let session = Session::new(&credentials, &super::Service::s3("us-east-1"), 3600);

        let mut backend = S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", session)
            .with_prefix("signed-test");

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
    async fn it_fails_with_wrong_secret_key() -> anyhow::Result<()> {
        let service =
            test_server::start_with_auth("test-access-key", "correct-secret", &["test-bucket"])
                .await?;

        // Create credentials with WRONG secret key
        let credentials = super::Credentials {
            access_key_id: "test-access-key".into(),
            secret_access_key: "wrong-secret".into(),
            session_token: None,
        };

        let session = Session::new(&credentials, &super::Service::s3("us-east-1"), 3600);

        let mut backend = S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", session);

        // Attempt to set a value - should fail due to signature mismatch
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure with wrong secret key"
        );

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_with_wrong_access_key() -> anyhow::Result<()> {
        let service =
            test_server::start_with_auth("correct-access-key", "test-secret", &["test-bucket"])
                .await?;

        // Create credentials with WRONG access key
        let credentials = super::Credentials {
            access_key_id: "wrong-access-key".into(),
            secret_access_key: "test-secret".into(),
            session_token: None,
        };

        let session = Session::new(&credentials, &super::Service::s3("us-east-1"), 3600);

        let mut backend = S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", session);

        // Attempt to set a value - should fail due to unknown access key
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure with wrong access key"
        );

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_unsigned_request_to_auth_server() -> anyhow::Result<()> {
        // Server requires authentication
        let service =
            test_server::start_with_auth("test-access-key", "test-secret-key", &["test-bucket"])
                .await?;

        // Client uses Session::Public (no signing)
        let mut backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", Session::Public);

        // Attempt to set a value - should fail because server expects signed requests
        let result = backend.set(b"key".to_vec(), b"value".to_vec()).await;

        assert!(
            result.is_err(),
            "Expected authentication failure when sending unsigned request to authenticated server"
        );

        Ok(())
    }

    #[tokio::test]
    async fn it_fails_get_with_wrong_credentials() -> anyhow::Result<()> {
        let service =
            test_server::start_with_auth("test-access-key", "test-secret-key", &["test-bucket"])
                .await?;

        // First, set a value with correct credentials
        let correct_credentials = super::Credentials {
            access_key_id: "test-access-key".into(),
            secret_access_key: "test-secret-key".into(),
            session_token: None,
        };
        let correct_session =
            Session::new(&correct_credentials, &super::Service::s3("us-east-1"), 3600);
        let mut correct_backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", correct_session);

        correct_backend
            .set(b"protected-key".to_vec(), b"secret-value".to_vec())
            .await?;

        // Now try to GET with wrong credentials
        let wrong_credentials = super::Credentials {
            access_key_id: "test-access-key".into(),
            secret_access_key: "wrong-secret".into(),
            session_token: None,
        };
        let wrong_session =
            Session::new(&wrong_credentials, &super::Service::s3("us-east-1"), 3600);
        let wrong_backend =
            S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", wrong_session);

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
    async fn it_lists_with_signed_session() -> anyhow::Result<()> {
        let service =
            test_server::start_with_auth("test-access-key", "test-secret-key", &["test-bucket"])
                .await?;

        // Create credentials matching the test server
        let credentials = super::Credentials {
            access_key_id: "test-access-key".into(),
            secret_access_key: "test-secret-key".into(),
            session_token: None,
        };

        let session = Session::new(&credentials, &super::Service::s3("us-east-1"), 3600);

        let mut backend = S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", session)
            .with_prefix("signed-list-test");

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
    async fn it_reads_stream_with_signed_session() -> anyhow::Result<()> {
        use futures_util::TryStreamExt;

        let service =
            test_server::start_with_auth("test-access-key", "test-secret-key", &["test-bucket"])
                .await?;

        let credentials = super::Credentials {
            access_key_id: "test-access-key".into(),
            secret_access_key: "test-secret-key".into(),
            session_token: None,
        };

        let session = Session::new(&credentials, &super::Service::s3("us-east-1"), 3600);

        let mut backend = S3::<Vec<u8>, Vec<u8>>::open(service.endpoint(), "test-bucket", session)
            .with_prefix("signed-stream-test");

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

#[cfg(all(any(test, feature = "s3-test-utils"), not(target_arch = "wasm32")))]
/// S3-compatible test server for integration testing.
///
/// This module provides a simple in-memory S3-compatible server
/// for testing S3 storage backend functionality.
pub mod test_server {
    use async_trait::async_trait;
    use bytes::Bytes;
    use hyper::server::conn::http1;
    use hyper_util::rt::TokioIo;
    use s3s::dto::*;
    use s3s::service::S3ServiceBuilder;
    use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tokio::net::TcpListener;
    use tokio::sync::RwLock;

    /// Simple in-memory backend for testing.
    ///
    /// Structure: bucket_name -> key -> StoredObject
    #[derive(Clone, Default)]
    pub struct InMemoryS3 {
        buckets: Arc<RwLock<HashMap<String, HashMap<String, StoredObject>>>>,
    }

    /// A running S3 test server instance.
    pub struct Service {
        /// The endpoint URL where the server is listening
        pub endpoint: String,
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
        storage: InMemoryS3,
    }
    impl Service {
        /// Stops the test server.
        pub fn stop(self) -> Result<(), ()> {
            self.shutdown_tx.send(())
        }

        /// Returns the endpoint URL of the running server.
        pub fn endpoint(&self) -> &str {
            &self.endpoint
        }

        /// Returns a reference to the in-memory storage.
        pub fn storage(&self) -> &InMemoryS3 {
            &self.storage
        }
    }

    #[derive(Clone)]
    struct StoredObject {
        data: Vec<u8>,
        content_type: Option<String>,
        e_tag: String,
        last_modified: Timestamp,
    }

    impl InMemoryS3 {
        /// Create a bucket if it doesn't exist.
        pub async fn create_bucket(&self, bucket: &str) {
            let mut buckets = self.buckets.write().await;
            if !buckets.contains_key(bucket) {
                buckets.insert(bucket.to_string(), HashMap::new());
            }
        }

        async fn get_or_create_bucket(
            &self,
            bucket: &str,
        ) -> tokio::sync::RwLockWriteGuard<'_, HashMap<String, HashMap<String, StoredObject>>>
        {
            let mut buckets = self.buckets.write().await;
            if !buckets.contains_key(bucket) {
                buckets.insert(bucket.to_string(), HashMap::new());
            }
            buckets
        }
    }

    #[async_trait]
    impl S3 for InMemoryS3 {
        async fn get_object(
            &self,
            req: S3Request<GetObjectInput>,
        ) -> S3Result<S3Response<GetObjectOutput>> {
            let bucket = &req.input.bucket;
            let key = &req.input.key;

            let buckets = self.buckets.read().await;
            if let Some(bucket_contents) = buckets.get(bucket) {
                if let Some(obj) = bucket_contents.get(key) {
                    let body = s3s::Body::from(Bytes::from(obj.data.clone()));
                    let output = GetObjectOutput {
                        body: Some(StreamingBlob::from(body)),
                        content_length: Some(obj.data.len() as i64),
                        content_type: obj.content_type.clone(),
                        e_tag: Some(ETag::Strong(obj.e_tag.clone())),
                        last_modified: Some(obj.last_modified.clone()),
                        ..Default::default()
                    };
                    return Ok(S3Response::new(output));
                }
            }
            Err(s3_error!(NoSuchKey))
        }

        async fn put_object(
            &self,
            req: S3Request<PutObjectInput>,
        ) -> S3Result<S3Response<PutObjectOutput>> {
            let bucket = req.input.bucket.clone();
            let key = req.input.key.clone();
            let content_type = req.input.content_type.clone();

            let data = if let Some(mut body) = req.input.body {
                // Collect stream data chunk by chunk using Stream trait
                use futures_util::StreamExt;
                let mut chunks = Vec::new();
                while let Some(result) = body.next().await {
                    if let Ok(bytes) = result {
                        chunks.extend_from_slice(&bytes);
                    }
                }
                chunks
            } else {
                Vec::new()
            };

            // Calculate MD5 for ETag
            let e_tag = format!("{:x}", md5::compute(&data));

            let stored = StoredObject {
                data,
                content_type,
                e_tag: e_tag.clone(),
                last_modified: Timestamp::from(SystemTime::now()),
            };

            let mut buckets = self.get_or_create_bucket(&bucket).await;
            if let Some(bucket_contents) = buckets.get_mut(&bucket) {
                bucket_contents.insert(key, stored);
            }

            let output = PutObjectOutput {
                e_tag: Some(ETag::Strong(e_tag)),
                ..Default::default()
            };
            Ok(S3Response::new(output))
        }

        async fn delete_object(
            &self,
            req: S3Request<DeleteObjectInput>,
        ) -> S3Result<S3Response<DeleteObjectOutput>> {
            let bucket = &req.input.bucket;
            let key = &req.input.key;

            let mut buckets = self.buckets.write().await;
            if let Some(bucket_contents) = buckets.get_mut(bucket) {
                bucket_contents.remove(key);
            }

            Ok(S3Response::new(DeleteObjectOutput::default()))
        }

        async fn head_object(
            &self,
            req: S3Request<HeadObjectInput>,
        ) -> S3Result<S3Response<HeadObjectOutput>> {
            let bucket = &req.input.bucket;
            let key = &req.input.key;

            let buckets = self.buckets.read().await;
            if let Some(bucket_contents) = buckets.get(bucket) {
                if let Some(obj) = bucket_contents.get(key) {
                    let output = HeadObjectOutput {
                        content_length: Some(obj.data.len() as i64),
                        content_type: obj.content_type.clone(),
                        e_tag: Some(ETag::Strong(obj.e_tag.clone())),
                        last_modified: Some(obj.last_modified.clone()),
                        ..Default::default()
                    };
                    return Ok(S3Response::new(output));
                }
            }
            Err(s3_error!(NoSuchKey))
        }

        async fn list_objects_v2(
            &self,
            req: S3Request<ListObjectsV2Input>,
        ) -> S3Result<S3Response<ListObjectsV2Output>> {
            let bucket = &req.input.bucket;
            let prefix = req.input.prefix.as_deref().unwrap_or("");

            let buckets = self.buckets.read().await;

            // Return NoSuchBucket error if bucket doesn't exist (matches real S3 behavior)
            // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_Errors
            let bucket_contents = buckets.get(bucket).ok_or_else(|| s3_error!(NoSuchBucket))?;

            let mut contents = Vec::new();
            for (key, obj) in bucket_contents.iter() {
                // Filter by prefix
                if key.starts_with(prefix) {
                    contents.push(Object {
                        key: Some(key.clone()),
                        size: Some(obj.data.len() as i64),
                        e_tag: Some(ETag::Strong(obj.e_tag.clone())),
                        last_modified: Some(obj.last_modified.clone()),
                        ..Default::default()
                    });
                }
            }

            // Sort by key for consistent ordering
            contents.sort_by(|a, b| a.key.cmp(&b.key));

            let output = ListObjectsV2Output {
                contents: Some(contents),
                is_truncated: Some(false),
                key_count: None,
                ..Default::default()
            };
            Ok(S3Response::new(output))
        }
    }

    /// Start a local S3-compatible test server with pre-created buckets.
    ///
    /// Returns a handle that can be used to get the endpoint URL and stop the server.
    pub async fn start(buckets: &[&str]) -> anyhow::Result<Service> {
        start_internal(None, buckets).await
    }

    /// Start a test server with authentication and pre-created buckets.
    pub async fn start_with_auth(
        access_key: &str,
        secret_key: &str,
        buckets: &[&str],
    ) -> anyhow::Result<Service> {
        let auth = s3s::auth::SimpleAuth::from_single(access_key, secret_key);
        start_internal(Some(auth), buckets).await
    }

    async fn start_internal(
        auth: Option<s3s::auth::SimpleAuth>,
        buckets: &[&str],
    ) -> anyhow::Result<Service> {
        use std::sync::Arc;

        let storage = InMemoryS3::default();

        // Pre-create buckets
        for bucket in buckets {
            storage.create_bucket(bucket).await;
        }

        let mut builder = S3ServiceBuilder::new(storage.clone());
        if let Some(auth) = auth {
            builder.set_auth(auth);
        }
        let service = Arc::new(builder.build());

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let endpoint = format!("http://{}", addr);

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    result = listener.accept() => {
                        if let Ok((stream, _)) = result {
                            let service = service.clone();
                            tokio::spawn(async move {
                                let _ = http1::Builder::new()
                                    .serve_connection(TokioIo::new(stream), service.as_ref().clone())
                                    .await;
                            });
                        }
                    }
                }
            }
        });

        Ok(Service {
            endpoint,
            shutdown_tx,
            storage,
        })
    }
}

/// Integration tests that run against a real S3/R2/MinIO endpoint.
///
/// ## Environment Variables
///
/// These tests require the following environment variables:
/// - R2S3_HOST: The S3-compatible endpoint (e.g., "https://s3.amazonaws.com" or "https://xxx.r2.cloudflarestorage.com")
/// - R2S3_REGION: AWS region (e.g., "us-east-1" or "auto" for R2)
/// - R2S3_BUCKET: Bucket name
/// - R2S3_ACCESS_KEY_ID: Access key ID
/// - R2S3_SECRET_ACCESS_KEY: Secret access key
///
/// Run these tests with:
/// ```bash
/// R2S3_HOST=https://2fc7ca2f9584223662c5a882977b89ac.r2.cloudflarestorage.com \
///   R2S3_REGION=auto \
///   R2S3_BUCKET=dialog-test \
///   R2S3_ACCESS_KEY_ID=access_key \
///   R2S3_SECRET_ACCESS_KEY=secret \
///   cargo test s3_integration_tests --features s3-integration-tests
/// ```
#[cfg(all(test, feature = "s3-integration-tests"))]
mod s3_integration_tests {
    use super::*;
    use anyhow::Result;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Helper to create an S3 backend from environment variables.
    fn create_s3_backend_from_env() -> Result<S3<Vec<u8>, Vec<u8>>> {
        let credentials = Credentials {
            access_key_id: env!("R2S3_ACCESS_KEY_ID").into(),
            secret_access_key: env!("R2S3_SECRET_ACCESS_KEY").into(),
            session_token: option_env!("R2S3_SESSION_TOKEN").map(|v| v.into()),
        };

        let region = env!("R2S3_REGION");
        let service = Service::s3(region);
        let session = Session::new(&credentials, &service, 3600);

        let endpoint = env!("R2S3_HOST");
        let bucket = env!("R2S3_BUCKET");

        Ok(S3::open(endpoint, bucket, session).with_prefix("test-prefix"))
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_sets_and_gets_values() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

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

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_returns_none_for_missing_key() -> Result<()> {
        let backend = create_s3_backend_from_env()?;

        // Try to get a key that doesn't exist
        let key = b"nonexistent-key-12345".to_vec();
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, None);

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_overwrites_values() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        let key = b"test-key-overwrite".to_vec();
        let value1 = b"original-value".to_vec();
        let value2 = b"updated-value".to_vec();

        // Set initial value
        backend.set(key.clone(), value1.clone()).await?;

        // Verify it was set
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value1));

        // Overwrite with new value
        backend.set(key.clone(), value2.clone()).await?;

        // Verify it was updated
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value2));

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_handles_large_values() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        let key = b"test-key-large".to_vec();
        // Create a 1MB value
        let value: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        // Set the large value
        backend.set(key.clone(), value.clone()).await?;

        // Get it back and verify
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_handles_multiple_keys() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        // Set multiple key-value pairs
        let pairs = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        for (key, value) in &pairs {
            backend.set(key.clone(), value.clone()).await?;
        }

        // Verify all keys can be retrieved
        for (key, expected_value) in &pairs {
            let retrieved = backend.get(key).await?;
            assert_eq!(retrieved.as_ref(), Some(expected_value));
        }

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_handles_binary_data() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        let key = b"test-key-binary".to_vec();
        // Create binary data with all possible byte values
        let value: Vec<u8> = (0..=255).collect();

        // Set the binary value
        backend.set(key.clone(), value.clone()).await?;

        // Get it back and verify
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_performs_bulk_operations() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        // Create a stream of test data
        use async_stream::try_stream;

        let test_data = vec![
            (b"bulk1".to_vec(), b"value1".to_vec()),
            (b"bulk2".to_vec(), b"value2".to_vec()),
            (b"bulk3".to_vec(), b"value3".to_vec()),
        ];

        let data_clone = test_data.clone();
        let source_stream = try_stream! {
            for (key, value) in data_clone {
                yield (key, value);
            }
        };

        // Write all data
        backend.write(source_stream).await?;

        // Verify all items were written
        for (key, expected_value) in test_data {
            let retrieved = backend.get(&key).await?;
            assert_eq!(retrieved, Some(expected_value));
        }

        Ok(())
    }

    /// Helper to create an S3 backend without prefix from environment variables.
    fn create_s3_backend_without_prefix_from_env() -> Result<S3<Vec<u8>, Vec<u8>>> {
        let credentials = Credentials {
            access_key_id: env!("R2S3_ACCESS_KEY_ID").into(),
            secret_access_key: env!("R2S3_SECRET_ACCESS_KEY").into(),
            session_token: option_env!("R2S3_SESSION_TOKEN").map(|v| v.into()),
        };

        let region = env!("R2S3_REGION");
        let service = Service::s3(region);
        let session = Session::new(&credentials, &service, 3600);

        let endpoint = env!("R2S3_HOST");
        let bucket = env!("R2S3_BUCKET");

        // No prefix - keys go directly into the bucket root
        Ok(S3::open(endpoint, bucket, session))
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_works_without_prefix() -> Result<()> {
        let mut backend = create_s3_backend_without_prefix_from_env()?;

        // Test data - use unique key to avoid conflicts
        let key = b"no-prefix-test-key".to_vec();
        let value = b"no-prefix-test-value".to_vec();

        // Set the value
        backend.set(key.clone(), value.clone()).await?;

        // Get the value back
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_handles_encoded_key_segments() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        // Test key with path structure where one segment is safe and another needs encoding
        // "safe-segment/user@example.com" - first segment is safe, second has @ which is unsafe
        let key_mixed = b"safe-segment/user@example.com".to_vec();
        let value_mixed = b"value-for-mixed-key".to_vec();

        // Verify encoding behavior
        let encoded = super::encode_s3_key(&key_mixed);
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

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_handles_fully_encoded_key() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        // Test key that is fully binary (all segments need encoding)
        let key_binary = vec![0x01, 0x02, 0xFF, 0xFE];
        let value_binary = b"value-for-binary-key".to_vec();

        // Verify encoding behavior - binary data should be encoded
        let encoded = super::encode_s3_key(&key_binary);
        assert!(
            encoded.starts_with('!'),
            "Binary key should be encoded with ! prefix: {}",
            encoded
        );

        // Write and read back
        backend
            .set(key_binary.clone(), value_binary.clone())
            .await?;
        let retrieved = backend.get(&key_binary).await?;
        assert_eq!(retrieved, Some(value_binary));

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_handles_multi_segment_mixed_encoding() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        // Test key with multiple segments: safe/unsafe/safe/unsafe pattern
        // "data/file name with spaces/v1/special!chars"
        let key = b"data/file name with spaces/v1/special!chars".to_vec();
        let value = b"value-for-complex-path".to_vec();

        // Verify encoding behavior
        let encoded = super::encode_s3_key(&key);
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

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_handles_encoded_key_without_prefix() -> Result<()> {
        let mut backend = create_s3_backend_without_prefix_from_env()?;

        // Test encoded key without prefix
        let key = b"path/with spaces/data".to_vec();
        let value = b"value-for-encoded-no-prefix".to_vec();

        // Verify encoding
        let encoded = super::encode_s3_key(&key);
        let segments: Vec<&str> = encoded.split('/').collect();
        assert_eq!(segments[0], "path", "First segment should be safe");
        assert!(
            segments[1].starts_with('!'),
            "Second segment should be encoded"
        );
        assert_eq!(segments[2], "data", "Third segment should be safe");

        // Write and read back without prefix
        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_deletes_values() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        let key = b"delete-integration-test".to_vec();
        let value = b"value-to-delete".to_vec();

        // Set the value
        backend.set(key.clone(), value.clone()).await?;

        // Verify it exists
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        // Delete it
        backend.delete(&key).await?;

        // Verify it's gone
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, None);

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_lists_objects() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        // Use a unique prefix for this test
        let test_prefix = format!(
            "list-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        // Create a backend with the unique prefix
        let credentials = Credentials {
            access_key_id: env!("R2S3_ACCESS_KEY_ID").into(),
            secret_access_key: env!("R2S3_SECRET_ACCESS_KEY").into(),
            session_token: option_env!("R2S3_SESSION_TOKEN").map(|v| v.into()),
        };
        let region = env!("R2S3_REGION");
        let service = Service::s3(region);
        let session = Session::new(&credentials, &service, 3600);
        let endpoint = env!("R2S3_HOST");
        let bucket = env!("R2S3_BUCKET");

        let mut backend = S3::open(endpoint, bucket, session).with_prefix(&test_prefix);

        // Set a few values
        backend
            .set(b"list-key1".to_vec(), b"value1".to_vec())
            .await?;
        backend
            .set(b"list-key2".to_vec(), b"value2".to_vec())
            .await?;

        // List objects
        let result = backend.list(None).await?;

        // Should have at least 2 keys (may have more if other tests ran)
        assert!(
            result.keys.len() >= 2,
            "Expected at least 2 keys, got {}",
            result.keys.len()
        );

        // All keys should have our prefix
        for key in &result.keys {
            assert!(
                key.starts_with(&test_prefix),
                "Key {} should start with prefix {}",
                key,
                test_prefix
            );
        }

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_reads_stream() -> Result<()> {
        use futures_util::TryStreamExt;

        // Use a unique prefix for this test
        let test_prefix = format!(
            "stream-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let credentials = Credentials {
            access_key_id: env!("R2S3_ACCESS_KEY_ID").into(),
            secret_access_key: env!("R2S3_SECRET_ACCESS_KEY").into(),
            session_token: option_env!("R2S3_SESSION_TOKEN").map(|v| v.into()),
        };
        let region = env!("R2S3_REGION");
        let service = Service::s3(region);
        let session = Session::new(&credentials, &service, 3600);
        let endpoint = env!("R2S3_HOST");
        let bucket = env!("R2S3_BUCKET");

        let mut backend = S3::open(endpoint, bucket, session).with_prefix(&test_prefix);

        // Set a few values
        backend
            .set(b"stream-a".to_vec(), b"value-a".to_vec())
            .await?;
        backend
            .set(b"stream-b".to_vec(), b"value-b".to_vec())
            .await?;

        // Read all items via StorageSource
        let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut stream = Box::pin(backend.read());

        while let Some((key, value)) = stream.try_next().await? {
            items.push((key, value));
        }

        assert_eq!(items.len(), 2);

        // Verify the items (order may vary)
        let keys: Vec<&[u8]> = items.iter().map(|(k, _)| k.as_slice()).collect();
        assert!(keys.contains(&b"stream-a".as_slice()));
        assert!(keys.contains(&b"stream-b".as_slice()));

        Ok(())
    }

    /// Test that listing with a nonexistent prefix returns an empty list (not an error).
    ///
    /// This verifies real S3/R2 behavior: a prefix is just a filter, not a path that must exist.
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn it_lists_empty_for_nonexistent_prefix() -> Result<()> {
        let credentials = Credentials {
            access_key_id: env!("R2S3_ACCESS_KEY_ID").into(),
            secret_access_key: env!("R2S3_SECRET_ACCESS_KEY").into(),
            session_token: option_env!("R2S3_SESSION_TOKEN").map(|v| v.into()),
        };

        let region = env!("R2S3_REGION");
        let service = Service::s3(region);
        let session = Session::new(&credentials, &service, 3600);

        let endpoint = env!("R2S3_HOST");
        let bucket = env!("R2S3_BUCKET");

        // Use a prefix that definitely doesn't exist
        let backend = S3::<Vec<u8>, Vec<u8>>::open(endpoint, bucket, session)
            .with_prefix("nonexistent-prefix-that-should-not-exist-12345");

        // Listing should return empty result, not an error
        let result = backend.list(None).await?;

        assert!(result.keys.is_empty());
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());

        Ok(())
    }
}
