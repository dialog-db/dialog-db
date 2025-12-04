use std::marker::PhantomData;

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use reqwest;
use thiserror::Error;
use url::Url;

mod access;
pub use access::{Acl, Credentials, Invocation, Service, Session, SigningError};

mod checksum;
pub use checksum::{Checksum, Hasher};

use crate::{DialogStorageError, StorageBackend, StorageSink, StorageSource};

// ============================================================================
// Request Types
// ============================================================================

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

/// S3-safe key encoding that preserves path structure.
///
/// Keys are treated as `/`-delimited paths. Each path component is checked:
/// - If it contains only safe characters (alphanumeric, `-`, `_`, `.`), it's kept as-is
/// - Otherwise, it's base58-encoded and prefixed with `!`
///
/// The `!` character is used as a prefix marker because it's in AWS S3's
/// "safe for use" list and unlikely to appear at the start of path components.
///
/// Examples:
/// - `remote/main` → `remote/main` (all components safe)
/// - `remote/user@example` → `remote/!<base58>` (@ is unsafe, encode component)
/// - `foo/bar/baz` → `foo/bar/baz` (all safe)
pub fn encode_s3_key(bytes: &[u8]) -> String {
    use base58::ToBase58;

    let key_str = String::from_utf8_lossy(bytes);
    let components: Vec<&str> = key_str.split('/').collect();

    let encoded_components: Vec<String> = components
        .iter()
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
        .collect();

    encoded_components.join("/")
}

/// Decode an S3-encoded key back to bytes.
///
/// Path components starting with `!` are base58-decoded.
/// Other components are used as-is.
pub fn decode_s3_key(encoded: &str) -> Result<Vec<u8>, S3StorageError> {
    use base58::FromBase58;

    let components: Vec<&str> = encoded.split('/').collect();
    let mut decoded_components: Vec<Vec<u8>> = Vec::new();

    for component in components {
        if let Some(encoded_part) = component.strip_prefix('!') {
            // Base58 decode
            let decoded = encoded_part.from_base58().map_err(|e| {
                S3StorageError::SerializationFailed(format!(
                    "Invalid base58 encoding in component '{}': {:?}",
                    component, e
                ))
            })?;
            decoded_components.push(decoded);
        } else {
            // Use as-is
            decoded_components.push(component.as_bytes().to_vec());
        }
    }

    // Join with /
    let mut result = Vec::new();
    for (i, component) in decoded_components.iter().enumerate() {
        if i > 0 {
            result.push(b'/');
        }
        result.extend_from_slice(component);
    }

    Ok(result)
}

/// Errors that can occur when using the S3 storage backend
#[derive(Error, Debug)]
pub enum S3StorageError {
    /// Error that occurs when connection to the S3 API fails
    #[error("Failed to connect to S3: {0}")]
    ConnectionFailed(String),

    /// Error that occurs when an S3 operation fails
    #[error("Failed to perform S3 operation: {0}")]
    OperationFailed(String),

    /// Error that occurs when an API request fails
    #[error("S3 request failed: {0}")]
    RequestFailed(String),

    /// Error that occurs during serialization or deserialization of data
    #[error("Failed to serialize/deserialize data: {0}")]
    SerializationFailed(String),
}

impl From<S3StorageError> for DialogStorageError {
    fn from(error: S3StorageError) -> Self {
        DialogStorageError::StorageBackend(error.to_string())
    }
}

impl From<reqwest::Error> for S3StorageError {
    fn from(error: reqwest::Error) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if error.is_connect() {
                S3StorageError::ConnectionFailed(error.to_string())
            } else if error.is_request() {
                S3StorageError::OperationFailed(error.to_string())
            } else {
                S3StorageError::RequestFailed(error.to_string())
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            // WASM doesn't have is_connect() or is_request() methods
            S3StorageError::RequestFailed(error.to_string())
        }
    }
}

/// Extension trait for requests that can be performed against an S3 backend.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Request: Invocation + Sized {
    /// The request body, if any. Used when sending the request.
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
        let authorized = s3.session.authorize(self).map_err(|e| {
            S3StorageError::OperationFailed(format!("Failed to authorize request: {}", e))
        })?;

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
/// ```ignore
/// use dialog_storage::s3::{S3, Session, Service, Credentials};
///
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
    _key: PhantomData<Key>,
    _value: PhantomData<Value>,
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
            _key: PhantomData,
            _value: PhantomData,
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
        let key_str = encode_s3_key(key);

        let object_key = match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, key_str),
            None => key_str,
        };

        let base_url = self.endpoint.trim_end_matches('/');
        let url_str = format!("{base_url}/{}/{object_key}", self.bucket);

        Url::parse(&url_str)
            .map_err(|e| S3StorageError::OperationFailed(format!("Failed to parse URL: {}", e)))
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
            Err(S3StorageError::OperationFailed(format!(
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
                .map_err(|e| S3StorageError::RequestFailed(e.to_string()))?;
            Ok(Some(Value::from(bytes.to_vec())))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(S3StorageError::OperationFailed(format!(
                "Failed to get value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageSink for S3<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    async fn write<S>(&mut self, source: S) -> Result<(), Self::Error>
    where
        S: Stream<Item = Result<(Self::Key, Self::Value), Self::Error>> + Send,
    {
        use futures_util::StreamExt;
        futures_util::pin_mut!(source);
        while let Some(result) = source.next().await {
            let (key, value) = result?;
            self.set(key, value).await?;
        }
        Ok(())
    }
}

impl<Key, Value> StorageSource for S3<Key, Value>
where
    Key: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    fn read(&self) -> impl Stream<Item = Result<(Self::Key, Self::Value), Self::Error>> {
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();

        try_stream! {
            // Get list of keys from _list endpoint
            let list_url = format!("{}/_list", endpoint);
            let list_response = client.get(&list_url).send().await?;

            if list_response.status().is_success() {
                let keys: Vec<String> = list_response.json().await
                    .map_err(|e| S3StorageError::SerializationFailed(e.to_string()))?;

                for encoded_key in keys {
                    // Decode the key
                    let key_bytes = decode_s3_key(&encoded_key)?;

                    // Fetch the value
                    if let Some(value) = self.get(&Key::from(key_bytes.clone())).await? {
                        yield (Key::from(key_bytes), value);
                    }
                }
            }
        }
    }

    fn drain(&mut self) -> impl Stream<Item = Result<(Self::Key, Self::Value), Self::Error>> {
        // S3 doesn't support draining, so just read
        self.read()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(all(test, not(target_arch = "wasm32")))]
mod unit_tests {
    use super::*;

    #[test]
    fn test_url_without_prefix() {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com", "bucket", Session::Public);

        let url = backend.url(&[1, 2, 3]).unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/!Ldp");
    }

    #[test]
    fn test_url_with_prefix() {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com", "bucket", Session::Public)
                .with_prefix("prefix");

        let url = backend.url(&[1, 2, 3]).unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/prefix/!Ldp");
    }

    #[test]
    fn test_url_with_trailing_slash() {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com/", "bucket", Session::Public);

        let url = backend.url(&[1, 2, 3]).unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/!Ldp");
    }

    #[test]
    fn test_s3_url_with_bucket_only() {
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
    }

    #[test]
    fn test_s3_url_with_bucket_and_prefix() {
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
    }

    #[test]
    fn test_s3_signed_url_generation() {
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
    }

    #[test]
    fn test_put_with_checksum() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value").with_checksum(&Hasher::Sha256);

        // Checksum should be present after with_checksum
        assert!(request.checksum().is_some());
        // Checksum should have the correct algorithm name
        assert_eq!(request.checksum().unwrap().name(), "sha256");
    }

    #[test]
    fn test_put_without_checksum() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value");

        // Checksum should be None by default
        assert!(request.checksum().is_none());
    }

    #[test]
    fn test_put_with_acl() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value").with_acl(Acl::PublicRead);

        assert_eq!(request.acl(), Some(Acl::PublicRead));
    }

    #[test]
    fn test_get_request() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Get::new(url.clone());

        assert_eq!(request.method(), "GET");
        assert_eq!(request.url(), &url);
        assert!(request.checksum().is_none());
        assert!(request.acl().is_none());
    }

    #[test]
    fn test_encode_s3_key_safe_chars() {
        // Safe characters should pass through unchanged
        assert_eq!(encode_s3_key(b"simple-key"), "simple-key");
        assert_eq!(encode_s3_key(b"with_underscore"), "with_underscore");
        assert_eq!(encode_s3_key(b"with.dot"), "with.dot");
        assert_eq!(encode_s3_key(b"CamelCase123"), "CamelCase123");
    }

    #[test]
    fn test_encode_s3_key_path_structure() {
        // Path structure should be preserved
        assert_eq!(encode_s3_key(b"path/to/key"), "path/to/key");
        assert_eq!(encode_s3_key(b"a/b/c"), "a/b/c");
    }

    #[test]
    fn test_encode_s3_key_unsafe_chars() {
        // Unsafe characters trigger base58 encoding with ! prefix
        let encoded = encode_s3_key(b"user@example");
        assert!(encoded.starts_with('!'));

        let encoded = encode_s3_key(b"has space");
        assert!(encoded.starts_with('!'));
    }

    #[test]
    fn test_encode_s3_key_binary() {
        // Binary data gets base58 encoded
        let encoded = encode_s3_key(&[0x01, 0x02, 0x03]);
        assert!(encoded.starts_with('!'));
    }

    #[test]
    fn test_decode_s3_key_safe_chars() {
        // Safe keys decode to themselves
        assert_eq!(decode_s3_key("simple-key").unwrap(), b"simple-key");
        assert_eq!(decode_s3_key("path/to/key").unwrap(), b"path/to/key");
    }

    #[test]
    fn test_encode_decode_roundtrip() {
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
    }

    #[test]
    fn test_public_session_authorization() {
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
    }

    #[test]
    fn test_s3_with_hasher() {
        let backend =
            S3::<Vec<u8>, Vec<u8>>::open("https://s3.amazonaws.com", "bucket", Session::Public)
                .with_hasher(Hasher::Sha256);

        // Hasher should be set (we can't directly inspect it, but the backend should work)
        assert!(backend.url(b"key").is_ok());
    }

    #[test]
    fn test_decode_s3_key_invalid_base58() {
        // Invalid base58 should return an error
        let result = decode_s3_key("!invalid@@base58");
        assert!(result.is_err());
    }

    #[test]
    fn test_error_conversion() {
        let error = S3StorageError::ConnectionFailed("test".into());
        let dialog_error: DialogStorageError = error.into();
        assert!(dialog_error.to_string().contains("test"));
    }

    #[test]
    fn test_error_types() {
        // Test all error variants
        let conn_err = S3StorageError::ConnectionFailed("conn".into());
        assert!(conn_err.to_string().contains("connect"));

        let op_err = S3StorageError::OperationFailed("op".into());
        assert!(op_err.to_string().contains("operation"));

        let req_err = S3StorageError::RequestFailed("req".into());
        assert!(req_err.to_string().contains("request"));

        let ser_err = S3StorageError::SerializationFailed("ser".into());
        assert!(ser_err.to_string().contains("serialize"));
    }

    #[test]
    fn test_acl_as_str() {
        assert_eq!(Acl::Private.as_str(), "private");
        assert_eq!(Acl::PublicRead.as_str(), "public-read");
        assert_eq!(Acl::PublicReadWrite.as_str(), "public-read-write");
        assert_eq!(Acl::AuthenticatedRead.as_str(), "authenticated-read");
        assert_eq!(Acl::BucketOwnerRead.as_str(), "bucket-owner-read");
        assert_eq!(
            Acl::BucketOwnerFullControl.as_str(),
            "bucket-owner-full-control"
        );
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use anyhow::Result;
    use mockito::Server;
    use serde::{Deserialize, Serialize};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;

    /// Test struct for exercising serialization/deserialization
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestValue {
        data: String,
    }

    // Helper function to create an unsigned S3 backend with a mock server
    async fn create_test_backend() -> (S3<Vec<u8>, Vec<u8>>, mockito::ServerGuard) {
        let server = Server::new_async().await;

        let backend = S3::open(server.url(), "bucket", Session::Public);

        (backend, server)
    }

    // Helper function to create a signed S3 backend with a mock server
    async fn create_s3_test_backend() -> (S3<Vec<u8>, Vec<u8>>, mockito::ServerGuard) {
        let server = Server::new_async().await;
        let endpoint = server.url();

        // Use fixed credentials for testing
        let credentials = Credentials {
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            session_token: None,
        };

        let service = Service::s3("us-east-1");
        let session = Session::new(&credentials, &service, 86400);

        let backend = S3::open(endpoint, "test-bucket", session);

        (backend, server)
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_writes_and_reads_a_value() -> Result<()> {
        let (mut backend, mut server) = create_test_backend().await;

        // Key encodes to: !Ldp
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        // Mock PUT request for set operation (with bucket in path)
        let put_mock = server
            .mock("PUT", "/bucket/!Ldp")
            .with_status(200)
            .with_body("")
            .create();

        // Mock GET request for successful retrieval
        let get_mock = server
            .mock("GET", "/bucket/!Ldp")
            .with_status(200)
            .with_body(&[4, 5, 6])
            .create();

        // Test set operation
        backend.set(key.clone(), value.clone()).await?;
        put_mock.assert();

        // Test get operation
        let retrieved = backend.get(&key).await?;
        get_mock.assert();

        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_returns_none_for_missing_values() -> Result<()> {
        let (backend, mut server) = create_test_backend().await;

        let key = vec![10, 11, 12]; // Encodes to: !<base58> (binary data with ! prefix)

        // Mock GET request for missing value (404 response)
        // Match any path starting with /bucket/! (encoded binary keys)
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/bucket/!.*".to_string()))
            .with_status(404)
            .with_body("")
            .create();

        let retrieved = backend.get(&key).await?;
        mock.assert();

        assert_eq!(retrieved, None);

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_handles_error_responses() -> Result<()> {
        let (backend, mut server) = create_test_backend().await;

        let key = vec![20, 21, 22]; // Encodes to: !7kEh (base58-encoded with ! prefix)

        // Mock GET request for server error
        // Use regex matcher for URL-encoded paths
        let mock = server
            .mock(
                "GET",
                mockito::Matcher::Regex(r"^/bucket/!7kEh.*".to_string()),
            )
            .with_status(500)
            .with_body("Internal Server Error")
            .create();

        let result = backend.get(&key).await;
        mock.assert();

        assert!(result.is_err());

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_uses_prefix() -> Result<()> {
        let mut server = Server::new_async().await;
        let mut backend =
            S3::open(server.url(), "bucket", Session::Public).with_prefix("test-prefix");

        // Mock PUT request with prefix in path
        let mock = server
            .mock("PUT", "/bucket/test-prefix/!Ldp")
            .with_status(200)
            .create();

        backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        mock.assert();

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_perform_list_operations() -> Result<()> {
        let (backend, mut server) = create_test_backend().await;

        // Mock the list endpoint
        let list_mock = server
            .mock("GET", "/_list")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"["!Ldp", "!6xdze"]"#)
            .create();

        // Mock the GET for first key
        let get_mock1 = server
            .mock("GET", "/bucket/!Ldp")
            .with_status(200)
            .with_body(&[4, 5, 6])
            .create();

        // Mock the GET for second key
        let get_mock2 = server
            .mock("GET", "/bucket/!6xdze")
            .with_status(200)
            .with_body(&[8, 9, 10])
            .create();

        use futures_util::TryStreamExt;

        let mut items = Vec::new();
        let mut stream = Box::pin(backend.read());

        while let Some((key, value)) = stream.try_next().await? {
            items.push((key, value));
        }

        list_mock.assert();
        get_mock1.assert();
        get_mock2.assert();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0], (vec![1, 2, 3], vec![4, 5, 6]));
        assert_eq!(items[1], (vec![4, 5, 6, 7], vec![8, 9, 10]));

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[ignore] // S3 signing doesn't work with mockito's IP-based URLs
    async fn it_can_perform_s3_list_operations() -> Result<()> {
        let (backend, mut server) = create_s3_test_backend().await;

        // Mock the list endpoint, which will have a signed URL
        let list_mock = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"["!Ldp", "!6xdze"]"#)
            .create();

        // Mock the GET for first key
        let get_mock1 = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_body(&[4, 5, 6])
            .create();

        // Mock the GET for second key
        let get_mock2 = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_body(&[8, 9, 10])
            .create();

        use futures_util::TryStreamExt;

        let mut items = Vec::new();
        let mut stream = Box::pin(backend.read());

        while let Some((key, value)) = stream.try_next().await? {
            items.push((key, value));
        }

        list_mock.assert();
        get_mock1.assert();
        get_mock2.assert();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0], (vec![1, 2, 3], vec![4, 5, 6]));
        assert_eq!(items[1], (vec![4, 5, 6, 7], vec![8, 9, 10]));

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_perform_bulk_writes() -> Result<()> {
        let (mut backend, mut server) = create_test_backend().await;

        // Create mocks for two PUT operations (with bucket in path)
        let put_mock1 = server.mock("PUT", "/bucket/!Ldp").with_status(200).create();

        let put_mock2 = server
            .mock("PUT", "/bucket/!6xdze")
            .with_status(200)
            .create();

        // Create a source stream with two items
        use async_stream::try_stream;

        let source_stream = try_stream! {
            yield (vec![1, 2, 3], vec![4, 5, 6]);
            yield (vec![4, 5, 6, 7], vec![8, 9, 10]);
        };

        // Perform the bulk write
        backend.write(source_stream).await?;

        put_mock1.assert();
        put_mock2.assert();

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[ignore] // S3 signing doesn't work with mockito's IP-based URLs
    async fn it_can_perform_s3_bulk_writes() -> Result<()> {
        let (mut backend, mut server) = create_s3_test_backend().await;

        // Create mocks for two PUT operations with signed URLs
        let put_mock1 = server
            .mock("PUT", mockito::Matcher::Any)
            .match_header("x-amz-checksum-sha256", mockito::Matcher::Any)
            .with_status(200)
            .create();

        let put_mock2 = server
            .mock("PUT", mockito::Matcher::Any)
            .match_header("x-amz-checksum-sha256", mockito::Matcher::Any)
            .with_status(200)
            .create();

        // Create a source stream with two items
        use async_stream::try_stream;

        let source_stream = try_stream! {
            yield (vec![1, 2, 3], vec![4, 5, 6]);
            yield (vec![4, 5, 6, 7], vec![8, 9, 10]);
        };

        // Perform the bulk write
        backend.write(source_stream).await?;

        put_mock1.assert();
        put_mock2.assert();

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_integrates_with_memory_backend() -> Result<()> {
        let (mut rest_backend, mut server) = create_test_backend().await;

        // Create a memory backend with some data
        let mut memory_backend = crate::MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();

        // Add some data to the memory backend
        memory_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        memory_backend.set(vec![4, 5, 6, 7], vec![8, 9, 10]).await?;

        // Create mocks for two PUT operations that will happen during transfer
        let put_mock1 = server.mock("PUT", "/bucket/!Ldp").with_status(200).create();

        let put_mock2 = server
            .mock("PUT", "/bucket/!6xdze")
            .with_status(200)
            .create();

        // Create a stream with the memory backend data
        use async_stream::try_stream;
        let custom_stream = try_stream! {
            yield (vec![1, 2, 3], vec![4, 5, 6]);
            yield (vec![4, 5, 6, 7], vec![8, 9, 10]);
        };

        // Transfer data to REST backend
        rest_backend.write(custom_stream).await?;

        put_mock1.assert();
        put_mock2.assert();

        Ok(())
    }
}

/// Local S3 server tests using s3s for end-to-end testing
#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(unused_imports, unused_variables, unused_mut, dead_code)]
mod local_s3_tests {
    use super::*;
    use crate::CborEncoder;
    use s3s::dto::*;
    use s3s::{S3 as S3Trait, S3Request, S3Response, S3Result};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use tokio::net::TcpListener;

    /// Test struct for exercising serialization/deserialization
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestValue {
        data: String,
    }

    impl TestValue {
        fn new(data: impl Into<String>) -> Self {
            Self { data: data.into() }
        }
    }

    #[tokio::test]
    async fn test_local_s3_set_and_get() -> anyhow::Result<()> {
        let service = test_server::start().await?;

        // Note: We use no auth for the local test server since S3 signing doesn't work
        // with IP-based URLs (it creates bucket.127.0.0.1 which is invalid).
        // The real S3 signing is tested with mockito unit tests and the environment-based
        // integration tests with real S3/R2 endpoints.
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
    async fn test_local_s3_multiple_operations() -> anyhow::Result<()> {
        let service = test_server::start().await?;

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
    async fn test_local_s3_large_value() -> anyhow::Result<()> {
        let service = test_server::start().await?;

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
}

#[cfg(all(any(test, feature = "test-utils"), not(target_arch = "wasm32")))]
#[allow(unused_imports, unused_variables, unused_mut, dead_code)]
/// S3-compatible test server for integration testing.
///
/// This module provides a simple in-memory S3-compatible server
/// for testing S3 storage backend functionality.
pub mod test_server {
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures_util::TryStreamExt;
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
            let e_tag_str = format!("{:x}", md5::compute(&data));

            let stored = StoredObject {
                data,
                content_type,
                e_tag: e_tag_str.clone(),
                last_modified: Timestamp::from(SystemTime::now()),
            };

            let mut buckets = self.get_or_create_bucket(&bucket).await;
            if let Some(bucket_contents) = buckets.get_mut(&bucket) {
                bucket_contents.insert(key, stored);
            }

            let output = PutObjectOutput {
                e_tag: Some(ETag::Strong(e_tag_str)),
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
    }

    /// Start a local S3-compatible test server.
    ///
    /// Returns a handle that can be used to get the endpoint URL and stop the server.
    pub async fn start() -> anyhow::Result<Service> {
        use std::sync::Arc;

        let storage = InMemoryS3::default();

        let service = Arc::new(S3ServiceBuilder::new(storage.clone()).build());

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

// ============================================================================
// Integration Tests (requires s3_integration_tests feature and env vars)
// ============================================================================

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
///   cargo test s3_integration_tests --features s3_integration_tests
/// ```
///
/// Or use with MinIO locally:
/// ```bash
/// # Start MinIO
/// docker run -p 9000:9000 -p 9001:9001 \
///   -e "MINIO_ROOT_USER=minioadmin" \
///   -e "MINIO_ROOT_PASSWORD=minioadmin" \
///   minio/minio server /data --console-address ":9001"
///
/// # Create a bucket (using mc client or MinIO console)
/// # Then run tests:
/// R2S3_HOST=http://localhost:9000 \
///   R2S3_REGION=us-east-1 \
///   R2S3_BUCKET=test-bucket \
///   R2S3_ACCESS_KEY_ID=minioadmin \
///   R2S3_SECRET_ACCESS_KEY=minioadmin \
///   cargo test s3_integration_tests --features s3_integration_tests
/// ```
#[cfg(all(test, feature = "s3_integration_tests"))]
mod s3_integration_tests {
    use super::*;
    use anyhow::Result;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::*;

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
    async fn test_s3_set_and_get() -> Result<()> {
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
    async fn test_s3_get_missing_key() -> Result<()> {
        let backend = create_s3_backend_from_env()?;

        // Try to get a key that doesn't exist
        let key = b"nonexistent-key-12345".to_vec();
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, None);

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn test_s3_overwrite_value() -> Result<()> {
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
    async fn test_s3_large_value() -> Result<()> {
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
    async fn test_s3_multiple_keys() -> Result<()> {
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
    async fn test_s3_binary_data() -> Result<()> {
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
    async fn test_s3_checksum_verification() -> Result<()> {
        let mut backend = create_s3_backend_from_env()?;

        let key = b"test-key-checksum".to_vec();
        let value = b"data-to-checksum".to_vec();

        // The backend should automatically calculate and include checksums for S3
        backend.set(key.clone(), value.clone()).await?;

        // Retrieve and verify the data is intact
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn test_s3_bulk_operations() -> Result<()> {
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
    async fn test_s3_without_prefix() -> Result<()> {
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
    async fn test_s3_encoded_key_segments() -> Result<()> {
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
    async fn test_s3_fully_encoded_key() -> Result<()> {
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
    async fn test_s3_multi_segment_mixed_encoding() -> Result<()> {
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
    async fn test_s3_without_prefix_encoded_key() -> Result<()> {
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
}
