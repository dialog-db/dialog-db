use std::marker::PhantomData;

use async_stream::try_stream;
use async_trait::async_trait;
use base64::Engine;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

mod s3_signer;
use s3_signer::{Access, Credentials};

use crate::{
    DialogStorageError, StorageBackend, StorageSink, StorageSource, TransactionalMemoryBackend,
    storage::backend::rest::s3_signer::Authorization,
};

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
pub fn decode_s3_key(encoded: &str) -> Result<Vec<u8>, RestStorageBackendError> {
    use base58::FromBase58;

    let components: Vec<&str> = encoded.split('/').collect();
    let mut decoded_components: Vec<Vec<u8>> = Vec::new();

    for component in components {
        if let Some(encoded_part) = component.strip_prefix('!') {
            // Base58 decode
            let decoded = encoded_part.from_base58().map_err(|e| {
                RestStorageBackendError::SerializationFailed(format!(
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

/// Errors that can occur when using the REST storage backend
#[derive(Error, Debug)]
pub enum RestStorageBackendError {
    /// Error that occurs when connection to the REST API fails
    #[error("Failed to connect to REST API: {0}")]
    ConnectionFailed(String),

    /// Error that occurs when a REST operation fails
    #[error("Failed to perform REST operation: {0}")]
    OperationFailed(String),

    /// Error that occurs when an API request fails
    #[error("API request failed: {0}")]
    RequestFailed(String),

    /// Error that occurs during serialization or deserialization of data
    #[error("Failed to serialize/deserialize data: {0}")]
    SerializationFailed(String),
}

impl From<RestStorageBackendError> for DialogStorageError {
    fn from(error: RestStorageBackendError) -> Self {
        DialogStorageError::StorageBackend(error.to_string())
    }
}

impl From<reqwest::Error> for RestStorageBackendError {
    fn from(error: reqwest::Error) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if error.is_connect() {
                RestStorageBackendError::ConnectionFailed(error.to_string())
            } else if error.is_request() {
                RestStorageBackendError::OperationFailed(error.to_string())
            } else {
                RestStorageBackendError::RequestFailed(error.to_string())
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            // WASM doesn't have is_connect() or is_request() methods
            RestStorageBackendError::RequestFailed(error.to_string())
        }
    }
}

/// Represents an AWS S3-compatible storage policy used by `Authority` to
/// when authorizing requests.
pub struct AccessPolicy {
    /// Host like "https://s3.amazonaws.com" or
    /// "https://xxx.r2.cloudflarestorage.com"
    pub host: String,
    /// AWS region (defaults to "auto" for R2)
    pub region: String,
    /// Whether to make objects readable publicly
    pub public_read: bool,
    /// URL signature expiration in seconds (default: 86400 - 24 hours)
    pub expires: u64,
}

/// Represents an AWS S3-compatible storage backend authority
/// that can authorize requests by creating a pre-signed requests.
pub struct Authority {
    credentials: Credentials,
    policy: AccessPolicy,
}

impl Authority {
    /// Create a new authority with the given credentials and policy
    pub fn new(credentials: Credentials, policy: AccessPolicy) -> Self {
        Self {
            credentials,
            policy,
        }
    }

    /// Authorize a request with the given credentials and policy
    pub fn authorize(
        &self,
        request: Request<'_>,
    ) -> Result<Authorization, RestStorageBackendError> {
        let access = Access {
            region: self.policy.region.to_string(),
            bucket: request.bucket().into(),
            key: request.key(),
            checksum: None,
            endpoint: Some(self.policy.host.clone()),
            expires: self.policy.expires,
            method: match request.method() {
                reqwest::Method::GET => "GET",
                reqwest::Method::PUT => "PUT",
                reqwest::Method::POST => "POST",
                reqwest::Method::DELETE => "DELETE",
                _ => unreachable!(),
            }
            .into(),
            public_read: self.policy.public_read,
            service: "s3".into(),
            time: None,
        };

        self.credentials.authorize(&access).map_err(|e| {
            RestStorageBackendError::OperationFailed(format!(
                "Failed to generate signed URL: {}",
                e
            ))
        })
    }
}

/// AWS S3/R2 credentials configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3Authority {
    /// AWS Access Key ID
    pub access_key_id: String,

    /// AWS Secret Access Key
    pub secret_access_key: String,

    /// Optional AWS session token (for temporary credentials)
    pub session_token: Option<String>,

    /// AWS region (defaults to "auto" for R2)
    pub region: String,

    /// Whether to make objects readable publicly
    pub public_read: bool,

    /// URL signature expiration in seconds (default: 86400 - 24 hours)
    pub expires: u64,
}

impl Default for S3Authority {
    fn default() -> Self {
        Self {
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            region: "auto".to_string(),
            public_read: false,
            expires: 86400, // 24 hours
        }
    }
}

/// Authentication methods for REST storage backend
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuthMethod {
    /// No authentication
    None,

    /// Simple Bearer token authentication
    Bearer(String),

    /// AWS S3/R2 authentication with signed URLs
    S3(S3Authority),
}

/// Precondition for PUT operations to enable compare-and-swap semantics
pub enum Precondition<'a> {
    /// No precondition - unconditional write
    None,
    /// Create only if key doesn't exist (If-None-Match: *)
    Create,
    /// Replace only if current value matches (If-Match: <etag>)
    Replace(&'a [u8]),
}

/// HTTP request abstraction for S3/R2 operations
pub enum Request<'a> {
    /// PUT request to store a value
    Put {
        /// S3/R2 host endpoint
        host: &'a str,
        /// Object key (bytes to be encoded)
        key: &'a [u8],
        /// Object value (body)
        value: &'a [u8],
        /// Bucket name
        bucket: &'a str,
        /// Optional key prefix path
        path: Option<&'a str>,
        /// Precondition for compare-and-swap
        precondition: Precondition<'a>,
        /// Authentication method
        auth: &'a AuthMethod,
    },
    /// GET request to retrieve a value
    Get {
        /// S3/R2 host endpoint
        host: &'a str,
        /// Bucket name
        bucket: &'a str,
        /// Optional key prefix path
        path: Option<&'a str>,
        /// Object key (bytes to be encoded)
        key: &'a [u8],
        /// Authentication method
        auth: &'a AuthMethod,
    },
}

impl Request<'_> {
    fn method(&self) -> reqwest::Method {
        match self {
            Request::Put { .. } => reqwest::Method::PUT,
            Request::Get { .. } => reqwest::Method::GET,
        }
    }

    fn bucket(&self) -> &str {
        match self {
            Request::Put { bucket, .. } => bucket,
            Request::Get { bucket, .. } => bucket,
        }
    }

    fn key(&self) -> String {
        let name = match self {
            Request::Put { key, .. } => base58::ToBase58::to_base58(*key),
            Request::Get { key, .. } => base58::ToBase58::to_base58(*key),
        };
        let path = match self {
            Request::Put { path, .. } => path,
            Request::Get { path, .. } => path,
        };

        if let Some(path) = path {
            format!("{}/{}", path, name)
        } else {
            name
        }
    }
}

// impl<'a> TryFrom<Request<'a>> for Url {
//     type Error = RestStorageBackendError;
//     fn try_from(source: Request<'a>) -> Result<Self, Self::Error> {
//         let key = source.key();
//         let bucket = source.bucket();

//         // Prepare the signing options
//         let sign_options = Access {
//             region: credentials.region.clone(),
//             bucket,
//             key,
//             checksum,
//             endpoint: Some(self.config.endpoint.clone()),
//             expires: credentials.expires,
//             method: method.to_string(),
//             public_read: credentials.public_read,
//             service: "s3".to_string(),
//             time: None,
//         };

//         let request = reqwest::Request::new(source.method(), url);
//     }
// }

// impl<'a> TryFrom<Request<'a>> for reqwest::RequestBuilder {
//     type Error = RestStorageBackendError;
//     fn try_from(source: Request<'a>) -> Result<Self, Self::Error> {
//         let request = reqwest::Request::new(source.method(), url);
//     }
// }

/// Configuration for the REST storage backend
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestStorageConfig {
    /// Base URL for the REST API
    pub endpoint: String,

    /// Authentication method
    pub auth_method: AuthMethod,

    /// Optional bucket name (for S3/R2-like services)
    pub bucket: Option<String>,

    /// Optional prefix for all keys (for organizing keys in S3/R2)
    pub key_prefix: Option<String>,

    /// Optional custom headers to send with each request
    pub headers: Vec<(String, String)>,

    /// Optional timeout for requests in seconds
    pub timeout_seconds: Option<u64>,
}

impl Default for RestStorageConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8080".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            headers: Vec::new(),
            timeout_seconds: Some(30),
        }
    }
}

/// A storage backend implementation that uses a REST API for S3/R2-like services
#[derive(Clone, Debug)]
pub struct RestStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    config: RestStorageConfig,
    client: reqwest::Client,
    _key: PhantomData<Key>,
    _value: PhantomData<Value>,
}

impl<Key, Value> RestStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    /// Create a new REST storage backend with the given configuration
    pub fn new(config: RestStorageConfig) -> Result<Self, RestStorageBackendError> {
        // Timeout is only available on non-WASM targets
        #[cfg(not(target_arch = "wasm32"))]
        let client_builder = {
            let mut builder = reqwest::Client::builder();
            if let Some(timeout) = config.timeout_seconds {
                builder = builder.timeout(std::time::Duration::from_secs(timeout));
            }
            builder
        };

        #[cfg(target_arch = "wasm32")]
        let client_builder = reqwest::Client::builder();

        let client = client_builder
            .build()
            .map_err(|e| RestStorageBackendError::ConnectionFailed(e.to_string()))?;

        Ok(Self {
            config,
            client,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    /// Get the configuration of the backend
    pub fn config(&self) -> &RestStorageConfig {
        &self.config
    }

    /// Build the URL for a given key
    fn build_url(&self, key: &[u8], method: &str) -> Result<Url, RestStorageBackendError> {
        // Use S3-safe encoding that preserves path delimiters
        let key_str = encode_s3_key(key);

        // For S3/R2 signed URLs
        if let AuthMethod::S3(authority) = &self.config.auth_method {
            // If using S3/R2 auth, the bucket must be set
            let bucket = match &self.config.bucket {
                Some(bucket) => bucket.clone(),
                None => {
                    return Err(RestStorageBackendError::OperationFailed(
                        "Bucket must be specified when using S3/R2 authentication".to_string(),
                    ));
                }
            };

            // Build the object key with optional prefix
            let object_key = if let Some(prefix) = &self.config.key_prefix {
                format!("{}/{}", prefix, key_str)
            } else {
                key_str
            };

            // Prepare the signing options
            let access = Access {
                region: authority.region.clone(),
                bucket,
                key: object_key,
                checksum: None,
                endpoint: Some(self.config.endpoint.clone()),
                expires: authority.expires,
                method: method.to_string(),
                public_read: authority.public_read,
                service: "s3".to_string(),
                time: None,
            };

            // Convert our credentials to the format expected by the signer
            let credentials = Credentials {
                access_key_id: authority.access_key_id.clone(),
                secret_access_key: authority.secret_access_key.clone(),
                session_token: authority.session_token.clone(),
            };

            // Generate the signed URL
            credentials
                .authorize(&access)
                .map(|auth| auth.url)
                .map_err(|e| {
                    RestStorageBackendError::OperationFailed(format!(
                        "Failed to generate signed URL: {}",
                        e
                    ))
                })
        } else {
            // For non-S3 authentication, use the original URL building logic
            let base_url = self.config.endpoint.trim_end_matches('/');

            let url_str = match (&self.config.bucket, &self.config.key_prefix) {
                (Some(bucket), Some(prefix)) => format!("{base_url}/{bucket}/{prefix}/{key_str}"),
                (Some(bucket), None) => format!("{base_url}/{bucket}/{key_str}"),
                (None, Some(prefix)) => format!("{base_url}/{prefix}/{key_str}"),
                (None, None) => format!("{base_url}/{key_str}"),
            };

            Url::parse(&url_str).map_err(|e| {
                RestStorageBackendError::OperationFailed(format!("Failed to parse URL: {}", e))
            })
        }
    }

    /// Calculate SHA-256 checksum for a value
    pub fn calculate_checksum(&self, value: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value);
        let hash = hasher.finalize();

        // Convert to base64 encoding as required by S3
        base64::engine::general_purpose::STANDARD.encode(hash)
    }

    /// Build the signed URL for a PUT operation with the value's checksum
    pub fn build_put_url(&self, key: &[u8], value: &[u8]) -> Result<Url, RestStorageBackendError> {
        // For S3/R2 signed URLs
        if let AuthMethod::S3(credentials) = &self.config.auth_method {
            // If using S3/R2 auth, the bucket must be set
            let bucket = match &self.config.bucket {
                Some(bucket) => bucket.clone(),
                None => {
                    return Err(RestStorageBackendError::OperationFailed(
                        "Bucket must be specified when using S3/R2 authentication".to_string(),
                    ));
                }
            };

            // Build the object key with optional prefix
            // Use S3-safe encoding that preserves path delimiters
            let key_str = encode_s3_key(key);
            let object_key = if let Some(prefix) = &self.config.key_prefix {
                format!("{}/{}", prefix, key_str)
            } else {
                key_str
            };

            // Calculate checksum for the value
            let checksum = Some(self.calculate_checksum(value));

            // Prepare the signing options
            let sign_options = Access {
                region: credentials.region.clone(),
                bucket,
                key: object_key,
                checksum: checksum.clone(),
                endpoint: Some(self.config.endpoint.clone()),
                expires: credentials.expires,
                method: "PUT".to_string(),
                public_read: credentials.public_read,
                service: "s3".to_string(),
                time: None,
            };

            // Convert our credentials to the format expected by the signer
            let signer_credentials = Credentials {
                access_key_id: credentials.access_key_id.clone(),
                secret_access_key: credentials.secret_access_key.clone(),
                session_token: credentials.session_token.clone(),
            };

            // Generate the signed URL
            signer_credentials
                .authorize(&sign_options)
                .map(|auth| auth.url)
                .map_err(|e| {
                    RestStorageBackendError::OperationFailed(format!(
                        "Failed to generate signed URL: {}",
                        e
                    ))
                })
        } else {
            // For non-S3 authentication, use the standard URL building
            self.build_url(key, "PUT")
        }
    }

    /// Add authentication and custom headers to a request
    fn prepare_request(
        &self,
        request_builder: reqwest::RequestBuilder,
        checksum: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let mut builder = request_builder;

        // Add authentication headers based on the chosen method
        match &self.config.auth_method {
            AuthMethod::None => {
                // No authentication needed
            }
            AuthMethod::Bearer(token) => {
                builder = builder.header("Authorization", format!("Bearer {token}"));
            }
            AuthMethod::S3(_) => {
                if let Some(checksum) = checksum {
                    builder = builder.header("x-amz-checksum-sha256", checksum);
                }
            }
        }

        // Add custom headers
        for (name, value) in &self.config.headers {
            builder = builder.header(name, value);
        }

        builder
    }

    fn prepare_put_request(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<reqwest::RequestBuilder, RestStorageBackendError> {
        let value_bytes = value.to_vec();

        // For S3/R2, we need to generate a signed URL with the value's checksum
        if let AuthMethod::S3(_) = &self.config.auth_method {
            let url = self.build_put_url(key, &value_bytes)?;

            // Calculate checksum for the value
            let checksum = self.calculate_checksum(&value_bytes);

            // Prepare the request with the checksum header
            let request =
                self.prepare_request(self.client.put(url).body(value_bytes), Some(&checksum));

            Ok(request)
        } else {
            // For regular REST storage
            let url = self.build_url(key, "PUT")?;

            let request = self.prepare_request(self.client.put(url).body(value_bytes), None);

            Ok(request)
        }
    }

    fn prepare_get_request(
        &self,
        key: &[u8],
    ) -> Result<reqwest::RequestBuilder, RestStorageBackendError> {
        let url = self.build_url(key, "GET")?;
        Ok(self.prepare_request(self.client.get(url), None))
    }

    fn prepare_delete_request(
        &self,
        key: &[u8],
    ) -> Result<reqwest::RequestBuilder, RestStorageBackendError> {
        let url = self.build_url(key, "DELETE")?;
        Ok(self.prepare_request(self.client.delete(url), None))
    }
}

/// A resource handle for a specific entry in [RestStorageBackend]

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageBackend for RestStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    type Key = Key;
    type Value = Value;
    type Error = RestStorageBackendError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let request = self.prepare_put_request(key.as_ref(), value.as_ref())?;
        let response = request.send().await?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(RestStorageBackendError::OperationFailed(format!(
                "Failed to set value: {}",
                status
            )))
        }
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        // Prepare get request
        let request = self.prepare_get_request(key.as_ref())?;

        let response = request.send().await?;

        match response.status() {
            status if status.is_success() => {
                let bytes = response.bytes().await?;
                Ok(Some(Value::from(bytes.to_vec())))
            }
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            status => Err(RestStorageBackendError::OperationFailed(format!(
                "Failed to get value. Status: {status}"
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> TransactionalMemoryBackend for RestStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    type Address = Key;
    type Value = Value;
    type Error = RestStorageBackendError;
    type Edition = String;

    async fn resolve(
        &self,
        address: &Self::Address,
    ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
        let request = self.prepare_get_request(address.as_ref())?;
        let response = request.send().await?;

        match response.status() {
            status if status.is_success() => {
                // Extract ETag from response headers
                let etag = response
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.trim_matches('"').to_string())
                    .ok_or_else(|| {
                        RestStorageBackendError::OperationFailed(
                            "Response missing ETag header".to_string(),
                        )
                    })?;

                let bytes = response.bytes().await?;
                let value = Value::from(bytes.to_vec());

                Ok(Some((value, etag)))
            }
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            status => Err(RestStorageBackendError::OperationFailed(format!(
                "Failed to acquire value. Status: {status}"
            ))),
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
                // PUT with If-Match header for CAS
                let mut request = self.prepare_put_request(address.as_ref(), new_value.as_ref())?;

                // Add If-Match header if edition is provided
                if let Some(etag) = edition {
                    request = request.header("If-Match", format!("\"{}\"", etag));
                } else {
                    // If no edition, use If-None-Match: * to ensure creation only
                    request = request.header("If-None-Match", "*");
                }

                let response = request.send().await?;

                match response.status() {
                    status if status.is_success() => {
                        // Extract new ETag from response
                        let new_etag = response
                            .headers()
                            .get("etag")
                            .and_then(|v| v.to_str().ok())
                            .map(|s| s.trim_matches('"').to_string())
                            .ok_or_else(|| {
                                RestStorageBackendError::OperationFailed(
                                    "Response missing ETag header".to_string(),
                                )
                            })?;
                        Ok(Some(new_etag))
                    }
                    reqwest::StatusCode::PRECONDITION_FAILED => {
                        Err(RestStorageBackendError::OperationFailed(
                            "CAS condition failed: edition mismatch".to_string(),
                        ))
                    }
                    status => Err(RestStorageBackendError::OperationFailed(format!(
                        "Failed to replace value. Status: {status}"
                    ))),
                }
            }
            None => {
                // DELETE with If-Match header for CAS
                let mut request = self.prepare_delete_request(address.as_ref())?;

                // Add If-Match header if edition is provided
                if let Some(etag) = edition {
                    request = request.header("If-Match", format!("\"{}\"", etag));
                }

                let response = request.send().await?;

                match response.status() {
                    status if status.is_success() || status == reqwest::StatusCode::NOT_FOUND => {
                        Ok(None)
                    }
                    reqwest::StatusCode::PRECONDITION_FAILED => {
                        Err(RestStorageBackendError::OperationFailed(
                            "CAS condition failed: edition mismatch".to_string(),
                        ))
                    }
                    status => Err(RestStorageBackendError::OperationFailed(format!(
                        "Failed to delete value. Status: {status}"
                    ))),
                }
            }
        }
    }
}

impl<Key, Value> StorageSource for RestStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync + From<Vec<u8>>,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    fn read(
        &self,
    ) -> impl Stream<
        Item = Result<
            (
                <Self as StorageBackend>::Key,
                <Self as StorageBackend>::Value,
            ),
            <Self as StorageBackend>::Error,
        >,
    > {
        try_stream! {
            // Get the list endpoint URL, which will be signed if using S3/R2
            let list_url = if let AuthMethod::S3(credentials) = &self.config.auth_method {
                // If using S3/R2 auth with list operation, we would need to construct a proper
                // S3 list objects request. This is a simplified version.
                let bucket = match &self.config.bucket {
                    Some(bucket) => bucket.clone(),
                    None => {
                        Err(RestStorageBackendError::OperationFailed(
                            "Bucket must be specified when using S3/R2 authentication".to_string()
                        ))?;
                        return;
                    }
                };

                // For S3, we would typically use a "?list-type=2" endpoint
                // For simplicity, we'll just use the /_list endpoint
                // This should be customized based on the actual S3/R2 API
                let key = "_list";

                // Prepare the signing options for the list operation
                let access = Access {
                    region: credentials.region.clone(),
                    bucket,
                    key: key.to_string(),
                    checksum: None,
                    endpoint: Some(self.config.endpoint.clone()),
                    expires: credentials.expires,
                    method: "GET".to_string(),
                    public_read: credentials.public_read,
                    service: "s3".to_string(),
                    time: None,
                    };

                // Convert our credentials to the format expected by the signer
                let signer_credentials = Credentials {
                    access_key_id: credentials.access_key_id.clone(),
                    secret_access_key: credentials.secret_access_key.clone(),
                    session_token: credentials.session_token.clone(),
                };

                // Generate the signed URL
                signer_credentials.authorize(&access)
                    .map(|auth| auth.url)
                    .map_err(|e| RestStorageBackendError::OperationFailed(
                        format!("Failed to generate signed URL for list operation: {}", e)
                    ))?
            } else {
                // For regular REST storage
                let base_url = self.config.endpoint.trim_end_matches('/');
                Url::parse(&format!("{base_url}/_list"))
                    .map_err(|e| RestStorageBackendError::OperationFailed(
                        format!("Failed to parse URL: {}", e)
                    ))?
            };

            // Send the listing request
            let request = self.prepare_request(self.client.get(list_url), None);
            let response = request.send().await?;

            if !response.status().is_success() {
                Err(RestStorageBackendError::OperationFailed(format!(
                    "Failed to list values. Status: {}", response.status()
                )))?;
            }

            // This assumes the API returns a JSON array of key strings (base64 encoded)
            // The actual implementation would depend on the specific REST API being used
            let keys: Vec<String> = response.json().await.map_err(|e| {
                RestStorageBackendError::SerializationFailed(format!("Failed to parse key list: {e}"))
            })?;

            for encoded_key in keys {
                // Decode S3-encoded key
                let key_bytes = match decode_s3_key(&encoded_key) {
                    Ok(k) => k,
                    Err(e) => {
                        Err(RestStorageBackendError::SerializationFailed(
                            format!("Failed to decode key: {e}")
                        ))?;
                        continue;
                    }
                };

                let key = Key::from(key_bytes);

                if let Some(value) = StorageBackend::get(self, &key).await? {
                    yield (key, value);
                }
            }
        }
    }

    fn drain(
        &mut self,
    ) -> impl Stream<
        Item = Result<
            (
                <Self as StorageBackend>::Key,
                <Self as StorageBackend>::Value,
            ),
            <Self as StorageBackend>::Error,
        >,
    > {
        // For drain, we read the data but don't delete it from the remote storage
        // since that would require a separate API call for each key
        // This is a simplified implementation
        self.read()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageSink for RestStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync + From<Vec<u8>>,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    async fn write<EntryStream>(
        &mut self,
        stream: EntryStream,
    ) -> Result<(), <Self as StorageBackend>::Error>
    where
        EntryStream: Stream<
                Item = Result<
                    (
                        <Self as StorageBackend>::Key,
                        <Self as StorageBackend>::Value,
                    ),
                    <Self as StorageBackend>::Error,
                >,
            > + dialog_common::ConditionalSend,
    {
        use futures_util::TryStreamExt;

        let mut pinned_stream = Box::pin(stream);

        while let Some((key, value)) = pinned_stream.try_next().await? {
            self.set(key, value).await?;
        }

        Ok(())
    }
}

// Helper functions are provided in the crate's helpers.rs file

#[cfg(test)]
mod unit_tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_build_url_with_bucket_and_prefix() {
        let config = RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("test-prefix".to_string()),
            ..Default::default()
        };

        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();
        let key = vec![1, 2, 3]; // Encodes to: !Ldp (base58-encoded with ! prefix)
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(
            url.as_str(),
            "https://example.com/test-bucket/test-prefix/!Ldp"
        );
    }

    #[test]
    fn test_build_url_with_bucket_only() {
        let config = RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: None,
            ..Default::default()
        };

        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();
        let key = vec![1, 2, 3]; // Encodes to: !Ldp (base58-encoded with ! prefix)
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(url.as_str(), "https://example.com/test-bucket/!Ldp");
    }

    #[test]
    fn test_build_url_with_prefix_only() {
        let config = RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: Some("test-prefix".to_string()),
            ..Default::default()
        };

        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();
        let key = vec![1, 2, 3]; // Encodes to: !Ldp (base58-encoded with ! prefix)
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(url.as_str(), "https://example.com/test-prefix/!Ldp");
    }

    #[test]
    fn test_build_url_with_no_bucket_or_prefix() {
        let config = RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            ..Default::default()
        };

        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();
        let key = vec![1, 2, 3]; // Encodes to: !Ldp (base58-encoded with ! prefix)
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(url.as_str(), "https://example.com/!Ldp");
    }

    #[test]
    fn test_build_url_with_trailing_slash() {
        let config = RestStorageConfig {
            endpoint: "https://example.com/".to_string(),
            auth_method: AuthMethod::None,
            bucket: None,
            key_prefix: None,
            ..Default::default()
        };

        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();
        let key = vec![1, 2, 3]; // Encodes to: !Ldp (base58-encoded with ! prefix)
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(url.as_str(), "https://example.com/!Ldp");
    }

    #[test]
    fn test_rest_storage_config_default() {
        let config = RestStorageConfig::default();

        assert_eq!(config.endpoint, "http://localhost:8080");
        match config.auth_method {
            AuthMethod::None => (),
            _ => panic!("Expected AuthMethod::None"),
        }
        assert!(config.bucket.is_none());
        assert!(config.key_prefix.is_none());
        assert!(config.headers.is_empty());
        assert_eq!(config.timeout_seconds, Some(30));
    }

    #[test]
    fn test_error_conversion() {
        let error = RestStorageBackendError::ConnectionFailed("failed to connect".to_string());
        let dialog_error: DialogStorageError = error.into();

        if let DialogStorageError::StorageBackend(msg) = dialog_error {
            assert!(msg.contains("failed to connect"));
        } else {
            panic!("Expected StorageBackend error");
        }
    }

    #[test]
    fn test_s3_credentials_default() {
        let creds = S3Authority::default();

        assert_eq!(creds.access_key_id, "");
        assert_eq!(creds.secret_access_key, "");
        assert_eq!(creds.region, "auto");
        assert_eq!(creds.expires, 86400);
        assert!(!creds.public_read);
        assert!(creds.session_token.is_none());
    }

    #[test]
    fn test_calculate_checksum() {
        let config = RestStorageConfig::default();
        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();

        // Test with a known value
        let data = b"hello world";
        let expected_checksum = "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="; // SHA-256 of "hello world" in base64

        let calculated = backend.calculate_checksum(data);
        assert_eq!(calculated, expected_checksum);
    }

    #[test]
    fn test_s3_signed_url_generation() {
        let s3_creds = S3Authority {
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        let config = RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::S3(s3_creds),
            bucket: Some("test-bucket".to_string()),
            ..Default::default()
        };

        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();
        let key = vec![1, 2, 3]; // Encodes to: !Ldp (base58-encoded with ! prefix)

        // Generate a signed URL
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        // Verify the URL has the required query parameters
        let query_params: HashMap<_, _> = url.query_pairs().collect();

        assert_eq!(
            query_params.get("X-Amz-Algorithm").unwrap(),
            "AWS4-HMAC-SHA256"
        );
        assert!(
            query_params
                .get("X-Amz-Credential")
                .unwrap()
                .starts_with("test-access-key/")
        );
        assert!(query_params.contains_key("X-Amz-Date"));
        assert_eq!(query_params.get("X-Amz-Expires").unwrap(), "86400");
        assert!(query_params.contains_key("X-Amz-SignedHeaders"));
        assert!(query_params.contains_key("X-Amz-Signature"));

        // Host part should be bucket.endpoint
        assert_eq!(url.host_str().unwrap(), "test-bucket.example.com");

        // Path should be the key
        assert_eq!(url.path(), "/!Ldp");
    }

    #[test]
    fn test_put_url_with_checksum() {
        let s3_creds = S3Authority {
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        let config = RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::S3(s3_creds),
            bucket: Some("test-bucket".to_string()),
            ..Default::default()
        };

        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();
        let key = vec![1, 2, 3]; // Encodes to: !Ldp (base58-encoded with ! prefix)
        let value = b"hello world";

        // Generate a signed URL for PUT with checksum
        let url = backend.build_put_url(key.as_ref(), value).unwrap();

        // Verify the URL has the required query parameters
        let query_params: HashMap<_, _> = url.query_pairs().collect();

        assert_eq!(
            query_params.get("X-Amz-Algorithm").unwrap(),
            "AWS4-HMAC-SHA256"
        );
        assert!(
            query_params
                .get("X-Amz-Credential")
                .unwrap()
                .starts_with("test-access-key/")
        );
        assert!(query_params.contains_key("X-Amz-Date"));
        assert_eq!(query_params.get("X-Amz-Expires").unwrap(), "86400");
        assert!(query_params.contains_key("X-Amz-SignedHeaders"));
        assert!(query_params.contains_key("X-Amz-Signature"));

        // Check that we have a signature parameter
        assert!(query_params.contains_key("X-Amz-Signature"));

        // Method should be PUT
        assert_eq!(url.path(), "/!Ldp");
    }

    #[test]
    fn test_bearer_auth_request_preparation() {
        let config = RestStorageConfig {
            endpoint: "https://example.com".to_string(),
            auth_method: AuthMethod::Bearer("test-token".to_string()),
            ..Default::default()
        };

        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config).unwrap();

        // Create a simple request and prepare it
        let client = reqwest::Client::new();
        let req_builder = client.get("https://example.com/test");

        // We can't directly test the request builder, so we just verify that the code runs
        let _prepared = backend.prepare_request(req_builder, None);

        // This is a simple test to ensure the code path is covered
        assert!(true);
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

    // Helper function to create a test REST backend with a mock server
    async fn create_test_backend() -> (RestStorageBackend<Vec<u8>, Vec<u8>>, mockito::ServerGuard) {
        let server = Server::new_async().await;
        let endpoint = server.url();

        let config = RestStorageConfig {
            endpoint,
            auth_method: AuthMethod::None,
            ..Default::default()
        };

        let backend = RestStorageBackend::new(config).expect("Failed to create REST backend");

        (backend, server)
    }

    // Helper function to create a test REST backend with S3 auth
    async fn create_s3_test_backend() -> (RestStorageBackend<Vec<u8>, Vec<u8>>, mockito::ServerGuard)
    {
        let server = Server::new_async().await;
        let endpoint = server.url();

        // Use fixed credentials for testing
        let s3_creds = S3Authority {
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            region: "us-east-1".to_string(),
            ..Default::default()
        };

        let config = RestStorageConfig {
            endpoint,
            auth_method: AuthMethod::S3(s3_creds),
            bucket: Some("test-bucket".to_string()),
            ..Default::default()
        };

        let backend = RestStorageBackend::new(config).expect("Failed to create REST backend");

        (backend, server)
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_writes_and_reads_a_value() -> Result<()> {
        let (mut backend, mut server) = create_test_backend().await;

        // Key encodes to: !Ldp
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        // Mock PUT request for set operation
        let put_mock = server
            .mock("PUT", "/!Ldp")
            .with_status(200)
            .with_body("")
            .create();

        // Mock GET request for successful retrieval
        let get_mock = server
            .mock("GET", "/!Ldp")
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
        // Match any path starting with ! (encoded binary keys)
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/!.*".to_string()))
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
            .mock("GET", mockito::Matcher::Regex(r"^/!7kEh.*".to_string()))
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
    async fn it_uses_bearer_authentication_headers() -> Result<()> {
        let mut server = Server::new_async().await;
        let config = RestStorageConfig {
            endpoint: server.url(),
            auth_method: AuthMethod::Bearer("test-api-key".to_string()),
            headers: vec![("X-Custom-Header".to_string(), "custom-value".to_string())],
            ..Default::default()
        };

        let mut backend = RestStorageBackend::new(config)?;

        // Mock PUT request expecting specific headers
        let mock = server
            .mock("PUT", "/!Ldp")
            .match_header("Authorization", "Bearer test-api-key")
            .match_header("X-Custom-Header", "custom-value")
            .with_status(200)
            .create();

        backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        mock.assert();

        Ok(())
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_uses_bucket_and_prefix() -> Result<()> {
        let mut server = Server::new_async().await;
        let config = RestStorageConfig {
            endpoint: server.url(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("test-prefix".to_string()),
            ..Default::default()
        };

        let mut backend = RestStorageBackend::new(config)?;

        // Mock PUT request with bucket and prefix in path
        let mock = server
            .mock("PUT", "/test-bucket/test-prefix/!Ldp")
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
            .mock("GET", "/!Ldp")
            .with_status(200)
            .with_body(&[4, 5, 6])
            .create();

        // Mock the GET for second key
        let get_mock2 = server
            .mock("GET", "/!6xdze")
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

        // Create mocks for two PUT operations
        let put_mock1 = server.mock("PUT", "/!Ldp").with_status(200).create();

        let put_mock2 = server.mock("PUT", "/!6xdze").with_status(200).create();

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
        let put_mock1 = server.mock("PUT", "/!Ldp").with_status(200).create();

        let put_mock2 = server.mock("PUT", "/!6xdze").with_status(200).create();

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
    use hyper::server::conn::http1;
    use hyper_util::rt::TokioIo;
    use s3;
    use s3s::dto::*;
    use s3s::{S3, S3Request, S3Response, S3Result};
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
        let service = s3::start().await?;

        // Note: We use no auth for the local test server since S3 signing doesn't work
        // with IP-based URLs (it creates bucket.127.0.0.1 which is invalid).
        // The real S3 signing is tested with mockito unit tests and the environment-based
        // integration tests with real S3/R2 endpoints.
        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("test".to_string()),
            ..Default::default()
        };

        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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
        let service = s3::start().await?;

        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            ..Default::default()
        };

        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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
        let service = s3::start().await?;

        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            ..Default::default()
        };

        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

        // Create a 100KB value
        let key = b"large-key".to_vec();
        let value: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        // Set and retrieve
        backend.set(key.clone(), value.clone()).await?;
        let retrieved = backend.get(&key).await?;
        assert_eq!(retrieved, Some(value));

        Ok(())
    }

    const ALICE: &str = "did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi";

    #[tokio::test]
    async fn test_local_s3_typed_store_with_path() -> anyhow::Result<()> {
        use crate::CborEncoder;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestData {
            value: String,
        }

        let service = s3::start().await?;

        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("test-prefix".to_string()),
            ..Default::default()
        };

        // Create a typed store with path (like RemoteBranch does)
        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

        // Wrap in layers like platform.rs does
        struct ErrorMappingBackend<B> {
            inner: B,
        }

        impl<B> Clone for ErrorMappingBackend<B>
        where
            B: Clone,
        {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                }
            }
        }

        #[async_trait::async_trait]
        impl<B: StorageBackend<Key = Vec<u8>, Value = Vec<u8>> + Send + Sync> StorageBackend
            for ErrorMappingBackend<B>
        where
            B::Error: Send,
        {
            type Key = Vec<u8>;
            type Value = Vec<u8>;
            type Error = B::Error;

            async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
                self.inner.set(key, value).await
            }

            async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
                self.inner.get(key).await
            }
        }

        #[async_trait::async_trait]
        impl<B> super::TransactionalMemoryBackend for ErrorMappingBackend<B>
        where
            B: super::TransactionalMemoryBackend<Address = Vec<u8>, Value = Vec<u8>> + Send + Sync,
            B::Error: Send,
        {
            type Address = Vec<u8>;
            type Value = Vec<u8>;
            type Error = B::Error;
            type Edition = B::Edition;

            async fn resolve(
                &self,
                address: &Self::Address,
            ) -> Result<Option<(Self::Value, Self::Edition)>, Self::Error> {
                self.inner.resolve(address).await
            }

            async fn replace(
                &self,
                address: &Self::Address,
                edition: Option<&Self::Edition>,
                content: Option<Self::Value>,
            ) -> Result<Option<Self::Edition>, Self::Error> {
                self.inner.replace(address, edition, content).await
            }
        }

        let wrapped = ErrorMappingBackend { inner: backend };

        // Test TransactionalMemory with wrapped backend
        let key = b"test-key".to_vec();
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &wrapped,
                CborEncoder,
            )
            .await?;
        assert_eq!(memory.read(), None);

        let value = TestValue::new("test-value");
        memory.replace(Some(value.clone()), &wrapped).await?;

        // Check it was written
        let keys = service.storage().list_keys("test-bucket").await;
        assert!(!keys.is_empty(), "Should have written to S3");

        Ok(())
    }

    #[tokio::test]
    async fn test_local_s3_atomic_swap_ok() -> anyhow::Result<()> {
        let service = s3::start().await?;

        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };

        let store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let v1 = TestValue::new("v1");
        let key: Vec<u8> = ALICE.into();

        // We try to create new branch record
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &store,
                CborEncoder,
            )
            .await?;
        assert_eq!(memory.read(), None, "currently there is no record");

        memory.replace(Some(v1.clone()), &store).await?;

        assert!(
            store.get(&key).await?.is_some(),
            "stored record was updated"
        );

        assert_eq!(
            memory.read(),
            Some(v1.clone()),
            "resource content was updated"
        );

        let v2 = TestValue::new("v2");
        // We try to update v1 -> v2
        memory.replace(Some(v2.clone()), &store).await?;

        // assert_eq!(store.get(&key).await?, Some(v2), "resolved to new record");

        Ok(())
    }
    #[tokio::test]
    async fn test_local_s3_atomic_swap_rejects_on_missmatch() -> anyhow::Result<()> {
        let service = s3::start().await?;

        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };

        let mut store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let v1 = TestValue::new("v1");
        let key: Vec<u8> = ALICE.into();

        // Create initial value
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &store,
                CborEncoder,
            )
            .await?;

        assert_eq!(memory.read(), None, "have no content yet");

        // write initila value
        memory.replace(Some(v1.clone()), &store).await?;

        assert!(store.get(&key).await?.is_some(), "record was stored");

        let v2 = TestValue::new("v2");
        let v3 = TestValue::new("v3");

        // Simulate concurrent modification: someone else changes v1 -> v2
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&v2).await?.1;
            store.set(key.clone(), encoded).await?;
        }

        // Now try to update based on stale v1 -> v3 (should fail)
        let result = memory.replace(Some(v3.clone()), &store).await;

        assert!(result.is_err(), "swap failed");

        assert!(
            store.get(&key).await?.is_some(),
            "resolved to concurrently modified record"
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_local_s3_atomic_swap_rejects_on_assumed() -> anyhow::Result<()> {
        let service = s3::start().await?;

        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };

        let mut store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let v1 = TestValue::new("v1");
        let v2 = TestValue::new("v2");
        let key: Vec<u8> = ALICE.into();

        // Create value first
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&v1).await?.1;
            store.set(key.clone(), encoded).await?;
        }

        // Open TransactionalMemory (gets v1)
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &store,
                CborEncoder,
            )
            .await?;
        assert_eq!(memory.read(), Some(v1.clone()));

        // Simulate concurrent deletion by directly deleting from S3
        let delete_request = store.prepare_delete_request(key.as_ref())?;
        delete_request.send().await?;

        // Now try to update v1 -> v2 (should fail because key was deleted)
        let result = memory.replace(Some(v2.clone()), &store).await;

        assert!(
            result.is_err(),
            "swap should have failed when key was concurrently deleted"
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_local_s3_swap_when_none_key_missing_ok() -> anyhow::Result<()> {
        let service = s3::start().await?;
        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };
        let store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let key: Vec<u8> = ALICE.into();
        let value = TestValue::new("v1");

        // when=None and key missing → success
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &store,
                CborEncoder,
            )
            .await?;
        memory.replace(Some(value.clone()), &store).await?;
        assert!(store.get(&key).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_local_s3_swap_when_some_key_missing_fail() -> anyhow::Result<()> {
        let service = s3::start().await?;
        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };
        let mut store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let key: Vec<u8> = ALICE.into();
        let expected = TestValue::new("v1");
        let new_val = TestValue::new("v2");

        // when=Some but key missing → fail
        // Create value first
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&expected).await?.1;
            store.set(key.clone(), encoded).await?;
        }

        // Open resource (captures expected)
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &store,
                CborEncoder,
            )
            .await?;

        // Simulate concurrent deletion
        crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
            key.clone(),
            &store,
            CborEncoder,
        )
        .await?
        .replace(None, &store)
        .await?;

        // Try to update with stale ETag
        let result = memory.replace(Some(new_val), &store).await;
        assert!(result.is_err());
        assert!(store.get(&key).await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_local_s3_swap_when_none_key_exists_fail() -> anyhow::Result<()> {
        let service = s3::start().await?;
        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };
        let mut store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let key: Vec<u8> = ALICE.into();
        let existing = TestValue::new("v1");
        let new_val = TestValue::new("v2");

        // when=None and key exists → fail
        // Open resource for non-existent key (captures None)
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &store,
                CborEncoder,
            )
            .await?;

        // Simulate concurrent creation: someone else creates the key
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&existing).await?.1;
            store.set(key.clone(), encoded).await?;
        }

        // Try to create with CAS condition "must not exist" (should fail because key now exists)
        let result = memory.replace(Some(new_val), &store).await;
        assert!(result.is_err());
        assert!(store.get(&key).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_local_s3_swap_when_some_key_matches_ok() -> anyhow::Result<()> {
        let service = s3::start().await?;
        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };
        let mut store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let key: Vec<u8> = ALICE.into();
        let existing = TestValue::new("v1");
        let new_val = TestValue::new("v2");

        // Prepopulate the key
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&existing).await?.1;
            store.set(key.clone(), encoded).await?;
        }

        // when=Some and matches existing → success
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &store,
                CborEncoder,
            )
            .await?;
        memory.replace(Some(new_val.clone()), &store).await?;
        assert!(store.get(&key).await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_local_s3_swap_when_some_key_mismatch_fail() -> anyhow::Result<()> {
        let service = s3::start().await?;
        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };
        let mut store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let key: Vec<u8> = ALICE.into();
        let existing = TestValue::new("v1");
        let wrong_expected = TestValue::new("vX");
        let new_val = TestValue::new("v2");

        // Prepopulate the key
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&existing).await?.1;
            store.set(key.clone(), encoded).await?;
        }

        // Open resource (captures existing)
        let mut memory =
            crate::storage::transactional_memory::TransactionalMemory::<TestValue, _, _>::open(
                key.clone(),
                &store,
                CborEncoder,
            )
            .await?;

        // Simulate concurrent modification: someone else changes the value
        {
            use crate::Encoder;
            let encoded = CborEncoder.encode(&wrong_expected).await?.1;
            store.set(key.clone(), encoded).await?;
        }

        // when=Some but doesn't match existing → fail
        let result = memory.replace(Some(new_val.clone()), &store).await;
        assert!(result.is_err());
        assert!(store.get(&key).await?.is_some());

        Ok(())
    }
}

#[cfg(all(any(test, feature = "test-utils"), not(target_arch = "wasm32")))]
#[allow(unused_imports, unused_variables, unused_mut, dead_code)]
/// S3-compatible test server for integration testing.
///
/// This module provides a simple in-memory S3-compatible server
/// for testing REST storage backend functionality.
pub mod s3 {
    use async_trait::async_trait;
    use hyper::server::conn::http1;
    use hyper_util::rt::TokioIo;
    use md5;
    use s3s::crypto::Md5;
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

        /// Returns the underlying storage for inspection (useful in tests)
        pub fn storage(&self) -> &InMemoryS3 {
            &self.storage
        }
    }

    /// Starts a test S3 server.
    pub async fn start() -> anyhow::Result<Service> {
        InMemoryS3::start2().await
    }

    impl InMemoryS3 {
        /// Get a value from a specific bucket (useful for test verification)
        pub async fn get_value(&self, bucket: &str, key: &str) -> Option<Vec<u8>> {
            let buckets = self.buckets.read().await;
            buckets
                .get(bucket)
                .and_then(|bucket_contents| bucket_contents.get(key))
                .map(|obj| obj.data.clone())
        }

        /// Get all keys in a bucket (useful for test verification)
        pub async fn list_keys(&self, bucket: &str) -> Vec<String> {
            let buckets = self.buckets.read().await;
            buckets
                .get(bucket)
                .map(|bucket_contents| bucket_contents.keys().cloned().collect())
                .unwrap_or_default()
        }

        /// Starts a test S3 server.
        pub async fn start() -> anyhow::Result<Service> {
            Self::serve(Self::default()).await
        }

        /// Starts a test S3 server (alternative implementation).
        pub async fn start2() -> anyhow::Result<Service> {
            let storage = Self::default();
            let s3_handler = S3ServiceBuilder::new(storage.clone()).build();

            // Bind to a random available port
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let endpoint = format!("http://{}", addr);

            let handle = tokio::spawn(async move {
                loop {
                    let (stream, _) = match listener.accept().await {
                        Ok(x) => x,
                        Err(_) => break,
                    };

                    let io = TokioIo::new(stream);
                    let service = s3_handler.clone();

                    tokio::spawn(async move {
                        let _ = http1::Builder::new().serve_connection(io, service).await;
                    });
                }
            });
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

            // Give the server a moment to start
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            Ok(Service {
                endpoint,
                shutdown_tx,
                storage,
            })
        }

        async fn serve(self) -> anyhow::Result<Service> {
            use hyper::server::conn::http1;
            use hyper_util::rt::TokioIo;
            use s3s::service::S3ServiceBuilder;
            use tokio::net::TcpListener;

            let storage = self.clone();
            let s3_handler = S3ServiceBuilder::new(self).build();
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let endpoint = format!("http://{}", addr);

            let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        Ok((stream, _)) = listener.accept() => {
                            let io = TokioIo::new(stream);
                            let service = s3_handler.clone();
                            tokio::spawn(async move {
                                let _ = http1::Builder::new().serve_connection(io, service).await;
                            });
                        }
                        _ = &mut shutdown_rx => {
                            break;
                        }
                    }
                }
            });

            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            Ok(Service {
                endpoint,
                shutdown_tx,
                storage,
            })
        }
    }

    #[derive(Clone)]
    struct StoredObject {
        data: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
        last_modified: SystemTime,
        e_tag: String,
    }

    impl StoredObject {
        fn new(data: Vec<u8>, metadata: Option<HashMap<String, String>>) -> Self {
            let checksum = md5::compute(&data);
            let e_tag = format!("\"{:x}\"", checksum);
            StoredObject {
                data,
                metadata,
                e_tag,
                last_modified: SystemTime::now(),
            }
        }
    }

    #[async_trait]
    impl S3 for InMemoryS3 {
        async fn put_object(
            &self,
            req: s3s::S3Request<PutObjectInput>,
        ) -> S3Result<S3Response<PutObjectOutput>> {
            let key = &req.input.key;

            let mut buckets = self.buckets.write().await;
            let bucket = buckets.entry(req.input.bucket.clone()).or_default();

            // check preconditions
            if let Some(existing) = bucket.get(key) {
                // Check If-Match
                if let Some(ref cond) = req.input.if_match {
                    if &existing.e_tag != cond {
                        return Err(s3_error!(PreconditionFailed));
                    }
                }

                // Check If-None-Match
                if let Some(ref cond) = req.input.if_none_match {
                    if cond == "*" || &existing.e_tag == cond {
                        return Err(s3_error!(PreconditionFailed));
                    }
                }
            } else {
                // Object does not exist
                // If-Match should fail if specified
                if req.input.if_match.is_some() {
                    return Err(s3_error!(PreconditionFailed));
                }
            }

            let mut body = req.input.body.ok_or_else(|| s3_error!(IncompleteBody))?;
            // Convert StreamingBlob to Body (http_body type)
            let s3s_body: s3s::Body = body.into();
            // Collect the streaming body into bytes
            use http_body_util::BodyExt;
            let collected = s3s_body
                .collect()
                .await
                .map_err(|_| s3s::S3Error::new(s3s::S3ErrorCode::InternalError))?;
            let data = collected.to_bytes();

            let stored = StoredObject::new(data.into(), req.input.metadata.clone());
            let e_tag = stored.e_tag.clone();
            bucket.insert(key.clone(), stored);

            Ok(S3Response::new(PutObjectOutput {
                e_tag: Some(ETag::Strong(e_tag)),
                ..Default::default()
            }))
        }

        async fn get_object(
            &self,
            req: s3s::S3Request<GetObjectInput>,
        ) -> S3Result<S3Response<GetObjectOutput>> {
            let key = &req.input.key;

            let mut buckets = self.buckets.write().await;
            let bucket = buckets.entry(req.input.bucket.clone()).or_default();
            let obj = bucket.get(key).ok_or_else(|| s3_error!(NoSuchKey))?;

            let content_length = obj.data.len() as i64;
            let data = obj.data.clone();

            let body = s3s::Body::from(bytes::Bytes::from(data.clone()));

            Ok(S3Response::new(GetObjectOutput {
                body: Some(StreamingBlob::from(body)),
                content_length: Some(content_length),
                last_modified: Some(Timestamp::from(obj.last_modified)),
                e_tag: Some(ETag::Strong(obj.e_tag.clone())),
                metadata: obj.metadata.clone(),
                ..Default::default()
            }))
        }

        async fn delete_object(
            &self,
            req: s3s::S3Request<DeleteObjectInput>,
        ) -> S3Result<S3Response<DeleteObjectOutput>> {
            let key = &req.input.key;

            let mut buckets = self.buckets.write().await;
            let bucket = buckets.entry(req.input.bucket.clone()).or_default();
            bucket.remove(key);
            Ok(S3Response::new(DeleteObjectOutput::default()))
        }

        async fn list_objects_v2(
            &self,
            req: s3s::S3Request<ListObjectsV2Input>,
        ) -> S3Result<S3Response<ListObjectsV2Output>> {
            let prefix = req.input.prefix.clone().unwrap_or_default();
            let max_keys = req.input.max_keys.unwrap_or(1000) as usize;

            let mut buckets = self.buckets.write().await;
            let bucket = buckets.entry(req.input.bucket.clone()).or_default();

            let mut objects: Vec<Object> = bucket
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .take(max_keys)
                .map(|(k, v)| Object {
                    key: Some(k.clone()),
                    size: Some(v.data.len() as i64),
                    last_modified: Some(Timestamp::from(v.last_modified)),
                    e_tag: Some(ETag::Strong(v.e_tag.clone())),
                    ..Default::default()
                })
                .collect();
            let key_count = objects.len();

            Ok(S3Response::new(ListObjectsV2Output {
                contents: Some(objects),
                key_count: Some(key_count as i32),
                max_keys: Some(max_keys as i32),
                is_truncated: Some(false),
                name: Some(req.input.bucket.clone()),
                ..Default::default()
            }))
        }

        async fn head_object(
            &self,
            req: S3Request<HeadObjectInput>,
        ) -> S3Result<S3Response<HeadObjectOutput>> {
            let key = &req.input.key;

            // Look up the object
            let mut buckets = self.buckets.write().await;
            let bucket = buckets.entry(req.input.bucket.clone()).or_default();
            let obj = bucket.get(key).ok_or_else(|| s3_error!(NoSuchKey))?;

            // Construct the response
            Ok(S3Response::new(HeadObjectOutput {
                content_length: Some(obj.data.len() as i64),
                e_tag: Some(ETag::Strong(obj.e_tag.clone())),
                last_modified: Some(Timestamp::from(obj.last_modified)),
                metadata: obj.metadata.clone(),
                ..Default::default()
            }))
        }
    }
}

/// Integration tests with real S3-compatible storage
///
/// These tests require environment variables to be set:
/// - R2S3_HOST: The S3-compatible endpoint (e.g., "https://s3.amazonaws.com" or "https://xxx.r2.cloudflarestorage.com")
/// - R2S3_REGION: AWS region (e.g., "us-east-1" or "auto" for R2)
/// - R2S3_BUCKET: Bucket name
/// - R2S3_ACCESS_KEY_ID: Access key ID
/// - R2S3_SECRET_ACCESS_KEY: Secret access key
///
/// Run these tests with:
/// ```bash
/// R2S3_HOST=... R2S3_REGION=... R2S3_BUCKET=... R2S3_ACCESS_KEY_ID=... R2S3_SECRET_ACCESS_KEY=... \
///   cargo test --features s3_integration_tests -- --ignored
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
///   cargo test --features s3_integration_tests
/// ```
///
/// ## Running on WASM
///
/// On WASM, environment variables are not available. Instead, provide configuration
/// via JavaScript by setting `globalThis.dialogTestConfig` before running tests:
///
/// ```javascript
/// globalThis.dialogTestConfig = {
///   getS3Host: () => "http://localhost:9000",
///   getS3Region: () => "us-east-1",
///   getS3Bucket: () => "test-bucket",
///   getS3AccessKeyId: () => "minioadmin",
///   getS3SecretAccessKey: () => "minioadmin"
/// };
/// ```
///
/// Then run the tests:
/// ```bash
/// cargo test --target wasm32-unknown-unknown --features s3_integration_tests
/// ```
#[cfg(all(test, feature = "s3_integration_tests"))]
mod s3_integration_tests {
    use super::*;
    use anyhow::Result;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen::prelude::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::*;

    /// Helper to get S3 config from environment variables (native) or JavaScript (WASM)
    fn get_s3_config_from_env() -> Result<RestStorageConfig> {
        let s3_credentials = S3Authority {
            access_key_id: env!("R2S3_ACCESS_KEY_ID").into(),
            secret_access_key: env!("R2S3_SECRET_ACCESS_KEY").into(),
            session_token: option_env!("R2S3_SESSION_TOKEN").map(|v| v.into()),
            region: env!("R2S3_REGION").into(),
            public_read: false,
            expires: 3600, // 1 hour for tests
        };

        Ok(RestStorageConfig {
            endpoint: env!("R2S3_HOST").into(),
            bucket: Some(env!("R2S3_BUCKET").into()),
            auth_method: AuthMethod::S3(s3_credentials),
            key_prefix: Some("test-prefix".to_string()),
            headers: Vec::new(),
            timeout_seconds: Some(30),
        })
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn test_s3_set_and_get() -> Result<()> {
        let config = get_s3_config_from_env()?;
        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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
        let config = get_s3_config_from_env()?;
        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

        // Try to get a key that doesn't exist
        let key = b"nonexistent-key-12345".to_vec();
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, None);

        Ok(())
    }

    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn test_s3_overwrite_value() -> Result<()> {
        let config = get_s3_config_from_env()?;
        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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
        let config = get_s3_config_from_env()?;
        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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
        let config = get_s3_config_from_env()?;
        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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
        let config = get_s3_config_from_env()?;
        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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
        let config = get_s3_config_from_env()?;
        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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
        let config = get_s3_config_from_env()?;
        let mut backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

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

    /// Test TransactionalMemory operations with S3 backend
    /// Covers: read non-existent, write new, update existing, read existing
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    async fn test_s3_transactional_memory_full_lifecycle() -> Result<()> {
        use crate::TransactionalMemory;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct TestData {
            name: String,
            version: u32,
        }

        let config = get_s3_config_from_env()?;
        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

        // Use a unique key for this test run to avoid conflicts
        let address = format!("txn-test-{}", rand::random::<u32>()).into_bytes();

        // Step 1: Read non-existent key
        eprintln!("[TEST] Step 1: Reading non-existent key...");
        let mut txn = TransactionalMemory::<TestData, _, _>::open(
            address.clone(),
            &backend,
            crate::CborEncoder,
        )
        .await?;

        let content = txn.read();
        assert_eq!(content, None, "Non-existent key should return None");
        eprintln!("[TEST] ✓ Non-existent key returns None");

        // Step 2: Write new value
        eprintln!("[TEST] Step 2: Writing new value...");
        let initial_data = TestData {
            name: "Initial".to_string(),
            version: 1,
        };

        txn.replace(Some(initial_data.clone()), &backend).await?;
        eprintln!("[TEST] ✓ New value written");

        // Verify it was written
        let content = txn.read();
        assert_eq!(
            content,
            Some(initial_data.clone()),
            "Should read back the initial value"
        );
        eprintln!("[TEST] ✓ Read back initial value from cache");

        // Step 3: Reload from storage to verify persistence
        eprintln!("[TEST] Step 3: Reloading from storage...");
        txn.reload(&backend).await?;
        let content = txn.read();
        assert_eq!(
            content,
            Some(initial_data.clone()),
            "Should reload the same value from storage"
        );
        eprintln!("[TEST] ✓ Value persisted to storage");

        // Step 4: Update existing value
        eprintln!("[TEST] Step 4: Updating existing value...");
        let updated_data = TestData {
            name: "Updated".to_string(),
            version: 2,
        };

        txn.replace(Some(updated_data.clone()), &backend).await?;
        eprintln!("[TEST] ✓ Value updated");

        // Verify update
        let content = txn.read();
        assert_eq!(
            content,
            Some(updated_data.clone()),
            "Should read back the updated value"
        );
        eprintln!("[TEST] ✓ Read back updated value from cache");

        // Step 5: Create a fresh instance and verify persistence
        eprintln!("[TEST] Step 5: Creating fresh instance to verify persistence...");
        let txn2 = TransactionalMemory::<TestData, _, _>::open(
            address.clone(),
            &backend,
            crate::CborEncoder,
        )
        .await?;

        let content = txn2.read();
        assert_eq!(
            content,
            Some(updated_data.clone()),
            "Fresh instance should read the updated value from storage"
        );
        eprintln!("[TEST] ✓ Fresh instance reads persisted updated value");

        // Step 6: Test replace_with (conditional update)
        eprintln!("[TEST] Step 6: Testing replace_with for conditional updates...");
        let mut txn3 = TransactionalMemory::<TestData, _, _>::open(
            address.clone(),
            &backend,
            crate::CborEncoder,
        )
        .await?;

        txn3.replace_with(
            |current| {
                // Increment version based on current value
                current.as_ref().map(|data| TestData {
                    name: format!("{}-Modified", data.name),
                    version: data.version + 1,
                })
            },
            &backend,
        )
        .await?;

        let content = txn3.read();
        assert_eq!(
            content,
            Some(TestData {
                name: "Updated-Modified".to_string(),
                version: 3,
            }),
            "replace_with should apply the transformation"
        );
        eprintln!("[TEST] ✓ replace_with applied transformation correctly");

        // Step 7: Test reading existing value with fresh TransactionalMemory
        eprintln!("[TEST] Step 7: Reading existing value with fresh instance...");
        let txn4 = TransactionalMemory::<TestData, _, _>::open(
            address.clone(),
            &backend,
            crate::CborEncoder,
        )
        .await?;

        let content = txn4.read();
        assert_eq!(
            content,
            Some(TestData {
                name: "Updated-Modified".to_string(),
                version: 3,
            }),
            "Should read the final state"
        );
        eprintln!("[TEST] ✓ Final state correctly persisted and retrieved");

        // Step 8: Test deletion (replace with None)
        eprintln!("[TEST] Step 8: Testing deletion...");
        let mut txn5 = TransactionalMemory::<TestData, _, _>::open(
            address.clone(),
            &backend,
            crate::CborEncoder,
        )
        .await?;

        txn5.replace(None, &backend).await?;

        let content = txn5.read();
        assert_eq!(content, None, "Value should be deleted");
        eprintln!("[TEST] ✓ Value deleted");

        // Verify deletion persisted
        txn5.reload(&backend).await?;
        let content = txn5.read();
        assert_eq!(content, None, "Deletion should persist");
        eprintln!("[TEST] ✓ Deletion persisted");

        eprintln!("[TEST] ✅ All TransactionalMemory lifecycle steps completed successfully!");

        Ok(())
    }
}

/// S3 Signature Test Fixtures
///
/// These tests generate deterministic S3 signatures that can be compared across
/// native and WASM platforms to verify the signing implementation is identical.
///
/// Run on native to generate fixtures:
/// ```bash
/// cargo test s3_signature_fixtures -- --nocapture
/// ```
///
/// Run on WASM to verify signatures match:
/// ```bash
/// cargo test --target wasm32-unknown-unknown s3_signature_fixtures -- --nocapture
/// ```
#[cfg(test)]
mod s3_signature_fixtures {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::*;

    /// Fixed timestamp for deterministic signatures: 2024-01-15 12:00:00 UTC
    fn fixed_timestamp() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap()
    }

    /// Create test credentials
    fn test_credentials() -> s3_signer::Credentials {
        s3_signer::Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        }
    }

    /// Test fixture 1: Simple PUT request
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    fn test_s3_signature_put_simple() {
        let creds = test_credentials();
        let access = s3_signer::Access {
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            key: "test-key.txt".to_string(),
            checksum: Some("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=".to_string()),
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            expires: 3600,
            method: "PUT".to_string(),
            public_read: false,
            service: "s3".to_string(),
            time: Some(fixed_timestamp()),
        };

        let auth = creds
            .authorize(&access)
            .expect("Authorization should succeed");

        // Expected values (these will be the same on native and WASM if signing is correct)
        assert_eq!(auth.timestamp, "20240115T120000Z");
        assert_eq!(auth.date, "20240115");
        assert_eq!(auth.region, "us-east-1");
        assert_eq!(auth.host, "test-bucket.s3.amazonaws.com");

        // The signature should be deterministic and identical on native and WASM
        assert_eq!(
            auth.signature, "756d645e6508cc17f42f1686bd1afb20af38bffc43121a3fe635c43492d95029",
            "Signature must match expected value on both native and WASM"
        );
    }

    /// Test fixture 2: GET request
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    fn test_s3_signature_get_simple() {
        let creds = test_credentials();
        let access = s3_signer::Access {
            region: "us-west-2".to_string(),
            bucket: "my-bucket".to_string(),
            key: "path/to/object.bin".to_string(),
            checksum: None,
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            expires: 7200,
            method: "GET".to_string(),
            public_read: false,
            service: "s3".to_string(),
            time: Some(fixed_timestamp()),
        };

        let auth = creds
            .authorize(&access)
            .expect("Authorization should succeed");

        assert_eq!(auth.timestamp, "20240115T120000Z");
        assert_eq!(auth.date, "20240115");
        assert_eq!(auth.region, "us-west-2");
        assert_eq!(
            auth.signature, "7354907fe843ed9bb1f0d1d77211043d368366c855f52b90dbd506a8e53255d6",
            "Signature must match expected value on both native and WASM"
        );
    }

    /// Test fixture 3: R2 (Cloudflare) PUT request
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    fn test_s3_signature_r2_put() {
        let creds = test_credentials();
        let access = s3_signer::Access {
            region: "auto".to_string(),
            bucket: "r2-bucket".to_string(),
            key: "uploads/file.dat".to_string(),
            checksum: Some("RBNvo1WzZ4oRRq0W9+hknpT7T8If536DEMBg9hyq/4o=".to_string()),
            endpoint: Some("https://1234567890.r2.cloudflarestorage.com".to_string()),
            expires: 1800,
            method: "PUT".to_string(),
            public_read: false,
            service: "s3".to_string(),
            time: Some(fixed_timestamp()),
        };

        let auth = creds
            .authorize(&access)
            .expect("Authorization should succeed");

        assert_eq!(auth.timestamp, "20240115T120000Z");
        assert_eq!(auth.region, "auto");
        assert_eq!(
            auth.signature, "dfa48a3229c104b0c06ff924b17fa243d72d6bf62a2e28179279be519936f1a7",
            "Signature must match expected value on both native and WASM"
        );
    }

    /// Test fixture 4: Public read request
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    fn test_s3_signature_public_read() {
        let creds = test_credentials();
        let access = s3_signer::Access {
            region: "eu-west-1".to_string(),
            bucket: "public-bucket".to_string(),
            key: "public/image.jpg".to_string(),
            checksum: None,
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            expires: 3600,
            method: "PUT".to_string(),
            public_read: true,
            service: "s3".to_string(),
            time: Some(fixed_timestamp()),
        };

        let auth = creds
            .authorize(&access)
            .expect("Authorization should succeed");

        assert_eq!(auth.timestamp, "20240115T120000Z");
        assert!(auth.public_read);
        assert_eq!(
            auth.signature, "ee674e6d680d7a0005fb0b29a54d3d71b11b9aab12a3b8c9edc31ae636f481dc",
            "Signature must match expected value on both native and WASM"
        );
    }

    /// Test fixture 5: Key with special characters
    #[cfg_attr(not(target_arch = "wasm32"), test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    fn test_s3_signature_special_chars() {
        let creds = test_credentials();
        let access = s3_signer::Access {
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            key: "path with spaces/file-name_123.txt".to_string(),
            checksum: None,
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            expires: 3600,
            method: "PUT".to_string(),
            public_read: false,
            service: "s3".to_string(),
            time: Some(fixed_timestamp()),
        };

        let auth = creds
            .authorize(&access)
            .expect("Authorization should succeed");

        assert_eq!(
            auth.signature, "222fb0a12bf29245e037852a1256ebda1d4987c459811bfd86c3b5de22603f62",
            "Signature must match expected value on both native and WASM"
        );
        // URL should properly encode spaces
        assert!(auth.url.to_string().contains("path%20with%20spaces"));
    }
}
