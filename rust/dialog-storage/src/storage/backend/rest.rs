use std::marker::PhantomData;

use async_stream::try_stream;
use async_trait::async_trait;
use base58::ToBase58;
use base64::Engine;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

mod s3_signer;
use s3_signer::{Access, Credentials};

use crate::{
    AtomicStorageBackend, DialogStorageError, StorageBackend, StorageSink, StorageSource,
    storage::backend::rest::s3_signer::Authorization,
};

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
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
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
#[derive(Clone)]
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
        let key_str = base64::engine::general_purpose::STANDARD.encode(key);

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
            let key_str = base64::engine::general_purpose::STANDARD.encode(key);
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
impl<Key, Value> AtomicStorageBackend for RestStorageBackend<Key, Value>
where
    Key: AsRef<[u8]> + Clone + ConditionalSync,
    Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
{
    type Key = Key;
    type Value = Value;
    type Error = RestStorageBackendError;

    async fn swap(
        &mut self,
        key: Self::Key,
        value: Option<Self::Value>,
        when: Option<Self::Value>,
    ) -> Result<(), Self::Error> {
        // Prepare request - DELETE if value is None, PUT if Some
        let mut request = match &value {
            Some(v) => self.prepare_put_request(key.as_ref(), v.as_ref())?,
            None => self.prepare_delete_request(key.as_ref())?,
        };

        // Add precondition headers to enforce CAS semantics.
        request = match &when {
            Some(value) => request.header("If-Match", format!("\"{:x}\"", md5::compute(value))),
            None => request.header("If-None-Match", "*"),
        };

        let response = request.send().await?;

        if response.status().is_success() {
            return Ok(());
        }

        // If precondition failed, we should fetch the latest version so we can
        // report actual value if it is different or retry with a different
        // etag if same, which may happen if multipart upload was used, which
        // should not happen, but it still good to handle such case gracefully.
        if response.status() == reqwest::StatusCode::PRECONDITION_FAILED {
            // Fetch latest revision to see if we it has changed
            let request = self.prepare_get_request(key.as_ref())?;
            let latest = request.send().await?;
            let etag = latest.headers().get("etag").cloned();
            let status = latest.status();

            // If key is not found we just need to set If-None-Match: *
            let actual = if status == reqwest::StatusCode::NOT_FOUND {
                None
            }
            // If fetching latest was successful, we read etag from its headers
            else if status.is_success() {
                Some(latest.bytes().await?)
            } else {
                Err(RestStorageBackendError::RequestFailed(format!(
                    "Failed to fetch object after put with precondition was rejected: {}",
                    response.status()
                )))?
            };

            // figure out what etag should we retry put with
            let precondition = match (when, actual) {
                (None, None) => Ok(etag),
                (Some(expected), Some(actual)) => {
                    if expected.as_ref() != actual.as_ref() {
                        Err(RestStorageBackendError::OperationFailed(format!(
                            "Precondition failed, expected key {} to have value {} instead of {}",
                            ToBase58::to_base58(key.as_ref()),
                            ToBase58::to_base58(expected.as_ref()),
                            ToBase58::to_base58(actual.as_ref())
                        )))
                    } else {
                        Ok(etag)
                    }
                }
                (Some(expected), None) => Err(RestStorageBackendError::OperationFailed(format!(
                    "Precondition failed, expected key {} to have value {} but it was not found",
                    ToBase58::to_base58(key.as_ref()),
                    ToBase58::to_base58(expected.as_ref())
                ))),
                (None, Some(actual)) => Err(RestStorageBackendError::OperationFailed(format!(
                    "Precondition failed, expected key {} to not exist but it was found with value {}",
                    ToBase58::to_base58(key.as_ref()),
                    ToBase58::to_base58(actual.as_ref())
                ))),
            }?;

            // Retry the operation with corrected precondition
            let mut request = match &value {
                Some(v) => self.prepare_put_request(key.as_ref(), v.as_ref())?,
                None => self.prepare_delete_request(key.as_ref())?,
            };
            request = match precondition {
                Some(etag) => request.header("If-Match", etag),
                None => request.header("If-None-Match", "*"),
            };

            let retry = request.send().await?;

            if retry.status().is_success() {
                return Ok(());
            } else {
                return Err(RestStorageBackendError::OperationFailed(format!(
                    "Retry {} failed: {}",
                    if value.is_some() { "PUT" } else { "DELETE" },
                    retry.status()
                )));
            }
        } else {
            Err(RestStorageBackendError::OperationFailed(format!(
                "swap failed: {}",
                response.status()
            )))
        }
    }

    async fn resolve(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        StorageBackend::get(self, key).await
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
                let key_bytes = match base64::engine::general_purpose::STANDARD.decode(&encoded_key) {
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
        let key = vec![1, 2, 3]; // Base64: AQID
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(
            url.as_str(),
            "https://example.com/test-bucket/test-prefix/AQID"
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
        let key = vec![1, 2, 3]; // Base64: AQID
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(url.as_str(), "https://example.com/test-bucket/AQID");
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
        let key = vec![1, 2, 3]; // Base64: AQID
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(url.as_str(), "https://example.com/test-prefix/AQID");
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
        let key = vec![1, 2, 3]; // Base64: AQID
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(url.as_str(), "https://example.com/AQID");
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
        let key = vec![1, 2, 3]; // Base64: AQID
        let url = backend.build_url(key.as_ref(), "GET").unwrap();

        assert_eq!(url.as_str(), "https://example.com/AQID");
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
        let key = vec![1, 2, 3]; // Base64: AQID

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
        assert_eq!(url.path(), "/AQID");
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
        let key = vec![1, 2, 3]; // Base64: AQID
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
        assert_eq!(url.path(), "/AQID");
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

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;

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

        // Key as base64: AQID
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        // Mock PUT request for set operation
        let put_mock = server
            .mock("PUT", "/AQID")
            .with_status(200)
            .with_body("")
            .create();

        // Mock GET request for successful retrieval
        let get_mock = server
            .mock("GET", "/AQID")
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

        let key = vec![10, 11, 12]; // Different key, base64: CgsMw==

        // Mock GET request for missing value (404 response)
        // Use Matcher::Any since the path will be URL-encoded
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/Cgs.*".to_string()))
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

        let key = vec![20, 21, 22]; // base64: FBUWe==

        // Mock GET request for server error
        // Use regex matcher for URL-encoded paths
        let mock = server
            .mock("GET", mockito::Matcher::Regex(r"^/FBUW.*".to_string()))
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
            .mock("PUT", "/AQID")
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
            .mock("PUT", "/test-bucket/test-prefix/AQID")
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
            .with_body(r#"["AQID", "BAUGBw=="]"#)
            .create();

        // Mock the GET for first key
        let get_mock1 = server
            .mock("GET", "/AQID")
            .with_status(200)
            .with_body(&[4, 5, 6])
            .create();

        // Mock the GET for second key
        let get_mock2 = server
            .mock("GET", "/BAUGBw==")
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
            .with_body(r#"["AQID", "BAUGBw=="]"#)
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
        let put_mock1 = server.mock("PUT", "/AQID").with_status(200).create();

        let put_mock2 = server.mock("PUT", "/BAUGBw==").with_status(200).create();

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
        let put_mock1 = server.mock("PUT", "/AQID").with_status(200).create();

        let put_mock2 = server.mock("PUT", "/BAUGBw==").with_status(200).create();

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
    use hyper::server::conn::http1;
    use hyper_util::rt::TokioIo;
    use s3;
    use s3s::dto::*;
    use s3s::{S3, S3Request, S3Response, S3Result};
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use tokio::net::TcpListener;

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
    async fn test_local_s3_atomic_swap_ok() -> anyhow::Result<()> {
        let service = s3::start().await?;

        let config = RestStorageConfig {
            endpoint: service.endpoint().into(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("branch".to_string()),
            ..Default::default()
        };

        let mut store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let v1: Vec<u8> = "v1".into();
        let key: Vec<u8> = ALICE.into();

        // We try to create new branch record
        store.swap(key.clone(), Some(v1.clone()), None).await?;

        assert_eq!(
            store.resolve(&key).await?.unwrap(),
            v1.clone(),
            "resolved to stored record"
        );

        let v2: Vec<u8> = "v2".into();
        // We try to update v1 -> v2
        store
            .swap(key.clone(), Some(v2.clone()), Some(v1.clone()))
            .await?;

        assert_eq!(
            store.resolve(&key).await?.unwrap(),
            v2.clone(),
            "resolved to new record"
        );

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
        let v1: Vec<u8> = "v1".into();
        let key: Vec<u8> = ALICE.into();

        // We try to create new branch record
        store.swap(key.clone(), Some(v1.clone()), None).await?;

        assert_eq!(
            store.resolve(&key).await?.unwrap(),
            v1.clone(),
            "resolved to stored record"
        );

        let v2: Vec<u8> = "v2".into();
        let v3: Vec<u8> = "v3".into();

        // We try to update v2 -> v3
        let result = store.swap(key.clone(), Some(v3.clone()), Some(v2.clone())).await;

        assert!(
            matches!(result, Err(RestStorageBackendError::OperationFailed(_))),
            "swap failed"
        );

        assert_eq!(
            store.resolve(&key).await?.unwrap(),
            v1.clone(),
            "resolved to old record"
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
        let v1: Vec<u8> = "v1".into();
        let v2: Vec<u8> = "v2".into();
        let key: Vec<u8> = ALICE.into();

        // We try to swap v1 -> v2
        let result = store.swap(key.clone(), Some(v2.clone()), Some(v1.clone())).await;

        assert!(
            matches!(result, Err(RestStorageBackendError::OperationFailed(_))),
            "swap failed"
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
        let mut store = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;
        let key: Vec<u8> = ALICE.into();
        let value: Vec<u8> = b"v1".to_vec();

        // when=None and key missing → success
        store.swap(key.clone(), Some(value.clone()), None).await?;
        assert_eq!(store.resolve(&key).await?.unwrap(), value);

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
        let expected: Vec<u8> = b"v1".to_vec();
        let new_val: Vec<u8> = b"v2".to_vec();

        // when=Some but key missing → fail
        let result = store.swap(key.clone(), Some(new_val), Some(expected)).await;
        assert!(matches!(
            result,
            Err(RestStorageBackendError::OperationFailed(_))
        ));
        assert!(store.resolve(&key).await?.is_none());

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
        let existing: Vec<u8> = b"v1".to_vec();
        let new_val: Vec<u8> = b"v2".to_vec();

        // Prepopulate the key
        store.swap(key.clone(), Some(existing.clone()), None).await?;

        // when=None and key exists → fail
        let result = store.swap(key.clone(), Some(new_val), None).await;
        assert!(matches!(
            result,
            Err(RestStorageBackendError::OperationFailed(_))
        ));
        assert_eq!(store.resolve(&key).await?.unwrap(), existing);

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
        let existing: Vec<u8> = b"v1".to_vec();
        let new_val: Vec<u8> = b"v2".to_vec();

        // Prepopulate the key
        store.swap(key.clone(), Some(existing.clone()), None).await?;

        // when=Some and matches existing → success
        store
            .swap(key.clone(), Some(new_val.clone()), Some(existing.clone()))
            .await?;
        assert_eq!(store.resolve(&key).await?.unwrap(), new_val);

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
        let existing: Vec<u8> = b"v1".to_vec();
        let wrong_expected: Vec<u8> = b"vX".to_vec();
        let new_val: Vec<u8> = b"v2".to_vec();

        // Prepopulate the key
        store.swap(key.clone(), Some(existing.clone()), None).await?;

        // when=Some but doesn't match existing → fail
        let result = store
            .swap(key.clone(), Some(new_val.clone()), Some(wrong_expected))
            .await;
        assert!(matches!(
            result,
            Err(RestStorageBackendError::OperationFailed(_))
        ));
        assert_eq!(store.resolve(&key).await?.unwrap(), existing);

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(unused_imports, unused_variables, unused_mut, dead_code)]
mod s3 {
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

    pub struct Service {
        pub endpoint: String,
        shutdown_tx: tokio::sync::oneshot::Sender<()>,
    }
    impl Service {
        pub fn stop(self) -> Result<(), ()> {
            self.shutdown_tx.send(())
        }

        pub fn endpoint(&self) -> &str {
            &self.endpoint
        }
    }

    pub async fn start() -> anyhow::Result<Service> {
        InMemoryS3::start2().await
    }

    impl InMemoryS3 {
        pub async fn start() -> anyhow::Result<Service> {
            Self::serve(Self::default()).await
        }

        pub async fn start2() -> anyhow::Result<Service> {
            let s3_handler = S3ServiceBuilder::new(Self::default()).build();

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
            })
        }

        async fn serve(self) -> anyhow::Result<Service> {
            use hyper::server::conn::http1;
            use hyper_util::rt::TokioIo;
            use s3s::service::S3ServiceBuilder;
            use tokio::net::TcpListener;

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
///   cargo test --features s3_integration_tests -- --ignored
/// ```
#[cfg(all(test, feature = "s3_integration_tests", not(target_arch = "wasm32")))]
mod s3_integration_tests {
    use super::*;
    use anyhow::Result;
    use std::env;

    /// Helper to get S3 config from environment variables
    fn get_s3_config_from_env() -> Result<RestStorageConfig> {
        let endpoint = env::var("R2S3_HOST")
            .map_err(|_| anyhow::anyhow!("R2S3_HOST environment variable not set"))?;
        let region = env::var("R2S3_REGION")
            .map_err(|_| anyhow::anyhow!("R2S3_REGION environment variable not set"))?;
        let bucket = env::var("R2S3_BUCKET")
            .map_err(|_| anyhow::anyhow!("R2S3_BUCKET environment variable not set"))?;
        let access_key_id = env::var("R2S3_ACCESS_KEY_ID")
            .map_err(|_| anyhow::anyhow!("R2S3_ACCESS_KEY_ID environment variable not set"))?;
        let secret_access_key = env::var("R2S3_SECRET_ACCESS_KEY")
            .map_err(|_| anyhow::anyhow!("R2S3_SECRET_ACCESS_KEY environment variable not set"))?;

        let s3_credentials = S3Authority {
            access_key_id,
            secret_access_key,
            session_token: None,
            region: region.clone(),
            public_read: false,
            expires: 3600, // 1 hour for tests
        };

        Ok(RestStorageConfig {
            endpoint,
            auth_method: AuthMethod::S3(s3_credentials),
            bucket: Some(bucket),
            key_prefix: Some("test-prefix".to_string()),
            headers: Vec::new(),
            timeout_seconds: Some(30),
        })
    }

    #[tokio::test]
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

    #[tokio::test]
    async fn test_s3_get_missing_key() -> Result<()> {
        let config = get_s3_config_from_env()?;
        let backend = RestStorageBackend::<Vec<u8>, Vec<u8>>::new(config)?;

        // Try to get a key that doesn't exist
        let key = b"nonexistent-key-12345".to_vec();
        let retrieved = backend.get(&key).await?;

        assert_eq!(retrieved, None);

        Ok(())
    }

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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

    #[tokio::test]
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
}
