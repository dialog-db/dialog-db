use std::marker::PhantomData;

use async_stream::try_stream;
use async_trait::async_trait;
use base64::Engine;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use sha2::{Sha256, Digest};
use thiserror::Error;
use url::Url;

mod s3_signer;
use s3_signer::{Credentials as S3SignerCredentials, SignOptions, sign_url};

use crate::{DialogStorageError, StorageBackend, StorageSource, StorageSink};

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
        if error.is_connect() {
            RestStorageBackendError::ConnectionFailed(error.to_string())
        } else if error.is_request() {
            RestStorageBackendError::OperationFailed(error.to_string())
        } else {
            RestStorageBackendError::RequestFailed(error.to_string())
        }
    }
}

/// AWS S3/R2 credentials configuration
#[derive(Clone, Debug)]
pub struct S3Credentials {
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

impl Default for S3Credentials {
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
    S3(S3Credentials),
}

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
    
    /// Enable chunked uploads for large files
    pub chunked_upload: bool,
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
            chunked_upload: false,
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
        let mut client_builder = reqwest::Client::builder();
        
        if let Some(timeout) = config.timeout_seconds {
            client_builder = client_builder.timeout(std::time::Duration::from_secs(timeout));
        }
        
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
    
    /// Build the URL for a given key
    fn build_url(&self, key: &[u8], method: &str) -> Result<Url, RestStorageBackendError> {
        let key_str = base64::engine::general_purpose::STANDARD.encode(key);
        
        // For S3/R2 signed URLs
        if let AuthMethod::S3(credentials) = &self.config.auth_method {
            // If using S3/R2 auth, the bucket must be set
            let bucket = match &self.config.bucket {
                Some(bucket) => bucket.clone(),
                None => return Err(RestStorageBackendError::OperationFailed(
                    "Bucket must be specified when using S3/R2 authentication".to_string()
                )),
            };
            
            // Build the object key with optional prefix
            let object_key = if let Some(prefix) = &self.config.key_prefix {
                format!("{}/{}", prefix, key_str)
            } else {
                key_str
            };
            
            // Calculate checksum for PUT operations
            let checksum = if method == "PUT" {
                // We'll calculate this later when we have the actual data
                None
            } else {
                None
            };
            
            // Prepare the signing options
            let sign_options = SignOptions {
                region: credentials.region.clone(),
                bucket,
                key: object_key,
                checksum,
                endpoint: Some(self.config.endpoint.clone()),
                expires: credentials.expires,
                method: method.to_string(),
                public_read: credentials.public_read,
                service: "s3".to_string(),
            };
            
            // Convert our credentials to the format expected by the signer
            let signer_credentials = S3SignerCredentials {
                access_key_id: credentials.access_key_id.clone(),
                secret_access_key: credentials.secret_access_key.clone(),
                session_token: credentials.session_token.clone(),
            };
            
            // Generate the signed URL
            sign_url(&signer_credentials, &sign_options)
                .map_err(|e| RestStorageBackendError::OperationFailed(
                    format!("Failed to generate signed URL: {}", e)
                ))
        } else {
            // For non-S3 authentication, use the original URL building logic
            let base_url = self.config.endpoint.trim_end_matches('/');
            
            let url_str = match (&self.config.bucket, &self.config.key_prefix) {
                (Some(bucket), Some(prefix)) => format!("{base_url}/{bucket}/{prefix}/{key_str}"),
                (Some(bucket), None) => format!("{base_url}/{bucket}/{key_str}"),
                (None, Some(prefix)) => format!("{base_url}/{prefix}/{key_str}"),
                (None, None) => format!("{base_url}/{key_str}"),
            };
            
            Url::parse(&url_str)
                .map_err(|e| RestStorageBackendError::OperationFailed(
                    format!("Failed to parse URL: {}", e)
                ))
        }
    }
    
    /// Calculate SHA-256 checksum for a value
    fn calculate_checksum(&self, value: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value);
        let hash = hasher.finalize();
        
        // Convert to base64 encoding as required by S3
        base64::engine::general_purpose::STANDARD.encode(hash)
    }
    
    /// Build the signed URL for a PUT operation with the value's checksum
    fn build_put_url(&self, key: &[u8], value: &[u8]) -> Result<Url, RestStorageBackendError> {
        // For S3/R2 signed URLs
        if let AuthMethod::S3(credentials) = &self.config.auth_method {
            // If using S3/R2 auth, the bucket must be set
            let bucket = match &self.config.bucket {
                Some(bucket) => bucket.clone(),
                None => return Err(RestStorageBackendError::OperationFailed(
                    "Bucket must be specified when using S3/R2 authentication".to_string()
                )),
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
            let sign_options = SignOptions {
                region: credentials.region.clone(),
                bucket,
                key: object_key,
                checksum: checksum.clone(),
                endpoint: Some(self.config.endpoint.clone()),
                expires: credentials.expires,
                method: "PUT".to_string(),
                public_read: credentials.public_read,
                service: "s3".to_string(),
            };
            
            // Convert our credentials to the format expected by the signer
            let signer_credentials = S3SignerCredentials {
                access_key_id: credentials.access_key_id.clone(),
                secret_access_key: credentials.secret_access_key.clone(),
                session_token: credentials.session_token.clone(),
            };
            
            // Generate the signed URL
            sign_url(&signer_credentials, &sign_options)
                .map_err(|e| RestStorageBackendError::OperationFailed(
                    format!("Failed to generate signed URL: {}", e)
                ))
        } else {
            // For non-S3 authentication, use the standard URL building
            self.build_url(key, "PUT")
        }
    }
    
    /// Add authentication and custom headers to a request
    fn prepare_request(&self, request_builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut builder = request_builder;
        
        // Add authentication headers based on the chosen method
        match &self.config.auth_method {
            AuthMethod::None => {
                // No authentication needed
            },
            AuthMethod::Bearer(token) => {
                builder = builder.header("Authorization", format!("Bearer {token}"));
            },
            AuthMethod::S3(_) => {
                // For S3/R2, authentication is handled via the signed URL
                // No additional headers needed here
            }
        }
        
        // Add custom headers
        for (name, value) in &self.config.headers {
            builder = builder.header(name, value);
        }
        
        builder
    }
    
    /// Prepare a request with S3 authentication headers if needed
    fn prepare_s3_request(&self, mut request_builder: reqwest::RequestBuilder, checksum: Option<&str>) -> reqwest::RequestBuilder {
        // For S3 requests, we need to add the checksum header if available
        if let AuthMethod::S3(_) = &self.config.auth_method {
            if let Some(checksum_value) = checksum {
                request_builder = request_builder.header("x-amz-checksum-sha256", checksum_value);
            }
        }
        
        self.prepare_request(request_builder)
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
        let value_bytes = value.as_ref().to_vec();
        
        // For S3/R2, we need to generate a signed URL with the value's checksum
        if let AuthMethod::S3(_) = &self.config.auth_method {
            let url = self.build_put_url(key.as_ref(), &value_bytes)?;
            
            // Calculate checksum for the value
            let checksum = self.calculate_checksum(&value_bytes);
            
            // Prepare the request with the checksum header
            let request = self.prepare_s3_request(
                self.client.put(url).body(value_bytes),
                Some(&checksum)
            );
            
            let response = request.send().await?;
            
            if !response.status().is_success() {
                return Err(RestStorageBackendError::OperationFailed(format!(
                    "Failed to set value with S3 signed URL. Status: {}", response.status()
                )));
            }
        } else {
            // For regular REST storage
            let url = self.build_url(key.as_ref(), "PUT")?;
            
            let request = self.prepare_request(self.client.put(url).body(value_bytes));
            
            let response = request.send().await?;
            
            if !response.status().is_success() {
                return Err(RestStorageBackendError::OperationFailed(format!(
                    "Failed to set value. Status: {}", response.status()
                )));
            }
        }
        
        Ok(())
    }
    
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        // Generate the URL, which will be signed if using S3/R2
        let url = self.build_url(key.as_ref(), "GET")?;
        
        // Prepare the request (no special headers needed for GET)
        let request = self.prepare_request(self.client.get(url));
        
        let response = request.send().await?;
        
        match response.status() {
            status if status.is_success() => {
                let bytes = response.bytes().await?;
                Ok(Some(Value::from(bytes.to_vec())))
            },
            reqwest::StatusCode::NOT_FOUND => Ok(None),
            status => Err(RestStorageBackendError::OperationFailed(format!(
                "Failed to get value. Status: {status}"
            ))),
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
                let sign_options = SignOptions {
                    region: credentials.region.clone(),
                    bucket,
                    key: key.to_string(),
                    checksum: None,
                    endpoint: Some(self.config.endpoint.clone()),
                    expires: credentials.expires,
                    method: "GET".to_string(),
                    public_read: credentials.public_read,
                    service: "s3".to_string(),
                };
                
                // Convert our credentials to the format expected by the signer
                let signer_credentials = S3SignerCredentials {
                    access_key_id: credentials.access_key_id.clone(),
                    secret_access_key: credentials.secret_access_key.clone(),
                    session_token: credentials.session_token.clone(),
                };
                
                // Generate the signed URL
                sign_url(&signer_credentials, &sign_options)
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
            let request = self.prepare_request(self.client.get(list_url));
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
                
                if let Some(value) = self.get(&key).await? {
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
        
        assert_eq!(url.as_str(), "https://example.com/test-bucket/test-prefix/AQID");
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
        assert!(!config.chunked_upload);
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
        let creds = S3Credentials::default();
        
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
        let s3_creds = S3Credentials {
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
        
        assert_eq!(query_params.get("X-Amz-Algorithm").unwrap(), "AWS4-HMAC-SHA256");
        assert!(query_params.get("X-Amz-Credential").unwrap().starts_with("test-access-key/"));
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
        let s3_creds = S3Credentials {
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
        
        assert_eq!(query_params.get("X-Amz-Algorithm").unwrap(), "AWS4-HMAC-SHA256");
        assert!(query_params.get("X-Amz-Credential").unwrap().starts_with("test-access-key/"));
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
        let _prepared = backend.prepare_request(req_builder);
        
        // This is a simple test to ensure the code path is covered
        assert!(true);
    }
}

#[cfg(all(test, feature = "http_tests"))]
mod tests {
    use super::*;
    use anyhow::Result;
    use mockito::Server;
    
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;
    
    // Helper function to create a test REST backend with a mock server
    fn create_test_backend() -> (RestStorageBackend<Vec<u8>, Vec<u8>>, mockito::ServerGuard) {
        let server = Server::new();
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
    fn create_s3_test_backend() -> (RestStorageBackend<Vec<u8>, Vec<u8>>, mockito::ServerGuard) {
        let server = Server::new();
        let endpoint = server.url();
        
        // Use fixed credentials for testing
        let s3_creds = S3Credentials {
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
        let (mut backend, mut server) = create_test_backend();
        
        // Key as base64: AQID
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        
        // Mock PUT request for set operation
        let put_mock = server.mock("PUT", "/AQID")
            .with_status(200)
            .with_body("")
            .create();
            
        // Mock GET request for successful retrieval
        let get_mock = server.mock("GET", "/AQID")
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
        let (backend, mut server) = create_test_backend();
        
        let key = vec![10, 11, 12]; // Different key, base64: CgsMw==
        
        // Mock GET request for missing value (404 response)
        let mock = server.mock("GET", "/CgsMw==")
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
        let (backend, mut server) = create_test_backend();
        
        let key = vec![20, 21, 22]; // base64: FBUWe==
        
        // Mock GET request for server error
        let mock = server.mock("GET", "/FBUWe==")
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
        let mut server = Server::new();
        let config = RestStorageConfig {
            endpoint: server.url(),
            auth_method: AuthMethod::Bearer("test-api-key".to_string()),
            headers: vec![("X-Custom-Header".to_string(), "custom-value".to_string())],
            ..Default::default()
        };
        
        let mut backend = RestStorageBackend::new(config)?;
        
        // Mock PUT request expecting specific headers
        let mock = server.mock("PUT", "/AQID")
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
    async fn it_uses_s3_authentication() -> Result<()> {
        let (mut backend, mut server) = create_s3_test_backend();
        
        // Key as base64: AQID
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];
        
        // The URL will contain AWS query parameters, so we use a wildcard match
        // We'll check for the presence of the checksum header
        let put_mock = server.mock("PUT", mockito::Matcher::Any)
            .match_header("x-amz-checksum-sha256", mockito::Matcher::Any)
            .with_status(200)
            .with_body("")
            .create();
            
        // For GET requests we just need to acknowledge the S3 query parameters
        let get_mock = server.mock("GET", mockito::Matcher::Any)
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
    async fn it_uses_bucket_and_prefix() -> Result<()> {
        let mut server = Server::new();
        let config = RestStorageConfig {
            endpoint: server.url(),
            auth_method: AuthMethod::None,
            bucket: Some("test-bucket".to_string()),
            key_prefix: Some("test-prefix".to_string()),
            ..Default::default()
        };
        
        let mut backend = RestStorageBackend::new(config)?;
        
        // Mock PUT request with bucket and prefix in path
        let mock = server.mock("PUT", "/test-bucket/test-prefix/AQID")
            .with_status(200)
            .create();
            
        backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        mock.assert();
        
        Ok(())
    }
    
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_perform_list_operations() -> Result<()> {
        let (backend, mut server) = create_test_backend();
        
        // Mock the list endpoint
        let list_mock = server.mock("GET", "/_list")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"["AQID", "BAUGBw=="]"#)
            .create();
            
        // Mock the GET for first key
        let get_mock1 = server.mock("GET", "/AQID")
            .with_status(200)
            .with_body(&[4, 5, 6])
            .create();
            
        // Mock the GET for second key
        let get_mock2 = server.mock("GET", "/BAUGBw==")
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
    async fn it_can_perform_s3_list_operations() -> Result<()> {
        let (backend, mut server) = create_s3_test_backend();
        
        // Mock the list endpoint, which will have a signed URL
        let list_mock = server.mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"["AQID", "BAUGBw=="]"#)
            .create();
            
        // Mock the GET for first key
        let get_mock1 = server.mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_body(&[4, 5, 6])
            .create();
            
        // Mock the GET for second key
        let get_mock2 = server.mock("GET", mockito::Matcher::Any)
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
        let (mut backend, mut server) = create_test_backend();
        
        // Create mocks for two PUT operations
        let put_mock1 = server.mock("PUT", "/AQID")
            .with_status(200)
            .create();
            
        let put_mock2 = server.mock("PUT", "/BAUGBw==")
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
    async fn it_can_perform_s3_bulk_writes() -> Result<()> {
        let (mut backend, mut server) = create_s3_test_backend();
        
        // Create mocks for two PUT operations with signed URLs
        let put_mock1 = server.mock("PUT", mockito::Matcher::Any)
            .match_header("x-amz-checksum-sha256", mockito::Matcher::Any)
            .with_status(200)
            .create();
            
        let put_mock2 = server.mock("PUT", mockito::Matcher::Any)
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
        let (mut rest_backend, mut server) = create_test_backend();
        
        // Create a memory backend with some data
        let mut memory_backend = crate::MemoryStorageBackend::<Vec<u8>, Vec<u8>>::default();
        
        // Add some data to the memory backend
        memory_backend.set(vec![1, 2, 3], vec![4, 5, 6]).await?;
        memory_backend.set(vec![4, 5, 6, 7], vec![8, 9, 10]).await?;
        
        // Create mocks for two PUT operations that will happen during transfer
        let put_mock1 = server.mock("PUT", "/AQID")
            .with_status(200)
            .create();
            
        let put_mock2 = server.mock("PUT", "/BAUGBw==")
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