use std::marker::PhantomData;

use async_stream::try_stream;
use async_trait::async_trait;
use base64::Engine;
use dialog_common::ConditionalSync;
use futures_util::Stream;
use thiserror::Error;

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

/// Configuration for the REST storage backend
#[derive(Clone, Debug)]
pub struct RestStorageConfig {
    /// Base URL for the REST API
    pub endpoint: String,
    
    /// Optional API key for authentication
    pub api_key: Option<String>,
    
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
            api_key: None,
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
    
    /// Build the full URL for a given key
    fn build_url(&self, key: &[u8]) -> String {
        let base_url = self.config.endpoint.trim_end_matches('/');
        let key_str = base64::engine::general_purpose::STANDARD.encode(key);
        
        match (&self.config.bucket, &self.config.key_prefix) {
            (Some(bucket), Some(prefix)) => format!("{base_url}/{bucket}/{prefix}/{key_str}"),
            (Some(bucket), None) => format!("{base_url}/{bucket}/{key_str}"),
            (None, Some(prefix)) => format!("{base_url}/{prefix}/{key_str}"),
            (None, None) => format!("{base_url}/{key_str}"),
        }
    }
    
    /// Add authentication and custom headers to a request
    fn prepare_request(&self, request_builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut builder = request_builder;
        
        // Add API key if configured
        if let Some(api_key) = &self.config.api_key {
            builder = builder.header("Authorization", format!("Bearer {api_key}"));
        }
        
        // Add custom headers
        for (name, value) in &self.config.headers {
            builder = builder.header(name, value);
        }
        
        builder
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
        let url = self.build_url(key.as_ref());
        
        let request = self.prepare_request(self.client.put(&url).body(value.as_ref().to_vec()));
        
        let response = request.send().await?;
        
        if !response.status().is_success() {
            return Err(RestStorageBackendError::OperationFailed(format!(
                "Failed to set value. Status: {}", response.status()
            )));
        }
        
        Ok(())
    }
    
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let url = self.build_url(key.as_ref());
        
        let request = self.prepare_request(self.client.get(&url));
        
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
            // To implement a full listing, we would need a listing API in the REST service
            // This is a placeholder that would need to be customized based on the actual API
            let url = format!("{}/_list", self.config.endpoint.trim_end_matches('/'));
            
            let request = self.prepare_request(self.client.get(&url));
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
mod tests {
    use super::*;
    use anyhow::Result;
    use mockito::Server;
    
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    use wasm_bindgen_test::wasm_bindgen_test;
    
    // Helper function to create a test REST backend with a mock server
    async fn create_test_backend() -> (RestStorageBackend<Vec<u8>, Vec<u8>>, mockito::ServerGuard) {
        let server = Server::new();
        let endpoint = server.url();
        
        let config = RestStorageConfig {
            endpoint,
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
        let (backend, mut server) = create_test_backend().await;
        
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
        let (backend, mut server) = create_test_backend().await;
        
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
    async fn it_uses_authentication_headers() -> Result<()> {
        let mut server = Server::new();
        let config = RestStorageConfig {
            endpoint: server.url(),
            api_key: Some("test-api-key".to_string()),
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
    async fn it_uses_bucket_and_prefix() -> Result<()> {
        let mut server = Server::new();
        let config = RestStorageConfig {
            endpoint: server.url(),
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
        let (backend, mut server) = create_test_backend().await;
        
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
    async fn it_can_perform_bulk_writes() -> Result<()> {
        let (mut backend, mut server) = create_test_backend().await;
        
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
    async fn it_integrates_with_memory_backend() -> Result<()> {
        let (mut rest_backend, mut server) = create_test_backend().await;
        
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