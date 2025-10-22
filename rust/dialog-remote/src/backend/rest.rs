//! REST backend implementation for remote revision storage

use async_trait::async_trait;
use dialog_artifacts::Revision;
use dialog_storage::StorageBackend;
use reqwest::Client;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{RevisionBackendError, RevisionUpgrade, Subject};

/// Authentication methods for REST backend
#[derive(Clone, Debug)]
pub enum AuthMethod {
    /// No authentication
    ///
    /// Uses HEAD and PUT requests to the endpoint:
    /// - HEAD `{endpoint}/{did}` - Query current revision (returns ETag header)
    /// - PUT `{endpoint}/{did}` - Update revision with If-Match header
    None,

    /// Bearer token authentication
    ///
    /// Same as None, but includes `Authorization: Bearer {token}` header in all requests
    Bearer(String),
}

/// Configuration for REST revision backend
#[derive(Clone, Debug)]
pub struct RestBackendConfig {
    /// Base URL for the REST API (e.g., "https://api.example.com/register")
    pub endpoint: String,

    /// Authentication method
    pub auth_method: AuthMethod,

    /// Optional timeout for requests in seconds (default: 30)
    pub timeout_seconds: Option<u64>,

    /// Optional custom headers to send with each request
    pub headers: Vec<(String, String)>,
}

impl Default for RestBackendConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8080".to_string(),
            auth_method: AuthMethod::None,
            timeout_seconds: Some(30),
            headers: Vec::new(),
        }
    }
}

impl RestBackendConfig {
    /// Create a new REST backend configuration
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Default::default()
        }
    }

    /// Set the authentication method
    pub fn with_auth(mut self, auth_method: AuthMethod) -> Self {
        self.auth_method = auth_method;
        self
    }

    /// Set the request timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = Some(seconds);
        self
    }

    /// Add a custom header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}

// Helper functions for hex serialization
fn serialize_revision_hex<S>(revision: &Revision, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&hex::encode(revision.index()))
}

fn deserialize_revision_hex<'de, D>(deserializer: D) -> Result<Revision, D::Error>
where
    D: Deserializer<'de>,
{
    let hex_str = String::deserialize(deserializer)?;
    let bytes = hex::decode(&hex_str).map_err(serde::de::Error::custom)?;

    if bytes.len() != 32 {
        return Err(serde::de::Error::custom(format!(
            "Invalid revision length: expected 32, got {}",
            bytes.len()
        )));
    }

    let mut array = [0u8; 32];
    array.copy_from_slice(&bytes);
    Ok(Revision::new(&array))
}

/// Payload structure for revision updates (implements RevisionUpgrade)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevisionPayload {
    /// The issuer (same as subject)
    pub iss: String,
    /// The subject (DID being updated)
    pub sub: String,
    /// The command (always "/state/assert")
    pub cmd: String,
    /// Arguments containing the revision
    pub args: RevisionArgs,
    /// The expected current revision (for CAS) - serialized at top level
    #[serde(
        serialize_with = "serialize_revision_hex",
        deserialize_with = "deserialize_revision_hex"
    )]
    pub origin: Revision,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevisionArgs {
    /// The new revision hash
    #[serde(
        serialize_with = "serialize_revision_hex",
        deserialize_with = "deserialize_revision_hex"
    )]
    pub revision: Revision,
}

impl RevisionUpgrade for RevisionPayload {
    fn revision(&self) -> &Revision {
        &self.args.revision
    }

    fn origin(&self) -> &Revision {
        &self.origin
    }
}

impl RevisionPayload {
    /// Create a new revision payload
    pub fn new(subject: &Subject, origin: Revision, revision: Revision) -> Self {
        Self {
            iss: subject.did().to_string(),
            sub: subject.did().to_string(),
            cmd: "/state/assert".to_string(),
            args: RevisionArgs { revision },
            origin,
        }
    }
}

/// REST revision backend implementation
///
/// Implements `RevisionStorageBackend` using the Register protocol from sync.md:
/// - HEAD /{did} - Query current revision (returns ETag with revision hash)
/// - PUT /{did} - Update revision with compare-and-swap (uses If-Match header)
///
/// # Examples
///
/// ```no_run
/// use dialog_remote::backend::{RestBackend, RestBackendConfig, AuthMethod, Subject, RevisionUpgrade, RevisionStorageBackend, RevisionPayload};
/// use dialog_remote::StorageBackend;
/// use dialog_artifacts::Revision;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create REST backend with no auth
/// let config = RestBackendConfig::new("https://api.example.com/register")
///     .with_timeout(60);
///
/// let mut backend = RestBackend::new(config);
///
/// // With bearer token
/// let config = RestBackendConfig::new("https://api.example.com/register")
///     .with_auth(AuthMethod::Bearer("my-token".to_string()));
///
/// let mut backend = RestBackend::new(config);
///
/// // Get current revision
/// let subject = Subject::new("did:key:z6Mkk...");
/// let current = backend.get(&subject).await?;
///
/// // Update with CAS
/// if let Some(upgrade) = current {
///     let new_rev = Revision::new(&[2; 32]);
///     let new_payload = RevisionPayload::new(&subject, upgrade.revision().clone(), new_rev);
///     backend.set(subject, new_payload).await?;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct RestBackend {
    config: RestBackendConfig,
    client: Client,
}

impl RestBackend {
    /// Create a new REST backend with the given configuration
    pub fn new(config: RestBackendConfig) -> Self {
        let mut client_builder = Client::builder();

        if let Some(timeout) = config.timeout_seconds {
            client_builder = client_builder.timeout(std::time::Duration::from_secs(timeout));
        }

        let client = client_builder.build().unwrap_or_else(|_| Client::new());

        Self { config, client }
    }

    /// Get the URL for a subject
    fn url_for_subject(&self, subject: &Subject) -> String {
        format!("{}/{}", self.config.endpoint, subject.did())
    }

    /// Build a request with authentication and custom headers
    fn build_request(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut builder = builder;

        // Add authentication
        match &self.config.auth_method {
            AuthMethod::None => {}
            AuthMethod::Bearer(token) => {
                builder = builder.bearer_auth(token);
            }
        }

        // Add custom headers
        for (key, value) in &self.config.headers {
            builder = builder.header(key, value);
        }

        builder
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl StorageBackend for RestBackend {
    type Key = Subject;
    type Value = RevisionPayload;
    type Error = RevisionBackendError;

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let url = self.url_for_subject(key);

        let request = self.client.head(&url);
        let request = self.build_request(request);

        let response = request
            .send()
            .await
            .map_err(|e| RevisionBackendError::FetchFailed {
                subject: key.clone(),
                reason: format!("HTTP request failed: {}", e),
            })?;

        let status = response.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(RevisionBackendError::Unauthorized {
                subject: key.clone(),
                reason: "Authentication failed".to_string(),
            });
        }

        if !status.is_success() {
            return Err(RevisionBackendError::FetchFailed {
                subject: key.clone(),
                reason: format!(
                    "HTTP {} - {}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("Unknown")
                ),
            });
        }

        // Get revision from ETag header
        let etag = response
            .headers()
            .get("etag")
            .ok_or_else(|| RevisionBackendError::FetchFailed {
                subject: key.clone(),
                reason: "Missing ETag header in response".to_string(),
            })?
            .to_str()
            .map_err(|e| RevisionBackendError::FetchFailed {
                subject: key.clone(),
                reason: format!("Invalid ETag header: {}", e),
            })?;

        // Parse revision from hex ETag
        let revision = deserialize_revision_hex(&mut serde_json::Deserializer::from_str(&format!(
            "\"{}\"",
            etag.trim_matches('"')
        )))
        .map_err(|e| RevisionBackendError::FetchFailed {
            subject: key.clone(),
            reason: format!("Failed to parse ETag: {}", e),
        })?;

        // For GET, return a payload with origin = current revision
        Ok(Some(RevisionPayload::new(key, revision.clone(), revision)))
    }

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let url = self.url_for_subject(&key);
        let expected_hex = hex::encode(value.origin().index());

        let request = self
            .client
            .put(&url)
            .header("If-Match", expected_hex)
            .json(&value);
        let request = self.build_request(request);

        let response = request
            .send()
            .await
            .map_err(|e| RevisionBackendError::PublishFailed {
                subject: key.clone(),
                reason: format!("HTTP request failed: {}", e),
            })?;

        let status = response.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(RevisionBackendError::NotFound { subject: key });
        }

        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(RevisionBackendError::Unauthorized {
                subject: key,
                reason: "Authentication failed".to_string(),
            });
        }

        if status == reqwest::StatusCode::PRECONDITION_FAILED {
            // Get the actual revision from ETag
            let actual = if let Some(etag) = response.headers().get("etag") {
                if let Ok(etag_str) = etag.to_str() {
                    deserialize_revision_hex(&mut serde_json::Deserializer::from_str(&format!(
                        "\"{}\"",
                        etag_str.trim_matches('"')
                    )))
                    .ok()
                } else {
                    None
                }
            } else {
                None
            };

            return Err(RevisionBackendError::RevisionMismatch {
                subject: key,
                expected: value.origin().clone(),
                actual: actual.unwrap_or_else(|| value.origin().clone()),
            });
        }

        if !status.is_success() {
            return Err(RevisionBackendError::PublishFailed {
                subject: key,
                reason: format!(
                    "HTTP {} - {}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("Unknown")
                ),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = RestBackendConfig::new("https://api.example.com")
            .with_auth(AuthMethod::Bearer("token123".to_string()))
            .with_timeout(60)
            .with_header("X-Custom", "value");

        assert_eq!(config.endpoint, "https://api.example.com");
        assert!(matches!(config.auth_method, AuthMethod::Bearer(_)));
        assert_eq!(config.timeout_seconds, Some(60));
        assert_eq!(config.headers.len(), 1);
    }

    #[test]
    fn test_url_generation() {
        let config = RestBackendConfig::new("https://api.example.com/register");
        let backend = RestBackend::new(config);
        let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");

        let url = backend.url_for_subject(&subject);
        assert_eq!(
            url,
            "https://api.example.com/register/did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi"
        );
    }

    #[test]
    fn test_revision_payload_serde() {
        let subject = Subject::new("did:key:z6Mkk...");
        let origin = Revision::new(&[1; 32]);
        let revision = Revision::new(&[2; 32]);

        let payload = RevisionPayload::new(&subject, origin.clone(), revision.clone());

        // Test serialization
        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains(&hex::encode(revision.index())));
        assert!(json.contains(&hex::encode(origin.index())));

        // Test deserialization
        let parsed: RevisionPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.revision(), &revision);
        assert_eq!(parsed.origin(), &origin);
    }

    // Integration tests using actual HTTP server with MemoryBackendProvider
    #[cfg(not(target_arch = "wasm32"))]
    mod integration {
        use super::*;
        use crate::backend::{MemoryBackendProvider, RevisionUpgradeRecord};
        use axum::{
            extract::{Path, State},
            http::{HeaderMap, StatusCode},
            response::{IntoResponse, Response},
            routing::{head, put},
            Json, Router,
        };
        use tokio::net::TcpListener;

        /// Test HTTP server state
        #[derive(Clone)]
        struct ServerState {
            provider: MemoryBackendProvider,
        }

        /// Handle HEAD /{did} - query current revision
        async fn handle_head(
            State(state): State<ServerState>,
            Path(did): Path<String>,
        ) -> Response {
            let subject = Subject::new(did);
            let backend = state.provider.connect();

            match backend.get(&subject).await {
                Ok(Some(upgrade)) => {
                    let etag = hex::encode(upgrade.revision().index());
                    (
                        StatusCode::OK,
                        [(axum::http::header::ETAG, etag)],
                        "",
                    )
                        .into_response()
                }
                Ok(None) => StatusCode::NOT_FOUND.into_response(),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }

        /// Handle PUT /{did} - update revision with CAS
        async fn handle_put(
            State(state): State<ServerState>,
            Path(did): Path<String>,
            headers: HeaderMap,
            Json(payload): Json<RevisionPayload>,
        ) -> Response {
            let subject = Subject::new(did);
            let mut backend = state.provider.connect();

            // Check If-Match header
            let if_match = headers
                .get(axum::http::header::IF_MATCH)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            let expected_hex = hex::encode(payload.origin().index());
            if if_match != expected_hex {
                return (
                    StatusCode::BAD_REQUEST,
                    "If-Match header does not match origin",
                )
                    .into_response();
            }

            // Convert RevisionPayload to RevisionUpgradeRecord
            let upgrade = RevisionUpgradeRecord::new(
                payload.origin().clone(),
                payload.revision().clone(),
            );

            match backend.set(subject, upgrade).await {
                Ok(_) => StatusCode::OK.into_response(),
                Err(RevisionBackendError::RevisionMismatch { actual, .. }) => {
                    let etag = hex::encode(actual.index());
                    (
                        StatusCode::PRECONDITION_FAILED,
                        [(axum::http::header::ETAG, etag)],
                        "",
                    )
                        .into_response()
                }
                Err(RevisionBackendError::NotFound { .. }) => {
                    StatusCode::NOT_FOUND.into_response()
                }
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }

        /// Create and start test server, return the URL
        async fn start_test_server(provider: MemoryBackendProvider) -> String {
            let state = ServerState { provider };

            let app = Router::new()
                .route("/:did", head(handle_head))
                .route("/:did", put(handle_put))
                .with_state(state);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });

            format!("http://{}", addr)
        }

        #[tokio::test]
        async fn test_get_success() {
            let provider = MemoryBackendProvider::new();
            let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");
            let revision = Revision::new(&[1; 32]);

            provider.initialize(&subject, revision.clone()).await.unwrap();

            let url = start_test_server(provider).await;
            let config = RestBackendConfig::new(url);
            let backend = RestBackend::new(config);

            let result = backend.get(&subject).await.unwrap();
            assert!(result.is_some());
            let upgrade = result.unwrap();
            assert_eq!(upgrade.revision(), &revision);
            assert_eq!(upgrade.origin(), &revision);
        }

        #[tokio::test]
        async fn test_get_not_found() {
            let provider = MemoryBackendProvider::new();
            let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");

            let url = start_test_server(provider).await;
            let config = RestBackendConfig::new(url);
            let backend = RestBackend::new(config);

            let result = backend.get(&subject).await.unwrap();
            assert!(result.is_none());
        }

        #[tokio::test]
        async fn test_set_success() {
            let provider = MemoryBackendProvider::new();
            let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");
            let rev1 = Revision::new(&[1; 32]);
            let rev2 = Revision::new(&[2; 32]);

            provider.initialize(&subject, rev1.clone()).await.unwrap();

            let url = start_test_server(provider.clone()).await;
            let config = RestBackendConfig::new(url);
            let mut backend = RestBackend::new(config);

            let payload = RevisionPayload::new(&subject, rev1.clone(), rev2.clone());
            backend.set(subject.clone(), payload).await.unwrap();

            // Verify via provider
            let check_backend = provider.connect();
            let current = check_backend.get(&subject).await.unwrap().unwrap();
            assert_eq!(current.revision(), &rev2);
        }

        #[tokio::test]
        async fn test_set_precondition_failed() {
            let provider = MemoryBackendProvider::new();
            let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");
            let rev1 = Revision::new(&[1; 32]);
            let rev2 = Revision::new(&[2; 32]);
            let rev3 = Revision::new(&[3; 32]);

            // Initialize with rev1
            provider.initialize(&subject, rev1.clone()).await.unwrap();

            let url = start_test_server(provider).await;
            let config = RestBackendConfig::new(url);
            let mut backend = RestBackend::new(config);

            // Try to update from rev2 to rev3 (but current is rev1)
            let payload = RevisionPayload::new(&subject, rev2, rev3);
            let result = backend.set(subject, payload).await;

            assert!(matches!(
                result,
                Err(RevisionBackendError::RevisionMismatch { .. })
            ));

            if let Err(RevisionBackendError::RevisionMismatch { actual, .. }) = result {
                assert_eq!(actual, rev1);
            }
        }

        #[tokio::test]
        async fn test_set_not_found() {
            let provider = MemoryBackendProvider::new();
            let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");
            let rev1 = Revision::new(&[1; 32]);
            let rev2 = Revision::new(&[2; 32]);

            let url = start_test_server(provider).await;
            let config = RestBackendConfig::new(url);
            let mut backend = RestBackend::new(config);

            // Try to update non-existent subject with wrong origin
            let payload = RevisionPayload::new(&subject, rev1, rev2);
            let result = backend.set(subject, payload).await;

            assert!(matches!(result, Err(RevisionBackendError::NotFound { .. })));
        }

        #[tokio::test]
        async fn test_concurrent_updates() {
            let provider = MemoryBackendProvider::new();
            let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");
            let rev0 = Revision::new(&[0; 32]);
            let rev1 = Revision::new(&[1; 32]);
            let rev2 = Revision::new(&[2; 32]);

            provider.initialize(&subject, rev0.clone()).await.unwrap();

            let url = start_test_server(provider.clone()).await;

            // Create two REST backends
            let config1 = RestBackendConfig::new(url.clone());
            let mut backend1 = RestBackend::new(config1);

            let config2 = RestBackendConfig::new(url);
            let mut backend2 = RestBackend::new(config2);

            // Both try to update from rev0
            let payload1 = RevisionPayload::new(&subject, rev0.clone(), rev1.clone());
            let payload2 = RevisionPayload::new(&subject, rev0.clone(), rev2.clone());

            let result1 = backend1.set(subject.clone(), payload1).await;
            let result2 = backend2.set(subject.clone(), payload2).await;

            // One should succeed, one should fail
            let success_count = [result1.is_ok(), result2.is_ok()]
                .iter()
                .filter(|&&x| x)
                .count();
            assert_eq!(success_count, 1);

            // Final state should be one of the two
            let check_backend = provider.connect();
            let final_rev = check_backend.get(&subject).await.unwrap().unwrap();
            assert!(final_rev.revision() == &rev1 || final_rev.revision() == &rev2);
        }

        #[tokio::test]
        async fn test_initial_state_with_zero_hash() {
            let provider = MemoryBackendProvider::new();
            let subject = Subject::new("did:key:z6Mkk89bC3JrVqKie71YEcc5M1SMVxuCgNx6zLZ8SYJsxALi");
            let zero = Revision::new(&[0; 32]);
            let rev1 = Revision::new(&[1; 32]);

            let url = start_test_server(provider.clone()).await;
            let config = RestBackendConfig::new(url);
            let mut backend = RestBackend::new(config);

            // Set initial revision from zero hash
            let payload = RevisionPayload::new(&subject, zero, rev1.clone());
            backend.set(subject.clone(), payload).await.unwrap();

            // Verify it was set
            let check_backend = provider.connect();
            let current = check_backend.get(&subject).await.unwrap().unwrap();
            assert_eq!(current.revision(), &rev1);
        }
    }
}
