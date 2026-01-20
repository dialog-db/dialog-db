//! S3 credentials for direct access.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dialog_common::Provider;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use url::Url;

use crate::access::{Claim, RequestDescriptor, archive, memory, storage};
use crate::{Address, AuthorizationError};

use super::{build_url, extract_host, is_path_style_default};

#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;

#[cfg(target_arch = "wasm32")]
use web_time::{SystemTime, web::SystemTimeExt};

/// Public S3 credentials for unsigned access.
///
/// Use this for publicly accessible buckets that don't require authentication.
#[derive(Debug, Clone)]
pub struct PublicCredentials {
    /// S3 address (endpoint, region, bucket)
    address: Address,
    /// Parsed endpoint URL
    endpoint: Url,
    /// Whether to use path-style URLs
    path_style: bool,
}

impl PublicCredentials {
    /// Create new public credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL in the address is invalid.
    pub fn new(address: Address) -> Result<Self, AuthorizationError> {
        let endpoint = Url::parse(address.endpoint())
            .map_err(|e| AuthorizationError::Configuration(e.to_string()))?;
        let path_style = is_path_style_default(&endpoint);

        Ok(Self {
            address,
            endpoint,
            path_style,
        })
    }

    /// Set whether to use path-style URLs.
    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = path_style;
        self
    }

    /// Get the region.
    pub fn region(&self) -> &str {
        self.address.region()
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        self.address.bucket()
    }

    /// Build a URL for the given key path.
    pub fn build_url(&self, path: &str) -> Result<Url, AuthorizationError> {
        build_url(&self.endpoint, self.address.bucket(), path, self.path_style)
    }

    /// Authorize a claim by generating an unsigned URL for public access.
    pub fn authorize<C: Claim>(&self, claim: C) -> Result<RequestDescriptor, AuthorizationError> {
        let path = claim.path();
        let mut url = self.build_url(&path)?;

        // Add query parameters if specified
        if let Some(params) = claim.params() {
            let mut query = url.query_pairs_mut();
            for (key, value) in params {
                query.append_pair(key, value);
            }
        }

        let host = extract_host(&url)?;

        let mut headers = vec![("host".to_string(), host)];
        if let Some(checksum) = claim.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }

        Ok(RequestDescriptor {
            url,
            method: claim.method().to_string(),
            headers,
        })
    }
}

/// Private S3 credentials with AWS SigV4 signing.
///
/// Use this for authenticated access to S3 buckets.
#[derive(Debug, Clone)]
pub struct PrivateCredentials {
    /// AWS Access Key ID
    access_key_id: String,
    /// AWS Secret Access Key
    secret_access_key: String,
    /// S3 address (endpoint, region, bucket)
    address: Address,
    /// Parsed endpoint URL
    endpoint: Url,
    /// Whether to use path-style URLs
    path_style: bool,
}

impl PrivateCredentials {
    /// Create new private credentials with AWS SigV4 signing.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL in the address is invalid.
    pub fn new(
        address: Address,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self, AuthorizationError> {
        let endpoint = Url::parse(address.endpoint())
            .map_err(|e| AuthorizationError::Configuration(e.to_string()))?;
        let path_style = is_path_style_default(&endpoint);

        Ok(Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            address,
            endpoint,
            path_style,
        })
    }

    /// Set whether to use path-style URLs.
    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = path_style;
        self
    }

    /// Get the access key ID.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Get the region.
    pub fn region(&self) -> &str {
        self.address.region()
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        self.address.bucket()
    }

    /// Build a URL for the given key path.
    pub fn build_url(&self, path: &str) -> Result<Url, AuthorizationError> {
        build_url(&self.endpoint, self.address.bucket(), path, self.path_style)
    }

    /// Authorize a claim by generating a presigned URL with AWS SigV4 signature.
    pub fn authorize<C: Claim>(&self, claim: C) -> Result<RequestDescriptor, AuthorizationError> {
        let time = current_time();
        let timestamp = time.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[0..8];

        let region = self.region();
        let service = claim.service();
        let expires = claim.expires();

        // Derive signing key on demand
        let key = SigningKey::derive(&self.secret_access_key, date, region, service);
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        let path = claim.path();
        // Extract host from request URL, including port for non-standard ports.
        let url = self.build_url(&path)?;

        // hostname does not include port, so we check if there is port in the
        // host and include it if present
        let hostname = url
            .host_str()
            .ok_or_else(|| AuthorizationError::Configuration("URL missing host".into()))?;
        let host = if let Some(port) = url.port() {
            format!("{}:{}", hostname, port)
        } else {
            hostname.to_string()
        };

        // Build signed headers
        let mut headers = vec![("host".to_string(), host.clone())];
        // If request has a checksum, we add it to the headers so that S3 will
        // will perform integrity checks on the data.
        if let Some(checksum) = claim.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_headers: String = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");

        // Build query parameters
        let mut query_params: Vec<(String, String)> = vec![
            ("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()),
            ("X-Amz-Content-Sha256".into(), "UNSIGNED-PAYLOAD".into()),
            (
                "X-Amz-Credential".into(),
                format!("{}/{}", self.access_key_id, scope),
            ),
            ("X-Amz-Date".into(), timestamp.clone()),
            ("X-Amz-Expires".into(), expires.to_string()),
        ];

        // Include ACL if specified by the request
        if let Some(acl) = claim.acl() {
            query_params.push(("x-amz-acl".into(), acl.as_str().to_string()));
        }

        query_params.push(("X-Amz-SignedHeaders".into(), signed_headers.clone()));

        // Include existing query parameters from the request URL
        // (e.g., list-type=2, prefix=... for ListObjectsV2)
        if let Some(params) = claim.params() {
            for (key, value) in params {
                query_params.push((key.to_owned(), value.to_owned()));
            }
        }

        // Sort all query parameters alphabetically (required by SigV4)
        query_params.sort_by(|a, b| a.0.cmp(&b.0));

        // Build canonical request
        let canonical_uri = percent_encode_path(url.path());

        let canonical_query: String = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_headers: String = headers
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v.trim()))
            .collect::<Vec<_>>()
            .join("\n");

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n\n{}\nUNSIGNED-PAYLOAD",
            claim.method(),
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers
        );

        // Create string to sign
        let digest = Sha256::digest(canonical_request.as_bytes());
        let payload = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            timestamp,
            scope,
            hex_encode(&digest)
        );

        // Compute signature
        let signature = key.sign(payload.as_bytes());

        // Build final URL with all query parameters
        let mut url = url.clone();
        url.set_query(None); // Clear existing query params (we'll add them all back)
        {
            let mut query = url.query_pairs_mut();
            for (k, v) in &query_params {
                query.append_pair(k, v);
            }
            query.append_pair("X-Amz-Signature", &signature.to_string());
        }

        Ok(RequestDescriptor {
            url,
            method: claim.method().to_string(),
            headers,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Get> for PublicCredentials {
    async fn execute(&self, effect: storage::Get) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Set> for PublicCredentials {
    async fn execute(&self, effect: storage::Set) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Delete> for PublicCredentials {
    async fn execute(
        &self,
        effect: storage::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::List> for PublicCredentials {
    async fn execute(
        &self,
        effect: storage::List,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Resolve> for PublicCredentials {
    async fn execute(
        &self,
        effect: memory::Resolve,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Update> for PublicCredentials {
    async fn execute(
        &self,
        effect: memory::Update,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Delete> for PublicCredentials {
    async fn execute(
        &self,
        effect: memory::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Get> for PublicCredentials {
    async fn execute(&self, effect: archive::Get) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Put> for PublicCredentials {
    async fn execute(&self, effect: archive::Put) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Delete> for PublicCredentials {
    async fn execute(
        &self,
        effect: archive::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::List> for PublicCredentials {
    async fn execute(
        &self,
        effect: archive::List,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Get> for PrivateCredentials {
    async fn execute(&self, effect: storage::Get) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Set> for PrivateCredentials {
    async fn execute(&self, effect: storage::Set) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Delete> for PrivateCredentials {
    async fn execute(
        &self,
        effect: storage::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::List> for PrivateCredentials {
    async fn execute(
        &self,
        effect: storage::List,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Resolve> for PrivateCredentials {
    async fn execute(
        &self,
        effect: memory::Resolve,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Update> for PrivateCredentials {
    async fn execute(
        &self,
        effect: memory::Update,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Delete> for PrivateCredentials {
    async fn execute(
        &self,
        effect: memory::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Get> for PrivateCredentials {
    async fn execute(&self, effect: archive::Get) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Put> for PrivateCredentials {
    async fn execute(&self, effect: archive::Put) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Delete> for PrivateCredentials {
    async fn execute(
        &self,
        effect: archive::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::List> for PrivateCredentials {
    async fn execute(
        &self,
        effect: archive::List,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        self.authorize(effect)
    }
}

/// S3 credentials for direct bucket access.
///
/// This enum supports both public (unsigned) and private (SigV4 signed) access
/// to S3-compatible storage.
///
/// Implements [`Provider<Access<storage::*>>`] to produce [`RequestDescriptor`]
/// for making S3 requests.
///
/// # Example
///
/// ```no_run
/// use dialog_s3_credentials::{Address, s3::Credentials};
/// use dialog_common::Provider;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let address = Address::new(
///     "https://s3.us-east-1.amazonaws.com",
///     "us-east-1",
///     "my-bucket",
/// );
///
/// // Public access (no signing)
/// let public = Credentials::public(address.clone())?;
///
/// // Private access (SigV4 signing)
/// let private = Credentials::private(
///     address,
///     "AKIAIOSFODNN7EXAMPLE",
///     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
/// )?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub enum Credentials {
    /// Public access without signing.
    Public(PublicCredentials),
    /// Private access with AWS SigV4 signing.
    Private(PrivateCredentials),
}

impl Credentials {
    /// Create public credentials for unsigned access.
    pub fn public(address: Address) -> Result<Self, AuthorizationError> {
        Ok(Self::Public(PublicCredentials::new(address)?))
    }

    /// Create private credentials with AWS SigV4 signing.
    pub fn private(
        address: Address,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self, AuthorizationError> {
        Ok(Self::Private(PrivateCredentials::new(
            address,
            access_key_id,
            secret_access_key,
        )?))
    }

    /// Set whether to use path-style URLs.
    pub fn with_path_style(self, path_style: bool) -> Self {
        match self {
            Self::Public(c) => Self::Public(c.with_path_style(path_style)),
            Self::Private(c) => Self::Private(c.with_path_style(path_style)),
        }
    }

    /// Get the region.
    pub fn region(&self) -> &str {
        match self {
            Self::Public(c) => c.region(),
            Self::Private(c) => c.region(),
        }
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        match self {
            Self::Public(c) => c.bucket(),
            Self::Private(c) => c.bucket(),
        }
    }

    /// Build a URL for the given key path.
    pub fn build_url(&self, path: &str) -> Result<Url, AuthorizationError> {
        match self {
            Self::Public(c) => c.build_url(path),
            Self::Private(c) => c.build_url(path),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Get> for Credentials {
    async fn execute(&self, effect: storage::Get) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Set> for Credentials {
    async fn execute(&self, effect: storage::Set) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::Delete> for Credentials {
    async fn execute(
        &self,
        effect: storage::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<storage::List> for Credentials {
    async fn execute(
        &self,
        effect: storage::List,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Resolve> for Credentials {
    async fn execute(
        &self,
        effect: memory::Resolve,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Update> for Credentials {
    async fn execute(
        &self,
        effect: memory::Update,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<memory::Delete> for Credentials {
    async fn execute(
        &self,
        effect: memory::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Get> for Credentials {
    async fn execute(&self, effect: archive::Get) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Put> for Credentials {
    async fn execute(&self, effect: archive::Put) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::Delete> for Credentials {
    async fn execute(
        &self,
        effect: archive::Delete,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<archive::List> for Credentials {
    async fn execute(
        &self,
        effect: archive::List,
    ) -> Result<RequestDescriptor, AuthorizationError> {
        match self {
            Self::Public(c) => c.execute(effect).await,
            Self::Private(c) => c.execute(effect).await,
        }
    }
}

/// AWS SigV4 signing key derived from credentials.
#[derive(Debug, Clone)]
struct SigningKey(Vec<u8>);

impl SigningKey {
    fn derive(secret: &str, date: &str, region: &str, service: &str) -> Self {
        let secret = format!("AWS4{}", secret);
        let k_date = Self::hmac(secret.as_bytes(), date.as_bytes());
        let k_region = Self::hmac(&k_date, region.as_bytes());
        let k_service = Self::hmac(&k_region, service.as_bytes());
        Self(Self::hmac(&k_service, b"aws4_request"))
    }

    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts keys of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    fn sign(&self, data: &[u8]) -> Signature {
        Signature(Self::hmac(&self.0, data))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Signature(Vec<u8>);

impl std::fmt::Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex_encode(&self.0))
    }
}

/// Get the current time as a UTC datetime.
pub fn current_time() -> DateTime<Utc> {
    #[cfg(target_arch = "wasm32")]
    {
        DateTime::<Utc>::from(SystemTime::now().to_std())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        DateTime::<Utc>::from(SystemTime::now())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(s, "{:02x}", byte).unwrap();
    }
    s
}

fn percent_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                write!(result, "%{:02X}", byte).unwrap();
            }
        }
    }
    result
}

fn percent_encode_path(path: &str) -> String {
    percent_encode(path).replace("%2F", "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_address() -> Address {
        Address::new("https://s3.amazonaws.com", "us-east-1", "bucket")
    }

    #[test]
    fn it_creates_public_credentials() {
        let creds = PublicCredentials::new(test_address()).unwrap();
        assert_eq!(creds.region(), "us-east-1");
        assert_eq!(creds.bucket(), "bucket");
    }

    #[test]
    fn it_creates_private_credentials() {
        let creds = PrivateCredentials::new(test_address(), "access-key", "secret-key").unwrap();
        assert_eq!(creds.region(), "us-east-1");
        assert_eq!(creds.access_key_id(), "access-key");
    }

    #[test]
    fn it_creates_credentials_enum_public() {
        let creds = Credentials::public(test_address()).unwrap();
        assert_eq!(creds.region(), "us-east-1");
        assert!(matches!(creds, Credentials::Public(_)));
    }

    #[test]
    fn it_creates_credentials_enum_private() {
        let creds = Credentials::private(test_address(), "key", "secret").unwrap();
        assert_eq!(creds.region(), "us-east-1");
        assert!(matches!(creds, Credentials::Private(_)));
    }

    #[test]
    fn it_builds_virtual_hosted_url() {
        let creds = PublicCredentials::new(test_address()).unwrap();
        let url = creds.build_url("my-key").unwrap();
        assert_eq!(url.as_str(), "https://bucket.s3.amazonaws.com/my-key");
    }

    #[test]
    fn it_builds_path_style_url() {
        let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
        let creds = PublicCredentials::new(address).unwrap();
        let url = creds.build_url("my-key").unwrap();
        assert_eq!(url.as_str(), "http://localhost:9000/bucket/my-key");
    }

    #[test]
    fn it_presigns_public_request() {
        let creds = PublicCredentials::new(test_address()).unwrap();
        let effect = storage::Get::new("", "test-key");
        let descriptor = creds.authorize(effect).unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.headers.iter().any(|(k, _)| k == "host"));
        // Public requests should NOT have signing params
        assert!(!descriptor.url.as_str().contains("X-Amz-Signature"));
    }

    #[test]
    fn it_presigns_private_request() {
        let creds =
            PrivateCredentials::new(test_address(), "AKIAIOSFODNN7EXAMPLE", "secret").unwrap();
        let effect = storage::Get::new("", "test-key");
        let descriptor = creds.authorize(effect).unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(
            descriptor
                .url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(descriptor.url.as_str().contains("X-Amz-Signature="));
    }

    #[test]
    fn it_forces_path_style() {
        let creds = PublicCredentials::new(test_address())
            .unwrap()
            .with_path_style(true);
        let url = creds.build_url("key").unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/key");
    }

    #[test]
    fn it_forces_path_style_via_enum() {
        let creds = Credentials::public(test_address())
            .unwrap()
            .with_path_style(true);
        let url = creds.build_url("key").unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/key");
    }
}
