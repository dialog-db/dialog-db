//! S3 credentials for direct access.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use url::Url;

use crate::capability::{AuthorizedRequest, Precondition, S3Request};
use crate::{AccessError, Address};

use super::{build_url, extract_host, is_path_style_default};

/// Public S3 credentials for unsigned access.
///
/// Use this for publicly accessible buckets that don't require authentication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicCredentials {
    /// S3 address (endpoint, region, bucket)
    address: Address,
    /// Parsed endpoint URL
    endpoint: Url,
    /// Whether to use path-style URLs
    path_style: bool,
}

impl Serialize for PublicCredentials {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Only serialize the address - endpoint and path_style are derived
        self.address.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PublicCredentials {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let address = Address::deserialize(deserializer)?;
        PublicCredentials::new(address).map_err(serde::de::Error::custom)
    }
}

impl PublicCredentials {
    /// Create new public credentials.
    ///
    /// # Arguments
    ///
    /// * `address` - S3 address (endpoint, region, bucket)
    /// * `subject` - Subject DID used as path prefix within the bucket
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL in the address is invalid.
    pub fn new(address: Address) -> Result<Self, AccessError> {
        let endpoint = Url::parse(address.endpoint())
            .map_err(|e| AccessError::Configuration(e.to_string()))?;
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

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        self.address.endpoint()
    }

    /// Build a URL for the given key path.
    pub fn build_url(&self, path: &str) -> Result<Url, AccessError> {
        build_url(&self.endpoint, self.address.bucket(), path, self.path_style)
    }

    /// Generates an unsigned URL for public access.
    pub async fn grant<R: S3Request>(&self, request: &R) -> Result<AuthorizedRequest, AccessError> {
        let path = request.path();
        let mut url = self.build_url(&path)?;

        // Add query parameters if specified
        if let Some(params) = request.params() {
            let mut query = url.query_pairs_mut();
            for (key, value) in params {
                query.append_pair(&key, &value);
            }
        }

        let host = extract_host(&url)?;

        let mut headers = vec![("host".to_string(), host)];
        if let Some(checksum) = request.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }

        Ok(AuthorizedRequest {
            url,
            method: request.method().to_string(),
            headers,
        })
    }
}

/// Serialization helper for PrivateCredentials.
#[derive(Serialize, Deserialize)]
struct PrivateCredentialsSerde {
    address: Address,
    access_key_id: String,
    secret_access_key: String,
}

/// Private S3 credentials with AWS SigV4 signing.
///
/// Use this for authenticated access to S3 buckets.
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl Serialize for PrivateCredentials {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let helper = PrivateCredentialsSerde {
            address: self.address.clone(),
            access_key_id: self.access_key_id.clone(),
            secret_access_key: self.secret_access_key.clone(),
        };
        helper.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PrivateCredentials {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = PrivateCredentialsSerde::deserialize(deserializer)?;
        PrivateCredentials::new(
            helper.address,
            helper.access_key_id,
            helper.secret_access_key,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl PrivateCredentials {
    /// Create new private credentials with AWS SigV4 signing.
    ///
    /// # Arguments
    ///
    /// * `address` - S3 address (endpoint, region, bucket)
    /// * `subject` - Subject DID used as path prefix within the bucket
    /// * `access_key_id` - AWS Access Key ID
    /// * `secret_access_key` - AWS Secret Access Key
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL in the address is invalid.
    pub fn new(
        address: Address,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self, AccessError> {
        let endpoint = Url::parse(address.endpoint())
            .map_err(|e| AccessError::Configuration(e.to_string()))?;
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

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        self.address.endpoint()
    }

    /// Build a URL for the given key path.
    pub fn build_url(&self, path: &str) -> Result<Url, AccessError> {
        build_url(&self.endpoint, self.address.bucket(), path, self.path_style)
    }

    /// Generates an signed URL
    async fn grant<R: S3Request>(&self, request: &R) -> Result<AuthorizedRequest, AccessError> {
        let time = current_time();
        let timestamp = time.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[0..8];

        let region = self.region();
        let service = request.service();
        let expires = request.expires();

        // Derive signing key on demand
        let key = SigningKey::derive(&self.secret_access_key, date, region, service);
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        let path = request.path();
        // Extract host from request URL, including port for non-standard ports.
        let url = self.build_url(&path)?;

        // hostname does not include port, so we check if there is port in the
        // host and include it if present
        let hostname = url
            .host_str()
            .ok_or_else(|| AccessError::Configuration("URL missing host".into()))?;
        let host = if let Some(port) = url.port() {
            format!("{}:{}", hostname, port)
        } else {
            hostname.to_string()
        };

        // Build signed headers
        let mut headers = vec![("host".to_string(), host.clone())];
        // If request has a checksum, we add it to the headers so that S3 will
        // will perform integrity checks on the data.
        if let Some(checksum) = request.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }
        // Add precondition headers for CAS operations
        match request.precondition() {
            Precondition::IfMatch(etag) => {
                headers.push(("if-match".to_string(), format!("\"{}\"", etag)));
            }
            Precondition::IfNoneMatch => {
                headers.push(("if-none-match".to_string(), "*".to_string()));
            }
            Precondition::None => {}
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
        if let Some(acl) = request.acl() {
            query_params.push(("x-amz-acl".into(), acl.as_str().to_string()));
        }

        query_params.push(("X-Amz-SignedHeaders".into(), signed_headers.clone()));

        // Include existing query parameters from the request URL
        // (e.g., list-type=2, prefix=... for ListObjectsV2)
        if let Some(params) = request.params() {
            for (key, value) in params {
                query_params.push((key, value));
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
            request.method(),
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

        Ok(AuthorizedRequest {
            url,
            method: request.method().to_string(),
            headers,
        })
    }
}

/// S3 credentials for direct bucket access.
///
/// This enum supports both public (unsigned) and private (SigV4 signed) access
/// to S3-compatible storage.
///
/// The `subject` parameter identifies whose data we're accessing and is used
/// as a path prefix within the bucket. This allows multiple subjects to share
/// the same bucket with isolated storage paths.
///
/// Implements [`Signer`] to produce [`RequestDescriptor`] for making S3 requests.
///
/// # Example
///
/// ```no_run
/// use dialog_s3_credentials::{Address, s3::Credentials};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let address = Address::new(
///     "https://s3.us-east-1.amazonaws.com",
///     "us-east-1",
///     "my-bucket",
/// );
///
/// // Subject DID identifies whose data we're accessing
/// let subject = "did:key:zSubject";
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Credentials {
    /// Public access without signing.
    Public(PublicCredentials),
    /// Private access with AWS SigV4 signing.
    Private(PrivateCredentials),
}

impl From<PublicCredentials> for Credentials {
    fn from(credentials: PublicCredentials) -> Self {
        Self::Public(credentials)
    }
}

impl From<PrivateCredentials> for Credentials {
    fn from(credentials: PrivateCredentials) -> Self {
        Self::Private(credentials)
    }
}

impl Credentials {
    /// Create public credentials for unsigned access.
    pub fn public(address: Address) -> Result<Self, AccessError> {
        Ok(PublicCredentials::new(address)?.into())
    }

    /// Create private credentials with AWS SigV4 signing.
    pub fn private(
        address: Address,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Result<Self, AccessError> {
        Ok(PrivateCredentials::new(address, access_key_id, secret_access_key)?.into())
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

    /// Get the endpoint URL.
    pub fn endpoint(&self) -> &str {
        match self {
            Self::Public(c) => c.endpoint(),
            Self::Private(c) => c.endpoint(),
        }
    }

    /// Build a URL for the given key path.
    pub fn build_url(&self, path: &str) -> Result<Url, AccessError> {
        match self {
            Self::Public(c) => c.build_url(path),
            Self::Private(c) => c.build_url(path),
        }
    }

    /// Generates either an unsigned URL (for public credentials) or a
    /// presigned URL with AWS SigV4 signature (for private credentials).
    pub async fn grant<R: S3Request>(&self, request: &R) -> Result<AuthorizedRequest, AccessError> {
        match self {
            Self::Public(public) => public.grant(request).await,
            Self::Private(private) => private.grant(request).await,
        }
    }
}

/// AWS SigV4 signing key.
struct SigningKey(Hmac<Sha256>);

impl SigningKey {
    /// Derive a signing key for the given date, region, and service.
    fn derive(secret_key: &str, date: &str, region: &str, service: &str) -> Self {
        let date_key = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes());
        let region_key = hmac_sha256(&date_key, region.as_bytes());
        let service_key = hmac_sha256(&region_key, service.as_bytes());
        let signing_key = hmac_sha256(&service_key, b"aws4_request");

        Self(Hmac::new_from_slice(&signing_key).expect("HMAC can take key of any size"))
    }

    /// Sign a message with this key.
    fn sign(&self, message: &[u8]) -> Signature {
        let mut mac = self.0.clone();
        mac.update(message);
        Signature(mac.finalize().into_bytes().to_vec())
    }
}

/// AWS SigV4 signature.
struct Signature(Vec<u8>);

impl std::fmt::Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// Compute HMAC-SHA256.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Hex-encode bytes.
fn hex_encode(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(result, "{:02x}", byte).unwrap();
    }
    result
}

/// Percent-encode a string for URL use.
fn percent_encode(s: &str) -> String {
    let mut result = String::new();
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

/// Percent-encode a URL path (preserving slashes).
fn percent_encode_path(path: &str) -> String {
    path.split('/')
        .map(percent_encode)
        .collect::<Vec<_>>()
        .join("/")
}

/// Get the current time as a UTC datetime.
fn current_time() -> DateTime<Utc> {
    DateTime::<Utc>::from(dialog_common::time::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Checksum;
    // Use capability module for Storage/Store hierarchy, access module for effects
    use crate::capability::storage::{Get, Set, Storage, Store};
    use dialog_capability::{Capability, Subject};

    const TEST_SUBJECT: &str = "did:key:zTestSubject";

    /// Helper to build a storage Get capability.
    fn get_capability(store: &str, key: &[u8]) -> Capability<Get> {
        Subject::from(TEST_SUBJECT)
            .attenuate(Storage)
            .attenuate(Store::new(store))
            .invoke(Get::new(key))
    }

    /// Helper to build a storage Set capability.
    fn set_capability(store: &str, key: &[u8], checksum: Checksum) -> Capability<Set> {
        Subject::from(TEST_SUBJECT)
            .attenuate(Storage)
            .attenuate(Store::new(store))
            .invoke(Set::new(key, checksum))
    }

    #[dialog_common::test]
    async fn it_signs_with_public_credentials() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let creds = PublicCredentials::new(address).unwrap();

        let get = get_capability("index", b"test-key");
        let descriptor = creds.grant(&get).await.unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("my-bucket"));
        assert!(descriptor.url.as_str().contains("index/"));
    }

    #[dialog_common::test]
    async fn it_signs_with_private_credentials() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let creds = PrivateCredentials::new(address, "AKIATEST", "secret123").unwrap();

        let get = get_capability("index", b"test-key");
        let descriptor = creds.grant(&get).await.unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("X-Amz-Signature="));
        assert!(descriptor.url.as_str().contains("X-Amz-Credential="));
    }

    #[dialog_common::test]
    async fn it_signs_with_credentials_enum() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let creds = Credentials::public(address).unwrap();

        let get = get_capability("", b"key");
        let descriptor = creds.grant(&get).await.unwrap();

        assert_eq!(descriptor.method, "GET");
    }

    #[dialog_common::test]
    async fn it_includes_checksum_header() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let creds = Credentials::public(address).unwrap();

        let checksum = Checksum::Sha256([0u8; 32]);
        let set = set_capability("store", b"key", checksum);
        let descriptor = creds.grant(&set).await.unwrap();

        assert!(
            descriptor
                .headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }
}
