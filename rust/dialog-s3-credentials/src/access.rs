//! AWS S3 Signature Version 4 signing implementation.
//!
//! This module provides presigned URL generation for S3-compatible storage services
//! including AWS S3 and Cloudflare R2, using [query string authentication].
//!
//! [query string authentication]: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use thiserror::Error;
use url::Url;

use crate::{Address, Checksum};

#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;

#[cfg(target_arch = "wasm32")]
use web_time::{SystemTime, web::SystemTimeExt};

/// Default URL expiration: 1 hour.
pub const DEFAULT_EXPIRES: u64 = 3600;

/// AWS S3 credentials for signing requests.
///
/// This authorizer holds both the credentials and the S3 address, providing
/// a complete configuration for authenticated S3 access.
///
/// # Example
///
/// ```no_run
/// use dialog_s3_credentials::{Address, Credentials};
///
/// let address = Address::new(
///     "https://s3.us-east-1.amazonaws.com",
///     "us-east-1",
///     "my-bucket",
/// );
///
/// let credentials = Credentials::new(
///     address,
///     "AKIAIOSFODNN7EXAMPLE",
///     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
/// ).expect("valid endpoint URL");
/// ```
#[derive(Debug, Clone)]
pub struct Credentials {
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

impl Credentials {
    /// Create new credentials with the given address and keys.
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
            .map_err(|e| AuthorizationError::InvalidEndpoint(e.to_string()))?;
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
    ///
    /// - `true`: Use path-style URLs (`https://endpoint/bucket/key`)
    /// - `false`: Use virtual-hosted style URLs (`https://bucket.endpoint/key`)
    ///
    /// By default, path-style is enabled for IP addresses and localhost.
    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = path_style;
        self
    }

    /// Get the access key ID.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Authorize a request with AWS SigV4 presigned URL.
    ///
    /// Derives the signing key on demand using the request's time.
    /// The request provides all signing parameters (region, service, expires, time).
    fn sign<I: Invocation>(&self, request: &I) -> Result<Authorization, AuthorizationError> {
        let time = request.time();
        let timestamp = time.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[0..8];

        let region = request.region();
        let service = request.service();
        let expires = request.expires();

        // Derive signing key on demand
        let key = SigningKey::derive(&self.secret_access_key, date, region, service);
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        let url = request.url();
        let host = extract_host(url)?;

        // Build signed headers
        let mut headers = vec![("host".to_string(), host.clone())];
        // If request has a checksum, we add it to the headers so that S3 will
        // will perform integrity checks on the data.
        if let Some(checksum) = request.checksum() {
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
        if let Some(acl) = request.acl() {
            query_params.push(("x-amz-acl".into(), acl.as_str().to_string()));
        }

        query_params.push(("X-Amz-SignedHeaders".into(), signed_headers.clone()));

        // Include existing query parameters from the request URL
        // (e.g., list-type=2, prefix=... for ListObjectsV2)
        for (key, value) in request.url().query_pairs() {
            query_params.push((key.into_owned(), value.into_owned()));
        }

        // Sort all query parameters alphabetically (required by SigV4)
        query_params.sort_by(|a, b| a.0.cmp(&b.0));

        // Build canonical request
        let canonical_uri = percent_encode_path(request.url().path());

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
        let mut url = request.url().clone();
        url.set_query(None); // Clear existing query params (we'll add them all back)
        {
            let mut query = url.query_pairs_mut();
            for (k, v) in &query_params {
                query.append_pair(k, v);
            }
            query.append_pair("X-Amz-Signature", &signature.to_string());
        }

        Ok(Authorization { url, headers })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Authorizer for Credentials {
    async fn authorize(&self, request: &RequestInfo) -> Result<Authorization, AuthorizationError> {
        self.sign(request)
    }

    fn build_url(&self, path: &str) -> Result<Url, AuthorizationError> {
        build_s3_url(&self.endpoint, self.address.bucket(), path, self.path_style)
    }

    fn region(&self) -> &str {
        self.address.region()
    }

    fn path_style(&self) -> bool {
        self.path_style
    }
}

/// S3 Access Control List (ACL) settings.
///
/// These are canned ACLs supported by S3 and S3-compatible services.
/// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acl {
    /// Owner gets FULL_CONTROL. No one else has access rights.
    Private,
    /// Owner gets FULL_CONTROL. The AllUsers group gets READ access.
    PublicRead,
    /// Owner gets FULL_CONTROL. The AllUsers group gets READ and WRITE access.
    PublicReadWrite,
    /// Owner gets FULL_CONTROL. The AuthenticatedUsers group gets READ access.
    AuthenticatedRead,
    /// Object owner gets FULL_CONTROL. Bucket owner gets READ access.
    BucketOwnerRead,
    /// Both the object owner and the bucket owner get FULL_CONTROL.
    BucketOwnerFullControl,
}

impl Acl {
    /// Get the S3 ACL header value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public-read",
            Self::PublicReadWrite => "public-read-write",
            Self::AuthenticatedRead => "authenticated-read",
            Self::BucketOwnerRead => "bucket-owner-read",
            Self::BucketOwnerFullControl => "bucket-owner-full-control",
        }
    }
}

/// AWS SigV4 signing key derived from credentials.
///
/// The key is derived through an HMAC chain:
/// `HMAC(HMAC(HMAC(HMAC("AWS4" + secret, date), region), service), "aws4_request")`
#[derive(Debug, Clone)]
struct SigningKey(Vec<u8>);

impl SigningKey {
    /// Derive a signing key using the AWS4 key derivation algorithm.
    fn derive(secret: &str, date: &str, region: &str, service: &str) -> Self {
        let secret = format!("AWS4{}", secret);
        let k_date = Self::hmac(secret.as_bytes(), date.as_bytes());
        let k_region = Self::hmac(&k_date, region.as_bytes());
        let k_service = Self::hmac(&k_region, service.as_bytes());
        Self(Self::hmac(&k_service, b"aws4_request"))
    }

    /// Compute HMAC-SHA256.
    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        // HMAC-SHA256 accepts keys of any length - see detailed comment in original code
        let mut mac =
            Hmac::<Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts keys of any size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Sign data using this key.
    fn sign(&self, data: &[u8]) -> Signature {
        Signature(Self::hmac(&self.0, data))
    }
}

/// Authorizer for accessing public S3 buckets.
///
/// This authorizer holds the S3 address and provides unsigned access
/// to publicly readable buckets.
///
/// # Example
///
/// ```no_run
/// use dialog_s3_credentials::{Address, Public};
///
/// let address = Address::new(
///     "https://s3.us-east-1.amazonaws.com",
///     "us-east-1",
///     "public-bucket",
/// );
///
/// let public = Public::new(address).expect("valid endpoint URL");
/// ```
#[derive(Debug, Clone)]
pub struct Public {
    /// S3 address (endpoint, region, bucket)
    address: Address,
    /// Parsed endpoint URL
    endpoint: Url,
    /// Whether to use path-style URLs
    path_style: bool,
}

impl Public {
    /// Create a new public authorizer with the given address.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint URL in the address is invalid.
    pub fn new(address: Address) -> Result<Self, AuthorizationError> {
        let endpoint = Url::parse(address.endpoint())
            .map_err(|e| AuthorizationError::InvalidEndpoint(e.to_string()))?;
        let path_style = is_path_style_default(&endpoint);

        Ok(Self {
            address,
            endpoint,
            path_style,
        })
    }

    /// Set whether to use path-style URLs.
    ///
    /// - `true`: Use path-style URLs (`https://endpoint/bucket/key`)
    /// - `false`: Use virtual-hosted style URLs (`https://bucket.endpoint/key`)
    ///
    /// By default, path-style is enabled for IP addresses and localhost.
    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = path_style;
        self
    }

    /// Authorize a public request.
    ///
    /// Adds required headers (host, checksum) without signing.
    fn authorize_request<I: Invocation>(
        &self,
        request: &I,
    ) -> Result<Authorization, AuthorizationError> {
        let url = request.url();
        let host_str = url
            .host_str()
            .ok_or_else(|| AuthorizationError::InvalidEndpoint("URL missing host".into()))?;
        let host = if let Some(port) = url.port() {
            format!("{}:{}", host_str, port)
        } else {
            host_str.to_string()
        };

        let mut headers = vec![("host".to_string(), host)];
        if let Some(checksum) = request.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }

        Ok(Authorization {
            url: request.url().clone(),
            headers,
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Authorizer for Public {
    async fn authorize(&self, request: &RequestInfo) -> Result<Authorization, AuthorizationError> {
        self.authorize_request(request)
    }

    fn build_url(&self, path: &str) -> Result<Url, AuthorizationError> {
        build_s3_url(&self.endpoint, self.address.bucket(), path, self.path_style)
    }

    fn region(&self) -> &str {
        self.address.region()
    }

    fn path_style(&self) -> bool {
        self.path_style
    }
}

/// Request metadata required for S3 authorization.
///
/// This trait captures all information needed to sign an S3 request:
/// - HTTP method, URL, checksum, ACL (request-specific)
/// - Region, service, expires, time (signing parameters)
pub trait Invocation {
    /// The HTTP method for this request.
    fn method(&self) -> &'static str;

    /// The URL for this request.
    fn url(&self) -> &Url;

    /// The AWS region for signing (e.g., "us-east-1", "auto").
    fn region(&self) -> &str;

    /// The checksum of the body, if any.
    fn checksum(&self) -> Option<&Checksum> {
        None
    }

    /// The ACL for this request, if any.
    fn acl(&self) -> Option<Acl> {
        None
    }

    /// The service name for signing. Defaults to "s3".
    fn service(&self) -> &str {
        "s3"
    }

    /// URL signature expiration in seconds. Defaults to 1 hour.
    fn expires(&self) -> u64 {
        DEFAULT_EXPIRES
    }

    /// The timestamp for signing. Defaults to current time.
    fn time(&self) -> DateTime<Utc> {
        current_time()
    }
}

/// An authorization of the request
#[derive(Debug)]
pub struct Authorization {
    /// The presigned URL
    pub url: Url,
    /// Headers that must be included in the HTTP request
    pub headers: Vec<(String, String)>,
}

/// HMAC-SHA256 signature bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Signature(Vec<u8>);

impl std::fmt::Display for Signature {
    /// Displays hex encoded representation of the signature
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex_encode(&self.0))
    }
}

/// Errors that can occur during signing.
#[derive(Error, Debug)]
pub enum AuthorizationError {
    /// The endpoint URL is invalid (e.g., missing host).
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    /// Failed to parse a URL.
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),
    /// Error from access service.
    #[error("access service error: {0}")]
    AccessService(String),
}

/// Request metadata for authorization.
///
/// This struct captures all the information needed to authorize an S3 request.
/// It can be constructed from any type implementing [`Invocation`], or built directly.
///
/// `RequestInfo` itself implements `Invocation`, so it can be passed to
/// existing authorization methods.
#[derive(Debug, Clone)]
pub struct RequestInfo {
    /// HTTP method (GET, PUT, DELETE)
    pub method: &'static str,
    /// Target URL
    pub url: Url,
    /// AWS region for signing
    pub region: String,
    /// Content checksum for integrity verification
    pub checksum: Option<Checksum>,
    /// Access control list setting
    pub acl: Option<Acl>,
    /// URL signature expiration in seconds
    pub expires: u64,
    /// Timestamp for signing
    pub time: DateTime<Utc>,
    /// Service name (defaults to "s3")
    pub service: String,
}

impl RequestInfo {
    /// Create RequestInfo from any type implementing Invocation.
    pub fn from_invocation<I: Invocation>(inv: &I) -> Self {
        Self {
            method: inv.method(),
            url: inv.url().clone(),
            region: inv.region().to_string(),
            checksum: inv.checksum().cloned(),
            acl: inv.acl(),
            expires: inv.expires(),
            time: inv.time(),
            service: inv.service().to_string(),
        }
    }
}

impl Invocation for RequestInfo {
    fn method(&self) -> &'static str {
        self.method
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn region(&self) -> &str {
        &self.region
    }

    fn checksum(&self) -> Option<&Checksum> {
        self.checksum.as_ref()
    }

    fn acl(&self) -> Option<Acl> {
        self.acl
    }

    fn service(&self) -> &str {
        &self.service
    }

    fn expires(&self) -> u64 {
        self.expires
    }

    fn time(&self) -> DateTime<Utc> {
        self.time
    }
}

/// Async authorizer for S3 requests.
///
/// This trait abstracts over different authorization mechanisms:
/// - [`Credentials`] - AWS SigV4 local signing with address
/// - [`Public`] - No signing for public buckets, with address
/// - Custom implementations for access services, token providers, etc.
///
/// The authorizer is responsible for:
/// 1. Building URLs for S3 requests (via [`build_url`](Authorizer::build_url))
/// 2. Authorizing those requests (via [`authorize`](Authorizer::authorize))
///
/// # Example
///
/// ```ignore
/// use dialog_s3_credentials::{Authorizer, Credentials, Public, RequestInfo, Address};
///
/// // Both Credentials and Public implement Authorizer
/// async fn authorize_request<A: Authorizer>(
///     authorizer: &A,
///     request: &RequestInfo,
/// ) -> Result<Authorization, AuthorizationError> {
///     authorizer.authorize(request).await
/// }
/// ```
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Authorizer: Send + Sync + std::fmt::Debug {
    /// Authorize a request, returning the URL and headers to use.
    async fn authorize(&self, request: &RequestInfo) -> Result<Authorization, AuthorizationError>;

    /// Build a URL for the given key path.
    ///
    /// For S3-based authorizers, this constructs the bucket URL using the
    /// configured endpoint and bucket. For UCAN authorizers, this constructs
    /// the access service URL.
    fn build_url(&self, path: &str) -> Result<Url, AuthorizationError>;

    /// Get the region for signing requests.
    fn region(&self) -> &str;

    /// Whether to use path-style URLs.
    ///
    /// - `true`: Use path-style URLs (`https://endpoint/bucket/key`)
    /// - `false`: Use virtual-hosted style URLs (`https://bucket.endpoint/key`)
    fn path_style(&self) -> bool {
        false
    }
}

/// Build an S3 URL for the given path.
///
/// Handles both path-style and virtual-hosted style URLs.
fn build_s3_url(
    endpoint: &Url,
    bucket: &str,
    path: &str,
    path_style: bool,
) -> Result<Url, AuthorizationError> {
    if path_style {
        // Path-style: https://endpoint/bucket/path
        let mut url = endpoint.clone();
        let new_path = if path.is_empty() {
            format!("{}/", bucket)
        } else {
            format!("{}/{}", bucket, path)
        };
        url.set_path(&new_path);
        Ok(url)
    } else {
        // Virtual-hosted style: https://bucket.endpoint/path
        let host = endpoint.host_str().ok_or_else(|| {
            AuthorizationError::InvalidEndpoint("Invalid endpoint: no host".into())
        })?;
        let new_host = format!("{}.{}", bucket, host);

        let mut url = endpoint.clone();
        url.set_host(Some(&new_host))
            .map_err(|e| AuthorizationError::InvalidEndpoint(format!("Invalid host: {}", e)))?;

        let new_path = if path.is_empty() { "/" } else { path };
        url.set_path(new_path);
        Ok(url)
    }
}

/// Determine if path-style URLs should be used by default for this endpoint.
///
/// Returns true for IP addresses and localhost, since virtual-hosted style
/// URLs require DNS resolution of `{bucket}.{host}`.
pub fn is_path_style_default(endpoint: &Url) -> bool {
    use url::Host;
    match endpoint.host() {
        Some(Host::Ipv4(_)) | Some(Host::Ipv6(_)) => true,
        Some(Host::Domain(domain)) => domain == "localhost",
        None => false,
    }
}

/// Extract host string from URL, including port for non-standard ports.
fn extract_host(url: &Url) -> Result<String, AuthorizationError> {
    let hostname = url
        .host_str()
        .ok_or_else(|| AuthorizationError::InvalidEndpoint("URL missing host".into()))?;

    Ok(match url.port() {
        Some(port) => format!("{}:{}", hostname, port),
        None => hostname.to_string(),
    })
}

/// Get the current time as a UTC datetime.
///
/// Uses platform-appropriate time sources (std on native, web-time on wasm).
fn current_time() -> DateTime<Utc> {
    #[cfg(target_arch = "wasm32")]
    {
        DateTime::<Utc>::from(SystemTime::now().to_std())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        DateTime::<Utc>::from(SystemTime::now())
    }
}

/// Encode bytes as lowercase hexadecimal string.
///
/// Used for encoding SHA-256 hashes in AWS signature strings.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(s, "{:02x}", byte).unwrap();
    }
    s
}

/// Percent-encode a string according to RFC 3986.
///
/// Unreserved characters (A-Z, a-z, 0-9, `-`, `_`, `.`, `~`) are not encoded.
/// All other bytes are encoded as `%XX` where XX is the uppercase hex value.
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

/// Percent-encode a URL path, preserving forward slashes.
///
/// Like [`percent_encode`], but keeps `/` characters unencoded to preserve
/// the path hierarchy in S3 keys.
fn percent_encode_path(path: &str) -> String {
    percent_encode(path).replace("%2F", "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hasher;
    use chrono::TimeZone;

    fn test_address() -> Address {
        Address::new("https://s3.auto.amazonaws.com", "auto", "pale")
    }

    fn test_credentials() -> Credentials {
        Credentials::new(test_address(), "my-id", "top secret").unwrap()
    }

    fn test_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 5, 7, 5, 48, 59).unwrap()
    }

    const TEST_REGION: &str = "auto";

    fn s3_url(path: &str) -> Url {
        Url::parse(&format!("https://pale.s3.auto.amazonaws.com/{}", path)).unwrap()
    }

    fn r2_url(path: &str) -> Url {
        Url::parse(&format!(
            "https://pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com/{}",
            path
        ))
        .unwrap()
    }

    /// Simple test request that implements Invocation trait
    struct TestPutRequest {
        url: Url,
        checksum: Checksum,
        acl: Option<Acl>,
        region: String,
        time: Option<DateTime<Utc>>,
        expires: Option<u64>,
    }

    impl TestPutRequest {
        fn new(url: Url, body: &[u8], region: &str) -> Self {
            Self {
                url,
                checksum: Hasher::Sha256.checksum(body),
                acl: None,
                region: region.to_string(),
                time: None,
                expires: None,
            }
        }

        fn with_acl(mut self, acl: Acl) -> Self {
            self.acl = Some(acl);
            self
        }

        fn with_time(mut self, time: DateTime<Utc>) -> Self {
            self.time = Some(time);
            self
        }

        fn with_expires(mut self, expires: u64) -> Self {
            self.expires = Some(expires);
            self
        }
    }

    impl Invocation for TestPutRequest {
        fn method(&self) -> &'static str {
            "PUT"
        }

        fn url(&self) -> &Url {
            &self.url
        }

        fn region(&self) -> &str {
            &self.region
        }

        fn checksum(&self) -> Option<&Checksum> {
            Some(&self.checksum)
        }

        fn acl(&self) -> Option<Acl> {
            self.acl
        }

        fn time(&self) -> DateTime<Utc> {
            self.time.unwrap_or_else(current_time)
        }

        fn expires(&self) -> u64 {
            self.expires.unwrap_or(DEFAULT_EXPIRES)
        }
    }

    struct TestGetRequest {
        url: Url,
        region: String,
        time: Option<DateTime<Utc>>,
    }

    impl TestGetRequest {
        fn new(url: Url, region: &str) -> Self {
            Self {
                url,
                region: region.to_string(),
                time: None,
            }
        }

        fn with_time(mut self, time: DateTime<Utc>) -> Self {
            self.time = Some(time);
            self
        }
    }

    impl Invocation for TestGetRequest {
        fn method(&self) -> &'static str {
            "GET"
        }

        fn url(&self) -> &Url {
            &self.url
        }

        fn region(&self) -> &str {
            &self.region
        }

        fn time(&self) -> DateTime<Utc> {
            self.time.unwrap_or_else(current_time)
        }
    }

    struct TestDeleteRequest {
        url: Url,
        region: String,
        time: Option<DateTime<Utc>>,
    }

    impl TestDeleteRequest {
        fn new(url: Url, region: &str) -> Self {
            Self {
                url,
                region: region.to_string(),
                time: None,
            }
        }

        fn with_time(mut self, time: DateTime<Utc>) -> Self {
            self.time = Some(time);
            self
        }
    }

    impl Invocation for TestDeleteRequest {
        fn method(&self) -> &'static str {
            "DELETE"
        }

        fn url(&self) -> &Url {
            &self.url
        }

        fn region(&self) -> &str {
            &self.region
        }

        fn time(&self) -> DateTime<Utc> {
            self.time.unwrap_or_else(current_time)
        }
    }

    #[test]
    fn it_creates_credentials() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let creds = Credentials::new(address, "access-key", "secret-key").unwrap();
        assert_eq!(creds.access_key_id(), "access-key");
        assert_eq!(creds.region(), "us-east-1");
    }

    #[test]
    fn it_creates_public() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );
        let public = Public::new(address).unwrap();
        assert_eq!(public.region(), "us-east-1");
    }

    #[test]
    fn it_builds_virtual_hosted_url() {
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "bucket");
        let creds = Credentials::new(address, "id", "secret").unwrap();
        let url = creds.build_url("my-key").unwrap();
        assert_eq!(url.as_str(), "https://bucket.s3.amazonaws.com/my-key");
    }

    #[test]
    fn it_builds_path_style_url() {
        let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
        let creds = Credentials::new(address, "id", "secret").unwrap();
        // localhost defaults to path-style
        let url = creds.build_url("my-key").unwrap();
        assert_eq!(url.as_str(), "http://localhost:9000/bucket/my-key");
    }

    #[test]
    fn it_forces_path_style() {
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "bucket");
        let creds = Credentials::new(address, "id", "secret")
            .unwrap()
            .with_path_style(true);
        let url = creds.build_url("my-key").unwrap();
        assert_eq!(url.as_str(), "https://s3.amazonaws.com/bucket/my-key");
    }

    #[test]
    fn it_authorizes_s3_put_request() {
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("file/path"), b"test body", TEST_REGION)
            .with_time(test_time());
        let auth = credentials.sign(&request).unwrap();

        // URL should contain signing parameters
        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(auth.url.as_str().contains("X-Amz-Signature="));
    }

    #[test]
    fn it_authorizes_r2_put_request() {
        let address = Address::new(
            "https://2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com",
            "auto",
            "pale",
        );
        let credentials = Credentials::new(address, "my-id", "top secret").unwrap();
        let request = TestPutRequest::new(r2_url("file/path"), b"test body", TEST_REGION)
            .with_time(test_time());
        let auth = credentials.sign(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(auth.url.as_str().contains("X-Amz-Signature="));
    }

    #[test]
    fn it_authorizes_get_request() {
        let credentials = test_credentials();
        let request = TestGetRequest::new(s3_url("file/path"), TEST_REGION).with_time(test_time());
        let auth = credentials.sign(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
    }

    #[test]
    fn it_authorizes_delete_request() {
        let credentials = test_credentials();
        let request =
            TestDeleteRequest::new(s3_url("file/path"), TEST_REGION).with_time(test_time());
        let auth = credentials.sign(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
    }

    #[test]
    fn it_includes_checksum_header_in_put_request() {
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("file/path"), b"test body", TEST_REGION)
            .with_time(test_time());
        let auth = credentials.sign(&request).unwrap();

        // Should have checksum header
        assert!(
            auth.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
        assert!(auth.url.as_str().contains("x-amz-checksum-sha256"));
    }

    #[test]
    fn it_includes_acl_in_put_request() {
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("file/path"), b"test body", TEST_REGION)
            .with_acl(Acl::PublicRead)
            .with_time(test_time());
        let auth = credentials.sign(&request).unwrap();

        // Should have ACL in query params
        assert!(auth.url.as_str().contains("x-amz-acl=public-read"));
    }

    #[test]
    fn it_hex_encodes_bytes() {
        assert_eq!(hex_encode(&[0x01, 0x02, 0x03, 0x0A, 0x0F]), "0102030a0f");
    }

    #[test]
    fn it_percent_encodes_strings() {
        assert_eq!(percent_encode("abc123"), "abc123");
        assert_eq!(percent_encode("a b+c"), "a%20b%2Bc");
        assert_eq!(percent_encode("test/path"), "test%2Fpath");
    }

    #[test]
    fn it_includes_host_and_checksum_headers() {
        let credentials = test_credentials();
        let request =
            TestPutRequest::new(s3_url("file/path"), b"test", TEST_REGION).with_time(test_time());
        let auth = credentials.sign(&request).unwrap();

        assert!(auth.headers.iter().any(|(k, _)| k == "host"));
        assert!(
            auth.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    /// Test that current_time() returns a reasonable value on all platforms.
    #[test]
    fn it_gets_reasonable_current_time() {
        let now = current_time();
        let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();

        // Verify the time is reasonable (year should be 2024-2030)
        let year = &timestamp[0..4];
        let year_num: u32 = year.parse().unwrap();
        assert!(
            year_num >= 2024 && year_num <= 2030,
            "Year out of range: {}",
            timestamp
        );

        // Print for debugging
        #[cfg(not(target_arch = "wasm32"))]
        println!("Native current_time: {}", timestamp);

        // The signature is deterministic for a given time
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("test"), b"body", TEST_REGION).with_time(now);
        let auth = credentials.sign(&request).unwrap();

        // Just verify it produces a valid signature
        assert!(auth.url.to_string().contains("X-Amz-Signature="));
    }

    /// Uses fixed inputs to verify signature generation is identical across platforms.
    /// If the signatures differ, it indicates a platform-specific bug in the signing code.
    #[test]
    fn it_generates_identical_signatures_across_platforms() {
        // Use the same fixed inputs as other tests
        // Note: expires = 86400 (24 hours) to match the original test configuration
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("file/path"), b"test body", TEST_REGION)
            .with_time(test_time())
            .with_expires(86400);
        let auth = credentials.sign(&request).unwrap();

        // Extract the signature from the signed URL
        let signed_url = auth.url.to_string();
        let signature = signed_url
            .split("X-Amz-Signature=")
            .nth(1)
            .and_then(|s| s.split('&').next())
            .unwrap_or("");

        const EXPECTED_SIGNATURE: &str =
            "04b33a973b320c6aa27ab8e2f1821a563e80a032f6089b992070310de196bdff";

        assert_eq!(signature, EXPECTED_SIGNATURE);
    }

    #[test]
    fn it_detects_path_style_default() {
        let localhost = Url::parse("http://localhost:9000").unwrap();
        assert!(is_path_style_default(&localhost));

        let ipv4 = Url::parse("http://127.0.0.1:9000").unwrap();
        assert!(is_path_style_default(&ipv4));

        let remote = Url::parse("https://s3.amazonaws.com").unwrap();
        assert!(!is_path_style_default(&remote));
    }

    #[test]
    fn it_authorizes_public_request() {
        let address = Address::new("https://s3.amazonaws.com", "us-east-1", "bucket");
        let public = Public::new(address).unwrap();
        let url = Url::parse("https://bucket.s3.amazonaws.com/key").unwrap();
        let request = TestPutRequest::new(url, b"test", "us-east-1").with_time(test_time());

        let authorization = public.authorize_request(&request).unwrap();

        assert!(authorization.headers.iter().any(|(k, _)| k == "host"));
        assert!(
            authorization
                .headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }
}
