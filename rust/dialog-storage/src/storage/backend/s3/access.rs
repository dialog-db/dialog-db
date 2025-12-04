//! AWS S3 Signature Version 4 signing implementation.
//!
//! This module provides presigned URL generation for S3-compatible storage services
//! including AWS S3 and Cloudflare R2.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fmt::Write as FmtWrite;
use url::Url;

use super::Checksum;

#[cfg(not(target_arch = "wasm32"))]
use std::time::SystemTime;

#[cfg(target_arch = "wasm32")]
use web_time::{SystemTime, web::SystemTimeExt};

// ============================================================================
// Public Types
// ============================================================================

/// AWS S3 credentials for signing requests.
#[derive(Debug, Clone)]
pub struct Credentials {
    /// AWS Access Key ID
    pub access_key_id: String,
    /// AWS Secret Access Key
    pub secret_access_key: String,
    /// Optional AWS session token (for temporary credentials)
    pub session_token: Option<String>,
}

/// Cloud storage service configuration for request signing.
///
/// Contains the signing parameters for different cloud storage services.
/// Does NOT include bucket, endpoint, or session duration - those are
/// specified when creating a session.
#[derive(Debug, Clone)]
pub enum Service {
    /// AWS S3 or S3-compatible service (including Cloudflare R2).
    S3 {
        /// AWS region (e.g., "us-east-1", "auto" for R2)
        region: String,
    },
}

impl Service {
    /// Create S3 service configuration.
    pub fn s3(region: impl Into<String>) -> Self {
        Self::S3 {
            region: region.into(),
        }
    }

    /// Get the region for this service.
    fn region(&self) -> &str {
        match self {
            Self::S3 { region } => region,
        }
    }

    /// Get the service name for signing.
    fn name(&self) -> &str {
        match self {
            Self::S3 { .. } => "s3",
        }
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

/// Pre-derived signing key for request signing.
#[derive(Debug, Clone)]
struct SessionKey(Vec<u8>);

impl SessionKey {
    fn new(credentials: &Credentials, service: &Service, date: &[u8]) -> Self {
        let secret = format!("AWS4{}", credentials.secret_access_key);
        // Derive signing key: HMAC chain of date -> region -> service -> "aws4_request"
        let k_date = Self::hmac(secret.as_bytes(), date);
        let k_region = Self::hmac(&k_date, service.region().as_bytes());
        let k_service = Self::hmac(&k_region, service.name().as_bytes());
        Self(Self::hmac(&k_service, b"aws4_request"))
    }

    fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key size");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    }

    /// Sign data using this key.
    fn sign(&self, data: &[u8]) -> Signature {
        Signature(Self::hmac(&self.0, data))
    }
}

/// An authorization session for S3 requests.
///
/// This enum supports both authorized (authenticated) and public (unsigned) access.
#[derive(Debug, Clone)]
pub enum Session {
    /// AWS SigV4 authorized requests with credentials.
    Authorized(Authority),
    /// Unsigned/public requests (no authentication).
    Public,
}

/// An authorized session that uses AWS SigV4 signing.
///
/// Contains the pre-derived signing key and can be reused
/// to sign multiple requests efficiently within its duration.
#[derive(Debug, Clone)]
pub struct Authority {
    access_key_id: String,
    session_token: Option<String>,
    scope: String,
    timestamp: String,
    key: SessionKey,
    duration: u64,
}

impl Session {
    /// Create a new authorized session from credentials, service, and duration.
    ///
    /// The signing key is derived for the current time.
    ///
    /// # Arguments
    /// * `credentials` - AWS credentials for signing
    /// * `service` - The cloud storage service configuration
    /// * `duration` - URL signature expiration in seconds (e.g., 86400 = 24 hours)
    pub fn new(credentials: &Credentials, service: &Service, duration: u64) -> Self {
        Self::new_at(credentials, service, duration, current_time())
    }

    /// Create a new authorized session with a specific timestamp (useful for testing).
    pub fn new_at(
        credentials: &Credentials,
        service: &Service,
        duration: u64,
        time: DateTime<Utc>,
    ) -> Self {
        let timestamp = time.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[0..8];

        Session::Authorized(Authority {
            access_key_id: credentials.access_key_id.clone(),
            session_token: credentials.session_token.clone(),
            scope: format!(
                "{}/{}/{}/aws4_request",
                date,
                service.region(),
                service.name()
            ),
            timestamp: timestamp.to_string(),
            key: SessionKey::new(credentials, service, date.as_bytes()),
            duration,
        })
    }

    /// Authorize a request for sending.
    ///
    /// For authorized sessions, returns a presigned URL with AWS SigV4.
    /// For public sessions, returns the original URL with headers.
    pub fn authorize<R: Invocation>(&self, request: &R) -> Result<Authorized, SigningError> {
        match self {
            Session::Authorized(session) => session.authorize(request),
            Session::Public => {
                // For public access, just pass through with minimal headers
                let host = request
                    .url()
                    .host_str()
                    .ok_or_else(|| SigningError::InvalidEndpoint("URL missing host".into()))?
                    .to_string();

                let mut headers = vec![("host".to_string(), host)];
                if let Some(checksum) = request.checksum() {
                    let header_name = format!("x-amz-checksum-{}", checksum.name());
                    headers.push((header_name, checksum.to_string()));
                }

                Ok(Authorized {
                    url: request.url().clone(),
                    headers,
                })
            }
        }
    }
}

impl Authority {
    /// Authorize a request with AWS SigV4 signing.
    fn authorize<R: Invocation>(&self, request: &R) -> Result<Authorized, SigningError> {
        // Extract host from request URL
        let host = request
            .url()
            .host_str()
            .ok_or_else(|| SigningError::InvalidEndpoint("URL missing host".into()))?
            .to_string();

        // Build signed headers
        let mut headers = vec![("host".to_string(), host.clone())];
        if let Some(checksum) = request.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }
        headers.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_headers_str: String = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");

        // Build query parameters (using String for both key and value to accommodate
        // both static signing params and dynamic URL params)
        let mut query_params: Vec<(String, String)> = vec![
            ("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()),
            ("X-Amz-Content-Sha256".into(), "UNSIGNED-PAYLOAD".into()),
            (
                "X-Amz-Credential".into(),
                format!("{}/{}", self.access_key_id, self.scope),
            ),
            ("X-Amz-Date".into(), self.timestamp.clone()),
            ("X-Amz-Expires".into(), self.duration.to_string()),
        ];

        if let Some(token) = &self.session_token {
            query_params.push(("X-Amz-Security-Token".into(), token.clone()));
        }

        // Include ACL if specified by the request
        if let Some(acl) = request.acl() {
            query_params.push(("x-amz-acl".into(), acl.as_str().to_string()));
        }

        query_params.push(("X-Amz-SignedHeaders".into(), signed_headers_str.clone()));

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
            signed_headers_str
        );

        // Create string to sign
        let digest = Sha256::digest(canonical_request.as_bytes());
        let payload = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            self.timestamp,
            self.scope,
            hex_encode(&digest)
        );

        // Compute signature
        let signature = self.key.sign(payload.as_bytes());

        // Build final URL with all query parameters (original + signing + signature)
        let mut url = request.url().clone();
        url.set_query(None); // Clear existing query params (we'll add them all back)
        {
            let mut query = url.query_pairs_mut();
            for (k, v) in &query_params {
                query.append_pair(k, v);
            }
            query.append_pair("X-Amz-Signature", &signature.to_string());
        }

        Ok(Authorized { url, headers })
    }
}

/// Trait for request invocations that can be authorized and sent.
///
/// Implementors describe the HTTP method, URL, and optional checksum/ACL
/// for S3 requests. The [`Request`](super::Request) trait extends this
/// with a body and the ability to perform the request.
pub trait Invocation {
    /// The HTTP method for this request.
    fn method(&self) -> &'static str;

    /// The URL for this request.
    fn url(&self) -> &Url;

    /// The checksum of the body, if any.
    fn checksum(&self) -> Option<&Checksum> {
        None
    }

    /// The ACL for this request, if any.
    fn acl(&self) -> Option<Acl> {
        None
    }
}

/// An authorized request ready to be sent.
#[derive(Debug)]
pub struct Authorized {
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
#[derive(Debug)]
pub enum SigningError {
    /// The endpoint URL is invalid (e.g., missing host).
    InvalidEndpoint(String),
    /// Failed to parse a URL.
    UrlParse(url::ParseError),
}

impl std::fmt::Display for SigningError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidEndpoint(msg) => write!(f, "invalid endpoint: {}", msg),
            Self::UrlParse(e) => write!(f, "URL parse error: {}", e),
        }
    }
}

impl std::error::Error for SigningError {}

impl From<url::ParseError> for SigningError {
    fn from(e: url::ParseError) -> Self {
        Self::UrlParse(e)
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn test_credentials() -> Credentials {
        Credentials {
            access_key_id: "my-id".into(),
            secret_access_key: "top secret".into(),
            session_token: None,
        }
    }

    fn test_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 5, 7, 5, 48, 59).unwrap()
    }

    fn test_service() -> Service {
        Service::s3("auto")
    }

    const TEST_DURATION: u64 = 86400;

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
    }

    impl TestPutRequest {
        fn new(url: Url, body: &[u8]) -> Self {
            use super::super::Hasher;
            Self {
                url,
                checksum: Hasher::Sha256.checksum(body),
                acl: None,
            }
        }

        fn with_acl(mut self, acl: Acl) -> Self {
            self.acl = Some(acl);
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

        fn checksum(&self) -> Option<&Checksum> {
            Some(&self.checksum)
        }

        fn acl(&self) -> Option<Acl> {
            self.acl
        }
    }

    struct TestGetRequest {
        url: Url,
    }

    impl TestGetRequest {
        fn new(url: Url) -> Self {
            Self { url }
        }
    }

    impl Invocation for TestGetRequest {
        fn method(&self) -> &'static str {
            "GET"
        }

        fn url(&self) -> &Url {
            &self.url
        }
    }

    struct TestDeleteRequest {
        url: Url,
    }

    impl TestDeleteRequest {
        fn new(url: Url) -> Self {
            Self { url }
        }
    }

    impl Invocation for TestDeleteRequest {
        fn method(&self) -> &'static str {
            "DELETE"
        }

        fn url(&self) -> &Url {
            &self.url
        }
    }

    #[test]
    fn test_s3_authorize() {
        let authority = Session::new_at(
            &test_credentials(),
            &test_service(),
            TEST_DURATION,
            test_time(),
        );
        let request = TestPutRequest::new(s3_url("file/path"), b"test body");
        let auth = authority.authorize(&request).unwrap();

        // URL should contain signing parameters
        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(auth.url.as_str().contains("X-Amz-Signature="));
    }

    #[test]
    fn test_r2_authorize() {
        let authority = Session::new_at(
            &test_credentials(),
            &test_service(),
            TEST_DURATION,
            test_time(),
        );
        let request = TestPutRequest::new(r2_url("file/path"), b"test body");
        let auth = authority.authorize(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(auth.url.as_str().contains("X-Amz-Signature="));
    }

    #[test]
    fn test_get_request() {
        let authority = Session::new_at(
            &test_credentials(),
            &test_service(),
            TEST_DURATION,
            test_time(),
        );
        let request = TestGetRequest::new(s3_url("file/path"));
        let auth = authority.authorize(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
    }

    #[test]
    fn test_delete_request() {
        let authority = Session::new_at(
            &test_credentials(),
            &test_service(),
            TEST_DURATION,
            test_time(),
        );
        let request = TestDeleteRequest::new(s3_url("file/path"));
        let auth = authority.authorize(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
    }

    #[test]
    fn test_put_with_checksum() {
        let authority = Session::new_at(
            &test_credentials(),
            &test_service(),
            TEST_DURATION,
            test_time(),
        );
        let request = TestPutRequest::new(s3_url("file/path"), b"test body");
        let auth = authority.authorize(&request).unwrap();

        // Should have checksum header
        assert!(
            auth.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
        assert!(auth.url.as_str().contains("x-amz-checksum-sha256"));
    }

    #[test]
    fn test_put_with_acl() {
        let authority = Session::new_at(
            &test_credentials(),
            &test_service(),
            TEST_DURATION,
            test_time(),
        );
        let request =
            TestPutRequest::new(s3_url("file/path"), b"test body").with_acl(Acl::PublicRead);
        let auth = authority.authorize(&request).unwrap();

        // Should have ACL in query params
        assert!(auth.url.as_str().contains("x-amz-acl=public-read"));
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x01, 0x02, 0x03, 0x0A, 0x0F]), "0102030a0f");
    }

    #[test]
    fn test_percent_encode() {
        assert_eq!(percent_encode("abc123"), "abc123");
        assert_eq!(percent_encode("a b+c"), "a%20b%2Bc");
        assert_eq!(percent_encode("test/path"), "test%2Fpath");
    }

    #[test]
    fn test_headers_with_checksum() {
        let authority = Session::new_at(
            &test_credentials(),
            &test_service(),
            TEST_DURATION,
            test_time(),
        );
        let request = TestPutRequest::new(s3_url("file/path"), b"test");
        let auth = authority.authorize(&request).unwrap();

        assert!(auth.headers.iter().any(|(k, _)| k == "host"));
        assert!(
            auth.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }
}
