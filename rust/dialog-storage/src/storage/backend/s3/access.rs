//! AWS S3 Signature Version 4 signing implementation.
//!
//! This module provides presigned URL generation for S3-compatible storage services
//! including AWS S3 and Cloudflare R2, using [query string authentication].
//!
//! [query string authentication]: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::fmt::Write as FmtWrite;
use thiserror::Error;
use url::Url;

use super::Checksum;

/// Default URL expiration: 1 hours.
pub const DEFAULT_EXPIRES: u64 = 3600;

/// AWS S3 credentials for signing requests.
#[derive(Debug, Clone)]
pub struct Credentials {
    /// AWS Access Key ID
    pub access_key_id: String,
    /// AWS Secret Access Key
    pub secret_access_key: String,
}

impl Credentials {
    /// Authorize a request with AWS SigV4 presigned URL.
    ///
    /// Derives the signing key on demand using the request's time.
    /// The request provides all signing parameters (region, service, expires, time).
    pub fn authorize<I: Invocation>(
        &self,
        request: &I,
    ) -> Result<Authorization, AuthorizationError> {
        let time = request.time();
        let timestamp = time.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[0..8];

        let region = request.region();
        let service = request.service();
        let expires = request.expires();

        // Derive signing key on demand
        let key = SigningKey::derive(&self.secret_access_key, date, region, service);
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        // Extract host from request URL, including port for non-standard ports.
        let url = request.url();
        // hostname does not include port, so we check if there is port in the
        // host and include it if present
        let hostname = url
            .host_str()
            .ok_or_else(|| AuthorizationError::InvalidEndpoint("URL missing host".into()))?;
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

/// AWS S3 credential used for accessing public buckets
#[derive(Debug, Clone)]
pub struct Public;

impl Public {
    /// Authorize a public request.
    ///
    /// Adds required headers (host, checksum) without signing.
    pub fn authorize<I: Invocation>(
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

/// Request metadata required for S3 authorization.
///
/// This trait captures all information needed to sign an S3 request:
/// - HTTP method, URL, checksum, ACL (request-specific)
/// - Region, service, expires, time (signing parameters)
///
/// The [`Request`](super::Request) trait extends this with the body and execution
/// capability.
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

    /// URL signature expiration in seconds. Defaults to 24 hours.
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
}

/// Get the current time as a UTC datetime.
///
/// Uses platform-appropriate time sources (std on native, web-time on wasm).
fn current_time() -> DateTime<Utc> {
    DateTime::<Utc>::from(dialog_common::time::now())
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
    use chrono::TimeZone;
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn test_credentials() -> Credentials {
        Credentials {
            access_key_id: "my-id".into(),
            secret_access_key: "top secret".into(),
        }
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
            use super::super::Hasher;
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

    #[dialog_common::test]
    fn it_authorizes_s3_put_request() {
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("file/path"), b"test body", TEST_REGION)
            .with_time(test_time());
        let auth = credentials.authorize(&request).unwrap();

        // URL should contain signing parameters
        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(auth.url.as_str().contains("X-Amz-Signature="));
    }

    #[dialog_common::test]
    fn it_authorizes_r2_put_request() {
        let credentials = test_credentials();
        let request = TestPutRequest::new(r2_url("file/path"), b"test body", TEST_REGION)
            .with_time(test_time());
        let auth = credentials.authorize(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
        assert!(auth.url.as_str().contains("X-Amz-Signature="));
    }

    #[dialog_common::test]
    fn it_authorizes_get_request() {
        let credentials = test_credentials();
        let request = TestGetRequest::new(s3_url("file/path"), TEST_REGION).with_time(test_time());
        let auth = credentials.authorize(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
    }

    #[dialog_common::test]
    fn it_authorizes_delete_request() {
        let credentials = test_credentials();
        let request =
            TestDeleteRequest::new(s3_url("file/path"), TEST_REGION).with_time(test_time());
        let auth = credentials.authorize(&request).unwrap();

        assert!(
            auth.url
                .as_str()
                .contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
        );
    }

    #[dialog_common::test]
    fn it_includes_checksum_header_in_put_request() {
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("file/path"), b"test body", TEST_REGION)
            .with_time(test_time());
        let auth = credentials.authorize(&request).unwrap();

        // Should have checksum header
        assert!(
            auth.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
        assert!(auth.url.as_str().contains("x-amz-checksum-sha256"));
    }

    #[dialog_common::test]
    fn it_includes_acl_in_put_request() {
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("file/path"), b"test body", TEST_REGION)
            .with_acl(Acl::PublicRead)
            .with_time(test_time());
        let auth = credentials.authorize(&request).unwrap();

        // Should have ACL in query params
        assert!(auth.url.as_str().contains("x-amz-acl=public-read"));
    }

    #[dialog_common::test]
    fn it_hex_encodes_bytes() {
        assert_eq!(hex_encode(&[0x01, 0x02, 0x03, 0x0A, 0x0F]), "0102030a0f");
    }

    #[dialog_common::test]
    fn it_percent_encodes_strings() {
        assert_eq!(percent_encode("abc123"), "abc123");
        assert_eq!(percent_encode("a b+c"), "a%20b%2Bc");
        assert_eq!(percent_encode("test/path"), "test%2Fpath");
    }

    #[dialog_common::test]
    fn it_includes_host_and_checksum_headers() {
        let credentials = test_credentials();
        let request =
            TestPutRequest::new(s3_url("file/path"), b"test", TEST_REGION).with_time(test_time());
        let auth = credentials.authorize(&request).unwrap();

        assert!(auth.headers.iter().any(|(k, _)| k == "host"));
        assert!(
            auth.headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    /// Test that current_time() returns a reasonable value on all platforms.
    #[dialog_common::test]
    async fn it_gets_reasonable_current_time() -> anyhow::Result<()> {
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
        let auth = credentials.authorize(&request).unwrap();

        // Just verify it produces a valid signature
        assert!(auth.url.to_string().contains("X-Amz-Signature="));

        Ok(())
    }

    /// Uses fixed inputs to verify signature generation is identical across platforms.
    /// If the signatures differ, it indicates a platform-specific bug in the signing code.
    #[dialog_common::test]
    async fn it_generates_identical_signatures_across_platforms() -> anyhow::Result<()> {
        // Use the same fixed inputs as other tests
        // Note: expires = 86400 (24 hours) to match the original test configuration
        let credentials = test_credentials();
        let request = TestPutRequest::new(s3_url("file/path"), b"test body", TEST_REGION)
            .with_time(test_time())
            .with_expires(86400);
        let auth = credentials.authorize(&request).unwrap();

        // Extract the signature from the signed URL
        let signed_url = auth.url.to_string();
        let signature = signed_url
            .split("X-Amz-Signature=")
            .nth(1)
            .and_then(|s| s.split('&').next())
            .unwrap_or("");

        const EXPECTED_SIGNATURE: &str =
            "04b33a973b320c6aa27ab8e2f1821a563e80a032f6089b992070310de196bdff";

        assert_eq!(signature, EXPECTED_SIGNATURE,);

        Ok(())
    }
}
