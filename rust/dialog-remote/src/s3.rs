use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sha2::{Digest, Sha256};
use std::fmt::Write as FmtWrite;
use std::time::SystemTime;
use url::Url;

use crate::DialogRemoteError;

// AWS S3 Signing Constants
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const SHA256_HEADER: &str = "X-Amz-Content-Sha256";
pub const ALGORITHM_QUERY_PARAM: &str = "X-Amz-Algorithm";
pub const CREDENTIAL_QUERY_PARAM: &str = "X-Amz-Credential";
pub const AMZ_DATE_QUERY_PARAM: &str = "X-Amz-Date";
pub const SIGNED_HEADERS_QUERY_PARAM: &str = "X-Amz-SignedHeaders";
pub const EXPIRES_QUERY_PARAM: &str = "X-Amz-Expires";
pub const HOST_HEADER: &str = "host";
pub const ALGORITHM_IDENTIFIER: &str = "AWS4-HMAC-SHA256";
pub const CHECKSUM_SHA256: &str = "x-amz-checksum-sha256";
pub const KEY_TYPE_IDENTIFIER: &str = "aws4_request";
pub const AMZ_SECURITY_TOKEN_QUERY_PARAM: &str = "X-Amz-Security-Token";
pub const AMZ_ACL_QUERY_PARAM: &str = "x-amz-acl";
pub const PUBLIC_READ: &str = "public-read";
pub const AMZ_SIGNATURE_QUERY_PARAM: &str = "X-Amz-Signature";

/// AWS S3 credentials
#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// Options for signing an S3/R2 request
#[derive(Debug, Clone)]
pub struct SignOptions {
    pub region: String,
    pub bucket: String,
    pub key: String,
    pub checksum: Option<String>,
    pub endpoint: Option<String>,
    pub expires: u64,
    pub method: String,
    pub public_read: bool,
    pub service: String,
    pub time: Option<DateTime<Utc>>,
}

impl Default for SignOptions {
    fn default() -> Self {
        Self {
            region: "auto".to_string(),
            bucket: String::new(),
            key: String::new(),
            checksum: None,
            endpoint: None,
            expires: 86400, // 24 hours
            method: "PUT".to_string(),
            public_read: false,
            service: "s3".to_string(),
            time: None,
        }
    }
}

/// Create authorization for S3/R2 storage
pub fn authorize(
    credentials: &Credentials,
    options: &SignOptions,
) -> Result<Authorization, DialogRemoteError> {
    Authorization::create(credentials, options)
}

/// Sign a URL for AWS S3 or compatible storage (like Cloudflare R2)
///
/// This function generates AWS SigV4 signed URLs that are compatible with S3 and R2 storage backends.
/// It implements the AWS SigV4 algorithm to produce signatures that are compatible with the
/// TypeScript implementation.
pub fn sign_url(
    credentials: &Credentials,
    options: &SignOptions,
) -> Result<Url, DialogRemoteError> {
    let auth = authorize(credentials, options)?;
    Ok(auth.url)
}

/// Authorization for S3/R2 storage access
#[derive(Debug)]
pub struct Authorization {
    /// S3 service name
    pub service: String,
    /// AWS credentials
    pub credentials: Credentials,
    /// HTTP method (PUT, GET, etc.)
    pub method: String,
    /// Host part of the URL
    pub host: String,
    /// Path part of the URL
    pub pathname: String,
    /// Original headers to include
    pub base_headers: HeaderMap,
    /// Timestamp in ISO 8601 format with special formatting
    pub timestamp: String,
    /// Date part of the timestamp
    pub date: String,
    /// AWS region
    pub region: String,
    /// S3/R2 bucket name
    pub bucket: String,
    /// Expiration time in seconds
    pub expires: u64,
    /// Credential scope
    pub scope: String,
    /// Content checksum
    pub checksum: Option<String>,
    /// Session token if using temporary credentials
    pub session_token: Option<String>,
    /// Whether to make the content publicly readable
    pub public_read: bool,
    /// Complete URL with signature
    pub url: Url,
    /// Signature of the request
    pub signature: String,
    /// Signing key
    pub signing_key: Vec<u8>,
    /// Prepared headers for signing
    pub signed_headers: HeaderMap,
    /// Search parameters for the URL
    pub search_params: Vec<(String, String)>,
}

impl Authorization {
    /// Create a new authorization
    pub fn create(
        credentials: &Credentials,
        options: &SignOptions,
    ) -> Result<Self, DialogRemoteError> {
        // Get current time or use the provided time option
        let datetime = match options.time {
            Some(time) => time,
            None => DateTime::<Utc>::from(SystemTime::now()),
        };

        // Format the timestamp
        let timestamp = format_timestamp(&datetime);
        let date = timestamp[0..8].to_string();

        // Get host from options
        let host = derive_host(options)?;

        // Create base URL
        let url_str = format!("https://{}/{}", host, options.key);
        let base_url = Url::parse(&url_str).map_err(|error| {
            DialogRemoteError::S3UrlSigner(format!("Could not derive a base URL: {}", error))
        })?;

        // Create headers
        let mut base_headers = HeaderMap::new();
        base_headers.insert(
            HeaderName::from_static(HOST_HEADER),
            HeaderValue::from_str(&host)
                .map_err(|error| DialogRemoteError::S3UrlSigner(format!("{error}")))?,
        );

        if let Some(checksum) = &options.checksum {
            base_headers.insert(
                HeaderName::from_static(CHECKSUM_SHA256),
                HeaderValue::from_str(checksum)
                    .map_err(|error| DialogRemoteError::S3UrlSigner(format!("{error}")))?,
            );
        }

        // Derive credential scope
        let scope = derive_scope(&date, &options.region, &options.service);

        // Create instance to compute all components
        let mut auth = Self {
            service: options.service.clone(),
            credentials: credentials.clone(),
            method: options.method.clone(),
            host: host.clone(),
            pathname: base_url.path().to_string(),
            base_headers,
            timestamp: timestamp.clone(),
            date: date.clone(),
            region: options.region.clone(),
            bucket: options.bucket.clone(),
            expires: options.expires,
            scope: scope.clone(),
            checksum: options.checksum.clone(),
            session_token: credentials.session_token.clone(),
            public_read: options.public_read,
            url: base_url,
            signature: String::new(),         // Will be computed later
            signing_key: Vec::new(),          // Will be computed later
            signed_headers: HeaderMap::new(), // Will be computed later
            search_params: Vec::new(),        // Will be computed later
        };

        // Compute the signed headers
        auth.signed_headers = derive_headers(&auth)?;

        // Compute search parameters
        auth.search_params = derive_search_params(&auth);

        // Compute signing key
        auth.signing_key = derive_signing_key(&auth);

        // Calculate signature
        let signing_payload = derive_signing_payload(&auth)?;
        auth.signature = hex_encode(&hmac_sign(&auth.signing_key, signing_payload.as_bytes()));

        // Build the final URL
        auth.url = build_url(&auth)?;

        Ok(auth)
    }

    /// Get the payload header string
    pub fn payload_header(&self) -> String {
        derive_payload_header(self)
    }

    /// Get the payload body string
    pub fn payload_body(&self) -> String {
        derive_payload_body(self).unwrap()
    }

    /// Get the complete signing payload string
    pub fn signing_payload(&self) -> String {
        derive_signing_payload(self).unwrap()
    }
}

/// Build the final URL with the signature
fn build_url(auth: &Authorization) -> Result<Url, DialogRemoteError> {
    let url_str = format!("https://{}{}", auth.host, auth.pathname);
    let mut url = Url::parse(&url_str).map_err(|error| {
        DialogRemoteError::S3UrlSigner(format!("Could not build final URL: {}", error))
    })?;

    // Add all search parameters
    for (name, value) in &auth.search_params {
        url.query_pairs_mut().append_pair(name, value);
    }

    // Add the signature
    url.query_pairs_mut()
        .append_pair(AMZ_SIGNATURE_QUERY_PARAM, &auth.signature);

    Ok(url)
}

/// Format timestamp in the format required by AWS
fn format_timestamp(time: &DateTime<Utc>) -> String {
    time.format("%Y%m%dT%H%M%SZ").to_string()
}

/// Create complete signing payload string
fn derive_signing_payload(auth: &Authorization) -> Result<String, DialogRemoteError> {
    let payload_header = derive_payload_header(auth);
    let payload_body = derive_payload_body(auth)?;

    // Hash the payload body
    let body_hash = sha256_hash(payload_body.as_bytes());
    let body_hash_hex = hex_encode(&body_hash);

    // Combine the header and body hash
    Ok(format!("{}\n{}", payload_header, body_hash_hex))
}

/// Create the payload header part
fn derive_payload_header(auth: &Authorization) -> String {
    format!(
        "{}\n{}\n{}",
        ALGORITHM_IDENTIFIER, auth.timestamp, auth.scope
    )
}

/// Create the payload body part
fn derive_payload_body(auth: &Authorization) -> Result<String, DialogRemoteError> {
    // Generate search params URL string
    let search_params_str = format_search_params(&auth.search_params);

    // Format the headers string
    let headers_str = format_headers(&auth.signed_headers);

    // Get signed headers string
    let signed_headers_str = format_signed_headers(&auth.signed_headers);

    // Format the pathname
    let path = format_path(&auth.pathname);

    // Build the complete body
    Ok(format!(
        "{}\n{}\n{}\n{}\n\n{}\n{}",
        auth.method, path, search_params_str, headers_str, signed_headers_str, UNSIGNED_PAYLOAD
    ))
}

/// Format URL path according to AWS requirements
fn format_path(pathname: &str) -> String {
    percent_encode(pathname).replace("%2F", "/")
}

/// Format headers according to AWS requirements
fn format_headers(headers: &HeaderMap) -> String {
    let mut header_map: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    // Process all headers and gather values
    for (name, value) in headers.iter() {
        let key = name.as_str().to_lowercase();
        let val = value.to_str().unwrap_or_default().trim().to_string();

        header_map.entry(key).or_default().push(val);
    }

    // Sort the keys
    let mut keys: Vec<String> = header_map.keys().cloned().collect();
    keys.sort();

    // Format lines
    let mut lines = Vec::new();
    for key in keys {
        let values = header_map.get(&key).unwrap();
        lines.push(format!("{}:{}", key, values.join(";")));
    }

    // Join with newlines
    lines.join("\n")
}

/// Format search parameters according to AWS requirements
fn format_search_params(params: &[(String, String)]) -> String {
    let mut seen_keys = std::collections::HashSet::new();
    let mut filtered_params = Vec::new();

    // Filter with the same logic as TypeScript
    for (key, value) in params {
        if key.is_empty() || seen_keys.contains(key) {
            continue;
        }

        seen_keys.insert(key.clone());
        filtered_params.push((key.clone(), value.clone()));
    }

    // Sort parameters
    filtered_params.sort_by(|(k1, v1), (k2, v2)| match k1.cmp(k2) {
        std::cmp::Ordering::Equal => v1.cmp(v2),
        other => other,
    });

    // Build query string with correct encoding
    let query_parts: Vec<String> = filtered_params
        .iter()
        .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
        .collect();

    query_parts.join("&")
}

/// Format signed headers string
fn format_signed_headers(headers: &HeaderMap) -> String {
    let mut keys: Vec<String> = headers.keys().map(|k| k.as_str().to_lowercase()).collect();
    keys.sort();
    keys.join(";")
}

/// Create signed headers
fn derive_headers(auth: &Authorization) -> Result<HeaderMap, DialogRemoteError> {
    let mut headers = HeaderMap::new();

    // Copy headers from the base
    for (name, value) in auth.base_headers.iter() {
        headers.insert(name.clone(), value.clone());
    }

    // Ensure host is set
    if !headers.contains_key(HOST_HEADER) {
        headers.insert(
            HeaderName::from_static(HOST_HEADER),
            HeaderValue::from_str(&auth.host).map_err(|error| {
                DialogRemoteError::S3UrlSigner(format!(
                    "Could not parse host header value: {}",
                    error
                ))
            })?,
        );
    }

    // Add checksum if present
    if let Some(checksum) = &auth.checksum {
        headers.insert(
            HeaderName::from_static(CHECKSUM_SHA256),
            HeaderValue::from_str(checksum).map_err(|error| {
                DialogRemoteError::S3UrlSigner(format!(
                    "Could not parse checksume header value: {}",
                    error
                ))
            })?,
        );
    }

    Ok(headers)
}

/// Create search parameters
fn derive_search_params(auth: &Authorization) -> Vec<(String, String)> {
    let mut params = Vec::new();

    // Add standard parameters
    params.push((
        ALGORITHM_QUERY_PARAM.to_string(),
        ALGORITHM_IDENTIFIER.to_string(),
    ));
    params.push((SHA256_HEADER.to_string(), UNSIGNED_PAYLOAD.to_string()));

    // Add credential
    params.push((
        CREDENTIAL_QUERY_PARAM.to_string(),
        format!("{}/{}", auth.credentials.access_key_id, auth.scope),
    ));

    // Add date and expiration
    params.push((AMZ_DATE_QUERY_PARAM.to_string(), auth.timestamp.clone()));
    params.push((EXPIRES_QUERY_PARAM.to_string(), auth.expires.to_string()));

    // Add session token if present
    if let Some(token) = &auth.session_token {
        params.push((AMZ_SECURITY_TOKEN_QUERY_PARAM.to_string(), token.clone()));
    }

    // Add ACL if public read
    if auth.public_read {
        params.push((AMZ_ACL_QUERY_PARAM.to_string(), PUBLIC_READ.to_string()));
    }

    // Add signed headers
    params.push((
        SIGNED_HEADERS_QUERY_PARAM.to_string(),
        format_signed_headers(&auth.signed_headers),
    ));

    params
}

/// Calculate the host part of the URL
fn derive_host(options: &SignOptions) -> Result<String, DialogRemoteError> {
    let host = if let Some(endpoint) = &options.endpoint {
        let endpoint_url = Url::parse(endpoint).map_err(|error| {
            DialogRemoteError::S3UrlSigner(format!("Could not parse endpoint as URL: {}", error))
        })?;
        let host = endpoint_url.host_str().ok_or_else(|| {
            DialogRemoteError::S3UrlSigner("Invalid endpoint (missing host)".into())
        })?;
        format!("{}.{}", options.bucket, host)
    } else {
        format!("{}.s3.{}.amazonaws.com", options.bucket, options.region)
    };

    Ok(host)
}

/// Calculate credential scope
fn derive_scope(date: &str, region: &str, service: &str) -> String {
    format!("{}/{}/{}/{}", date, region, service, KEY_TYPE_IDENTIFIER)
}

/// Calculate signing key
fn derive_signing_key(auth: &Authorization) -> Vec<u8> {
    let key = format!("AWS4{}", auth.credentials.secret_access_key);
    let k_date = hmac_sign(key.as_bytes(), auth.date.as_bytes());
    let k_region = hmac_sign(&k_date, auth.region.as_bytes());
    let k_service = hmac_sign(&k_region, auth.service.as_bytes());
    hmac_sign(&k_service, KEY_TYPE_IDENTIFIER.as_bytes())
}

// Helper function to calculate SHA256 hash
fn sha256_hash(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

// Helper function to perform HMAC-SHA256 signing
fn hmac_sign(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

// Helper function to encode bytes as lowercase hex - matching TypeScript's toHex
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(s, "{:02x}", byte).unwrap(); // lowercase hex for signatures per AWS requirements
    }
    s
}

// Helper function to percent-encode a string
// This exactly matches JavaScript's encodeURIComponent function
fn percent_encode(s: &str) -> String {
    let mut result = String::new();

    for byte in s.bytes() {
        match byte {
            // These characters don't get encoded in encodeURIComponent
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            // Everything else gets percent-encoded with uppercase hex to exactly match JavaScript
            _ => {
                result.push('%');
                write!(result, "{:02X}", byte).unwrap(); // uppercase to match JS encodeURIComponent
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use reqwest::header::HeaderMap;
    use std::collections::HashMap;

    /// Utility function to extract the signature from a URL for easier testing
    fn extract_signature_from_url(url: &str) -> String {
        let parsed_url = Url::parse(url).expect("Failed to parse URL");
        let query_params: HashMap<_, _> = parsed_url.query_pairs().collect();
        query_params
            .get("X-Amz-Signature")
            .expect("Signature not found in URL")
            .to_string()
    }

    #[test]
    fn it_produces_an_aws_s3_presigned_url() {
        let auth = authorize(
            &Credentials {
                access_key_id: "my-id".to_string(),
                secret_access_key: "top secret".to_string(),
                session_token: None,
            },
            &SignOptions {
                region: "auto".to_string(),
                bucket: "pale".to_string(),
                key: "file/path".to_string(),
                checksum: None,
                endpoint: None,
                expires: 86400,
                method: "PUT".to_string(),
                public_read: false,
                service: "s3".to_string(),
                time: Some(Utc.with_ymd_and_hms(2025, 5, 7, 5, 48, 59).unwrap()),
            },
        )
        .unwrap();

        // Check the final URL matches exactly
        assert_eq!(
            auth.url.as_str(),
            "https://pale.s3.auto.amazonaws.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=09cdc9df7d3590e098888b3663c7e417f6720543da1b35f57e15aed24d438bff"
        );

        // Check the signing key matches
        let expected_key: [u8; 32] = [
            79, 106, 222, 178, 108, 52, 104, 178, 205, 22, 58, 104, 193, 109, 221, 37, 179, 183,
            58, 87, 9, 22, 242, 56, 155, 133, 98, 156, 239, 136, 247, 8,
        ];
        assert_eq!(auth.signing_key, expected_key);

        // Check payload header
        assert_eq!(
            auth.payload_header(),
            "AWS4-HMAC-SHA256\n20250507T054859Z\n20250507/auto/s3/aws4_request"
        );

        // Check payload body
        assert_eq!(
            auth.payload_body(),
            "PUT\n/file/path\nX-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\nhost:pale.s3.auto.amazonaws.com\n\nhost\nUNSIGNED-PAYLOAD"
        );

        // Check signing payload
        assert_eq!(
            auth.signing_payload(),
            "AWS4-HMAC-SHA256\n20250507T054859Z\n20250507/auto/s3/aws4_request\n5c93f6200b90cd7dcb4e1e90256531a3f24ed6dc2c54e2837b0b9804456e7ca7"
        );

        // Check signature
        assert_eq!(
            extract_signature_from_url(auth.url.as_str()),
            "09cdc9df7d3590e098888b3663c7e417f6720543da1b35f57e15aed24d438bff"
        );
    }

    #[test]
    fn it_produces_a_cloudflare_r2_presigned_url() {
        let auth = authorize(
            &Credentials {
                access_key_id: "my-id".to_string(),
                secret_access_key: "top secret".to_string(),
                session_token: None,
            },
            &SignOptions {
                region: "auto".to_string(),
                bucket: "pale".to_string(),
                key: "file/path".to_string(),
                checksum: None,
                endpoint: Some(
                    "https://2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com".to_string(),
                ),
                expires: 86400,
                method: "PUT".to_string(),
                public_read: false,
                service: "s3".to_string(),
                time: Some(Utc.with_ymd_and_hms(2025, 5, 7, 5, 48, 59).unwrap()),
            },
        )
        .unwrap();

        // Check the final URL matches exactly
        assert_eq!(
            auth.url.as_str(),
            "https://pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=e0363a29790c09a3f0eb52fa12aa1c0dcf6166312c82473d9076178e330afaf9"
        );

        // Check the signing key matches
        let expected_key: [u8; 32] = [
            79, 106, 222, 178, 108, 52, 104, 178, 205, 22, 58, 104, 193, 109, 221, 37, 179, 183,
            58, 87, 9, 22, 242, 56, 155, 133, 98, 156, 239, 136, 247, 8,
        ];
        assert_eq!(auth.signing_key, expected_key);

        // Check payload header
        assert_eq!(
            auth.payload_header(),
            "AWS4-HMAC-SHA256\n20250507T054859Z\n20250507/auto/s3/aws4_request"
        );

        // Check payload body
        assert_eq!(
            auth.payload_body(),
            "PUT\n/file/path\nX-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\nhost:pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com\n\nhost\nUNSIGNED-PAYLOAD"
        );

        // Check signing payload
        assert_eq!(
            auth.signing_payload(),
            "AWS4-HMAC-SHA256\n20250507T054859Z\n20250507/auto/s3/aws4_request\nff151cc91640c650163866371ddca4fd268b05c9bb71e5703e7b4c9663696d41"
        );

        // Check signature
        assert_eq!(
            extract_signature_from_url(auth.url.as_str()),
            "e0363a29790c09a3f0eb52fa12aa1c0dcf6166312c82473d9076178e330afaf9"
        );
    }

    #[test]
    fn it_produces_an_aws_s3_presigned_url_with_checksum() {
        let auth = authorize(
            &Credentials {
                access_key_id: "my-id".to_string(),
                secret_access_key: "top secret".to_string(),
                session_token: None,
            },
            &SignOptions {
                region: "auto".to_string(),
                bucket: "pale".to_string(),
                key: "file/path".to_string(),
                checksum: Some("kgGGxxs9Hqpv0UdShU0CxA4hIU1zaNBpTFMfy4P2ZYs=".to_string()),
                endpoint: None,
                expires: 86400,
                method: "PUT".to_string(),
                public_read: false,
                service: "s3".to_string(),
                time: Some(Utc.with_ymd_and_hms(2025, 5, 7, 5, 48, 59).unwrap()),
            },
        )
        .unwrap();

        // Check the final URL matches exactly
        assert_eq!(
            auth.url.as_str(),
            "https://pale.s3.auto.amazonaws.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256&X-Amz-Signature=2932f4085c638682dbb368529ef59c9da3ecafb4f524533a5e07355a20038555"
        );

        // Check the signing key matches
        let expected_key: [u8; 32] = [
            79, 106, 222, 178, 108, 52, 104, 178, 205, 22, 58, 104, 193, 109, 221, 37, 179, 183,
            58, 87, 9, 22, 242, 56, 155, 133, 98, 156, 239, 136, 247, 8,
        ];
        assert_eq!(auth.signing_key, expected_key);

        // Check payload header
        assert_eq!(
            auth.payload_header(),
            "AWS4-HMAC-SHA256\n20250507T054859Z\n20250507/auto/s3/aws4_request"
        );

        // Check payload body
        assert_eq!(
            auth.payload_body(),
            "PUT\n/file/path\nX-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256\nhost:pale.s3.auto.amazonaws.com\nx-amz-checksum-sha256:kgGGxxs9Hqpv0UdShU0CxA4hIU1zaNBpTFMfy4P2ZYs=\n\nhost;x-amz-checksum-sha256\nUNSIGNED-PAYLOAD"
        );

        // Check signing payload
        assert_eq!(
            auth.signing_payload(),
            "AWS4-HMAC-SHA256\n20250507T054859Z\n20250507/auto/s3/aws4_request\n9fff0936a02aed12e49bd03e2bd7678c7be7b8252433848e5a3a76d887983e5f"
        );

        // Check signature
        assert_eq!(
            extract_signature_from_url(auth.url.as_str()),
            "2932f4085c638682dbb368529ef59c9da3ecafb4f524533a5e07355a20038555"
        );
    }

    #[test]
    fn it_produces_a_cloudflare_r2_presigned_url_with_checksum() {
        let auth = authorize(
            &Credentials {
                access_key_id: "my-id".to_string(),
                secret_access_key: "top secret".to_string(),
                session_token: None,
            },
            &SignOptions {
                region: "auto".to_string(),
                bucket: "pale".to_string(),
                key: "file/path".to_string(),
                checksum: Some("kgGGxxs9Hqpv0UdShU0CxA4hIU1zaNBpTFMfy4P2ZYs=".to_string()),
                endpoint: Some(
                    "https://2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com".to_string(),
                ),
                expires: 86400,
                method: "PUT".to_string(),
                public_read: false,
                service: "s3".to_string(),
                time: Some(Utc.with_ymd_and_hms(2025, 5, 7, 5, 48, 59).unwrap()),
            },
        )
        .unwrap();

        // Check the final URL matches exactly
        assert_eq!(
            auth.url.as_str(),
            "https://pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com/file/path?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256&X-Amz-Signature=8dc119745d387770784b234ad3a6f5e5afa13b04c9a99e777418bd3380c228cc"
        );

        // Check the signing key matches
        let expected_key: [u8; 32] = [
            79, 106, 222, 178, 108, 52, 104, 178, 205, 22, 58, 104, 193, 109, 221, 37, 179, 183,
            58, 87, 9, 22, 242, 56, 155, 133, 98, 156, 239, 136, 247, 8,
        ];
        assert_eq!(auth.signing_key, expected_key);

        // Check payload header
        assert_eq!(
            auth.payload_header(),
            "AWS4-HMAC-SHA256\n20250507T054859Z\n20250507/auto/s3/aws4_request"
        );

        // Check payload body
        assert_eq!(
            auth.payload_body(),
            "PUT\n/file/path\nX-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=my-id%2F20250507%2Fauto%2Fs3%2Faws4_request&X-Amz-Date=20250507T054859Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host%3Bx-amz-checksum-sha256\nhost:pale.2c5a882977b89ac2fc7ca2f958422366.r2.cloudflarestorage.com\nx-amz-checksum-sha256:kgGGxxs9Hqpv0UdShU0CxA4hIU1zaNBpTFMfy4P2ZYs=\n\nhost;x-amz-checksum-sha256\nUNSIGNED-PAYLOAD"
        );

        // Check signing payload
        assert_eq!(
            auth.signing_payload(),
            "AWS4-HMAC-SHA256\n20250507T054859Z\n20250507/auto/s3/aws4_request\nd1f95bef0508d5a1d77d74b88b6928990bdc43322b0ca015be521793a3edf2ba"
        );

        // Check signature
        assert_eq!(
            extract_signature_from_url(auth.url.as_str()),
            "8dc119745d387770784b234ad3a6f5e5afa13b04c9a99e777418bd3380c228cc"
        );
    }

    #[test]
    fn it_can_derive_a_scope_from_inputs() {
        let date = "20220101"; // The input date should be just the date part, not the full timestamp
        let region = "us-east-1";
        let service = "s3";

        let scope = derive_scope(date, region, service);
        assert_eq!(scope, "20220101/us-east-1/s3/aws4_request");
    }

    #[test]
    fn it_encodes_bytes_as_lowercase_hex() {
        let bytes = [0x01, 0x02, 0x03, 0x0A, 0x0F];
        assert_eq!(hex_encode(&bytes), "0102030a0f");
    }

    #[test]
    fn it_percent_encodes_strings() {
        assert_eq!(percent_encode("abc123"), "abc123");
        assert_eq!(percent_encode("a b+c"), "a%20b%2Bc");
        assert_eq!(percent_encode("test/path"), "test%2Fpath");
    }

    #[test]
    fn it_formats_timestamps() {
        let time = Utc.with_ymd_and_hms(2025, 5, 7, 5, 48, 59).unwrap();
        let timestamp = format_timestamp(&time);
        assert_eq!(timestamp, "20250507T054859Z");
    }

    #[test]
    fn it_formats_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("host", "example.com".parse().unwrap());
        headers.insert("x-amz-checksum-sha256", "checksum123".parse().unwrap());

        let formatted = format_headers(&headers);
        assert!(formatted.contains("host:example.com"));
        assert!(formatted.contains("x-amz-checksum-sha256:checksum123"));
    }
}
