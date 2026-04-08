//! S3 credentials for direct access.
//!
//! [`S3Credentials`] carries only auth material (access key + secret).
//! Address info (endpoint, bucket, path_style) lives in [`Address`](crate::Address).
//!
//! - `None` → public/unsigned access
//! - `Some(S3Credentials)` → private access with SigV4 signing

use crate::capability::{Access, Precondition};
use crate::permit::Permit;
use crate::{AccessError, Address};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::hash::{Hash, Hasher};

/// S3 credentials for authenticated (SigV4 signed) access.
///
/// Use `Option<S3Credentials>`:
/// - `None` for public/unsigned access
/// - `Some(creds)` for private SigV4 signing
///
/// # Example
///
/// ```no_run
/// use dialog_remote_s3::s3::S3Credentials;
///
/// // Private credentials
/// let creds = S3Credentials::new("AKIAEXAMPLE", "secret-key");
///
/// // Use with Address::authorize()
/// // address.authorize(&request, Some(&creds)).await?;
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3Credentials {
    /// AWS Access Key ID
    access_key_id: String,
    /// AWS Secret Access Key
    secret_access_key: String,
}

impl Hash for S3Credentials {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.access_key_id.hash(state);
        self.secret_access_key.hash(state);
    }
}

impl S3Credentials {
    /// Create new credentials with the given access key and secret.
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
        }
    }

    /// Get the access key ID.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Get the secret access key.
    pub fn secret_access_key(&self) -> &str {
        &self.secret_access_key
    }

    /// Generates a signed URL using the given address for endpoint/region/bucket info.
    pub(crate) async fn grant<R: Access>(
        &self,
        request: &R,
        address: &Address,
    ) -> Result<Permit, AccessError> {
        let time = current_time();
        let timestamp = time.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[0..8];

        let region = address.region();
        let service = request.service();
        let expires = request.expires();

        // Derive signing key on demand
        let key = SigningKey::derive(&self.secret_access_key, date, region, service);
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        let path = request.path();
        let url = address.build_url(&path)?;

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
        if let Some(checksum) = request.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }
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
        url.set_query(None);
        {
            let mut query = url.query_pairs_mut();
            for (k, v) in &query_params {
                query.append_pair(k, v);
            }
            query.append_pair("X-Amz-Signature", &signature.to_string());
        }

        Ok(Permit {
            url,
            method: request.method().to_string(),
            headers,
        })
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
    use dialog_capability::{Subject, did};
    use dialog_effects::archive;

    fn test_subject() -> dialog_capability::Did {
        did!("key:zTestSubject")
    }

    fn test_address() -> Address {
        Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        )
    }

    fn localhost_address() -> Address {
        Address::new("http://localhost:9000", "us-east-1", "test-bucket")
    }

    #[dialog_common::test]
    async fn it_signs_with_public_access() {
        let address = test_address();

        let get = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new([0x42; 32]));
        let descriptor = address.authorize(&get).await.unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("my-bucket"));
        assert!(descriptor.url.as_str().contains("blobs/"));
    }

    #[dialog_common::test]
    async fn it_signs_with_private_credentials() {
        let address = test_address().with_credentials(S3Credentials::new("AKIATEST", "secret123"));

        let get = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new([0x42; 32]));
        let descriptor = address.authorize(&get).await.unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("X-Amz-Signature="));
        assert!(descriptor.url.as_str().contains("X-Amz-Credential="));
    }

    #[dialog_common::test]
    async fn it_includes_checksum_header() {
        let address = test_address();

        let checksum = Checksum::Sha256([0u8; 32]);
        let put = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("index"))
            .attenuate(archive::PutClaim {
                digest: [0x99; 32].into(),
                checksum,
            });
        let descriptor = address.authorize(&put).await.unwrap();

        assert!(
            descriptor
                .headers
                .iter()
                .any(|(k, _)| k == "x-amz-checksum-sha256")
        );
    }

    #[dialog_common::test]
    async fn it_uses_path_style_for_localhost() {
        let address = localhost_address();

        let get = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new([0x42; 32]));
        let descriptor = address.authorize(&get).await.unwrap();

        assert_eq!(descriptor.url.host_str().unwrap(), "localhost");
        assert!(descriptor.url.path().starts_with("/test-bucket/"));
    }
}
