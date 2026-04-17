//! S3 credentials for direct access.
//!
//! [`S3Credential`] carries only auth material (access key + secret).
//! Address info (endpoint, bucket, path_style) lives in [`Address`](crate::s3::Address).
//!
//! - `None` -> public/unsigned access
//! - `Some(S3Credential)` -> private access with SigV4 signing

use super::Address;
use crate::Permit;
use crate::S3Error;
use crate::request::{Precondition, S3Request};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt::Write;
use std::hash::{Hash, Hasher};

/// S3 credentials for authenticated (SigV4 signed) access.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct S3Credential {
    /// AWS Access Key ID
    access_key_id: String,
    /// AWS Secret Access Key
    secret_access_key: String,
}

impl Hash for S3Credential {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.access_key_id.hash(state);
        self.secret_access_key.hash(state);
    }
}

impl S3Credential {
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

    /// Build an unsigned permit for the given captured request and address.
    ///
    /// Resolves the URL, adds query params, and builds headers
    /// (host, checksum, precondition).
    pub fn permit(request: &S3Request, address: &Address) -> Permit {
        let mut url = address.resolve(&request.path);

        if let Some(params) = &request.params {
            let mut query = url.query_pairs_mut();
            for (key, value) in params {
                query.append_pair(key, value);
            }
        }

        let mut headers = vec![("host".to_string(), address.authority().to_string())];
        if let Some(checksum) = &request.checksum {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }
        match &request.precondition {
            Precondition::IfMatch(etag) => {
                headers.push(("if-match".to_string(), format!("\"{}\"", etag)));
            }
            Precondition::IfNoneMatch => {
                headers.push(("if-none-match".to_string(), "*".to_string()));
            }
            Precondition::None => {}
        }

        Permit {
            url,
            method: request.method.to_string(),
            headers,
        }
    }

    /// Sign a permit with SigV4, returning a new permit with presigned URL.
    pub async fn authorize(
        &self,
        request: &S3Request,
        address: &Address,
    ) -> Result<Permit, S3Error> {
        let mut permit = Self::permit(request, address);

        let timestamp = request.time.format("%Y%m%dT%H%M%SZ").to_string();
        let date = &timestamp[0..8];

        let region = address.region();
        let service = request.service.as_str();
        let expires = request.expires;

        let key = SigningKey::derive(&self.secret_access_key, date, region, service);
        let scope = format!("{}/{}/{}/aws4_request", date, region, service);

        permit.headers.sort_by(|a, b| a.0.cmp(&b.0));

        let signed_headers: String = permit
            .headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");

        let mut query_params: Vec<(String, String)> = vec![
            ("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into()),
            ("X-Amz-Content-Sha256".into(), "UNSIGNED-PAYLOAD".into()),
            (
                "X-Amz-Credential".into(),
                format!("{}/{}", self.access_key_id, scope),
            ),
            ("X-Amz-Date".into(), timestamp.clone()),
            ("X-Amz-Expires".into(), expires.to_string()),
            ("X-Amz-SignedHeaders".into(), signed_headers.clone()),
        ];

        if let Some(params) = &request.params {
            for (key, value) in params {
                query_params.push((key.clone(), value.clone()));
            }
        }

        query_params.sort_by(|a, b| a.0.cmp(&b.0));

        let canonical_uri = percent_encode_path(permit.url.path());

        let canonical_query: String = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        let canonical_headers: String = permit
            .headers
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v.trim()))
            .collect::<Vec<_>>()
            .join("\n");

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n\n{}\nUNSIGNED-PAYLOAD",
            permit.method, canonical_uri, canonical_query, canonical_headers, signed_headers
        );

        let digest = Sha256::digest(canonical_request.as_bytes());
        let payload = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            timestamp,
            scope,
            hex_encode(&digest)
        );

        let signature = key.sign(payload.as_bytes());

        permit.url.set_query(None);
        {
            let mut query = permit.url.query_pairs_mut();
            for (k, v) in &query_params {
                query.append_pair(k, v);
            }
            query.append_pair("X-Amz-Signature", &signature.to_string());
        }

        Ok(permit)
    }
}

struct SigningKey(Hmac<Sha256>);

impl SigningKey {
    fn derive(secret_key: &str, date: &str, region: &str, service: &str) -> Self {
        let date_key = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes());
        let region_key = hmac_sha256(&date_key, region.as_bytes());
        let service_key = hmac_sha256(&region_key, service.as_bytes());
        let signing_key = hmac_sha256(&service_key, b"aws4_request");

        Self(Hmac::new_from_slice(&signing_key).expect("HMAC can take key of any size"))
    }

    fn sign(&self, message: &[u8]) -> Signature {
        let mut mac = self.0.clone();
        mac.update(message);
        Signature(mac.finalize().into_bytes().to_vec())
    }
}

struct Signature(Vec<u8>);

impl std::fmt::Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(result, "{:02x}", byte).unwrap();
    }
    result
}

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

fn percent_encode_path(path: &str) -> String {
    path.split('/')
        .map(percent_encode)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::super::S3Authorization;
    use super::*;
    use crate::request::S3Request;
    use dialog_capability::{Subject, did};
    use dialog_common::Checksum;
    use dialog_effects::archive;

    fn test_subject() -> dialog_capability::Did {
        did!("key:zTestSubject")
    }

    fn test_address() -> Address {
        Address::builder("https://s3.us-east-1.amazonaws.com")
            .region("us-east-1")
            .bucket("my-bucket")
            .build()
            .unwrap()
    }

    fn localhost_address() -> Address {
        Address::builder("http://localhost:9000")
            .region("us-east-1")
            .bucket("test-bucket")
            .build()
            .unwrap()
    }

    #[dialog_common::test]
    async fn it_signs_with_public_access() {
        let address = test_address();
        let get = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new([0x42; 32]));
        let auth = S3Authorization::public(S3Request::from(&get));
        let descriptor = auth.redeem(&address).await.unwrap();

        assert_eq!(descriptor.method, "GET");
        assert!(descriptor.url.as_str().contains("my-bucket"));
        assert!(descriptor.url.as_str().contains("blobs/"));
    }

    #[dialog_common::test]
    async fn it_signs_with_private_credentials() {
        let address = test_address();
        let get = Subject::from(test_subject())
            .attenuate(archive::Archive)
            .attenuate(archive::Catalog::new("blobs"))
            .invoke(archive::Get::new([0x42; 32]));
        let auth = S3Request::from(&get).attest(S3Credential::new("AKIATEST", "secret123"));
        let descriptor = auth.redeem(&address).await.unwrap();

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
        let auth = S3Authorization::public(S3Request::from(&put));
        let descriptor = auth.redeem(&address).await.unwrap();

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
        let auth = S3Authorization::public(S3Request::from(&get));
        let descriptor = auth.redeem(&address).await.unwrap();

        assert_eq!(descriptor.url.host_str().unwrap(), "localhost");
        assert!(descriptor.url.path().starts_with("/test-bucket/"));
    }
}
