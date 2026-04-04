//! S3 site type, credential types, and Provider implementations.
//!
//! This module provides [`S3`] and [`Bucket`], S3-compatible storage types
//! that execute pre-authorized HTTP requests via presigned URLs.
//!
//! Submodules:
//! - [`credentials`] — S3 credential types for direct AWS SigV4 signing
//! - [`provider`] — `Provider<Fork<S3, Fx>>` implementations for archive, memory, storage

pub(crate) mod credentials;
pub mod provider;

pub use crate::Address;
pub use credentials::S3Credentials;

use dialog_capability::Did;
use dialog_capability::site::Site;
use thiserror::Error;
use url::Url;

use crate::Permit;

/// Extension trait for RequestDescriptor to convert to reqwest RequestBuilder.
pub trait RequestDescriptorExt {
    /// Convert into a reqwest RequestBuilder with the client.
    fn into_request(self, client: &reqwest::Client) -> reqwest::RequestBuilder;
}

impl RequestDescriptorExt for Permit {
    fn into_request(self, client: &reqwest::Client) -> reqwest::RequestBuilder {
        let mut builder = match self.method.as_str() {
            "GET" => client.get(self.url),
            "PUT" => client.put(self.url),
            "DELETE" => client.delete(self.url),
            _ => client.request(
                reqwest::Method::from_bytes(self.method.as_bytes()).unwrap(),
                self.url,
            ),
        };

        for (key, value) in self.headers {
            builder = builder.header(key, value);
        }

        builder
    }
}

/// Errors that can occur when using the S3 storage backend.
#[derive(Error, Debug)]
pub enum S3StorageError {
    /// Failed to authorize the request (signing or credential issues).
    #[error("Authorization error: {0}")]
    AuthorizationError(String),

    /// Transport-level error (connection failed, timeout, network issues).
    #[error("Transport error: {0}")]
    TransportError(String),

    /// Service-level error (S3 returned an error response).
    #[error("Service error: {0}")]
    ServiceError(String),

    /// Error during serialization or deserialization of data.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// CAS edition mismatch (concurrent modification detected).
    #[error("Edition mismatch: expected {expected:?}, got {actual:?}")]
    EditionMismatch {
        /// The expected edition.
        expected: Option<String>,
        /// The actual edition found.
        actual: Option<String>,
    },
}

impl From<reqwest::Error> for S3StorageError {
    fn from(error: reqwest::Error) -> Self {
        S3StorageError::TransportError(error.to_string())
    }
}

impl From<crate::AccessError> for S3StorageError {
    fn from(error: crate::AccessError) -> Self {
        S3StorageError::AuthorizationError(error.to_string())
    }
}

/// S3 direct-access site.
///
/// Combines SigV4 credential signing with HTTP execution. Fork provider impls
/// authorize via `Address::authorize` then delegate to [`Http`] for the
/// actual HTTP round-trip.
///
/// S3 is both a [`Site`] and a [`Protocol`](dialog_capability::access::Protocol).
/// Authorization is handled via SigV4 presigned URLs, not capability-level
/// delegation chains.
#[derive(Debug, Clone, Copy, Default)]
pub struct S3;

impl Site for S3 {
    type Protocol = S3;
    type Address = crate::Address;
}

mod protocol {
    use super::S3;
    use dialog_capability::Did;
    use dialog_capability::access::{self, AuthorizeError};

    /// S3 access scope — just the subject DID.
    ///
    /// S3 authorization is handled by presigned URLs, not capability delegation,
    /// so the scope is minimal.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct S3Access {
        subject: Did,
    }

    impl S3Access {
        /// Create a new S3 access scope.
        pub fn new(subject: Did) -> Self {
            Self { subject }
        }
    }

    impl access::Scope for S3Access {
        fn subject(&self) -> &Did {
            &self.subject
        }
    }

    /// S3 has no delegation chain — presigned URLs are self-contained.
    #[derive(serde::Deserialize)]
    pub struct S3Proof;

    impl access::Delegation for S3Proof {
        type Access = S3Access;
        fn issuer(&self) -> &Did {
            unreachable!("S3 has no delegation chain")
        }
        fn audience(&self) -> &Did {
            unreachable!("S3 has no delegation chain")
        }
        fn subject(&self) -> Option<&Did> {
            None
        }
        fn verify(
            &self,
            _: &S3Access,
        ) -> Result<access::TimeRange, AuthorizeError> {
            Ok(access::TimeRange {
                not_before: None,
                expiration: None,
            })
        }
        fn encode(&self) -> Result<Vec<u8>, AuthorizeError> {
            Err(AuthorizeError::Denied(
                "S3 does not use stored proofs".into(),
            ))
        }
        fn decode(_bytes: &[u8]) -> Result<Self, AuthorizeError> {
            Err(AuthorizeError::Denied(
                "S3 does not use stored proofs".into(),
            ))
        }
    }

    /// S3 permit — trivially allowed (credentials are on the Address).
    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct S3Permit(S3Access);

    impl access::ProofChain<S3> for S3Permit {
        fn new(access: S3Access) -> Self {
            Self(access)
        }

        fn access(&self) -> &S3Access {
            &self.0
        }

        fn push(&mut self, _proof: S3Proof) {}

        fn proofs(&self) -> &[S3Proof] {
            &[]
        }

        fn claim(self, _signer: S3) -> Result<S3, AuthorizeError> {
            Ok(S3)
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl access::Authorization<S3> for S3 {
        async fn delegate(&self, _audience: Did) -> Result<(), AuthorizeError> {
            Err(AuthorizeError::Denied(
                "S3 does not support delegation".into(),
            ))
        }

        async fn invoke(&self) -> Result<(), AuthorizeError> {
            Ok(())
        }
    }

    impl access::Protocol for S3 {
        type Access = S3Access;
        type Signer = S3;
        type Proof = S3Proof;
        type Delegation = ();
        type Invocation = ();
        type ProofChain = S3Permit;
        type Authorization = S3;
    }
}

impl dialog_varsig::Principal for S3 {
    fn did(&self) -> Did {
        "did:web:s3".parse().expect("valid DID")
    }
}

/// A scoped S3 storage bucket with subject and namespace path.
///
/// This is a wrapper around [`S3`] that adds the subject DID and namespace path.
///
/// # Example
///
/// ```no_run
/// use dialog_remote_s3::s3::{S3, Bucket};
///
/// let subject: dialog_capability::Did = "did:key:zMySubject".parse().unwrap();
/// let bucket = Bucket::new(S3, subject, "my-store");
/// ```
#[derive(Debug, Clone)]
pub struct Bucket {
    bucket: S3,
    /// The subject DID (whose data we're accessing)
    subject: Did,
    /// The namespace path (store for StorageBackend, space for TransactionalMemoryBackend)
    path: String,
}

impl Bucket {
    /// Create a new scoped S3 bucket.
    ///
    /// - `bucket`: The underlying S3
    /// - `subject`: The subject DID (whose data we're accessing)
    /// - `path`: The namespace path (store for storage, space for memory)
    pub fn new(bucket: S3, subject: impl Into<Did>, path: impl Into<String>) -> Self {
        Self {
            bucket,
            subject: subject.into(),
            path: path.into(),
        }
    }

    /// Get the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Get the namespace path.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Create a new scoped bucket with a different path (nested namespace).
    pub fn at(&self, path: impl Into<String>) -> Self {
        Self {
            bucket: self.bucket,
            subject: self.subject.clone(),
            path: format!("{}/{}", self.path, path.into()),
        }
    }

    /// Get a reference to the underlying S3.
    pub fn s3(&self) -> &S3 {
        &self.bucket
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

/// Build an S3 URL for the given path.
///
/// Handles both path-style and virtual-hosted style URLs.
pub(crate) fn build_url(
    endpoint: &Url,
    bucket: &str,
    path: &str,
    path_style: bool,
) -> Result<Url, crate::AccessError> {
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
        let host = endpoint
            .host_str()
            .ok_or_else(|| crate::AccessError::Configuration("Invalid endpoint: no host".into()))?;
        let new_host = format!("{}.{}", bucket, host);

        let mut url = endpoint.clone();
        url.set_host(Some(&new_host))
            .map_err(|e| crate::AccessError::Configuration(format!("Invalid host: {}", e)))?;

        let new_path = if path.is_empty() { "/" } else { path };
        url.set_path(new_path);
        Ok(url)
    }
}

/// Extract host string from URL, including port for non-standard ports.
pub(crate) fn extract_host(url: &Url) -> Result<String, crate::AccessError> {
    let hostname = url
        .host_str()
        .ok_or_else(|| crate::AccessError::Configuration("URL missing host".into()))?;

    Ok(match url.port() {
        Some(port) => format!("{}:{}", hostname, port),
        None => hostname.to_string(),
    })
}

/// Default URL expiration: 1 hour.
pub const DEFAULT_EXPIRES: u64 = 3600;

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn test_address() -> Address {
        Address::new("https://s3.amazonaws.com", "us-east-1", "bucket")
    }

    #[dialog_common::test]
    fn it_creates_address() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );

        assert_eq!(address.endpoint(), "https://s3.us-east-1.amazonaws.com");
        assert_eq!(address.region(), "us-east-1");
        assert_eq!(address.bucket(), "my-bucket");
    }

    mod url_building_tests {
        use super::*;

        #[dialog_common::test]
        fn it_creates_address_for_virtual_hosted() {
            let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
            assert!(!address.path_style());
        }

        #[dialog_common::test]
        fn it_creates_path_style_for_localhost() {
            let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
            assert!(address.path_style());
        }

        #[dialog_common::test]
        fn it_allows_forcing_path_style() {
            let address = Address::new("https://custom-s3.example.com", "us-east-1", "bucket")
                .with_path_style();
            assert!(address.path_style());
        }

        #[dialog_common::test]
        fn it_creates_r2_address() {
            let address = Address::new("https://abc123.r2.cloudflarestorage.com", "auto", "bucket");
            assert!(!address.path_style());
        }
    }
}
