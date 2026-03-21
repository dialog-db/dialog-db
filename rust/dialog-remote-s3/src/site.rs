//! S3 site type and Provider implementations.
//!
//! This module provides [`S3`] and [`Bucket`], S3-compatible storage types
//! that execute pre-authorized HTTP requests via presigned URLs.

use dialog_capability::Did;
use dialog_capability::credential::Allow;
use dialog_capability::site::{Site, SiteAddress};
use thiserror::Error;

use crate::AuthorizedRequest;
use crate::s3::S3Credentials;

pub use crate::Address;

pub mod archive;
pub mod memory;
pub mod storage;

/// Extension trait for RequestDescriptor to convert to reqwest RequestBuilder.
pub trait RequestDescriptorExt {
    /// Convert into a reqwest RequestBuilder with the client.
    fn into_request(self, client: &reqwest::Client) -> reqwest::RequestBuilder;
}

impl RequestDescriptorExt for AuthorizedRequest {
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

/// S3 HTTP execution layer.
///
/// A stateless executor that presigns and dispatches `ForkInvocation<Fx, S3>` requests
/// via HTTP. The invocation carries the address, credentials, and capability --
/// `S3` just performs the HTTP round-trip.
#[derive(Debug, Clone, Copy, Default)]
pub struct S3;

/// S3 implements `Site` with `Option<S3Credentials>` for credentials.
///
/// Uses [`S3Address`] as the address type, which implements [`SiteAddress`]
/// for turbofish-free `.fork(&address)` calls.
impl Site for S3 {
    type Credentials = Option<S3Credentials>;
    type Format = Allow;
    type Address = S3Address;
}

/// An S3 address that implements [`SiteAddress`], enabling turbofish-free
/// `.fork(&address)` calls.
///
/// Wraps [`Address`] and dereferences to it, so all `Address` methods work directly.
///
/// # Example
///
/// ```no_run
/// # use dialog_remote_s3::site::{S3Address};
/// let address = S3Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "my-bucket");
/// // address.endpoint(), address.region(), etc. all work via Deref
/// ```
#[derive(Debug, Clone)]
pub struct S3Address(pub Address);

impl S3Address {
    /// Create a new S3 address.
    pub fn new(endpoint: &str, region: &str, bucket: &str) -> Self {
        Self(Address::new(endpoint, region, bucket))
    }

    /// Force path-style URLs.
    pub fn with_path_style(self) -> Self {
        Self(self.0.with_path_style())
    }

    /// Get the inner `Address`.
    pub fn into_inner(self) -> Address {
        self.0
    }
}

impl std::ops::Deref for S3Address {
    type Target = Address;
    fn deref(&self) -> &Address {
        &self.0
    }
}

impl From<Address> for S3Address {
    fn from(address: Address) -> Self {
        Self(address)
    }
}

impl From<S3Address> for Address {
    fn from(address: S3Address) -> Self {
        address.0
    }
}

impl SiteAddress for S3Address {
    type Site = S3;
}

impl dialog_capability::credential::Addressable<Option<S3Credentials>> for S3Address {
    fn credential_address(&self) -> dialog_capability::credential::Address<Option<S3Credentials>> {
        self.0.credential_address()
    }
}

/// A scoped S3 storage bucket with subject and namespace path.
///
/// This is a wrapper around [`S3`] that adds the subject DID and namespace path.
///
/// # Example
///
/// ```no_run
/// use dialog_remote_s3::site::{S3, Bucket};
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
