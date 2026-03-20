//! This module provides [`S3`] and [`Bucket`], S3-compatible storage types
//! that execute pre-authorized HTTP requests via presigned URLs.
//!
//! # Features
//!
//! - AWS SigV4 presigned URL signing for authorization
//! - Support for public (unsigned) and authenticated access
//! - Automatic key encoding to handle binary and special characters
//! - Checksum verification using SHA-256
//! - Compatible with S3-compatible services (AWS S3, Cloudflare R2)
//!
//! # Examples
//!
//! ## Public Access (No Authentication)
//!
//! ```no_run
//! use dialog_storage::s3::{Address, S3, S3Credentials};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//! let credentials = S3Credentials::public(address)?;
//! let s3 = S3::from_s3(credentials);
//! # Ok(())
//! # }
//! ```
//!
//! ## Authorized Access (Credentials based Authentication)
//!
//! ```no_run
//! use dialog_storage::s3::{Address, S3Credentials, S3};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let address = Address::new(
//!     "https://s3.us-east-1.amazonaws.com",
//!     "us-east-1",
//!     "my-bucket",
//! );
//! let credentials = S3Credentials::private(
//!     address,
//!     std::env::var("AWS_ACCESS_KEY_ID")?,
//!     std::env::var("AWS_SECRET_ACCESS_KEY")?,
//! )?;
//! let s3 = S3::from_s3(credentials);
//! # Ok(())
//! # }
//! ```
//!
//! ## Cloudflare R2
//!
//! ```no_run
//! use dialog_storage::s3::{Address, S3Credentials, S3};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let address = Address::new(
//!     "https://account-id.r2.cloudflarestorage.com",
//!     "auto",
//!     "my-bucket",
//! );
//! let credentials = S3Credentials::private(
//!     address,
//!     std::env::var("R2_ACCESS_KEY_ID")?,
//!     std::env::var("R2_SECRET_ACCESS_KEY")?,
//! )?;
//! let s3 = S3::from_s3(credentials);
//! # Ok(())
//! # }
//! ```
//!
//! ## Local Development (MinIO)
//!
//! ```no_run
//! use dialog_storage::s3::{Address, S3Credentials, S3};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
//! let credentials = S3Credentials::private(address, "minioadmin", "minioadmin")?;
//! let s3 = S3::from_s3(credentials);
//! # Ok(())
//! # }
//! ```
//!
//! # Key Encoding
//!
//! Keys are automatically encoded to be S3-safe. Keys are treated as `/`-delimited
//! paths, and each segment is encoded independently:
//! - Segments containing only safe characters (`a-z`, `A-Z`, `0-9`, `-`, `_`, `.`) are kept as-is
//! - Segments containing unsafe characters or binary data are base58-encoded with a `!` prefix
//! - Path separators (`/`) preserve the S3 key hierarchy

use dialog_capability::Did;
use thiserror::Error;

// Re-export core types from dialog-s3-credentials crate
pub use dialog_s3_credentials::{
    AccessError, Address, AuthorizedRequest, Checksum, Credentials, Hasher,
};
// Re-export S3-specific credentials type for direct use
pub use dialog_s3_credentials::s3::Credentials as S3Credentials;
// Use access module types for direct S3 authorization
pub use dialog_s3_credentials::capability::{Precondition, S3Request};

use crate::DialogStorageError;

// Re-export capability type modules
pub mod archive;
pub mod memory;
pub mod storage;

mod key;
#[cfg(feature = "s3-list")]
#[allow(dead_code)]
mod list;

pub use key::{decode as decode_s3_key, encode as encode_s3_key};
#[cfg(feature = "s3-list")]
pub use list::ListResult;

// Testing helpers module
#[cfg(any(feature = "helpers", test))]
pub mod helpers;
#[cfg(any(feature = "helpers", test))]
pub use helpers::*;

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

impl From<S3StorageError> for DialogStorageError {
    fn from(error: S3StorageError) -> Self {
        DialogStorageError::StorageBackend(error.to_string())
    }
}

impl From<reqwest::Error> for S3StorageError {
    fn from(error: reqwest::Error) -> Self {
        S3StorageError::TransportError(error.to_string())
    }
}

impl From<AccessError> for S3StorageError {
    fn from(error: AccessError) -> Self {
        S3StorageError::AuthorizationError(error.to_string())
    }
}

/// S3-backed HTTP execution layer for authorized requests.
///
/// This type holds S3 credentials and can execute pre-authorized capabilities
/// by presigning HTTP requests. Authorization (identity verification, signing)
/// is handled externally by the caller before handing `Authorization<...>` values
/// to S3 for HTTP execution.
#[derive(Debug, Clone)]
pub struct S3 {
    credentials: Credentials,
}

impl S3 {
    /// Create a new S3 with the given credentials.
    pub fn new(credentials: Credentials) -> Self {
        Self { credentials }
    }

    /// Create a new S3 from S3 credentials.
    pub fn from_s3(credentials: dialog_s3_credentials::s3::Credentials) -> Self {
        Self {
            credentials: Credentials::S3(credentials),
        }
    }

    /// Get a reference to the credentials.
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }
}

// Provider<S3Invocation<Fx>> impls for application-level effects
// live in the submodules: archive.rs, memory.rs, storage.rs.
// Each impl executes the presigned HTTP request and interprets the response.

/// A scoped S3 storage bucket with subject and namespace path.
///
/// This is a wrapper around [`S3`] that adds the subject DID and namespace path.
///
/// # Example
///
/// ```no_run
/// use dialog_storage::s3::{S3, S3Credentials, Address, Bucket};
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let address = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
/// let credentials = S3Credentials::public(address)?;
/// let s3 = S3::from_s3(credentials);
/// let subject: dialog_capability::Did = "did:key:zMySubject".parse().unwrap();
/// let bucket = Bucket::new(s3, subject, "my-store");
/// # Ok(())
/// # }
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
            bucket: self.bucket.clone(),
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
    use dialog_s3_credentials::s3::Credentials as S3Credentials;

    #[allow(dead_code)]
    fn test_address() -> Address {
        Address::new("https://s3.amazonaws.com", "us-east-1", "bucket")
    }

    #[allow(dead_code)]
    fn test_credentials() -> S3Credentials {
        S3Credentials::public(test_address()).unwrap()
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

    #[dialog_common::test]
    fn it_converts_errors_to_dialog_error() {
        let error = S3StorageError::TransportError("test".into());
        let dialog_error: DialogStorageError = error.into();
        assert!(dialog_error.to_string().contains("test"));
    }

    /// Key encoding/decoding unit tests (no S3 server needed).
    mod key_encoding_tests {
        use super::*;

        #[dialog_common::test]
        fn it_roundtrips_key_encoding() {
            let test_keys: Vec<Vec<u8>> = vec![
                b"simple-key".to_vec(),
                b"path/to/key".to_vec(),
                b"key with spaces".to_vec(),
                b"key@with!special#chars".to_vec(),
                b"safe/unsafe@mixed/safe2".to_vec(),
            ];

            for key in test_keys {
                let encoded = encode_s3_key(&key);
                let decoded = decode_s3_key(&encoded).unwrap();
                assert_eq!(
                    decoded, key,
                    "Roundtrip failed for key: {:?}, encoded as: {}",
                    key, encoded
                );
            }
        }

        #[dialog_common::test]
        fn it_encodes_mixed_segments() {
            let key = b"safe-segment/user@example.com".to_vec();
            let encoded = encode_s3_key(&key);

            assert!(
                encoded.starts_with("safe-segment/!"),
                "First segment should be safe, second should be encoded with ! prefix: {}",
                encoded
            );
        }

        #[dialog_common::test]
        fn it_encodes_multi_segment_mixed() {
            let key = b"data/file name with spaces/v1/special!chars".to_vec();
            let encoded = encode_s3_key(&key);
            let segments: Vec<&str> = encoded.split('/').collect();

            assert_eq!(segments.len(), 4, "Should have 4 segments");
            assert_eq!(segments[0], "data", "First segment should be safe");
            assert!(
                segments[1].starts_with('!'),
                "Second segment should be encoded (has spaces)"
            );
            assert_eq!(segments[2], "v1", "Third segment should be safe");
            assert!(
                segments[3].starts_with('!'),
                "Fourth segment should be encoded (has !)"
            );
        }
    }

    /// URL building unit tests (no S3 server needed).
    mod url_building_tests {
        use super::*;

        #[dialog_common::test]
        fn it_creates_virtual_hosted_style_credentials() {
            let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
            let _credentials = S3Credentials::public(address).unwrap();
        }

        #[dialog_common::test]
        fn it_creates_path_style_credentials_for_localhost() {
            let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
            let _credentials = S3Credentials::public(address).unwrap();
        }

        #[dialog_common::test]
        fn it_allows_forcing_path_style() {
            let address = Address::new("https://custom-s3.example.com", "us-east-1", "bucket");
            let _credentials = S3Credentials::public(address).unwrap().with_path_style();
        }

        #[dialog_common::test]
        fn it_creates_r2_credentials() {
            let address = Address::new("https://abc123.r2.cloudflarestorage.com", "auto", "bucket");
            let _credentials = S3Credentials::public(address).unwrap();
        }

        #[dialog_common::test]
        fn it_creates_private_credentials() {
            let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
            let _credentials = S3Credentials::private(address, "access-key", "secret-key").unwrap();
        }
    }
}
