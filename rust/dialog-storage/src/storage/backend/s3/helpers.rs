//! Testing helpers for S3 integration tests.
//!
//! This module provides resources and servers for cross-platform S3 integration testing
//! using the `#[dialog_common::test]` macro with automatic service provisioning.
//!
//! # Architecture
//!
//! The module is split into two parts:
//!
//! - **Address types** (all platforms): Serializable address types that get passed to tests.
//!   These are compiled for both native and wasm targets so inner tests can deserialize them.
//!
//! - **Server implementation** (native only): The actual S3 test server that runs during tests.
//!   This is only available on native platforms via the `server` submodule.
//!
//! # Usage
//!
//! ## Authenticated S3 Tests (Default)
//!
//! Use [`S3Address`] for tests that require AWS credentials:
//!
//! ```rs
//! use dialog_storage::s3::{Address, Bucket, Credentials};
//! use dialog_storage::StorageBackend;
//!
//! #[dialog_common::test]
//! async fn it_stores_with_auth(env: S3Address) -> anyhow::Result<()> {
//!     let credentials = Credentials {
//!         access_key_id: env.access_key_id.clone(),
//!         secret_access_key: env.secret_access_key.clone(),
//!     };
//!     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
//!     let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?
//!         .with_path_style(true);
//!     backend.set(b"key".to_vec(), b"value".to_vec()).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Public S3 Tests (No Authentication)
//!
//! Use [`PublicS3Address`] for tests against publicly accessible buckets:
//!
//! ```rs
//! use dialog_storage::s3::{Address, Bucket};
//! use dialog_storage::StorageBackend;
//!
//! #[dialog_common::test]
//! async fn it_stores_publicly(env: PublicS3Address) -> anyhow::Result<()> {
//!     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
//!     let mut backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
//!         .with_path_style(true);
//!     backend.set(b"key".to_vec(), b"value".to_vec()).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Custom Settings
//!
//! Provider settings can be customized via macro attributes:
//!
//! ```rs
//! #[dialog_common::test(bucket = "custom-bucket")]
//! async fn it_uses_custom_bucket(env: S3Address) -> anyhow::Result<()> {
//!     assert_eq!(env.bucket, "custom-bucket");
//!     Ok(())
//! }
//! ```

use serde::{Deserialize, Serialize};

/// S3 test server connection info with credentials, passed to inner tests.
///
/// This is the primary address type for S3 integration tests. It includes
/// credentials for signing requests. Gets serialized and passed via environment
/// variable to wasm inner tests.
///
/// Use with `#[dialog_common::test]` by taking it as a parameter:
///
/// ```rs
/// use dialog_storage::s3::{Address, Bucket, Credentials};
///
/// #[dialog_common::test]
/// async fn my_test(env: S3Address) -> anyhow::Result<()> {
///     let credentials = Credentials {
///         access_key_id: env.access_key_id.clone(),
///         secret_access_key: env.secret_access_key.clone(),
///     };
///     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
///     let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, Some(credentials))?
///         .with_path_style(true);
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Address {
    /// The endpoint URL of the running S3 server (e.g., "http://127.0.0.1:9000")
    pub endpoint: String,
    /// The bucket name to use for testing
    pub bucket: String,
    /// AWS access key ID
    pub access_key_id: String,
    /// AWS secret access key
    pub secret_access_key: String,
}

/// Settings for configuring the S3 test server.
///
/// Pass these via macro attributes to customize the test environment:
///
/// ```rs
/// #[dialog_common::test(bucket = "my-bucket")]
/// async fn test_with_custom_bucket(env: S3Address) -> anyhow::Result<()> {
///     assert_eq!(env.bucket, "my-bucket");
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone)]
pub struct S3Settings {
    /// The bucket name to create. Defaults to "test-bucket".
    pub bucket: String,
    /// AWS access key ID. Defaults to "test-access-key".
    pub access_key_id: String,
    /// AWS secret access key. Defaults to "test-secret-key".
    pub secret_access_key: String,
}

impl Default for S3Settings {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
        }
    }
}

/// Public S3 test server connection info passed to inner tests.
///
/// This is for testing against publicly accessible S3 buckets that don't
/// require authentication. Use [`S3Address`] instead if you need credentials.
///
/// Use with `#[dialog_common::test]` by taking it as a parameter:
///
/// ```rs
/// use dialog_storage::s3::{Address, Bucket};
///
/// #[dialog_common::test]
/// async fn my_test(env: PublicS3Address) -> anyhow::Result<()> {
///     let address = Address::new(&env.endpoint, "us-east-1", &env.bucket);
///     let backend = Bucket::<Vec<u8>, Vec<u8>>::open(address, None)?
///         .with_path_style(true);
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicS3Address {
    /// The endpoint URL of the running S3 server (e.g., "http://127.0.0.1:9000")
    pub endpoint: String,
    /// The bucket name to use for testing
    pub bucket: String,
}

/// Settings for configuring the public S3 test server.
///
/// Pass these via macro attributes to customize the test environment:
///
/// ```rs
/// #[dialog_common::test(bucket = "my-bucket")]
/// async fn test_with_custom_bucket(env: PublicS3Address) -> anyhow::Result<()> {
///     assert_eq!(env.bucket, "my-bucket");
///     Ok(())
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub struct PublicS3Settings {
    /// The bucket name to create. Defaults to "test-bucket".
    pub bucket: String,
}

#[cfg(not(target_arch = "wasm32"))]
mod server;
