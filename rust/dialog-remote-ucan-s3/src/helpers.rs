//! UCAN access service test infrastructure.
//!
//! Provides [`UcanS3Address`] and a local UCAN access server backed by
//! an in-memory S3 server for integration testing.

use serde::{Deserialize, Serialize};

#[cfg(not(target_arch = "wasm32"))]
mod server;
#[cfg(not(target_arch = "wasm32"))]
pub use server::*;

/// UCAN+S3 test server connection info.
///
/// Combines a UCAN access service endpoint with the backing S3 server details.
/// Passed to integration tests via `#[dialog_common::test]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UcanS3Address {
    /// URL of the UCAN access service.
    pub access_service_url: String,
    /// URL of the backing S3 server (for test verification).
    pub s3_endpoint: String,
    /// The bucket name.
    pub bucket: String,
    /// AWS access key ID (used by the access service).
    pub access_key_id: String,
    /// AWS secret access key (used by the access service).
    pub secret_access_key: String,
}
