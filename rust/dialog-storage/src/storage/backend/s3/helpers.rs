//! S3-compatible test server for integration testing.
//!
//! This module provides address types and a simple in-memory S3-compatible server
//! for testing S3 storage backend functionality.
//!
//! The module is split into:
//! - Address types (available on all platforms for deserialization in WASM tests)
//! - Server implementation (native-only, in the `server` submodule)
use serde::{Deserialize, Serialize};

/// S3 test server connection info with credentials, passed to inner tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Address {
    /// The endpoint URL of the running S3 server
    pub endpoint: String,
    /// The bucket name to use for testing
    pub bucket: String,
    /// AWS access key ID
    pub access_key_id: String,
    /// AWS secret access key
    pub secret_access_key: String,
}

/// Public S3 test server connection info (no authentication).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicS3Address {
    /// The endpoint URL of the running S3 server
    pub endpoint: String,
    /// The bucket name to use for testing
    pub bucket: String,
}

#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub use server::*;
