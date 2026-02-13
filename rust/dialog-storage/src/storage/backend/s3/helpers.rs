//! S3-compatible test server for integration testing.
//!
//! This module provides address types and a simple in-memory S3-compatible server
//! for testing S3 storage backend functionality.
//!
//! The module is split into:
//! - Address types (available on all platforms for deserialization in WASM tests)
//! - Server implementation (native-only, in the `server` submodule)
//! - UCAN access service (native-only, requires `ucan` feature)
//! - Test issuer types for capability-based testing
use dialog_capability::{Authority, Did, Principal, Signer};
use dialog_varsig::eddsa::Ed25519Signature;
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

/// UCAN access service test server connection info.
///
/// Contains all information needed to configure `ucan::Credentials` and
/// connect to the backing S3 server for test verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UcanS3Address {
    /// URL of the UCAN access service
    pub access_service_url: String,
    /// URL of the backing S3 server (for test verification)
    pub s3_endpoint: String,
    /// The bucket name
    pub bucket: String,
    /// AWS access key ID (used by access service, exposed for verification)
    pub access_key_id: String,
    /// AWS secret access key (used by access service, exposed for verification)
    pub secret_access_key: String,
}

/// A simple test session that implements [`Authority`] and [`Principal`].
///
/// This is useful for testing capability-based S3 operations where an issuer
/// is required but actual cryptographic signing is not needed (S3 uses its
/// own SigV4 signing).
///
/// # Example
///
/// ```no_run
/// use dialog_storage::s3::helpers::Session;
/// use dialog_storage::s3::{S3, S3Credentials, Address};
///
/// let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
/// let credentials = S3Credentials::public(address).unwrap();
/// let issuer = Session::new("did:key:zTestIssuer".parse::<dialog_capability::Did>().unwrap());
/// let bucket = S3::from_s3(credentials, issuer);
/// ```
#[derive(Debug, Clone)]
pub struct Session {
    did: Did,
}

impl Session {
    /// Create a new test session with the given DID.
    pub fn new(did: impl Into<Did>) -> Self {
        Self { did: did.into() }
    }
}

impl Principal for Session {
    fn did(&self) -> Did {
        self.did.clone()
    }
}

impl Signer<Ed25519Signature> for Session {
    async fn sign(&self, _payload: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        // S3 direct access uses SigV4 signing, not external signatures.
        // The signature is never verified — S3 uses its own SigV4 auth.
        Ok(Ed25519Signature::from_bytes([0u8; 64]))
    }
}

impl Authority for Session {
    type Signature = Ed25519Signature;
}

/// Re-export [`dialog_credentials::Ed25519Signer`] for UCAN-based S3 operations.
///
/// This signer implements [`Authority`], [`Principal`], and [`Signer`] and can be
/// used directly as the `Issuer` type parameter for [`super::S3`] and [`super::Bucket`].
#[cfg(feature = "ucan")]
pub use dialog_credentials::Ed25519Signer;

#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub use server::*;

#[cfg(all(not(target_arch = "wasm32"), feature = "ucan"))]
pub mod ucan_server;
#[cfg(all(not(target_arch = "wasm32"), feature = "ucan"))]
pub use ucan_server::{UcanAccessServer, UcanS3Settings};
