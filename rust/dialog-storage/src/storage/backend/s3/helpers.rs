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
use dialog_common::{Authority, capability::{Did, Principal}};
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
/// ```ignore
/// use dialog_storage::s3::helpers::Session;
/// use dialog_storage::s3::{S3, S3Credentials, Address};
///
/// let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
/// let credentials = S3Credentials::public(address).unwrap();
/// let issuer = Session::new("did:key:zTestIssuer");
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
    fn did(&self) -> &Did {
        &self.did
    }
}

impl Authority for Session {
    fn sign(&mut self, _payload: &[u8]) -> Vec<u8> {
        // S3 direct access uses SigV4 signing, not external signatures
        Vec::new()
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        None
    }
}

/// An operator that wraps a signing key and provides [`Principal`] + [`Authority`].
///
/// This is useful for testing UCAN-based S3 operations where actual cryptographic
/// signing is required. The operator can sign payloads using its Ed25519 key.
///
/// # Example
///
/// ```ignore
/// use dialog_storage::s3::helpers::Operator;
/// use dialog_storage::s3::{S3, Credentials};
/// use dialog_s3_credentials::ucan::Credentials as UcanCredentials;
///
/// let operator = Operator::generate();
/// let ucan_credentials = UcanCredentials::new(access_service_url, delegation);
/// let bucket = S3::new(Credentials::Ucan(ucan_credentials), operator);
/// ```
#[cfg(feature = "ucan")]
#[derive(Clone)]
pub struct Operator {
    signer: ucan::did::Ed25519Signer,
    did: Did,
}

#[cfg(feature = "ucan")]
impl Operator {
    /// Create a new operator from an existing signer.
    pub fn new(signer: ucan::did::Ed25519Signer) -> Self {
        let did = signer.did().to_string();
        Self { signer, did }
    }

    /// Generate a new operator with a random signing key.
    pub fn generate() -> Self {
        use dialog_s3_credentials::ucan::test_helpers::generate_signer;
        Self::new(generate_signer())
    }

    /// Get the underlying signer.
    pub fn signer(&self) -> &ucan::did::Ed25519Signer {
        &self.signer
    }
}

#[cfg(feature = "ucan")]
impl Principal for Operator {
    fn did(&self) -> &Did {
        &self.did
    }
}

#[cfg(feature = "ucan")]
impl Authority for Operator {
    fn sign(&mut self, payload: &[u8]) -> Vec<u8> {
        use ed25519_dalek::Signer;
        self.signer.signer().sign(payload).to_vec()
    }

    fn secret_key_bytes(&self) -> Option<[u8; 32]> {
        Some(self.signer.signer().to_bytes())
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub use server::*;

#[cfg(all(not(target_arch = "wasm32"), feature = "ucan"))]
pub mod ucan_server;
#[cfg(all(not(target_arch = "wasm32"), feature = "ucan"))]
pub use ucan_server::{UcanAccessServer, UcanS3Settings};
