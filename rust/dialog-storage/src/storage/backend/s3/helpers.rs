//! S3-compatible test server for integration testing.
//!
//! This module provides address types and a simple in-memory S3-compatible server
//! for testing S3 storage backend functionality.
//!
//! The module is split into:
//! - Address types (available on all platforms for deserialization in WASM tests)
//! - Server implementation (native-only, in the `server` submodule)
//! - UCAN access service (native-only, requires `ucan` feature)
//! - Test operator types for capability-based testing
use dialog_capability::authorization::Authorized;
use dialog_capability::{Capability, Did, Effect, Principal, Provider, credential};
use dialog_s3_credentials::capability::S3Request;
use dialog_s3_credentials::s3::site::S3Access;
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

/// A simple test session that implements [`Principal`] and credential effects.
///
/// This is useful for testing capability-based operations where an operator
/// is required but actual cryptographic signing is not needed (S3 uses its
/// own SigV4 signing).
///
/// # Example
///
/// ```no_run
/// use dialog_storage::s3::helpers::Session;
///
/// let session = Session::new("did:key:zTestIssuer".parse::<dialog_capability::Did>().unwrap());
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Identify> for Session {
    async fn execute(
        &self,
        _input: Capability<credential::Identify>,
    ) -> Result<Did, credential::CredentialError> {
        Ok(self.did.clone())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Sign> for Session {
    async fn execute(
        &self,
        _input: Capability<credential::Sign>,
    ) -> Result<Vec<u8>, credential::CredentialError> {
        // S3 direct access uses SigV4 signing, not external signatures.
        // The signature is never verified -- S3 uses its own SigV4 auth.
        Ok(vec![0u8; 64])
    }
}

/// Session delegates S3 authorization to the credentials themselves.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<C> Provider<credential::Authorize<C, S3Access>> for Session
where
    C: Effect + Clone + 'static,
    Capability<C>: S3Request,
{
    async fn execute(
        &self,
        _input: credential::Authorize<C, S3Access>,
    ) -> Result<Authorized<C, S3Access>, credential::AuthorizeError> {
        Err(credential::AuthorizeError::Configuration(
            "Session does not hold S3 credentials directly; use S3Credentials as Provider instead"
                .to_string(),
        ))
    }
}

/// Re-export [`dialog_credentials::Ed25519Signer`] for UCAN-based operations.
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
