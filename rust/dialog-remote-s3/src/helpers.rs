//! S3-compatible test server for integration testing.
//!
//! This module provides address types and a simple in-memory S3-compatible server
//! for testing S3 storage backend functionality.
//!
//! The module is split into:
//! - Address types (available on all platforms for deserialization in WASM tests)
//! - Server implementation (native-only, in the `server` submodule)
//! - Test operator types for capability-based testing
use dialog_capability::{Capability, Did, Principal, Provider};
use dialog_effects::authority;
use serde::{Deserialize, Serialize};

use crate::s3::{S3, S3Authorization, S3Credential};

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

/// A simple test session that implements [`Principal`] and credential effects.
///
/// This is useful for testing capability-based operations where an operator
/// is required but actual cryptographic signing is not needed (S3 uses its
/// own SigV4 signing).
///
/// # Example
///
/// ```no_run
/// use dialog_remote_s3::helpers::Session;
///
/// let session = Session::new("did:key:zTestIssuer".parse::<dialog_capability::Did>().unwrap());
/// ```
#[derive(Debug, Clone)]
pub struct Session {
    did: Did,
    credentials: Option<S3Credential>,
}

impl Session {
    /// Create a new test session with the given DID.
    pub fn new(did: impl Into<Did>) -> Self {
        Self {
            did: did.into(),
            credentials: None,
        }
    }

    /// Attach S3 credentials to this session.
    pub fn with_credentials(mut self, credentials: S3Credential) -> Self {
        self.credentials = Some(credentials);
        self
    }
}

impl Principal for Session {
    fn did(&self) -> Did {
        self.did.clone()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Identify> for Session {
    async fn execute(
        &self,
        _input: Capability<authority::Identify>,
    ) -> Result<Capability<authority::Operator>, authority::AuthorityError> {
        Err(authority::AuthorityError::Identity(
            "Session does not provide identity".into(),
        ))
    }
}

/// Helper macro to implement Provider<Fork<S3, Fx>> for Session by delegating to S3.
///
/// Session builds a `ForkInvocation` with default `S3Credential` and
/// delegates to the S3 `Provider<ForkInvocation<S3, Fx>>` impl.
macro_rules! impl_fork_provider {
    ($fx:ty, $output:ty) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<dialog_capability::fork::Fork<S3, $fx>> for Session {
            async fn execute(
                &self,
                fork: dialog_capability::fork::Fork<S3, $fx>,
            ) -> Result<$output, dialog_capability::access::AuthorizeError> {
                let (capability, address) = fork.into_parts();
                let invocation = dialog_capability::fork::ForkInvocation::new(
                    capability,
                    address,
                    S3Authorization(self.credentials.clone()),
                );
                let s3 = S3;
                Ok(invocation.perform(&s3).await)
            }
        }
    };
}

impl_fork_provider!(
    dialog_effects::memory::Resolve,
    Result<Option<dialog_effects::memory::Publication>, dialog_effects::memory::MemoryError>
);
impl_fork_provider!(
    dialog_effects::memory::Publish,
    Result<Vec<u8>, dialog_effects::memory::MemoryError>
);
impl_fork_provider!(
    dialog_effects::memory::Retract,
    Result<(), dialog_effects::memory::MemoryError>
);
impl_fork_provider!(
    dialog_effects::archive::Get,
    Result<Option<Vec<u8>>, dialog_effects::archive::ArchiveError>
);
impl_fork_provider!(
    dialog_effects::archive::Put,
    Result<(), dialog_effects::archive::ArchiveError>
);

#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub use server::*;
