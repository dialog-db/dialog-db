//! S3-compatible test server for integration testing.
//!
//! This module provides address types and a simple in-memory S3-compatible server
//! for testing S3 storage backend functionality.
//!
//! The module is split into:
//! - Address types (available on all platforms for deserialization in WASM tests)
//! - Server implementation (native-only, in the `server` submodule)
//! - Test operator types for capability-based testing
use dialog_capability::{Capability, Did, Principal, Provider, authority};

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

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<authority::Sign> for Session {
    async fn execute(
        &self,
        _input: Capability<authority::Sign>,
    ) -> Result<Vec<u8>, authority::AuthorityError> {
        Err(authority::AuthorityError::SigningFailed(
            "Session does not provide signing".into(),
        ))
    }
}

use dialog_capability::storage;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<storage::Get> for Session {
    async fn execute(
        &self,
        _input: Capability<storage::Get>,
    ) -> Result<Option<Vec<u8>>, storage::StorageError> {
        Err(storage::StorageError::Storage(
            "Session does not provide storage".into(),
        ))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<storage::List> for Session {
    async fn execute(
        &self,
        _input: Capability<storage::List>,
    ) -> Result<storage::ListResult, storage::StorageError> {
        Err(storage::StorageError::Storage(
            "Session does not provide storage".into(),
        ))
    }
}

/// Helper macro to implement Provider<Fork<S3, Fx>> for Session by delegating to S3.
///
/// Session builds a `ForkInvocation` with `()` invocation (S3 protocol) and
/// delegates to the S3 `Provider<ForkInvocation<S3, Fx>>` impl.
macro_rules! impl_fork_provider {
    ($fx:ty, $output:ty) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<dialog_capability::fork::Fork<crate::s3::S3, $fx>> for Session {
            async fn execute(
                &self,
                fork: dialog_capability::fork::Fork<crate::s3::S3, $fx>,
            ) -> Result<$output, dialog_capability::access::AuthorizeError> {
                let (capability, address) = fork.into_parts();
                let invocation =
                    dialog_capability::fork::ForkInvocation::new(capability, address, ());
                let s3 = crate::s3::S3;
                Ok(<crate::s3::S3 as Provider<
                    dialog_capability::fork::ForkInvocation<crate::s3::S3, $fx>,
                >>::execute(&s3, invocation)
                .await)
            }
        }
    };
}

impl_fork_provider!(
    dialog_effects::storage::Get,
    Result<Option<Vec<u8>>, dialog_effects::storage::StorageError>
);
impl_fork_provider!(
    dialog_effects::storage::Set,
    Result<(), dialog_effects::storage::StorageError>
);
impl_fork_provider!(
    dialog_effects::storage::Delete,
    Result<(), dialog_effects::storage::StorageError>
);
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
