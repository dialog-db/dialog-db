//! S3-compatible test server for integration testing.
//!
//! This module provides address types and a simple in-memory S3-compatible server
//! for testing S3 storage backend functionality.
//!
//! The module is split into:
//! - Address types (available on all platforms for deserialization in WASM tests)
//! - Server implementation (native-only, in the `server` submodule)
//! - Test operator types for capability-based testing
use dialog_capability::credential::{Allow, Authorization};
use dialog_capability::{Capability, Did, Principal, Provider, credential};

use crate::s3::S3Credentials;

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
    s3_credentials: Option<S3Credentials>,
}

impl Session {
    /// Create a new test session with the given DID.
    pub fn new(did: impl Into<Did>) -> Self {
        Self {
            did: did.into(),
            s3_credentials: None,
        }
    }

    /// Attach S3 credentials to this session for site-based authorization.
    pub fn with_s3_credentials(mut self, credentials: S3Credentials) -> Self {
        self.s3_credentials = Some(credentials);
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
impl Provider<credential::Identify> for Session {
    async fn execute(
        &self,
        _input: Capability<credential::Identify>,
    ) -> Result<credential::Identity, credential::CredentialError> {
        Ok(credential::Identity {
            profile: self.did.clone(),
            operator: self.did.clone(),
            account: None,
        })
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

/// Session implements Provider<Authorize<C, Allow>> for simple authorization.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<C> Provider<credential::Authorize<C, Allow>> for Session
where
    C: dialog_capability::Effect + Clone + 'static,
    C::Of: dialog_capability::Constraint,
    Capability<C>: dialog_common::ConditionalSend,
    credential::Authorize<C, Allow>: dialog_common::ConditionalSend + 'static,
{
    async fn execute(
        &self,
        input: Capability<credential::Authorize<C, Allow>>,
    ) -> Result<Authorization<C, Allow>, credential::AuthorizeError> {
        let authorize = input.into_inner().constraint;
        Ok(Authorization::new(authorize.capability, ()))
    }
}

/// Session implements Provider<Retrieve<Option<S3Credentials>>> for credential lookup.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<credential::Retrieve<Option<S3Credentials>>> for Session {
    async fn execute(
        &self,
        _input: Capability<credential::Retrieve<Option<S3Credentials>>>,
    ) -> Result<Option<S3Credentials>, credential::CredentialError> {
        Ok(self.s3_credentials.clone())
    }
}

/// Helper macro to implement Provider<Fork<S3, Fx>> for Session by delegating to S3.
macro_rules! impl_fork_provider {
    ($fx:ty, $output:ty) => {
        #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
        impl Provider<dialog_capability::fork::Fork<crate::site::S3, $fx>> for Session {
            async fn execute(
                &self,
                invocation: dialog_capability::fork::ForkInvocation<crate::site::S3, $fx>,
            ) -> $output {
                let s3 = crate::site::S3;
                <crate::site::S3 as Provider<dialog_capability::fork::Fork<crate::site::S3, $fx>>>::execute(&s3, invocation).await
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
