//! Test helpers for S3 integration testing.
//!
//! Provides [`S3Network`] for testing `Fork<S3, Fx>` capabilities
//! without a full Operator, and address types for passing server
//! configuration into tests.

use async_trait::async_trait;
use dialog_capability::access::AuthorizeError;
use dialog_capability::fork::{Fork, ForkInvocation};
use dialog_capability::{Constraint, Effect, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
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

/// Test environment for S3 fork execution.
///
/// Handles `Fork<S3, Fx>` for any effect by building a `ForkInvocation`
/// with the attached credentials and delegating to the S3 site provider.
///
/// # Example
///
/// ```no_run
/// use dialog_remote_s3::helpers::S3Network;
/// use dialog_remote_s3::S3Credential;
///
/// let env = S3Network::new();
/// let env = S3Network::from(S3Credential::new("AKIA...", "secret"));
/// ```
#[derive(Debug, Clone, Default)]
pub struct S3Network {
    /// Optional S3 credentials for authenticated access.
    credentials: Option<S3Credential>,
}

impl S3Network {
    /// Create a new test environment without credentials (public access).
    pub fn new() -> Self {
        Self::default()
    }
}

impl From<S3Credential> for S3Network {
    fn from(credentials: S3Credential) -> Self {
        Self {
            credentials: Some(credentials),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Fx> Provider<Fork<S3, Fx>> for S3Network
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Fx::Output: ConditionalSend,
    Fork<S3, Fx>: ConditionalSend,
    ForkInvocation<S3, Fx>: ConditionalSend,
    S3: Provider<ForkInvocation<S3, Fx>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, fork: Fork<S3, Fx>) -> Result<Fx::Output, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let invocation = ForkInvocation::new(
            capability,
            address,
            S3Authorization(self.credentials.clone()),
        );
        Ok(invocation.perform(&S3).await)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub mod server;
#[cfg(not(target_arch = "wasm32"))]
pub use server::*;
