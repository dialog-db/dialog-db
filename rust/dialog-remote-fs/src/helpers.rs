//! Test helpers for FS-remote.
//!
//! Provides [`FsNetwork`] for exercising `Fork<Fs, Fx>` capabilities without a
//! full Operator: it carries an already-resolved
//! [`FileSystem`](dialog_storage::provider::FileSystem) and attests it directly,
//! bypassing the `Identify` + `Load<Grant>` credential resolution the real
//! [`Fs`](crate::Fs) fork performs.

use async_trait::async_trait;
use dialog_capability::{Capability, Constraint, Effect, Fork, ForkInvocation, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_storage::provider::FileSystem;

use crate::fs::{Fs, FsAuthorization};

/// Test environment for FS-remote fork execution.
///
/// Handles `Fork<Fs, Fx>` for any effect by attesting the carried provider as
/// the authorization and delegating to the [`Fs`] site provider.
#[derive(Clone)]
pub struct FsNetwork {
    filesystem: FileSystem,
}

impl FsNetwork {
    /// Create a test network rooted at the given provider.
    pub fn new(filesystem: FileSystem) -> Self {
        Self { filesystem }
    }
}

impl From<FileSystem> for FsNetwork {
    fn from(filesystem: FileSystem) -> Self {
        Self::new(filesystem)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Fx> Provider<Fork<Fs, Fx>> for FsNetwork
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    Fx::Output: ConditionalSend,
    Fork<Fs, Fx>: ConditionalSend,
    ForkInvocation<Fs, Fx>: ConditionalSend,
    Capability<Fx>: ConditionalSend + ConditionalSync,
    Fs: Provider<ForkInvocation<Fs, Fx>> + ConditionalSync,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, fork: Fork<Fs, Fx>) -> Fx::Output {
        let authorization = FsAuthorization::new(self.filesystem.clone());
        fork.attest(authorization).perform(&Fs).await
    }
}
