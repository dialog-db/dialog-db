//! Memory providers for FS-remote.
//!
//! Delegates the memory capability to the
//! [`FileSystem`](dialog_storage::provider::FileSystem) the authorization
//! resolved from the directory grant. CAS semantics, edition hashing,
//! cross-writer locking, and atomic writes all live in `dialog_storage`.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::memory::{Edition, MemoryError, Publish, Resolve, Retract, Version};

use crate::fs::Fs;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Resolve>> for Fs {
    async fn execute(
        &self,
        input: ForkInvocation<Fs, Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        Provider::<Resolve>::execute(input.authorization.filesystem(), input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Publish>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Publish>) -> Result<Version, MemoryError> {
        Provider::<Publish>::execute(input.authorization.filesystem(), input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Retract>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Retract>) -> Result<(), MemoryError> {
        Provider::<Retract>::execute(input.authorization.filesystem(), input.capability).await
    }
}
