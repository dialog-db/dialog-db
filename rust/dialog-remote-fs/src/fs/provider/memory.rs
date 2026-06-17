//! Memory providers for FS-remote.
//!
//! Resolves the invocation's [`FsAddress`](crate::FsAddress) to the registered
//! [`FileSystem`](dialog_storage::provider::FileSystem) and delegates the
//! memory capability to it. CAS semantics, edition hashing, cross-writer
//! locking, and atomic writes all live in `dialog_storage`.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::memory::{Edition, MemoryError, Publish, Resolve, Retract, Version};

use crate::fs::Fs;
use crate::registry;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Resolve>> for Fs {
    async fn execute(
        &self,
        input: ForkInvocation<Fs, Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        let provider = registry::lookup(input.address.id())?;
        Provider::<Resolve>::execute(&provider, input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Publish>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Publish>) -> Result<Version, MemoryError> {
        let provider = registry::lookup(input.address.id())?;
        Provider::<Publish>::execute(&provider, input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Retract>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Retract>) -> Result<(), MemoryError> {
        let provider = registry::lookup(input.address.id())?;
        Provider::<Retract>::execute(&provider, input.capability).await
    }
}
