//! Archive providers for FS-remote.
//!
//! Resolves the invocation's [`FsAddress`](crate::FsAddress) to the registered
//! [`FileSystem`](dialog_storage::provider::FileSystem) and delegates the
//! archive capability to it. The on-disk layout, idempotent content-addressed
//! writes, and atomic temp+rename all live in `dialog_storage`.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::archive::*;

use crate::fs::Fs;
use crate::registry;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Get>> for Fs {
    async fn execute(
        &self,
        input: ForkInvocation<Fs, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let provider = registry::lookup(input.address.id())?;
        Provider::<Get>::execute(&provider, input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Put>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Put>) -> Result<(), ArchiveError> {
        let provider = registry::lookup(input.address.id())?;
        Provider::<Put>::execute(&provider, input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Import>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Import>) -> Result<(), ArchiveError> {
        let provider = registry::lookup(input.address.id())?;
        Provider::<Import>::execute(&provider, input.capability).await
    }
}
