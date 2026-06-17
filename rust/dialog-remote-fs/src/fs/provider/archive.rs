//! Archive providers for FS-remote.
//!
//! Delegates the archive capability to the
//! [`FileSystem`](dialog_storage::provider::FileSystem) the authorization
//! resolved from the directory grant. The on-disk layout, idempotent
//! content-addressed writes, and atomic temp+rename all live in `dialog_storage`.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::archive::*;

use crate::fs::Fs;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Get>> for Fs {
    async fn execute(
        &self,
        input: ForkInvocation<Fs, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        Provider::<Get>::execute(input.authorization.filesystem(), input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Put>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Put>) -> Result<(), ArchiveError> {
        Provider::<Put>::execute(input.authorization.filesystem(), input.capability).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Import>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Import>) -> Result<(), ArchiveError> {
        Provider::<Import>::execute(input.authorization.filesystem(), input.capability).await
    }
}
