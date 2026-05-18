//! Archive providers for FS-remote.
//!
//! Layout matches `dialog_storage::storage::provider::fs::archive` so a
//! native `dialog-storage` consumer can read the same directory:
//!
//! ```text
//! {registered_root}/archive/{catalog}/{base58(digest)}
//! ```
//!
//! Put is content-addressed and idempotent: if the target file already
//! exists, the write is skipped. Writes are atomic via temp file + rename.

use async_trait::async_trait;
use base58::ToBase58;
use dialog_capability::ForkInvocation;
use dialog_capability::Provider;
use dialog_common::Blake3Hash;
use dialog_effects::archive::prelude::PutExt;
use dialog_effects::archive::*;

use crate::fs::provider::{navigate, split_target};
use crate::fs::{Fs, FsInvocation};
use crate::handle::FsHandle;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Get>> for Fs {
    async fn execute(
        &self,
        input: ForkInvocation<Fs, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        input
            .authorization
            .redeem(&input.address)
            .invoke(input.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<FsInvocation<Get>> for Fs {
    async fn execute(&self, input: FsInvocation<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let target = navigate(&input.permit.handle_id, &input.permit.request.path).await?;
        target.read_optional().await.map_err(ArchiveError::from)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Put>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Put>) -> Result<(), ArchiveError> {
        input
            .authorization
            .redeem(&input.address)
            .invoke(input.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<FsInvocation<Put>> for Fs {
    async fn execute(&self, input: FsInvocation<Put>) -> Result<(), ArchiveError> {
        let digest = input.capability.digest().clone();
        let content = input.capability.content().to_vec();

        // Defense-in-depth: confirm the content really hashes to the
        // declared digest. dialog-storage's native FS provider does the
        // same check; doing it here too means a corrupt capability can't
        // poison the on-disk content-addressed store.
        let actual = Blake3Hash::hash(&content);
        if actual != digest {
            return Err(ArchiveError::DigestMismatch {
                expected: digest.as_bytes().to_base58(),
                actual: actual.as_bytes().to_base58(),
            });
        }

        let (parent, file_name) =
            split_target(&input.permit.handle_id, &input.permit.request.path).await?;
        parent.ensure_dir().await?;

        let target = parent.resolve(file_name).await?;
        if target.exists().await {
            // Idempotent: content-addressed store, same digest → same file.
            return Ok(());
        }

        let tmp = parent.resolve(&format!("{}.tmp", file_name)).await?;
        tmp.write(&content).await?;
        tmp.rename(&target).await?;
        Ok(())
    }
}
