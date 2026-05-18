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

use crate::FsError;
use crate::fs::{Fs, FsInvocation};
use crate::handle::FsHandle;
use crate::registry;
use crate::request::FsRequest;

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
        let target = navigate(&input.permit.request, &input.permit.handle_id).await?;
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

        let request = &input.permit.request;
        let handle_id = &input.permit.handle_id;

        // Split the request path: `[archive, catalog, base58digest]` → the
        // last element is the file name; everything before is the parent
        // directory we need to ensure exists for the atomic temp+rename.
        let (file_name, parent_segments) =
            request.path.split_last().ok_or_else(|| FsError::Io(
                "empty path for archive Put — request translation produced no segments".into(),
            ))?;

        let mut parent = registry::lookup(handle_id)?;
        for segment in parent_segments {
            parent = parent.resolve(segment).await?;
        }
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

/// Navigate the request's path under the registered handle and return the
/// resolved leaf handle. Used by Get; Put needs the parent separately for
/// the temp+rename dance so it open-codes the navigation.
async fn navigate(
    request: &FsRequest,
    handle_id: &str,
) -> Result<crate::handle::Handle, FsError> {
    let mut current = registry::lookup(handle_id)?;
    for segment in &request.path {
        current = current.resolve(segment).await?;
    }
    Ok(current)
}
