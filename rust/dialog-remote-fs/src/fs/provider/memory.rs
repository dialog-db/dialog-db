//! Memory providers for FS-remote.
//!
//! Layout matches `dialog_storage::storage::provider::fs::memory` so a
//! native `dialog-storage` consumer can read the same directory:
//!
//! ```text
//! {registered_root}/memory/{space}/{cell}
//! ```
//!
//! Memory cells carry CAS (compare-and-swap) semantics. The version of a
//! cell is the BLAKE3 hash of its content. `Publish` and `Retract` carry
//! a precondition (captured into the [`FsRequest`](crate::FsRequest) by
//! the translation layer):
//!
//! - `IfNoneMatch` — succeeds only if the cell does not currently exist
//! - `IfMatch(version)` — succeeds only if the cell's current version
//!   equals `version`
//! - `None` — unconditional (currently never emitted by the request
//!   translation, but accepted defensively)
//!
//! A `.lock` file adjacent to the cell coordinates concurrent writes.
//! Native uses `pidlock` (the same primitive as
//! `dialog-storage::fs::memory`), so a native `dialog-storage` consumer
//! and a native FS-remote consumer of this crate serialize their writes
//! correctly against each other.

use async_trait::async_trait;
use dialog_capability::ForkInvocation;
use dialog_capability::Provider;
use dialog_common::Blake3Hash;
use dialog_effects::memory::prelude::PublishExt;
use dialog_effects::memory::{Edition, MemoryError, Publish, Resolve, Retract, Version};

use crate::fs::provider::{navigate, split_target};
use crate::fs::{Fs, FsInvocation};
use crate::handle::FsHandle;
use crate::lock::LockGuard;
use crate::request::Precondition;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Resolve>> for Fs {
    async fn execute(
        &self,
        invocation: ForkInvocation<Fs, Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        invocation
            .authorization
            .redeem(&invocation.address)
            .invoke(invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<FsInvocation<Resolve>> for Fs {
    async fn execute(
        &self,
        input: FsInvocation<Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        let target = navigate(&input.permit.handle_id, &input.permit.request.path).await?;
        match target.read_optional().await? {
            Some(content) => {
                let version = Version::from(Blake3Hash::hash(&content).as_bytes());
                Ok(Some(Edition { content, version }))
            }
            None => Ok(None),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Publish>> for Fs {
    async fn execute(
        &self,
        invocation: ForkInvocation<Fs, Publish>,
    ) -> Result<Version, MemoryError> {
        invocation
            .authorization
            .redeem(&invocation.address)
            .invoke(invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<FsInvocation<Publish>> for Fs {
    async fn execute(&self, input: FsInvocation<Publish>) -> Result<Version, MemoryError> {
        let new_content = input.capability.content().to_vec();
        let new_version = Version::from(Blake3Hash::hash(&new_content).as_bytes());

        let (parent, file_name) =
            split_target(&input.permit.handle_id, &input.permit.request.path).await?;
        parent.ensure_dir().await?;
        let target = parent.resolve(file_name).await?;

        // Hold the lock across the read-check-write critical section so a
        // concurrent writer (CLI or browser tab) can't slip an update
        // between our CAS check and our write.
        let _guard = LockGuard::acquire(&target)
            .await
            .map_err(MemoryError::from)?;

        let current = target.read_optional().await?;
        let current_version = current
            .as_deref()
            .map(|bytes| Version::from(Blake3Hash::hash(bytes).as_bytes()));

        // Same-content shortcut: idempotent if the cell already holds
        // exactly what we're trying to publish.
        if current_version.as_ref() == Some(&new_version) {
            return Ok(new_version);
        }

        check_cas(&input.permit.request.precondition, current_version.as_ref())?;

        // Atomic temp+rename. The temp name includes the new version's
        // string repr to avoid collisions if multiple writers race past
        // the lock (shouldn't happen, but defense in depth).
        let tmp = parent
            .resolve(&format!("{}.{}.tmp", file_name, new_version))
            .await?;
        tmp.write(&new_content).await?;
        tmp.rename(&target).await?;

        Ok(new_version)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Retract>> for Fs {
    async fn execute(&self, invocation: ForkInvocation<Fs, Retract>) -> Result<(), MemoryError> {
        invocation
            .authorization
            .redeem(&invocation.address)
            .invoke(invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<FsInvocation<Retract>> for Fs {
    async fn execute(&self, input: FsInvocation<Retract>) -> Result<(), MemoryError> {
        let (parent, file_name) =
            split_target(&input.permit.handle_id, &input.permit.request.path).await?;
        let target = parent.resolve(file_name).await?;

        // Fast path: nothing to retract.
        if !target.exists().await {
            return Ok(());
        }

        let _guard = LockGuard::acquire(&target)
            .await
            .map_err(MemoryError::from)?;

        let current = match target.read_optional().await? {
            Some(bytes) => bytes,
            None => return Ok(()),
        };
        let current_version = Version::from(Blake3Hash::hash(&current).as_bytes());

        check_cas(&input.permit.request.precondition, Some(&current_version))?;

        target.remove().await?;
        Ok(())
    }
}

/// Validate the captured precondition against the cell's current version.
/// Returns `Err(VersionMismatch)` on failure.
fn check_cas(precondition: &Precondition, current: Option<&Version>) -> Result<(), MemoryError> {
    match (precondition, current) {
        (Precondition::None, _) => Ok(()),
        (Precondition::IfNoneMatch, None) => Ok(()),
        (Precondition::IfNoneMatch, Some(actual)) => Err(MemoryError::VersionMismatch {
            expected: None,
            actual: Some(actual.clone()),
        }),
        (Precondition::IfMatch(expected_str), Some(actual)) => {
            if expected_str == &actual.to_string() {
                Ok(())
            } else {
                Err(MemoryError::VersionMismatch {
                    expected: Some(Version::from(expected_str.clone())),
                    actual: Some(actual.clone()),
                })
            }
        }
        (Precondition::IfMatch(expected_str), None) => Err(MemoryError::VersionMismatch {
            expected: Some(Version::from(expected_str.clone())),
            actual: None,
        }),
    }
}
