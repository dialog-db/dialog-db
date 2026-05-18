//! Archive providers for FS-remote.
//!
//! Stub implementations: actual FS Access API / `std::fs` I/O lands in a
//! follow-up commit. The structural surface is in place so downstream
//! callers (Network composite, operator dispatch) can be wired up against
//! the final API shape.

use async_trait::async_trait;
use dialog_capability::ForkInvocation;
use dialog_capability::Provider;
use dialog_effects::archive::*;

use crate::fs::{Fs, FsInvocation};

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
    async fn execute(&self, _input: FsInvocation<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        Err(ArchiveError::Io(
            "dialog-remote-fs archive Get not yet implemented".into(),
        ))
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
    async fn execute(&self, _input: FsInvocation<Put>) -> Result<(), ArchiveError> {
        Err(ArchiveError::Io(
            "dialog-remote-fs archive Put not yet implemented".into(),
        ))
    }
}
