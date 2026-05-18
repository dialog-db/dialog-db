//! Memory providers for FS-remote.
//!
//! Stub implementations: actual FS Access API / `std::fs` I/O lands in a
//! follow-up commit. The CAS layer (`.lock` files with UUIDv7 + heartbeat
//! for cross-tab coordination) is part of that follow-up.

use async_trait::async_trait;
use dialog_capability::ForkInvocation;
use dialog_capability::Provider;
use dialog_effects::memory::*;

use crate::fs::{Fs, FsInvocation};

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
        _input: FsInvocation<Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        Err(MemoryError::Storage(
            "dialog-remote-fs memory Resolve not yet implemented".into(),
        ))
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
    async fn execute(&self, _input: FsInvocation<Publish>) -> Result<Version, MemoryError> {
        Err(MemoryError::Storage(
            "dialog-remote-fs memory Publish not yet implemented".into(),
        ))
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
    async fn execute(&self, _input: FsInvocation<Retract>) -> Result<(), MemoryError> {
        Err(MemoryError::Storage(
            "dialog-remote-fs memory Retract not yet implemented".into(),
        ))
    }
}
