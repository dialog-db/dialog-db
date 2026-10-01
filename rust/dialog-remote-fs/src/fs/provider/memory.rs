//! Memory providers for FS-remote.
//!
//! Delegates the memory capability to the verified
//! [`FileSystem`](dialog_storage::provider::FileSystem) the authorization
//! carries. CAS semantics, edition hashing, cross-writer locking, and atomic
//! writes all live in `dialog_storage`.
//!
//! Every invocation passes through the transfer meter (and, when configured,
//! the simulated link) in [`simulation`](crate::fs::simulation) — a head-cell
//! resolve or publish is one round trip a real remote would serve.

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::Rejection;
use dialog_effects::memory::{
    Edition, Editions, MemoryError, Publish, Resolve, Retract, Version, Watch,
};

use crate::fs::Fs;
use crate::fs::simulation::{self, Traffic};

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Resolve>> for Fs {
    async fn execute(
        &self,
        input: ForkInvocation<Fs, Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        let flight = simulation::begin(Traffic::MemoryResolve).await;
        let result =
            Provider::<Resolve>::execute(input.authorization.filesystem(), input.capability).await;
        let bytes = match &result {
            Ok(Some(edition)) => edition.content.len(),
            _ => 0,
        };
        flight.complete(bytes).await;
        result
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Publish>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Publish>) -> Result<Version, MemoryError> {
        let flight = simulation::begin(Traffic::MemoryPublish).await;
        let bytes = input.capability.constraint.content.len();
        let result =
            Provider::<Publish>::execute(input.authorization.filesystem(), input.capability).await;
        flight.complete(bytes).await;
        result
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Retract>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Retract>) -> Result<(), MemoryError> {
        let flight = simulation::begin(Traffic::MemoryRetract).await;
        let result =
            Provider::<Retract>::execute(input.authorization.filesystem(), input.capability).await;
        flight.complete(0).await;
        result
    }
}

/// A directory cannot follow a cell as it changes: a watch is refused, and the
/// cell is read by resolving it again instead.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Watch>> for Fs {
    async fn execute(&self, _: ForkInvocation<Fs, Watch>) -> Result<Editions, MemoryError> {
        Err(Rejection::Unsupported {
            reason: "a filesystem remote does not watch its cells".into(),
        }
        .into())
    }
}
