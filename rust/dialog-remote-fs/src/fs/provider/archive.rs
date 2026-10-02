//! Archive providers for FS-remote.
//!
//! Delegates the archive capability to the verified
//! [`FileSystem`](dialog_storage::provider::FileSystem) the authorization
//! carries. The on-disk layout, idempotent content-addressed writes, and atomic
//! temp+rename all live in `dialog_storage`.
//!
//! Every invocation passes through the transfer meter (and, when configured,
//! the simulated link) in [`simulation`](crate::fs::simulation): each of these
//! effects is one request a real remote would serve, so this is where the
//! transport's round trips are counted and shaped.

use std::sync::Arc;

use dialog_capability::{ForkInvocation, Provider};
use dialog_effects::archive::*;

use crate::fs::Fs;
use crate::fs::simulation::{self, Traffic};

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Get>> for Fs {
    async fn execute(
        &self,
        input: ForkInvocation<Fs, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let digest = input.capability.constraint.digest.clone();
        let filesystem = input.authorization.filesystem().clone();
        let key = format!("{}#{digest}", filesystem.handle().url());
        // The metered request (and, when configured, the simulated link's
        // delays) live inside the shared future: joiners share one wire
        // request, exactly as a deduplicating remote would serve them.
        let capability = input.capability;
        let outcome = self
            .gets()
            .join(key, move || async move {
                #[cfg(not(target_arch = "wasm32"))]
                if std::env::var("DIALOG_SOAK_TRACE_GETS").is_ok() {
                    static EPOCH: std::sync::LazyLock<tokio::time::Instant> =
                        std::sync::LazyLock::new(tokio::time::Instant::now);
                    eprintln!(
                        "GET t={} {digest}",
                        (tokio::time::Instant::now() - *EPOCH).as_millis()
                    );
                }
                let flight = simulation::begin(Traffic::ArchiveGet).await;
                let result = Provider::<Get>::execute(&filesystem, capability).await;
                let bytes = match &result {
                    Ok(Some(block)) => block.len(),
                    _ => 0,
                };
                simulation::record_get(
                    digest.to_string(),
                    match &result {
                        Ok(Some(_)) => Some(bytes),
                        _ => None,
                    },
                );
                flight.complete(bytes).await;
                match result {
                    Ok(block) => Ok(block.map(Arc::new)),
                    Err(error) => Err(error.to_string()),
                }
            })
            .await;
        match outcome {
            Ok(block) => Ok(block.map(|bytes| bytes.as_ref().clone())),
            Err(message) => Err(ArchiveError::Storage(message)),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Put>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Put>) -> Result<(), ArchiveError> {
        let flight = simulation::begin(Traffic::ArchivePut).await;
        let bytes = input.capability.constraint.block.as_ref().len();
        let result =
            Provider::<Put>::execute(input.authorization.filesystem(), input.capability).await;
        flight.complete(bytes).await;
        result
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Fs, Import>> for Fs {
    async fn execute(&self, input: ForkInvocation<Fs, Import>) -> Result<(), ArchiveError> {
        let flight = simulation::begin(Traffic::ArchiveImport).await;
        let bytes = input
            .capability
            .constraint
            .blocks
            .iter()
            .map(|block| block.as_ref().len())
            .sum();
        let result =
            Provider::<Import>::execute(input.authorization.filesystem(), input.capability).await;
        flight.complete(bytes).await;
        result
    }
}
