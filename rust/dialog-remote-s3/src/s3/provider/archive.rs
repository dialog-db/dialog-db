//! Archive capability providers for S3.
//!
//! Each effect is paired: `Provider<ForkInvocation<S3, Fx>>` authorizes via SigV4,
//! then delegates to `Provider<S3Invocation<Fx>>` for HTTP execution.

use async_trait::async_trait;
use dialog_capability::fork::ForkInvocation;
use dialog_capability::{Policy, Provider};
use dialog_effects::archive::*;

use crate::s3::{RequestDescriptorExt, S3, S3Invocation};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Get>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let permit = invocation
            .authorization
            .grant(&invocation.capability, &invocation.address)
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        S3Invocation::new(permit, invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Get>> for S3 {
    async fn execute(&self, input: S3Invocation<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let client = reqwest::Client::new();
        let response = input
            .permit
            .into_request(&client)
            .send()
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        if response.status().is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| ArchiveError::Io(e.to_string()))?;
            Ok(Some(bytes.to_vec()))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(ArchiveError::Storage(format!(
                "Failed to get value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Put>> for S3 {
    async fn execute(&self, invocation: ForkInvocation<S3, Put>) -> Result<(), ArchiveError> {
        let permit = invocation
            .authorization
            .grant(&invocation.capability, &invocation.address)
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        S3Invocation::new(permit, invocation.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Put>> for S3 {
    async fn execute(&self, input: S3Invocation<Put>) -> Result<(), ArchiveError> {
        let content = Put::of(&input.capability).content.clone();

        let client = reqwest::Client::new();
        let response = input
            .permit
            .into_request(&client)
            .body(content)
            .send()
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(ArchiveError::Storage(format!(
                "Failed to put value: {}",
                response.status()
            )))
        }
    }
}
