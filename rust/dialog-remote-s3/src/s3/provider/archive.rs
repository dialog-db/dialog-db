//! Archive providers for S3.

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::ForkInvocation;
use dialog_effects::archive::*;
use reqwest::StatusCode;

use crate::s3::{S3, S3Invocation};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<S3, Get>> for S3 {
    async fn execute(
        &self,
        input: ForkInvocation<S3, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        input
            .authorization
            .permit(&input.capability, &input.address)
            .await?
            .invoke(input.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Get>> for S3 {
    async fn execute(&self, input: S3Invocation<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        let response = input.permit.send().await?;

        if response.status().is_success() {
            let bytes = response.bytes().await.map_err(crate::S3Error::from)?;
            Ok(Some(bytes.to_vec()))
        } else if response.status() == StatusCode::NOT_FOUND {
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
    async fn execute(&self, input: ForkInvocation<S3, Put>) -> Result<(), ArchiveError> {
        input
            .authorization
            .permit(&input.capability, &input.address)
            .await?
            .invoke(input.capability)
            .perform(self)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Put>> for S3 {
    async fn execute(&self, input: S3Invocation<Put>) -> Result<(), ArchiveError> {
        let put = input.capability.into_effect();
        let response = input.permit.upload(put.content).await?;

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
