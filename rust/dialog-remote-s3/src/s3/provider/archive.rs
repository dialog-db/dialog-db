//! Archive capability providers for S3.
//!
//! Each effect is paired: `Provider<Fork<S3, Fx>>` authorizes via SigV4,
//! then delegates to `Provider<Authorized<Fx>>` for HTTP execution.

use async_trait::async_trait;
use dialog_capability::access::AuthorizeError;
use dialog_capability::fork::Fork;
use dialog_capability::{Policy, Provider};
use dialog_effects::archive::*;

use crate::Authorized;
use crate::s3::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<S3, Get>> for S3 {
    async fn execute(
        &self,
        fork: Fork<S3, Get>,
    ) -> Result<Result<Option<Vec<u8>>, ArchiveError>, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let permit = address
            .authorize(&capability)
            .await
            .map_err(|e| AuthorizeError::Denied(e.to_string()))?;

        Ok(
            <S3 as Provider<Authorized<Get>>>::execute(self, Authorized::new(permit, capability))
                .await,
        )
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Get>> for S3 {
    async fn execute(&self, input: Authorized<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
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
impl Provider<Fork<S3, Put>> for S3 {
    async fn execute(
        &self,
        fork: Fork<S3, Put>,
    ) -> Result<Result<(), ArchiveError>, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let permit = address
            .authorize(&capability)
            .await
            .map_err(|e| AuthorizeError::Denied(e.to_string()))?;

        Ok(
            <S3 as Provider<Authorized<Put>>>::execute(self, Authorized::new(permit, capability))
                .await,
        )
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Put>> for S3 {
    async fn execute(&self, input: Authorized<Put>) -> Result<(), ArchiveError> {
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
