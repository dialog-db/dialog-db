//! Storage capability providers for S3.
//!
//! Each effect is paired: `Provider<Fork<S3, Fx>>` authorizes via SigV4,
//! then delegates to `Provider<Authorized<Fx>>` for HTTP execution.

use async_trait::async_trait;
use dialog_capability::fork::{Fork, ForkInvocation};
use dialog_capability::{Policy, Provider};
use dialog_effects::storage::*;

use crate::Authorized;
use crate::s3::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<S3, Get>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Get>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let permit = invocation
            .address
            .authorize(&invocation.authorization.capability)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Get>>>::execute(
            self,
            Authorized::new(permit, invocation.authorization.capability),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Get>> for S3 {
    async fn execute(&self, input: Authorized<Get>) -> Result<Option<Vec<u8>>, StorageError> {
        let client = reqwest::Client::new();
        let response = input
            .permit
            .into_request(&client)
            .send()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        if response.status().is_success() {
            let bytes = response
                .bytes()
                .await
                .map_err(|e| StorageError::Storage(e.to_string()))?;
            Ok(Some(bytes.to_vec()))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            Err(StorageError::Storage(format!(
                "Failed to get value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<S3, Set>> for S3 {
    async fn execute(&self, invocation: ForkInvocation<S3, Set>) -> Result<(), StorageError> {
        let permit = invocation
            .address
            .authorize(&invocation.authorization.capability)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Set>>>::execute(
            self,
            Authorized::new(permit, invocation.authorization.capability),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Set>> for S3 {
    async fn execute(&self, input: Authorized<Set>) -> Result<(), StorageError> {
        let value = Set::of(&input.capability).value.clone();

        let client = reqwest::Client::new();
        let response = input
            .permit
            .into_request(&client)
            .body(value)
            .send()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(StorageError::Storage(format!(
                "Failed to set value: {}",
                response.status()
            )))
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<S3, Delete>> for S3 {
    async fn execute(&self, invocation: ForkInvocation<S3, Delete>) -> Result<(), StorageError> {
        let permit = invocation
            .address
            .authorize(&invocation.authorization.capability)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Delete>>>::execute(
            self,
            Authorized::new(permit, invocation.authorization.capability),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorized<Delete>> for S3 {
    async fn execute(&self, input: Authorized<Delete>) -> Result<(), StorageError> {
        let client = reqwest::Client::new();
        let response = input
            .permit
            .into_request(&client)
            .send()
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(StorageError::Storage(format!(
                "Failed to delete value: {}",
                response.status()
            )))
        }
    }
}
