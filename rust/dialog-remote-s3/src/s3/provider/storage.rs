//! Storage capability types and Provider implementations for S3 backend.
//!
//! Re-exports storage types from [`dialog_effects`] and implements
//! `Provider<Fork<S3, Fx>>` for [`S3`].
//! Each impl presigns the request and executes it via HTTP.

pub use dialog_effects::storage::*;

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::{Fork, ForkInvocation};

use crate::s3::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<S3, Get>> for S3 {
    async fn execute(
        &self,
        invocation: ForkInvocation<S3, Get>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let request = invocation
            .address
            .authorize(
                &invocation.authorization.capability,
                invocation.credentials.as_ref(),
            )
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let client = reqwest::Client::new();
        let response = request
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
        let value = Set::of(&invocation.authorization.capability).value.clone();

        let request = invocation
            .address
            .authorize(
                &invocation.authorization.capability,
                invocation.credentials.as_ref(),
            )
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let client = reqwest::Client::new();
        let response = request
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
        let request = invocation
            .address
            .authorize(
                &invocation.authorization.capability,
                invocation.credentials.as_ref(),
            )
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let client = reqwest::Client::new();
        let response = request
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
