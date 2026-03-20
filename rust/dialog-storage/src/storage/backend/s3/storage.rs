//! Storage capability types and Provider implementations for S3 backend.
//!
//! Re-exports storage types from [`dialog_effects`] and implements
//! `Provider<S3Invocation<Fx>>` for [`S3`].
//! Each impl executes the presigned HTTP request and interprets the response.

pub use dialog_effects::storage::*;

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_s3_credentials::s3::site::S3Invocation;

use super::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<S3Invocation<Get>> for S3 {
    async fn execute(
        &self,
        invocation: S3Invocation<Get>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let client = reqwest::Client::new();
        let response = invocation
            .request
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
impl Provider<S3Invocation<Set>> for S3 {
    async fn execute(&self, invocation: S3Invocation<Set>) -> Result<(), StorageError> {
        let value = Set::of(&invocation.capability).value.clone();

        let client = reqwest::Client::new();
        let response = invocation
            .request
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
impl Provider<S3Invocation<Delete>> for S3 {
    async fn execute(&self, invocation: S3Invocation<Delete>) -> Result<(), StorageError> {
        let client = reqwest::Client::new();
        let response = invocation
            .request
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
