//! Storage capability types and Provider implementations for S3 backend.
//!
//! Re-exports storage types from [`dialog_effects`] and implements
//! `Provider<Authorization<Fx, AuthorizedRequest>>` for [`S3`].
//! Each impl executes the presigned HTTP request and interprets the response.

pub use dialog_effects::storage::*;

use async_trait::async_trait;
use dialog_capability::{Authorization, Provider};
use dialog_s3_credentials::AuthorizedRequest;

use super::{RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Authorization<Get, AuthorizedRequest>> for S3 {
    async fn execute(
        &self,
        authorized: Authorization<Get, AuthorizedRequest>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let request = authorized.into_site();

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
impl Provider<Authorization<Set, AuthorizedRequest>> for S3 {
    async fn execute(
        &self,
        authorized: Authorization<Set, AuthorizedRequest>,
    ) -> Result<(), StorageError> {
        let value = Set::of(authorized.capability()).value.clone();
        let request = authorized.into_site();

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
impl Provider<Authorization<Delete, AuthorizedRequest>> for S3 {
    async fn execute(
        &self,
        authorized: Authorization<Delete, AuthorizedRequest>,
    ) -> Result<(), StorageError> {
        let request = authorized.into_site();

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
