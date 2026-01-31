//! Storage capability types and Provider implementations for S3 backend.
//!
//! Re-exports storage types from [`dialog_effects`] and implements
//! `Provider<Get>`, `Provider<Set>`, and `Provider<Delete>` for [`S3`].

pub use dialog_effects::storage::*;

use async_trait::async_trait;
use dialog_capability::{Authority, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_s3_credentials::capability::storage::{
    Delete as AuthorizeDelete, Get as AuthorizeGet, Set as AuthorizeSet,
};

use super::{Hasher, RequestDescriptorExt, S3};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Issuer> Provider<Get> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(&mut self, input: Capability<Get>) -> Result<Option<Vec<u8>>, StorageError> {
        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(Storage)
            .attenuate(Store::of(&input).clone())
            .invoke(AuthorizeGet {
                key: Get::of(&input).key.clone(),
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| StorageError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
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
impl<Issuer> Provider<Set> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(&mut self, input: Capability<Set>) -> Result<(), StorageError> {
        let Set { key, value } = Set::of(&input);
        let checksum = Hasher::Sha256.checksum(value);

        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(Storage)
            .attenuate(Store::of(&input).clone())
            .invoke(AuthorizeSet {
                key: key.clone(),
                checksum,
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| StorageError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client).body(value.to_vec());
        let response = builder
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
impl<Issuer> Provider<Delete> for S3<Issuer>
where
    Issuer: Authority + ConditionalSend + ConditionalSync,
{
    async fn execute(&mut self, input: Capability<Delete>) -> Result<(), StorageError> {
        // Build the authorization capability
        let capability = Subject::from(input.subject().to_string())
            .attenuate(Storage)
            .attenuate(Store::of(&input).clone())
            .invoke(AuthorizeDelete {
                key: Delete::of(&input).key.clone(),
            });

        // Acquire authorization and perform
        let authorized = capability
            .acquire(self)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let authorization = authorized
            .perform(self)
            .await
            .map_err(|e| StorageError::Storage(format!("{:?}", e)))?;

        let client = reqwest::Client::new();
        let builder = authorization.into_request(&client);
        let response = builder
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
