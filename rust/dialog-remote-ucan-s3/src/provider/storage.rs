//! Storage capability `Provider<Fork<UcanSite, Fx>>` implementations.

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::access::AuthorizeError;
use dialog_capability::fork::Fork;
use dialog_effects::storage::*;
use dialog_remote_s3::{Authorized, S3};

use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Get>> for UcanSite {
    async fn execute(
        &self,
        fork: Fork<UcanSite, Get>,
    ) -> Result<Result<Option<Vec<u8>>, StorageError>, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let permit = address
            .authorize(&capability)
            .await
            .map_err(|e| AuthorizeError::Denied(e.to_string()))?;

        Ok(
            <S3 as Provider<Authorized<Get>>>::execute(&S3, Authorized::new(permit, capability))
                .await,
        )
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Set>> for UcanSite {
    async fn execute(
        &self,
        fork: Fork<UcanSite, Set>,
    ) -> Result<Result<(), StorageError>, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let permit = address
            .authorize(&capability)
            .await
            .map_err(|e| AuthorizeError::Denied(e.to_string()))?;

        Ok(
            <S3 as Provider<Authorized<Set>>>::execute(&S3, Authorized::new(permit, capability))
                .await,
        )
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Delete>> for UcanSite {
    async fn execute(
        &self,
        fork: Fork<UcanSite, Delete>,
    ) -> Result<Result<(), StorageError>, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let permit = address
            .authorize(&capability)
            .await
            .map_err(|e| AuthorizeError::Denied(e.to_string()))?;

        Ok(
            <S3 as Provider<Authorized<Delete>>>::execute(&S3, Authorized::new(permit, capability))
                .await,
        )
    }
}
