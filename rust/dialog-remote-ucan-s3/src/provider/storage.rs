//! Storage capability `Provider<Fork<UcanSite, Fx>>` implementations.

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::{Fork, ForkInvocation};
use dialog_effects::storage::*;
use dialog_remote_s3::{Authorized, S3};

use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Get>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Get>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let permit = invocation
            .address
            .authorize(&invocation.authorization.authorization)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Get>>>::execute(
            &S3,
            Authorized::new(permit, invocation.authorization.capability),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Set>> for UcanSite {
    async fn execute(&self, invocation: ForkInvocation<UcanSite, Set>) -> Result<(), StorageError> {
        let permit = invocation
            .address
            .authorize(&invocation.authorization.authorization)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Set>>>::execute(
            &S3,
            Authorized::new(permit, invocation.authorization.capability),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Delete>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Delete>,
    ) -> Result<(), StorageError> {
        let permit = invocation
            .address
            .authorize(&invocation.authorization.authorization)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Delete>>>::execute(
            &S3,
            Authorized::new(permit, invocation.authorization.capability),
        )
        .await
    }
}
