//! Memory capability `Provider<Fork<UcanSite, Fx>>` implementations.

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::access::AuthorizeError;
use dialog_capability::fork::Fork;
use dialog_effects::memory::*;
use dialog_remote_s3::{Authorized, S3};

use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Resolve>> for UcanSite {
    async fn execute(
        &self,
        fork: Fork<UcanSite, Resolve>,
    ) -> Result<Result<Option<Publication>, MemoryError>, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let permit = address
            .authorize(&capability)
            .await
            .map_err(|e| AuthorizeError::Denied(e.to_string()))?;

        Ok(
            <S3 as Provider<Authorized<Resolve>>>::execute(
                &S3,
                Authorized::new(permit, capability),
            )
            .await,
        )
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Publish>> for UcanSite {
    async fn execute(
        &self,
        fork: Fork<UcanSite, Publish>,
    ) -> Result<Result<Vec<u8>, MemoryError>, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let permit = address
            .authorize(&capability)
            .await
            .map_err(|e| AuthorizeError::Denied(e.to_string()))?;

        Ok(
            <S3 as Provider<Authorized<Publish>>>::execute(
                &S3,
                Authorized::new(permit, capability),
            )
            .await,
        )
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<Fork<UcanSite, Retract>> for UcanSite {
    async fn execute(
        &self,
        fork: Fork<UcanSite, Retract>,
    ) -> Result<Result<(), MemoryError>, AuthorizeError> {
        let (capability, address) = fork.into_parts();
        let permit = address
            .authorize(&capability)
            .await
            .map_err(|e| AuthorizeError::Denied(e.to_string()))?;

        Ok(
            <S3 as Provider<Authorized<Retract>>>::execute(
                &S3,
                Authorized::new(permit, capability),
            )
            .await,
        )
    }
}
