//! Memory capability `Provider<ForkInvocation<UcanSite, Fx>>` implementations.

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::ForkInvocation;
use dialog_effects::memory::*;
use dialog_remote_s3::{Authorized, S3};

use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Resolve>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        
        let permit = invocation
            .address
            .authorize(&invocation.invocation)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Resolve>>>::execute(
            &S3,
            Authorized::new(permit, invocation.capability),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Publish>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Publish>,
    ) -> Result<Vec<u8>, MemoryError> {
        
        let permit = invocation
            .address
            .authorize(&invocation.invocation)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Publish>>>::execute(
            &S3,
            Authorized::new(permit, invocation.capability),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Retract>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Retract>,
    ) -> Result<(), MemoryError> {
        
        let permit = invocation
            .address
            .authorize(&invocation.invocation)
            .await
            .map_err(|e| MemoryError::Storage(e.to_string()))?;

        <S3 as Provider<Authorized<Retract>>>::execute(
            &S3,
            Authorized::new(permit, invocation.capability),
        )
        .await
    }
}
