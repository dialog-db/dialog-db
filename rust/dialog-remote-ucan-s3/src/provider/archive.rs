//! Archive capability `Provider<ForkInvocation<UcanSite, Fx>>` implementations.

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::ForkInvocation;
use dialog_effects::archive::*;
use dialog_remote_s3::{Authorized, S3};

use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Get>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let permit = invocation
            .address
            .authorize(&invocation.invocation)
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        <S3 as Provider<Authorized<Get>>>::execute(
            &S3,
            Authorized::new(permit, invocation.capability),
        )
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Put>> for UcanSite {
    async fn execute(&self, invocation: ForkInvocation<UcanSite, Put>) -> Result<(), ArchiveError> {
        let permit = invocation
            .address
            .authorize(&invocation.invocation)
            .await
            .map_err(|e| ArchiveError::Io(e.to_string()))?;

        <S3 as Provider<Authorized<Put>>>::execute(
            &S3,
            Authorized::new(permit, invocation.capability),
        )
        .await
    }
}
