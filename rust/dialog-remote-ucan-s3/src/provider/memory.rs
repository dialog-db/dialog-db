//! Memory providers for UCAN-authorized S3.

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_capability::fork::ForkInvocation;
use dialog_effects::memory::*;
use dialog_remote_s3::S3;

use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Resolve>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Resolve>,
    ) -> Result<Option<Publication>, MemoryError> {
        invocation
            .address
            .authorize(&invocation.authorization)
            .await?
            .invoke(invocation.capability)
            .perform(&S3)
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
        invocation
            .address
            .authorize(&invocation.authorization)
            .await?
            .invoke(invocation.capability)
            .perform(&S3)
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
        invocation
            .address
            .authorize(&invocation.authorization)
            .await?
            .invoke(invocation.capability)
            .perform(&S3)
            .await
    }
}
