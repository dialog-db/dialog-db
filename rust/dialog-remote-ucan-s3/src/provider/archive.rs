//! Archive providers for UCAN-authorized S3.

use async_trait::async_trait;
use dialog_capability::ForkInvocation;
use dialog_capability::Provider;
use dialog_effects::archive::*;
use dialog_remote_s3::S3;

use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Get>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        let (permit, key) = crate::permit_cache::redeem_cached(
            &invocation.authorization,
            &invocation.address,
            &invocation.capability,
        )
        .await?;
        let result = permit.invoke(invocation.capability).perform(&S3).await;
        if result.is_err() {
            // A permit that failed downstream may be stale (revoked or
            // expired server-side); drop it so the next attempt redeems.
            crate::permit_cache::PermitCache::shared().invalidate(&key);
        }
        result
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Put>> for UcanSite {
    async fn execute(&self, invocation: ForkInvocation<UcanSite, Put>) -> Result<(), ArchiveError> {
        let (permit, key) = crate::permit_cache::redeem_cached(
            &invocation.authorization,
            &invocation.address,
            &invocation.capability,
        )
        .await?;
        let result = permit.invoke(invocation.capability).perform(&S3).await;
        if result.is_err() {
            // A permit that failed downstream may be stale (revoked or
            // expired server-side); drop it so the next attempt redeems.
            crate::permit_cache::PermitCache::shared().invalidate(&key);
        }
        result
    }
}
