//! Blob providers for UCAN-authorized S3.
//!
//! Thin wrappers: redeem the UCAN authorization at the access service to obtain
//! a presigned permit, then delegate to the shared S3 HTTP execution. The
//! access service is responsible for presigning the blob path
//! (`{subject}/blob/{digest}`) — a GET for `Read`, a PUT for single-part
//! `Import`.

use async_trait::async_trait;
use dialog_capability::ForkInvocation;
use dialog_capability::Provider;
use dialog_effects::blob::{BlobError, BlobReader, BlobWriter, Import, Read};
use dialog_remote_s3::S3;

use crate::site::UcanSite;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Read>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Read>,
    ) -> Result<BlobReader, BlobError> {
        invocation
            .authorization
            .redeem(&invocation.address)
            .await?
            .invoke(invocation.capability)
            .perform(&S3)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Import>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Import>,
    ) -> Result<BlobWriter, BlobError> {
        invocation
            .authorization
            .redeem(&invocation.address)
            .await?
            .invoke(invocation.capability)
            .perform(&S3)
            .await
    }
}
