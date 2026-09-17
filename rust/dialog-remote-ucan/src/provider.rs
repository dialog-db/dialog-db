//! `Provider<ForkInvocation<UcanSite, Fx>>` for [`UcanSite`], one impl
//! per effect the access service performs.
//!
//! Every impl runs the same exchange ([`direct::invoke`]) and then reads
//! the answer the way the object route's client would have: the
//! statuses, the `ETag`, the body. A permit in place of an answer is
//! completed through the permit-based site's request runner, so a
//! service that only redeems costs what it always did. A service that
//! does not read the invocation at all, or one that turned the request
//! away for its size or never answered it, gets the operation through
//! the permit-based site from the start.

use base58::ToBase58;
use dialog_capability::{Constraint, Effect, ForkInvocation, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::archive::prelude::PutExt;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _};
use dialog_effects::blob::{BlobError, BlobReader, BlobSink, BlobWriter, Import, Read};
use dialog_effects::memory::prelude::{PublishExt, RetractExt};
use dialog_effects::memory::{Edition, MemoryError, Publish, Resolve, Retract, Version};
use dialog_remote_s3::S3;
use dialog_remote_ucan_s3::UcanSite as PermitSite;

use crate::direct::{self, Outcome};
use crate::site::UcanSite;

/// Hand a fork to the permit-based site, which redeems and performs it
/// the way it always has.
async fn through_permits<Fx>(
    site: &UcanSite,
    invocation: ForkInvocation<UcanSite, Fx>,
) -> Fx::Output
where
    Fx: Effect + 'static,
    Fx::Of: Constraint,
    PermitSite: Provider<ForkInvocation<PermitSite, Fx>>,
{
    let ForkInvocation {
        capability,
        address,
        authorization,
    } = invocation;
    site.permits()
        .execute(ForkInvocation::new(
            capability,
            address.permits(),
            authorization,
        ))
        .await
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Get>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Get>,
    ) -> Result<Option<Vec<u8>>, ArchiveError> {
        match direct::invoke(&invocation.address, &invocation.authorization, None).await? {
            Outcome::Unsupported => through_permits(self, invocation).await,
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answered(answer) if answer.is_success() => Ok(Some(answer.bytes().await?)),
            Outcome::Answered(answer) if answer.status == 404 => Ok(None),
            Outcome::Answered(answer) if answer.is_refusal() => Err(answer.refusal().await.into()),
            Outcome::Answered(answer) => Err(ArchiveError::Storage(format!(
                "Failed to get value: {}",
                answer.status
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Put>> for UcanSite {
    async fn execute(&self, invocation: ForkInvocation<UcanSite, Put>) -> Result<(), ArchiveError> {
        let payload = invocation.capability.content().to_vec();
        match direct::invoke(
            &invocation.address,
            &invocation.authorization,
            Some(payload),
        )
        .await?
        {
            Outcome::Unsupported => through_permits(self, invocation).await,
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answered(answer) if answer.is_success() => Ok(()),
            Outcome::Answered(answer) if answer.is_refusal() => Err(answer.refusal().await.into()),
            Outcome::Answered(answer) => Err(ArchiveError::Storage(format!(
                "Failed to put value: {}",
                answer.status
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Resolve>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        match direct::invoke(&invocation.address, &invocation.authorization, None).await? {
            Outcome::Unsupported => through_permits(self, invocation).await,
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answered(answer) if answer.is_success() => {
                let version = Version::from(answer.version()?);
                Ok(Some(Edition {
                    content: answer.bytes().await?,
                    version,
                }))
            }
            Outcome::Answered(answer) if answer.status == 404 => Ok(None),
            Outcome::Answered(answer) if answer.is_refusal() => Err(answer.refusal().await.into()),
            Outcome::Answered(answer) => Err(MemoryError::Storage(format!(
                "Failed to resolve value: {}",
                answer.status
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Publish>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Publish>,
    ) -> Result<Version, MemoryError> {
        let payload = invocation.capability.content().to_vec();
        match direct::invoke(
            &invocation.address,
            &invocation.authorization,
            Some(payload),
        )
        .await?
        {
            Outcome::Unsupported => through_permits(self, invocation).await,
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answered(answer) if answer.is_success() => {
                Ok(Version::from(answer.version()?))
            }
            Outcome::Answered(answer) if answer.status == 412 => {
                Err(MemoryError::VersionMismatch {
                    expected: invocation.capability.when().cloned(),
                    actual: None,
                })
            }
            Outcome::Answered(answer) if answer.is_refusal() => Err(answer.refusal().await.into()),
            Outcome::Answered(answer) => Err(MemoryError::Storage(format!(
                "Failed to publish value: {}",
                answer.status
            ))),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Retract>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Retract>,
    ) -> Result<(), MemoryError> {
        match direct::invoke(&invocation.address, &invocation.authorization, None).await? {
            Outcome::Unsupported => through_permits(self, invocation).await,
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answered(answer) if answer.is_success() => Ok(()),
            Outcome::Answered(answer) if answer.status == 412 => {
                Err(MemoryError::VersionMismatch {
                    expected: Some(invocation.capability.when().clone()),
                    actual: None,
                })
            }
            Outcome::Answered(answer) if answer.is_refusal() => Err(answer.refusal().await.into()),
            Outcome::Answered(answer) => Err(MemoryError::Storage(format!(
                "Failed to retract value: {}",
                answer.status
            ))),
        }
    }
}

/// A blob read answers with the bytes, the range the invocation asked
/// for when it asked for one, as chunks off the wire.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Read>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Read>,
    ) -> Result<BlobReader, BlobError> {
        match direct::invoke(&invocation.address, &invocation.authorization, None).await? {
            Outcome::Unsupported => through_permits(self, invocation).await,
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answered(answer) if answer.is_success() => Ok(answer.source()),
            Outcome::Answered(answer) if answer.status == 404 => Err(BlobError::NotFound(
                invocation.capability.digest().as_bytes().to_base58(),
            )),
            Outcome::Answered(answer) if answer.is_refusal() => Err(answer.refusal().await.into()),
            Outcome::Answered(answer) => Err(BlobError::Storage(format!(
                "blob read failed: {}",
                answer.status
            ))),
        }
    }
}

/// A blob import is written into a sink that sends the bytes when it
/// is finished: the digest is bound in the invocation, so the bytes are
/// checked against it before anything is sent.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Import>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Import>,
    ) -> Result<BlobWriter, BlobError> {
        Ok(Box::new(Upload {
            site: self.clone(),
            invocation,
            buffer: Vec::new(),
        }))
    }
}

/// Gathers a blob's bytes and sends them, in the request that proves
/// the import, once they have been checked against the declared digest.
struct Upload {
    site: UcanSite,
    invocation: ForkInvocation<UcanSite, Import>,
    buffer: Vec<u8>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSink for Upload {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        let Upload {
            site,
            invocation,
            buffer,
        } = *self;
        let expected = invocation.capability.digest().clone();
        let hash = Blake3Hash::hash(&buffer);
        if hash != expected {
            return Err(BlobError::DigestMismatch {
                expected: expected.as_bytes().to_base58(),
                actual: hash.as_bytes().to_base58(),
            });
        }
        match direct::invoke(
            &invocation.address,
            &invocation.authorization,
            Some(buffer.clone()),
        )
        .await?
        {
            Outcome::Unsupported => {
                let mut writer = through_permits(&site, invocation).await?;
                writer.write_all(&buffer).await?;
                writer.finish().await
            }
            Outcome::Permit(permit) => {
                let mut writer = permit.invoke(invocation.capability).perform(&S3).await?;
                writer.write_all(&buffer).await?;
                writer.finish().await
            }
            Outcome::Answered(answer) if answer.is_success() => Ok(hash),
            Outcome::Answered(answer) if answer.is_refusal() => Err(answer.refusal().await.into()),
            Outcome::Answered(answer) => Err(BlobError::Storage(format!(
                "blob import failed: {}",
                answer.status
            ))),
        }
    }
}
