//! `Provider<ForkInvocation<UcanSite, Fx>>` for [`UcanSite`], one impl
//! per effect the access service performs.
//!
//! Every impl runs the same exchange ([`direct::invoke`]) and then reads
//! the answer the way the object route's client would have: the
//! statuses, the `ETag`, the body. A permit in place of an answer is
//! completed through the permit-based site's request runner, so a
//! service that does not perform operations costs what it always did.
//! A request turned away for its size, which a payload can run into at
//! such a service, is retried through the permit-based site whole.

use dialog_capability::{Constraint, Effect, ForkInvocation, Provider};
use dialog_effects::archive::prelude::PutExt;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::{BlobError, BlobReader, BlobWriter, Import, Read};
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
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answer(answer) if answer.is_success() => Ok(Some(answer.body)),
            Outcome::Answer(answer) if answer.status == 404 => Ok(None),
            Outcome::Answer(answer) if answer.is_refusal() => Err(answer.refusal().into()),
            Outcome::Answer(answer) => Err(ArchiveError::Storage(format!(
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
        let payload = invocation.capability.content();
        match direct::invoke(
            &invocation.address,
            &invocation.authorization,
            Some(payload),
        )
        .await?
        {
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answer(answer) if answer.is_success() => Ok(()),
            Outcome::Answer(answer) if answer.is_too_large() => {
                through_permits(self, invocation).await
            }
            Outcome::Answer(answer) if answer.is_refusal() => Err(answer.refusal().into()),
            Outcome::Answer(answer) => Err(ArchiveError::Storage(format!(
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
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answer(answer) if answer.is_success() => {
                let version = Version::from(answer.version()?);
                Ok(Some(Edition {
                    content: answer.body,
                    version,
                }))
            }
            Outcome::Answer(answer) if answer.status == 404 => Ok(None),
            Outcome::Answer(answer) if answer.is_refusal() => Err(answer.refusal().into()),
            Outcome::Answer(answer) => Err(MemoryError::Storage(format!(
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
        let payload = invocation.capability.content();
        match direct::invoke(
            &invocation.address,
            &invocation.authorization,
            Some(payload),
        )
        .await?
        {
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answer(answer) if answer.is_success() => Ok(Version::from(answer.version()?)),
            Outcome::Answer(answer) if answer.status == 412 => Err(MemoryError::VersionMismatch {
                expected: invocation.capability.when().cloned(),
                actual: None,
            }),
            Outcome::Answer(answer) if answer.is_too_large() => {
                through_permits(self, invocation).await
            }
            Outcome::Answer(answer) if answer.is_refusal() => Err(answer.refusal().into()),
            Outcome::Answer(answer) => Err(MemoryError::Storage(format!(
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
            Outcome::Permit(permit) => permit.invoke(invocation.capability).perform(&S3).await,
            Outcome::Answer(answer) if answer.is_success() => Ok(()),
            Outcome::Answer(answer) if answer.status == 412 => Err(MemoryError::VersionMismatch {
                expected: Some(invocation.capability.when().clone()),
                actual: None,
            }),
            Outcome::Answer(answer) if answer.is_refusal() => Err(answer.refusal().into()),
            Outcome::Answer(answer) => Err(MemoryError::Storage(format!(
                "Failed to retract value: {}",
                answer.status
            ))),
        }
    }
}

/// Blob streams read and write ranges over a URL, which is what a permit
/// is for; they go through the permit-based site unchanged.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Read>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Read>,
    ) -> Result<BlobReader, BlobError> {
        through_permits(self, invocation).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Import>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Import>,
    ) -> Result<BlobWriter, BlobError> {
        through_permits(self, invocation).await
    }
}
