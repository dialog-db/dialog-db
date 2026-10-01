//! `Provider<ForkInvocation<UcanSite, Fx>>` for [`UcanSite`], one impl
//! per effect the access service performs.
//!
//! An address names the exchange to speak. The direct exchange runs
//! [`direct::invoke`] and reads the answer the way the object route's
//! client would have: the statuses, the `ETag`, the body. A cell's
//! invocation goes over the service's socket instead when the address
//! names one, answered with the same status, version and body. The permit
//! exchange hands the fork to the permit-based site, which redeems and
//! performs it the way it always has.

use base58::ToBase58;
use dialog_capability::{Constraint, Effect, ForkInvocation, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::Rejection;
use dialog_effects::archive::prelude::PutExt;
use dialog_effects::archive::{ArchiveError, Get, Put};
use dialog_effects::blob::prelude::{BlobImportExt as _, BlobReadExt as _};
use dialog_effects::blob::{BlobError, BlobReader, BlobSink, BlobWriter, Import, Read};
use dialog_effects::memory::prelude::{PublishExt, RetractExt};
use dialog_effects::memory::{
    Edition, Editions, MemoryError, Publish, Resolve, Retract, Version, Watch,
};
use dialog_remote_s3::S3Error;
use dialog_remote_ucan_s3::UcanAuthorization;
use dialog_remote_ucan_s3::UcanSite as PermitSite;
use dialog_ucan_core::Container;

use crate::address::{Exchange, UcanAddress};
use crate::direct;
use crate::site::UcanSite;
use crate::socket::Reply;

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
        if invocation.address.exchange() == Exchange::Permit {
            return through_permits(self, invocation).await;
        }
        let answer = direct::invoke(&invocation.address, &invocation.authorization, None).await?;
        match answer {
            answer if answer.is_success() => Ok(Some(answer.bytes().await?)),
            answer if answer.status == 404 => Ok(None),
            answer if answer.is_refusal() => Err(answer.refusal().await.into()),
            answer => Err(ArchiveError::Storage(format!(
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
        if invocation.address.exchange() == Exchange::Permit {
            return through_permits(self, invocation).await;
        }
        let payload = invocation.capability.content().to_vec();
        let answer = direct::invoke(
            &invocation.address,
            &invocation.authorization,
            Some(payload),
        )
        .await?;
        match answer {
            answer if answer.is_success() => Ok(()),
            answer if answer.is_refusal() => Err(answer.refusal().await.into()),
            answer => Err(ArchiveError::Storage(format!(
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
        if invocation.address.exchange() == Exchange::Permit {
            return through_permits(self, invocation).await;
        }
        let answer = exchange(self, &invocation.address, &invocation.authorization, None).await?;
        match answer {
            answer if answer.is_success() => {
                let version = Version::from(answer.version()?);
                Ok(Some(Edition {
                    content: answer.bytes().await?,
                    version,
                }))
            }
            answer if answer.status == 404 => Ok(None),
            answer if answer.is_refusal() => Err(answer.refusal().await.into()),
            answer => Err(MemoryError::Storage(format!(
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
        if invocation.address.exchange() == Exchange::Permit {
            return through_permits(self, invocation).await;
        }
        let payload = invocation.capability.content().to_vec();
        let answer = exchange(
            self,
            &invocation.address,
            &invocation.authorization,
            Some(payload),
        )
        .await?;
        match answer {
            answer if answer.is_success() => Ok(Version::from(answer.version()?)),
            answer if answer.status == 412 => Err(MemoryError::VersionMismatch {
                expected: invocation.capability.when().cloned(),
                actual: None,
            }),
            answer if answer.is_refusal() => Err(answer.refusal().await.into()),
            answer => Err(MemoryError::Storage(format!(
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
        if invocation.address.exchange() == Exchange::Permit {
            return through_permits(self, invocation).await;
        }
        let answer = exchange(self, &invocation.address, &invocation.authorization, None).await?;
        match answer {
            answer if answer.is_success() => Ok(()),
            answer if answer.status == 412 => Err(MemoryError::VersionMismatch {
                expected: Some(invocation.capability.when().clone()),
                actual: None,
            }),
            answer if answer.is_refusal() => Err(answer.refusal().await.into()),
            answer => Err(MemoryError::Storage(format!(
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
        if invocation.address.exchange() == Exchange::Permit {
            return through_permits(self, invocation).await;
        }
        let answer = direct::invoke(&invocation.address, &invocation.authorization, None).await?;
        match answer {
            answer if answer.is_success() => Ok(answer.source()),
            answer if answer.status == 404 => Err(BlobError::NotFound(
                invocation.capability.digest().as_bytes().to_base58(),
            )),
            answer if answer.is_refusal() => Err(answer.refusal().await.into()),
            answer => Err(BlobError::Storage(format!(
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
        if invocation.address.exchange() == Exchange::Permit {
            return through_permits(self, invocation).await;
        }
        Ok(Box::new(Upload {
            invocation,
            buffer: Vec::new(),
        }))
    }
}

/// Gathers a blob's bytes and sends them, in the request that proves
/// the import, once they have been checked against the declared digest.
struct Upload {
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
        let Upload { invocation, buffer } = *self;
        let expected = invocation.capability.digest().clone();
        let hash = Blake3Hash::hash(&buffer);
        if hash != expected {
            return Err(BlobError::DigestMismatch {
                expected: expected.as_bytes().to_base58(),
                actual: hash.as_bytes().to_base58(),
            });
        }
        let answer =
            direct::invoke(&invocation.address, &invocation.authorization, Some(buffer)).await?;
        match answer {
            answer if answer.is_success() => Ok(hash),
            answer if answer.is_refusal() => Err(answer.refusal().await.into()),
            answer => Err(BlobError::Storage(format!(
                "blob import failed: {}",
                answer.status
            ))),
        }
    }
}

/// A watch rides the service's socket, the one connection to it shared by
/// every watch of a cell in the same space: a service that names no
/// socket cannot follow a cell, and a watch there is refused, so the cell
/// is read by resolving it again.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<UcanSite, Watch>> for UcanSite {
    async fn execute(
        &self,
        invocation: ForkInvocation<UcanSite, Watch>,
    ) -> Result<Editions, MemoryError> {
        let Some(socket) = invocation.address.socket() else {
            return Err(Rejection::Unsupported {
                reason: "the access service names no socket".into(),
            }
            .into());
        };
        let url = per_space(socket, invocation.capability.subject().as_ref());
        let chain = invocation.authorization.invocation().chain();
        let container = Container::from(chain)
            .to_bytes()
            .map_err(|error| MemoryError::Storage(error.to_string()))?;
        let name = chain.invocation.to_cid().to_string();
        let connection = self.sockets().connect(&url).await?;
        Ok(Box::new(connection.watch(name, container)?))
    }
}

/// Carry a cell's invocation to the service: over its socket when the
/// address names one, as a request otherwise.
///
/// A socket that cannot be reached is passed over for a request. One that
/// fails once the frame is sent is not: the operation may have been
/// carried out, and the same invocation sent again would be refused as
/// presented before, so the failure is answered as it is.
async fn exchange(
    site: &UcanSite,
    address: &UcanAddress,
    authorization: &UcanAuthorization,
    payload: Option<Vec<u8>>,
) -> Result<direct::Answer, S3Error> {
    let Some(socket) = address.socket() else {
        return direct::invoke(address, authorization, payload).await;
    };
    let chain = authorization.invocation().chain();
    let Ok(connection) = site
        .sockets()
        .connect(&per_space(socket, chain.subject().as_str()))
        .await
    else {
        return direct::invoke(address, authorization, payload).await;
    };
    let container = Container::from(chain)
        .to_bytes()
        .map_err(|error| S3Error::Serialization(error.to_string()))?;
    let name = chain.invocation.to_cid().to_string();
    match connection.invoke(name, container, payload).await {
        Ok(Reply::Answer {
            status,
            version,
            body,
            ..
        }) => Ok(direct::Answer::framed(status, version, body)),
        Ok(other) => Err(S3Error::Rejected(Rejection::Unclassified {
            detail: format!("the service answered an invocation with {other:?}"),
        })),
        Err(error) => Err(S3Error::Rejected(Rejection::Unclassified {
            detail: error.to_string(),
        })),
    }
}

/// The socket of the space `subject` names: a service answers each space
/// on a connection of its own.
fn per_space(socket: &str, subject: &str) -> String {
    let separator = if socket.contains('?') { '&' } else { '?' };
    format!("{socket}{separator}sub={subject}")
}
