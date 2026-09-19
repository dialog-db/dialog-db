//! The blob effects, which answer with a transfer rather than a value.
//!
//! Every other effect is one request and one answer, and
//! [`perform`](super::provider) decodes that answer into the effect's
//! own output. A blob cannot work that way: `BlobReader` and
//! `BlobWriter` are handles over a live transfer, so what crosses the
//! wire is the bytes, and the handle is built on each side out of the
//! stream it already holds.
//!
//! # Which way the bytes go is the effect's business
//!
//! A read sends its container and nothing more, then reads until the
//! peer finishes. A write sends its container, then the blob, then
//! reads one answer. Both are the same stream opened the same way —
//! [`Channel::open`] leaves the send side open precisely so this module
//! can decide.
//!
//! # The digest comes from the peer, and is not taken on trust
//!
//! An ingest discovers its hash while writing, so only the peer can say
//! what it was. An import declares one up front, and the peer verifies
//! the bytes against it before committing — so
//! [`BlobAnswer::Written`](crate::wire::BlobAnswer::Written) is
//! believable because a peer that disagreed would have answered
//! [`BlobError::DigestMismatch`] instead, not because the client could
//! check it here.

use dialog_capability::access::AuthorizeError;
use dialog_capability::{ForkInvocation, Provider};
use dialog_common::Blake3Hash;
use dialog_effects::Rejection;
use dialog_effects::blob::{self, BlobError, BlobReader, BlobSink, BlobSource, BlobWriter};

use crate::channel::{ChannelError, Transfer};
use crate::site::{Iroh, IrohAddress};
use crate::wire::{BlobAnswer, Refusal, Response, decode, read_frame};

/// A transport failure is not the blob store failing.
///
/// It says nothing about whether the blob exists or was written, so it
/// must not read as either. [`Rejection::Unavailable`] is the one a
/// caller retries, which is the right advice when the exchange never
/// completed.
fn broken(error: ChannelError) -> BlobError {
    BlobError::Rejected(Rejection::Unavailable {
        reason: error.to_string(),
    })
}

/// Read the peer's framed answer and unwrap it to a [`BlobAnswer`].
async fn answered(transfer: &mut dyn Transfer) -> Result<BlobAnswer, BlobError> {
    let framed = read_frame("response", transfer).await.map_err(broken)?;

    let response: Response = decode("response", &framed).map_err(|error| {
        BlobError::Rejected(Rejection::Unclassified {
            detail: format!("the peer did not answer in this protocol: {error}"),
        })
    })?;

    let performed = match response {
        Response::Performed(output) => output,
        Response::Refused(refusal) => return Err(refused(refusal)),
    };

    decode::<Result<BlobAnswer, BlobError>>("blob answer", &performed).map_err(|error| {
        BlobError::Rejected(Rejection::Unclassified {
            detail: format!("the peer's answer did not fit the command: {error}"),
        })
    })?
}

/// The peer answered, but not the way this effect is answered.
///
/// Not a blob failure and not a refusal: the peer is speaking this
/// protocol and using it wrongly, which a caller can neither retry nor
/// re-authorize its way out of.
fn mismatched(expected: &str, got: BlobAnswer) -> BlobError {
    BlobError::Rejected(Rejection::Unclassified {
        detail: format!("the peer answered {expected} with {got:?}"),
    })
}

/// A peer's refusal in the caller's vocabulary, as in
/// [`super::provider`]: only an access decision reads as one.
fn refused(refusal: Refusal) -> BlobError {
    match refusal {
        Refusal::Unauthorized(reason) => BlobError::Authorization(AuthorizeError::Declined {
            recourse: dialog_capability::access::Recourse::None,
            reason,
        }),
        Refusal::UnknownCommand(command) => BlobError::Rejected(Rejection::Unclassified {
            detail: format!("the peer does not serve {command}"),
        }),
        Refusal::Malformed(detail) => BlobError::Rejected(Rejection::Unclassified {
            detail: format!("the peer could not read the request: {detail}"),
        }),
        Refusal::Internal(reason) => BlobError::Rejected(Rejection::Unavailable { reason }),
    }
}

/// Start the exchange and read the peer's opening answer.
async fn opened(
    site: &Iroh,
    address: &IrohAddress,
    container: Vec<u8>,
) -> Result<(Box<dyn Transfer>, BlobAnswer), BlobError> {
    let mut transfer = site
        .channel()
        .await
        .map_err(broken)?
        .open(address, container)
        .await
        .map_err(broken)?;
    let answer = answered(transfer.as_mut()).await?;
    Ok((transfer, answer))
}

/// The peer's blob, arriving in whatever chunks the network cut it into.
struct PeerBlobSource(Box<dyn Transfer>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSource for PeerBlobSource {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, BlobError> {
        self.0.recv().await.map_err(broken)
    }
}

/// Bytes on their way to a peer, which answers once they have all
/// arrived.
struct PeerBlobSink(Box<dyn Transfer>);

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl BlobSink for PeerBlobSink {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<(), BlobError> {
        self.0.send(bytes).await.map_err(broken)
    }

    async fn finish(mut self: Box<Self>) -> Result<Blake3Hash, BlobError> {
        // Finishing is what tells the peer the blob is whole; it will
        // not hash or commit anything until it sees that.
        self.0.finish().await.map_err(broken)?;

        match answered(self.0.as_mut()).await? {
            BlobAnswer::Written(digest) => Ok(Blake3Hash::from(digest)),
            other => Err(mismatched("a finished write", other)),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<ForkInvocation<Iroh, blob::Read>> for Iroh {
    async fn execute(
        &self,
        invocation: ForkInvocation<Iroh, blob::Read>,
    ) -> Result<BlobReader, BlobError> {
        let ForkInvocation {
            address,
            authorization,
            ..
        } = invocation;

        let (mut transfer, answer) = opened(self, &address, authorization.into_bytes()).await?;

        // Nothing follows a read's container, and the peer will not
        // start sending until this side says so.
        transfer.finish().await.map_err(broken)?;

        match answer {
            BlobAnswer::Reading => Ok(Box::new(PeerBlobSource(transfer))),
            other => Err(mismatched("a read", other)),
        }
    }
}

/// An ingest and an import differ only in what the peer checks, so they
/// send and receive identically and share this.
macro_rules! writes_a_blob {
    ($($effect:ty),+ $(,)?) => {
        $(
            #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
            #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
            impl Provider<ForkInvocation<Iroh, $effect>> for Iroh {
                async fn execute(
                    &self,
                    invocation: ForkInvocation<Iroh, $effect>,
                ) -> Result<BlobWriter, BlobError> {
                    let ForkInvocation {
                        address,
                        authorization,
                        ..
                    } = invocation;

                    // The send side stays open: the blob goes next.
                    let (transfer, answer) =
                        opened(self, &address, authorization.into_bytes()).await?;

                    match answer {
                        BlobAnswer::Accepted => Ok(Box::new(PeerBlobSink(transfer))),
                        other => Err(mismatched("a write", other)),
                    }
                }
            }
        )+
    };
}

writes_a_blob!(blob::Write, blob::Import);
