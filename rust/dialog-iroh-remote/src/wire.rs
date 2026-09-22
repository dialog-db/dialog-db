//! What travels on a stream.
//!
//! One exchange is one QUIC stream: the dialer writes a `ctn-v1` UCAN
//! container, finishes its side, and reads a [`Response`]. Streams are
//! cheap and independently cancellable, so there is no multiplexing,
//! framing or correlation id here — QUIC already supplies all three.
//!
//! # The request has no type of its own
//!
//! An earlier shape here was a struct carrying a command, a capability
//! payload and an authorization, which was a mistake worth recording:
//! it put the payload beside the signature rather than under it, so two
//! things claimed to be the invocation's arguments and only one of them
//! was signed. Keeping them honest then needed a comparison, and a
//! comparison is a thing that can be got wrong.
//!
//! The container already solves this. Its root token is the invocation —
//! which carries the command, the subject and the arguments, all signed —
//! its `prf` blocks are the proofs, and material that is neither proof
//! nor argument rides as its own block, addressed by the hash of its
//! bytes. So the request *is* the container, and [`crate::resolve`] is
//! how a peer reaches what it names.
//!
//! # Refusal is not failure
//!
//! `ArchiveError::NotFound` is an *answer*: the peer performed the
//! effect and that is what it found. "I do not know that command" is
//! not an answer, and collapsing the two would make a misconfigured
//! peer indistinguishable from an empty one — a client would treat a
//! protocol mismatch as missing data and happily push a repository into
//! a peer that never stored it. [`Response`] keeps them apart.

use serde::{Deserialize, Serialize};

use crate::channel::{ChannelError, Transfer};

/// What comes back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    /// The peer ran the effect. The bytes are its encoded `Fx::Output`,
    /// which is itself a `Result` — a failure *of the effect* lives in
    /// here, not in [`Response::Refused`].
    Performed(Vec<u8>),
    /// The peer never got as far as running it.
    Refused(Refusal),
}

/// What a streamed effect answers with, in place of its own output.
///
/// The blob effects return `BlobReader` and `BlobWriter` — trait objects
/// over a live transfer — so unlike every other effect there is nothing
/// of `Fx::Output` to encode. What crosses the wire is the part that is
/// actually information: that bytes are coming, or that the bytes sent
/// were committed and under which digest. The handle itself is built on
/// each side out of the stream it already holds.
///
/// It rides inside [`Response::Performed`] as an encoded
/// `Result<BlobAnswer, BlobError>`, so a blob store's own failure is
/// still the peer answering rather than refusing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobAnswer {
    /// The blob's bytes follow this frame, until the peer finishes.
    Reading,
    /// The invocation is good; send the blob.
    ///
    /// A write is answered twice, and this is the first. It exists
    /// because the alternative deadlocks: a peer cannot report a digest
    /// it has not computed, so if a client waited for its only answer
    /// before sending, each side would be waiting for the other. Saying
    /// so up front also means an unauthorized write is refused before a
    /// gigabyte travels rather than after.
    Accepted,
    /// Every byte sent was received and committed under this digest.
    ///
    /// For an ingest this is the hash the peer discovered; for an import
    /// it is the declared digest, which the peer verified before
    /// answering — so a client can trust it because the peer refused to
    /// say it otherwise, not because it sent it.
    Written([u8; 32]),
}

/// Why a peer would not perform an invocation.
///
/// Deliberately coarse. A peer talking to a stranger should not narrate
/// its internals, and every variant here is something the *client* can
/// act on: retry, re-authorize, upgrade, or give up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Refusal {
    /// The command is not one this peer serves. A client that sees this
    /// is talking to an older or narrower peer, not to an empty one.
    UnknownCommand(String),
    /// The frame or a payload did not decode against the named command.
    Malformed(String),
    /// The authorization did not cover the invocation.
    Unauthorized(String),
    /// The peer failed for a reason of its own.
    Internal(String),
}

/// Encoding failures, which are bugs rather than conditions.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// A value could not be encoded.
    #[error("could not encode {what}: {detail}")]
    Encode {
        /// What was being encoded.
        what: &'static str,
        /// The underlying reason.
        detail: String,
    },
    /// Bytes could not be decoded as the expected type.
    #[error("could not decode {what}: {detail}")]
    Decode {
        /// What was being decoded.
        what: &'static str,
        /// The underlying reason.
        detail: String,
    },
}

/// The largest header a framed exchange will read.
///
/// Only the container and the [`Response`] are framed — a blob's bytes
/// are not, and follow the frame rather than sitting inside it — so this
/// bounds a proof chain, not a payload. A peer is a stranger, and a
/// length it chose is how much memory this process reserves before it
/// has read anything.
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// A length-prefixed frame: four big-endian bytes, then the payload.
///
/// Every container and every [`Response`] is framed, including the ones
/// that have no body behind them. An earlier shape here framed only the
/// streamed exchanges and left a plain one as "the whole stream", which
/// is worth recording as a mistake: both kinds share a connection, and a
/// peer reads a stream *before* it knows which effect is on it, so there
/// was no point at which it could have chosen the right framing. The
/// uniform prefix is what makes the command — which only the verified
/// invocation inside the frame reveals — something the peer can learn
/// without having already guessed it.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(payload.len() + 4);
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(payload);
    framed
}

/// Read one frame from `transfer`.
///
/// Refuses a length past [`MAX_FRAME`] before reading a byte of it,
/// which is the point of checking here rather than after.
pub async fn read_frame(
    what: &'static str,
    transfer: &mut dyn Transfer,
) -> Result<Vec<u8>, ChannelError> {
    let prefix = transfer.read_exact(4).await?;
    let len = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;
    if len > MAX_FRAME {
        return Err(ChannelError::Unsupported {
            attempted: "read a frame",
            detail: format!("{what} claims {len} bytes, past the {MAX_FRAME} this reads"),
        });
    }
    transfer.read_exact(len).await
}

/// Encode a value for the wire.
pub fn encode<T: Serialize>(what: &'static str, value: &T) -> Result<Vec<u8>, CodecError> {
    serde_ipld_dagcbor::to_vec(value).map_err(|error| CodecError::Encode {
        what,
        detail: error.to_string(),
    })
}

/// Decode a value from the wire.
pub fn decode<T: serde::de::DeserializeOwned>(
    what: &'static str,
    bytes: &[u8],
) -> Result<T, CodecError> {
    serde_ipld_dagcbor::from_slice(bytes).map_err(|error| CodecError::Decode {
        what,
        detail: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_capability::{Subject, did};
    use dialog_effects::peer::{Hello, Peer, Spaces};
    use dialog_effects::prelude::*;

    /// The command a peer dispatches on is the chain's own ability,
    /// taken from the signed invocation rather than from a field beside
    /// it, so its dispatch table and its authorization check cannot
    /// disagree.
    #[dialog_common::test]
    fn the_command_is_the_chains_own() {
        let subject = Subject::from(did!("key:zSpace"));
        assert_eq!(
            subject
                .clone()
                .reader()
                .archive()
                .get(dialog_effects::archive::Blake3Hash::from([0u8; 32]))
                .ability(),
            "/use/get/archive/block"
        );
        assert_eq!(
            subject
                .clone()
                .reader()
                .attenuate(Peer)
                .attenuate(Hello)
                .ability(),
            "/use/get/peer"
        );
        assert_eq!(
            subject.reader().attenuate(Peer).attenuate(Spaces).ability(),
            "/use/get/peer/space"
        );
    }

    #[dialog_common::test]
    fn a_refusal_is_not_an_answer() {
        let refused = Response::Refused(Refusal::UnknownCommand("nope".into()));
        let bytes = encode("response", &refused).unwrap();
        let back: Response = decode("response", &bytes).unwrap();
        assert_eq!(back, refused);
        assert!(!matches!(back, Response::Performed(_)));
    }
}
