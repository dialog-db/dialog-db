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
    use dialog_capability::Effect;
    use dialog_effects::archive::Get;

    /// The command a peer dispatches on is the effect's own string,
    /// taken from the signed invocation rather than from a field beside
    /// it, so its dispatch table and its authorization check cannot
    /// disagree.
    #[dialog_common::test]
    fn the_command_is_the_effects_own() {
        assert_eq!(Get::command(), "get/archive/block");
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
