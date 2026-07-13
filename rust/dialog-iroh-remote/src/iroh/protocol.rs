//! Wire protocol for the iroh remote.
//!
//! ALPN [`ALPN`] (`dialog-db/remote/0`). One request per bidirectional QUIC
//! stream; a connection carries many streams. Frames are u32-BE
//! length-prefixed DAG-CBOR values, except blob chunk frames which are
//! length-prefixed raw bytes terminated by a zero-length frame.
//!
//! ```text
//! client → server   RequestEnvelope { invocation, body? }
//!                   (blob/import only: chunk frames … zero-length frame)
//! server → client   per-effect response frame (a WireResult)
//!                   (blob/read only: header frame, then chunk frames … zero-length frame)
//! ```
//!
//! The **invocation** is the CBOR UCAN container (invocation + delegation
//! proofs) — everything that was *signed*: subject, command path, and
//! attenuated arguments (digests, checksums, catalog/space/cell, CAS
//! preconditions). The **body** carries payload bytes that never travel
//! inside a capability (block content, publish content, import batches);
//! the server cross-checks them against the signed content bindings before
//! performing the effect.
//!
//! These types are pure data and compile on every target; the transport
//! that moves them is native-only.

use dialog_effects::memory::Version;
use serde::{Deserialize, Serialize};

/// ALPN identifier of the dialog-db remote protocol.
pub const ALPN: &[u8] = b"dialog-db/remote/0";

/// Upper bound on a single frame, protecting hosts from hostile lengths.
/// Blocks and memory cells are small; blob payloads stream in chunks.
pub const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

/// Preferred chunk size for blob streaming.
pub const BLOB_CHUNK_SIZE: usize = 1024 * 1024;

/// A single request as sent by the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestEnvelope {
    /// CBOR-encoded UCAN container (invocation + delegation chain).
    #[serde(with = "serde_bytes")]
    pub invocation: Vec<u8>,
    /// Effect-specific payload bytes:
    /// - `/archive/put`: the block content
    /// - `/archive/import`: CBOR list of block contents
    /// - `/memory/publish`: the cell content
    /// - everything else: absent
    #[serde(with = "serde_bytes", default, skip_serializing_if = "Option::is_none")]
    pub body: Option<Vec<u8>>,
}

/// A structured error crossing the wire.
///
/// Structure is preserved where the caller depends on it — CAS mismatches
/// must surface as [`MemoryError::VersionMismatch`](dialog_effects::memory::MemoryError)
/// for push conflict detection — and stringly-typed elsewhere.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireError {
    /// The invocation failed verification or is not authorized.
    Denied(String),
    /// The request is malformed or targets a subject this peer does not serve.
    Rejected(String),
    /// The effect failed during execution on the peer.
    Execution(String),
    /// Memory CAS precondition failed.
    VersionMismatch {
        /// The version the request expected.
        expected: Option<Version>,
        /// The version the peer holds.
        actual: Option<Version>,
    },
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Denied(e) => write!(f, "denied: {e}"),
            WireError::Rejected(e) => write!(f, "rejected: {e}"),
            WireError::Execution(e) => write!(f, "execution failed: {e}"),
            WireError::VersionMismatch { expected, actual } => {
                write!(f, "version mismatch: expected {expected:?}, got {actual:?}")
            }
        }
    }
}

/// Result alias for response frames.
pub type WireResult<T> = Result<T, WireError>;

/// Response to `/archive/get`: the block bytes, if the peer has them.
pub type GetResponse = WireResult<Option<serde_bytes::ByteBuf>>;

/// Response to `/archive/put`, `/archive/import`, and `/memory/retract`.
pub type UnitResponse = WireResult<()>;

/// Response to `/memory/publish`: the new version.
pub type PublishResponse = WireResult<Version>;

/// A memory cell edition in wire form (compact byte encoding).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireEdition {
    /// The cell content.
    #[serde(with = "serde_bytes")]
    pub content: Vec<u8>,
    /// The content's version.
    pub version: Version,
}

/// Response to `/memory/resolve`.
pub type ResolveResponse = WireResult<Option<WireEdition>>;

/// Header frame preceding the chunk stream of a `/archive/blob/read`.
pub type BlobReadResponse = WireResult<()>;

/// Final frame of a `/archive/blob/import`: the digest of the imported blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobImported {
    /// blake3 digest of the committed blob.
    #[serde(with = "serde_bytes")]
    pub digest: Vec<u8>,
}

/// Response terminating a `/archive/blob/import`.
pub type BlobImportResponse = WireResult<BlobImported>;

/// Gossip swarm message, broadcast on a space's topic.
///
/// Messages are advisory only: no payload bytes and no authority travel
/// over the gossip overlay. Block transfer always happens over the direct
/// remote protocol, where the regular UCAN invocation is presented and
/// verified.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SwarmMessage {
    /// "Who has this block?" — broadcast by a peer whose read missed both
    /// locally and on its addressed remote.
    Want {
        /// The archive catalog the block lives in.
        catalog: String,
        /// blake3 digest of the wanted block.
        #[serde(with = "serde_bytes")]
        digest: Vec<u8>,
    },
    /// "I do." — includes the responder's dialable address so the requester
    /// can connect without a discovery round trip.
    Have {
        /// The archive catalog the block lives in.
        catalog: String,
        /// blake3 digest of the offered block.
        #[serde(with = "serde_bytes")]
        digest: Vec<u8>,
        /// Where to fetch it: the responder's own address.
        provider: crate::IrohAddress,
    },
    /// A new head revision was published to a memory cell (advisory;
    /// enables reactive pull in a future subscription layer).
    Announce {
        /// The memory cell that changed.
        cell: String,
        /// The new version of the cell.
        version: Version,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_roundtrips_request_envelope_through_dag_cbor() {
        let envelope = RequestEnvelope {
            invocation: vec![1, 2, 3],
            body: Some(vec![4, 5, 6]),
        };
        let bytes = serde_ipld_dagcbor::to_vec(&envelope).unwrap();
        let parsed: RequestEnvelope = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
        assert_eq!(parsed.invocation, vec![1, 2, 3]);
        assert_eq!(parsed.body, Some(vec![4, 5, 6]));
    }

    #[dialog_common::test]
    fn it_roundtrips_version_mismatch() {
        let error = WireError::VersionMismatch {
            expected: Some(Version::from("abc")),
            actual: None,
        };
        let bytes = serde_ipld_dagcbor::to_vec(&error).unwrap();
        let parsed: WireError = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
        match parsed {
            WireError::VersionMismatch { expected, actual } => {
                assert_eq!(expected, Some(Version::from("abc")));
                assert_eq!(actual, None);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[dialog_common::test]
    fn it_roundtrips_swarm_messages() {
        let message = SwarmMessage::Have {
            catalog: "index".into(),
            digest: vec![7; 32],
            provider: crate::IrohAddress::new("endpoint-id").with_direct_address("127.0.0.1:4433"),
        };
        let bytes = serde_ipld_dagcbor::to_vec(&message).unwrap();
        let parsed: SwarmMessage = serde_ipld_dagcbor::from_slice(&bytes).unwrap();
        match parsed {
            SwarmMessage::Have {
                catalog,
                digest,
                provider,
            } => {
                assert_eq!(catalog, "index");
                assert_eq!(digest, vec![7; 32]);
                assert_eq!(provider.endpoint(), "endpoint-id");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
