//! The record a cell written through an access service holds: the signed
//! invocation that wrote it, the delegations it proves with, and the
//! content it carries, so whoever reads the cell, from wherever, can tell
//! that someone the subject authorized wrote what it holds.
//!
//! # Layout
//!
//! ```text
//! "dialog-cell/1" frame frame …
//! frame = 0x00 varint(length) bytes     ; a block, carried
//!       | 0x01                          ; a block, elided: the reader holds it
//! ```
//!
//! The first frame is the invocation's token, always carried. It names
//! everything else the record holds, and in what order: each delegation
//! its `prf` lists, once and in that order, then the content its
//! `checksum` argument signed, when it has one. Each later frame is the
//! block for the next of those references.
//!
//! Nothing in the stream claims an address. A frame's position says which
//! reference it answers, and the reader hashes it to check: a delegation
//! by its DAG-CBOR/SHA2-256 CID, the content by its SHA-256. A frame that
//! does not hash to the reference it answers, one past the last
//! reference, or a stream that ends before the last, refuses the record.
//! So the record carries nothing unaccounted for, and a reader verifies
//! each block as it arrives, without waiting for the rest.
//!
//! A block may be elided, in one byte, where the reader is known to hold
//! it already (a delegation an earlier record on the same connection
//! carried, say): its position names the reference, which the reader
//! resolves from what it holds. A cell is stored with every block carried,
//! so its bytes, and the version a store computes from them, are the same
//! whoever reads it; eliding is for carrying a record, not storing one.

use std::collections::VecDeque;

use dialog_common::Checksum;
use dialog_ucan_core::cid::dagcbor_cid;
use dialog_ucan_core::{Container, Invocation};
use dialog_varsig::AnySignature;
use ipld_core::cid::Cid;
use ipld_core::ipld::Ipld;
use thiserror::Error;

/// What a record's bytes begin with.
pub const MAGIC: &[u8] = b"dialog-cell/1";

/// A frame that carries its block.
const CARRIED: u8 = 0x00;

/// A frame that elides its block.
const ELIDED: u8 = 0x01;

/// The largest block a frame carries. A record holds a cell's content and
/// the tokens that wrote it, which are small; a length past this is not a
/// record, and reading it would only cost memory.
pub const MAX_BLOCK: u64 = 4 * 1024 * 1024;

/// The multihash code of SHA2-256, the hash a delegation's CID names.
const SHA2_256: u64 = 0x12;

/// The DAG-CBOR codec, which a delegation's CID names.
const DAG_CBOR: u64 = 0x71;

/// What a block of a record is, as the invocation names it.
#[derive(Debug, Clone, PartialEq)]
pub enum Reference {
    /// A delegation the invocation proves with, by its CID.
    Delegation(Cid),
    /// The content the invocation wrote, by the checksum it signed.
    Content(Checksum),
}

impl Reference {
    /// Whether `bytes` are the block this names.
    fn names(&self, bytes: &[u8]) -> bool {
        match self {
            Reference::Delegation(cid) => {
                cid.codec() == DAG_CBOR
                    && cid.hash().code() == SHA2_256
                    && dagcbor_cid(bytes) == *cid
            }
            Reference::Content(checksum) => Checksum::sha256(bytes) == *checksum,
        }
    }
}

/// One frame of a record, read and checked.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// The invocation's token: the record's first frame.
    Invocation(Vec<u8>),
    /// A block the record carries, which hashes to the reference it
    /// answers.
    Block {
        /// What the block is.
        reference: Reference,
        /// The block.
        bytes: Vec<u8>,
    },
    /// A block the record elides, for the reader to resolve from what it
    /// holds.
    Elided(Reference),
}

/// Why bytes are not a record.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum RecordError {
    /// The bytes do not begin as a record does.
    #[error("not a cell record")]
    NotARecord,
    /// A frame begins with a byte that is not a frame's.
    #[error("frame {frame} begins with 0x{tag:02x}, not a frame's")]
    Frame {
        /// Which frame.
        frame: usize,
        /// The byte it begins with.
        tag: u8,
    },
    /// A frame's length is past what a record carries.
    #[error("frame {frame} is {length} bytes, past the {MAX_BLOCK} a record carries")]
    TooLarge {
        /// Which frame.
        frame: usize,
        /// The length it gives.
        length: u64,
    },
    /// The first frame elides the invocation, which a record always
    /// carries.
    #[error("the invocation is elided")]
    InvocationElided,
    /// The first frame is not an invocation's token.
    #[error("the first frame is not an invocation: {0}")]
    Invocation(String),
    /// The invocation's checksum argument is not a checksum.
    #[error("the invocation's checksum is not one: {0}")]
    Checksum(String),
    /// A frame does not hash to the reference it answers.
    #[error("frame {frame} is not the block {reference:?}")]
    Mismatch {
        /// Which frame.
        frame: usize,
        /// The reference it answers.
        reference: Reference,
    },
    /// A frame follows the last reference the invocation names.
    #[error("frame {frame} answers no reference the invocation names")]
    Unexpected {
        /// Which frame.
        frame: usize,
    },
    /// The bytes end before the record does.
    #[error("the record ends before {0:?}")]
    Truncated(Option<Reference>),
}

/// Encode the record of `container`'s invocation writing `content`, with
/// every block carried: what a cell holds.
///
/// `content` is `None` for an invocation that carries none.
pub fn encode(container: &Container, content: Option<&[u8]>) -> Result<Vec<u8>, RecordError> {
    encode_eliding(container, content, |_| false)
}

/// Encode a record as [`encode`] does, eliding each block `held` says the
/// reader holds.
pub fn encode_eliding(
    container: &Container,
    content: Option<&[u8]>,
    held: impl Fn(&Reference) -> bool,
) -> Result<Vec<u8>, RecordError> {
    let tokens = container.tokens();
    let Some(invocation) = tokens.first() else {
        return Err(RecordError::Invocation("the container is empty".into()));
    };
    let mut out = Vec::with_capacity(MAGIC.len() + tokens.iter().map(Vec::len).sum::<usize>());
    out.extend_from_slice(MAGIC);
    carry(&mut out, invocation);
    for reference in references(invocation)? {
        if held(&reference) {
            out.push(ELIDED);
            continue;
        }
        let block = match &reference {
            Reference::Delegation(cid) => tokens[1..]
                .iter()
                .find(|token| dagcbor_cid(token) == *cid)
                .map(Vec::as_slice),
            Reference::Content(_) => content,
        };
        match block {
            Some(block) if reference.names(block) => carry(&mut out, block),
            _ => return Err(RecordError::Truncated(Some(reference))),
        }
    }
    Ok(out)
}

/// Read a whole record, frame by frame.
pub fn decode(bytes: &[u8]) -> Result<Vec<Frame>, RecordError> {
    let mut decoder = Decoder::new();
    let mut frames = decoder.push(bytes)?;
    frames.extend(decoder.finish()?);
    Ok(frames)
}

/// A record read as its bytes arrive: each frame is checked and handed on
/// as soon as it is whole.
#[derive(Debug, Default)]
pub struct Decoder {
    /// Bytes received and not yet read into a frame.
    pending: Vec<u8>,
    /// Whether the magic has been read.
    begun: bool,
    /// How many frames have been read.
    frames: usize,
    /// The references still to be answered, once the invocation is read.
    expected: Option<VecDeque<Reference>>,
}

impl Decoder {
    /// A decoder at the start of a record.
    pub fn new() -> Self {
        Self::default()
    }

    /// Read `bytes`, answering the frames they complete.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, RecordError> {
        self.pending.extend_from_slice(bytes);
        let mut frames = Vec::new();
        if !self.begun {
            let have = self.pending.len().min(MAGIC.len());
            if self.pending[..have] != MAGIC[..have] {
                return Err(RecordError::NotARecord);
            }
            if have < MAGIC.len() {
                return Ok(frames);
            }
            self.pending.drain(..MAGIC.len());
            self.begun = true;
        }
        while let Some(frame) = self.frame()? {
            frames.push(frame);
        }
        Ok(frames)
    }

    /// The end of the bytes: whatever the record still lacks refuses it.
    pub fn finish(self) -> Result<Vec<Frame>, RecordError> {
        if !self.begun {
            return Err(RecordError::NotARecord);
        }
        let Some(mut expected) = self.expected else {
            return Err(RecordError::Truncated(None));
        };
        if !self.pending.is_empty() || !expected.is_empty() {
            return Err(RecordError::Truncated(expected.pop_front()));
        }
        Ok(Vec::new())
    }

    /// The next whole frame in what is pending, when there is one.
    fn frame(&mut self) -> Result<Option<Frame>, RecordError> {
        let frame = self.frames;
        let Some(&tag) = self.pending.first() else {
            return Ok(None);
        };
        let carried = match tag {
            CARRIED => {
                let mut at = 1;
                let Some(length) = varint(&self.pending, &mut at) else {
                    return Ok(None);
                };
                if length > MAX_BLOCK {
                    return Err(RecordError::TooLarge { frame, length });
                }
                let end = at + length as usize;
                if self.pending.len() < end {
                    return Ok(None);
                }
                let block = self.pending[at..end].to_vec();
                self.pending.drain(..end);
                Some(block)
            }
            ELIDED => {
                self.pending.drain(..1);
                None
            }
            tag => return Err(RecordError::Frame { frame, tag }),
        };
        self.frames += 1;

        let Some(expected) = self.expected.as_mut() else {
            let Some(invocation) = carried else {
                return Err(RecordError::InvocationElided);
            };
            self.expected = Some(references(&invocation)?.into());
            return Ok(Some(Frame::Invocation(invocation)));
        };
        let Some(reference) = expected.pop_front() else {
            return Err(RecordError::Unexpected { frame });
        };
        Ok(Some(match carried {
            Some(bytes) if reference.names(&bytes) => Frame::Block { reference, bytes },
            Some(_) => return Err(RecordError::Mismatch { frame, reference }),
            None => Frame::Elided(reference),
        }))
    }
}

/// What the invocation token `bytes` names, in the order a record answers
/// it: each delegation its `prf` lists, once, then the content its
/// `checksum` argument signed, when it has one.
fn references(bytes: &[u8]) -> Result<Vec<Reference>, RecordError> {
    let invocation: Invocation<AnySignature> = serde_ipld_dagcbor::from_slice(bytes)
        .map_err(|error| RecordError::Invocation(error.to_string()))?;
    let mut references: Vec<Reference> = Vec::new();
    for cid in invocation.proofs() {
        let reference = Reference::Delegation(*cid);
        if !references.contains(&reference) {
            references.push(reference);
        }
    }
    if let Some(checksum) = invocation.arguments().get("checksum") {
        let Ok(Ipld::Bytes(checksum)) = Ipld::try_from(checksum) else {
            return Err(RecordError::Checksum("it is not bytes".into()));
        };
        let checksum = Checksum::try_from(checksum).map_err(RecordError::Checksum)?;
        references.push(Reference::Content(checksum));
    }
    Ok(references)
}

/// Write `block` as a carried frame.
fn carry(out: &mut Vec<u8>, block: &[u8]) {
    out.push(CARRIED);
    let mut length = block.len() as u64;
    loop {
        let byte = (length & 0x7f) as u8;
        length >>= 7;
        if length == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
    out.extend_from_slice(block);
}

/// The unsigned LEB128 varint at `at` in `bytes`, advancing past it, or
/// `None` when `bytes` end before it does. One that runs past 64 bits
/// reads as a length past any block.
fn varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value: u64 = 0;
    let mut shift = 0;
    loop {
        let byte = *bytes.get(*at)?;
        *at += 1;
        if shift >= 64 {
            return Some(u64::MAX);
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
    }
}

#[cfg(test)]
mod test;
