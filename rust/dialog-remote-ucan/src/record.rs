//! The record a cell written through an access service holds: the signed
//! invocation that wrote it, the delegations it proves with, and the
//! content it carries, so whoever reads the cell, from wherever, can tell
//! that someone the subject authorized wrote what it holds.
//!
//! # Layout
//!
//! ```text
//! "dialog-cell/1" frame frame …
//! frame = bijou64(codec) bijou64(length) bytes
//! ```
//!
//! Integers are [bijou64](https://crates.io/crates/bijoux), as the search
//! tree's are: an integer has one encoding, so a record has one byte form.
//! `codec` is the [multicodec](https://github.com/multiformats/multicodec)
//! code of the block's encoding: DAG-CBOR (`0x71`) for the invocation and
//! each delegation, raw (`0x55`) for the content, which the access service
//! carries without reading.
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
//! No signature covers a codec. It lets a reader that does not know this
//! layout tell how to read each block, and the reader that does refuses a
//! frame whose codec is not the one its reference implies, before reading
//! the block: a delegation's is the codec its CID names, the content's is
//! raw, as its checksum names bytes and not a value.

use std::collections::VecDeque;

use bijoux::{Decode as _, Encode as _};
use dialog_capability::access::AuthorizeError;
use dialog_capability::{Did, Policy as _, Provider};
use dialog_common::{Checksum, ConditionalSync};
use dialog_effects::memory::{self, Cell, Space};
use dialog_remote_ucan_s3::{FromUcanArgs as _, verify_invocation_at};
use dialog_ucan_core::cid::dagcbor_cid;
use dialog_ucan_core::{Container, Invocation, UnverifiedRevocations};
use dialog_varsig::AnySignature;
use ipld_core::cid::Cid;
use ipld_core::ipld::Ipld;
use thiserror::Error;

/// What a record's bytes begin with.
pub const MAGIC: &[u8] = b"dialog-cell/1";

/// The largest block a frame carries. A record holds a cell's content and
/// the tokens that wrote it, which are small; a length past this is not a
/// record, and reading it would only cost memory.
pub const MAX_BLOCK: u64 = 4 * 1024 * 1024;

/// The multihash code of SHA2-256, the hash a delegation's CID names.
const SHA2_256: u64 = 0x12;

/// The DAG-CBOR codec: an invocation's and a delegation's encoding.
pub const DAG_CBOR: u64 = 0x71;

/// The raw codec: the content's, which a record carries as bytes.
pub const RAW: u64 = 0x55;

/// What a block of a record is, as the invocation names it.
#[derive(Debug, Clone, PartialEq)]
pub enum Reference {
    /// A delegation the invocation proves with, by its CID.
    Delegation(Cid),
    /// The content the invocation wrote, by the checksum it signed.
    Content(Checksum),
}

impl Reference {
    /// The codec of the block this names.
    pub fn codec(&self) -> u64 {
        match self {
            Reference::Delegation(cid) => cid.codec(),
            Reference::Content(_) => RAW,
        }
    }

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
}

/// Why bytes are not a record.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum RecordError {
    /// The bytes do not begin as a record does.
    #[error("not a cell record")]
    NotARecord,
    /// A frame holds an integer past 64 bits.
    #[error("frame {frame} holds an integer past 64 bits")]
    Integer {
        /// Which frame.
        frame: usize,
    },
    /// A frame's codec is not the one the block it answers is in.
    #[error("frame {frame} is in codec 0x{codec:x}, not 0x{expected:x}")]
    Codec {
        /// Which frame.
        frame: usize,
        /// The codec it gives.
        codec: u64,
        /// The codec the block it answers is in.
        expected: u64,
    },
    /// A frame's length is past what a record carries.
    #[error("frame {frame} is {length} bytes, past the {MAX_BLOCK} a record carries")]
    TooLarge {
        /// Which frame.
        frame: usize,
        /// The length it gives.
        length: u64,
    },
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

/// Encode the record of `container`'s invocation writing `content`: what
/// a cell holds.
///
/// `content` is `None` for an invocation that carries none.
pub fn encode(container: &Container, content: Option<&[u8]>) -> Result<Vec<u8>, RecordError> {
    let tokens = container.tokens();
    let Some(invocation) = tokens.first() else {
        return Err(RecordError::Invocation("the container is empty".into()));
    };
    let mut out = Vec::with_capacity(MAGIC.len() + tokens.iter().map(Vec::len).sum::<usize>());
    out.extend_from_slice(MAGIC);
    carry(&mut out, DAG_CBOR, invocation);
    for reference in references(invocation)? {
        let block = match &reference {
            Reference::Delegation(cid) => tokens[1..]
                .iter()
                .find(|token| dagcbor_cid(token) == *cid)
                .map(Vec::as_slice),
            Reference::Content(_) => content,
        };
        match block {
            Some(block) if reference.names(block) => carry(&mut out, reference.codec(), block),
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

/// What a cell holds, read whole: the invocation that wrote it, with the
/// delegations it proves with, and the content it signed.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    container: Container,
    content: Option<Vec<u8>>,
}

impl Record {
    /// Whether `bytes` are a record, rather than what a cell held before
    /// cells held records: its content alone.
    pub fn is_record(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC)
    }

    /// Read a record a cell stores.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut tokens = Vec::new();
        let mut content = None;
        for frame in decode(bytes)? {
            match frame {
                Frame::Invocation(token) => tokens.push(token),
                Frame::Block {
                    reference: Reference::Delegation(_),
                    bytes,
                } => tokens.push(bytes),
                Frame::Block {
                    reference: Reference::Content(_),
                    bytes,
                } => content = Some(bytes),
            }
        }
        Ok(Self {
            container: Container::new(tokens),
            content,
        })
    }

    /// The invocation and the delegations it proves with, as a container
    /// to verify.
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// The content the invocation wrote, when it wrote any.
    pub fn content(&self) -> Option<&[u8]> {
        self.content.as_deref()
    }

    /// The content the invocation wrote, when it wrote any.
    pub fn into_content(self) -> Option<Vec<u8>> {
        self.content
    }

    /// Check that the record proves its content was written to the cell
    /// `cell` of the space `space` of `subject`, by someone `subject`
    /// authorized to.
    ///
    /// The chain is judged as of the time the invocation says it was
    /// issued, the only time the record carries: no one vouches for when
    /// it was presented, and authority that lapsed or was revoked since
    /// does not unmake a write made while it held. A revocation is not
    /// consulted, as a reader knows of none. An invocation that does not
    /// say when it was issued proves nothing.
    pub async fn verify<Resolver>(
        &self,
        subject: &Did,
        space: &str,
        cell: &str,
        resolver: &Resolver,
    ) -> Result<(), Unproven>
    where
        Resolver: Provider<dialog_did_web::Resolve> + ConditionalSync,
    {
        let token = &self.container.tokens()[0];
        let invocation: Invocation<AnySignature> = serde_ipld_dagcbor::from_slice(token)
            .map_err(|error| Unproven::Record(RecordError::Invocation(error.to_string())))?;
        let Some(issued) = invocation.issued_at() else {
            return Err(Unproven::Undated);
        };
        let chain = verify_invocation_at(
            self.container.clone(),
            resolver,
            &UnverifiedRevocations,
            issued,
        )
        .await
        .map_err(Unproven::Chain)?;
        let command: Vec<&str> = chain.command().0.iter().map(String::as_str).collect();
        if !matches!(
            command.as_slice(),
            ["use", "put", "memory", "cell"] | ["memory", "publish"]
        ) {
            return Err(Unproven::Command(command.join("/")));
        }
        let publish = memory::Publish::capability_from_args(chain.subject(), chain.arguments())
            .map_err(|error| Unproven::Record(RecordError::Invocation(error.to_string())))?;
        let written = (
            chain.subject(),
            Space::of(&publish).space.as_str(),
            Cell::of(&publish).cell.as_str(),
        );
        if written != (subject, space, cell) {
            return Err(Unproven::Elsewhere {
                expected: format!("{subject}/{space}/{cell}"),
                written: format!("{}/{}/{}", written.0, written.1, written.2),
            });
        }
        if self.content.is_none() {
            return Err(Unproven::Empty);
        }
        Ok(())
    }
}

/// Why a cell's record does not prove its content.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum Unproven {
    /// The bytes are not a record a cell stores.
    #[error(transparent)]
    Record(RecordError),
    /// The invocation does not say when it was issued, so there is no
    /// time to judge its chain at.
    #[error("the invocation does not say when it was issued")]
    Undated,
    /// The invocation's chain does not verify as of its issue time.
    #[error("the invocation's authority does not verify: {0}")]
    Chain(AuthorizeError),
    /// The invocation is not a publish.
    #[error("the invocation is /{0}, not a publish")]
    Command(String),
    /// The invocation publishes to another cell than the one read.
    #[error("the invocation publishes to {written}, not {expected}")]
    Elsewhere {
        /// The cell read.
        expected: String,
        /// The cell the invocation publishes to.
        written: String,
    },
    /// The record carries no content.
    #[error("the record carries no content")]
    Empty,
    /// The cell holds content and no record, as cells did before they
    /// held records, where a record is required.
    #[error("the cell holds content no record proves")]
    Unrecorded,
}

impl From<Unproven> for memory::MemoryError {
    fn from(unproven: Unproven) -> Self {
        memory::MemoryError::Unproven {
            reason: unproven.to_string(),
        }
    }
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
    ///
    /// A frame's codec, and whether it answers any reference, are checked
    /// as soon as they are read, before the block they head arrives.
    fn frame(&mut self) -> Result<Option<Frame>, RecordError> {
        let frame = self.frames;
        let mut at = 0;
        let Some(codec) = integer(&self.pending, &mut at, frame)? else {
            return Ok(None);
        };
        let reference = match &self.expected {
            None => None,
            Some(expected) => match expected.front() {
                Some(reference) => Some(reference.clone()),
                None => return Err(RecordError::Unexpected { frame }),
            },
        };
        let wanted = reference.as_ref().map_or(DAG_CBOR, Reference::codec);
        if codec != wanted {
            return Err(RecordError::Codec {
                frame,
                codec,
                expected: wanted,
            });
        }
        let Some(length) = integer(&self.pending, &mut at, frame)? else {
            return Ok(None);
        };
        if length > MAX_BLOCK {
            return Err(RecordError::TooLarge { frame, length });
        }
        let end = at + length as usize;
        if self.pending.len() < end {
            return Ok(None);
        }
        let bytes = self.pending[at..end].to_vec();
        self.pending.drain(..end);
        self.frames += 1;

        let Some(reference) = reference else {
            self.expected = Some(references(&bytes)?.into());
            return Ok(Some(Frame::Invocation(bytes)));
        };
        if let Some(expected) = self.expected.as_mut() {
            expected.pop_front();
        }
        if !reference.names(&bytes) {
            return Err(RecordError::Mismatch { frame, reference });
        }
        Ok(Some(Frame::Block { reference, bytes }))
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

/// Write `block`, in `codec`, as a frame.
fn carry(out: &mut Vec<u8>, codec: u64, block: &[u8]) {
    codec.encode(out);
    (block.len() as u64).encode(out);
    out.extend_from_slice(block);
}

/// The bijou64 integer at `at` in `bytes`, advancing past it, or `None`
/// when `bytes` end before it does.
fn integer(bytes: &[u8], at: &mut usize, frame: usize) -> Result<Option<u64>, RecordError> {
    match u64::decode(&bytes[*at..]) {
        Ok((value, read)) => {
            *at += read;
            Ok(Some(value))
        }
        Err(bijoux::u64::DecodeError::BufferTooShort) => Ok(None),
        Err(bijoux::u64::DecodeError::Overflow) => Err(RecordError::Integer { frame }),
    }
}

#[cfg(test)]
mod test;
