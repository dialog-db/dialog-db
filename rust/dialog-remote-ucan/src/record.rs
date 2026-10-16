//! The record a cell written through an access service holds: the signed
//! invocation that wrote it, the delegations it proves with, and the
//! content it carries, so whoever reads the cell, from wherever, can tell
//! that someone the subject authorized wrote what it holds.
//!
//! # Layout
//!
//! A record is a [CARv1](https://ipld.io/specs/transport/car/carv1/):
//!
//! ```text
//! varint | {version: 1, roots: [invocation]}
//! varint | CID | the invocation's token
//! varint | CID | a delegation           ; each its prf lists, once, in order
//! varint | CID | the content            ; when its checksum signed one
//! ```
//!
//! Its one root is the invocation, which names everything else the record
//! holds. Each block's CID names its codec: DAG-CBOR for the tokens, raw
//! for the content, which the access service carries without reading. The
//! content's CID is SHA2-256, the digest the invocation's `checksum`
//! signed, so it is addressed by what was signed.
//!
//! CARv1 leaves the order of blocks, duplicates, extra blocks and a
//! canonical form open, and a record closes each: its blocks come in the
//! order above and nothing else. A block whose CID is not the next
//! reference's or whose bytes do not hash to it, one past the last
//! reference, or a record that ends before the last refuses the record.
//! Varints are minimal and the header is exactly `{version, roots}`, so a
//! record has one byte form, and its bytes, and the version a store
//! computes from them, are the same for every reader. The header holds
//! nothing else: CARv1 defines no other field, and `@ipld/car` refuses a
//! header that has one.

use std::collections::VecDeque;

use dialog_capability::access::AuthorizeError;
use dialog_capability::{Did, Policy as _, Provider};
use dialog_common::{Checksum, ConditionalSync};
use dialog_effects::memory::{self, Cell, Space};
use dialog_remote_ucan_s3::{FromUcanArgs as _, verify_invocation_at};
use dialog_ucan_core::cid::dagcbor_cid;
use dialog_ucan_core::{Container, Invocation, UnverifiedRevocations};
use dialog_varsig::AnySignature;
use ipld_core::cid::Cid;
use ipld_core::cid::multihash::Multihash;
use ipld_core::ipld::Ipld;
use thiserror::Error;

/// The media type a record's bytes are.
pub const MEDIA_TYPE: &str = "application/vnd.ipld.car";

/// The largest section a record holds. A record holds a cell's content
/// and the tokens that wrote it, which are small; a length past this is
/// not a record, and reading it would only cost memory.
pub const MAX_BLOCK: u64 = 4 * 1024 * 1024;

/// The multihash code of SHA2-256, the hash every CID in a record names.
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
    /// The CID a record addresses this block by.
    pub fn cid(&self) -> Cid {
        match self {
            Reference::Delegation(cid) => *cid,
            Reference::Content(Checksum::Sha256(digest)) => {
                #[allow(clippy::expect_used)]
                let multihash =
                    Multihash::wrap(SHA2_256, digest).expect("a SHA2-256 digest fits a multihash");
                Cid::new_v1(RAW, multihash)
            }
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

/// One block of a record, read and checked.
#[derive(Debug, Clone, PartialEq)]
pub enum Section {
    /// The invocation's token: the record's root and first block.
    Invocation(Vec<u8>),
    /// A block the invocation names, which hashes to the reference it
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
    /// The bytes do not begin with a CARv1 header.
    #[error("not a cell record: {0}")]
    NotARecord(String),
    /// The header names other than one root.
    #[error("the record names {0} roots, not one")]
    Roots(usize),
    /// The header is not exactly `{version: 1, roots}` in its one byte
    /// form.
    #[error("the record's header is not in its canonical form")]
    Header,
    /// A varint is not minimal, or runs past 64 bits.
    #[error("the varint at byte {at} is not a minimal one")]
    Varint {
        /// Where it begins.
        at: usize,
    },
    /// A section's length is past what a record holds.
    #[error("section {section} is {length} bytes, past the {MAX_BLOCK} a record holds")]
    TooLarge {
        /// Which section.
        section: usize,
        /// The length it gives.
        length: u64,
    },
    /// A section does not begin with a CID.
    #[error("section {section} does not begin with a CID: {reason}")]
    Cid {
        /// Which section.
        section: usize,
        /// Why.
        reason: String,
    },
    /// The first block is not the root, or does not hash to it.
    #[error("the first block is not the record's root")]
    Root,
    /// The root is not an invocation's token.
    #[error("the root is not an invocation: {0}")]
    Invocation(String),
    /// The invocation's checksum argument is not a checksum.
    #[error("the invocation's checksum is not one: {0}")]
    Checksum(String),
    /// A section's CID is not the reference it answers, or its block does
    /// not hash to it.
    #[error("section {section} is not the block {reference:?}")]
    Mismatch {
        /// Which section.
        section: usize,
        /// The reference it answers.
        reference: Reference,
    },
    /// A section follows the last reference the invocation names.
    #[error("section {section} answers no reference the invocation names")]
    Unexpected {
        /// Which section.
        section: usize,
    },
    /// The bytes end before the record does.
    #[error("the record ends before {0:?}")]
    Truncated(Option<Reference>),
}

/// The CARv1 header: `{version: 1, roots: [...]}`.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CarHeader {
    version: u64,
    roots: Vec<Cid>,
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
    let root = dagcbor_cid(invocation);
    let mut out =
        Vec::with_capacity(64 + tokens.iter().map(|token| token.len() + 40).sum::<usize>());
    put_varint(&mut out, header(root).len() as u64);
    out.extend_from_slice(&header(root));
    section(&mut out, &root, invocation);
    for reference in references(invocation)? {
        let block = match &reference {
            Reference::Delegation(cid) => tokens[1..]
                .iter()
                .find(|token| dagcbor_cid(token) == *cid)
                .map(Vec::as_slice),
            Reference::Content(_) => content,
        };
        match block {
            Some(block) if reference.names(block) => section(&mut out, &reference.cid(), block),
            _ => return Err(RecordError::Truncated(Some(reference))),
        }
    }
    Ok(out)
}

/// Read a record, block by block, checking each against what the
/// invocation names.
pub fn decode(bytes: &[u8]) -> Result<Vec<Section>, RecordError> {
    let mut at = 0;
    let root = read_header(bytes, &mut at)?;
    let mut sections = Vec::new();
    let mut expected: Option<VecDeque<Reference>> = None;
    while at < bytes.len() {
        let index = sections.len();
        let Some((cid, block)) = read_section(bytes, &mut at, index)? else {
            return Err(RecordError::Truncated(
                expected.and_then(|mut expected| expected.pop_front()),
            ));
        };
        let Some(expected) = expected.as_mut() else {
            if cid != root || dagcbor_cid(block) != root {
                return Err(RecordError::Root);
            }
            expected = Some(references(block)?.into());
            sections.push(Section::Invocation(block.to_vec()));
            continue;
        };
        let Some(reference) = expected.pop_front() else {
            return Err(RecordError::Unexpected { section: index });
        };
        if cid != reference.cid() || !reference.names(block) {
            return Err(RecordError::Mismatch {
                section: index,
                reference,
            });
        }
        sections.push(Section::Block {
            reference,
            bytes: block.to_vec(),
        });
    }
    match expected {
        None => Err(RecordError::Truncated(None)),
        Some(mut expected) if !expected.is_empty() => {
            Err(RecordError::Truncated(expected.pop_front()))
        }
        Some(_) => Ok(sections),
    }
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
    /// cells held records: its content alone. Bytes that begin with a
    /// record's header are a record, to be read whole or refused.
    pub fn is_record(bytes: &[u8]) -> bool {
        read_header(bytes, &mut 0).is_ok()
    }

    /// Read a record a cell stores.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let mut tokens = Vec::new();
        let mut content = None;
        for frame in decode(bytes)? {
            match frame {
                Section::Invocation(token) => tokens.push(token),
                Section::Block {
                    reference: Reference::Delegation(_),
                    bytes,
                } => tokens.push(bytes),
                Section::Block {
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

/// The header of a record whose root is `root`, in its one byte form.
fn header(root: Cid) -> Vec<u8> {
    #[allow(clippy::expect_used)]
    serde_ipld_dagcbor::to_vec(&CarHeader {
        version: 1,
        roots: vec![root],
    })
    .expect("a header encodes")
}

/// Read a record's header at `at`, answering its one root.
fn read_header(bytes: &[u8], at: &mut usize) -> Result<Cid, RecordError> {
    let Some(length) = take_varint(bytes, at)? else {
        return Err(RecordError::NotARecord("it ends before a header".into()));
    };
    if length > MAX_BLOCK {
        return Err(RecordError::NotARecord(format!("a {length} byte header")));
    }
    let Some(encoded) = bytes.get(*at..*at + length as usize) else {
        return Err(RecordError::NotARecord("it ends inside its header".into()));
    };
    let car: CarHeader = serde_ipld_dagcbor::from_slice(encoded)
        .map_err(|error| RecordError::NotARecord(error.to_string()))?;
    if car.version != 1 {
        return Err(RecordError::NotARecord(format!(
            "CAR version {}",
            car.version
        )));
    }
    let [root] = car.roots.as_slice() else {
        return Err(RecordError::Roots(car.roots.len()));
    };
    if header(*root) != encoded {
        return Err(RecordError::Header);
    }
    *at += length as usize;
    Ok(*root)
}

/// The section at `at`, as its CID and block, or `None` when the bytes
/// end inside it.
fn read_section<'a>(
    bytes: &'a [u8],
    at: &mut usize,
    section: usize,
) -> Result<Option<(Cid, &'a [u8])>, RecordError> {
    let Some(length) = take_varint(bytes, at)? else {
        return Ok(None);
    };
    if length > MAX_BLOCK {
        return Err(RecordError::TooLarge { section, length });
    }
    let Some(mut body) = bytes.get(*at..*at + length as usize) else {
        return Ok(None);
    };
    *at += length as usize;
    let cid = Cid::read_bytes(&mut body).map_err(|error| RecordError::Cid {
        section,
        reason: error.to_string(),
    })?;
    Ok(Some((cid, body)))
}

/// Append one section: `varint(len(cid) + len(block)) | cid | block`.
fn section(out: &mut Vec<u8>, cid: &Cid, block: &[u8]) {
    let cid = cid.to_bytes();
    put_varint(out, (cid.len() + block.len()) as u64);
    out.extend_from_slice(&cid);
    out.extend_from_slice(block);
}

/// Append an unsigned LEB128 varint, in its minimal form.
fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Read the unsigned LEB128 varint at `at`, advancing past it, or `None`
/// when the bytes end before it does. One that is not minimal (a last
/// byte of zero after the first), or runs past 64 bits, is refused, so a
/// length has one encoding.
fn take_varint(bytes: &[u8], at: &mut usize) -> Result<Option<u64>, RecordError> {
    let start = *at;
    let mut value: u64 = 0;
    for index in 0..10 {
        let Some(&byte) = bytes.get(start + index) else {
            return Ok(None);
        };
        let shift = index * 7;
        if index == 9 && byte > 1 {
            return Err(RecordError::Varint { at: start });
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            if index > 0 && byte == 0 {
                return Err(RecordError::Varint { at: start });
            }
            *at = start + index + 1;
            return Ok(Some(value));
        }
    }
    Err(RecordError::Varint { at: start })
}

#[cfg(test)]
mod test;
