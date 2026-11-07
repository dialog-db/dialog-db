//! The file as of an append, read without a lock: its slot and a mapping
//! of everything up to the slot's end.
//!
//! Everything a snapshot reads was written before its slot, and nothing
//! before a slot's end is written again, so a snapshot stays true for as
//! long as anyone holds it. An append publishes a new snapshot; readers
//! take whichever is current and never wait.
//!
//! The index is the tree in the file (see [`tree`](super::tree)), keyed by
//! 64-bit keys in three spaces:
//!
//! - a block, by its catalog and hash: where its frame or whole record is;
//! - a token, by the hash of its bytes: where it is, for a writer that
//!   would otherwise write it again;
//! - a lineage (a catalog, a kind and a node's first key): the latest
//!   frame of it, for the next to be written against.
//!
//! Keys are hashes, so two blocks or tokens can share one: the second is
//! filed at the next free key, and a lookup checks each value against the
//! record it names, moving to the next key until it finds the one or a
//! free key.

use std::borrow::Cow;
use std::io;
use std::sync::Arc;

use dialog_common::Blake3Hash;
use memmap2::Mmap;

use super::slot::Slot;
use super::tree::Nodes;
use crate::file::{HEADER, Kind};
use crate::rows::{Reader, invalid, put_varint};
use crate::tokens::{ALIGN, TokenRead, token_at};

const SPACE: u32 = 62;
const LOW: u64 = (1 << SPACE) - 1;

/// The key spaces.
const BLOCKS: u64 = 0;
const TOKENS: u64 = 1;
const LINEAGES: u64 = 2;

fn key(space: u64, hash: u64) -> u64 {
    space << SPACE | (hash & LOW)
}

/// The key after `key` in its space, for a value filed past a taken one.
pub(crate) fn next(key: u64) -> u64 {
    (key & !LOW) | (key.wrapping_add(1) & LOW)
}

/// The first key a block of `catalog` with `hash` is filed under.
pub(crate) fn block_key(catalog: u16, hash: &Blake3Hash) -> u64 {
    let prefix = u64::from_le_bytes(hash.as_bytes()[..8].try_into().expect("eight bytes"));
    key(
        BLOCKS,
        prefix ^ (catalog as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
    )
}

/// The first key a token whose bytes hash to `hash` is filed under.
pub(crate) fn token_key(hash: u64) -> u64 {
    key(TOKENS, hash)
}

/// The key of a lineage: the latest frame of it is filed there, and
/// replaces the one before.
pub(crate) fn lineage_key(catalog: u16, kind: u8, first: [u32; 4]) -> u64 {
    let mut bytes = Vec::with_capacity(19);
    bytes.extend_from_slice(&catalog.to_le_bytes());
    bytes.push(kind);
    for id in first {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    key(LINEAGES, crate::hash::hash(&bytes))
}

/// How the file holds a block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Place {
    /// Whole, in the block record at this offset.
    Block(u64),
    /// As a frame, whose data starts at this offset divided by [`ALIGN`].
    Frame(u32),
}

const FRAME: u64 = 1 << 63;

impl Place {
    pub fn value(self) -> u64 {
        match self {
            Place::Block(at) => at,
            Place::Frame(at) => FRAME | at as u64,
        }
    }

    fn of(value: u64) -> Self {
        if value & FRAME != 0 {
            Place::Frame((value & !FRAME) as u32)
        } else {
            Place::Block(value)
        }
    }
}

/// What a frame says ahead of its rows. Its data starts on an [`ALIGN`]
/// boundary with the node's hash, so a parent read in place takes a
/// child's hash from where the child's frame starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FrameHeader {
    pub hash: Blake3Hash,
    pub kind: u8,
    /// How many frames its rows are written against, one after another.
    pub depth: u8,
    pub catalog: u16,
    /// The frame it is written against, if any, by where it starts.
    pub base: Option<u32>,
    /// The token of the node's manifest.
    pub manifest: u32,
}

impl FrameHeader {
    pub fn put(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.hash.as_bytes());
        out.push(self.kind);
        out.push(self.depth);
        put_varint(out, self.catalog as u64);
        put_varint(out, self.base.map_or(0, |base| base as u64 + 1));
        put_varint(out, self.manifest as u64);
    }

    pub fn get(reader: &mut Reader) -> io::Result<Self> {
        let hash = Blake3Hash::from(<[u8; 32]>::try_from(reader.take(32)?).expect("32 bytes"));
        let kind = reader.byte()?;
        let depth = reader.byte()?;
        let catalog =
            u16::try_from(reader.varint()?).map_err(|_| invalid("a catalog over 16 bits"))?;
        let base = match reader.varint()? {
            0 => None,
            base => Some(u32::try_from(base - 1).map_err(|_| invalid("a frame past 64 GiB"))?),
        };
        let manifest = reader.u32()?;
        Ok(Self {
            hash,
            kind,
            depth,
            catalog,
            base,
            manifest,
        })
    }
}

/// The file as of an append.
pub(crate) struct Snapshot {
    pub slot: Slot,
    /// The file, mapped at least to the slot's end.
    pub map: Arc<Mmap>,
}

impl Snapshot {
    /// The file's bytes up to the slot's end.
    pub fn bytes(&self) -> &[u8] {
        let end = (self.slot.end as usize).min(self.map.len());
        &self.map[..end]
    }

    /// The kind and body of the record at `at`.
    pub fn record(&self, at: u64) -> io::Result<(Kind, &[u8])> {
        let bytes = self.bytes();
        let at = usize::try_from(at).map_err(|_| invalid("a record past the mapping"))?;
        let header = bytes
            .get(at..at + HEADER as usize)
            .ok_or_else(|| invalid("a record past the file's end"))?;
        let kind = Kind::from_tag(header[0]).ok_or_else(|| invalid("a record of no kind"))?;
        let len = u32::from_le_bytes(header[1..5].try_into().expect("four bytes")) as usize;
        let body = bytes
            .get(at + HEADER as usize..at + HEADER as usize + len)
            .ok_or_else(|| invalid("a record past the file's end"))?;
        Ok((kind, body))
    }

    pub fn catalog_id(&self, name: &str) -> Option<u16> {
        self.slot
            .catalogs
            .iter()
            .position(|catalog| catalog == name)
            .map(|at| at as u16)
    }

    pub fn catalog(&self, id: u16) -> Option<&str> {
        self.slot.catalogs.get(id as usize).map(String::as_str)
    }

    /// The bytes of the frame whose data starts at `at` divided by
    /// [`ALIGN`], up to the snapshot's end: its header, then its rows.
    pub fn frame_bytes(&self, at: u32) -> io::Result<&[u8]> {
        self.bytes()
            .get(at as usize * ALIGN as usize..)
            .filter(|bytes| !bytes.is_empty())
            .ok_or_else(|| invalid("a frame past the file's end"))
    }

    pub fn frame(&self, at: u32) -> io::Result<FrameHeader> {
        FrameHeader::get(&mut Reader::new(self.frame_bytes(at)?))
    }

    /// The hash a frame starts with.
    pub fn frame_hash(&self, at: u32) -> Option<&[u8]> {
        let start = at as usize * ALIGN as usize;
        self.bytes().get(start..start + 32)
    }

    /// A whole block's catalog, hash and bytes, from its record.
    pub fn block(&self, at: u64) -> io::Result<(u16, Blake3Hash, &[u8])> {
        let (kind, body) = self.record(at)?;
        if kind != Kind::Block {
            return Err(invalid("not a block record"));
        }
        let mut reader = Reader::new(body);
        let catalog =
            u16::try_from(reader.varint()?).map_err(|_| invalid("a catalog over 16 bits"))?;
        let hash = Blake3Hash::from(<[u8; 32]>::try_from(reader.take(32)?).expect("32 bytes"));
        Ok((catalog, hash, &body[reader.position()..]))
    }

    /// How the file holds the block `hash` names in `catalog`.
    pub fn place(&self, catalog: &str, hash: &Blake3Hash) -> io::Result<Option<Place>> {
        let Some(catalog) = self.catalog_id(catalog) else {
            return Ok(None);
        };
        Ok(self.probe_block(catalog, hash)?.0)
    }

    /// How the file holds a block, and the first free key past the ones
    /// its search met, where a new block that shares its key is filed.
    pub fn probe_block(&self, catalog: u16, hash: &Blake3Hash) -> io::Result<(Option<Place>, u64)> {
        let mut at = block_key(catalog, hash);
        while let Some(value) = self.slot.root.get(self, at)? {
            let place = Place::of(value);
            let (held_catalog, held_hash) = match place {
                Place::Frame(frame) => {
                    let header = self.frame(frame)?;
                    (header.catalog, header.hash)
                }
                Place::Block(record) => {
                    let (catalog, hash, _) = self.block(record)?;
                    (catalog, hash)
                }
            };
            if held_catalog == catalog && &held_hash == hash {
                return Ok((Some(place), at));
            }
            at = next(at);
        }
        Ok((None, at))
    }

    /// The token equal to `token` (whose bytes hash to `hash`), by where it
    /// starts divided by [`ALIGN`], and the first free key past the ones
    /// the search met.
    pub fn probe_token(&self, hash: u64, token: &[u8]) -> io::Result<(Option<u32>, u64)> {
        let mut at = token_key(hash);
        while let Some(value) = self.slot.root.get(self, at)? {
            let id = value as u32;
            if self.get(id) == Some(token) {
                return Ok((Some(id), at));
            }
            at = next(at);
        }
        Ok((None, at))
    }

    /// The latest frame of a lineage, by where it starts divided by
    /// [`ALIGN`].
    pub fn lineage(&self, key: u64) -> io::Result<Option<u32>> {
        Ok(self.slot.root.get(self, key)?.map(|value| value as u32))
    }

    /// Every block, by catalog and hash, and how it is held.
    pub fn blocks(&self) -> io::Result<Vec<(u16, Blake3Hash, Place)>> {
        let mut out = Vec::new();
        for (key, value) in self.slot.root.entries(self)? {
            if key >> SPACE != BLOCKS {
                continue;
            }
            let place = Place::of(value);
            let (catalog, hash) = match place {
                Place::Frame(frame) => {
                    let header = self.frame(frame)?;
                    (header.catalog, header.hash)
                }
                Place::Block(record) => {
                    let (catalog, hash, _) = self.block(record)?;
                    (catalog, hash)
                }
            };
            out.push((catalog, hash, place));
        }
        Ok(out)
    }
}

impl Nodes for Snapshot {
    fn read(&self, at: u64) -> io::Result<Cow<'_, [u8]>> {
        let (kind, body) = self.record(at)?;
        if kind != Kind::Index {
            return Err(invalid("not an index record"));
        }
        Ok(Cow::Borrowed(body))
    }
}

impl TokenRead for Snapshot {
    fn get(&self, id: u32) -> Option<&[u8]> {
        token_at(self.bytes(), id as u64 * ALIGN)
    }
}

/// The value of a token at `offset` in the file (its bytes' start) as rows
/// name it.
pub(crate) fn token_id(offset: u64) -> io::Result<u32> {
    u32::try_from(offset / ALIGN).map_err(|_| invalid("a token past 64 GiB"))
}

/// Records `catalog` in `slot`, if it is new, and returns its number.
pub(crate) fn catalog_in(slot: &mut Slot, name: &str) -> io::Result<u16> {
    if let Some(at) = slot.catalogs.iter().position(|catalog| catalog == name) {
        return Ok(at as u16);
    }
    let id =
        u16::try_from(slot.catalogs.len()).map_err(|_| invalid("more than 65,536 catalogs"))?;
    slot.catalogs.push(name.to_string());
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_keeps_key_spaces_apart() {
        let hash = Blake3Hash::hash(b"block");
        assert_eq!(block_key(0, &hash) >> SPACE, BLOCKS);
        assert_eq!(token_key(u64::MAX) >> SPACE, TOKENS);
        assert_eq!(lineage_key(0, 0, [1, 2, 3, 4]) >> SPACE, LINEAGES);
        assert_eq!(
            next(token_key(u64::MAX)) >> SPACE,
            TOKENS,
            "the next key wraps in its space"
        );
        assert_ne!(
            block_key(0, &hash),
            block_key(1, &hash),
            "catalogs file apart"
        );
    }

    #[test]
    fn it_reads_back_a_frame_header() -> io::Result<()> {
        let header = FrameHeader {
            hash: Blake3Hash::hash(b"node"),
            kind: 1,
            depth: 3,
            catalog: 2,
            base: Some(77),
            manifest: 9,
        };
        let mut bytes = Vec::new();
        header.put(&mut bytes);
        assert_eq!(FrameHeader::get(&mut Reader::new(&bytes))?, header);
        Ok(())
    }
}
