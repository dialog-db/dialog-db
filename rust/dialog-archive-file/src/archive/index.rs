//! The file as of an append, read without a lock: its slot and a mapping
//! of everything up to the slot's end.
//!
//! Everything a snapshot reads was written before its slot, and nothing
//! before a slot's end is written again but the index's pages, which a
//! reader checks as it reads them (see [`tree`](super::tree)): a frame or
//! token a snapshot reads stays true for as long as anyone holds it, and
//! an index page written over since reads as stale, for the reader to look
//! again from a newer snapshot. An append publishes a new snapshot; readers
//! take whichever is current and never wait.
//!
//! The index is the tree in the file, keyed by 32-bit keys (two bits of
//! space, thirty of a hash) in three spaces, each value a place in the file
//! divided by [`ALIGN`]:
//!
//! - a block, by its catalog and hash: where its frame is (a block kept
//!   whole is a frame of the [whole](WHOLE) kind, its bytes after the
//!   header);
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
use super::tree::{Child, Nodes, PAGE, check};
use crate::rows::{Reader, invalid, put_varint};
use crate::tokens::{ALIGN, TokenRead, token_at};

const SPACE: u32 = 30;
const LOW: u32 = (1 << SPACE) - 1;

/// The key spaces.
const BLOCKS: u32 = 0;
const TOKENS: u32 = 1;
const LINEAGES: u32 = 2;

/// A frame's kind when it holds a block whole: its bytes follow the header.
pub(crate) const WHOLE: u8 = 0xFF;

fn key(space: u32, hash: u64) -> u32 {
    space << SPACE | (hash as u32 & LOW)
}

/// The key after `key` in its space, for a value filed past a taken one.
pub(crate) fn next(key: u32) -> u32 {
    (key & !LOW) | (key.wrapping_add(1) & LOW)
}

/// The first key a block of `catalog` with `hash` is filed under.
pub(crate) fn block_key(catalog: u16, hash: &Blake3Hash) -> u32 {
    let prefix = u64::from_le_bytes(hash.as_bytes()[..8].try_into().expect("eight bytes"));
    key(
        BLOCKS,
        prefix ^ (catalog as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
    )
}

/// The first key a token whose bytes hash to `hash` is filed under.
pub(crate) fn token_key(hash: u64) -> u32 {
    key(TOKENS, hash)
}

/// The key of a lineage: the latest frame of it is filed there, and
/// replaces the one before.
pub(crate) fn lineage_key(catalog: u16, kind: u8, first: [u32; 4]) -> u32 {
    let mut bytes = Vec::with_capacity(19);
    bytes.extend_from_slice(&catalog.to_le_bytes());
    bytes.push(kind);
    for id in first {
        bytes.extend_from_slice(&id.to_le_bytes());
    }
    key(LINEAGES, crate::hash::hash(&bytes))
}

/// How the file holds a block: in the frame whose data starts at this
/// offset divided by [`ALIGN`], whole or as rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Place {
    /// Whole, its bytes after the frame's header.
    Block(u32),
    /// As rows.
    Frame(u32),
}

impl Place {
    pub fn value(self) -> u32 {
        match self {
            Place::Block(at) | Place::Frame(at) => at,
        }
    }

    fn of(at: u32, header: &FrameHeader) -> Self {
        if header.kind == WHOLE {
            Place::Block(at)
        } else {
            Place::Frame(at)
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

    /// A whole block's bytes, after its frame's header.
    pub fn block(&self, at: u32) -> io::Result<&[u8]> {
        let bytes = self.frame_bytes(at)?;
        let mut reader = Reader::new(bytes);
        let header = FrameHeader::get(&mut reader)?;
        if header.kind != WHOLE {
            return Err(invalid("not a block kept whole"));
        }
        let len = reader.varint()? as usize;
        let start = reader.position();
        bytes
            .get(start..start + len)
            .ok_or_else(|| invalid("a block past the file's end"))
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
    pub fn probe_block(&self, catalog: u16, hash: &Blake3Hash) -> io::Result<(Option<Place>, u32)> {
        let mut at = block_key(catalog, hash);
        while let Some(value) = self.slot.root.get(self, at)? {
            let header = self.frame(value)?;
            if header.catalog == catalog && &header.hash == hash {
                return Ok((Some(Place::of(value, &header)), at));
            }
            at = next(at);
        }
        Ok((None, at))
    }

    /// The token equal to `token` (whose bytes hash to `hash`), by where it
    /// starts divided by [`ALIGN`], and the first free key past the ones
    /// the search met.
    pub fn probe_token(&self, hash: u64, token: &[u8]) -> io::Result<(Option<u32>, u32)> {
        let mut at = token_key(hash);
        while let Some(id) = self.slot.root.get(self, at)? {
            if self.get(id) == Some(token) {
                return Ok((Some(id), at));
            }
            at = next(at);
        }
        Ok((None, at))
    }

    /// The latest frame of a lineage, by where it starts divided by
    /// [`ALIGN`].
    pub fn lineage(&self, key: u32) -> io::Result<Option<u32>> {
        self.slot.root.get(self, key)
    }

    /// Every block, by catalog and hash, and how it is held.
    pub fn blocks(&self) -> io::Result<Vec<(u16, Blake3Hash, Place)>> {
        let mut out = Vec::new();
        for (key, value) in self.slot.root.entries(self)? {
            if key >> SPACE != BLOCKS {
                continue;
            }
            let header = self.frame(value)?;
            out.push((
                header.catalog,
                header.hash.clone(),
                Place::of(value, &header),
            ));
        }
        Ok(out)
    }
}

impl Nodes for Snapshot {
    /// The page, copied out of the mapping before it is checked: another
    /// writer may be writing over it as it is read.
    fn read(&self, child: Child) -> io::Result<Cow<'_, [u8]>> {
        let start = child.page as usize * PAGE;
        let bytes = self
            .bytes()
            .get(start..start + PAGE)
            .ok_or_else(|| invalid("an index page past the file's end"))?
            .to_vec();
        check(&bytes, child)?;
        Ok(Cow::Owned(bytes))
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
