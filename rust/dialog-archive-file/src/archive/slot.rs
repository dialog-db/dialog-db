//! The two slots after the magic number: where the file says what it
//! holds.
//!
//! A slot is the file's state as of an append: a sequence number, where
//! the appended records end, the counts the archive reports, the catalogs'
//! names, and the root of the index. Every append writes its records past
//! the end, then the older of the two slots; opening reads both and takes
//! the newer one that is whole. So one slot is always whole: an append cut
//! short leaves the other, and records past its end, which the next writer
//! drops. Nothing else in the file is ever written twice.
//!
//! A slot is its body's length (u32), the crc32 of the body, then the body:
//! sequence and end (u64 each), the counts (u64 each), the catalogs (a
//! count, then each name as a length byte and its bytes), the root.

use std::fs::File;
use std::io;

use super::tree;
use crate::file::{MAGIC, read_at, write_at};
use crate::rows::{Reader, invalid, put_varint};

/// The bytes a slot may take.
pub(crate) const SLOT: u64 = 64 * 1024;

/// Where the first record goes: after the magic number and the two slots.
pub(crate) const DATA: u64 = MAGIC.len() as u64 + 2 * SLOT;

/// What the archive reports holding, kept in the slot so a reopened file
/// reports it without reading everything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counts {
    pub blocks: u64,
    pub block_bytes: u64,
    pub frames: u64,
    pub frames_against: u64,
    pub frame_bytes: u64,
    pub tokens: u64,
    pub token_bytes: u64,
}

impl Counts {
    fn fields(&self) -> [u64; 7] {
        [
            self.blocks,
            self.block_bytes,
            self.frames,
            self.frames_against,
            self.frame_bytes,
            self.tokens,
            self.token_bytes,
        ]
    }

    fn from_fields(
        [
            blocks,
            block_bytes,
            frames,
            frames_against,
            frame_bytes,
            tokens,
            token_bytes,
        ]: [u64; 7],
    ) -> Self {
        Self {
            blocks,
            block_bytes,
            frames,
            frames_against,
            frame_bytes,
            tokens,
            token_bytes,
        }
    }
}

/// The file's state as of an append.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Slot {
    pub sequence: u64,
    /// Where the appended records end.
    pub end: u64,
    pub counts: Counts,
    /// The catalogs' names, by the number records name them by.
    pub catalogs: Vec<String>,
    pub root: tree::Node,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            sequence: 0,
            end: DATA,
            counts: Counts::default(),
            catalogs: Vec::new(),
            root: tree::Node::default(),
        }
    }
}

impl Slot {
    fn encode(&self) -> io::Result<Vec<u8>> {
        let mut body = Vec::new();
        body.extend_from_slice(&self.sequence.to_le_bytes());
        body.extend_from_slice(&self.end.to_le_bytes());
        for field in self.counts.fields() {
            body.extend_from_slice(&field.to_le_bytes());
        }
        put_varint(&mut body, self.catalogs.len() as u64);
        for catalog in &self.catalogs {
            let name = catalog.as_bytes();
            body.push(
                u8::try_from(name.len()).map_err(|_| invalid("a catalog name over 255 bytes"))?,
            );
            body.extend_from_slice(name);
        }
        body.extend_from_slice(&self.root.encode());
        let mut out = Vec::with_capacity(8 + body.len());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
        out.extend_from_slice(&body);
        if out.len() as u64 > SLOT {
            return Err(invalid("a slot over its size"));
        }
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let len = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
        let crc = u32::from_le_bytes(bytes.get(4..8)?.try_into().ok()?);
        let body = bytes.get(8..8 + len)?;
        if crc32fast::hash(body) != crc {
            return None;
        }
        let mut reader = Reader::new(body);
        let u64_of = |reader: &mut Reader| -> Option<u64> {
            Some(u64::from_le_bytes(reader.take(8).ok()?.try_into().ok()?))
        };
        let sequence = u64_of(&mut reader)?;
        let end = u64_of(&mut reader)?;
        let mut fields = [0u64; 7];
        for field in &mut fields {
            *field = u64_of(&mut reader)?;
        }
        let mut catalogs = Vec::new();
        for _ in 0..reader.varint().ok()? {
            let len = reader.byte().ok()? as usize;
            catalogs.push(String::from_utf8(reader.take(len).ok()?.to_vec()).ok()?);
        }
        let root = tree::Node::decode(&body[reader.position()..]).ok()?;
        Some(Self {
            sequence,
            end,
            counts: Counts::from_fields(fields),
            catalogs,
            root,
        })
    }

    /// Writes this slot over the older of the two: the one its sequence
    /// number picks.
    pub fn write(&self, file: &File) -> io::Result<()> {
        write_at(
            file,
            MAGIC.len() as u64 + (self.sequence % 2) * SLOT,
            &self.encode()?,
        )
    }

    /// The slots of `file` that are whole, newest first.
    pub fn read(file: &File) -> io::Result<Vec<Slot>> {
        let mut slots = Vec::new();
        let len = file.metadata()?.len();
        for at in 0..2 {
            let offset = MAGIC.len() as u64 + at * SLOT;
            if offset >= len {
                continue;
            }
            let mut bytes = vec![0u8; SLOT.min(len - offset) as usize];
            read_at(file, offset, &mut bytes)?;
            if let Some(slot) = Self::decode(&bytes) {
                slots.push(slot);
            }
        }
        slots.sort_by_key(|slot| std::cmp::Reverse(slot.sequence));
        Ok(slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_reads_the_newer_whole_slot() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(directory.path().join("slots"))?;
        write_at(&file, 0, MAGIC)?;
        let first = Slot {
            sequence: 1,
            end: DATA + 100,
            catalogs: vec!["index".into()],
            ..Slot::default()
        };
        let second = Slot {
            sequence: 2,
            end: DATA + 200,
            ..first.clone()
        };
        first.write(&file)?;
        second.write(&file)?;
        assert_eq!(Slot::read(&file)?, vec![second.clone(), first.clone()]);

        // A slot cut short or overwritten in part is not whole: the other
        // one is read. The newer one is the one sequence 2 picks, slot 0.
        write_at(&file, MAGIC.len() as u64 + 12, &[0xFF; 4])?;
        assert_eq!(Slot::read(&file)?, vec![first]);
        Ok(())
    }
}
