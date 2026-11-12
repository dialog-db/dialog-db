//! The four slots after the magic number: where the file says what it
//! holds.
//!
//! A slot is the file's state as of an append: a sequence number, where
//! the appended records end, the counts the archive reports, the catalogs'
//! names, the root of the index, and the index's free pages. Every append
//! writes its records past the end and its index pages, then the older of
//! the two rolling slots; opening reads both and takes the newer one that
//! is whole. So one rolling slot is always whole: an append cut short
//! leaves the other, and records past its end, which the next writer
//! drops.
//!
//! The other two hold the durable slot: it is written only at a sync point,
//! once everything before it is on the disk, and synced itself. The index's
//! pages are written over only once a sync point has made that safe (see
//! the archive's sync), so nothing the durable slot reaches is ever written
//! over, and after a power loss the file can always go back to it. A slot
//! can span sectors, and a power loss during its write can keep one and
//! lose another, so each sync point writes over the copy that does not
//! hold the newest whole durable slot, and opening takes the newer of the
//! two that is whole: one is whole whatever a power loss cut short.
//!
//! A slot is its body's length (u32), the crc32 of the body, then the body:
//! sequence, end, the last sync point's sequence and the boot it was
//! written in (u64 each), the counts (u64 each), the catalogs (a count,
//! then each name as a length byte and its bytes), the root (its length,
//! u32, then its bytes), then the pages free to write over and the pages
//! freed since the last sync point (each a count, then u32 each), then the
//! extents writers hold, the extents free to claim again and the extents
//! freed since the last sync point (each a count, then each record's
//! offset as u64), then the root of the cells' index (its length, u32, then
//! its bytes).

use std::fs::File;
use std::io;

use super::tree;
use crate::file::{MAGIC, read_at, write_at};
use crate::rows::{Reader, invalid, put_varint};

/// The bytes a slot may take.
pub(crate) const SLOT: u64 = 64 * 1024;

/// Where the two copies of the durable slot are.
const DURABLE: [u64; 2] = [MAGIC.len() as u64 + 2 * SLOT, MAGIC.len() as u64 + 3 * SLOT];

/// Where the first record goes: after the magic number and the slots.
pub(crate) const DATA: u64 = MAGIC.len() as u64 + 4 * SLOT;

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
    /// The sequence of the last sync point: the durable slot's.
    pub durable: u64,
    /// The boot this slot was written in (0 when the platform does not
    /// say): one written in this boot was not cut short by a power loss.
    pub boot: u64,
    pub counts: Counts,
    /// The catalogs' names, by the number records name them by.
    pub catalogs: Vec<String>,
    pub root: tree::Node,
    /// Index pages no root reaches since a sync point: free to write over.
    pub reusable: Vec<u32>,
    /// Index pages freed since the last sync point: free once the next is.
    pub pending: Vec<u32>,
    /// The extents writers append to, not yet integrated, by where their
    /// records start.
    pub extents: Vec<u64>,
    /// Extents integrated before a sync point: free to claim again.
    pub extents_free: Vec<u64>,
    /// Extents integrated since the last sync point: free once the next is.
    pub extents_pending: Vec<u64>,
    /// The root of the cells' index, from a key of each cell's name to
    /// where its record's data starts (see the archive's `cells` module).
    pub cells: tree::Node,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            sequence: 0,
            end: DATA,
            durable: 0,
            boot: 0,
            counts: Counts::default(),
            catalogs: Vec::new(),
            root: tree::Node::default(),
            reusable: Vec::new(),
            pending: Vec::new(),
            extents: Vec::new(),
            extents_free: Vec::new(),
            extents_pending: Vec::new(),
            cells: tree::Node::default(),
        }
    }
}

impl Slot {
    fn encode(&self) -> io::Result<Vec<u8>> {
        let mut body = Vec::new();
        for field in [self.sequence, self.end, self.durable, self.boot] {
            body.extend_from_slice(&field.to_le_bytes());
        }
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
        let root = self.root.encode();
        body.extend_from_slice(&(root.len() as u32).to_le_bytes());
        body.extend_from_slice(&root);
        for pages in [&self.reusable, &self.pending] {
            put_varint(&mut body, pages.len() as u64);
            for page in pages {
                body.extend_from_slice(&page.to_le_bytes());
            }
        }
        for extents in [&self.extents, &self.extents_free, &self.extents_pending] {
            put_varint(&mut body, extents.len() as u64);
            for extent in extents {
                body.extend_from_slice(&extent.to_le_bytes());
            }
        }
        let cells = self.cells.encode();
        body.extend_from_slice(&(cells.len() as u32).to_le_bytes());
        body.extend_from_slice(&cells);
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
        let u32_of = |reader: &mut Reader| -> Option<u32> {
            Some(u32::from_le_bytes(reader.take(4).ok()?.try_into().ok()?))
        };
        let sequence = u64_of(&mut reader)?;
        let end = u64_of(&mut reader)?;
        let durable = u64_of(&mut reader)?;
        let boot = u64_of(&mut reader)?;
        let mut fields = [0u64; 7];
        for field in &mut fields {
            *field = u64_of(&mut reader)?;
        }
        let mut catalogs = Vec::new();
        for _ in 0..reader.varint().ok()? {
            let len = reader.byte().ok()? as usize;
            catalogs.push(String::from_utf8(reader.take(len).ok()?.to_vec()).ok()?);
        }
        let root_len = u32_of(&mut reader)? as usize;
        let root = tree::Node::decode(reader.take(root_len).ok()?).ok()?;
        let mut lists = [Vec::new(), Vec::new()];
        for list in &mut lists {
            for _ in 0..reader.varint().ok()? {
                list.push(u32_of(&mut reader)?);
            }
        }
        let [reusable, pending] = lists;
        let mut extent_lists = [Vec::new(), Vec::new(), Vec::new()];
        for list in &mut extent_lists {
            for _ in 0..reader.varint().ok()? {
                list.push(u64_of(&mut reader)?);
            }
        }
        let [extents, extents_free, extents_pending] = extent_lists;
        let cells_len = u32_of(&mut reader)? as usize;
        let cells = tree::Node::decode(reader.take(cells_len).ok()?).ok()?;
        Some(Self {
            sequence,
            end,
            durable,
            boot,
            counts: Counts::from_fields(fields),
            catalogs,
            root,
            reusable,
            pending,
            extents,
            extents_free,
            extents_pending,
            cells,
        })
    }

    /// Writes this slot over the older rolling slot: the one its sequence
    /// number picks.
    pub fn write(&self, file: &File) -> io::Result<()> {
        #[cfg(test)]
        self.check_extents();
        write_at(
            file,
            MAGIC.len() as u64 + (self.sequence % 2) * SLOT,
            &self.encode()?,
        )
    }

    /// In tests, that no extent is listed twice, in one list or across
    /// them: one listed free twice is claimed by two writers at once.
    #[cfg(test)]
    fn check_extents(&self) {
        let mut seen = std::collections::HashSet::new();
        for record in self
            .extents
            .iter()
            .chain(&self.extents_free)
            .chain(&self.extents_pending)
        {
            assert!(
                seen.insert(*record),
                "extent {record} listed twice in slot {}: in use {:?}, free {:?}, pending {:?}",
                self.sequence,
                self.extents,
                self.extents_free,
                self.extents_pending
            );
        }
    }

    /// Writes this slot as the durable one, over the copy that does not
    /// hold the newest whole durable slot.
    pub fn write_durable(&self, file: &File) -> io::Result<()> {
        #[cfg(test)]
        self.check_extents();
        let len = file.metadata()?.len();
        let held = [
            Self::read_at(file, DURABLE[0], len)?,
            Self::read_at(file, DURABLE[1], len)?,
        ];
        let newest = match &held {
            [Some(a), Some(b)] => Some(usize::from(b.sequence > a.sequence)),
            [Some(_), None] => Some(0),
            [None, Some(_)] => Some(1),
            [None, None] => None,
        };
        let copy = newest.map_or(0, |newest| 1 - newest);
        write_at(file, DURABLE[copy], &self.encode()?)
    }

    fn read_at(file: &File, offset: u64, len: u64) -> io::Result<Option<Slot>> {
        if offset >= len {
            return Ok(None);
        }
        let mut bytes = vec![0u8; SLOT.min(len - offset) as usize];
        read_at(file, offset, &mut bytes)?;
        Ok(Self::decode(&bytes))
    }

    /// The rolling slots of `file` that are whole, newest first.
    pub fn read(file: &File) -> io::Result<Vec<Slot>> {
        let len = file.metadata()?.len();
        let mut slots = Vec::new();
        for at in 0..2 {
            if let Some(slot) = Self::read_at(file, MAGIC.len() as u64 + at * SLOT, len)? {
                slots.push(slot);
            }
        }
        slots.sort_by_key(|slot| std::cmp::Reverse(slot.sequence));
        Ok(slots)
    }

    /// The durable slot: the newer copy that is whole, if either is.
    pub fn read_durable(file: &File) -> io::Result<Option<Slot>> {
        let len = file.metadata()?.len();
        let a = Self::read_at(file, DURABLE[0], len)?;
        let b = Self::read_at(file, DURABLE[1], len)?;
        Ok(match (a, b) {
            (Some(a), Some(b)) => Some(if b.sequence > a.sequence { b } else { a }),
            (a, b) => a.or(b),
        })
    }
}

/// This boot, as a number (0 where the platform does not say): a slot
/// written in this boot was not cut short by a power loss or a crash of
/// the operating system, which end a boot.
pub(crate) fn boot() -> u64 {
    #[cfg(target_os = "linux")]
    if let Ok(id) = std::fs::read("/proc/sys/kernel/random/boot_id") {
        return crate::hash::hash(&id).max(1);
    }
    0
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
            reusable: vec![3, 4],
            pending: vec![5],
            extents: vec![DATA + 64, DATA + 4160],
            extents_free: vec![DATA + 8256],
            extents_pending: vec![],
            boot: 9,
            durable: 1,
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
        assert_eq!(Slot::read(&file)?, vec![first.clone()]);

        assert_eq!(Slot::read_durable(&file)?, None);
        first.write_durable(&file)?;
        assert_eq!(Slot::read_durable(&file)?, Some(first.clone()));
        assert_eq!(
            Slot::read(&file)?,
            vec![first],
            "the durable slot is not a rolling one"
        );
        Ok(())
    }
}
