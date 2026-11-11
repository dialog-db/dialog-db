//! Memory cells in the file: a branch's head, and every other cell a space
//! keeps.
//!
//! A cell is a record of the [cell](Kind::Cell) kind, appended in the
//! writer's turn when the cell is created and filed in the cells' own index
//! (a tree like the blocks', rooted in the slot), by a key from its name.
//! It is written over in place from then on, so cells do not accrete: an
//! update takes a lock on the cell alone (see the `locks` module), never
//! the writer's turn, and two writers of two cells never meet.
//!
//! The record's body: a byte of padding and the padding, so what follows
//! starts on an [`ALIGN`] boundary (the index names the cell by where that
//! is); the name's length (u16) and the name (the memory space, a zero
//! byte, the cell); padding to eight bytes; the capacity (u32) and four
//! bytes unused; then two halves of `HALF + capacity` bytes each, padded
//! to eight. A half is a sequence (u64), a length (u32), a crc32 (u32) of
//! the sequence, the length and the value, then the value. An update writes
//! the half that is not the newest whole one, so a crash or a reader mid
//! write finds the other one whole.
//!
//! A value up to the capacity is kept in the half. A value larger than it
//! spills: it is kept as a block (in the `cells` catalog) and the half
//! holds its hash, so it is found wherever integration has put the block.
//! A cell whose value was retracted holds an empty half.

use std::fs::File;
use std::io;
use std::sync::Arc;

use dialog_capability::{Capability, Provider, Resume};
use dialog_common::{Blake3Hash, Buffer};
use dialog_effects::memory::prelude::{ListExt, PublishExt, ResolveExt, RetractExt};
use dialog_effects::memory::{
    Current, Edition, List, MemoryError, Publish, Resolve, Retract, Version,
};

use super::index::Snapshot;
use super::{FileArchive, Laid, write_index};
use crate::file::{HEADER, Kind, put_record, write_at};
use crate::locks;
use crate::rows::invalid;
use crate::tokens::ALIGN;

/// The catalog a spilled value is kept in, as a block.
pub(crate) const SPILLED: &str = "cells";

/// What a half says before its value.
const HALF: u64 = 16;

/// A length that marks a half as holding a spilled value's hash.
const SPILL: u32 = 1 << 31;

/// A length that marks a cell retracted.
const EMPTY: u32 = u32::MAX;

/// The capacity a cell is created with, for a first value of `len` bytes:
/// a fifth more, so a value that grows a little still fits.
pub(crate) fn capacity(len: usize) -> u32 {
    (len as u32).saturating_add(len as u32 / 5).max(64)
}

/// The name a cell is filed under: its memory space and its own.
pub(crate) fn name(space: &str, cell: &str) -> Vec<u8> {
    let mut name = Vec::with_capacity(space.len() + 1 + cell.len());
    name.extend_from_slice(space.as_bytes());
    name.push(0);
    name.extend_from_slice(cell.as_bytes());
    name
}

/// What a cell holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Held {
    /// Nothing: never written, or retracted.
    Empty,
    /// A value kept in the half.
    Value(Vec<u8>),
    /// A value kept as a block, by its hash.
    Spilled(Blake3Hash),
}

/// A cell's record, its layout worked out from where its data starts.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Layout {
    /// Where the name's length is: the place the index names, times
    /// [`ALIGN`].
    pub data: u64,
    pub capacity: u32,
    halves: u64,
}

impl Layout {
    fn half_len(capacity: u32) -> u64 {
        (HALF + capacity as u64).div_ceil(8) * 8
    }

    fn at(&self, half: u64) -> u64 {
        self.halves + half * Self::half_len(self.capacity)
    }

    /// The layout of the cell whose data starts at `data` in `bytes` (the
    /// file, mapped), and its name.
    pub fn of(bytes: &[u8], data: u64) -> io::Result<(Self, &[u8])> {
        let at = data as usize;
        let len = u16::from_le_bytes(
            bytes
                .get(at..at + 2)
                .ok_or_else(|| invalid("a cell past the file's end"))?
                .try_into()
                .expect("two bytes"),
        ) as usize;
        let name = bytes
            .get(at + 2..at + 2 + len)
            .ok_or_else(|| invalid("a cell's name past the file's end"))?;
        let fixed = (data + 2 + len as u64).div_ceil(8) * 8;
        let capacity = u32::from_le_bytes(
            bytes
                .get(fixed as usize..fixed as usize + 4)
                .ok_or_else(|| invalid("a cell past the file's end"))?
                .try_into()
                .expect("four bytes"),
        );
        Ok((
            Self {
                data,
                capacity,
                halves: fixed + 8,
            },
            name,
        ))
    }

    /// The newest whole half: its sequence and what it holds. `None` when
    /// neither half is whole. For bytes no writer changes as they are read:
    /// a copy, or the file under the cell's lock.
    pub fn read(&self, bytes: &[u8]) -> Option<(u64, Held)> {
        (0..2)
            .filter_map(|half| self.read_half(bytes, half))
            .max_by_key(|(sequence, _)| *sequence)
    }

    /// The value the cell held at one moment, read from `file` while
    /// writers may write it: as [`read`](Self::read), without its lock.
    ///
    /// Read alone, the halves can disagree with any one moment: the first
    /// read whole, then written over, the second read torn mid-write, and
    /// the first's older value taken as the newest, after a read that saw
    /// a newer one. So the first is read again after the second. Unchanged,
    /// no write to it began or ended between the two reads, and at the
    /// moment the second was read the newest whole of the two was the
    /// newest value written: the second torn is the write after the
    /// first's. Changed, the halves are read again. A half a crashed writer
    /// left torn reads the same each time, and is passed over.
    ///
    /// Each half is read with a read call, not from the mapping: the
    /// mapping is borrowed as bytes that never change, and two reads of it
    /// could be taken as one.
    pub fn read_settled(&self, file: &File) -> io::Result<Option<(u64, Held)>> {
        settled(|half| self.read_half_from(file, half))
    }

    fn read_half_from(&self, file: &File, half: u64) -> io::Result<Option<(u64, Held)>> {
        let mut bytes = vec![0u8; (HALF + self.capacity as u64) as usize];
        crate::file::read_at(file, self.at(half), &mut bytes)?;
        Ok(self.decode_half(&bytes))
    }

    fn read_half(&self, bytes: &[u8], half: u64) -> Option<(u64, Held)> {
        self.decode_half(bytes.get(self.at(half) as usize..)?)
    }

    /// A half from its bytes, if it is whole.
    fn decode_half(&self, bytes: &[u8]) -> Option<(u64, Held)> {
        let at = 0;
        let head = bytes.get(at..at + HALF as usize)?;
        let sequence = u64::from_le_bytes(head[0..8].try_into().ok()?);
        let len = u32::from_le_bytes(head[8..12].try_into().ok()?);
        let crc = u32::from_le_bytes(head[12..16].try_into().ok()?);
        let value_len = match len {
            EMPTY => 0,
            len if len & SPILL != 0 => 32,
            len => len as usize,
        };
        if value_len > self.capacity as usize && len & SPILL == 0 {
            return None;
        }
        let value = bytes.get(at + HALF as usize..at + HALF as usize + value_len)?;
        if crc != half_crc(sequence, len, value) || sequence == 0 {
            return None;
        }
        let held = match len {
            EMPTY => Held::Empty,
            len if len & SPILL != 0 => {
                Held::Spilled(Blake3Hash::from(<[u8; 32]>::try_from(value).ok()?))
            }
            _ => Held::Value(value.to_vec()),
        };
        Some((sequence, held))
    }

    /// Writes `held` over the half that is not the newest whole one, with
    /// the sequence after `sequence` (the newest's).
    pub fn write(&self, file: &File, bytes: &[u8], sequence: u64, held: &Held) -> io::Result<()> {
        let older = match (self.read_half(bytes, 0), self.read_half(bytes, 1)) {
            (Some((a, _)), Some((b, _))) => u64::from(a > b),
            (Some(_), None) => 1,
            _ => 0,
        };
        write_at(
            file,
            self.at(older),
            &encode_half(sequence + 1, held, self.capacity)?,
        )
    }
}

/// The newest whole half at one moment, from `read`, which reads a half
/// as it is now (see [`Layout::read_settled`]).
fn settled(
    mut read: impl FnMut(u64) -> io::Result<Option<(u64, Held)>>,
) -> io::Result<Option<(u64, Held)>> {
    let sequence = |half: &Option<(u64, Held)>| half.as_ref().map(|(sequence, _)| *sequence);
    loop {
        let first = read(0)?;
        let second = read(1)?;
        let again = read(0)?;
        if sequence(&first) == sequence(&again) {
            return Ok([again, second]
                .into_iter()
                .flatten()
                .max_by_key(|(sequence, _)| *sequence));
        }
    }
}

fn half_crc(sequence: u64, len: u32, value: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&sequence.to_le_bytes());
    hasher.update(&len.to_le_bytes());
    hasher.update(value);
    hasher.finalize()
}

fn encode_half(sequence: u64, held: &Held, capacity: u32) -> io::Result<Vec<u8>> {
    let (len, value): (u32, &[u8]) = match held {
        Held::Empty => (EMPTY, &[]),
        Held::Spilled(hash) => (SPILL | 32, hash.as_bytes()),
        Held::Value(value) => {
            if value.len() > capacity as usize {
                return Err(invalid("a value over its cell's capacity"));
            }
            (value.len() as u32, value)
        }
    };
    let mut half = Vec::with_capacity(HALF as usize + value.len());
    half.extend_from_slice(&sequence.to_le_bytes());
    half.extend_from_slice(&len.to_le_bytes());
    half.extend_from_slice(&half_crc(sequence, len, value).to_le_bytes());
    half.extend_from_slice(value);
    Ok(half)
}

/// A new cell's record, to be appended at `start` in the file, named
/// `name`, of `capacity`, holding `held` in its first half. Returns the
/// record and where the cell's data starts, divided by [`ALIGN`] as the
/// index names it.
pub(crate) fn record(
    start: u64,
    name: &[u8],
    capacity: u32,
    held: &Held,
) -> io::Result<(Vec<u8>, u32)> {
    let name_len = u16::try_from(name.len()).map_err(|_| invalid("a cell's name over 64 KiB"))?;
    let body = start + HEADER;
    let pad = (ALIGN - (body + 1) % ALIGN) % ALIGN;
    let data = body + 1 + pad;
    let fixed = (data + 2 + name.len() as u64).div_ceil(8) * 8;
    let halves = fixed + 8;
    let half_len = Layout::half_len(capacity);
    let end = halves + 2 * half_len;

    let mut bytes = Vec::with_capacity((end - body) as usize);
    bytes.push(pad as u8);
    bytes.resize(1 + pad as usize, 0);
    bytes.extend_from_slice(&name_len.to_le_bytes());
    bytes.extend_from_slice(name);
    bytes.resize((fixed - body) as usize, 0);
    bytes.extend_from_slice(&capacity.to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    let first = encode_half(1, held, capacity)?;
    bytes.extend_from_slice(&first);
    bytes.resize((end - body) as usize, 0);
    let mut out = Vec::with_capacity(HEADER as usize + bytes.len());
    put_record(&mut out, Kind::Cell, &bytes)?;
    let at = u32::try_from(data / ALIGN).map_err(|_| invalid("a cell past 64 GiB"))?;
    Ok((out, at))
}

/// The first key a cell named `name` is filed under; a cell whose name
/// shares it is filed at the next free key.
fn key(name: &[u8]) -> u32 {
    crate::hash::hash(name) as u32
}

/// A cell's lock: this process's, as POSIX locks do not keep a process's
/// own threads apart, and the cell's byte in the file, across processes.
struct Locked<'a> {
    _process: parking_lot::ArcMutexGuard<parking_lot::RawMutex, ()>,
    file: &'a File,
    byte: u64,
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        let _ = locks::unlock(self.file, self.byte);
    }
}

fn version(content: &[u8]) -> Version {
    Version::from(Blake3Hash::hash(content).as_bytes())
}

fn holding(content: Option<Vec<u8>>) -> Current {
    match content {
        Some(content) => Current::Holds(Edition {
            version: version(&content),
            content,
        }),
        None => Current::Empty,
    }
}

fn storage(error: io::Error) -> MemoryError {
    MemoryError::Storage(error.to_string())
}

impl FileArchive {
    /// The cell named `name` as `snapshot` files it.
    fn find_cell(&self, snapshot: &Snapshot, name: &[u8]) -> io::Result<Option<Layout>> {
        let mut key = key(name);
        while let Some(at) = snapshot.slot.cells.get(snapshot, key)? {
            let (layout, found) = Layout::of(snapshot.bytes(), at as u64 * ALIGN)?;
            if found == name {
                return Ok(Some(layout));
            }
            key = key.wrapping_add(1);
        }
        Ok(None)
    }

    /// The cell named `name`, looking again after reading the slots when it
    /// is not found and another process grew the file.
    pub(super) fn cell(&self, name: &[u8]) -> io::Result<Option<(Arc<Snapshot>, Layout)>> {
        let (snapshot, found) = self.look(|snapshot| self.find_cell(snapshot, name))?;
        if let Some(layout) = found {
            return Ok(Some((snapshot, layout)));
        }
        if !self.reread()? {
            return Ok(None);
        }
        let (snapshot, found) = self.look(|snapshot| self.find_cell(snapshot, name))?;
        Ok(found.map(|layout| (snapshot, layout)))
    }

    /// What a cell holds, its spilled value read from its block.
    fn value(&self, held: Held) -> io::Result<Option<Vec<u8>>> {
        match held {
            Held::Empty => Ok(None),
            Held::Value(value) => Ok(Some(value)),
            Held::Spilled(hash) => self
                .read(SPILLED, &hash)?
                .map(Some)
                .ok_or_else(|| invalid("a cell's spilled value the file lacks")),
        }
    }

    /// The value the cell `cell` of memory space `space` holds.
    pub(crate) fn resolve_cell(&self, space: &str, cell: &str) -> io::Result<Option<Vec<u8>>> {
        let Some((snapshot, layout)) = self.cell(&name(space, cell))? else {
            return Ok(None);
        };
        drop(snapshot);
        let (_, held) = layout
            .read_settled(&self.inner.file)?
            .ok_or_else(|| invalid("a cell with neither half whole"))?;
        self.value(held)
    }

    /// Takes the cell's lock, waiting for it: only writers of this one
    /// cell wait on each other.
    fn lock_cell(&self, layout: &Layout) -> io::Result<Locked<'_>> {
        let process = self
            .inner
            .cell_locks
            .lock()
            .entry(layout.data)
            .or_default()
            .clone();
        let process = process.lock_arc();
        let byte = locks::owner(layout.data);
        locks::lock(&self.inner.file, byte, true, true)?;
        Ok(Locked {
            _process: process,
            file: &self.inner.file,
            byte,
        })
    }

    /// Creates the cell named `name` holding `content`, in the writer's
    /// turn. `false` when another writer created it first.
    fn create_cell(&self, name: &[u8], content: &[u8]) -> io::Result<bool> {
        let turn = self.turn()?;
        let snapshot = self.newest_in_turn(&turn)?;
        if self.find_cell(&snapshot, name)?.is_some() {
            return Ok(false);
        }
        let mut slot = snapshot.slot.clone();
        slot.sequence += 1;
        let start = slot.end;
        let (mut bytes, at) = record(
            start,
            name,
            capacity(content.len()),
            &Held::Value(content.to_vec()),
        )?;
        let mut free = key(name);
        while slot.cells.get(&*snapshot, free)?.is_some() {
            free = free.wrapping_add(1);
        }
        let pages = write_index(
            &snapshot,
            &mut slot,
            start,
            &mut bytes,
            Vec::new(),
            vec![(free, at)],
        )?;
        self.commit(Laid {
            start,
            bytes,
            pages,
            slot,
            rows: Vec::new(),
            tokens: Vec::new(),
        })
        .inspect_err(|_| {
            let _ = self.inner.file.set_len(snapshot.slot.end);
        })?;
        Ok(true)
    }

    /// Publishes `content` to the cell, if it holds what `when` names (or
    /// nothing, for `None`): written over in place under the cell's lock;
    /// a cell not there yet is created in the writer's turn.
    pub(crate) fn publish_cell(
        &self,
        space: &str,
        cell: &str,
        content: &[u8],
        when: Option<&Version>,
    ) -> Result<Version, MemoryError> {
        let name = name(space, cell);
        let new = version(content);
        loop {
            let Some((_, layout)) = self.cell(&name).map_err(storage)? else {
                if let Some(expected) = when {
                    return Err(MemoryError::VersionMismatch {
                        expected: Some(expected.clone()),
                        current: Current::Empty,
                    });
                }
                if self.create_cell(&name, content).map_err(storage)? {
                    return Ok(new);
                }
                continue;
            };
            let _locked = self.lock_cell(&layout).map_err(storage)?;
            let snapshot = self.current();
            let (sequence, held) = layout
                .read(snapshot.bytes())
                .ok_or_else(|| storage(invalid("a cell with neither half whole")))?;
            let current = self.value(held).map_err(storage)?;
            if current.as_deref().map(version).as_ref() == Some(&new) {
                return Ok(new);
            }
            let refused = match (when, &current) {
                (None, Some(_)) => true,
                (Some(expected), Some(current)) => *expected != version(current),
                (Some(_), None) => true,
                (None, None) => false,
            };
            if refused {
                return Err(MemoryError::VersionMismatch {
                    expected: when.cloned(),
                    current: holding(current),
                });
            }
            let held = if content.len() <= layout.capacity as usize {
                Held::Value(content.to_vec())
            } else {
                let block = Buffer::from(content.to_vec());
                self.append(SPILLED, std::slice::from_ref(&block))
                    .map_err(|error| MemoryError::Storage(error.to_string()))?;
                Held::Spilled(block.blake3_hash().clone())
            };
            layout
                .write(&self.inner.file, snapshot.bytes(), sequence, &held)
                .map_err(storage)?;
            return Ok(new);
        }
    }

    /// Empties the cell, if it holds what `when` names.
    pub(crate) fn retract_cell(
        &self,
        space: &str,
        cell: &str,
        when: &Version,
    ) -> Result<(), MemoryError> {
        let Some((_, layout)) = self.cell(&name(space, cell)).map_err(storage)? else {
            return Ok(());
        };
        let _locked = self.lock_cell(&layout).map_err(storage)?;
        let snapshot = self.current();
        let (sequence, held) = layout
            .read(snapshot.bytes())
            .ok_or_else(|| storage(invalid("a cell with neither half whole")))?;
        let Some(current) = self.value(held).map_err(storage)? else {
            return Ok(());
        };
        if *when != version(&current) {
            return Err(MemoryError::VersionMismatch {
                expected: Some(when.clone()),
                current: holding(Some(current)),
            });
        }
        layout
            .write(&self.inner.file, snapshot.bytes(), sequence, &Held::Empty)
            .map_err(storage)
    }

    /// The cells of memory space `space` that hold a value, sorted.
    pub(crate) fn list_cells(&self, space: &str) -> io::Result<Vec<String>> {
        self.reread()?;
        let (snapshot, entries) = self.look(|snapshot| snapshot.slot.cells.entries(snapshot))?;
        let bytes = snapshot.bytes();
        let mut cells = Vec::new();
        for (_, at) in entries {
            let (layout, name) = Layout::of(bytes, at as u64 * ALIGN)?;
            let Some(split) = name.iter().position(|byte| *byte == 0) else {
                continue;
            };
            if &name[..split] != space.as_bytes() {
                continue;
            }
            if matches!(
                layout.read_settled(&self.inner.file)?,
                Some((_, Held::Empty)) | None
            ) {
                continue;
            }
            cells.push(String::from_utf8_lossy(&name[split + 1..]).into_owned());
        }
        cells.sort();
        Ok(cells)
    }
}

impl Provider<Resolve> for FileArchive {
    fn execute(
        &self,
        effect: Capability<Resolve>,
    ) -> Resume<'_, Result<Option<Edition<Vec<u8>>>, MemoryError>> {
        Resume::now(
            self.resolve_cell(effect.space(), effect.cell())
                .map(|content| {
                    content.map(|content| Edition {
                        version: version(&content),
                        content,
                    })
                })
                .map_err(storage),
        )
    }
}

impl Provider<Publish> for FileArchive {
    fn execute(&self, effect: Capability<Publish>) -> Resume<'_, Result<Version, MemoryError>> {
        Resume::now(self.publish_cell(
            effect.space(),
            effect.cell(),
            effect.content(),
            effect.when(),
        ))
    }
}

impl Provider<Retract> for FileArchive {
    fn execute(&self, effect: Capability<Retract>) -> Resume<'_, Result<(), MemoryError>> {
        Resume::now(self.retract_cell(effect.space(), effect.cell(), effect.when()))
    }
}

impl Provider<List> for FileArchive {
    fn execute(&self, effect: Capability<List>) -> Resume<'_, Result<Vec<String>, MemoryError>> {
        Resume::now(self.list_cells(effect.space()).map_err(storage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::read_at;

    fn archive() -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf, FileArchive)> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        Ok((directory, path, archive))
    }

    /// A cell is created, read, updated against its edition, refused
    /// against another, retracted and listed, all in the file.
    #[test]
    fn it_keeps_a_cell_in_the_file() -> anyhow::Result<()> {
        let (_directory, path, archive) = archive()?;
        assert_eq!(archive.resolve_cell("local", "main")?, None);
        let first = archive.publish_cell("local", "main", b"first", None)?;
        assert_eq!(
            archive.resolve_cell("local", "main")?.as_deref(),
            Some(&b"first"[..])
        );

        // Creating it again is refused: it holds a value.
        let refused = archive.publish_cell("local", "main", b"again", None);
        assert!(matches!(
            refused,
            Err(MemoryError::VersionMismatch { expected: None, .. })
        ));

        let second = archive.publish_cell("local", "main", b"second", Some(&first))?;
        assert_eq!(
            archive.resolve_cell("local", "main")?.as_deref(),
            Some(&b"second"[..])
        );

        // An edition it is no longer at is refused, and nothing is written.
        let stale = archive.publish_cell("local", "main", b"stale", Some(&first));
        assert!(matches!(
            stale,
            Err(MemoryError::VersionMismatch {
                current: Current::Holds(_),
                ..
            })
        ));
        assert_eq!(
            archive.resolve_cell("local", "main")?.as_deref(),
            Some(&b"second"[..])
        );

        archive.publish_cell("local", "other", b"x", None)?;
        archive.publish_cell("remote", "main", b"y", None)?;
        assert_eq!(archive.list_cells("local")?, vec!["main", "other"]);

        assert!(matches!(
            archive.retract_cell("local", "main", &first),
            Err(MemoryError::VersionMismatch { .. })
        ));
        archive.retract_cell("local", "main", &second)?;
        assert_eq!(archive.resolve_cell("local", "main")?, None);
        assert_eq!(archive.list_cells("local")?, vec!["other"]);

        drop(archive);
        let reopened = FileArchive::open(&path)?;
        assert_eq!(
            reopened.resolve_cell("local", "other")?.as_deref(),
            Some(&b"x"[..])
        );
        assert_eq!(reopened.resolve_cell("local", "main")?, None);
        Ok(())
    }

    /// A value past the cell's capacity spills to a block, reads back, and
    /// survives reopening; a smaller one after it is kept in the half again.
    #[test]
    fn it_spills_a_value_past_the_capacity() -> anyhow::Result<()> {
        let (_directory, path, archive) = archive()?;
        let first = archive.publish_cell("local", "main", b"small", None)?;
        let big = vec![7u8; 4096];
        let spilled = archive.publish_cell("local", "main", &big, Some(&first))?;
        assert_eq!(
            archive.resolve_cell("local", "main")?.as_deref(),
            Some(&big[..])
        );
        drop(archive);
        let reopened = FileArchive::open(&path)?;
        assert_eq!(
            reopened.resolve_cell("local", "main")?.as_deref(),
            Some(&big[..])
        );
        reopened.publish_cell("local", "main", b"small again", Some(&spilled))?;
        assert_eq!(
            reopened.resolve_cell("local", "main")?.as_deref(),
            Some(&b"small again"[..])
        );
        Ok(())
    }

    /// Updating a cell does not take the writer's turn: another process
    /// holding it does not hold the update up.
    #[test]
    fn it_updates_a_cell_while_another_process_holds_the_turn() -> anyhow::Result<()> {
        let (_directory, path, archive) = archive()?;
        let first = archive.publish_cell("local", "main", b"first", None)?;
        let mut holder = crate::locks::tests::hold_turn(&path)?;
        let started = std::time::Instant::now();
        let second = archive.publish_cell("local", "main", b"second", Some(&first))?;
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        assert_eq!(
            archive.resolve_cell("local", "main")?.as_deref(),
            Some(&b"second"[..])
        );
        assert_ne!(first, second);
        drop(holder.stdin.take());
        holder.wait()?;
        Ok(())
    }

    #[test]
    fn it_writes_over_the_older_half_and_reads_the_newer() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(directory.path().join("cells"))?;
        let start = 1000;
        let name = name("local", "branch/main");
        let first = Held::Value(b"first".to_vec());
        let (bytes, at) = record(start, &name, capacity(5), &first)?;
        write_at(&file, start, &bytes)?;
        let read = |file: &File| -> anyhow::Result<Vec<u8>> {
            let mut out = vec![0u8; (start as usize) + bytes.len()];
            read_at(file, 0, &mut out)?;
            Ok(out)
        };

        let mapped = read(&file)?;
        let (layout, read_name) = Layout::of(&mapped, at as u64 * ALIGN)?;
        assert_eq!(read_name, &name[..]);
        assert_eq!(layout.capacity, 64);
        assert_eq!(layout.read(&mapped), Some((1, first.clone())));

        let second = Held::Value(b"second, longer".to_vec());
        layout.write(&file, &mapped, 1, &second)?;
        let mapped = read(&file)?;
        assert_eq!(layout.read(&mapped), Some((2, second.clone())));

        let third = Held::Empty;
        layout.write(&file, &mapped, 2, &third)?;
        let mapped = read(&file)?;
        assert_eq!(
            layout.read(&mapped),
            Some((3, third)),
            "the first half, written over"
        );

        // A half caught half written is not read: the other one is. (The
        // empty value has no bytes, so tear its length.)
        let torn = layout.at(0) + 8;
        write_at(&file, torn, &[0xAA])?;
        let mapped = read(&file)?;
        assert_eq!(layout.read(&mapped), Some((2, second)));

        // Over capacity does not encode: it spills instead.
        let big = Held::Value(vec![0; 65]);
        assert!(layout.write(&file, &mapped, 3, &big).is_err());
        let spilled = Held::Spilled(Blake3Hash::hash(b"big"));
        layout.write(&file, &mapped, 3, &spilled)?;
        assert_eq!(layout.read(&read(&file)?), Some((4, spilled)));
        Ok(())
    }

    /// A reader that read the first half, then the second torn as it was
    /// written, does not take the first's value if the first was written
    /// over in between: that value is older than one a reader could have
    /// read before. It reads both again, and takes the newest settled.
    #[test]
    fn it_reads_a_cell_as_it_was_at_one_moment() -> anyhow::Result<()> {
        let value = |text: &str| Held::Value(text.as_bytes().to_vec());
        // Half 0 holds 10, half 1 holds 11 (read by an earlier reader).
        // Then half 0 is written with 12, and half 1 is being written with
        // 13 when this reader reads it.
        let mut reads = vec![
            Some((10, value("ten"))),
            None,
            Some((12, value("twelve"))),
            // Read again: the write of 13 has finished.
            Some((12, value("twelve"))),
            Some((13, value("thirteen"))),
            Some((12, value("twelve"))),
        ]
        .into_iter();
        let read = settled(|_| Ok(reads.next().expect("no more reads than scripted")))?;
        let sequence = read.map(|(sequence, _)| sequence);
        assert!(
            sequence >= Some(11),
            "not older than what an earlier reader read: {sequence:?}"
        );

        // A half a crashed writer left torn stays torn: the other is taken.
        let mut reads = vec![None, Some((7, value("seven"))), None].into_iter();
        let read = settled(|_| Ok(reads.next().expect("no more reads than scripted")))?;
        assert_eq!(read, Some((7, value("seven"))));
        Ok(())
    }
}
