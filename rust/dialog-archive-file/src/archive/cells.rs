//! Memory cells in the file: a branch's head, and every other cell a space
//! keeps.
//!
//! A cell is kept as SQLite keeps a commit in its write-ahead log: every
//! update appends a record of the [cell](Kind::Cell) kind holding the
//! cell's name and its value (or that it was retracted), and the cells' own
//! index (a tree like the blocks', rooted in the slot) files the name, by a
//! key from it, at the newest such record. The record is appended in the
//! writer's turn, after this process's extents are folded in, and the slot
//! that names it is written after it. So a value is never in the file
//! without what it names before it: whatever a power loss keeps, recovery
//! returns to a slot whose every record since the durable slot is whole,
//! and a cell's record and the blocks before it are kept or dropped
//! together. As in SQLite with `synchronous=NORMAL`, an update is made
//! durable by the next sync point, not by one of its own; a power loss can
//! undo the last updates, never leave one naming what is gone.
//!
//! Writers of cells take turns, as SQLite's writers take its write lock,
//! for as long as an append of one small record takes. Readers take no
//! lock: a read looks again at the slots when the file grew, as a SQLite
//! reader looks for a newer commit when it starts.
//!
//! The record's body: a byte of padding and the padding, so what follows
//! starts on an [`ALIGN`] boundary (the index names the record by where
//! that is); the name's length (u16) and the name (the memory space, a
//! zero byte, the cell); a byte, 1 when a value follows and 0 when the
//! cell was retracted; the value's length (u32) and the value.

use std::io;

use dialog_capability::{Capability, Provider, Resume};
use dialog_common::Blake3Hash;
use dialog_effects::memory::prelude::{ListExt, PublishExt, ResolveExt, RetractExt};
use dialog_effects::memory::{
    Current, Edition, List, MemoryError, Publish, Resolve, Retract, Version,
};

use super::index::Snapshot;
use super::{FileArchive, Laid, write_index};
use crate::file::{HEADER, Kind, put_record};
use crate::rows::invalid;
use crate::tokens::ALIGN;

/// The name a cell is filed under: its memory space and its own.
pub(crate) fn name(space: &str, cell: &str) -> Vec<u8> {
    let mut name = Vec::with_capacity(space.len() + 1 + cell.len());
    name.extend_from_slice(space.as_bytes());
    name.push(0);
    name.extend_from_slice(cell.as_bytes());
    name
}

/// The first key a cell named `name` is filed under; a cell whose name
/// shares it is filed at the next free key.
fn key(name: &[u8]) -> u32 {
    crate::hash::hash(name) as u32
}

/// A cell's record, to be appended at `start`: named `name`, holding
/// `value`, or retracted when `None`. Returns the record and where its data
/// starts, divided by [`ALIGN`], as the index names it.
pub(crate) fn record(start: u64, name: &[u8], value: Option<&[u8]>) -> io::Result<(Vec<u8>, u32)> {
    let name_len = u16::try_from(name.len()).map_err(|_| invalid("a cell's name over 64 KiB"))?;
    let value_len = u32::try_from(value.map_or(0, <[u8]>::len))
        .map_err(|_| invalid("a cell's value over 4 GiB"))?;
    let body = start + HEADER;
    let pad = (ALIGN - (body + 1) % ALIGN) % ALIGN;
    let data = body + 1 + pad;
    let mut bytes = Vec::with_capacity(1 + pad as usize + 7 + name.len() + value_len as usize);
    bytes.push(pad as u8);
    bytes.resize(1 + pad as usize, 0);
    bytes.extend_from_slice(&name_len.to_le_bytes());
    bytes.extend_from_slice(name);
    bytes.push(u8::from(value.is_some()));
    bytes.extend_from_slice(&value_len.to_le_bytes());
    bytes.extend_from_slice(value.unwrap_or_default());
    let mut out = Vec::with_capacity(HEADER as usize + bytes.len());
    put_record(&mut out, Kind::Cell, &bytes)?;
    let at = u32::try_from(data / ALIGN).map_err(|_| invalid("a cell past 64 GiB"))?;
    Ok((out, at))
}

/// The name and the value of the cell record whose data starts at `data` in
/// `bytes` (the file, mapped); `None` for the value of a retracted cell.
fn read(bytes: &[u8], data: u64) -> io::Result<(&[u8], Option<&[u8]>)> {
    let at = data as usize;
    let past = || invalid("a cell past the file's end");
    let len = u16::from_le_bytes(
        bytes
            .get(at..at + 2)
            .ok_or_else(past)?
            .try_into()
            .expect("two bytes"),
    ) as usize;
    let name = bytes.get(at + 2..at + 2 + len).ok_or_else(past)?;
    let at = at + 2 + len;
    let held = *bytes.get(at).ok_or_else(past)?;
    let value_len = u32::from_le_bytes(
        bytes
            .get(at + 1..at + 5)
            .ok_or_else(past)?
            .try_into()
            .expect("four bytes"),
    ) as usize;
    let value = bytes.get(at + 5..at + 5 + value_len).ok_or_else(past)?;
    Ok((name, (held == 1).then_some(value)))
}

/// Where a cell is filed: under a key, at its newest record; or not at all,
/// with the key it would be filed under.
enum Filed {
    At { key: u32, data: u64 },
    Free(u32),
}

fn storage(error: io::Error) -> MemoryError {
    MemoryError::Storage(error.to_string())
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

impl FileArchive {
    /// Where `snapshot` files the cell named `name`.
    fn find_cell(&self, snapshot: &Snapshot, name: &[u8]) -> io::Result<Filed> {
        let mut key = key(name);
        while let Some(at) = snapshot.slot.cells.get(snapshot, key)? {
            let data = at as u64 * ALIGN;
            if read(snapshot.bytes(), data)?.0 == name {
                return Ok(Filed::At { key, data });
            }
            key = key.wrapping_add(1);
        }
        Ok(Filed::Free(key))
    }

    /// The value `snapshot` gives the cell named `name`.
    fn value_in(&self, snapshot: &Snapshot, name: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self.find_cell(snapshot, name)? {
            Filed::At { data, .. } => Ok(read(snapshot.bytes(), data)?.1.map(<[u8]>::to_vec)),
            Filed::Free(_) => Ok(None),
        }
    }

    /// The value the cell `cell` of memory space `space` holds, as of the
    /// newest slot.
    pub(crate) fn resolve_cell(&self, space: &str, cell: &str) -> io::Result<Option<Vec<u8>>> {
        self.reread()?;
        let name = name(space, cell);
        // Again over a newer snapshot if a page of the cells' index was
        // written over since this one's root was read.
        Ok(self.look(|snapshot| self.value_in(snapshot, &name))?.1)
    }

    /// Writes `value` to the cell named `name` (`None` retracts it), in the
    /// writer's turn, if `admit` says to, given what it holds now: `true`
    /// writes, `false` leaves it as it is.
    fn write_cell(
        &self,
        name: &[u8],
        value: Option<&[u8]>,
        admit: impl FnOnce(Option<&[u8]>) -> Result<bool, MemoryError>,
    ) -> Result<(), MemoryError> {
        let turn = self.turn().map_err(storage)?;
        // What this process wrote into its extents goes into the file
        // first, so whatever the value names is before it.
        let snapshot = self.newest_in_turn(&turn).map_err(storage)?;
        self.integrate(&turn, snapshot).map_err(storage)?;
        let snapshot = self.newest_in_turn(&turn).map_err(storage)?;
        let filed = self.find_cell(&snapshot, name).map_err(storage)?;
        let current = match &filed {
            Filed::At { data, .. } => read(snapshot.bytes(), *data).map_err(storage)?.1,
            Filed::Free(_) => None,
        };
        if !admit(current)? {
            return Ok(());
        }
        let key = match filed {
            Filed::At { key, .. } | Filed::Free(key) => key,
        };
        let mut slot = snapshot.slot.clone();
        slot.sequence += 1;
        let start = slot.end;
        let (mut bytes, at) = record(start, name, value).map_err(storage)?;
        let pages = write_index(
            &snapshot,
            &mut slot,
            start,
            &mut bytes,
            Vec::new(),
            vec![(key, at)],
        )
        .map_err(storage)?;
        self.commit(Laid {
            start,
            bytes,
            pages,
            slot,
            rows: Vec::new(),
            tokens: Vec::new(),
        })
        .inspect_err(|_| {
            let _ = crate::file::set_len(&self.inner.file, snapshot.slot.end);
        })
        .map_err(storage)
    }

    /// Publishes `content` to the cell, if it holds what `when` names (or
    /// nothing, for `None`). Publishing what it holds already succeeds
    /// whatever `when` names, as on every memory store: its edition is its
    /// content's hash.
    pub(crate) fn publish_cell(
        &self,
        space: &str,
        cell: &str,
        content: &[u8],
        when: Option<&Version>,
    ) -> Result<Version, MemoryError> {
        let new = version(content);
        self.write_cell(&name(space, cell), Some(content), |current| {
            if current.map(version).as_ref() == Some(&new) {
                return Ok(false);
            }
            let refused = match (when, current) {
                (None, Some(_)) => true,
                (Some(expected), Some(current)) => *expected != version(current),
                (Some(_), None) => true,
                (None, None) => false,
            };
            if refused {
                return Err(MemoryError::VersionMismatch {
                    expected: when.cloned(),
                    current: holding(current.map(<[u8]>::to_vec)),
                });
            }
            Ok(true)
        })?;
        Ok(new)
    }

    /// Empties the cell, if it holds what `when` names.
    pub(crate) fn retract_cell(
        &self,
        space: &str,
        cell: &str,
        when: &Version,
    ) -> Result<(), MemoryError> {
        self.write_cell(&name(space, cell), None, |current| {
            let Some(current) = current else {
                return Ok(false);
            };
            if *when != version(current) {
                return Err(MemoryError::VersionMismatch {
                    expected: Some(when.clone()),
                    current: holding(Some(current.to_vec())),
                });
            }
            Ok(true)
        })
    }

    /// The cells of memory space `space` that hold a value, sorted.
    pub(crate) fn list_cells(&self, space: &str) -> io::Result<Vec<String>> {
        self.reread()?;
        let (snapshot, entries) = self.look(|snapshot| snapshot.slot.cells.entries(snapshot))?;
        let mut cells = Vec::new();
        for (_, at) in entries {
            let (name, value) = read(snapshot.bytes(), at as u64 * ALIGN)?;
            let Some(split) = name.iter().position(|byte| *byte == 0) else {
                continue;
            };
            if &name[..split] != space.as_bytes() || value.is_none() {
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

    fn archive() -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf, FileArchive)> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = FileArchive::open(&path)?;
        Ok((directory, path, archive))
    }

    /// A cell is created, updated, listed and retracted, and every value it
    /// held is a record of its own: the file grows with each update, and
    /// reopens with the last.
    #[test]
    fn it_keeps_a_cell_in_the_file() -> anyhow::Result<()> {
        let (_directory, path, archive) = archive()?;
        assert_eq!(archive.resolve_cell("local", "main")?, None);
        let first = archive.publish_cell("local", "main", b"first", None)?;
        let before = std::fs::metadata(&path)?.len();
        let second = archive.publish_cell("local", "main", b"second", Some(&first))?;
        assert!(
            std::fs::metadata(&path)?.len() > before,
            "a record appended"
        );
        archive.publish_cell("local", "other", b"x", None)?;
        archive.publish_cell("elsewhere", "main", b"y", None)?;
        assert_eq!(archive.list_cells("local")?, vec!["main", "other"]);
        assert_eq!(
            archive.resolve_cell("local", "main")?.as_deref(),
            Some(&b"second"[..])
        );
        archive.retract_cell("local", "other", &version(b"x"))?;
        assert_eq!(archive.list_cells("local")?, vec!["main"]);
        drop(archive);
        let reopened = FileArchive::open(&path)?;
        assert_eq!(
            reopened.resolve_cell("local", "main")?.as_deref(),
            Some(&b"second"[..])
        );
        assert_eq!(reopened.resolve_cell("local", "other")?, None);
        let _ = second;
        Ok(())
    }

    /// A value of any size is kept in its record: no capacity to outgrow.
    #[test]
    fn it_keeps_a_value_of_any_size() -> anyhow::Result<()> {
        let (_directory, path, archive) = archive()?;
        let small = archive.publish_cell("local", "main", b"small", None)?;
        let large = vec![7u8; 200 * 1024];
        archive.publish_cell("local", "main", &large, Some(&small))?;
        assert_eq!(
            archive.resolve_cell("local", "main")?.as_deref(),
            Some(&large[..])
        );
        drop(archive);
        assert_eq!(
            FileArchive::open(&path)?.resolve_cell("local", "main")?,
            Some(large)
        );
        Ok(())
    }

    /// A publish naming an edition the cell is not at is refused with what
    /// it holds, and writes nothing.
    #[test]
    fn it_refuses_a_stale_publish_and_writes_nothing() -> anyhow::Result<()> {
        let (_directory, path, archive) = archive()?;
        let first = archive.publish_cell("local", "main", b"first", None)?;
        archive.publish_cell("local", "main", b"second", Some(&first))?;
        let before = std::fs::metadata(&path)?.len();
        let refused = archive.publish_cell("local", "main", b"third", Some(&first));
        assert!(matches!(
            refused,
            Err(MemoryError::VersionMismatch {
                current: Current::Holds(_),
                ..
            })
        ));
        let refused = archive.publish_cell("local", "fresh", b"x", Some(&first));
        assert!(matches!(
            refused,
            Err(MemoryError::VersionMismatch {
                current: Current::Empty,
                ..
            })
        ));
        assert_eq!(std::fs::metadata(&path)?.len(), before, "nothing written");
        assert_eq!(
            archive.resolve_cell("local", "main")?.as_deref(),
            Some(&b"second"[..])
        );
        Ok(())
    }

    /// Another handle on the file, as another process has, reads a value
    /// written through this one as soon as it is published.
    #[test]
    fn it_reads_another_handles_update_at_once() -> anyhow::Result<()> {
        let (_directory, path, archive) = archive()?;
        let other = FileArchive::open_unshared(&path, super::super::Options::default())?;
        let first = archive.publish_cell("local", "main", b"first", None)?;
        assert_eq!(
            other.resolve_cell("local", "main")?.as_deref(),
            Some(&b"first"[..])
        );
        archive.publish_cell("local", "main", b"second", Some(&first))?;
        assert_eq!(
            other.resolve_cell("local", "main")?.as_deref(),
            Some(&b"second"[..])
        );
        other
            .inner
            .dies
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}
