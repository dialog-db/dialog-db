//! Memory cells in the file: a branch's head, and every other cell a space
//! keeps.
//!
//! A cell is kept as SQLite keeps a commit in its write-ahead log: every
//! update appends a record of the [cell](Kind::Cell) kind holding the
//! cell's name and its value (or that it was retracted), and the cells' own
//! index (a tree like the blocks', rooted in the slot) files the name, by a
//! key from it, at the newest such record. The slot that names the record
//! is written after it, so whatever a power loss keeps, recovery returns to
//! a slot whose every record since the durable slot is whole: a value in
//! the file's log is there with every block the log holds before it. As in
//! SQLite with `synchronous=NORMAL`, an update is made durable by the next
//! sync point, not by one of its own; a power loss can undo the last
//! updates, never leave one naming what is gone.
//!
//! Unlike SQLite, a writer's blocks need not be in the log before the
//! value: they may still be in this process's extents, written without the
//! turn. The record names them instead: each of this process's extents
//! that holds blocks, by its record, epoch and how many bytes of frames it
//! held. A record is whole only if every extent it names that the slot
//! still lists in that epoch holds whole frames up to there (one the slot
//! no longer lists, or lists claimed again, was folded into the log before
//! the slot). So a cell's update holds the writer's turn for the append of
//! one small record, never for folding blocks in.
//!
//! Readers take no lock: a read looks again at the slots when the file
//! grew, as a SQLite reader looks for a newer commit when it starts.
//!
//! The record's body: a byte of padding and the padding, so what follows
//! starts on an [`ALIGN`] boundary (the index names the record by where
//! that is); the name's length (u16) and the name (the memory space, a
//! zero byte, the cell); a byte, 1 when a value follows and 0 when the
//! cell was retracted; the value's length (u32) and the value; then how
//! many extents it names (u16), each its record, epoch and bytes of frames
//! (u64 each).

use std::io;

use dialog_capability::{Capability, Provider, Resume};
use dialog_common::Blake3Hash;
use dialog_effects::memory::prelude::{ListExt, PublishExt, ResolveExt, RetractExt};
use dialog_effects::memory::{
    Current, Edition, List, MemoryError, Publish, Resolve, Retract, Version,
};

use super::index::Snapshot;
use super::{Chronicle, Laid, write_index};
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
pub(crate) fn record(
    start: u64,
    name: &[u8],
    value: Option<&[u8]>,
    needs: &[Needed],
) -> io::Result<(Vec<u8>, u32)> {
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
    let count =
        u16::try_from(needs.len()).map_err(|_| invalid("a cell naming too many extents"))?;
    bytes.extend_from_slice(&count.to_le_bytes());
    for needed in needs {
        bytes.extend_from_slice(&needed.record.to_le_bytes());
        bytes.extend_from_slice(&needed.epoch.to_le_bytes());
        bytes.extend_from_slice(&needed.used.to_le_bytes());
    }
    let mut out = Vec::with_capacity(HEADER as usize + bytes.len());
    put_record(&mut out, Kind::Cell, &bytes)?;
    let at = u32::try_from(data / ALIGN).map_err(|_| invalid("a cell past 64 GiB"))?;
    Ok((out, at))
}

/// An extent a cell's record needs whole: by its record, in the epoch it
/// was claimed in, up to how many bytes of frames it held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Needed {
    pub record: u64,
    pub epoch: u64,
    pub used: u64,
}

/// The extents the cell record whose body starts at `body` in `bytes`
/// needs whole (see the module docs).
pub(crate) fn needs(bytes: &[u8], body: usize) -> io::Result<Vec<Needed>> {
    let pad = *bytes
        .get(body)
        .ok_or_else(|| invalid("a cell past the file's end"))? as usize;
    let data = body + 1 + pad;
    let (name, value) = read(bytes, data as u64)?;
    let mut at = data + 2 + name.len() + 1 + 4 + value.map_or(0, <[u8]>::len);
    let word = |at: usize| -> io::Result<u64> {
        Ok(u64::from_le_bytes(
            bytes
                .get(at..at + 8)
                .ok_or_else(|| invalid("a cell past the file's end"))?
                .try_into()
                .expect("eight bytes"),
        ))
    };
    let count = u16::from_le_bytes(
        bytes
            .get(at..at + 2)
            .ok_or_else(|| invalid("a cell past the file's end"))?
            .try_into()
            .expect("two bytes"),
    );
    at += 2;
    (0..count)
        .map(|n| {
            let at = at + n as usize * 24;
            Ok(Needed {
                record: word(at)?,
                epoch: word(at + 8)?,
                used: word(at + 16)?,
            })
        })
        .collect()
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

impl Chronicle {
    /// This process's extents that hold blocks, as `snapshot` maps them: what
    /// a cell's record it appends now needs whole.
    fn loose_needed(&self, snapshot: &Snapshot) -> Vec<Needed> {
        let extents = self.inner.extents.lock();
        let ours = extents
            .idle
            .iter()
            .map(|owned| owned.record)
            .chain(extents.busy.iter().copied());
        ours.filter_map(|record| {
            let used = super::extent::used(snapshot.bytes(), record)?;
            let epoch = super::extent::epoch(snapshot.bytes(), record)?;
            (used > 0).then_some(Needed {
                record,
                epoch,
                used,
            })
        })
        .collect()
    }

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
        #[cfg(test)]
        let waiting = std::time::Instant::now();
        let turn = self.turn().map_err(storage)?;
        #[cfg(test)]
        self.inner.turn_waited.store(
            waiting.elapsed().as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
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
        // The blocks the value may name that are not in the log yet: in
        // this process's extents, as far as each held whole frames.
        let needs = self.loose_needed(&snapshot);
        let (mut bytes, at) = record(start, name, value, &needs).map_err(storage)?;
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
        .map_err(storage)?;
        drop(turn);
        self.sync_if_due().map_err(storage)
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

impl Provider<Resolve> for Chronicle {
    fn execute(
        &self,
        effect: Capability<Resolve>,
    ) -> Resume<'_, Result<Option<Edition<Vec<u8>>>, MemoryError>> {
        #[cfg(all(feature = "trace", unix))]
        crate::trace::resolve(
            &self.inner.file,
            &format!("{}/{}", effect.space(), effect.cell()),
        );
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

impl Provider<Publish> for Chronicle {
    fn execute(&self, effect: Capability<Publish>) -> Resume<'_, Result<Version, MemoryError>> {
        #[cfg(all(feature = "trace", unix))]
        crate::trace::publish(
            &self.inner.file,
            &format!("{}/{}", effect.space(), effect.cell()),
            effect.content(),
        );
        Resume::now(self.publish_cell(
            effect.space(),
            effect.cell(),
            effect.content(),
            effect.when(),
        ))
    }
}

impl Provider<Retract> for Chronicle {
    fn execute(&self, effect: Capability<Retract>) -> Resume<'_, Result<(), MemoryError>> {
        Resume::now(self.retract_cell(effect.space(), effect.cell(), effect.when()))
    }
}

impl Provider<List> for Chronicle {
    fn execute(&self, effect: Capability<List>) -> Resume<'_, Result<Vec<String>, MemoryError>> {
        Resume::now(self.list_cells(effect.space()).map_err(storage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive() -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf, Chronicle)> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("archive");
        let archive = Chronicle::open(&path)?;
        Ok((directory, path, archive))
    }

    /// A cell is created, updated, listed and retracted, and every value it
    /// held is a record of its own: the file grows with each update, and
    /// reopens with the last.
    #[dialog_common::test]
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
        let reopened = Chronicle::open(&path)?;
        assert_eq!(
            reopened.resolve_cell("local", "main")?.as_deref(),
            Some(&b"second"[..])
        );
        assert_eq!(reopened.resolve_cell("local", "other")?, None);
        let _ = second;
        Ok(())
    }

    /// A value of any size is kept in its record: no capacity to outgrow.
    #[dialog_common::test]
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
            Chronicle::open(&path)?.resolve_cell("local", "main")?,
            Some(large)
        );
        Ok(())
    }

    /// A publish naming an edition the cell is not at is refused with what
    /// it holds, and writes nothing.
    #[dialog_common::test]
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
    #[dialog_common::test]
    fn it_reads_another_handles_update_at_once() -> anyhow::Result<()> {
        let (_directory, path, archive) = archive()?;
        let other = Chronicle::open_unshared(&path, super::super::Options::default())?;
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
