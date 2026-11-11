//! A disk a power loss can be played on, for tests.
//!
//! A test watches a file: from then on every write, change of length and
//! sync the archive makes to it is noted, in order (the archive makes them
//! all through `write_at`, `set_len` and `sync` in the `file` module).
//! [`after_power_loss`] then builds what the disk could hold had the power
//! gone after any of them: everything up to the last sync, which the
//! operating system had made durable, and of what came after it any part,
//! in any order, a write torn at any of its sectors. Nothing the
//! platform promises rules any of those out.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::LazyLock;

use parking_lot::Mutex;

/// What the archive did to a file.
#[derive(Clone, Debug)]
pub(crate) enum Op {
    Write(u64, Vec<u8>),
    Len(u64),
    Flush,
}

/// A file, by what names it whatever path it is reached by.
type Id = (u64, u64);

static WATCHED: LazyLock<Mutex<HashMap<Id, Vec<Op>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Notes `op` on `file`, if a test watches it.
pub(crate) fn note(file: &File, op: impl FnOnce() -> Op) {
    let mut watched = WATCHED.lock();
    if watched.is_empty() {
        return;
    }
    let Ok(id) = super::identity(file) else {
        return;
    };
    if let Some(ops) = watched.get_mut(&id) {
        ops.push(op());
    }
}

/// A file watched, until this is dropped.
pub(crate) struct Watch {
    id: Id,
}

/// Watches the file at `path`, which exists, empty.
pub(crate) fn watch(path: &Path) -> io::Result<Watch> {
    let id = super::identity(&File::open(path)?)?;
    WATCHED.lock().insert(id, Vec::new());
    Ok(Watch { id })
}

impl Watch {
    /// What the archive did to the file so far.
    pub fn ops(&self) -> Vec<Op> {
        WATCHED.lock().get(&self.id).cloned().unwrap_or_default()
    }

    /// How many things the archive did to the file so far.
    pub fn len(&self) -> usize {
        WATCHED.lock().get(&self.id).map_or(0, Vec::len)
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        WATCHED.lock().remove(&self.id);
    }
}

/// The bytes of a sector: what a disk writes whole or not at all.
const SECTOR: u64 = 512;

/// A small generator, so a failing image is made again from its seed.
pub(crate) struct Draw(u64);

impl Draw {
    pub fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn apply(image: &mut Vec<u8>, op: &Op, keep: &mut dyn FnMut() -> bool) {
    match op {
        Op::Len(len) => image.resize(*len as usize, 0),
        Op::Flush => {}
        Op::Write(offset, bytes) => {
            let end = *offset as usize + bytes.len();
            if image.len() < end {
                image.resize(end, 0);
            }
            let mut at = *offset;
            while (at as usize) < end {
                let next = ((at / SECTOR + 1) * SECTOR).min(end as u64);
                if keep() {
                    let from = (at - offset) as usize;
                    let to = (next - offset) as usize;
                    image[at as usize..next as usize].copy_from_slice(&bytes[from..to]);
                }
                at = next;
            }
        }
    }
}

/// What the disk could hold had the power gone after the first `at` of
/// `ops`, as `draw` picks among the possibilities: what the last sync
/// among them made durable, then each later one dropped, kept, or (a
/// write) kept in some of its sectors only. Dropping some and keeping
/// later ones stands for the disk writing them out of order.
pub(crate) fn after_power_loss(ops: &[Op], at: usize, draw: &mut Draw) -> Vec<u8> {
    let synced = ops[..at]
        .iter()
        .rposition(|op| matches!(op, Op::Flush))
        .map_or(0, |last| last + 1);
    let mut image = Vec::new();
    for op in &ops[..synced] {
        apply(&mut image, op, &mut || true);
    }
    for op in &ops[synced..at] {
        match draw.below(3) {
            0 => {}
            1 => apply(&mut image, op, &mut || true),
            _ => {
                let mut tear = Draw::new(draw.next());
                apply(&mut image, op, &mut || tear.below(2) == 0);
            }
        }
    }
    image
}
