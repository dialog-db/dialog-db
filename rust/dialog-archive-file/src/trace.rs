//! A trace of what the archive does to its files, for replaying the file
//! format's pattern elsewhere (the browser spike in
//! `research/single-file-format/browser`).
//!
//! Built with the `trace` feature, on Unix, and inert until [`start`] names
//! a directory: every archive file opened under it is traced from then on.
//! A trace records, in order:
//!
//! - every page of a traced file's mapping the process reads, the first
//!   time in each operation: the mapping is protected when an operation
//!   [begins](begin), and a read of a protected page is caught (`SIGSEGV`),
//!   noted and let through by unprotecting that page alone;
//! - every positioned read, write, change of length and sync of a traced
//!   file, and every lock taken or let go on one of its bytes;
//! - the archive's effects on the file as blocks and cells: a block read
//!   by its hash (an archive `Get` or a tree node read), blocks written with
//!   their bytes, a cell resolved, a cell published with its content.
//!
//! The first two are the file's pattern, the third what a store keeping a
//! block per record would see. Operations are what the caller names with
//! [`begin`] and [`end`]; what happens outside one is recorded too, under
//! no name.
//!
//! The trace is a file: `DTRACE01`, then records, each a tag byte and its
//! fields, little endian (see [`Tag`]).

use std::fs::File;
use std::io::{self, BufWriter, Write as _};
use std::os::fd::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use memmap2::Mmap;
use parking_lot::Mutex;

/// The bytes of a page: what one caught read unprotects.
pub const PAGE: u64 = 4096;

/// What a record of the trace is.
#[repr(u8)]
#[derive(Clone, Copy, Debug)]
pub enum Tag {
    /// An operation begins: its name (u16 length, bytes).
    Begin = 1,
    /// The operation ends: how long it took, in nanoseconds (u64), traced.
    End = 2,
    /// A page of a mapping read: file (u8), page (u64).
    Page = 3,
    /// A positioned read: file (u8), offset (u64), length (u32).
    Read = 4,
    /// A positioned write: file (u8), offset (u64), length (u32), bytes.
    Write = 5,
    /// The file's length set: file (u8), length (u64).
    Len = 6,
    /// The file synced: file (u8).
    Synced = 7,
    /// A lock on a byte: file (u8), byte (u64), flags (u8: exclusive 1,
    /// waited for 2, taken 4).
    Lock = 8,
    /// A lock let go: file (u8), byte (u64).
    Unlock = 9,
    /// A block read by its hash: file (u8), hash (32 bytes), found (u8).
    Get = 10,
    /// A block written: file (u8), hash (32 bytes), length (u32), bytes.
    Put = 11,
    /// A cell resolved: file (u8), name (u16 length, bytes).
    Resolve = 12,
    /// A cell published: file (u8), name (u16 length, bytes), content (u32
    /// length, bytes).
    Publish = 13,
    /// A traced file opened: file (u8), its path (u16 length, bytes).
    Open = 14,
}

/// When the operation running began.
static BEGAN: Mutex<Option<std::time::Instant>> = Mutex::new(None);

/// Where traced files are, once [`start`] names it.
static UNDER: Mutex<Option<PathBuf>> = Mutex::new(None);

/// The trace's file.
static OUT: Mutex<Option<BufWriter<File>>> = Mutex::new(None);

/// The descriptors of traced files, by their number in the trace; -1 for
/// none.
static FILES: [AtomicI32; 16] = [const { AtomicI32::new(-1) }; 16];

/// Mappings of traced files: their start and length, and the file's
/// number, read by the handler. A length of 0 is a free entry.
static REGIONS: [(AtomicUsize, AtomicUsize, AtomicUsize); 256] = [const {
    (
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    )
}; 256];

/// The mappings behind `REGIONS`, so one is protected only while alive.
static LIVE: Mutex<Vec<(usize, Weak<Mmap>)>> = Mutex::new(Vec::new());

/// Pages the handler caught, not yet written to the trace: each its file's
/// number in the top byte and its page below, plus one (0 is empty).
const CAUGHT: usize = 1 << 20;
static RING: [AtomicU64; CAUGHT] = [const { AtomicU64::new(0) }; CAUGHT];
static HEAD: AtomicUsize = AtomicUsize::new(0);
/// How far the trace has written the ring; only `drain` moves it, holding
/// the trace's file.
static TAIL: AtomicUsize = AtomicUsize::new(0);
static OVERFLOW: AtomicBool = AtomicBool::new(false);

/// The handler that was installed before this one, for faults not ours: a
/// leaked `sigaction`, 0 until the handler is installed.
static PREVIOUS: AtomicUsize = AtomicUsize::new(0);

/// Traces every archive file opened under `under` from now on, into `out`.
pub fn start(out: &Path, under: &Path) -> io::Result<()> {
    let mut file = BufWriter::with_capacity(1 << 20, File::create(out)?);
    file.write_all(b"DTRACE01")?;
    *OUT.lock() = Some(file);
    *UNDER.lock() = Some(under.to_path_buf());
    install()
}

/// Writes out what the trace holds and stops it.
pub fn finish() -> io::Result<()> {
    let mut out = OUT.lock();
    drain(&mut out)?;
    if let Some(mut file) = out.take() {
        file.flush()?;
    }
    *UNDER.lock() = None;
    Ok(())
}

/// An operation named `name` begins: the mappings of traced files are
/// protected, so each page it reads is caught once.
pub fn begin(name: &str) {
    let mut out = OUT.lock();
    if out.is_none() {
        return;
    }
    protect();
    *BEGAN.lock() = Some(std::time::Instant::now());
    record(&mut out, |bytes| {
        bytes.push(Tag::Begin as u8);
        text(bytes, name.as_bytes());
    });
}

/// The operation ends.
pub fn end() {
    let took = BEGAN
        .lock()
        .take()
        .map_or(0, |began| began.elapsed().as_nanos() as u64);
    let mut out = OUT.lock();
    record(&mut out, |bytes| {
        bytes.push(Tag::End as u8);
        bytes.extend_from_slice(&took.to_le_bytes());
    });
}

fn text(bytes: &mut Vec<u8>, text: &[u8]) {
    let len = text.len().min(u16::MAX as usize);
    bytes.extend_from_slice(&(len as u16).to_le_bytes());
    bytes.extend_from_slice(&text[..len]);
}

/// Writes the pages caught so far, then the record `fill` makes.
fn record(out: &mut Option<BufWriter<File>>, fill: impl FnOnce(&mut Vec<u8>)) {
    if out.is_none() {
        return;
    }
    let mut bytes = Vec::new();
    fill(&mut bytes);
    let written =
        drain(out).and_then(|()| out.as_mut().map_or(Ok(()), |file| file.write_all(&bytes)));
    if let Err(error) = written {
        panic!("the trace could not be written: {error}");
    }
}

fn drain(out: &mut Option<BufWriter<File>>) -> io::Result<()> {
    let Some(file) = out.as_mut() else {
        return Ok(());
    };
    assert!(
        !OVERFLOW.load(Ordering::Acquire),
        "more pages were caught between two records than the trace holds"
    );
    let mut tail = TAIL.load(Ordering::Acquire);
    let head = HEAD.load(Ordering::Acquire);
    while tail < head {
        let entry = &RING[tail % CAUGHT];
        let mut value = entry.load(Ordering::Acquire);
        while value == 0 {
            std::hint::spin_loop();
            value = entry.load(Ordering::Acquire);
        }
        entry.store(0, Ordering::Release);
        let value = value - 1;
        file.write_all(&[Tag::Page as u8, (value >> 56) as u8])?;
        file.write_all(&(value & ((1 << 56) - 1)).to_le_bytes())?;
        tail += 1;
        TAIL.store(tail, Ordering::Release);
    }
    Ok(())
}

/// The trace's number for `file`, if it is traced.
fn number(file: &File) -> Option<u8> {
    let fd = file.as_raw_fd();
    FILES
        .iter()
        .position(|traced| traced.load(Ordering::Relaxed) == fd)
        .map(|n| n as u8)
}

/// Traces `file`, opened at `path`, if it is under the traced directory.
pub(crate) fn opened(file: &File, path: &Path) {
    let under = UNDER.lock().clone();
    let Some(under) = under else {
        return;
    };
    if !path.starts_with(&under) {
        return;
    }
    let fd = file.as_raw_fd();
    let Some(n) = FILES.iter().position(|traced| {
        traced
            .compare_exchange(-1, fd, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }) else {
        panic!("more files to trace than the trace numbers");
    };
    let mut out = OUT.lock();
    record(&mut out, |bytes| {
        bytes.extend_from_slice(&[Tag::Open as u8, n as u8]);
        text(bytes, path.to_string_lossy().as_bytes());
    });
}

/// A new mapping of `file`: caught from the next operation on, and from
/// now on if one runs.
pub(crate) fn mapped(file: &File, map: &Arc<Mmap>) {
    let Some(n) = number(file) else {
        return;
    };
    let start = map.as_ptr() as usize;
    let len = map.len();
    let mut live = LIVE.lock();
    live.retain(|(entry, weak)| {
        let alive = weak.strong_count() > 0;
        if !alive {
            REGIONS[*entry].1.store(0, Ordering::Release);
        }
        alive
    });
    let Some(entry) =
        (0..REGIONS.len()).find(|&entry| REGIONS[entry].1.load(Ordering::Acquire) == 0)
    else {
        panic!("more mappings to trace than the trace keeps");
    };
    REGIONS[entry].0.store(start, Ordering::Release);
    REGIONS[entry].2.store(n as usize, Ordering::Release);
    REGIONS[entry].1.store(len, Ordering::Release);
    live.push((entry, Arc::downgrade(map)));
    // SAFETY: the mapping is alive (`map` borrows it) and spans `len`
    // bytes from `start`; it only loses its read permission until the
    // handler gives each page caught back.
    unsafe { libc::mprotect(start as *mut libc::c_void, len, libc::PROT_NONE) };
}

/// Protects every live mapping of a traced file.
fn protect() {
    let mut live = LIVE.lock();
    live.retain(|(entry, weak)| {
        let Some(map) = weak.upgrade() else {
            REGIONS[*entry].1.store(0, Ordering::Release);
            return false;
        };
        // SAFETY: `map` keeps the mapping alive while it is protected; the
        // handler gives each page caught its read permission back.
        unsafe {
            libc::mprotect(
                map.as_ptr() as *mut libc::c_void,
                map.len(),
                libc::PROT_NONE,
            )
        };
        true
    });
}

fn install() -> io::Result<()> {
    if PREVIOUS.load(Ordering::Acquire) != 0 {
        return Ok(());
    }
    // SAFETY: a zeroed sigaction is a valid value of the C struct, filled
    // in before use.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = caught as *const () as usize;
    action.sa_flags = libc::SA_SIGINFO | libc::SA_ONSTACK;
    // SAFETY: as above.
    let mut old: libc::sigaction = unsafe { std::mem::zeroed() };
    // SAFETY: both pointers are to live sigaction values; the handler only
    // makes async-signal-safe calls.
    if unsafe { libc::sigaction(libc::SIGSEGV, &action, &mut old) } != 0 {
        return Err(io::Error::last_os_error());
    }
    PREVIOUS.store(Box::into_raw(Box::new(old)) as usize, Ordering::Release);
    Ok(())
}

/// The `SIGSEGV` handler: a read of a protected page of a traced mapping
/// is noted and let through; any other fault goes to the handler before.
extern "C" fn caught(signal: libc::c_int, info: *mut libc::siginfo_t, context: *mut libc::c_void) {
    // SAFETY: the kernel passes a valid siginfo for SA_SIGINFO handlers.
    let address = unsafe { (*info).si_addr() } as usize;
    for (start, len, file) in &REGIONS {
        let len = len.load(Ordering::Acquire);
        let start = start.load(Ordering::Acquire);
        if len == 0 || address < start || address >= start + len {
            continue;
        }
        let page = (address - start) as u64 / PAGE;
        let at = start + (page * PAGE) as usize;
        // SAFETY: the page is within a mapping of a traced file, which is
        // read-only; giving its read permission back is what it had.
        unsafe { libc::mprotect(at as *mut libc::c_void, PAGE as usize, libc::PROT_READ) };
        let slot = HEAD.fetch_add(1, Ordering::AcqRel);
        if slot >= TAIL.load(Ordering::Acquire) + CAUGHT {
            OVERFLOW.store(true, Ordering::Release);
        }
        let value = ((file.load(Ordering::Acquire) as u64) << 56) | page;
        RING[slot % CAUGHT].store(value + 1, Ordering::Release);
        return;
    }
    let previous = PREVIOUS.load(Ordering::Acquire) as *const libc::sigaction;
    // SAFETY: set as the handler was installed, never freed.
    let previous = unsafe { &*previous };
    if previous.sa_flags & libc::SA_SIGINFO != 0 && previous.sa_sigaction > 1 {
        // SAFETY: the previous handler was installed for SA_SIGINFO and is
        // called as the kernel would have.
        let handler: extern "C" fn(libc::c_int, *mut libc::siginfo_t, *mut libc::c_void) =
            unsafe { std::mem::transmute(previous.sa_sigaction) };
        handler(signal, info, context);
    } else {
        // Not ours and no handler to give it to: the default action, as if
        // this handler were not installed.
        // SAFETY: restoring the default disposition and returning re-runs
        // the faulting instruction, which then takes it.
        unsafe { libc::signal(libc::SIGSEGV, libc::SIG_DFL) };
    }
}

fn event(file: &File, fill: impl FnOnce(u8, &mut Vec<u8>)) {
    let Some(n) = number(file) else {
        return;
    };
    let mut out = OUT.lock();
    record(&mut out, |bytes| fill(n, bytes));
}

pub(crate) fn read(file: &File, offset: u64, len: usize) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Read as u8, n]);
        bytes.extend_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&(len as u32).to_le_bytes());
    });
}

pub(crate) fn write(file: &File, offset: u64, written: &[u8]) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Write as u8, n]);
        bytes.extend_from_slice(&offset.to_le_bytes());
        bytes.extend_from_slice(&(written.len() as u32).to_le_bytes());
        bytes.extend_from_slice(written);
    });
}

pub(crate) fn len(file: &File, len: u64) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Len as u8, n]);
        bytes.extend_from_slice(&len.to_le_bytes());
    });
}

pub(crate) fn sync(file: &File) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Synced as u8, n])
    });
}

pub(crate) fn lock(file: &File, byte: u64, exclusive: bool, wait: bool, taken: bool) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Lock as u8, n]);
        bytes.extend_from_slice(&byte.to_le_bytes());
        bytes.push(u8::from(exclusive) | (u8::from(wait) << 1) | (u8::from(taken) << 2));
    });
}

pub(crate) fn unlock(file: &File, byte: u64) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Unlock as u8, n]);
        bytes.extend_from_slice(&byte.to_le_bytes());
    });
}

pub(crate) fn get(file: &File, hash: &[u8; 32], found: bool) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Get as u8, n]);
        bytes.extend_from_slice(hash);
        bytes.push(u8::from(found));
    });
}

pub(crate) fn put(file: &File, hash: &[u8; 32], block: &[u8]) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Put as u8, n]);
        bytes.extend_from_slice(hash);
        bytes.extend_from_slice(&(block.len() as u32).to_le_bytes());
        bytes.extend_from_slice(block);
    });
}

pub(crate) fn resolve(file: &File, name: &str) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Resolve as u8, n]);
        text(bytes, name.as_bytes());
    });
}

pub(crate) fn publish(file: &File, name: &str, content: &[u8]) {
    event(file, |n, bytes| {
        bytes.extend_from_slice(&[Tag::Publish as u8, n]);
        text(bytes, name.as_bytes());
        bytes.extend_from_slice(&(content.len() as u32).to_le_bytes());
        bytes.extend_from_slice(content);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FileArchive;
    use dialog_common::Buffer;

    /// The records of a trace, as (tag, the file's number) pairs, with the
    /// pages read.
    fn parse(bytes: &[u8]) -> (Vec<u8>, Vec<u64>) {
        assert_eq!(&bytes[..8], b"DTRACE01");
        let mut at = 8;
        let mut tags = Vec::new();
        let mut pages = Vec::new();
        let u16_at = |at: usize| u16::from_le_bytes([bytes[at], bytes[at + 1]]) as usize;
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
        while at < bytes.len() {
            let tag = bytes[at];
            tags.push(tag);
            at += 1;
            at += match tag {
                1 => 2 + u16_at(at),
                2 => 8,
                3 => {
                    pages.push(u64::from_le_bytes(
                        bytes[at + 1..at + 9].try_into().unwrap(),
                    ));
                    9
                }
                4 => 13,
                5 => 13 + u32_at(at + 9),
                6 => 9,
                7 => 1,
                8 => 10,
                9 => 9,
                10 => 34,
                11 => 37 + u32_at(at + 33),
                12 | 14 => 3 + u16_at(at + 1),
                13 => {
                    let name = u16_at(at + 1);
                    3 + name + 4 + u32_at(at + 3 + name)
                }
                other => panic!("no tag {other}"),
            };
        }
        (tags, pages)
    }

    /// Reads through the mapping inside an operation are caught, page by
    /// page, the archive answering them as it would untraced; positioned
    /// writes, syncs and locks are recorded too.
    #[test]
    fn it_catches_the_pages_an_operation_reads() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let out = directory.path().join("trace");
        let traced = directory.path().join("traced");
        std::fs::create_dir(&traced)?;
        start(&out, &traced)?;
        let archive = FileArchive::open(traced.join("archive"))?;
        let blocks: Vec<Buffer> = (0..64).map(|n| Buffer::from(vec![n as u8; 3000])).collect();
        archive.append("index", &blocks)?;
        archive.sync()?;
        begin("read");
        for block in &blocks {
            assert_eq!(
                archive.get("index", block.blake3_hash())?.as_deref(),
                Some(block.as_ref())
            );
        }
        end();
        drop(archive);
        finish()?;
        let (tags, pages) = parse(&std::fs::read(&out)?);
        let read = tags
            .iter()
            .position(|&tag| tag == Tag::Begin as u8)
            .unwrap();
        let ended = tags.iter().position(|&tag| tag == Tag::End as u8).unwrap();
        let caught = tags[read..ended]
            .iter()
            .filter(|&&tag| tag == Tag::Page as u8)
            .count();
        // 64 blocks of 3000 bytes span at least 47 pages, each caught once.
        assert!(caught >= 47, "caught {caught} pages");
        let mut distinct = pages.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert!(tags.contains(&(Tag::Write as u8)));
        assert!(tags.contains(&(Tag::Synced as u8)));
        assert!(tags.contains(&(Tag::Lock as u8)));
        Ok(())
    }
}
