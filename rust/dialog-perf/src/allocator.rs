//! The calls the measured phase makes into the allocator, counted by a
//! global allocator that passes every call through to [`System`].
//!
//! Callgrind can count calls too, but it counts them whether it is
//! collecting or not, so its counts take in the setup. These count only
//! between [`start`] and [`stop`], around the measured phase.
//!
//! Besides the calls, two byte counts: the bytes asked for, and the
//! bytes a `realloc` was asked to grow or shrink from. The second is
//! what the allocator may have to copy. Whether it does copy depends on
//! where the heap put the block, and varies between runs; what it may
//! copy depends on how the code grows its buffers, so a buffer grown
//! without a `reserve` shows here.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

/// A global allocator that counts the measured phase's calls. The `perf`
/// binary installs it; without it the counts stay empty.
pub struct Counting;

static ON: AtomicBool = AtomicBool::new(false);
static ALLOC: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static FREE: AtomicU64 = AtomicU64::new(0);
static REALLOC: AtomicU64 = AtomicU64::new(0);
static REALLOC_FROM_BYTES: AtomicU64 = AtomicU64::new(0);

fn alloced(layout: Layout) {
    if ON.load(Relaxed) {
        ALLOC.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Relaxed);
    }
}

// SAFETY: every call is passed to `System` unchanged; the counting
// touches nothing but atomics.
//
// Never inlined, so callgrind sees each method as a function of its own
// that the sweep counts as the allocator's (`sweep::ENTRY_TYPE`), and
// the counting is not in the instructions outside it.
unsafe impl GlobalAlloc for Counting {
    #[inline(never)]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        alloced(layout);
        // SAFETY: the caller's contract, passed on.
        unsafe { System.alloc(layout) }
    }

    #[inline(never)]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        alloced(layout);
        // SAFETY: the caller's contract, passed on.
        unsafe { System.alloc_zeroed(layout) }
    }

    #[inline(never)]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ON.load(Relaxed) {
            FREE.fetch_add(1, Relaxed);
        }
        // SAFETY: the caller's contract, passed on.
        unsafe { System.dealloc(ptr, layout) }
    }

    #[inline(never)]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ON.load(Relaxed) {
            REALLOC.fetch_add(1, Relaxed);
            REALLOC_FROM_BYTES.fetch_add(layout.size() as u64, Relaxed);
        }
        // SAFETY: the caller's contract, passed on.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Start counting from zero.
pub fn start() {
    for counter in [&ALLOC, &ALLOC_BYTES, &FREE, &REALLOC, &REALLOC_FROM_BYTES] {
        counter.store(0, Relaxed);
    }
    ON.store(true, Relaxed);
}

/// Stop counting and return the counts, keyed as a report keeps them;
/// empty when [`Counting`] is not the global allocator, which leaves
/// every count at zero.
pub fn stop() -> BTreeMap<String, u64> {
    ON.store(false, Relaxed);
    let counts = [
        ("alloc.calls", ALLOC.load(Relaxed)),
        ("alloc.bytes", ALLOC_BYTES.load(Relaxed)),
        ("free.calls", FREE.load(Relaxed)),
        ("realloc.calls", REALLOC.load(Relaxed)),
        ("realloc.from.bytes", REALLOC_FROM_BYTES.load(Relaxed)),
    ];
    if counts.iter().all(|(_, count)| *count == 0) {
        return BTreeMap::new();
    }
    counts
        .into_iter()
        .map(|(key, count)| (key.to_string(), count))
        .collect()
}
