//! What the reader allocates for a frame, measured by a global allocator
//! that records the largest single request while it is armed. This file
//! holds one test so no other test allocates while it is armed.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use dialog_answer::{Dictionary, Error, MAGIC};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

struct Largest;

static ARMED: AtomicBool = AtomicBool::new(false);
static LARGEST: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Largest {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Largest = Largest;

/// A frame is refused before the reader allocates more than the frame
/// could describe. A 13-byte header that declares 2^22 tokens holds none
/// of them, yet the reader reserves room for all of them (16 bytes each,
/// 64 MiB) before reading the first. Declaring `u32::MAX` tokens asks for
/// about 64 GiB, which aborts the process (and overflows the capacity on
/// a 32-bit target such as wasm32) instead of returning
/// [`Error::Truncated`]; the survivable count here shows the same
/// reservation without taking the test process down.
#[dialog_common::test]
fn it_allocates_no_more_than_the_frame_describes() {
    let mut header = Vec::new();
    header.extend_from_slice(MAGIC);
    header.push(0);
    header.extend_from_slice(&0u32.to_le_bytes());
    header.extend_from_slice(&(1u32 << 22).to_le_bytes());
    let bytes: std::sync::Arc<[u8]> = header.into();
    let mut dictionary = Dictionary::default();

    LARGEST.store(0, Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let refused = dictionary.accept(bytes);
    ARMED.store(false, Ordering::Relaxed);
    let largest = LARGEST.load(Ordering::Relaxed);

    assert!(
        matches!(refused, Err(Error::Truncated)),
        "expected Truncated, found {refused:?}"
    );
    assert!(
        largest <= 1024,
        "expected no allocation over 1024 bytes for a 13-byte frame, found one of {largest} bytes"
    );
}
