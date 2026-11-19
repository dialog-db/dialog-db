//! Where values are hydrated: every value made out of a scanned row,
//! counted by the source line whose read asked for it. Behind the
//! `trace-hydration` feature, for finding hydration nothing needed.

use std::collections::BTreeMap;
use std::mem;
use std::panic::Location;
use std::sync::Mutex;

static COUNTS: Mutex<BTreeMap<(String, &'static str), u64>> = Mutex::new(BTreeMap::new());

/// Counts one hydration of `what`, asked for at `site`.
pub fn record(site: &'static Location<'static>, what: &'static str) {
    let site = format!("{}:{}", site.file(), site.line());
    *COUNTS
        .lock()
        .expect("the hydration counts are not poisoned")
        .entry((site, what))
        .or_default() += 1;
}

/// Counts one hydration of `what`, asked for by the caller.
#[track_caller]
pub fn here(what: &'static str) {
    record(Location::caller(), what);
}

/// The counts so far, by (site, what), and clears them.
pub fn take() -> BTreeMap<(String, &'static str), u64> {
    mem::take(
        &mut *COUNTS
            .lock()
            .expect("the hydration counts are not poisoned"),
    )
}
