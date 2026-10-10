//! Hash accounting for measurement, compiled in by the `audit` feature.
//!
//! Every hash the shaping paths ask for is counted by purpose, and every
//! hash actually computed is counted as [`hashed`], so a replay can attribute
//! hash cost and see how much of what was asked was already kept. A harness
//! snapshots and resets the counters with `report`.
//!
//! Without the feature every call here is empty: a build that does not
//! measure pays nothing for the counters, and keeps none.

#[cfg(feature = "audit")]
use std::sync::atomic::{AtomicU64, Ordering};

/// How many times something happened, and over how many bytes.
struct Tally {
    #[cfg(feature = "audit")]
    count: AtomicU64,
    #[cfg(feature = "audit")]
    bytes: AtomicU64,
}

impl Tally {
    const fn new() -> Self {
        Self {
            #[cfg(feature = "audit")]
            count: AtomicU64::new(0),
            #[cfg(feature = "audit")]
            bytes: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn add(&self, bytes: usize) {
        #[cfg(feature = "audit")]
        {
            self.count.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
        #[cfg(not(feature = "audit"))]
        let _ = (self, bytes);
    }

    /// The count and bytes since the last drain.
    #[cfg(feature = "audit")]
    fn drain(&self) -> (u64, u64) {
        (
            self.count.swap(0, Ordering::Relaxed),
            self.bytes.swap(0, Ordering::Relaxed),
        )
    }
}

static KEY: Tally = Tally::new();
static SEAM: Tally = Tally::new();
static ELECTION: Tally = Tally::new();
static NODE: Tally = Tally::new();
static HASHED: Tally = Tally::new();

/// A leaf coin asked for the hash of a key of `bytes` bytes.
#[inline(always)]
pub fn key(bytes: usize) {
    KEY.add(bytes)
}

/// A seam coin asked for the hash of a separator of `bytes` bytes.
#[inline(always)]
pub fn seam(bytes: usize) {
    SEAM.add(bytes)
}

/// An anchor election asked for the hash of `bytes` bytes.
#[inline(always)]
pub fn election(bytes: usize) {
    ELECTION.add(bytes)
}

/// A node of `bytes` bytes was sealed.
#[inline(always)]
pub fn node(bytes: usize) {
    NODE.add(bytes)
}

/// A key or separator hash was computed over `bytes` bytes, not found kept.
#[inline(always)]
pub fn hashed(bytes: usize) {
    HASHED.add(bytes)
}

/// Every counter since the last report, as one line, resetting them.
#[cfg(feature = "audit")]
pub fn report() -> String {
    let (key_hashes, key_bytes) = KEY.drain();
    let (seam_hashes, seam_bytes) = SEAM.drain();
    let (election_hashes, election_bytes) = ELECTION.drain();
    let (node_hashes, node_bytes) = NODE.drain();
    let (hashed, hashed_bytes) = HASHED.drain();
    format!(
        "key_hashes={key_hashes} key_bytes={key_bytes} seam_hashes={seam_hashes} seam_bytes={seam_bytes} election_hashes={election_hashes} election_bytes={election_bytes} node_hashes={node_hashes} node_bytes={node_bytes} hashed={hashed} hashed_bytes={hashed_bytes}"
    )
}

#[cfg(all(test, feature = "audit"))]
mod tests {
    use super::{hashed, report};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// What is counted comes back in the report. Other tests in the same
    /// process may count too, so this reads a floor, not an exact figure.
    #[dialog_common::test]
    fn it_reports_what_was_counted() {
        hashed(7);

        let line = report();
        let field = |name: &str| -> u64 {
            line.split_whitespace()
                .find_map(|pair| pair.strip_prefix(name)?.strip_prefix('='))
                .and_then(|value| value.parse().ok())
                .unwrap_or_default()
        };
        assert!(field("hashed") >= 1, "{line}");
        assert!(field("hashed_bytes") >= 7, "{line}");
    }
}
