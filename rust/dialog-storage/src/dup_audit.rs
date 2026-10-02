//! Duplicate-store attribution for measurement, compiled in by the `audit`
//! feature.
//!
//! Without the feature every hook here is empty and nothing is kept. With
//! it, block stores are counted ([`note_set`]), and `DIALOG_DUP_AUDIT=1`
//! additionally switches on three hooks that cooperate to attribute
//! byte-identical re-stores of content-addressed blocks:
//!
//! - [`note_lift`]: a persistent node was lifted into transient (editable)
//!   form; records the node's hash with a tag naming the lift call path.
//! - [`note_seal`]: a transient node was sealed (encoded + hashed) into a
//!   block; records the block hash with a descriptor combining the node kind
//!   and the lift tag, if any, for that same hash. A lifted-but-unmodified
//!   node re-seals to its original hash, so the lookup attributes exactly the
//!   wasteful case; a modified node seals to a fresh hash and finds no tag.
//! - [`note_store`]: the block store observed a set/import; bumps per
//!   descriptor totals and duplicate counts.
//!
//! All maps live behind a `Mutex` and cost nothing unless the env gate is on.

pub use hooks::*;

/// The hooks of a build that does not measure: all empty.
#[cfg(not(feature = "audit"))]
mod hooks {
    /// Whether the attribution hooks are switched on: never, in this build.
    #[inline(always)]
    pub fn enabled() -> bool {
        false
    }

    /// Records a lift. Empty in this build.
    #[inline(always)]
    pub fn note_lift(_hash: &[u8; 32], _tag: &'static str) {}

    /// Records a seal. Empty in this build.
    #[inline(always)]
    pub fn note_seal(_hash: &[u8; 32], _kind: &str) {}

    /// Records a block store. Empty in this build.
    #[inline(always)]
    pub fn note_store(_hash: &[u8; 32], _site: &str, _bytes: usize, _duplicate: bool) {}
}

#[cfg(feature = "audit")]
mod hooks {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, OnceLock};

    /// Archive block stores whose block was already stored, since the last
    /// drain.
    static DUPLICATE_SETS: AtomicU64 = AtomicU64::new(0);
    /// Archive block stores since the last drain.
    static TOTAL_SETS: AtomicU64 = AtomicU64::new(0);

    /// Counts an archive block store, `duplicate` when the block was already
    /// stored.
    pub fn note_set(duplicate: bool) {
        DUPLICATE_SETS.fetch_add(u64::from(duplicate), Ordering::Relaxed);
        TOTAL_SETS.fetch_add(1, Ordering::Relaxed);
    }

    /// The archive block stores since the last drain, as `(duplicates, total)`,
    /// resetting both.
    pub fn drain_sets() -> (u64, u64) {
        (
            DUPLICATE_SETS.swap(0, Ordering::Relaxed),
            TOTAL_SETS.swap(0, Ordering::Relaxed),
        )
    }

    /// Whether the attribution hooks are switched on (`DIALOG_DUP_AUDIT` set),
    /// read once.
    pub fn enabled() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| std::env::var("DIALOG_DUP_AUDIT").is_ok())
    }

    /// Hash of a lifted persistent node -> the call path that lifted it.
    fn lifts() -> &'static Mutex<HashMap<[u8; 32], &'static str>> {
        static MAP: OnceLock<Mutex<HashMap<[u8; 32], &'static str>>> = OnceLock::new();
        MAP.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Hash of a sealed block -> its descriptor ("kind/lift-tag").
    fn seals() -> &'static Mutex<HashMap<[u8; 32], String>> {
        static MAP: OnceLock<Mutex<HashMap<[u8; 32], String>>> = OnceLock::new();
        MAP.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Descriptor -> [store count, store bytes, duplicate count, duplicate bytes].
    fn counts() -> &'static Mutex<HashMap<String, [u64; 4]>> {
        static MAP: OnceLock<Mutex<HashMap<String, [u64; 4]>>> = OnceLock::new();
        MAP.get_or_init(|| Mutex::new(HashMap::new()))
    }

    /// Records that the persistent node at `hash` was lifted to transient form by
    /// the call path named `tag`.
    pub fn note_lift(hash: &[u8; 32], tag: &'static str) {
        if !enabled() {
            return;
        }
        lifts().lock().expect("audit lock").insert(*hash, tag);
    }

    /// Records that a transient node of `kind` sealed into the block at `hash`.
    /// The descriptor stored for the hash is `kind/lift-tag` when the same hash
    /// was previously lifted (an unmodified re-seal), `kind/fresh` otherwise.
    pub fn note_seal(hash: &[u8; 32], kind: &str) {
        if !enabled() {
            return;
        }
        let tag = lifts()
            .lock()
            .expect("audit lock")
            .get(hash)
            .copied()
            .unwrap_or("fresh");
        seals()
            .lock()
            .expect("audit lock")
            .insert(*hash, format!("{kind}/{tag}"));
    }

    /// Records a block store at `site` of `bytes` bytes; `duplicate` when the
    /// store's key already existed (a byte-identical re-store).
    pub fn note_store(hash: &[u8; 32], site: &str, bytes: usize, duplicate: bool) {
        if !enabled() {
            return;
        }
        let descriptor = seals()
            .lock()
            .expect("audit lock")
            .get(hash)
            .cloned()
            .unwrap_or_else(|| "unregistered".to_string());
        let mut counts = counts().lock().expect("audit lock");
        let entry = counts
            .entry(format!("{site} {descriptor}"))
            .or_insert([0; 4]);
        entry[0] += 1;
        entry[1] += bytes as u64;
        if duplicate {
            entry[2] += 1;
            entry[3] += bytes as u64;
        }
    }

    /// Drains the counters into a sorted human-readable table, one line per
    /// descriptor, duplicates first.
    pub fn report() -> String {
        let mut rows: Vec<(String, [u64; 4])> =
            counts().lock().expect("audit lock").drain().collect();
        rows.sort_by(|a, b| b.1[2].cmp(&a.1[2]));
        let mut out = String::new();
        for (descriptor, [stores, bytes, dups, dup_bytes]) in rows {
            out.push_str(&format!(
            "\nDUPAUDIT {descriptor}: stores={stores} bytes={bytes} dups={dups} dup_bytes={dup_bytes}"
        ));
        }
        out
    }
}
