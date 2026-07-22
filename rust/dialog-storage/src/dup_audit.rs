//! Measurement-only duplicate-store attribution (uncommitted, env-gated).
//!
//! Enabled by `DIALOG_DUP_AUDIT=1`. Three hooks cooperate to attribute
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

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Whether the audit is switched on (`DIALOG_DUP_AUDIT` set), read once.
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
    let mut rows: Vec<(String, [u64; 4])> = counts().lock().expect("audit lock").drain().collect();
    rows.sort_by(|a, b| b.1[2].cmp(&a.1[2]));
    let mut out = String::new();
    for (descriptor, [stores, bytes, dups, dup_bytes]) in rows {
        out.push_str(&format!(
            "\nDUPAUDIT {descriptor}: stores={stores} bytes={bytes} dups={dups} dup_bytes={dup_bytes}"
        ));
    }
    out
}
