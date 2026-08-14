use crate::{Key, Value};
use dialog_common::Blake3Hash;

/// A key-value pair stored in the tree.
#[derive(Clone, Debug)]
pub struct Entry<Key, Value> {
    /// The key for this entry.
    pub key: Key,
    /// The value associated with the key.
    pub value: Value,
}

/// Per-entry encoding overhead charged by [`Entry::weight`], calibrated
/// against measured leaf encodings on the real SE dataset: beyond key
/// bytes and the value payload estimate, each entry costs ~64-72 encoded
/// bytes of columnar bookkeeping (front-coding offsets, dictionary and
/// value-table framing, polarity). Without this term the frame ceiling —
/// which provably holds in weight — let encoded BYTES drift to 1.85x the
/// metered weight at p90 (max 2.1x); charging it brings bytes/weight to
/// p50 1.02 / p90 1.05, so the ceiling denominates in effective bytes.
pub const ENTRY_ENCODING_OVERHEAD: usize = 64;

impl<Key, Value> Entry<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
{
    /// Computes the [`Blake3Hash`] of the entry's key.
    pub fn key_hash(&self) -> Blake3Hash {
        Blake3Hash::hash(self.key.as_ref())
    }

    /// The weight this entry contributes toward `Manifest::max_segment`:
    /// its key bytes, its value's payload weight
    /// ([`Value::payload_weight`]), and the per-entry encoding overhead.
    /// The charge every byte-pacing decision (the leaf coin's bank,
    /// stretch and frame budgets, the edit path's ceiling gates) meters an
    /// entry by.
    pub fn weight(&self) -> usize {
        self.key.as_ref().len() + self.value.payload_weight() + ENTRY_ENCODING_OVERHEAD
    }
}
