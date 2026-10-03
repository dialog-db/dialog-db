use crate::{Key, Manifest, Value};
use dialog_common::Blake3Hash;

/// A key-value pair stored in the tree.
#[derive(Clone, Debug)]
pub struct Entry<Key, Value> {
    /// The key for this entry.
    pub key: Key,
    /// The value associated with the key.
    pub value: Value,
}

impl<Key, Value> Entry<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
{
    /// Computes the [`Blake3Hash`] of the entry's key.
    pub fn key_hash(&self) -> Blake3Hash {
        Blake3Hash::hash(self.key.as_ref())
    }

    /// The weight this entry contributes toward `manifest.max_segment`:
    /// its key bytes, its value's payload weight
    /// ([`Value::payload_weight`]), and the tree's per-entry encoding
    /// overhead ([`Manifest::entry_overhead`]). The charge every
    /// byte-pacing decision (the leaf coin's bank, stretch and frame
    /// budgets, the edit path's ceiling gates) meters an entry by.
    pub fn weight(&self, manifest: &Manifest) -> usize {
        self.raw_weight() + manifest.entry_overhead()
    }

    /// The weight's content part: key bytes plus the value's payload
    /// weight, without the per-entry overhead. Cached totals keep this and
    /// add `count * entry_overhead` when read under a manifest.
    pub(crate) fn raw_weight(&self) -> usize {
        self.key.as_ref().len() + self.value.payload_weight()
    }
}
