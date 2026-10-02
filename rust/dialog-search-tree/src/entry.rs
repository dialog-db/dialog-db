use crate::{Hashed, Key, LazyHash, Manifest, Value};
use dialog_common::Blake3Hash;

/// A key-value pair stored in the tree.
///
/// An entry keeps the blake3 hash of its key, computed the first time a
/// boundary rule asks for it (see [`key_hash`](Self::key_hash)). An entry is
/// built for one key and is replaced whole when the key changes; assigning a
/// different key in place would leave the hash of the old one behind.
#[derive(Clone, Debug)]
pub struct Entry<Key, Value> {
    /// The key for this entry.
    pub key: Key,
    /// The value associated with the key.
    pub value: Value,
    /// The hash of `key`, once asked for.
    hash: LazyHash,
}

impl<Key, Value> Entry<Key, Value> {
    /// An entry of `key` and `value`.
    pub fn new(key: Key, value: Value) -> Self {
        Self {
            key,
            value,
            hash: LazyHash::default(),
        }
    }

    /// An entry of `key` and `value` whose key hash is kept in `hash`: the
    /// cell of the stored leaf the entry was opened from.
    pub(crate) fn opened(key: Key, value: Value, hash: LazyHash) -> Self {
        Self { key, value, hash }
    }

    /// The hash of the key, if it has been asked for already.
    pub(crate) fn known_hash(&self) -> Option<Blake3Hash> {
        self.hash.known()
    }
}

impl<Key, Value> Entry<Key, Value>
where
    Key: self::Key,
{
    /// The [`Blake3Hash`] of the entry's key, computed on the first ask and
    /// kept with the entry.
    pub fn key_hash(&self) -> Blake3Hash {
        self.hash.get(self.key.as_ref())
    }

    /// The key's bytes with the cell their hash is kept in: what the
    /// boundary rules of a [`Distribution`](crate::Distribution) decide from.
    pub fn hashed(&self) -> Hashed<'_> {
        Hashed::new(self.key.as_ref(), &self.hash)
    }
}

impl<Key, Value> Entry<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
{
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
