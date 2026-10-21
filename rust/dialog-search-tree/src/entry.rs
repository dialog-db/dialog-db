use std::borrow::Borrow;
use std::ops::Deref;

use crate::{Hashed, Key, LazyHash, Manifest, Value};
use dialog_common::Blake3Hash;

/// A key-value pair stored in the tree.
///
/// What a read yields: the key and the value, nothing else. The hash of the
/// key, which only the boundary rules read, is kept by the segment the entry
/// is edited in (see [`SegmentEntry`]), so an entry passed along a scan does
/// not carry it.
#[derive(Clone, Debug)]
pub struct Entry<Key, Value> {
    /// The key for this entry.
    pub key: Key,
    /// The value associated with the key.
    pub value: Value,
}

impl<Key, Value> Entry<Key, Value> {
    /// An entry of `key` and `value`.
    pub fn new(key: Key, value: Value) -> Self {
        Self { key, value }
    }
}

impl<Key, Value> Entry<Key, Value>
where
    Key: self::Key,
{
    /// Computes the [`Blake3Hash`] of the entry's key. Nothing keeps it:
    /// [`SegmentEntry::key_hash`] is the one the boundary rules ask.
    pub fn key_hash(&self) -> Blake3Hash {
        Blake3Hash::hash(self.key.as_ref())
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

/// An entry as a [`TransientSegment`](crate::TransientSegment) holds it:
/// with the blake3 hash of its key, computed the first time a boundary rule
/// asks for it (see [`key_hash`](Self::key_hash)).
///
/// It reads as the [`Entry`] it wraps. It is built for one key and is
/// replaced whole when the key changes; only its value is edited in place,
/// which leaves the hash of the key true.
#[derive(Clone, Debug)]
pub struct SegmentEntry<Key, Value> {
    entry: Entry<Key, Value>,
    /// The hash of `entry.key`, once asked for.
    hash: LazyHash,
}

impl<Key, Value> SegmentEntry<Key, Value> {
    /// An entry of `key` and `value` whose key hash is kept in `hash`: the
    /// cell of the stored leaf the entry was opened from.
    pub(crate) fn opened(key: Key, value: Value, hash: LazyHash) -> Self {
        Self {
            entry: Entry::new(key, value),
            hash,
        }
    }

    /// The entry, taken out of the segment's keeping.
    pub fn into_entry(self) -> Entry<Key, Value> {
        self.entry
    }

    /// Replaces the value, keeping the key and its hash.
    pub fn set_value(&mut self, value: Value) {
        self.entry.value = value;
    }

    /// The hash of the key, if it has been asked for already.
    pub(crate) fn known_hash(&self) -> Option<Blake3Hash> {
        self.hash.known()
    }
}

impl<Key, Value> SegmentEntry<Key, Value>
where
    Key: self::Key,
{
    /// The [`Blake3Hash`] of the entry's key, computed on the first ask and
    /// kept with the entry.
    pub fn key_hash(&self) -> Blake3Hash {
        self.hash.get(self.entry.key.as_ref())
    }

    /// The key's bytes with the cell their hash is kept in: what the
    /// boundary rules of a [`Distribution`](crate::Distribution) decide from.
    pub fn hashed(&self) -> Hashed<'_> {
        Hashed::new(self.entry.key.as_ref(), &self.hash)
    }
}

/// An entry no stored leaf holds: its key is hashed when first asked for.
impl<Key, Value> From<Entry<Key, Value>> for SegmentEntry<Key, Value> {
    fn from(entry: Entry<Key, Value>) -> Self {
        Self {
            entry,
            hash: LazyHash::default(),
        }
    }
}

impl<Key, Value> From<SegmentEntry<Key, Value>> for Entry<Key, Value> {
    fn from(entry: SegmentEntry<Key, Value>) -> Self {
        entry.entry
    }
}

impl<Key, Value> Deref for SegmentEntry<Key, Value> {
    type Target = Entry<Key, Value>;

    fn deref(&self) -> &Entry<Key, Value> {
        &self.entry
    }
}

impl<Key, Value> Borrow<Entry<Key, Value>> for SegmentEntry<Key, Value> {
    fn borrow(&self) -> &Entry<Key, Value> {
        &self.entry
    }
}
