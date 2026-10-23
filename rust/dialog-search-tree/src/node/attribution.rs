//! Where a persisted node's bytes go, component by component.
//!
//! A frame's size is what a reader pays to fetch it and what a writer pays
//! to rewrite it, and the tree's pacing never sees that size: it meters a
//! **weight** per entry (key bytes, the value's
//! [`payload_weight`](crate::Value::payload_weight) and the manifest's
//! per-entry overhead), a proxy chosen to be a pure function of the entry.
//! An attribution puts the two side by side: the bytes the frame actually
//! holds for its links, separators, keys and values, and the weight the
//! pacing charged for the same entries.

use std::collections::BTreeMap;

use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    DialogSearchTreeError, Key, Manifest, NodeBody, NoveltyOp, PersistentNode, Value,
    node::columnar::{ColumnSlices, archived_column_slices},
};

/// The bytes of one persisted node, attributed to what they carry.
///
/// Every field is in bytes except `children` and `entries`, which count.
/// The components sum to `total`: `prelude + hashes + separators + scales +
/// keys + values + polarity + structure`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FrameAttribution {
    /// The frame's whole length.
    pub total: usize,
    /// The prelude: layout tag, kind and manifest fields, zero-padded to
    /// the body's alignment.
    pub prelude: usize,
    /// How many children an index has; 0 for a segment.
    pub children: usize,
    /// The child hashes of an index, 32 bytes each.
    pub hashes: usize,
    /// The separator table of an index: the shared prefix, the suffixes
    /// and the per-child end offsets.
    pub separators: usize,
    /// The per-child scale of an index.
    pub scales: usize,
    /// How many entries the frame holds: buffered ops in an index, leaf
    /// entries in a segment.
    pub entries: usize,
    /// The entries' keys as the columnar codec encodes them.
    pub keys: usize,
    /// The entries' values as rkyv lays them out, measured by serializing
    /// the values of each layout on their own, a vector header included.
    pub values: usize,
    /// The op polarity column of an index's buffers, one byte per op.
    pub polarity: usize,
    /// Everything else: rkyv's vector headers and relative pointers, the
    /// per-buffer fields (child, count, layout), alignment padding.
    pub structure: usize,
    /// The entries' key bytes before columnar encoding, what the weight
    /// charges for keys.
    pub raw_keys: usize,
    /// The payload weight of the entries' values, what the weight charges
    /// for values.
    pub value_weight: usize,
    /// The weight the manifest meters for these entries: `raw_keys +
    /// value_weight + entries * entry_overhead`. For an index this is what
    /// the buffered-byte flush cap compares against; for a segment, what
    /// the leaf pacing compares against `max_segment`.
    pub weight: usize,
    /// The entries by key layout (the key type's layout id, a region tag
    /// for the artifact tree), for telling which region's entries fill the
    /// frame.
    pub layouts: BTreeMap<u8, LayoutShare>,
}

/// The entries of one key layout in a frame.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LayoutShare {
    /// How many entries have this layout.
    pub entries: usize,
    /// Their key bytes before columnar encoding.
    pub raw_keys: usize,
    /// Their values as rkyv lays them out, serialized on their own.
    pub values: usize,
    /// The payload weight of their values.
    pub value_weight: usize,
}

impl FrameAttribution {
    /// The bytes the frame spends on its entries: keys, values, polarity
    /// and the structure around them. For an index this is what its write
    /// buffer costs a reader, over the links alone.
    pub fn entry_bytes(&self) -> usize {
        self.total - self.prelude - self.hashes - self.separators - self.scales
    }
}

impl<Key, Value> PersistentNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value
        + for<'a> rkyv::Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// Attributes this node's bytes to its components, and meters its
    /// entries the way the pacing does, under `manifest` (the tree's own:
    /// [`manifest`](Self::manifest)).
    pub fn attribution(
        &self,
        manifest: &Manifest,
    ) -> Result<FrameAttribution, DialogSearchTreeError> {
        let mut out = FrameAttribution {
            total: self.size(),
            prelude: self.prelude_len(),
            ..FrameAttribution::default()
        };
        match self.body() {
            NodeBody::Index(index) => {
                out.children = index.len();
                out.hashes = 32 * index.len();
                out.separators = index.prefix.len() + index.suffixes.len() + 4 * index.ends.len();
                out.scales = index.scales.len();
                let mut values: BTreeMap<u8, Vec<Value>> = BTreeMap::new();
                for buffer in index.novelty.iter() {
                    out.keys += buffer
                        .columns
                        .iter()
                        .map(archived_column_bytes)
                        .sum::<usize>();
                    out.polarity += buffer.polarity.len();
                    for entry in buffer.entries::<Key>()? {
                        let layout = Key::layout_of(&entry.key)?;
                        let share = out.layouts.entry(layout).or_default();
                        share.entries += 1;
                        share.raw_keys += entry.key.len();
                        match entry.op {
                            NoveltyOp::Assert(value) => {
                                share.value_weight += value.payload_weight();
                                values.entry(layout).or_default().push(value);
                            }
                            NoveltyOp::Retract => share.value_weight += 16,
                        }
                    }
                }
                for (layout, values) in &values {
                    out.layouts.entry(*layout).or_default().values = serialized_len(values)?;
                }
            }
            NodeBody::Segment(segment) => {
                out.keys = segment.columns.iter().map(archived_column_bytes).sum();
                let mut values: BTreeMap<u8, Vec<Value>> = BTreeMap::new();
                segment.for_each_entry::<Key, _>(|key, value| {
                    let value: Value = crate::into_owned::<Value>(value)?;
                    let layout = Key::layout_of(key)?;
                    let share = out.layouts.entry(layout).or_default();
                    share.entries += 1;
                    share.raw_keys += key.len();
                    share.value_weight += value.payload_weight();
                    values.entry(layout).or_default().push(value);
                    Ok(())
                })?;
                for (layout, values) in &values {
                    out.layouts.entry(*layout).or_default().values = serialized_len(values)?;
                }
            }
        }
        for share in out.layouts.values() {
            out.entries += share.entries;
            out.raw_keys += share.raw_keys;
            out.values += share.values;
            out.value_weight += share.value_weight;
        }
        out.weight = out.raw_keys + out.value_weight + out.entries * manifest.entry_overhead();
        out.structure = out
            .total
            .saturating_sub(out.prelude + out.hashes + out.separators + out.scales)
            .saturating_sub(out.keys + out.values + out.polarity);
        Ok(out)
    }
}

/// The bytes a column's tables occupy in the frame.
fn archived_column_bytes(column: &crate::ArchivedColumnData) -> usize {
    match archived_column_slices(column) {
        ColumnSlices::Arena { prefix, stream } => prefix.len() + stream.len(),
        ColumnSlices::Dictionary {
            table,
            table_ends,
            indices,
        } => table.len() + table_ends.len() + indices.len(),
    }
}

/// The length of `values` serialized on their own.
fn serialized_len<Value>(values: &Vec<Value>) -> Result<usize, DialogSearchTreeError>
where
    Value: for<'a> rkyv::Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
{
    rkyv::to_bytes::<rkyv::rancor::Error>(values)
        .map(|bytes| bytes.len())
        .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))
}
