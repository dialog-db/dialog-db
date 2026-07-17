//! Columnar leaf encoding: one column per key component.
//!
//! A leaf's keys are split into their schema components (see
//! [`Schema`](crate::Schema)) and each component is stored in the column that
//! fits it:
//!
//! - an **arena column** concatenates the component's bytes contiguously,
//!   front-coded against the previous entry (so an entity's facts write the
//!   entity bytes once), addressed by a per-entry length; a component read is
//!   a borrowed slice into the arena plus the shared front-coded prefix.
//! - a **dictionary column** builds a per-leaf sorted table of the distinct
//!   values and stores one table index per entry. The table is a pure
//!   function of the column's contents (sorted, deduplicated), so it is
//!   canonical: the same key set yields the same table regardless of
//!   insertion order. A value recurring across many entries, even
//!   non-adjacently, is stored once.
//!
//! Comparison of a probe against a stored key walks components in order,
//! comparing each component's bytes; for a dictionary column the comparison
//! is against the *table value*, not the index, so it matches a comparison of
//! the concatenated key. Encoding is deterministic given the entry list, so
//! two leaves holding the same entries serialize to identical bytes.
//!
//! Every decode path is bounds-checked and returns an error on malformed
//! input.

use std::cmp::Ordering;

use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    Column, Component, DialogSearchTreeError, Schema,
    node::codec::{KeyCursor, encode_keys, read_varint, write_varint},
};

fn malformed(message: &str) -> DialogSearchTreeError {
    DialogSearchTreeError::Encoding(message.to_string())
}

/// One encoded column of a columnar leaf.
///
/// Serialized flat into the segment's byte columns; the discriminant is the
/// schema component's [`Column`], not stored per column (the schema is known
/// from the key type at decode time).
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedColumnData)]
pub enum ColumnData {
    /// Front-coded arena: `(prefix, stream, restarts)` as in the flat codec,
    /// but over just this component's bytes across all entries.
    Arena {
        /// Longest common prefix of this component across all entries.
        prefix: Vec<u8>,
        /// Front-coded per-entry stream of this component.
        stream: Vec<u8>,
        /// Restart offsets into `stream`.
        restarts: Vec<u32>,
    },
    /// Dictionary: a sorted table of distinct values (each length-prefixed)
    /// plus one table index per entry.
    Dictionary {
        /// Concatenated distinct values, each preceded by a varint length,
        /// in ascending byte order.
        table: Vec<u8>,
        /// End offset of each table entry within `table`.
        table_ends: Vec<u32>,
        /// Table index per entry.
        indices: Vec<u32>,
    },
}

/// Splits `entries` (each a full component-slice list, in schema order) into
/// one [`ColumnData`] per component, per the schema's column classes.
///
/// `rows[i]` is entry `i`'s components; every row has `schema.len()` slices.
/// Encoding is a pure function of `rows`.
pub fn encode_columns(
    schema: &Schema,
    rows: &[Vec<&[u8]>],
) -> Result<Vec<ColumnData>, DialogSearchTreeError> {
    let components = schema.components();
    let mut columns = Vec::with_capacity(components.len());

    for (index, component) in components.iter().enumerate() {
        let column_values: Vec<&[u8]> = rows
            .iter()
            .map(|row| {
                row.get(index)
                    .copied()
                    .ok_or_else(|| malformed("Key produced fewer components than its schema"))
            })
            .collect::<Result<_, _>>()?;

        columns.push(match component.column {
            Column::Arena => {
                let (prefix, stream, restarts) = encode_keys(&column_values);
                ColumnData::Arena {
                    prefix,
                    stream,
                    restarts,
                }
            }
            Column::Dictionary => encode_dictionary(&column_values),
        });
    }

    Ok(columns)
}

/// Builds a dictionary column: the sorted distinct values and per-entry
/// indices into them. Canonical because the table is the sorted dedup of the
/// column's values, independent of entry order.
fn encode_dictionary(values: &[&[u8]]) -> ColumnData {
    let mut distinct: Vec<&[u8]> = values.to_vec();
    distinct.sort_unstable();
    distinct.dedup();

    let mut table = Vec::new();
    let mut table_ends = Vec::with_capacity(distinct.len());
    for value in &distinct {
        write_varint(&mut table, value.len() as u32);
        table.extend_from_slice(value);
        table_ends.push(table.len() as u32);
    }

    // Binary-search the sorted distinct table for each entry's index.
    let indices = values
        .iter()
        .map(|value| {
            distinct
                .binary_search(value)
                .expect("every value is in the distinct table") as u32
        })
        .collect();

    ColumnData::Dictionary {
        table,
        table_ends,
        indices,
    }
}

/// A fully decoded columnar leaf: every entry's components reconstructed
/// into a row-major `Vec<Vec<u8>>`, ready for reconstruction and comparison.
///
/// Arena columns are cursor-decoded once; dictionary columns are index-
/// resolved once. Reconstruction is then pure slicing. This trades one
/// up-front decode pass for O(1) subsequent access, which suits the leaf's
/// access pattern (a lookup or a scan touches most entries).
pub struct ColumnarLeaf {
    /// `rows[i][c]` is entry `i`'s component `c` bytes.
    rows: Vec<Vec<Vec<u8>>>,
}

impl ColumnarLeaf {
    /// Decodes all columns into rows. `columns[c]` is component `c`'s data;
    /// `entry_count` is the leaf's entry count.
    pub fn decode(
        schema: &Schema,
        columns: &[ColumnData],
        entry_count: usize,
    ) -> Result<Self, DialogSearchTreeError> {
        let components = schema.components();
        if columns.len() != components.len() {
            return Err(malformed("Column count does not match the key schema"));
        }

        let mut rows: Vec<Vec<Vec<u8>>> = (0..entry_count).map(|_| Vec::new()).collect();

        for (component, column) in components.iter().zip(columns) {
            let decoded = decode_column(component, column, entry_count)?;
            if decoded.len() != entry_count {
                return Err(malformed("Column length does not match the entry count"));
            }
            for (row, value) in rows.iter_mut().zip(decoded) {
                row.push(value);
            }
        }

        Ok(Self { rows })
    }

    /// The number of entries in the leaf.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the leaf has no entries.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The full key of entry `at`, reconstructed by concatenating its
    /// components in schema order.
    pub fn key(&self, at: usize) -> Result<Vec<u8>, DialogSearchTreeError> {
        let row = self
            .rows
            .get(at)
            .ok_or_else(|| malformed("Columnar entry out of range"))?;
        Ok(row.concat())
    }

    /// Compares `probe` against entry `at`'s key, walking components without
    /// concatenating either side. Equivalent to comparing the concatenated
    /// keys because component boundaries partition the byte string.
    pub fn compare(&self, at: usize, probe: &[u8]) -> Result<Ordering, DialogSearchTreeError> {
        let row = self
            .rows
            .get(at)
            .ok_or_else(|| malformed("Columnar entry out of range"))?;
        let mut probe_at = 0usize;
        for component in row {
            let end = (probe_at + component.len()).min(probe.len());
            let probe_slice = &probe[probe_at..end];
            match component.as_slice().cmp(probe_slice) {
                Ordering::Equal => probe_at = end,
                other => return Ok(other),
            }
        }
        // All components matched their probe slices; the longer key sorts
        // after. If the probe still has bytes, it is longer.
        Ok(if probe_at < probe.len() {
            Ordering::Less
        } else {
            Ordering::Equal
        })
    }

    /// Position of the entry whose key equals `probe`, or `None`.
    /// Binary-searches over the (sorted) entries via component comparison.
    pub fn find(&self, probe: &[u8]) -> Result<Option<usize>, DialogSearchTreeError> {
        let (mut low, mut high) = (0usize, self.len());
        while low < high {
            let middle = (low + high) / 2;
            match self.compare(middle, probe)? {
                Ordering::Equal => return Ok(Some(middle)),
                Ordering::Less => low = middle + 1,
                Ordering::Greater => high = middle,
            }
        }
        Ok(None)
    }
}

/// Decodes one column into per-entry component byte vectors.
fn decode_column(
    component: &Component,
    column: &ColumnData,
    entry_count: usize,
) -> Result<Vec<Vec<u8>>, DialogSearchTreeError> {
    match (component.column, column) {
        (
            Column::Arena,
            ColumnData::Arena {
                prefix, stream, ..
            },
        ) => {
            let mut cursor = KeyCursor::new(prefix, stream, 0);
            let mut out = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                cursor.advance()?;
                out.push(cursor.key().to_vec());
            }
            Ok(out)
        }
        (
            Column::Dictionary,
            ColumnData::Dictionary {
                table,
                table_ends,
                indices,
            },
        ) => {
            let values = resolve_dictionary(table, table_ends)?;
            indices
                .iter()
                .map(|&index| {
                    values
                        .get(index as usize)
                        .map(|value| value.to_vec())
                        .ok_or_else(|| malformed("Dictionary index out of range"))
                })
                .collect()
        }
        _ => Err(malformed("Column class does not match the schema component")),
    }
}

/// Resolves a dictionary table into its distinct value slices.
fn resolve_dictionary<'a>(
    table: &'a [u8],
    table_ends: &[u32],
) -> Result<Vec<&'a [u8]>, DialogSearchTreeError> {
    let mut values = Vec::with_capacity(table_ends.len());
    let mut start = 0usize;
    for &end in table_ends {
        let end = end as usize;
        if end < start || end > table.len() {
            return Err(malformed("Dictionary table offset out of range"));
        }
        let field = table
            .get(start..end)
            .ok_or_else(|| malformed("Dictionary table slice out of range"))?;
        let (length, at) = read_varint(field, 0)?;
        let value = field
            .get(at..at + length as usize)
            .ok_or_else(|| malformed("Dictionary value exceeds its table field"))?;
        values.push(value);
        start = end;
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;

    use super::{ColumnData, ColumnarLeaf, encode_columns, encode_dictionary};
    use crate::{Component, Schema};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A dictionary column stores each distinct value once, in sorted order,
    /// regardless of how often or where it repeats, and indices resolve back
    /// to the right value.
    #[dialog_common::test]
    async fn it_interns_non_adjacent_repeats_once() -> Result<()> {
        // "age" and "name" alternate: classic non-adjacent repetition that
        // front coding would miss.
        let values: Vec<&[u8]> = vec![b"age", b"name", b"age", b"name", b"age"];
        let ColumnData::Dictionary {
            table,
            table_ends,
            indices,
        } = encode_dictionary(&values)
        else {
            panic!("expected a dictionary column");
        };

        // Two distinct values stored once each ("age" < "name").
        assert_eq!(table_ends.len(), 2);
        // 3 + 4 bytes of value plus one varint length byte each = 9 bytes.
        assert_eq!(table.len(), 3 + 4 + 2);
        // age -> 0, name -> 1.
        assert_eq!(indices, vec![0, 1, 0, 1, 0]);
        Ok(())
    }

    /// The dictionary table is a pure function of the column's value set:
    /// permuting the entries leaves the table identical (only indices move).
    #[dialog_common::test]
    async fn it_builds_a_canonical_dictionary() -> Result<()> {
        let forward: Vec<&[u8]> = vec![b"a", b"b", b"c", b"a"];
        let shuffled: Vec<&[u8]> = vec![b"c", b"a", b"a", b"b"];

        let ColumnData::Dictionary { table: t1, .. } = encode_dictionary(&forward) else {
            panic!()
        };
        let ColumnData::Dictionary { table: t2, .. } = encode_dictionary(&shuffled) else {
            panic!()
        };
        assert_eq!(t1, t2, "dictionary table must be order-independent");
        Ok(())
    }

    /// Encoding routes each component to its schema's column class.
    #[dialog_common::test]
    async fn it_encodes_one_column_per_component() -> Result<()> {
        const PARTS: &[Component] = &[Component::arena(2), Component::dictionary_var()];
        let schema = Schema::new(PARTS);

        let rows: Vec<Vec<&[u8]>> = vec![
            vec![b"AA", b"age"],
            vec![b"AB", b"name"],
            vec![b"AC", b"age"],
        ];
        let columns = encode_columns(&schema, &rows)?;
        assert_eq!(columns.len(), 2);
        assert!(matches!(columns[0], ColumnData::Arena { .. }));
        assert!(matches!(columns[1], ColumnData::Dictionary { .. }));
        Ok(())
    }

    /// A full EAV-shaped round trip: split keys into components, encode
    /// columns, decode, and reconstruct each key identically. The dictionary
    /// component (attribute) recurs non-adjacently and is stored once.
    #[dialog_common::test]
    async fn it_round_trips_a_multi_component_leaf() -> Result<()> {
        // schema: entity (arena, 2B fixed) ++ attribute (dictionary, var).
        const PARTS: &[Component] = &[Component::arena(2), Component::dictionary_var()];
        let schema = Schema::new(PARTS);

        // Sorted EAV keys: entity then attribute. Attributes repeat across
        // entities, never adjacently within an entity's run they alternate.
        let keys: Vec<Vec<u8>> = vec![
            [b"E1".as_slice(), b"age"].concat(),
            [b"E1".as_slice(), b"name"].concat(),
            [b"E2".as_slice(), b"age"].concat(),
            [b"E2".as_slice(), b"name"].concat(),
            [b"E3".as_slice(), b"age"].concat(),
        ];

        // Split each key into its components (entity=2B, attribute=rest).
        let rows: Vec<Vec<&[u8]>> = keys.iter().map(|k| vec![&k[..2], &k[2..]]).collect();

        let columns = encode_columns(&schema, &rows)?;
        let leaf = ColumnarLeaf::decode(&schema, &columns, keys.len())?;

        assert_eq!(leaf.len(), keys.len());
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(&leaf.key(index)?, key, "reconstructed key {index}");
        }

        // The attribute dictionary stored "age"/"name" once each despite
        // three "age" and two "name" occurrences.
        let ColumnData::Dictionary { table_ends, .. } = &columns[1] else {
            panic!("attribute column must be a dictionary");
        };
        assert_eq!(table_ends.len(), 2, "two distinct attributes stored once");

        Ok(())
    }

    /// `find` locates present keys and rejects absent ones, and `compare`
    /// orders keys exactly as byte comparison of the concatenation does.
    #[dialog_common::test]
    async fn it_finds_and_orders_by_component_comparison() -> Result<()> {
        const PARTS: &[Component] = &[Component::arena(2), Component::dictionary_var()];
        let schema = Schema::new(PARTS);

        let keys: Vec<Vec<u8>> = vec![
            [b"E1".as_slice(), b"age"].concat(),
            [b"E1".as_slice(), b"name"].concat(),
            [b"E2".as_slice(), b"age"].concat(),
        ];
        let rows: Vec<Vec<&[u8]>> = keys.iter().map(|k| vec![&k[..2], &k[2..]]).collect();
        let columns = encode_columns(&schema, &rows)?;
        let leaf = ColumnarLeaf::decode(&schema, &columns, keys.len())?;

        for (index, key) in keys.iter().enumerate() {
            assert_eq!(leaf.find(key)?, Some(index), "find {index}");
        }
        assert_eq!(leaf.find(b"E1zzz")?, None, "absent key between entries");
        assert_eq!(leaf.find(b"E9")?, None, "absent key past the end");
        assert_eq!(leaf.find(b"A0")?, None, "absent key before the start");

        // Component comparison matches concatenated byte comparison.
        use std::cmp::Ordering;
        assert_eq!(leaf.compare(0, &keys[0])?, Ordering::Equal);
        assert_eq!(leaf.compare(0, b"E1zz")?, Ordering::Less, "entry < probe");
        assert_eq!(leaf.compare(2, b"E1zz")?, Ordering::Greater, "entry > probe");
        Ok(())
    }
}
