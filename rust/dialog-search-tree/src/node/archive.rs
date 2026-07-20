use std::cmp::Ordering;

use dialog_common::Blake3Hash;
use rkyv::{Deserialize, rancor::Strategy};

use crate::{
    ArchivedIndex, ArchivedSegment, ColumnData, DialogSearchTreeError, Key, Link, Value,
    node::columnar::{ColumnarLeaf, StreamingLeaf, archived_column_slices},
};

fn malformed(message: &str) -> DialogSearchTreeError {
    DialogSearchTreeError::Encoding(message.to_string())
}

impl<Value> ArchivedIndex<Value>
where
    Value: rkyv::Archive,
{
    /// Number of children in this index.
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// Whether this index holds no children, which violates the node
    /// invariant but is answerable without decoding.
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// The separator suffix of the child at `at`, bounds-checked against the
    /// offset table.
    fn suffix(&self, at: usize) -> Result<&[u8], DialogSearchTreeError> {
        let end = self
            .ends
            .get(at)
            .ok_or_else(|| malformed("Index child out of range"))?
            .to_native() as usize;
        let start = if at == 0 {
            0
        } else {
            self.ends[at - 1].to_native() as usize
        };
        if start > end {
            return Err(malformed("Index suffix offsets are not monotone"));
        }
        self.suffixes
            .get(start..end)
            .ok_or_else(|| malformed("Index suffix offset exceeds the suffix table"))
    }

    /// The full separator of the child at `at`: the node prefix plus the
    /// child's suffix.
    pub fn separator(&self, at: usize) -> Result<Vec<u8>, DialogSearchTreeError> {
        let suffix = self.suffix(at)?;
        let mut separator = Vec::with_capacity(self.prefix.len() + suffix.len());
        separator.extend_from_slice(&self.prefix);
        separator.extend_from_slice(suffix);
        Ok(separator)
    }

    /// The content hash of the child at `at`.
    pub fn hash_at(&self, at: usize) -> Result<&Blake3Hash, DialogSearchTreeError> {
        self.hashes
            .get(at)
            .map(<&Blake3Hash>::from)
            .ok_or_else(|| malformed("Index child out of range"))
    }

    /// The child at `at`, materialized as an owned [`Link`].
    pub fn link_at(&self, at: usize) -> Result<Link, DialogSearchTreeError> {
        Ok(Link {
            separator: self.separator(at)?,
            node: self.hash_at(at)?.clone(),
        })
    }

    /// All children, materialized as owned [`Link`]s in child order.
    pub fn links(&self) -> Result<Vec<Link>, DialogSearchTreeError> {
        (0..self.len()).map(|at| self.link_at(at)).collect()
    }

    /// Whether any child's content hash equals `hash`. Compares raw hash
    /// bytes; no separator decoding.
    pub fn contains_hash(&self, hash: &Blake3Hash) -> bool {
        self.hashes
            .iter()
            .any(|candidate| <&Blake3Hash>::from(candidate) == hash)
    }

    /// Index of the child whose subtree covers `key`: the last child whose
    /// separator is at or below the key. A key below every separator (which
    /// can only happen when the leftmost separator is non-empty) is clamped
    /// to the leftmost child, whose subtree is the only place it could live.
    ///
    /// Compares the probe against the node prefix once, then against suffix
    /// slices; no separator is reconstructed.
    pub fn route(&self, key: &[u8]) -> Result<usize, DialogSearchTreeError> {
        let prefix: &[u8] = &self.prefix;
        let shared = prefix.len().min(key.len());
        // Children whose separator is <= key. Every separator starts with
        // the prefix, so when the probe diverges from the prefix the
        // comparison is decided for all children at once.
        let below = match key[..shared].cmp(&prefix[..shared]) {
            Ordering::Less => 0,
            Ordering::Greater => self.len(),
            Ordering::Equal if key.len() < prefix.len() => 0,
            Ordering::Equal => {
                let probe = &key[prefix.len()..];
                let (mut low, mut high) = (0, self.len());
                while low < high {
                    let middle = (low + high) / 2;
                    if self.suffix(middle)? <= probe {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
                low
            }
        };
        Ok(below.saturating_sub(1))
    }
}

impl<Value> ArchivedSegment<Value>
where
    Value: self::Value,
{
    /// Number of entries in this segment.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether this segment holds no entries, which violates the node
    /// invariant but is answerable without decoding.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The archived value of the entry at `at`.
    pub fn value_at(&self, at: usize) -> Result<&Value::Archived, DialogSearchTreeError> {
        self.values
            .get(at)
            .ok_or_else(|| malformed("Segment entry out of range"))
    }

    /// Deserializes the archived columns to owned form and decodes them into
    /// a [`ColumnarLeaf`], using `Key`'s schema. One decode pass; subsequent
    /// key access is O(1) slicing.
    pub fn decode<Key: self::Key>(&self) -> Result<ColumnarLeaf, DialogSearchTreeError> {
        let columns: Vec<ColumnData> = self
            .columns
            .iter()
            .map(|column| {
                column
                    .deserialize(Strategy::<_, rkyv::rancor::Error>::wrap(&mut ()))
                    .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))
            })
            .collect::<Result<_, _>>()?;
        let schema = if self.layout == crate::MIXED_LAYOUT {
            crate::Schema::opaque()
        } else {
            Key::schema(self.layout)
        };
        ColumnarLeaf::decode(&schema, &columns, self.count.to_native() as usize)
    }

    /// A streaming decoder over this segment's full keys, in entry order, for a
    /// given key schema. Borrows the archived columns and reconstructs each key
    /// into a single reused buffer — no owned-column deserialize, no row-major
    /// materialization, no per-entry allocation. This is the scan hot path; see
    /// [`StreamingLeaf`].
    pub fn keys<Key: self::Key>(&self) -> Result<StreamingLeaf<'_>, DialogSearchTreeError> {
        let schema = if self.layout == crate::MIXED_LAYOUT {
            crate::Schema::opaque()
        } else {
            Key::schema(self.layout)
        };
        let columns: Vec<_> = self.columns.iter().map(archived_column_slices).collect();
        StreamingLeaf::new(&schema, &columns, self.count.to_native() as usize)
    }

    /// The first (minimum) key of this segment, decoded to its bytes.
    pub fn first_key<Key: self::Key>(&self) -> Result<Vec<u8>, DialogSearchTreeError> {
        self.decode::<Key>()?.key(0)
    }

    /// The last (maximum) key of this segment, decoded to its bytes.
    pub fn last_key<Key: self::Key>(&self) -> Result<Vec<u8>, DialogSearchTreeError> {
        let leaf = self.decode::<Key>()?;
        let last = leaf
            .len()
            .checked_sub(1)
            .ok_or_else(|| malformed("Segment was unexpectedly empty"))?;
        leaf.key(last)
    }

    /// Position of the entry whose key equals `key`, or `None`. Binary
    /// search via component comparison, no key reconstruction.
    pub fn find<Key: self::Key>(&self, key: &[u8]) -> Result<Option<usize>, DialogSearchTreeError> {
        self.decode::<Key>()?.find(key)
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;

    use crate::{
        Buffer, ColumnData, Entry, Link, Manifest, PersistentIndex, PersistentNode,
        PersistentNodeBody, PersistentSegment,
    };

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type TestNode = PersistentNode<[u8; 8], Vec<u8>>;

    fn key(text: &str) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[..text.len()].copy_from_slice(text.as_bytes());
        bytes
    }

    fn segment_node(keys: &[[u8; 8]]) -> Result<TestNode> {
        let entries: Vec<Entry<[u8; 8], Vec<u8>>> = keys
            .iter()
            .map(|&key| Entry {
                key,
                value: key.to_vec(),
            })
            .collect();
        let body = PersistentNodeBody::segment_from_entries(entries, Manifest::default())?;
        Ok(PersistentNode::new(Buffer::from(body.as_bytes()?)))
    }

    fn index_node(separators: &[&[u8]]) -> Result<TestNode> {
        let links: Vec<Link> = separators
            .iter()
            .map(|separator| Link {
                separator: separator.to_vec(),
                node: Blake3Hash::hash(separator),
            })
            .collect();
        let body: PersistentNodeBody<Vec<u8>> =
            PersistentNodeBody::index_from_links(links, Vec::new(), Manifest::default())?;
        Ok(PersistentNode::new(Buffer::from(body.as_bytes()?)))
    }

    /// A serialized segment decodes back to exactly the entries it encoded:
    /// keys in order via the cursor, values by position, bounds by decode.
    #[dialog_common::test]
    async fn it_round_trips_segments_through_the_coded_form() -> Result<()> {
        let keys: Vec<[u8; 8]> = (0..40u32).map(|i| key(&format!("k{i:05}"))).collect();
        let node = segment_node(&keys)?;
        let segment = node.as_segment()?;

        assert_eq!(segment.len(), keys.len());
        assert_eq!(segment.first_key::<[u8; 8]>()?, keys[0].to_vec());
        assert_eq!(
            segment.last_key::<[u8; 8]>()?,
            keys.last().unwrap().to_vec()
        );

        let mut decoded = segment.keys::<[u8; 8]>()?;
        for expected in &keys {
            let (at, bytes) = decoded.next_key()?.expect("entry present");
            assert_eq!(bytes, expected.to_vec());
            assert_eq!(segment.value_at(at)?.as_slice(), expected.as_slice());
        }
        assert!(decoded.next_key()?.is_none());

        for (at, key) in keys.iter().enumerate() {
            assert_eq!(segment.find::<[u8; 8]>(key.as_slice())?, Some(at));
        }
        assert_eq!(segment.find::<[u8; 8]>(b"absent!!")?, None);
        assert_eq!(segment.find::<[u8; 8]>(&[0xffu8; 8])?, None);
        assert_eq!(segment.find::<[u8; 8]>(&[0u8; 8])?, None);
        Ok(())
    }

    /// A serialized index materializes its links back exactly and routes
    /// probes to the last child whose separator is at or below the probe.
    #[dialog_common::test]
    async fn it_round_trips_indexes_and_routes_by_separator() -> Result<()> {
        let separators: Vec<&[u8]> = vec![b"", b"car", b"carpet", b"cat"];
        let node = index_node(&separators)?;
        let index = node.as_index()?;

        assert_eq!(index.len(), separators.len());
        for (at, separator) in separators.iter().enumerate() {
            assert_eq!(index.separator(at)?, separator.to_vec());
            assert_eq!(index.link_at(at)?.node, Blake3Hash::hash(separator));
        }

        assert_eq!(index.route(b"a")?, 0, "below every non-empty separator");
        assert_eq!(index.route(b"car")?, 1, "probe equal routes right-side");
        assert_eq!(index.route(b"carp")?, 1);
        assert_eq!(index.route(b"carpet")?, 2);
        assert_eq!(index.route(b"cat")?, 3);
        assert_eq!(index.route(b"zebra")?, 3);
        Ok(())
    }

    /// Malformed offset tables are rejected with errors at access, never a
    /// panic: nodes arrive from untrusted peers.
    #[dialog_common::test]
    async fn it_rejects_malformed_index_tables() -> Result<()> {
        // Non-monotone ends.
        let body: PersistentNodeBody<Vec<u8>> = PersistentNodeBody::Index(PersistentIndex {
            header: Manifest::default(),
            prefix: vec![],
            suffixes: b"abcd".to_vec(),
            ends: vec![3, 1],
            hashes: vec![Blake3Hash::hash(b"x"), Blake3Hash::hash(b"y")],
            novelty: Vec::new(),
        });
        let node = TestNode::new(Buffer::from(body.as_bytes()?));
        assert!(node.as_index()?.separator(1).is_err());
        assert!(node.as_index()?.route(b"b").is_err());

        // End offset past the suffix table.
        let body: PersistentNodeBody<Vec<u8>> = PersistentNodeBody::Index(PersistentIndex {
            header: Manifest::default(),
            prefix: vec![],
            suffixes: b"ab".to_vec(),
            ends: vec![9],
            hashes: vec![Blake3Hash::hash(b"x")],
            novelty: Vec::new(),
        });
        let node = TestNode::new(Buffer::from(body.as_bytes()?));
        assert!(node.as_index()?.separator(0).is_err());
        Ok(())
    }

    /// A malformed arena column (truncated front-coded record) errors at
    /// decode rather than panicking: nodes arrive from untrusted peers.
    #[dialog_common::test]
    async fn it_rejects_malformed_segment_columns() -> Result<()> {
        // One opaque arena column whose stream is a truncated varint, but a
        // value table claiming two entries.
        let body: PersistentNodeBody<Vec<u8>> = PersistentNodeBody::Segment(PersistentSegment {
            header: Manifest::default(),
            count: 2,
            layout: 0,
            columns: vec![ColumnData::Arena {
                prefix: vec![],
                stream: vec![0x80],
                restarts: vec![0],
            }],
            values: vec![vec![1], vec![2]],
        });
        let node = TestNode::new(Buffer::from(body.as_bytes()?));
        let segment = node.as_segment()?;
        assert!(segment.keys::<[u8; 8]>().is_err() || segment.first_key::<[u8; 8]>().is_err());
        assert!(segment.last_key::<[u8; 8]>().is_err());
        assert!(segment.find::<[u8; 8]>(b"anything").is_err());
        Ok(())
    }

    /// Every persisted node carries the tree's manifest in its bytes, readable
    /// from the node alone: a segment and an index both round-trip the default
    /// manifest through serialization.
    #[dialog_common::test]
    async fn it_persists_the_manifest_in_every_node() -> Result<()> {
        let segment = segment_node(&[key("a"), key("b")])?;
        assert_eq!(segment.manifest()?, Manifest::default());

        let index = index_node(&[b"", b"m"])?;
        assert_eq!(index.manifest()?, Manifest::default());
        Ok(())
    }

    /// A NON-default manifest survives serialization, proving the manifest is
    /// real per-node data, not a compile-time constant read back.
    #[dialog_common::test]
    async fn it_round_trips_a_non_default_manifest() -> Result<()> {
        let manifest = Manifest {
            version: 1,
            fanout_n: 4,
            max_separator: 128,
            inline_n: 64,
        };
        let entries: Vec<Entry<[u8; 8], Vec<u8>>> = [key("x")]
            .into_iter()
            .map(|k| Entry {
                key: k,
                value: k.to_vec(),
            })
            .collect();
        let body = PersistentNodeBody::segment_from_entries(entries, manifest)?;
        let node = TestNode::new(Buffer::from(body.as_bytes()?));
        assert_eq!(node.manifest()?, manifest);
        Ok(())
    }
}
