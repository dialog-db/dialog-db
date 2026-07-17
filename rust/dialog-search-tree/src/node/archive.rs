use std::cmp::Ordering;

use dialog_common::Blake3Hash;

use crate::{
    ArchivedIndex, ArchivedSegment, DialogSearchTreeError, Link, Value,
    node::codec::{KeyCursor, RESTART_INTERVAL, read_varint},
};

fn malformed(message: &str) -> DialogSearchTreeError {
    DialogSearchTreeError::Encoding(message.to_string())
}

impl ArchivedIndex {
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

    /// A decoding cursor over this segment's full keys, in entry order.
    pub fn keys(&self) -> SegmentKeys<'_> {
        SegmentKeys {
            cursor: KeyCursor::new(&self.prefix, &self.keys, 0),
            count: self.len(),
            index: 0,
        }
    }

    /// The unprefixed key at restart block `block`: a restart record encodes
    /// its whole unprefixed key, so it is sliceable without a cursor.
    fn restart_head(&self, block: usize) -> Result<&[u8], DialogSearchTreeError> {
        let offset = self
            .restarts
            .get(block)
            .ok_or_else(|| malformed("Segment restart out of range"))?
            .to_native() as usize;
        let stream: &[u8] = &self.keys;
        let (shared, at) = read_varint(stream, offset)?;
        if shared != 0 {
            return Err(malformed("Segment restart record shares bytes"));
        }
        let (length, at) = read_varint(stream, at)?;
        stream
            .get(at..at + length as usize)
            .ok_or_else(|| malformed("Segment restart suffix exceeds the key stream"))
    }

    /// The first (minimum) key of this segment, decoded to its bytes.
    pub fn first_key(&self) -> Result<Vec<u8>, DialogSearchTreeError> {
        if self.is_empty() {
            return Err(malformed("Segment was unexpectedly empty"));
        }
        let head = self.restart_head(0)?;
        let mut key = Vec::with_capacity(self.prefix.len() + head.len());
        key.extend_from_slice(&self.prefix);
        key.extend_from_slice(head);
        Ok(key)
    }

    /// The last (maximum) key of this segment, decoded to its bytes.
    pub fn last_key(&self) -> Result<Vec<u8>, DialogSearchTreeError> {
        if self.is_empty() {
            return Err(malformed("Segment was unexpectedly empty"));
        }
        let last_block = self
            .restarts
            .len()
            .checked_sub(1)
            .ok_or_else(|| malformed("Segment restart table is empty"))?;
        let offset = self.restarts[last_block].to_native() as usize;
        let stream: &[u8] = &self.keys;
        let mut cursor = KeyCursor::new(&self.prefix, stream, offset);
        let mut remaining = self
            .len()
            .checked_sub(last_block * RESTART_INTERVAL)
            .ok_or_else(|| malformed("Segment restart table exceeds the entry count"))?;
        while remaining > 0 {
            cursor.advance()?;
            remaining -= 1;
        }
        Ok(cursor.key().to_vec())
    }

    /// Position of the entry whose key equals `key`, or `None`.
    ///
    /// Binary-searches the restart heads, then decodes at most one restart
    /// block linearly.
    pub fn find(&self, key: &[u8]) -> Result<Option<usize>, DialogSearchTreeError> {
        let prefix: &[u8] = &self.prefix;
        if key.len() < prefix.len() || &key[..prefix.len()] != prefix {
            return Ok(None);
        }
        let probe = &key[prefix.len()..];

        // Restart blocks whose head is <= the probe; the match, if present,
        // lives in the last such block.
        let (mut low, mut high) = (0, self.restarts.len());
        while low < high {
            let middle = (low + high) / 2;
            if self.restart_head(middle)? <= probe {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if low == 0 {
            return Ok(None);
        }
        let block = low - 1;

        let stream: &[u8] = &self.keys;
        let mut cursor = KeyCursor::new(&[], stream, self.restarts[block].to_native() as usize);
        let mut index = block * RESTART_INTERVAL;
        let end = self
            .restarts
            .get(block + 1)
            .map(|offset| offset.to_native() as usize)
            .unwrap_or(stream.len());
        while cursor.position() < end && index < self.len() {
            cursor.advance()?;
            match cursor.key().cmp(probe) {
                Ordering::Equal => return Ok(Some(index)),
                Ordering::Greater => return Ok(None),
                Ordering::Less => index += 1,
            }
        }
        Ok(None)
    }
}

/// A streaming decoder over a segment's full keys.
///
/// Yields `(entry index, key bytes)` pairs in order; the caller pairs the
/// index with [`ArchivedSegment::value_at`] as needed. One buffer is reused
/// across all keys.
pub struct SegmentKeys<'a> {
    cursor: KeyCursor<'a>,
    count: usize,
    index: usize,
}

impl SegmentKeys<'_> {
    /// Decodes the next key, or returns `None` past the last entry.
    ///
    /// Errors if the key stream ends before yielding as many keys as the
    /// segment has values, which marks the node malformed.
    pub fn next_key(&mut self) -> Result<Option<(usize, &[u8])>, DialogSearchTreeError> {
        if self.index >= self.count {
            return Ok(None);
        }
        self.cursor.advance()?;
        let at = self.index;
        self.index += 1;
        Ok(Some((at, self.cursor.key())))
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;

    use crate::{
        Buffer, Entry, Link, PersistentIndex, PersistentNode, PersistentNodeBody, PersistentSegment,
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
        let body = PersistentNodeBody::try_from(entries)?;
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
        let body: PersistentNodeBody<Vec<u8>> = PersistentNodeBody::try_from(links)?;
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
        assert_eq!(segment.first_key()?, keys[0].to_vec());
        assert_eq!(segment.last_key()?, keys.last().unwrap().to_vec());

        let mut decoded = segment.keys();
        for expected in &keys {
            let (at, bytes) = decoded.next_key()?.expect("entry present");
            assert_eq!(bytes, expected.as_slice());
            assert_eq!(segment.value_at(at)?.as_slice(), expected.as_slice());
        }
        assert!(decoded.next_key()?.is_none());

        for (at, key) in keys.iter().enumerate() {
            assert_eq!(segment.find(key.as_slice())?, Some(at));
        }
        assert_eq!(segment.find(b"absent!!")?, None);
        assert_eq!(segment.find(&[0xffu8; 8])?, None);
        assert_eq!(segment.find(&[0u8; 8])?, None);
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
            prefix: vec![],
            suffixes: b"abcd".to_vec(),
            ends: vec![3, 1],
            hashes: vec![Blake3Hash::hash(b"x"), Blake3Hash::hash(b"y")],
        });
        let node = TestNode::new(Buffer::from(body.as_bytes()?));
        assert!(node.as_index()?.separator(1).is_err());
        assert!(node.as_index()?.route(b"b").is_err());

        // End offset past the suffix table.
        let body: PersistentNodeBody<Vec<u8>> = PersistentNodeBody::Index(PersistentIndex {
            prefix: vec![],
            suffixes: b"ab".to_vec(),
            ends: vec![9],
            hashes: vec![Blake3Hash::hash(b"x")],
        });
        let node = TestNode::new(Buffer::from(body.as_bytes()?));
        assert!(node.as_index()?.separator(0).is_err());
        Ok(())
    }

    /// A malformed key stream (truncated records) errors at decode, and a
    /// value table longer than the key stream errors when the cursor runs
    /// past the stream's records.
    #[dialog_common::test]
    async fn it_rejects_malformed_segment_streams() -> Result<()> {
        let body: PersistentNodeBody<Vec<u8>> = PersistentNodeBody::Segment(PersistentSegment {
            prefix: vec![],
            keys: vec![0x80],
            restarts: vec![0],
            values: vec![vec![1], vec![2]],
        });
        let node = TestNode::new(Buffer::from(body.as_bytes()?));
        let segment = node.as_segment()?;
        assert!(segment.keys().next_key().is_err());
        assert!(segment.last_key().is_err());
        // The truncated restart head is reached by `find`'s binary search,
        // so the malformed stream is rejected with an error rather than a
        // silent miss.
        assert!(segment.find(b"anything").is_err());
        Ok(())
    }
}
