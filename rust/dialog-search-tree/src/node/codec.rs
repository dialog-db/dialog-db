//! Byte-level codec for the front-coded key stream stored in leaf segments.
//!
//! A segment stores its keys as a node-level prefix (the longest common
//! prefix of every key, stored once) plus a stream of per-entry records:
//!
//! ```text
//! varint(shared) varint(suffix_len) suffix_bytes
//! ```
//!
//! where `shared` counts the bytes an entry's unprefixed key shares with the
//! previous entry's unprefixed key (the first entry writes its whole
//! unprefixed key). Decoding is a single forward cursor from the start of
//! the stream; repeated point lookups are amortized by the per-buffer decode
//! memo rather than by in-stream seek points. (LevelDB-style restart tables
//! were written by an earlier revision of this codec but never read —
//! measured at ~1% of the live tree — and were dropped; if mid-leaf seeking
//! is ever wanted, it returns behind a manifest format-version bump.)
//!
//! The layout is a pure function of the entry list: the prefix is the LCP of
//! the first and last key (the list is sorted) and shared lengths are exact
//! LCPs. Two nodes holding the same entries therefore serialize to identical
//! bytes, which the tree's content-addressing depends on.
//!
//! Every decode path is bounds-checked and returns an error on malformed
//! input; nodes arrive from untrusted peers and must be rejected, never
//! panicked on.

use crate::DialogSearchTreeError;

/// Appends `value` to `output` as a LEB128 varint.
pub fn write_varint(output: &mut Vec<u8>, mut value: u32) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            output.push(byte);
            break;
        }
        output.push(byte | 0x80);
    }
}

/// Decodes a LEB128 varint at `at`, returning the value and the position
/// just past it.
pub fn read_varint(bytes: &[u8], at: usize) -> Result<(u32, usize), DialogSearchTreeError> {
    let mut value: u32 = 0;
    let mut shift = 0u32;
    let mut position = at;
    loop {
        let byte = *bytes.get(position).ok_or_else(|| {
            DialogSearchTreeError::Encoding("Key stream ended inside a varint".into())
        })?;
        if shift >= 32 || (shift == 28 && byte > 0x0f) {
            return Err(DialogSearchTreeError::Encoding(
                "Varint in key stream overflows u32".into(),
            ));
        }
        value |= u32::from(byte & 0x7f) << shift;
        position += 1;
        if byte & 0x80 == 0 {
            return Ok((value, position));
        }
        shift += 7;
    }
}

/// Packs a list of `u32`s into LEB128 varints, back to back.
pub fn pack_varints(values: &[u32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len());
    for &value in values {
        write_varint(&mut out, value);
    }
    out
}

/// Unpacks all LEB128 varints from `bytes` until it is exhausted (count is
/// implied by the byte length). Used where the number of packed values is
/// not known independently.
pub fn unpack_varints_all(bytes: &[u8]) -> Result<Vec<u32>, DialogSearchTreeError> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while at < bytes.len() {
        let (value, next) = read_varint(bytes, at)?;
        out.push(value);
        at = next;
    }
    Ok(out)
}

/// Length of the longest common prefix of two byte strings.
pub fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter().zip(right).take_while(|(l, r)| l == r).count()
}

/// Encodes `keys` into a `(prefix, stream)` pair: the node-level prefix
/// stored once, and the front-coded stream.
///
/// The node prefix is the common prefix of *every* key, not just the first and
/// last. For a sorted whole-key stream those coincide, but this also encodes
/// arena *columns*, whose values are in key order (so unsorted): a value
/// component (an inline order-preserving value) can be shorter than, or diverge
/// earlier than, the first/last pair, so a first/last prefix could exceed a
/// middle value's length. Folding over all keys keeps the stored prefix a true
/// prefix of each.
pub fn encode_keys<K: AsRef<[u8]>>(keys: &[K]) -> (Vec<u8>, Vec<u8>) {
    let prefix = match keys.first() {
        Some(first) => {
            let mut length = first.as_ref().len();
            for key in &keys[1..] {
                length = common_prefix(&first.as_ref()[..length], key.as_ref());
            }
            first.as_ref()[..length].to_vec()
        }
        None => Vec::new(),
    };

    let mut stream = Vec::new();
    let mut previous: &[u8] = &[];

    for key in keys {
        let unprefixed = &key.as_ref()[prefix.len()..];
        let shared = common_prefix(previous, unprefixed);
        write_varint(&mut stream, shared as u32);
        write_varint(&mut stream, (unprefixed.len() - shared) as u32);
        stream.extend_from_slice(&unprefixed[shared..]);
        previous = unprefixed;
    }

    (prefix, stream)
}

/// An incremental decoder over a front-coded key stream.
///
/// Yields each entry's full key by reconstructing it from the record's shared
/// length and suffix. The internal buffer starts with the node prefix and is
/// rewritten in place, so decoding a node allocates one buffer, not one per
/// key.
pub struct KeyCursor<'a> {
    prefix_length: usize,
    stream: &'a [u8],
    position: usize,
    /// The current full key: `prefix ++ unprefixed`, valid after a
    /// successful [`advance`](Self::advance).
    key: Vec<u8>,
}

impl<'a> KeyCursor<'a> {
    /// Creates a cursor over `stream`, starting at byte `offset` (a restart
    /// offset, or 0 for the whole stream).
    pub fn new(prefix: &[u8], stream: &'a [u8], offset: usize) -> Self {
        Self {
            prefix_length: prefix.len(),
            stream,
            position: offset,
            key: prefix.to_vec(),
        }
    }

    /// Whether the cursor has consumed the whole stream.
    #[cfg(test)]
    pub fn is_done(&self) -> bool {
        self.position >= self.stream.len()
    }

    /// Decodes the next record, leaving its full key in [`key`](Self::key).
    ///
    /// Errors on truncated records or on a shared length that exceeds the
    /// reconstructed key, either of which marks the node malformed.
    pub fn advance(&mut self) -> Result<(), DialogSearchTreeError> {
        let (shared, at) = read_varint(self.stream, self.position)?;
        let (suffix_length, at) = read_varint(self.stream, at)?;
        let end = at + suffix_length as usize;
        let suffix = self.stream.get(at..end).ok_or_else(|| {
            DialogSearchTreeError::Encoding("Key stream ended inside a suffix".into())
        })?;
        let retained = self.prefix_length + shared as usize;
        if retained > self.key.len() {
            return Err(DialogSearchTreeError::Encoding(
                "Key stream shares more bytes than the previous key holds".into(),
            ));
        }
        self.key.truncate(retained);
        self.key.extend_from_slice(suffix);
        self.position = end;
        Ok(())
    }

    /// The current full key. Valid only after a successful
    /// [`advance`](Self::advance).
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;

    use super::{KeyCursor, encode_keys, read_varint, write_varint};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    async fn it_round_trips_varints() -> Result<()> {
        for value in [0u32, 1, 127, 128, 300, 16383, 16384, u32::MAX] {
            let mut bytes = Vec::new();
            write_varint(&mut bytes, value);
            let (decoded, at) = read_varint(&bytes, 0)?;
            assert_eq!(decoded, value);
            assert_eq!(at, bytes.len());
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn it_rejects_truncated_and_overflowing_varints() -> Result<()> {
        assert!(read_varint(&[0x80], 0).is_err(), "truncated varint");
        assert!(read_varint(&[], 0).is_err(), "empty input");
        assert!(
            read_varint(&[0xff, 0xff, 0xff, 0xff, 0x7f], 0).is_err(),
            "six-septet value overflows u32"
        );
        Ok(())
    }

    /// Round-trips key lists through the codec, exercising shared prefixes,
    /// long runs, and single-entry nodes.
    #[dialog_common::test]
    async fn it_round_trips_front_coded_keys() -> Result<()> {
        let cases: Vec<Vec<Vec<u8>>> = vec![
            vec![b"car".to_vec()],
            vec![b"car".to_vec(), b"carpet".to_vec(), b"cat".to_vec()],
            (0..100u32)
                .map(|i| format!("person/{i:03}/age").into_bytes())
                .collect(),
        ];

        for keys in cases {
            let (prefix, stream) = encode_keys(&keys);

            let mut cursor = KeyCursor::new(&prefix, &stream, 0);
            for expected in &keys {
                cursor.advance()?;
                assert_eq!(cursor.key(), expected.as_slice());
            }
            assert!(cursor.is_done());
        }
        Ok(())
    }

    /// A column's values are in key order, not value-sorted, so a short value
    /// can sit between two longer ones whose shared prefix is longer than it.
    /// The node prefix must be the common prefix of every value (not just the
    /// first and last), or slicing it off a shorter middle value would panic.
    /// This reproduces the variable-length inline value column.
    #[dialog_common::test]
    async fn it_round_trips_an_unsorted_column_with_a_short_middle() -> Result<()> {
        let keys: Vec<Vec<u8>> = vec![
            b"value-aaaa".to_vec(),
            b"v".to_vec(),
            b"value-aaaz".to_vec(),
        ];
        let (prefix, stream) = encode_keys(&keys);
        assert!(
            prefix.len() <= keys.iter().map(|k| k.len()).min().unwrap(),
            "node prefix never exceeds the shortest value"
        );

        let mut cursor = KeyCursor::new(&prefix, &stream, 0);
        for expected in &keys {
            cursor.advance()?;
            assert_eq!(cursor.key(), expected.as_slice());
        }
        assert!(cursor.is_done());
        Ok(())
    }

    /// Malformed streams are rejected with errors, never panics.
    #[dialog_common::test]
    async fn it_rejects_malformed_streams() -> Result<()> {
        // Suffix length runs past the end of the stream.
        let mut stream = Vec::new();
        write_varint(&mut stream, 0);
        write_varint(&mut stream, 100);
        stream.push(b'x');
        let mut cursor = KeyCursor::new(b"", &stream, 0);
        assert!(cursor.advance().is_err(), "overlong suffix must error");

        // Shared length exceeds what the previous key can provide.
        let mut stream = Vec::new();
        write_varint(&mut stream, 9);
        write_varint(&mut stream, 1);
        stream.push(b'x');
        let mut cursor = KeyCursor::new(b"ab", &stream, 0);
        assert!(cursor.advance().is_err(), "overlong share must error");

        Ok(())
    }
}
