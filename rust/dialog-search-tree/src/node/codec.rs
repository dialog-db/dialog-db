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
//! previous entry's unprefixed key. Every [`RESTART_INTERVAL`]-th entry is a
//! *restart*: it encodes `shared = 0` (its whole unprefixed key is in the
//! stream), so a reader can begin decoding at any restart without a cursor
//! from the start of the node. Lookups binary-search the restart heads and
//! decode at most one restart block linearly.
//!
//! The layout is a pure function of the entry list: the prefix is the LCP of
//! the first and last key (the list is sorted), restarts fall on fixed entry
//! indices, and shared lengths are exact LCPs. Two nodes holding the same
//! entries therefore serialize to identical bytes, which the tree's
//! content-addressing depends on.
//!
//! Every decode path is bounds-checked and returns an error on malformed
//! input; nodes arrive from untrusted peers and must be rejected, never
//! panicked on.

use crate::DialogSearchTreeError;

/// Number of entries per restart block: a restart entry encodes its full
/// unprefixed key, and at most this many entries decode linearly per lookup.
/// Trades compression (restart entries repeat shared bytes) against decode
/// cost. Part of the storage format; changing it changes node bytes.
pub const RESTART_INTERVAL: usize = 16;

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

/// Unpacks `count` LEB128 varints from `bytes`, erroring on truncation or on
/// leftover bytes (which would mark the column malformed).
pub fn unpack_varints(bytes: &[u8], count: usize) -> Result<Vec<u32>, DialogSearchTreeError> {
    let mut out = Vec::with_capacity(count);
    let mut at = 0usize;
    for _ in 0..count {
        let (value, next) = read_varint(bytes, at)?;
        out.push(value);
        at = next;
    }
    if at != bytes.len() {
        return Err(DialogSearchTreeError::Encoding(
            "Trailing bytes after a varint-packed list".into(),
        ));
    }
    Ok(out)
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

/// Encodes sorted `keys` into a `(prefix, stream, restarts)` triple: the
/// node-level prefix stored once, the front-coded stream, and the byte
/// offset of each restart record within the stream.
pub fn encode_keys<K: AsRef<[u8]>>(keys: &[K]) -> (Vec<u8>, Vec<u8>, Vec<u32>) {
    let prefix = match (keys.first(), keys.last()) {
        (Some(first), Some(last)) => {
            let length = common_prefix(first.as_ref(), last.as_ref());
            first.as_ref()[..length].to_vec()
        }
        _ => Vec::new(),
    };

    let mut stream = Vec::new();
    let mut restarts = Vec::new();
    let mut previous: &[u8] = &[];

    for (index, key) in keys.iter().enumerate() {
        let unprefixed = &key.as_ref()[prefix.len()..];
        let shared = if index % RESTART_INTERVAL == 0 {
            restarts.push(stream.len() as u32);
            0
        } else {
            common_prefix(previous, unprefixed)
        };
        write_varint(&mut stream, shared as u32);
        write_varint(&mut stream, (unprefixed.len() - shared) as u32);
        stream.extend_from_slice(&unprefixed[shared..]);
        previous = unprefixed;
    }

    (prefix, stream, restarts)
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

    use super::{KeyCursor, RESTART_INTERVAL, encode_keys, read_varint, write_varint};

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
    /// multiple restart blocks, and single-entry nodes.
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
            let (prefix, stream, restarts) = encode_keys(&keys);
            assert_eq!(restarts.len(), keys.len().div_ceil(RESTART_INTERVAL));

            let mut cursor = KeyCursor::new(&prefix, &stream, 0);
            for expected in &keys {
                cursor.advance()?;
                assert_eq!(cursor.key(), expected.as_slice());
            }
            assert!(cursor.is_done());
        }
        Ok(())
    }

    /// Decoding may begin at any restart offset without seeing prior records.
    #[dialog_common::test]
    async fn it_decodes_from_restart_offsets() -> Result<()> {
        let keys: Vec<Vec<u8>> = (0..50u32)
            .map(|i| format!("entity/{i:04}").into_bytes())
            .collect();
        let (prefix, stream, restarts) = encode_keys(&keys);

        for (block, &offset) in restarts.iter().enumerate() {
            let mut cursor = KeyCursor::new(&prefix, &stream, offset as usize);
            cursor.advance()?;
            assert_eq!(cursor.key(), keys[block * RESTART_INTERVAL].as_slice());
        }
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
