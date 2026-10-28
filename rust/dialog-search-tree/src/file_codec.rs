//! Lossless single-file archive codec for the tree's columnar wire blocks.
//!
//! Arena columns are decoded to full component values, dictionary tables to
//! distinct values. These tokens are shared across nodes, indexes and historical
//! versions by the archive. On read the ordinary column codec recreates the exact
//! bytes. Other bytes (including value payloads and relative pointers) are kept
//! verbatim. Unrecognized blocks use an opaque fallback.
use crate::node::codec::{KeyCursor, encode_keys, read_varint, write_varint};
use crate::{ArchivedColumnData, Buffer, Key, NodeBody, PersistentNode, Value};
use dialog_storage::provider::single_file::{BlockFrame, FrameCodec};
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};
use std::{io, marker::PhantomData};

/// Codec specialized for the key and value types of an embedder's tree.
///
/// Construct with [`TreeFrameCodec::new`] and pass to
/// [`dialog_storage::provider::SingleFile::open`]. Opaque revision records and
/// unrelated block formats are stored losslessly in the same archive.
pub struct TreeFrameCodec<K, V>(PhantomData<fn() -> (K, V)>);
impl<K, V> TreeFrameCodec<K, V> {
    /// Create a codec for this tree's wire layout.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
impl<K, V> Default for TreeFrameCodec<K, V> {
    fn default() -> Self {
        Self::new()
    }
}
impl<K, V> Clone for TreeFrameCodec<K, V> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}
fn number(out: &mut Vec<u8>, value: usize) -> io::Result<()> {
    write_varint(
        out,
        value
            .try_into()
            .map_err(|_| invalid("frame length exceeds u32"))?,
    );
    Ok(())
}
fn read(bytes: &mut &[u8]) -> io::Result<usize> {
    let (value, n) = read_varint(bytes, 0).map_err(invalid)?;
    *bytes = &bytes[n..];
    Ok(value as usize)
}
fn take<'a>(bytes: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if n > bytes.len() {
        return Err(invalid("truncated column frame"));
    }
    let (head, tail) = bytes.split_at(n);
    *bytes = tail;
    Ok(head)
}
struct Patch {
    start: usize,
    end: usize,
    kind: u8,
    first: usize,
    count: usize,
}
fn patch(
    base: &[u8],
    bytes: &[u8],
    kind: u8,
    first: usize,
    count: usize,
    patches: &mut Vec<Patch>,
) -> io::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    let start = bytes
        .as_ptr()
        .addr()
        .checked_sub(base.as_ptr().addr())
        .ok_or_else(|| invalid("column outside node"))?;
    let end = start
        .checked_add(bytes.len())
        .filter(|end| *end <= base.len())
        .ok_or_else(|| invalid("column outside node"))?;
    patches.push(Patch {
        start,
        end,
        kind,
        first,
        count,
    });
    Ok(())
}
fn column(
    base: &[u8],
    column: &ArchivedColumnData,
    count: usize,
    tokens: &mut Vec<Vec<u8>>,
    patches: &mut Vec<Patch>,
) -> io::Result<()> {
    let first = tokens.len();
    match column {
        ArchivedColumnData::Arena { prefix, stream } => {
            let mut cursor = KeyCursor::new(prefix, stream, 0);
            for _ in 0..count {
                cursor.advance().map_err(invalid)?;
                tokens.push(cursor.key().to_vec());
            }
            patch(base, prefix, 1, first, count, patches)?;
            patch(base, stream, 2, first, count, patches)?;
        }
        ArchivedColumnData::Dictionary { table, .. } => {
            let mut bytes: &[u8] = table;
            while !bytes.is_empty() {
                let len = read(&mut bytes)?;
                tokens.push(take(&mut bytes, len)?.to_vec());
            }
            patch(base, table, 3, first, tokens.len() - first, patches)?;
        }
    }
    Ok(())
}
impl<K, V> FrameCodec for TreeFrameCodec<K, V>
where
    K: Key + 'static,
    V: Value + 'static,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    fn identifier(&self) -> &'static str {
        "dialog-columns-v1"
    }
    fn encode(&self, bytes: &[u8]) -> io::Result<BlockFrame> {
        let opaque = || BlockFrame {
            metadata: vec![0],
            tokens: vec![bytes.to_vec()],
        };
        let Ok(node) = PersistentNode::<K, V>::try_from(Buffer::from(bytes.to_vec())) else {
            return Ok(opaque());
        };
        let base = node.buffer().as_ref();
        let mut tokens = Vec::new();
        let mut patches = Vec::new();
        let columns = (|| -> io::Result<()> {
            match node.body() {
                NodeBody::Index(index) => {
                    for buffer in index.novelty.iter() {
                        for c in buffer.columns.iter() {
                            column(
                                base,
                                c,
                                buffer.count.to_native() as usize,
                                &mut tokens,
                                &mut patches,
                            )?;
                        }
                    }
                }
                NodeBody::Segment(segment) => {
                    for c in segment.columns.iter() {
                        column(
                            base,
                            c,
                            segment.count.to_native() as usize,
                            &mut tokens,
                            &mut patches,
                        )?;
                    }
                }
            }
            Ok(())
        })();
        if columns.is_err() || patches.is_empty() {
            return Ok(opaque());
        }
        patches.sort_unstable_by_key(|p| p.start);
        let mut metadata = vec![1];
        number(&mut metadata, bytes.len())?;
        let mut at = 0;
        for p in patches {
            if p.start < at {
                return Ok(opaque());
            }
            if p.start > at {
                metadata.push(0);
                number(&mut metadata, p.start - at)?;
                metadata.extend_from_slice(&base[at..p.start]);
            }
            metadata.push(p.kind);
            number(&mut metadata, p.first)?;
            number(&mut metadata, p.count)?;
            at = p.end;
        }
        if at < base.len() {
            metadata.push(0);
            number(&mut metadata, base.len() - at)?;
            metadata.extend_from_slice(&base[at..]);
        }
        let frame = BlockFrame { metadata, tokens };
        // Noncanonical external encodings must still round-trip exactly.
        let refs: Vec<_> = frame.tokens.iter().map(Vec::as_slice).collect();
        if self.decode(&frame.metadata, &refs).as_deref().ok() != Some(bytes) {
            return Ok(opaque());
        }
        Ok(frame)
    }
    fn decode(&self, metadata: &[u8], tokens: &[&[u8]]) -> io::Result<Vec<u8>> {
        if metadata == [0] {
            if tokens.len() != 1 {
                return Err(invalid("invalid opaque frame"));
            }
            return Ok(tokens[0].to_vec());
        }
        let mut input = metadata;
        if take(&mut input, 1)? != [1] {
            return Err(invalid("unknown column frame"));
        }
        let len = read(&mut input)?;
        // Avoid allocating a corrupt claimed output length in advance.
        let mut out = Vec::new();
        while !input.is_empty() {
            let kind = take(&mut input, 1)?[0];
            if kind == 0 {
                let n = read(&mut input)?;
                out.extend_from_slice(take(&mut input, n)?);
            } else {
                let first = read(&mut input)?;
                let count = read(&mut input)?;
                let end = first
                    .checked_add(count)
                    .ok_or_else(|| invalid("token range overflow"))?;
                let values = tokens
                    .get(first..end)
                    .ok_or_else(|| invalid("token range outside frame"))?;
                match kind {
                    1 => out.extend_from_slice(&encode_keys(values).0),
                    2 => out.extend_from_slice(&encode_keys(values).1),
                    3 => {
                        for value in values {
                            number(&mut out, value.len())?;
                            out.extend_from_slice(value);
                        }
                    }
                    _ => return Err(invalid("unknown column instruction")),
                }
            }
            if out.len() > len {
                return Err(invalid("column frame exceeds wire length"));
            }
        }
        if out.len() != len {
            return Err(invalid("column frame wire length mismatch"));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Entry, Manifest, PersistentNodeBody};
    #[test]
    fn tokens_roundtrip_and_recur_across_node_versions() -> anyhow::Result<()> {
        let codec = TreeFrameCodec::<[u8; 4], Vec<u8>>::new();
        let mut previous = Vec::new();
        for n in [100u32, 101] {
            let entries: Vec<_> = (0..n)
                .map(|i| Entry {
                    key: i.to_be_bytes(),
                    value: vec![7],
                })
                .collect();
            let body =
                PersistentNodeBody::segment_from_entries::<[u8; 4]>(entries, Manifest::default())?;
            let node = PersistentNode::<[u8; 4], Vec<u8>>::try_from(&body)?;
            let frame = codec.encode(node.buffer().as_ref())?;
            assert_eq!(frame.metadata[0], 1);
            let refs: Vec<_> = frame.tokens.iter().map(Vec::as_slice).collect();
            assert_eq!(
                codec.decode(&frame.metadata, &refs)?,
                node.buffer().as_ref()
            );
            if !previous.is_empty() {
                assert_eq!(&frame.tokens[..100], previous.as_slice());
            }
            previous = frame.tokens;
        }
        for bytes in [vec![], b"opaque non-node".to_vec()] {
            let frame = codec.encode(&bytes)?;
            assert_eq!(codec.decode(&frame.metadata, &[&frame.tokens[0]])?, bytes);
        }
        Ok(())
    }
}
