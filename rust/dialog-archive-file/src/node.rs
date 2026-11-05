//! A tree node as rows over tokens, and the block rows stand for.
//!
//! The nodes the file writes as frames are the artifact tree's: keys are
//! [`dialog_artifacts::Key`]s, split into entity, attribute and value
//! tokens, and values are [`State<Datum>`]s, each one token. A node of any
//! other tree, or one whose rows do not realize to its exact block, is not
//! written as a frame.

use std::io;

use dialog_artifacts::{
    ATTRIBUTE_KEY_TAG, BLOB_KEY_TAG, Datum, ENTITY_KEY_TAG, Key, State, VALUE_KEY_TAG,
};
use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{
    Entry, Key as TreeKey, Link, Manifest, NodeBody, NoveltyBuffer, NoveltyEntry, NoveltyOp,
    PersistentNode, PersistentNodeBody, PersistentSegment, Scale, Separator, into_owned,
};

use crate::rows::{INDEX, NONE, Row, Rows, SEGMENT, invalid, slots_of};
use crate::tokens::Tokens;

/// A node of the artifact tree.
pub(crate) type TreeNode = PersistentNode<Key, State<Datum>>;

/// The form of a key that does not split into tokens: the whole key is one.
const FORM_OPAQUE: u8 = 0xFE;

fn tree_error(error: impl std::fmt::Display) -> io::Error {
    invalid(&error.to_string())
}

/// `block` read as an artifact tree node, if it is one.
pub(crate) fn tree_node(block: &Buffer) -> Option<TreeNode> {
    TreeNode::try_from(block.clone()).ok()
}

/// Splits a key into its form and the tokens its slots point at: the
/// entity, the attribute and the value (its type byte and payload together,
/// so a value is one token in every ordering). A key of any other layout,
/// or one that does not split, is one opaque token.
fn key_tokens(key: &[u8]) -> (u8, Vec<Vec<u8>>) {
    let layout = <Key as TreeKey>::layout_of(key).unwrap_or(u8::MAX);
    let mut parts = Vec::new();
    if <Key as TreeKey>::components_of(key, layout, &mut parts).is_err() {
        return (FORM_OPAQUE, vec![key.to_vec()]);
    }
    let joined = |a: &[u8], b: &[u8]| [a, b].concat();
    match (layout, parts.as_slice()) {
        (ENTITY_KEY_TAG, [_, entity, attribute, kind, payload])
        | (ATTRIBUTE_KEY_TAG, [_, attribute, entity, kind, payload])
        | (VALUE_KEY_TAG, [_, kind, payload, attribute, entity]) => (
            layout,
            vec![entity.to_vec(), attribute.to_vec(), joined(kind, payload)],
        ),
        (BLOB_KEY_TAG, [_, hash]) => (BLOB_KEY_TAG, vec![hash.to_vec()]),
        _ => (FORM_OPAQUE, vec![key.to_vec()]),
    }
}

/// Rebuilds a key from its form and tokens: the inverse of [`key_tokens`].
fn key_from_tokens(form: u8, tokens: &[&[u8]]) -> io::Result<Vec<u8>> {
    Ok(match (form, tokens) {
        (ENTITY_KEY_TAG, [entity, attribute, value]) => {
            [&[form][..], entity, attribute, value].concat()
        }
        (ATTRIBUTE_KEY_TAG, [entity, attribute, value]) => {
            [&[form][..], attribute, entity, value].concat()
        }
        (VALUE_KEY_TAG, [entity, attribute, value]) => {
            [&[form][..], value, attribute, entity].concat()
        }
        (BLOB_KEY_TAG, [hash]) => [&[form][..], hash].concat(),
        (_, [key]) => key.to_vec(),
        _ => return Err(invalid("a key's form and its tokens disagree")),
    })
}

/// The row of one entry's key and datum, by token id.
fn entry_row(tokens: &mut Tokens, key: &[u8], datum: Option<&State<Datum>>) -> io::Result<Row> {
    let (form, parts) = key_tokens(key);
    let mut row = [form as u32, 0, 0, 0, NONE];
    for (slot, token) in parts.iter().enumerate() {
        row[1 + slot] = tokens.intern(token);
    }
    if let Some(datum) = datum {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(datum).map_err(tree_error)?;
        row[4] = tokens.intern(&bytes);
    }
    Ok(row)
}

/// The key and datum a row stands for.
fn entry_of(row: &Row, tokens: &Tokens) -> io::Result<(Vec<u8>, Option<State<Datum>>)> {
    let form = row[0] as u8;
    let token = |id: u32| {
        tokens
            .get(id)
            .ok_or_else(|| invalid("a token the file lacks"))
    };
    let parts = (0..slots_of(form))
        .map(|slot| token(row[1 + slot]))
        .collect::<io::Result<Vec<_>>>()?;
    let datum = match row[4] {
        NONE => None,
        id => {
            let bytes = token(id)?;
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
            aligned.extend_from_slice(bytes);
            Some(
                rkyv::from_bytes::<State<Datum>, rkyv::rancor::Error>(&aligned)
                    .map_err(tree_error)?,
            )
        }
    };
    Ok((key_from_tokens(form, &parts)?, datum))
}

/// Splits `node` into rows, interning its tokens. A child is named by its
/// node id when `child` knows one, else by a token of its hash.
pub(crate) fn rows_of(
    node: &TreeNode,
    tokens: &mut Tokens,
    child: impl Fn(&Blake3Hash) -> Option<u32>,
) -> io::Result<Rows> {
    match node.body() {
        NodeBody::Segment(segment) => {
            let mut entries = Vec::with_capacity(segment.len());
            let mut keys = segment.keys::<Key>().map_err(tree_error)?;
            while let Some((at, key)) = keys.next_key().map_err(tree_error)? {
                let value: State<Datum> =
                    into_owned(segment.value_at(at).map_err(tree_error)?).map_err(tree_error)?;
                entries.push(entry_row(tokens, key, Some(&value))?);
            }
            Ok(Rows {
                kind: SEGMENT,
                sections: vec![entries],
            })
        }
        NodeBody::Index(index) => {
            let mut links = Vec::with_capacity(index.len());
            for at in 0..index.len() {
                let separator = tokens.intern(&index.separator(at).map_err(tree_error)?);
                let hash = index.hash_at(at).map_err(tree_error)?;
                let (id, absent) = match child(hash) {
                    Some(id) => (id, NONE),
                    None => (NONE, tokens.intern(hash.as_bytes())),
                };
                let scale = index.scale_at(at).map_err(tree_error)?.as_u8() as u32;
                links.push([separator, id, scale, absent, 0]);
            }
            let (mut buffers, mut ops) = (Vec::new(), Vec::new());
            for buffer in index.novelty.iter() {
                let entries = buffer.entries::<Key>().map_err(tree_error)?;
                buffers.push([buffer.child.to_native(), entries.len() as u32, 0, 0, 0]);
                for item in &entries {
                    let datum = match &item.op {
                        NoveltyOp::Assert(value) => Some(value),
                        NoveltyOp::Retract => None,
                    };
                    ops.push(entry_row(tokens, &item.key, datum)?);
                }
            }
            Ok(Rows {
                kind: INDEX,
                sections: vec![links, buffers, ops],
            })
        }
    }
}

/// The block `rows` stand for, built by the tree's own encoder under
/// `manifest`. A child named by node id is named by its hash through
/// `child`.
pub(crate) fn realize(
    rows: &Rows,
    tokens: &Tokens,
    manifest: &Manifest,
    child: impl Fn(u32) -> Option<Blake3Hash>,
) -> io::Result<Vec<u8>> {
    let body = if rows.kind == INDEX {
        let [links, buffers, ops] = rows.sections.as_slice() else {
            return Err(invalid("an index frame has three sections"));
        };
        let links = links
            .iter()
            .map(|row| {
                let node = match row[1] {
                    NONE => {
                        let bytes = tokens
                            .get(row[3])
                            .ok_or_else(|| invalid("a token the file lacks"))?;
                        Blake3Hash::from(
                            <[u8; 32]>::try_from(bytes)
                                .map_err(|_| invalid("a child hash is not 32 bytes"))?,
                        )
                    }
                    id => child(id).ok_or_else(|| invalid("a child node the file lacks"))?,
                };
                Ok(Link {
                    separator: Separator::from(
                        tokens
                            .get(row[0])
                            .ok_or_else(|| invalid("a token the file lacks"))?
                            .to_vec(),
                    ),
                    node,
                    scale: Scale::from_u8(row[2] as u8),
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut novelty = Vec::with_capacity(buffers.len());
        let mut next = 0;
        for buffer in buffers {
            let len = buffer[1] as usize;
            let rows = ops
                .get(next..next + len)
                .ok_or_else(|| invalid("a buffer past its ops"))?;
            let entries = rows
                .iter()
                .map(|row| {
                    let (key, datum) = entry_of(row, tokens)?;
                    let op = match datum {
                        Some(value) => NoveltyOp::Assert(value),
                        None => NoveltyOp::Retract,
                    };
                    Ok(NoveltyEntry { key, op })
                })
                .collect::<io::Result<Vec<_>>>()?;
            next += len;
            novelty
                .push(NoveltyBuffer::from_entries::<Key>(buffer[0], entries).map_err(tree_error)?);
        }
        PersistentNodeBody::index_from_buffers(links, novelty, manifest.clone())
            .map_err(tree_error)?
    } else {
        let entries = rows
            .sections
            .first()
            .ok_or_else(|| invalid("a segment frame has a section"))?
            .iter()
            .map(|row| {
                let (key, datum) = entry_of(row, tokens)?;
                Ok(Entry {
                    key: <Key as TreeKey>::try_from_bytes(&key).map_err(tree_error)?,
                    value: datum.ok_or_else(|| invalid("a segment entry with no value"))?,
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        PersistentNodeBody::from_segment(
            PersistentSegment::from_entries::<Key>(entries).map_err(tree_error)?,
            manifest.clone(),
        )
    };
    Ok(body.as_bytes().map_err(tree_error)?.to_vec())
}
