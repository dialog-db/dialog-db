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
/// so a value is one token in every ordering), each a slice of the key. A
/// key of any other layout, or one that does not split, is one opaque
/// token. Returns the form, the tokens and how many there are.
fn key_tokens(key: &[u8]) -> (u8, [&[u8]; 3], usize) {
    let opaque = (FORM_OPAQUE, [key, &[][..], &[][..]], 1);
    let layout = <Key as TreeKey>::layout_of(key).unwrap_or(u8::MAX);
    let mut parts = Vec::with_capacity(5);
    if <Key as TreeKey>::components_of(key, layout, &mut parts).is_err() {
        return opaque;
    }
    // The bytes of `key` from the start of `first` to the end of `last`,
    // which components lay side by side.
    let span = |first: &[u8], last: &[u8]| -> Option<&[u8]> {
        let base = key.as_ptr() as usize;
        let start = (first.as_ptr() as usize).checked_sub(base)?;
        let end = (last.as_ptr() as usize).checked_sub(base)? + last.len();
        key.get(start..end)
    };
    let split = match (layout, parts.as_slice()) {
        (ENTITY_KEY_TAG, [_, entity, attribute, kind, payload])
        | (ATTRIBUTE_KEY_TAG, [_, attribute, entity, kind, payload])
        | (VALUE_KEY_TAG, [_, kind, payload, attribute, entity]) => {
            span(kind, payload).map(|value| (layout, [*entity, *attribute, value], 3))
        }
        (BLOB_KEY_TAG, [_, hash]) => Some((BLOB_KEY_TAG, [*hash, &[][..], &[][..]], 1)),
        _ => None,
    };
    split.unwrap_or(opaque)
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
    let (form, parts, count) = key_tokens(key);
    let mut row = [form as u32, 0, 0, 0, NONE];
    for (slot, token) in parts[..count].iter().enumerate() {
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
            for buffer in index.buffers() {
                let entries = buffer.entries::<Key>().map_err(tree_error)?;
                buffers.push([buffer.child(), entries.len() as u32, 0, 0, 0]);
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

#[cfg(test)]
mod profile {
    use super::*;
    use std::time::{Duration, Instant};

    /// Times each part of splitting every node of the archive file at
    /// `DIALOG_ARCHIVE_PROFILE` into rows. Run by hand.
    #[test]
    #[ignore = "profiles a file named by DIALOG_ARCHIVE_PROFILE"]
    fn split_costs() -> anyhow::Result<()> {
        let Ok(path) = std::env::var("DIALOG_ARCHIVE_PROFILE") else {
            return Ok(());
        };
        let archive = crate::FileArchive::open(&path)?;
        let mut blocks = Vec::new();
        for (catalog, hash) in archive.hashes() {
            blocks.push(Buffer::from(archive.get(&catalog, &hash)?.expect("listed")));
        }
        let (mut parse, mut keys, mut owned, mut archived, mut intern) = (
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        );
        let (mut entries, mut nodes, mut realizing) = (0usize, 0usize, Duration::ZERO);
        let mut splitting = Duration::ZERO;
        let mut whole = Duration::ZERO;
        let mut tokens = Tokens::default();
        for block in &blocks {
            let started = Instant::now();
            let Some(node) = tree_node(block) else {
                continue;
            };
            parse += started.elapsed();
            nodes += 1;
            if let NodeBody::Segment(segment) = node.body() {
                let started = Instant::now();
                let mut cursor = segment.keys::<Key>()?;
                let mut decoded = Vec::new();
                while let Some((at, key)) = cursor.next_key()? {
                    decoded.push((at, key.to_vec()));
                }
                keys += started.elapsed();
                let started = Instant::now();
                let mut all = Vec::with_capacity(decoded.len());
                for (at, key) in &decoded {
                    let (_, parts, count) = key_tokens(key);
                    all.push((*at, parts[..count].len()));
                }
                splitting += started.elapsed();
                let all: Vec<(usize, Vec<Vec<u8>>)> = decoded
                    .iter()
                    .map(|(at, key)| {
                        let (_, parts, count) = key_tokens(key);
                        (
                            *at,
                            parts[..count].iter().map(|part| part.to_vec()).collect(),
                        )
                    })
                    .collect();
                for (at, parts) in all {
                    entries += 1;
                    let started = Instant::now();
                    let value: State<Datum> = into_owned(segment.value_at(at)?)?;
                    owned += started.elapsed();
                    let started = Instant::now();
                    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
                    archived += started.elapsed();
                    let started = Instant::now();
                    for part in &parts {
                        tokens.intern(part);
                    }
                    tokens.intern(&bytes);
                    intern += started.elapsed();
                }
            }
            let started = Instant::now();
            let rows = rows_of(&node, &mut tokens, |_| None)?;
            whole += started.elapsed();
            let manifest = node.manifest()?;
            let started = Instant::now();
            realize(&rows, &tokens, &manifest, |_| None)?;
            realizing += started.elapsed();
        }
        let per = |d: Duration| d.as_secs_f64() * 1e6 / nodes as f64;
        println!(
            "{nodes} nodes, {entries} segment entries; per node µs: parse {:.0}, decode keys {:.0}, \
             split keys {:.0}, values owned {:.0}, values archived {:.0}, intern {:.0}, \
             rows_of (warm tokens) {:.0}, realize {:.0}",
            per(parse),
            per(keys),
            per(splitting),
            per(owned),
            per(archived),
            per(intern),
            per(whole),
            per(realizing)
        );
        Ok(())
    }
}
