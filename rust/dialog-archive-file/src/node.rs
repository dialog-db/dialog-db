//! A tree node as rows over tokens, and the block rows stand for.
//!
//! The nodes the file writes as frames are the artifact tree's: keys are
//! [`dialog_artifacts::Key`]s, split into entity, attribute and value
//! tokens, and values are [`State<Datum>`]s, each one token. A node of any
//! other tree, or one whose rows do not realize to its exact block, is not
//! written as a frame.

use std::io;

use dialog_artifacts::{
    ATTRIBUTE_KEY_TAG, BLOB_KEY_TAG, COVERAGE_KEY_TAG, Datum, ENTITY_KEY_TAG, HISTORY_KEY_TAG, Key,
    State, VALUE_KEY_TAG, split_record_components,
};
use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{
    Entry, Key as TreeKey, Link, MIXED_LAYOUT, Manifest, NodeBody, NoveltyBuffer, NoveltyEntry,
    NoveltyOp, PersistentNode, PersistentNodeBody, PersistentSegment, Scale, Separator, into_owned,
};

use crate::rows::{DATUM, INDEX, NONE, Row, Rows, SEGMENT, SLOTS, invalid, slots_of};
use crate::tokens::{Draft, TokenRead};

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

/// Splits a key into its form and the tokens its slots point at, each a
/// slice of the key: for a fact ordering, the entity, the attribute and the
/// value (its type byte and payload together, so a value is one token in
/// every ordering); for a history or coverage record, its version, then the
/// same three, so a record shares its fact's tokens and a revision's
/// records share one version. A key of any other layout, or one that does
/// not split, is one opaque token. Returns the form, the tokens and how many
/// there are.
fn key_tokens(key: &[u8]) -> (u8, [&[u8]; SLOTS], usize) {
    let empty: &[u8] = &[];
    let opaque = (FORM_OPAQUE, [key, empty, empty, empty], 1);
    let Some(parts) = split_record_components(key) else {
        return opaque;
    };
    // The bytes of `key` from the start of `first` to the end of `last`,
    // which components lay side by side.
    let span = |first: &[u8], last: &[u8]| -> Option<&[u8]> {
        let base = key.as_ptr() as usize;
        let start = (first.as_ptr() as usize).checked_sub(base)?;
        let end = (last.as_ptr() as usize).checked_sub(base)? + last.len();
        key.get(start..end)
    };
    let form = key[0];
    let split = match (form, parts.as_slice()) {
        (ENTITY_KEY_TAG, [_, entity, attribute, kind, payload])
        | (ATTRIBUTE_KEY_TAG, [_, attribute, entity, kind, payload])
        | (VALUE_KEY_TAG, [_, kind, payload, attribute, entity]) => {
            span(kind, payload).map(|value| (form, [*entity, *attribute, value, empty], 3))
        }
        (HISTORY_KEY_TAG | COVERAGE_KEY_TAG, [_, version, entity, attribute, kind, payload]) => {
            span(kind, payload).map(|value| (form, [*version, *entity, *attribute, value], 4))
        }
        (BLOB_KEY_TAG, [_, hash]) => Some((BLOB_KEY_TAG, [*hash, empty, empty, empty], 1)),
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
        (HISTORY_KEY_TAG | COVERAGE_KEY_TAG, [version, entity, attribute, value]) => {
            [&[form][..], version, entity, attribute, value].concat()
        }
        (BLOB_KEY_TAG, [hash]) => [&[form][..], hash].concat(),
        (_, [key]) => key.to_vec(),
        _ => return Err(invalid("a key's form and its tokens disagree")),
    })
}

/// The row of one entry's key and datum, by token id.
fn entry_row(tokens: &mut Draft<'_>, key: &[u8], datum: Option<&State<Datum>>) -> io::Result<Row> {
    let (form, parts, count) = key_tokens(key);
    let mut row = [form as u32, 0, 0, 0, 0, NONE];
    for (slot, token) in parts[..count].iter().enumerate() {
        row[1 + slot] = tokens.intern(token);
    }
    if let Some(datum) = datum {
        row[DATUM] = SCRATCH.with_borrow_mut(|scratch| -> io::Result<u32> {
            Ok(tokens.intern(scratch.archive(datum)?))
        })?;
    }
    Ok(row)
}

/// Where a datum is archived into its token's bytes: one output buffer and
/// one scratch arena, kept across entries rather than allocated for each.
/// The bytes are the same as `rkyv::to_bytes` makes.
#[derive(Default)]
struct Scratch {
    bytes: rkyv::util::AlignedVec<16>,
    arena: rkyv::ser::allocator::Arena,
}

impl Scratch {
    /// `datum` as a standalone archive, in this scratch's buffer.
    fn archive(&mut self, datum: &State<Datum>) -> io::Result<&[u8]> {
        let mut bytes = std::mem::take(&mut self.bytes);
        bytes.clear();
        self.bytes = rkyv::api::high::to_bytes_in_with_alloc::<_, _, rkyv::rancor::Error>(
            datum,
            bytes,
            self.arena.acquire(),
        )
        .map_err(tree_error)?;
        Ok(self.bytes.as_slice())
    }
}

thread_local! {
    static SCRATCH: std::cell::RefCell<Scratch> = std::cell::RefCell::new(Scratch::default());
}

/// The key and datum a row stands for.
fn entry_of<T: TokenRead + ?Sized>(
    row: &Row,
    tokens: &T,
) -> io::Result<(Vec<u8>, Option<State<Datum>>)> {
    let form = row[0] as u8;
    let token = |id: u32| {
        tokens
            .get(id)
            .ok_or_else(|| invalid("a token the file lacks"))
    };
    let parts = (0..slots_of(form))
        .map(|slot| token(row[1 + slot]))
        .collect::<io::Result<Vec<_>>>()?;
    let datum = match row[DATUM] {
        NONE => None,
        id => Some(datum_of(token(id)?)?),
    };
    Ok((key_from_tokens(form, &parts)?, datum))
}

/// Splits `node` into rows, interning its tokens. A child is named by its
/// node id when `child` knows one, else by a token of its hash.
pub(crate) fn rows_of(
    node: &TreeNode,
    tokens: &mut Draft<'_>,
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
                links.push([separator, id, scale, absent, 0, 0]);
            }
            let (mut buffers, mut ops) = (Vec::new(), Vec::new());
            for buffer in index.buffers() {
                let entries = buffer.entries::<Key>().map_err(tree_error)?;
                buffers.push([buffer.child(), entries.len() as u32, 0, 0, 0, 0]);
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

/// The segment `rows` stand for, its keys' columns taken from the tokens
/// themselves: a fact ordering's tokens are the slices its schema stores
/// (the value token split into its type byte and payload), and a segment
/// whose schema keeps whole keys gets each key once, laid side by side in
/// one buffer for the node. No key is rebuilt, parsed or split again.
/// `None` when a row does not fit its segment's schema, for the caller to
/// build the segment from whole entries instead.
fn segment_of<T: TokenRead + ?Sized>(
    rows: &[Row],
    tokens: &T,
) -> io::Result<Option<PersistentSegment<State<Datum>>>> {
    let token = |id: u32| {
        tokens
            .get(id)
            .ok_or_else(|| invalid("a token the file lacks"))
    };
    // A key's layout is its first byte, which a split row keeps as its form.
    let layout_of = |row: &Row| -> io::Result<u8> {
        match row[0] as u8 {
            FORM_OPAQUE => Ok(token(row[1])?.first().copied().unwrap_or(u8::MIN)),
            form => Ok(form),
        }
    };
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let first = layout_of(first)?;
    let mut uniform = true;
    for row in rows {
        if layout_of(row)? != first {
            uniform = false;
            break;
        }
    }
    let layout = if uniform { first } else { MIXED_LAYOUT };
    let values = rows
        .iter()
        .map(|row| match row[DATUM] {
            NONE => Err(invalid("a segment entry with no value")),
            id => datum_of(token(id)?),
        })
        .collect::<io::Result<Vec<_>>>()?;

    let split = matches!(
        layout,
        ENTITY_KEY_TAG | ATTRIBUTE_KEY_TAG | VALUE_KEY_TAG | BLOB_KEY_TAG
    );
    if split {
        let width = if layout == BLOB_KEY_TAG { 2 } else { 5 };
        let mut columns: Vec<Vec<&[u8]>> =
            (0..width).map(|_| Vec::with_capacity(rows.len())).collect();
        for row in rows {
            if row[0] as u8 != layout {
                return Ok(None);
            }
            let tag = &FORMS[layout as usize..layout as usize + 1];
            let parts: [&[u8]; 5] = if layout == BLOB_KEY_TAG {
                [tag, token(row[1])?, &[], &[], &[]]
            } else {
                let (entity, attribute) = (token(row[1])?, token(row[2])?);
                let value = token(row[3])?;
                if value.is_empty() {
                    return Ok(None);
                }
                let (kind, payload) = value.split_at(1);
                match layout {
                    ENTITY_KEY_TAG => [tag, entity, attribute, kind, payload],
                    ATTRIBUTE_KEY_TAG => [tag, attribute, entity, kind, payload],
                    _ => [tag, kind, payload, attribute, entity],
                }
            };
            for (column, part) in columns.iter_mut().zip(parts) {
                column.push(part);
            }
        }
        return PersistentSegment::from_columns::<Key>(layout, &columns, values)
            .map(Some)
            .map_err(tree_error);
    }
    if layout != MIXED_LAYOUT && <Key as TreeKey>::schema(layout).len() != 1 {
        return Ok(None);
    }

    // The schema keeps whole keys: an opaque row's one token is its key,
    // and a split row's key is its tokens joined, in one buffer for the
    // segment.
    let mut joined = Vec::new();
    let mut spans = Vec::with_capacity(rows.len());
    for row in rows {
        let form = row[0] as u8;
        if form == FORM_OPAQUE {
            spans.push(Err(row[1]));
            continue;
        }
        let parts = (0..slots_of(form))
            .map(|slot| token(row[1 + slot]))
            .collect::<io::Result<Vec<_>>>()?;
        let start = joined.len();
        joined.push(form);
        match (form, parts.as_slice()) {
            (ENTITY_KEY_TAG, [entity, attribute, value]) => {
                joined.extend_from_slice(entity);
                joined.extend_from_slice(attribute);
                joined.extend_from_slice(value);
            }
            (ATTRIBUTE_KEY_TAG, [entity, attribute, value]) => {
                joined.extend_from_slice(attribute);
                joined.extend_from_slice(entity);
                joined.extend_from_slice(value);
            }
            (VALUE_KEY_TAG, [entity, attribute, value]) => {
                joined.extend_from_slice(value);
                joined.extend_from_slice(attribute);
                joined.extend_from_slice(entity);
            }
            (_, parts) => parts.iter().for_each(|part| joined.extend_from_slice(part)),
        }
        spans.push(Ok(start..joined.len()));
    }
    let keys = spans
        .into_iter()
        .map(|span| match span {
            Ok(range) => Ok(&joined[range]),
            Err(id) => token(id),
        })
        .collect::<io::Result<Vec<_>>>()?;
    PersistentSegment::from_columns::<Key>(layout, &[keys], values)
        .map(Some)
        .map_err(tree_error)
}

/// Every byte, so a form's one-byte slice can be taken from it.
static FORMS: [u8; 256] = {
    let mut forms = [0u8; 256];
    let mut at = 0;
    while at < 256 {
        forms[at] = at as u8;
        at += 1;
    }
    forms
};

/// The datum `bytes` archive, read where they are when they are aligned
/// for it, as a token in the file is.
fn datum_of(bytes: &[u8]) -> io::Result<State<Datum>> {
    if (bytes.as_ptr() as usize).is_multiple_of(16) {
        return rkyv::from_bytes::<State<Datum>, rkyv::rancor::Error>(bytes).map_err(tree_error);
    }
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<State<Datum>, rkyv::rancor::Error>(&aligned).map_err(tree_error)
}

/// The block `rows` stand for, built by the tree's own encoder under
/// `manifest`. A child named by node id is named by its hash through
/// `child`.
pub(crate) fn realize<T: TokenRead + ?Sized>(
    rows: &Rows,
    tokens: &T,
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
        let rows = rows
            .sections
            .first()
            .ok_or_else(|| invalid("a segment frame has a section"))?;
        let segment = match segment_of(rows, tokens)? {
            Some(segment) => segment,
            None => {
                let entries = rows
                    .iter()
                    .map(|row| {
                        let (key, datum) = entry_of(row, tokens)?;
                        Ok(Entry {
                            key: <Key as TreeKey>::try_from_bytes(&key).map_err(tree_error)?,
                            value: datum.ok_or_else(|| invalid("a segment entry with no value"))?,
                        })
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                PersistentSegment::from_entries::<Key>(entries).map_err(tree_error)?
            }
        };
        PersistentNodeBody::from_segment(segment, manifest.clone())
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
        let mut tokens = Draft::detached();
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
                    let bytes = super::SCRATCH
                        .with_borrow_mut(|scratch| scratch.archive(&value).map(<[u8]>::to_vec))?;
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

#[cfg(test)]
pub(crate) mod tests {
    use std::str::FromStr;

    use dialog_artifacts::history::{Edition, Origin, Version};
    use dialog_artifacts::{
        Artifact, AttributeKey, BlobKey, Entity, EntityKey, Relation, Value, ValueKey,
        coverage_key, history_key,
    };

    use super::*;

    fn facts() -> anyhow::Result<Vec<Artifact>> {
        let long = "a value long enough to spill out of its key ".repeat(40);
        let values = [
            Value::String("short".into()),
            Value::String(long),
            Value::Entity(Entity::from_str("test:other")?),
            Value::Symbol(Relation::from_str("some/symbol")?),
            Value::UnsignedInt(7),
            Value::SignedInt(-7),
            Value::Float(1.5),
            Value::Boolean(true),
            Value::Bytes(vec![0, 1, 2, 0xff]),
            Value::Record(vec![9; 300]),
        ];
        let mut facts = Vec::new();
        for (at, is) in values.into_iter().enumerate() {
            facts.push(Artifact {
                the: Relation::from_str(&format!("test/attribute-{}", at % 3))?,
                of: Entity::from_str(&format!("test:entity-{}", at % 4))?,
                is,
                cause: None,
            });
        }
        Ok(facts)
    }

    /// A manifest under which the long values spill out of their keys.
    fn spilling() -> Manifest {
        Manifest {
            inline_n: 64,
            spill_prefix: 32,
            ..Manifest::default()
        }
    }

    fn version(edition: u64) -> Version {
        Version::new(Origin::from([7; 32]), Edition::new(edition))
    }

    /// The block of a segment holding `keys`, each with a datum.
    fn segment_block(mut keys: Vec<Key>, manifest: &Manifest) -> anyhow::Result<Vec<u8>> {
        keys.sort();
        keys.dedup();
        let entries = keys
            .into_iter()
            .enumerate()
            .map(|(at, key)| Entry {
                key,
                value: State::Added(Datum {
                    cause: None,
                    blob: (at % 2 == 0).then(|| vec![at as u8; 3]),
                    version: Some(version(at as u64)),
                    collapsed: Vec::new(),
                    supersedes: Vec::new(),
                    retraction: false,
                }),
            })
            .collect::<Vec<_>>();
        let body = PersistentNodeBody::from_segment(
            PersistentSegment::from_entries::<Key>(entries)?,
            manifest.clone(),
        );
        Ok(body.as_bytes()?.to_vec())
    }

    /// The manifest test segments are written under.
    pub(crate) fn segment_manifest() -> Manifest {
        spilling()
    }

    /// The block of a segment of facts about `name`, in the entity
    /// ordering.
    pub(crate) fn entity_segment(name: &str, manifest: &Manifest) -> anyhow::Result<Buffer> {
        let keys = facts()?
            .into_iter()
            .map(|mut fact| {
                fact.of = Entity::from_str(&format!("test:{name}"))?;
                Ok(EntityKey::from_artifact(&fact, manifest).0)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Buffer::from(segment_block(keys, manifest)?))
    }

    /// The block of an index node linking to `children` (segments, in key
    /// order), each under its first key.
    pub(crate) fn index_over(children: &[Buffer], manifest: &Manifest) -> anyhow::Result<Buffer> {
        let links = children
            .iter()
            .map(|child| {
                let node = tree_node(child).expect("a tree node");
                let NodeBody::Segment(segment) = node.body() else {
                    anyhow::bail!("an index over segments");
                };
                let mut keys = segment.keys::<Key>()?;
                let (_, first) = keys.next_key()?.expect("a segment with entries");
                Ok(Link {
                    separator: Separator::from(first.to_vec()),
                    node: child.blake3_hash().clone(),
                    scale: Scale::from_u8(1),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let body = PersistentNodeBody::<State<Datum>>::index_from_buffers(
            links,
            Vec::new(),
            manifest.clone(),
        )?;
        Ok(Buffer::from(body.as_bytes()?.to_vec()))
    }

    /// Splits the segment of `keys` into rows and realizes them again,
    /// asserting the columns came from the tokens and the block is the
    /// same, byte for byte.
    fn assert_realizes(keys: Vec<Key>) -> anyhow::Result<()> {
        let manifest = spilling();
        let block = segment_block(keys, &manifest)?;
        let node = tree_node(&Buffer::from(block.clone())).expect("a tree node");
        let mut tokens = Draft::detached();
        let rows = rows_of(&node, &mut tokens, |_| None)?;
        assert!(
            segment_of(&rows.sections[0], &tokens)?.is_some(),
            "the columns come from the tokens"
        );
        assert_eq!(realize(&rows, &tokens, &manifest, |_| None)?, block);
        Ok(())
    }

    #[test]
    fn it_realizes_each_layout_from_its_tokens() -> anyhow::Result<()> {
        let manifest = spilling();
        let facts = facts()?;
        let keys = |key: &dyn Fn(&Artifact) -> Key| facts.iter().map(key).collect::<Vec<_>>();
        // The long string spills: its keys carry its hash, not the string.
        assert!(
            ValueKey::from_artifact(&facts[1], &manifest)
                .0
                .as_ref()
                .len()
                < 200
        );
        assert_realizes(keys(&|fact| EntityKey::from_artifact(fact, &manifest).0))?;
        assert_realizes(keys(&|fact| AttributeKey::from_artifact(fact, &manifest).0))?;
        assert_realizes(keys(&|fact| ValueKey::from_artifact(fact, &manifest).0))?;
        assert_realizes(keys(&|fact| {
            history_key(&version(3), &fact.of, &fact.the, &fact.is, &manifest)
        }))?;
        assert_realizes(keys(&|fact| {
            coverage_key(&version(4), &fact.of, &fact.the, &fact.is)
        }))?;
        assert_realizes((0..5u8).map(|at| BlobKey::new(&[at; 32]).0).collect())?;
        Ok(())
    }

    #[test]
    fn it_realizes_a_segment_of_mixed_layouts_from_its_tokens() -> anyhow::Result<()> {
        let manifest = spilling();
        let mut keys = Vec::new();
        for fact in facts()? {
            keys.push(EntityKey::from_artifact(&fact, &manifest).0);
            keys.push(AttributeKey::from_artifact(&fact, &manifest).0);
            keys.push(history_key(
                &version(1),
                &fact.of,
                &fact.the,
                &fact.is,
                &manifest,
            ));
        }
        keys.push(BlobKey::new(&[1; 32]).0);
        // A key that splits into no parts, kept whole in its row.
        keys.push(Key::from(vec![0xFD, 1, 2, 3]));
        assert_realizes(keys)
    }
}

#[cfg(test)]
mod warm_split {
    use super::*;
    use std::time::Instant;

    /// Times splitting every node of the archive file at
    /// `DIALOG_ARCHIVE_PROFILE` into rows against the file's own id table,
    /// full, as an append's split runs: most tokens are found, not added.
    /// Run by hand.
    #[test]
    #[ignore = "profiles a file named by DIALOG_ARCHIVE_PROFILE"]
    fn warm_split() -> anyhow::Result<()> {
        let Ok(path) = std::env::var("DIALOG_ARCHIVE_PROFILE") else {
            return Ok(());
        };
        let archive = crate::FileArchive::open(&path)?;
        let mut blocks = Vec::new();
        for (catalog, hash) in archive.hashes() {
            blocks.push(Buffer::from(archive.get(&catalog, &hash)?.expect("listed")));
        }
        let nodes: Vec<_> = blocks.iter().filter_map(tree_node).collect();
        for round in 0..3 {
            let started = Instant::now();
            let mut new = 0;
            for node in &nodes {
                let mut draft = archive.draft();
                rows_of(node, &mut draft, |_| None)?;
                new += draft.pending();
            }
            println!(
                "round {round}: {} nodes, {:.1} µs a node, {new} tokens new",
                nodes.len(),
                started.elapsed().as_secs_f64() * 1e6 / nodes.len() as f64
            );
        }
        Ok(())
    }
}
