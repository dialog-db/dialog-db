//! The archive answers a tree's node reads: a node read where the file keeps it.
//!
//! A frame's rows name tokens by id; a node served to the tree turns them,
//! once, into where each token sits in the file, and reads every key part,
//! value and child hash there, through the file's mapping. Nothing a node
//! holds is copied out of the file or deserialized: a value is read as its
//! archived type in place (tokens are aligned for it, and each is checked
//! once, the first time a node that holds it is served), and a key is
//! compared part by part.

use std::any::TypeId;
use std::ops::Range;
use std::sync::Arc;

use dialog_artifacts::{
    ATTRIBUTE_KEY_TAG, BLOB_KEY_TAG, COVERAGE_KEY_TAG, Datum, ENTITY_KEY_TAG, HISTORY_KEY_TAG,
    State, VALUE_KEY_TAG,
};
use dialog_capability::{Provider, Resume};
use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::nodes::{Node, Preload, Read, Write, decode};
use dialog_search_tree::{
    DialogSearchTreeError, KeyParts, Manifest, NodeRows, PersistentNode, RowsKind,
};
use memmap2::Mmap;
use rkyv::{
    bytecheck::CheckBytes,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};
use zerocopy::FromBytes as _;

use super::index::{Place, Snapshot};
use super::{DATUMS_CHECKED, FileArchive};
use crate::rows::{DATUM, INDEX, NONE, Row, Rows, SLOTS};
use crate::tokens::{ALIGN, TokenRead as _};

/// Every byte value, so a key's form byte is a slice that lives forever.
static FORMS: [u8; 256] = {
    let mut forms = [0u8; 256];
    let mut at = 0;
    while at < 256 {
        forms[at] = at as u8;
        at += 1;
    }
    forms
};

/// Where some bytes sit in the file.
#[derive(Clone, Copy, Default)]
struct Part {
    offset: u64,
    len: u32,
}

/// No part: a slot a key's form does not use.
const NO_PART: Part = Part { offset: 0, len: 0 };

/// An entry of a segment, or an operation an index buffers: its key's parts
/// in key order and its value, each where the file keeps it.
#[derive(Clone, Copy)]
struct Entry {
    /// The key's form byte, or none for a key kept whole.
    form: Option<u8>,
    count: u8,
    parts: [Part; SLOTS],
    /// The archived value, or none for a retraction.
    value: Option<Part>,
}

/// A link of an index: its separator, where its child's hash is, its scale.
#[derive(Clone, Copy)]
struct Link {
    separator: Part,
    child: u64,
    scale: u8,
}

/// A node the archive keeps as a frame, read through the file's mapping.
pub(crate) struct FileNode {
    archive: FileArchive,
    map: Arc<Mmap>,
    catalog: String,
    hash: Blake3Hash,
    manifest: Arc<Manifest>,
    kind: RowsKind,
    /// A segment's entries, or an index's buffered operations.
    entries: Vec<Entry>,
    links: Vec<Link>,
    buffers: Vec<(u32, Range<usize>)>,
}

impl FileNode {
    fn bytes(&self, part: Part) -> &[u8] {
        let start = part.offset as usize;
        &self.map[start..start + part.len as usize]
    }
}

/// Whether `bytes` are a valid archive of a `State<Datum>`.
fn is_datum(bytes: &[u8]) -> bool {
    rkyv::access::<rkyv::Archived<State<Datum>>, rkyv::rancor::Error>(bytes).is_ok()
}

/// Where the token rows name `id` is: its bytes start at `id` times the
/// alignment, and their length is in the four bytes before.
fn part(snapshot: &Snapshot, id: u32) -> Option<Part> {
    let token = snapshot.get(id)?;
    Some(Part {
        offset: id as u64 * ALIGN,
        len: token.len() as u32,
    })
}

/// An entry or operation row as where its key parts and value are. A
/// datum is checked as an archive once, the first time a node that holds
/// it is served.
fn entry(archive: &FileArchive, snapshot: &Snapshot, row: &Row) -> Option<Entry> {
    let form = row[0] as u8;
    let slot = |at: usize| part(snapshot, row[at]);
    let (form, count, parts) = match form {
        ENTITY_KEY_TAG => (Some(form), 3, [slot(1)?, slot(2)?, slot(3)?, NO_PART]),
        ATTRIBUTE_KEY_TAG => (Some(form), 3, [slot(2)?, slot(1)?, slot(3)?, NO_PART]),
        VALUE_KEY_TAG => (Some(form), 3, [slot(3)?, slot(2)?, slot(1)?, NO_PART]),
        HISTORY_KEY_TAG | COVERAGE_KEY_TAG => {
            (Some(form), 4, [slot(1)?, slot(2)?, slot(3)?, slot(4)?])
        }
        BLOB_KEY_TAG => (Some(form), 1, [slot(1)?, NO_PART, NO_PART, NO_PART]),
        _ => (None, 1, [slot(1)?, NO_PART, NO_PART, NO_PART]),
    };
    let value = match row[DATUM] {
        NONE => None,
        id => {
            let checked = archive.inner.checked.pin();
            if !checked.contains(&id) {
                if !is_datum(snapshot.get(id)?) {
                    return None;
                }
                if checked.len() >= DATUMS_CHECKED {
                    checked.clear();
                }
                checked.insert(id);
            }
            Some(part(snapshot, id)?)
        }
    };
    Some(Entry {
        form,
        count,
        parts,
        value,
    })
}

impl FileArchive {
    /// The node `hash` names in `catalog`, as the archive serves it to a
    /// tree: read in place. `None` when the archive keeps it whole, or not
    /// at all.
    pub(crate) fn served(&self, catalog: &str, hash: &Blake3Hash) -> Option<FileNode> {
        let started = std::time::Instant::now();
        let (snapshot, Some(Place::Frame(at))) = self.place(catalog, hash).ok()? else {
            return None;
        };
        let (rows, manifest) = self
            .rows(&snapshot, &std::collections::HashMap::new(), at)
            .ok()?;
        let manifest = self.manifest(&snapshot, manifest)?;
        let mut node = FileNode {
            archive: self.clone(),
            map: snapshot.map.clone(),
            catalog: catalog.to_string(),
            hash: hash.clone(),
            manifest,
            kind: if rows.kind == INDEX {
                RowsKind::Index
            } else {
                RowsKind::Segment
            },
            entries: Vec::new(),
            links: Vec::new(),
            buffers: Vec::new(),
        };
        build(self, &snapshot, &rows, &mut node)?;
        super::Timings::add(&self.inner.timings.serve, started);
        self.inner
            .timings
            .served
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some(node)
    }

    /// The manifest token `id` decodes to, decoded once.
    fn manifest(&self, snapshot: &Snapshot, id: u32) -> Option<Arc<Manifest>> {
        let manifests = self.inner.manifests.pin();
        if let Some(manifest) = manifests.get(&id) {
            return Some(manifest.clone());
        }
        let manifest = Arc::new(Manifest::decode(snapshot.get(id)?).ok()?);
        Some(manifests.get_or_insert(id, manifest).clone())
    }
}

/// Fills `node` with where each of its rows' parts is.
fn build(
    archive: &FileArchive,
    snapshot: &Snapshot,
    rows: &Rows,
    node: &mut FileNode,
) -> Option<()> {
    if rows.kind != INDEX {
        node.entries = rows
            .sections
            .first()?
            .iter()
            .map(|row| entry(archive, snapshot, row))
            .collect::<Option<_>>()?;
        return Some(());
    }
    let [links, buffers, ops] = rows.sections.as_slice() else {
        return None;
    };
    node.links = links
        .iter()
        .map(|row| {
            let child = match row[1] {
                NONE => {
                    let hash = part(snapshot, row[3])?;
                    (hash.len == 32).then_some(hash.offset)?
                }
                // A frame's data starts with the node's hash.
                at => {
                    snapshot.frame_hash(at)?;
                    at as u64 * ALIGN
                }
            };
            Some(Link {
                separator: part(snapshot, row[0])?,
                child,
                scale: row[2] as u8,
            })
        })
        .collect::<Option<_>>()?;
    node.entries = ops
        .iter()
        .map(|row| entry(archive, snapshot, row))
        .collect::<Option<_>>()?;
    let mut next = 0;
    for buffer in buffers {
        let len = buffer[1] as usize;
        if next + len > node.entries.len() {
            return None;
        }
        node.buffers.push((buffer[0], next..next + len));
        next += len;
    }
    Some(())
}

// SAFETY: every value a `FileNode` hands out is a token the archive checked
// as an archive of a `State<Datum>` (`is_datum`) before it served the node,
// read through a mapping the node holds, at a file offset aligned to 16
// (every token is), and the mapping is page aligned. The file is
// append-only: nothing indexed is ever written again (see `Tokens::map`). A
// frame was written only once its rows rebuilt the very block its hash
// names.
unsafe impl NodeRows for FileNode {
    fn hash(&self) -> &Blake3Hash {
        &self.hash
    }

    fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    fn values(&self) -> TypeId {
        TypeId::of::<State<Datum>>()
    }

    fn kind(&self) -> RowsKind {
        self.kind
    }

    fn len(&self) -> usize {
        match self.kind {
            RowsKind::Segment => self.entries.len(),
            RowsKind::Index => self.links.len(),
        }
    }

    fn key(&self, at: usize) -> KeyParts<'_> {
        self.op_key(at)
    }

    fn value(&self, at: usize) -> &[u8] {
        self.op_value(at).expect("a segment entry has a value")
    }

    fn separator(&self, at: usize) -> &[u8] {
        self.bytes(self.links[at].separator)
    }

    fn child(&self, at: usize) -> &Blake3Hash {
        let start = self.links[at].child as usize;
        Blake3Hash::ref_from_bytes(&self.map[start..start + 32]).expect("32 bytes are a hash")
    }

    fn scale(&self, at: usize) -> u8 {
        self.links[at].scale
    }

    fn buffers(&self) -> usize {
        self.buffers.len()
    }

    fn buffer(&self, which: usize) -> (u32, Range<usize>) {
        self.buffers[which].clone()
    }

    fn op_key(&self, at: usize) -> KeyParts<'_> {
        let entry = &self.entries[at];
        let mut parts: [&[u8]; 1 + SLOTS] = [&[]; 1 + SLOTS];
        let mut count = 0;
        if let Some(form) = entry.form {
            parts[0] = &FORMS[form as usize..form as usize + 1];
            count = 1;
        }
        for part in &entry.parts[..entry.count as usize] {
            parts[count] = self.bytes(*part);
            count += 1;
        }
        KeyParts::new(&parts[..count])
    }

    fn op_value(&self, at: usize) -> Option<&[u8]> {
        self.entries[at].value.map(|value| self.bytes(value))
    }

    fn footprint(&self) -> usize {
        size_of::<Self>()
            + self.entries.capacity() * size_of::<Entry>()
            + self.links.capacity() * size_of::<Link>()
            + self.buffers.capacity() * size_of::<(u32, Range<usize>)>()
            + self.catalog.capacity()
    }

    fn block(&self) -> Result<Buffer, DialogSearchTreeError> {
        let block = self
            .archive
            .get(&self.catalog, &self.hash)
            .map_err(|error| DialogSearchTreeError::Encoding(error.to_string()))?
            .ok_or_else(|| {
                DialogSearchTreeError::Node(format!("{} is not in the archive", self.hash))
            })?;
        Ok(Buffer::from(block))
    }
}

/// The archive answers a tree's node reads itself (the node layer,
/// [`dialog_search_tree::nodes`]): a node it keeps as a frame is read from
/// its rows in place, and a node kept whole is decoded from its block. The
/// file is mapped, so both are answered at once; nothing here waits. The
/// archive is this replica's store, so a read it cannot answer is absent
/// here, and hydrating it from elsewhere is for whoever stands in front.
impl<N> Provider<Read<N>> for FileArchive
where
    N: Node,
    <N::Value as rkyv::Archive>::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    fn execute(&self, read: Read<N>) -> Resume<'_, Result<Option<N>, DialogSearchTreeError>> {
        let catalog = read.catalog.catalog_name();
        // Rows of another value type than the tree's are read from the
        // block instead, as `PersistentNode::from_rows` declines them.
        if let Some(node) = self
            .served(catalog, &read.name)
            .and_then(|rows| PersistentNode::<N::Key, N::Value>::from_rows(Arc::new(rows)))
        {
            return Resume::now(Ok(Some(N::from_persistent(node))));
        }
        Resume::now(match self.get(catalog, &read.name) {
            Ok(block) => block
                .map(|block| decode::<N>(Buffer::from(block), &read.name))
                .transpose(),
            Err(error) => Err(DialogSearchTreeError::Storage(error.into())),
        })
    }
}

/// Reads from the file are as cheap as a held node: there is nothing to
/// make cheaper ahead of time.
impl<N> Provider<Preload<N>> for FileArchive {
    fn execute(&self, _: Preload<N>) -> Resume<'_, ()> {
        Resume::now(())
    }
}

/// A write appends the nodes' blocks, which the archive frames as rows.
impl<N: Node> Provider<Write<N>> for FileArchive {
    fn execute(&self, write: Write<N>) -> Resume<'_, Result<(), DialogSearchTreeError>> {
        let blocks = match write
            .nodes
            .iter()
            .map(|node| node.buffer().cloned())
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(blocks) => blocks,
            Err(error) => return Resume::now(Err(error)),
        };
        Resume::now(
            self.append(write.catalog.catalog_name(), &blocks)
                .map_err(|error| DialogSearchTreeError::Storage(error.into())),
        )
    }
}
