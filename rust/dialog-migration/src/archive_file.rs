//! The `archive-file` migration: a block store into a [`FileArchive`].
//!
//! A replica kept a block per file (the `fs` provider's archive, or a
//! directory of blocks fetched from a remote) moves into the single file
//! with [`transcode`]: every block of every catalog, written so the file
//! keeps tree nodes as compactly as it can. A whole replica (every space
//! of a profile directory) moves with [`transcode_spaces`].
//!
//! How compact that is depends on the order blocks arrive in: a node is
//! written against the node it supersedes, so the versions of a node should
//! arrive oldest first, as the commits that wrote them did. A block store
//! keeps no order, so the transcode reconstructs one. It reads each block
//! once to learn the trees' shape (which nodes link to which, and how many
//! history records each subtree holds), takes the nodes nothing links to as
//! the roots commits left, and orders them by how much history they hold:
//! the history region only grows, so a root with fewer records was written
//! earlier. It then writes one append per root, of the nodes no earlier root
//! reached, children before parents, as the commit wrote them. Blocks that
//! are not tree nodes (spilled values, say) follow.
//!
//! Nothing is dropped: a node whose frame does not rebuild its exact block
//! (one in an older layout) is kept whole, as is anything that is not an
//! artifact tree node. Every block reads back from the file as the bytes
//! the source held.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use base58::{FromBase58 as _, ToBase58 as _};
use dialog_archive_file::{FILE_NAME, FileArchive};
use dialog_common::{Blake3Hash, Buffer};
use dialog_corpus::{Datum, HISTORY_KEY_TAG, Key, State};
use dialog_search_tree::{NodeBody, NoveltyOp, PersistentNode};

/// A node of the artifact tree.
type TreeNode = PersistentNode<Key, State<Datum>>;

/// `block` read as an artifact tree node, if it is one.
fn tree_node(block: &Buffer) -> Option<TreeNode> {
    TreeNode::try_from(block.clone()).ok()
}

/// Where a transcode reads blocks from.
pub trait BlockSource {
    /// The catalogs the source holds blocks in.
    fn catalogs(&self) -> io::Result<Vec<String>>;

    /// Every block `catalog` holds, by hash.
    fn hashes(&self, catalog: &str) -> io::Result<Vec<Blake3Hash>>;

    /// The block `hash` names in `catalog`, if the source holds it.
    fn read(&self, catalog: &str, hash: &Blake3Hash) -> io::Result<Option<Buffer>>;
}

/// A directory of blocks, a file per block named by the base58 of its hash:
/// either one catalog's blocks ([`Directory::catalog`]) or a directory per
/// catalog, as the `fs` provider keeps a space's archive
/// (`{space}/archive/{catalog}/{hash}`, [`Directory::space`]).
#[derive(Clone, Debug)]
pub struct Directory {
    root: PathBuf,
    /// The one catalog a flat directory holds; `None` for a directory per
    /// catalog.
    flat: Option<String>,
}

impl Directory {
    /// The archive of the space at `space`, as the `fs` provider keeps it:
    /// `{space}/archive/{catalog}/{hash}`. `space` may also name the
    /// `archive` directory itself.
    pub fn space(space: impl AsRef<Path>) -> Self {
        let space = space.as_ref();
        let archive = space.join("archive");
        Self {
            root: if archive.is_dir() {
                archive
            } else {
                space.to_path_buf()
            },
            flat: None,
        }
    }

    /// The blocks of one catalog, every file in `directory` a block.
    pub fn catalog(directory: impl AsRef<Path>, catalog: impl Into<String>) -> Self {
        Self {
            root: directory.as_ref().to_path_buf(),
            flat: Some(catalog.into()),
        }
    }

    /// The directory `catalog`'s blocks are in.
    pub fn directory(&self, catalog: &str) -> PathBuf {
        match &self.flat {
            Some(_) => self.root.clone(),
            None => self.root.join(catalog),
        }
    }
}

impl BlockSource for Directory {
    fn catalogs(&self) -> io::Result<Vec<String>> {
        if let Some(catalog) = &self.flat {
            return Ok(vec![catalog.clone()]);
        }
        let mut catalogs = Vec::new();
        for entry in std::fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && let Some(name) = entry.file_name().to_str()
            {
                catalogs.push(name.to_string());
            }
        }
        catalogs.sort();
        Ok(catalogs)
    }

    fn hashes(&self, catalog: &str) -> io::Result<Vec<Blake3Hash>> {
        let mut hashes = Vec::new();
        for entry in std::fs::read_dir(self.directory(catalog))? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            // Anything not named by a hash (a temporary file, say) is not a
            // block.
            let Some(bytes) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.from_base58().ok())
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            else {
                continue;
            };
            hashes.push(Blake3Hash::from(bytes));
        }
        hashes.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        Ok(hashes)
    }

    fn read(&self, catalog: &str, hash: &Blake3Hash) -> io::Result<Option<Buffer>> {
        let path = self.directory(catalog).join(hash.as_bytes().to_base58());
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(Buffer::from(bytes))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }
}

/// What a transcode wrote for one catalog.
#[derive(Clone, Debug, Default)]
pub struct CatalogReport {
    /// The catalog.
    pub catalog: String,
    /// Tree nodes kept as frames, and their bytes in the source.
    pub frames: usize,
    /// Their bytes as blocks.
    pub frame_source_bytes: u64,
    /// Tree nodes kept whole (a frame would not rebuild their block), and
    /// their bytes.
    pub whole_nodes: usize,
    /// Their bytes.
    pub whole_node_bytes: u64,
    /// Blocks that are not tree nodes, kept whole, and their bytes.
    pub others: usize,
    /// Their bytes.
    pub other_bytes: u64,
    /// The roots the order was reconstructed from: nodes nothing links to.
    pub roots: usize,
    /// Files whose bytes did not hash to their name, left out.
    pub corrupt: Vec<Blake3Hash>,
}

/// What a transcode wrote.
#[derive(Clone, Debug, Default)]
pub struct Report {
    /// Per catalog.
    pub catalogs: Vec<CatalogReport>,
}

/// How a transcode orders and batches what it writes.
#[derive(Clone, Debug)]
pub struct Transcode {
    /// Write roots by how much history they hold (the order commits wrote
    /// them in), rather than by hash.
    pub history_order: bool,
    /// The most bytes one append carries: a root that reaches more new
    /// nodes than this is written in several appends, children first still.
    pub append_bytes: usize,
}

impl Default for Transcode {
    fn default() -> Self {
        Self {
            history_order: true,
            append_bytes: 64 * 1024 * 1024,
        }
    }
}

/// What the first pass keeps of a tree node: its children and how many
/// history records it holds itself (leaf entries, or buffered assertions).
struct Shape {
    children: Vec<Blake3Hash>,
    history: u64,
}

/// Transcodes every block of `source` into `target`, as [`Transcode`] says
/// by default. See the [module](self).
pub fn transcode(source: &impl BlockSource, target: &FileArchive) -> io::Result<Report> {
    Transcode::default().run(source, target)
}

/// Where a space's archive is being written while it migrates: renamed to
/// [`FILE_NAME`] once every block is in it, so a space never has a partial
/// `archive.dialog`.
pub const MIGRATING: &str = "archive.dialog.migrating";

/// Every space under `root`: each directory holding an `fs` archive,
/// `{space}/archive/{catalog}/{hash}`. A profile directory holds several
/// (the peer's own state, and a space per repository).
pub fn spaces(root: impl AsRef<Path>) -> io::Result<Vec<PathBuf>> {
    let mut spaces = Vec::new();
    let mut stack = vec![root.as_ref().to_path_buf()];
    while let Some(directory) = stack.pop() {
        if directory.join("archive").is_dir() {
            spaces.push(directory.clone());
        }
        for entry in std::fs::read_dir(&directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() && entry.file_name() != "archive" {
                stack.push(entry.path());
            }
        }
    }
    spaces.sort();
    Ok(spaces)
}

/// The spaces under `root` that have not moved to the file yet: an `fs`
/// archive and no [`FILE_NAME`] beside it. A space that has one was
/// migrated, or was written by a file-archive space all along.
pub fn unmigrated(root: impl AsRef<Path>) -> io::Result<Vec<PathBuf>> {
    Ok(spaces(root)?
        .into_iter()
        .filter(|space| !space.join(FILE_NAME).exists())
        .collect())
}

/// How many blocks a space's `fs` archive holds, and their bytes: what
/// migrating it reads.
pub fn size(space: impl AsRef<Path>) -> io::Result<(u64, u64)> {
    let source = Directory::space(space);
    let (mut blocks, mut bytes) = (0, 0);
    for catalog in source.catalogs()? {
        for entry in std::fs::read_dir(source.directory(&catalog))? {
            let metadata = entry?.metadata()?;
            if metadata.is_file() {
                blocks += 1;
                bytes += metadata.len();
            }
        }
    }
    Ok((blocks, bytes))
}

/// Moves one space into the file a file-archive space reads: transcodes its
/// `fs` archive into [`MIGRATING`], makes that durable, then renames it to
/// [`FILE_NAME`]. Interrupted, it resumes: the blocks already in
/// [`MIGRATING`] are not written again. With `remove_source`, the `fs`
/// archive is deleted once the file is in place; otherwise it is left, and
/// a file-archive space never reads it.
pub fn migrate_space(space: impl AsRef<Path>, remove_source: bool) -> io::Result<Report> {
    let space = space.as_ref();
    let migrating = space.join(MIGRATING);
    let report = {
        let archive =
            FileArchive::open(&migrating).map_err(|error| io::Error::other(error.to_string()))?;
        let report = transcode(&Directory::space(space), &archive)?;
        archive
            .sync()
            .map_err(|error| io::Error::other(error.to_string()))?;
        report
    };
    std::fs::rename(&migrating, space.join(FILE_NAME))?;
    if remove_source {
        std::fs::remove_dir_all(space.join("archive"))?;
    }
    Ok(report)
}

/// Moves every space under `root` that has not moved yet (see
/// [`unmigrated`]) into the file, each with [`migrate_space`]. The `fs`
/// archives are left where they are.
pub fn transcode_spaces(root: impl AsRef<Path>) -> io::Result<Vec<(PathBuf, Report)>> {
    let mut reports = Vec::new();
    for space in unmigrated(root)? {
        let report = migrate_space(&space, false)?;
        reports.push((space, report));
    }
    Ok(reports)
}

/// The `archive-file` step: every space of a replica, moved into the
/// single-file archive (see [`migrate_space`]).
///
/// It is a conversion, not an upgrade: a replica on the `fs` provider is
/// not out of date, and a space reads the file only when it is opened as a
/// file-archive space. An application that switches its spaces to
/// [`FileArchive`] runs this step before it opens them.
#[derive(Clone, Debug, Default)]
pub struct ArchiveFile {
    /// Delete each space's `fs` archive once its file is in place.
    pub remove_source: bool,
}

impl crate::Step for ArchiveFile {
    fn id(&self) -> &'static str {
        "archive-file"
    }

    fn summary(&self) -> &'static str {
        "move each space's blocks from a file per block into one archive file"
    }

    fn pending(&self, replica: &Path) -> io::Result<Vec<crate::Pending>> {
        unmigrated(replica)?
            .into_iter()
            .map(|space| {
                let (blocks, bytes) = size(&space)?;
                Ok(crate::Pending {
                    step: self.id(),
                    detail: format!(
                        "{blocks} blocks ({:.1} MiB) into {}",
                        bytes as f64 / (1024.0 * 1024.0),
                        space.join(FILE_NAME).display()
                    ),
                    at: space,
                })
            })
            .collect()
    }

    fn apply(&self, pending: &crate::Pending) -> io::Result<crate::Applied> {
        let report = migrate_space(&pending.at, self.remove_source)?;
        let (frames, whole, others) = report.catalogs.iter().fold((0, 0, 0), |sum, catalog| {
            (
                sum.0 + catalog.frames,
                sum.1 + catalog.whole_nodes,
                sum.2 + catalog.others,
            )
        });
        Ok(crate::Applied {
            step: self.id(),
            at: pending.at.clone(),
            detail: format!(
                "{frames} tree nodes as frames, {whole} kept whole, {others} other blocks"
            ),
        })
    }
}

impl Transcode {
    /// Transcodes every block of `source` into `target`.
    pub fn run(&self, source: &impl BlockSource, target: &FileArchive) -> io::Result<Report> {
        let mut report = Report::default();
        for catalog in source.catalogs()? {
            report
                .catalogs
                .push(self.catalog(source, target, &catalog)?);
        }
        Ok(report)
    }

    fn catalog(
        &self,
        source: &impl BlockSource,
        target: &FileArchive,
        catalog: &str,
    ) -> io::Result<CatalogReport> {
        let mut report = CatalogReport {
            catalog: catalog.to_string(),
            ..CatalogReport::default()
        };

        // First pass: the shape of the trees, and which blocks are not
        // tree nodes.
        let mut shapes: HashMap<Blake3Hash, Shape> = HashMap::new();
        let mut others: Vec<Blake3Hash> = Vec::new();
        for hash in source.hashes(catalog)? {
            let Some(block) = source.read(catalog, &hash)? else {
                continue;
            };
            if block.blake3_hash() != &hash {
                report.corrupt.push(hash);
                continue;
            }
            match tree_node(&block) {
                Some(node) => {
                    shapes.insert(hash, shape(&node)?);
                }
                None => others.push(hash),
            }
        }

        // The roots, in the order the commits that left them were written.
        let linked: HashSet<&Blake3Hash> = shapes
            .values()
            .flat_map(|shape| shape.children.iter())
            .collect();
        let mut roots: Vec<Blake3Hash> = shapes
            .keys()
            .filter(|hash| !linked.contains(hash))
            .cloned()
            .collect();
        report.roots = roots.len();
        if self.history_order {
            let history = history(&roots, &shapes);
            roots.sort_by(|a, b| {
                history[a]
                    .cmp(&history[b])
                    .then_with(|| a.as_bytes().cmp(b.as_bytes()))
            });
        } else {
            roots.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        }

        // Second pass: each root's new nodes, children first.
        let mut written: HashSet<Blake3Hash> = HashSet::new();
        let mut nodes: Vec<(Blake3Hash, u64)> = Vec::new();
        let mut batch = Batch::new(self.append_bytes);
        for root in &roots {
            let mut stack = vec![(root.clone(), false)];
            while let Some((hash, expanded)) = stack.pop() {
                if written.contains(&hash) {
                    continue;
                }
                let Some(shape) = shapes.get(&hash) else {
                    // A child the source does not hold.
                    continue;
                };
                if !expanded {
                    stack.push((hash.clone(), true));
                    for child in shape.children.iter().rev() {
                        stack.push((child.clone(), false));
                    }
                    continue;
                }
                written.insert(hash.clone());
                if let Some(block) = source.read(catalog, &hash)? {
                    nodes.push((hash.clone(), block.as_ref().len() as u64));
                    batch.add(target, catalog, block)?;
                }
            }
            // One append per commit, as the commit wrote it.
            batch.flush(target, catalog)?;
        }
        for hash in &others {
            if let Some(block) = source.read(catalog, hash)? {
                report.others += 1;
                report.other_bytes += block.as_ref().len() as u64;
                batch.add(target, catalog, block)?;
            }
        }
        batch.flush(target, catalog)?;

        for (hash, len) in nodes {
            if target.is_frame(catalog, &hash) {
                report.frames += 1;
                report.frame_source_bytes += len;
            } else {
                report.whole_nodes += 1;
                report.whole_node_bytes += len;
            }
        }
        Ok(report)
    }
}

/// Blocks waiting to be appended together.
struct Batch {
    limit: usize,
    blocks: Vec<Buffer>,
    bytes: usize,
}

impl Batch {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            blocks: Vec::new(),
            bytes: 0,
        }
    }

    fn add(&mut self, target: &FileArchive, catalog: &str, block: Buffer) -> io::Result<()> {
        self.bytes += block.as_ref().len();
        self.blocks.push(block);
        if self.bytes >= self.limit {
            self.flush(target, catalog)?;
        }
        Ok(())
    }

    fn flush(&mut self, target: &FileArchive, catalog: &str) -> io::Result<()> {
        if self.blocks.is_empty() {
            return Ok(());
        }
        target
            .append(catalog, &self.blocks)
            .map_err(|error| io::Error::other(error.to_string()))?;
        self.blocks.clear();
        self.bytes = 0;
        Ok(())
    }
}

fn tree_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

/// A node's children, and the history records it holds itself.
fn shape(node: &TreeNode) -> io::Result<Shape> {
    Ok(match node.body() {
        NodeBody::Segment(segment) => {
            let mut history = 0;
            let mut keys = segment.keys::<Key>().map_err(tree_error)?;
            while let Some((_, key)) = keys.next_key().map_err(tree_error)? {
                history += (key.first() == Some(&HISTORY_KEY_TAG)) as u64;
            }
            Shape {
                children: Vec::new(),
                history,
            }
        }
        NodeBody::Index(index) => {
            let mut history = 0;
            for buffer in index.buffers() {
                for item in buffer.entries::<Key>().map_err(tree_error)? {
                    history += (matches!(item.op, NoveltyOp::Assert(_))
                        && item.key.first() == Some(&HISTORY_KEY_TAG))
                        as u64;
                }
            }
            Shape {
                children: (0..index.len())
                    .map(|at| index.hash_at(at).cloned())
                    .collect::<Result<_, _>>()
                    .map_err(tree_error)?,
                history,
            }
        }
    })
}

/// How many history records each root's tree holds.
fn history(roots: &[Blake3Hash], shapes: &HashMap<Blake3Hash, Shape>) -> HashMap<Blake3Hash, u64> {
    let mut totals: HashMap<Blake3Hash, u64> = HashMap::new();
    for root in roots {
        let mut stack = vec![(root.clone(), false)];
        while let Some((hash, expanded)) = stack.pop() {
            if totals.contains_key(&hash) {
                continue;
            }
            let Some(shape) = shapes.get(&hash) else {
                totals.insert(hash, 0);
                continue;
            };
            if expanded || shape.children.is_empty() {
                let below: u64 = shape
                    .children
                    .iter()
                    .map(|child| totals.get(child).copied().unwrap_or(0))
                    .sum();
                totals.insert(hash, shape.history + below);
            } else {
                stack.push((hash.clone(), true));
                for child in &shape.children {
                    stack.push((child.clone(), false));
                }
            }
        }
    }
    totals
}
