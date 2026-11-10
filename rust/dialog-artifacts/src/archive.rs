//! What a batch of edits reads and writes, by lane.
//!
//! An artifact tree touches two kinds of content-addressed bytes: the tree's
//! own nodes (blocks) and the values too large to live in a key (blobs,
//! spilled values). They share an address space, a BLAKE3 hash, but not a
//! meaning, so they never share a path either. A tree's nodes are blocks of
//! its catalog, read through [`archive::Load`]; spilled values are blobs of
//! the catalog's archive, read through [`blob::Load`] ([`load_spill`]). A
//! batch stages what it writes in an [`ArchiveDelta`] that keeps the two
//! lanes apart, and its commit writes each lane where that lane belongs.
use dialog_capability::{Capability, Provider, Resume};
use dialog_common::{Blake3Hash, Buffer, ConditionalSync, Held, Holds};
use dialog_effects::archive::prelude::ArchiveScope;
use dialog_effects::archive::{self, ArchiveError, Get};
use dialog_effects::blob;
use dialog_search_tree::nodes::{Preload, Read, Write};
use dialog_search_tree::{Delta, DialogSearchTreeError};

use crate::tree::CorpusNode;

use crate::DialogArtifactsError;

/// The spilled value stored under `hash` in `archive`'s blobs, loaded
/// through `env` and checked: `None` when `env` can reach no such value.
///
/// It performs [`blob::Load`], so an environment that knows where the
/// archive's subject keeps its blobs besides the local store hydrates a
/// local miss. Bytes that do not hash to `hash` fail the read rather than
/// reading as absent.
pub async fn load_spill<Env>(
    env: &Env,
    archive: &ArchiveScope,
    hash: &Blake3Hash,
) -> Result<Option<Buffer>, DialogArtifactsError>
where
    Env: Provider<blob::Load> + ConditionalSync + ?Sized,
{
    match blob::Load::new(archive.clone(), hash.clone())
        .perform(env)
        .await?
    {
        Some(blob) if blob.blake3_hash() != hash => Err(DialogArtifactsError::InvalidValue(
            format!("spilled value {hash} does not hash to its reference"),
        )),
        loaded => Ok(loaded),
    }
}

/// What a batch of edits stages for its commit: the tree nodes it persisted
/// and the spilled values it wrote (blobs), each under its content hash.
///
/// Clones share both lanes, so a reader over a clone
/// ([`DeltaOverlay`]) sees what the batch stages after the clone was taken:
/// a batch reads the values it spilled itself.
#[derive(Clone, Debug)]
pub struct ArchiveDelta {
    nodes: Delta<Blake3Hash, CorpusNode>,
    blobs: Delta<Blake3Hash, Buffer>,
}

impl Default for ArchiveDelta {
    fn default() -> Self {
        Self::zero()
    }
}

impl ArchiveDelta {
    /// Nothing staged.
    pub fn zero() -> Self {
        Self {
            nodes: Delta::zero(),
            blobs: Delta::zero(),
        }
    }

    /// A copy of what is staged that no longer shares it.
    pub fn branch(&self) -> Self {
        Self {
            nodes: self.nodes.branch(),
            blobs: self.blobs.branch(),
        }
    }

    /// The nodes lane: where a tree persists its new nodes.
    pub fn nodes(&mut self) -> &mut Delta<Blake3Hash, CorpusNode> {
        &mut self.nodes
    }

    /// Stages a spilled value, returning the hash its key carries.
    pub fn stage_blob(&mut self, blob: Buffer) -> Blake3Hash {
        let hash = blob.blake3_hash().clone();
        self.blobs.add(hash.clone(), blob);
        hash
    }

    /// The staged node under `hash`, if any.
    pub fn node(&self, hash: &Blake3Hash) -> Option<CorpusNode> {
        self.nodes.get(hash)
    }

    /// The staged node under `hash` as its block, if any.
    pub fn block(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.nodes
            .get(hash)
            .and_then(|node| node.buffer().ok().cloned())
    }

    /// The staged spilled value under `hash`, if any.
    pub fn blob(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.blobs.get(hash)
    }

    /// Takes every staged node, emptying the nodes lane.
    pub fn flush_nodes(&mut self) -> Vec<CorpusNode> {
        self.nodes.flush().map(|(_, node)| node).collect()
    }

    /// Takes every staged node as its block, emptying the nodes lane: for a
    /// caller that ships blocks.
    pub fn flush_blocks(&mut self) -> impl Iterator<Item = Buffer> + use<> {
        self.nodes
            .flush()
            .map(|(_, node)| {
                // A staged node is one a persist sealed, from its block.
                node.buffer()
                    .expect("a staged node is read from its block")
                    .clone()
            })
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Takes every staged spilled value, emptying the blobs lane.
    pub fn flush_blobs(&mut self) -> impl Iterator<Item = Buffer> + use<> {
        self.blobs
            .flush()
            .map(|(_, blob)| blob)
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Whether nothing is staged in either lane.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.blobs.is_empty()
    }
}

/// An environment that reads an [`ArchiveDelta`]'s staged lanes before the
/// environment it is over: a node or spilled value the batch staged is
/// served from its lane, and everything else from the environment.
///
/// This is what lets a batch keep editing a tree it persisted but has not
/// flushed, and read the values it spilled, before its commit writes them:
/// a node a persist sealed is the environment's only once the commit
/// writes it ([`Write`]), and until then the staged node is what a read
/// of it is answered with, with everything the seal derived still on it.
#[derive(Clone)]
pub struct DeltaOverlay<'a, Env: ?Sized> {
    delta: ArchiveDelta,
    env: &'a Env,
}

impl<'a, Env: ?Sized> DeltaOverlay<'a, Env> {
    /// Reads `delta`'s staged lanes before `env`. The overlay shares the
    /// delta, so it also sees what is staged after it was made.
    pub fn new(delta: &ArchiveDelta, env: &'a Env) -> Self {
        Self {
            delta: delta.clone(),
            env,
        }
    }

    /// The environment underneath.
    pub fn env(&self) -> &'a Env {
        self.env
    }
}

impl<Env: Holds + ?Sized> Holds for DeltaOverlay<'_, Env> {
    fn held(&self, key: &str) -> Option<Held> {
        self.env.held(key)
    }

    fn hold(&self, key: String, handle: Held) {
        self.env.hold(key, handle)
    }

    fn held_or(&self, key: &str, make: &dyn Fn() -> Held) -> Held {
        self.env.held_or(key, make)
    }
}

#[dialog_capability::provider]
impl<Env> Provider<archive::Load> for DeltaOverlay<'_, Env>
where
    Env: Provider<archive::Load> + ConditionalSync + ?Sized,
{
    async fn execute(&self, load: archive::Load) -> Result<Option<Buffer>, ArchiveError> {
        if let Some(block) = self.delta.block(&load.digest) {
            return Ok(Some(block));
        }
        load.perform(self.env).await
    }
}

/// A staged node answers its read at once, as the node the seal produced;
/// any other read is the environment's.
impl<Env> Provider<Read<CorpusNode>> for DeltaOverlay<'_, Env>
where
    Env: Provider<Read<CorpusNode>> + ?Sized,
{
    fn execute(
        &self,
        read: Read<CorpusNode>,
    ) -> Resume<'_, Result<Option<CorpusNode>, DialogSearchTreeError>> {
        if let Some(node) = self.delta.node(&read.name) {
            return Resume::now(Ok(Some(node)));
        }
        self.env.execute(read)
    }
}

/// A staged node is already as cheap to read as it gets; any other
/// preload is the environment's.
impl<Env> Provider<Preload<CorpusNode>> for DeltaOverlay<'_, Env>
where
    Env: Provider<Preload<CorpusNode>> + ?Sized,
{
    fn execute(&self, preload: Preload<CorpusNode>) -> Resume<'_, ()> {
        if self.delta.node(&preload.name).is_some() {
            return Resume::now(());
        }
        self.env.execute(preload)
    }
}

/// A write through the overlay is the environment's: nothing is staged by
/// writing.
impl<Env> Provider<Write<CorpusNode>> for DeltaOverlay<'_, Env>
where
    Env: Provider<Write<CorpusNode>> + ?Sized,
{
    fn execute(&self, write: Write<CorpusNode>) -> Resume<'_, Result<(), DialogSearchTreeError>> {
        write.perform(self.env)
    }
}

/// A staged block is one the replica minted, so a local-only read finds it
/// as it finds what the local archive holds.
#[dialog_capability::provider]
impl<Env> Provider<Get> for DeltaOverlay<'_, Env>
where
    Env: Provider<Get> + ConditionalSync + ?Sized,
{
    async fn execute(&self, get: Capability<Get>) -> Result<Option<Vec<u8>>, ArchiveError> {
        use dialog_effects::archive::prelude::GetExt as _;
        if let Some(block) = self.delta.block(get.digest()) {
            return Ok(Some(block.as_ref().to_vec()));
        }
        get.perform(self.env).await
    }
}

#[dialog_capability::provider]
impl<Env> Provider<blob::Load> for DeltaOverlay<'_, Env>
where
    Env: Provider<blob::Load> + ConditionalSync + ?Sized,
{
    async fn execute(&self, load: blob::Load) -> Result<Option<Buffer>, ArchiveError> {
        if let Some(blob) = self.delta.blob(&load.digest) {
            return Ok(Some(blob));
        }
        load.perform(self.env).await
    }
}

#[cfg(any(test, feature = "helpers"))]
use dialog_search_tree::helpers::Blocks;

#[cfg(any(test, feature = "helpers"))]
impl ArchiveDelta {
    /// Keeps every staged node and spilled value in `blocks`, emptying both
    /// lanes: an in-memory stand-in for a commit. A spilled value is kept
    /// as a block of the catalog, where a spilled-value read finds it.
    pub fn flush_into(&mut self, blocks: &Blocks) {
        for block in self.flush_blocks().chain(self.flush_blobs()) {
            blocks.store(block);
        }
    }
}
