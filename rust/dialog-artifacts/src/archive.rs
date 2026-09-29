//! What a batch of edits reads and writes, by lane.
//!
//! An artifact tree touches two kinds of content-addressed bytes: the tree's
//! own nodes (blocks) and the values too large to live in a key (blobs,
//! spilled values). They share an address space, a BLAKE3 hash, but not a
//! meaning, so they never share a path either. Nodes load through the tree's
//! [`Load`]; spilled values load through [`LoadBlob`]. A batch stages what it
//! writes in an [`ArchiveDelta`] that keeps the two lanes apart, and its
//! commit writes each lane where that lane belongs.

use async_trait::async_trait;
use dialog_capability::{Command, Provider};
use dialog_common::{Blake3Hash, Buffer, ConditionalSync};
use dialog_search_tree::{Delta, DialogSearchTreeError, Load, MemoryBlocks};

use crate::DialogArtifactsError;
#[cfg(test)]
use dialog_search_tree::helpers::ObservingBlocks;

/// Command for loading a spilled value's bytes by the hash its key carries.
///
/// The spilled-value counterpart of the tree's [`Load`]: the same hash, a
/// different lane. `Ok(None)` means the provider cannot reach the blob.
/// Readers check what they load against the hash they asked for, so a
/// provider never has to be trusted to return the right bytes.
pub struct LoadBlob;

impl Command for LoadBlob {
    type Input = Blake3Hash;
    type Output = Result<Option<Buffer>, DialogArtifactsError>;
}

/// An environment an artifact tree can be read from: it loads nodes and
/// spilled values.
pub trait ArchiveReader: Provider<Load> + Provider<LoadBlob> + ConditionalSync {}

impl<Env> ArchiveReader for Env where Env: Provider<Load> + Provider<LoadBlob> + ConditionalSync {}

/// Loads the blob stored under `hash` through `env`, refusing bytes that do
/// not hash to it.
pub async fn load_blob<Env>(
    env: &Env,
    hash: &Blake3Hash,
) -> Result<Option<Buffer>, DialogArtifactsError>
where
    Env: Provider<LoadBlob> + ConditionalSync,
{
    match env.execute(hash.clone()).await? {
        Some(blob) if blob.blake3_hash() != hash => Err(DialogArtifactsError::InvalidValue(
            format!("spilled value {hash} does not hash to its reference"),
        )),
        loaded => Ok(loaded),
    }
}

/// What a batch of edits stages for its commit: the tree nodes it persisted
/// (blocks) and the spilled values it wrote (blobs), each under its content
/// hash.
///
/// Clones share both lanes, so a reader over a clone
/// ([`DeltaOverlay`]) sees what the batch stages after the clone was taken:
/// a batch reads the values it spilled itself.
#[derive(Clone, Debug)]
pub struct ArchiveDelta {
    blocks: Delta<Blake3Hash, Buffer>,
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
            blocks: Delta::zero(),
            blobs: Delta::zero(),
        }
    }

    /// A copy of what is staged that no longer shares it.
    pub fn branch(&self) -> Self {
        Self {
            blocks: self.blocks.branch(),
            blobs: self.blobs.branch(),
        }
    }

    /// The blocks lane: where a tree persists its new nodes.
    pub fn blocks(&mut self) -> &mut Delta<Blake3Hash, Buffer> {
        &mut self.blocks
    }

    /// Stages a spilled value, returning the hash its key carries.
    pub fn stage_blob(&mut self, blob: Buffer) -> Blake3Hash {
        let hash = blob.blake3_hash().clone();
        self.blobs.add(hash.clone(), blob);
        hash
    }

    /// The staged node under `hash`, if any.
    pub fn block(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.blocks.get(hash)
    }

    /// The staged spilled value under `hash`, if any.
    pub fn blob(&self, hash: &Blake3Hash) -> Option<Buffer> {
        self.blobs.get(hash)
    }

    /// Takes every staged node, emptying the blocks lane.
    pub fn flush_blocks(&mut self) -> impl Iterator<Item = Buffer> + use<> {
        self.blocks
            .flush()
            .map(|(_, block)| block)
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
        self.blocks.is_empty() && self.blobs.is_empty()
    }
}

/// Reads an [`ArchiveDelta`]'s staged lanes over an environment: a node or
/// spilled value the batch staged is served from its lane, and everything
/// else from the environment.
///
/// This is what lets a batch keep editing a tree it persisted but has not
/// flushed, and read the values it spilled before its commit writes them.
#[derive(Clone)]
pub struct DeltaOverlay<'a, Env> {
    delta: ArchiveDelta,
    env: &'a Env,
}

impl<'a, Env> DeltaOverlay<'a, Env> {
    /// Reads `delta`'s staged lanes before `env`. The overlay shares the
    /// delta, so it also sees what is staged after it was made.
    pub fn new(delta: &ArchiveDelta, env: &'a Env) -> Self {
        Self {
            delta: delta.clone(),
            env,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<Load> for DeltaOverlay<'_, Env>
where
    Env: Provider<Load> + ConditionalSync,
{
    async fn execute(&self, hash: Blake3Hash) -> Result<Option<Buffer>, DialogSearchTreeError> {
        if let Some(block) = self.delta.block(&hash) {
            return Ok(Some(block));
        }
        self.env.execute(hash).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Env> Provider<LoadBlob> for DeltaOverlay<'_, Env>
where
    Env: Provider<LoadBlob> + ConditionalSync,
{
    async fn execute(&self, hash: Blake3Hash) -> Result<Option<Buffer>, DialogArtifactsError> {
        if let Some(blob) = self.delta.blob(&hash) {
            return Ok(Some(blob));
        }
        self.env.execute(hash).await
    }
}

/// Serves spilled values from the same memory the tree's nodes live in, the
/// way an archive holding both kinds of block does.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlob> for MemoryBlocks {
    async fn execute(&self, hash: Blake3Hash) -> Result<Option<Buffer>, DialogArtifactsError> {
        Ok(self.get(&hash))
    }
}

impl ArchiveDelta {
    /// Keeps every staged node and spilled value in `blocks`, emptying both
    /// lanes: an in-memory stand-in for a commit.
    pub fn flush_into(&mut self, blocks: &MemoryBlocks) {
        for block in self.flush_blocks().chain(self.flush_blobs()) {
            blocks.store(block);
        }
    }
}

/// Serves spilled values from the observed blocks, so tests can count reads
/// through both lanes.
#[cfg(test)]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlob> for ObservingBlocks {
    async fn execute(&self, hash: Blake3Hash) -> Result<Option<Buffer>, DialogArtifactsError> {
        Ok(Provider::<Load>::execute(self, hash).await?)
    }
}
