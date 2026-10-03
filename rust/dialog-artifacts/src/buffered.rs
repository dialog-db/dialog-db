//! Write-buffered artifact trees.
//!
//! The commit path applies instructions through one of two write targets:
//!
//! - a [`TransientTree`] edit batch, which reshapes the tree as it writes, so
//!   every batch rebuilds and re-hashes the leaves it touches; or
//! - a [`HitchhikerTree`], which appends the writes to bounded per-node buffers
//!   and lets the reshape happen later, amortized across many batches.
//!
//! Both are driven by exactly the same instruction semantics, so they are
//! abstracted here as [`ArtifactWriter`] and the instruction loop
//! ([`ArtifactTreeExt::apply_versioned`](crate::ArtifactTreeExt::apply_versioned))
//! is written once against it.
//!
//! # Buffered roots are publishable
//!
//! A node's hash covers its `novelty` as well as its links, so a buffered root
//! identifies the content beneath it exactly: reads merge the buffers over the
//! stored entries, the differential is novelty-aware, and a buffered node's
//! block carries its own ops for push. Publishing one is sound.
//!
//! What a buffered root is *not* is **canonical**: the same fact set hashes
//! differently depending on where its ops currently sit, so two replicas that
//! buffered and flushed at different points hold different roots for identical
//! content. Nothing breaks, but root equality stops implying content equality,
//! so a fast-forward check no longer recognizes such replicas as equal and they
//! do merge work that finds nothing.
//!
//! Canonicality is therefore a property to reach for deliberately, via
//! [`BufferedArtifactTree::canonicalize`] (surfaced on the commit path as
//! `commit(..).canonicalize()` and on stores as
//! [`Artifacts::canonicalize`](crate::Artifacts::canonicalize)), rather than a
//! precondition for publishing. The policy across the stack: ordinary commits
//! seal buffered; sync and publish do NOT canonicalize (buffered novelty sits
//! near the root, so replicas differ in a few top blocks rather than many
//! leaf paths, and the diff exchanges fewer blocks); bulk-load paths
//! canonicalize once, explicitly, at the end of the import.

use crate::{ArchiveDelta, ArchiveReader, DeltaOverlay};
use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::ConditionalSend;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSync};
use dialog_search_tree::LoadBlock;
use dialog_search_tree::{
    Buffer, DialogSearchTreeError, Entry, HitchhikerTree, Manifest, TransientTree,
};
use futures_util::Stream;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};

use crate::history::{RecordEntries, Version};
use crate::tree::{ArtifactNodeCache, ArtifactTree, Stamp, WriteScope, write_instructions};
use crate::{Datum, DialogArtifactsError, Instruction, Key, State};

/// The buffered counterpart of [`ArtifactTree`].
///
/// Holds the same content-addressed spine, but writes land in bounded per-node
/// buffers instead of reshaping the tree, so a commit does not rebuild and
/// re-hash the large leaves it touches. The reshape is deferred to
/// [`canonicalize`](Self::canonicalize).
pub type BufferedArtifactTree = HitchhikerTree<Key, State<Datum>>;

/// A target the instruction loop can write into.
///
/// Implemented by both the canonical edit batch and the buffered tree, so the
/// per-instruction semantics (supersession scans, coverage records, history
/// entries) are written once and run identically on both.
///
/// Every method consumes and returns `Self`: both implementations are persistent
/// data structures whose writes produce a new value rather than mutating in
/// place.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ArtifactWriter: Sized {
    /// Insert (or overwrite) `value` at `key`.
    async fn write<S>(
        self,
        key: Key,
        value: State<Datum>,
        storage: &S,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync;

    /// Insert (or overwrite) a batch of entries, one write at a time.
    ///
    /// A convenience over [`write`](Self::write) for the commit path's key
    /// fan-out (three index orderings per instruction, the folded history
    /// entries, the revision-record pair). Deliberately NOT specialized to a
    /// one-pass batched enqueue on the buffered tree: batching moves where
    /// the overflow cascade fires, and the resulting (still valid, still
    /// non-canonical) buffer shapes measured consistently slower on the
    /// commit-heavy workloads than the shapes sequential writes produce.
    async fn write_all<S>(
        mut self,
        entries: Vec<(Key, State<Datum>)>,
        storage: &S,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync,
        Self: ConditionalSend,
    {
        for (key, value) in entries {
            self = self.write(key, value, storage).await?;
        }
        Ok(self)
    }

    /// Remove `key`, if present.
    async fn erase<S>(self, key: &Key, storage: &S) -> Result<Self, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync;

    /// Read the value at `key`, seeing this batch's own pending writes.
    async fn read<S>(
        &self,
        key: &Key,
        storage: &S,
    ) -> Result<Option<State<Datum>>, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync;

    /// Scan `range` in key order, seeing this batch's own pending writes.
    ///
    /// Load-bearing for `Replace` and `Retract`, which must find every prior at
    /// a slot in order to supersede it and cite it. A scan that missed a pending
    /// write would leave a superseded value live at a cardinality-one slot and
    /// emit a claim whose lineage skips what it replaced.
    fn scan<'a, S>(
        &'a self,
        range: RangeInclusive<Key>,
        storage: &'a S,
    ) -> impl Stream<Item = Result<Entry<Key, State<Datum>>, DialogSearchTreeError>> + 'a
    where
        S: Provider<LoadBlock> + ConditionalSync;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ArtifactWriter for TransientTree<Key, State<Datum>> {
    async fn write<S>(
        self,
        key: Key,
        value: State<Datum>,
        storage: &S,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync,
    {
        self.insert(key, value, storage).await
    }

    async fn erase<S>(self, key: &Key, storage: &S) -> Result<Self, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync,
    {
        self.delete(key, storage).await
    }

    async fn read<S>(
        &self,
        key: &Key,
        storage: &S,
    ) -> Result<Option<State<Datum>>, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync,
    {
        self.get(key, storage).await
    }

    fn scan<'a, S>(
        &'a self,
        range: RangeInclusive<Key>,
        storage: &'a S,
    ) -> impl Stream<Item = Result<Entry<Key, State<Datum>>, DialogSearchTreeError>> + 'a
    where
        S: Provider<LoadBlock> + ConditionalSync,
    {
        self.stream_range(range, storage)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ArtifactWriter for BufferedArtifactTree {
    // Writes route into the buffers with the flush policy DEFERRED: the
    // batch's single settle runs in `seal`, so one commit cascades into any
    // given child at most once, instead of re-flushing the same child on
    // every overflow a large batch crosses. Reads are novelty-aware
    // wherever the ops sit, so the batch's own supersession scans see
    // deferred writes exactly as settled ones.
    async fn write<S>(
        self,
        key: Key,
        value: State<Datum>,
        storage: &S,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync,
    {
        self.insert_deferred(key, value, storage).await
    }

    async fn erase<S>(self, key: &Key, storage: &S) -> Result<Self, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync,
    {
        self.delete_deferred(key.clone(), storage).await
    }

    async fn read<S>(
        &self,
        key: &Key,
        storage: &S,
    ) -> Result<Option<State<Datum>>, DialogSearchTreeError>
    where
        S: Provider<LoadBlock> + ConditionalSync,
    {
        self.get(key, storage).await
    }

    fn scan<'a, S>(
        &'a self,
        range: RangeInclusive<Key>,
        storage: &'a S,
    ) -> impl Stream<Item = Result<Entry<Key, State<Datum>>, DialogSearchTreeError>> + 'a
    where
        S: Provider<LoadBlock> + ConditionalSync,
    {
        self.stream_range(range, storage)
    }
}

/// An open buffered write batch over an [`ArtifactTree`], not yet persisted.
///
/// The commit path's three-step surface:
///
/// 1. [`BufferedBatch::apply`] drains the instruction stream into the buffered
///    tree and reports whether it changed the indexes, persisting NOTHING;
/// 2. the caller decides: dropping the batch is a complete no-op (the delta is
///    untouched and the base tree root unchanged), which is how an unchanged
///    commit declines to mint a revision;
/// 3. otherwise the caller appends its revision-record entries with
///    [`record`](Self::record) and seals everything, data and records
///    together, with the ONE persist (or canonicalize) in
///    [`seal`](Self::seal).
///
/// This exists so the revision record rides the same buffered write as the
/// batch's data. Records need the batch's outcome (a no-op commit mints
/// nothing, and the record signs over its content), so they cannot be part of
/// the instruction stream; but routing them through the canonical edit path
/// after the batch persisted cost a second spine-to-leaf rewrite per commit,
/// whose leaf re-encode grew with the database. Appending them to the still
/// open buffered tree makes them ordinary buffered ops covered by the same
/// seal.
pub struct BufferedBatch {
    tree: BufferedArtifactTree,
    cache: ArtifactNodeCache,
    manifest: Manifest,
    changed: bool,
    slot: Option<SpineSlot>,
    /// The values this batch spilled, staged until [`seal`](Self::seal)
    /// hands them to the caller's delta with the batch's nodes.
    staged: ArchiveDelta,
}

/// A slot holding one live buffered spine between commits, keyed by the root
/// hash it was persisted as.
///
/// Without it, every commit re-opens the root frame from bytes (decoding the
/// frame, bulk-copying every sealed buffer) and re-drops the spine after the
/// seal — the measured dominant cost of an in-memory commit. A caller that
/// owns a sequence of commits holds one slot; [`BufferedBatch::apply_reusing`]
/// takes the spine when its recorded root matches the tree being written
/// (anything else — an external reset, a canonicalize, a concurrent writer —
/// simply misses and falls back to a fresh open), and the non-canonicalizing
/// [`seal`](BufferedBatch::seal) puts it back keyed by the new root.
///
/// Purely an in-process accelerator: the persisted bytes are identical with
/// or without it (pinned by `it_persists_identically_when_the_spine_stays_live`
/// in `dialog-search-tree` and `it_commits_identically_when_the_spine_is_reused`
/// here), and dropping the slot at any point only costs the next commit a
/// fresh open.
#[derive(Clone, Default)]
pub struct SpineSlot {
    #[allow(clippy::type_complexity)]
    slot: Arc<Mutex<Option<(NodeHash, BufferedArtifactTree)>>>,
}

impl Debug for SpineSlot {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let root = self
            .slot
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().map(|(root, _)| root.clone()));
        f.debug_struct("SpineSlot").field("root", &root).finish()
    }
}

impl SpineSlot {
    /// An empty slot.
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes the held spine if it was persisted as `root`; a mismatch drops
    /// the stale spine.
    fn take(&self, root: &NodeHash) -> Option<BufferedArtifactTree> {
        let mut slot = self.slot.lock().expect("spine slot lock");
        match slot.take() {
            Some((held_root, tree)) if &held_root == root => Some(tree),
            _ => None,
        }
    }

    /// Stores `tree` as the live spine for `root`.
    fn put(&self, root: NodeHash, tree: BufferedArtifactTree) {
        *self.slot.lock().expect("spine slot lock") = Some((root, tree));
    }
}

impl BufferedBatch {
    /// Applies `instructions` to a buffered tree opened over `tree`, without
    /// persisting anything.
    ///
    /// The buffered counterpart of
    /// [`ArtifactTreeExt::apply_versioned`](crate::ArtifactTreeExt::apply_versioned),
    /// running the very same instruction semantics (they share
    /// [`write_instructions`](crate::tree::write_instructions)). The
    /// difference is where the writes land: instead of reshaping the tree per
    /// batch, they accumulate in bounded per-node buffers, and the reshape
    /// happens only when a buffer overflows and cascades.
    ///
    /// `tree` itself is untouched; the batch lives in memory until
    /// [`seal`](Self::seal).
    #[tracing::instrument(skip_all, name = "apply_batch")]
    pub async fn apply<S, I>(
        tree: &ArtifactTree,
        store: &S,
        version: Option<Version>,
        instructions: I,
        scope: WriteScope,
    ) -> Result<Self, DialogArtifactsError>
    where
        S: ArchiveReader + Clone,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        Self::open_batch(tree, store, version.into(), instructions, scope, None).await
    }

    /// [`apply`](Self::apply) with a live spine carried across commits.
    ///
    /// When `slot` holds the spine persisted as `tree`'s root, the batch
    /// writes into it directly — no root-frame decode, no sealed-buffer bulk
    /// copies — and the non-canonicalizing [`seal`](Self::seal) puts it back
    /// keyed by the root it produces, ready for the caller's next commit. A
    /// slot miss (first commit, external root change, canonicalize) falls
    /// back to exactly [`apply`](Self::apply)'s fresh open. The persisted
    /// bytes are identical either way; the slot only removes the per-commit
    /// round trip through them.
    #[tracing::instrument(skip_all, name = "apply_batch_reusing")]
    pub async fn apply_reusing<S, I>(
        slot: &SpineSlot,
        tree: &ArtifactTree,
        store: &S,
        version: Option<Version>,
        instructions: I,
        scope: WriteScope,
    ) -> Result<Self, DialogArtifactsError>
    where
        S: ArchiveReader + Clone,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        Self::open_batch(
            tree,
            store,
            version.into(),
            instructions,
            scope,
            Some(slot.clone()),
        )
        .await
    }

    /// [`apply_reusing`](Self::apply_reusing) amending `version`: `tree`
    /// already carries writes under it (a commit being amended), and this
    /// batch writes more under the same version.
    ///
    /// Datums are tagged with `version` as in any versioned batch; the
    /// history each instruction records folds into what `tree` already
    /// records under `version` at the same history key, exactly as two
    /// writes within one batch fold. So applying a stream in two amending
    /// passes records what applying it in one pass records.
    #[tracing::instrument(skip_all, name = "amend_batch_reusing")]
    pub async fn amend_reusing<S, I>(
        slot: &SpineSlot,
        tree: &ArtifactTree,
        store: &S,
        version: Version,
        instructions: I,
        scope: WriteScope,
    ) -> Result<Self, DialogArtifactsError>
    where
        S: ArchiveReader + Clone,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        Self::open_batch(
            tree,
            store,
            Stamp::Amend(version),
            instructions,
            scope,
            Some(slot.clone()),
        )
        .await
    }

    async fn open_batch<S, I>(
        tree: &ArtifactTree,
        store: &S,
        stamp: Stamp,
        instructions: I,
        scope: WriteScope,
        slot: Option<SpineSlot>,
    ) -> Result<Self, DialogArtifactsError>
    where
        S: ArchiveReader + Clone,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        // The batch reads through what it stages, so a value it spills is
        // readable before the commit writes it.
        let mut staged = ArchiveDelta::zero();
        let storage = DeltaOverlay::new(&staged, store);
        // Keys are built under the target tree's own format, read from the
        // manifest its root node carries. Writes preserve that format, so the
        // manifest captured here also governs the record entries appended
        // later and the root `seal` produces.
        let manifest = tree.manifest(&storage).await?;
        let spine = slot
            .as_ref()
            .and_then(|slot| slot.take(tree.root()))
            .unwrap_or_else(|| HitchhikerTree::open(tree));
        let (buffered, changed) = write_instructions(
            spine,
            &storage,
            &mut staged,
            stamp,
            &manifest,
            instructions,
            scope,
        )
        .await?;
        Ok(Self {
            tree: buffered,
            cache: tree.node_cache(),
            manifest,
            changed,
            slot,
            staged,
        })
    }

    /// Applies a further instruction stream to this still open batch, under
    /// its own `scope`, with the same semantics and version as the first.
    ///
    /// A commit uses it to write the facts it derives itself, such as an
    /// asset's `dialog.asset/size`, under
    /// [`Machinery`](WriteScope::Machinery) scope in the same batch as the
    /// application's own instructions, which stay under
    /// [`Application`](WriteScope::Application) scope. The batch counts as
    /// changed when either stream changed the indexes.
    pub async fn then_apply<S, I>(
        mut self,
        store: &S,
        stamp: Stamp,
        instructions: I,
        scope: WriteScope,
    ) -> Result<Self, DialogArtifactsError>
    where
        S: ArchiveReader + Clone,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        // Read through what the batch staged so far, as its first pass did.
        let storage = DeltaOverlay::new(&self.staged, store);
        let (tree, changed) = write_instructions(
            self.tree,
            &storage,
            &mut self.staged,
            stamp,
            &self.manifest,
            instructions,
            scope,
        )
        .await?;
        Ok(Self {
            tree,
            changed: self.changed || changed,
            ..self
        })
    }

    /// Whether the applied instructions changed the indexes at all.
    ///
    /// A batch made entirely of no-ops (re-asserting values already in place,
    /// retracting absent facts) leaves the tree untouched and records no
    /// history; callers should mint no revision for it and drop the batch
    /// unsealed.
    pub fn changed(&self) -> bool {
        self.changed
    }

    /// The target tree's format [`Manifest`], captured at
    /// [`apply`](Self::apply) time.
    ///
    /// Record entries must be built under it (see
    /// [`RevisionRecord::entries`](crate::history::RevisionRecord::entries)):
    /// the record's value rides its key through the tree's own inline-vs-spill
    /// threshold, not the default.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Appends pre-built record entries (revision lineage records, which enter
    /// through this surface and never through instructions) to the open
    /// buffered tree.
    ///
    /// The entries become ordinary buffered ops: the single persist in
    /// [`seal`](Self::seal) covers them together with the batch's data, and
    /// readers see them through the same novelty-aware get and scan as any
    /// other buffered write.
    #[tracing::instrument(skip_all, name = "buffer_records")]
    pub async fn record<S>(
        mut self,
        store: &S,
        entries: impl Into<RecordEntries>,
    ) -> Result<Self, DialogArtifactsError>
    where
        S: ArchiveReader + Clone,
    {
        let RecordEntries { entries, spill } = entries.into();
        // A spilled record value is staged with the batch's other spills,
        // as the instruction path stages a spilling fact's.
        if let Some((_, bytes)) = spill {
            self.staged.stage_blob(Buffer::from(bytes));
        }
        let storage = DeltaOverlay::new(&self.staged, store);
        self.tree = self.tree.write_all(entries, &storage).await?;
        Ok(self)
    }

    /// Seals the whole batch, data and record entries alike, into `delta` with
    /// a single persist, returning the resulting tree.
    ///
    /// With `canonicalize` the buffers are flushed to the leaves first, so the
    /// result is the deterministic canonical form of its fact set. Without it
    /// the buffers are left in place and the result is the buffered form.
    ///
    /// **Both forms are publishable.** A node's hash covers its buffers as
    /// well as its links, so a buffered root identifies its content exactly:
    /// it reads (buffers merge over stored entries), diffs (the differential
    /// is novelty-aware), and pushes (its blocks carry the ops) like any other
    /// root. What the buffered form gives up is *canonicality*, meaning two
    /// replicas with the same facts hash differently if they flushed at
    /// different points. That costs convergence detection, not correctness: a
    /// fast-forward check compares roots, so such replicas fail to recognize
    /// each other as equal and do merge work that finds nothing.
    #[tracing::instrument(skip_all, name = "seal_batch")]
    pub async fn seal<S>(
        mut self,
        store: &S,
        delta: &mut ArchiveDelta,
        canonicalize: bool,
    ) -> Result<ArtifactTree, DialogArtifactsError>
    where
        S: ArchiveReader + Clone,
    {
        // The values the batch spilled go out with its nodes.
        for blob in self.staged.flush_blobs() {
            delta.stage_blob(blob);
        }
        let delta = delta.blocks();
        let storage = store.clone();
        Ok(if canonicalize {
            // Canonicalizing consumes the spine (and drains every buffer, so
            // the batch's deferred flush decision is moot); a slot the batch
            // carried stays empty and the next commit opens fresh from the
            // canonical root.
            self.tree.canonicalize(&storage, delta).await?
        } else if let Some(slot) = self.slot {
            // The batch's writes deferred the flush policy; apply it once
            // over everything the batch accumulated, then serialize the
            // spine with its buffers intact and hand it back to the slot
            // keyed by the root it just produced, so the caller's next
            // commit appends to it instead of re-opening the frame from
            // bytes.
            let mut tree = self.tree.settle(&storage).await?;
            let root = tree.persist_mut(delta)?;
            slot.put(root.clone(), tree);
            ArtifactTree::from_hash_with_cache(root, self.cache)
        } else {
            // Settle the batch's deferred writes, then serialize the spine
            // with its buffers intact and seal the resulting root: the hash
            // covers the buffered ops, so this is a complete identity for
            // the tree's content.
            let root = self.tree.settle(&storage).await?.persist(delta)?;
            ArtifactTree::from_hash_with_cache(root, self.cache)
        })
    }
}

/// Applies `instructions` to `tree` through the write buffer, returning whether
/// the batch changed the indexes.
///
/// The one-shot composition of [`BufferedBatch::apply`] and
/// [`BufferedBatch::seal`], for callers with no record entries to interleave
/// (the commit path has, and uses the three-step [`BufferedBatch`] surface
/// directly). See those for the semantics, including why both the canonical
/// and the buffered form are publishable.
#[tracing::instrument(skip_all, name = "apply_buffered")]
pub async fn apply_buffered<S, I>(
    tree: &mut ArtifactTree,
    store: &S,
    delta: &mut ArchiveDelta,
    version: Option<Version>,
    instructions: I,
    canonicalize: bool,
) -> Result<bool, DialogArtifactsError>
where
    S: ArchiveReader + Clone,
    I: Stream<Item = Instruction> + ConditionalSend,
{
    let batch =
        BufferedBatch::apply(tree, store, version, instructions, WriteScope::Application).await?;
    let changed = batch.changed();
    *tree = batch.seal(store, delta, canonicalize).await?;
    Ok(changed)
}

/// [`apply_buffered`] with a live spine carried across commits through
/// `slot`: see [`BufferedBatch::apply_reusing`] for the reuse contract.
#[tracing::instrument(skip_all, name = "apply_buffered_reusing")]
pub async fn apply_buffered_reusing<S, I>(
    slot: &SpineSlot,
    tree: &mut ArtifactTree,
    store: &S,
    delta: &mut ArchiveDelta,
    version: Option<Version>,
    instructions: I,
    canonicalize: bool,
) -> Result<bool, DialogArtifactsError>
where
    S: ArchiveReader + Clone,
    I: Stream<Item = Instruction> + ConditionalSend,
{
    let batch = BufferedBatch::apply_reusing(
        slot,
        tree,
        store,
        version,
        instructions,
        WriteScope::Application,
    )
    .await?;
    let changed = batch.changed();
    *tree = batch.seal(store, delta, canonicalize).await?;
    Ok(changed)
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;

    use crate::ArchiveDelta;
    use dialog_search_tree::MemoryBlocks;

    use futures_util::stream;

    use super::{BufferedBatch, WriteScope, apply_buffered};
    use crate::history::{Edition, Origin, Version};
    use crate::key::FromKey as _;
    use crate::tree::Stamp;
    use crate::tree::{ArtifactTree, ArtifactTreeExt as _};
    use crate::{
        Artifact, Asset, AttributeKey, BlobChange, BlobIndexExt as _, Datum, DialogArtifactsError,
        EntityKey, Instruction, SealedCopy, State, Value, blob_changes,
    };
    use futures_util::TryStreamExt as _;

    fn store() -> MemoryBlocks {
        MemoryBlocks::new()
    }

    fn assert_of(entity: &str, value: &str) -> Instruction {
        Instruction::Assert(Artifact {
            the: "test/field".parse().unwrap(),
            of: entity.parse().unwrap(),
            is: Value::String(value.to_string()),
            cause: None,
        })
    }

    fn replace_of(entity: &str, value: &str) -> Instruction {
        Instruction::Replace(Artifact {
            the: "test/field".parse().unwrap(),
            of: entity.parse().unwrap(),
            is: Value::String(value.to_string()),
            cause: None,
        })
    }

    fn retract_of(entity: &str, value: &str) -> Instruction {
        Instruction::Retract(Artifact {
            the: "test/field".parse().unwrap(),
            of: entity.parse().unwrap(),
            is: Value::String(value.to_string()),
            cause: None,
        })
    }

    /// Many sequential buffered commits must retain every fact.
    ///
    /// A branch commits repeatedly, each batch reopening the tree from the
    /// previously sealed root and persisting its spine. Every fact asserted
    /// this way must still be readable at the end: if a batch's buffered ops
    /// are dropped when the next one reopens and reseals the spine, the facts
    /// vanish silently and a replica reports a fraction of what it committed.
    #[dialog_common::test]
    async fn it_retains_every_fact_across_many_buffered_commits() -> Result<()> {
        use crate::tree::ArtifactTreeExt as _;

        const COMMITS: usize = 200;

        let store = store();
        let mut tree = ArtifactTree::empty();
        for i in 0..COMMITS {
            let mut delta = ArchiveDelta::zero();
            apply_buffered(
                &mut tree,
                &store,
                &mut delta,
                Some(Version::new(
                    Origin::from([7u8; 32]),
                    Edition::new(i as u64 + 1),
                )),
                stream::iter(vec![assert_of(&format!("user:{i}"), "resident")]),
                false,
            )
            .await?;
            delta.flush_into(&store);
        }

        let the = "test/field".parse().unwrap();
        let mut found = 0usize;
        for i in 0..COMMITS {
            let of = format!("user:{i}").parse().unwrap();
            if !tree.select_data(store.clone(), &of, &the).await?.is_empty() {
                found += 1;
            }
        }
        assert_eq!(
            found, COMMITS,
            "every buffered commit's fact must survive the following commits"
        );
        Ok(())
    }

    /// Writing the revision record after a buffered batch must not drop the
    /// batch's buffered ops.
    ///
    /// The commit path applies instructions through the buffer and then writes
    /// the revision record into the same tree via `record`, which goes through
    /// the canonical edit path. If that edit does not carry the buffered ops
    /// across, the commit publishes a root missing everything still buffered,
    /// and the branch loses those facts.
    #[dialog_common::test]
    async fn it_keeps_buffered_ops_when_the_revision_record_is_written() -> Result<()> {
        use crate::key::FromKey as _;
        use crate::tree::ArtifactTreeExt as _;

        let store = store();
        let mut tree = ArtifactTree::empty();

        // Enough commits that the spine grows and the buffers matter, each one
        // buffering its batch and then writing a record entry, as a commit does.
        const COMMITS: usize = 300;
        for i in 0..COMMITS {
            let mut delta = ArchiveDelta::zero();
            apply_buffered(
                &mut tree,
                &store,
                &mut delta,
                None,
                stream::iter(vec![assert_of(&format!("user:{i}"), "resident")]),
                false,
            )
            .await?;

            // The record write the commit path performs on the same tree.
            let artifact = Artifact {
                the: "test/record".parse().unwrap(),
                of: format!("rev:{i}").parse().unwrap(),
                is: Value::String(format!("{i}")),
                cause: None,
            };
            let entity_key = crate::EntityKey::from_artifact(
                &artifact,
                &dialog_search_tree::Manifest::default(),
            );
            let attribute_key = crate::AttributeKey::from_key(&entity_key);
            let added = crate::State::Added(crate::Datum::for_artifact(&artifact));
            tree.record(
                &store,
                &mut delta,
                vec![
                    (entity_key.into_key(), added.clone()),
                    (attribute_key.into_key(), added),
                ]
                .into(),
            )
            .await?;

            delta.flush_into(&store);
        }

        let the = "test/field".parse().unwrap();
        let mut found = 0usize;
        for i in 0..COMMITS {
            let of = format!("user:{i}").parse().unwrap();
            if !tree.select_data(store.clone(), &of, &the).await?.is_empty() {
                found += 1;
            }
        }
        assert_eq!(
            found, COMMITS,
            "writing the revision record must not drop buffered facts"
        );
        Ok(())
    }

    /// The buffered write path must land on the *same canonical root* as the
    /// direct path for the same instructions.
    ///
    /// This is what makes buffering safe to put under the commit path: the root
    /// a peer sees, adopts by hash, and diffs is unchanged, so every frugal pull
    /// scenario keeps working. Deletes and replaces are included because they
    /// are the read-modify-write cases, where the buffered path has to consult
    /// its own buffers to find the priors it supersedes.
    #[dialog_common::test]
    async fn it_lands_on_the_same_root_as_the_direct_path() -> Result<()> {
        // Built per run: `Instruction` is not `Clone`, and both paths must see
        // the identical stream.
        fn batches() -> Vec<Vec<Instruction>> {
            vec![
                vec![assert_of("user:1", "a"), assert_of("user:2", "b")],
                vec![replace_of("user:1", "c")],
                vec![assert_of("user:3", "d"), retract_of("user:2", "b")],
                vec![replace_of("user:3", "e"), assert_of("user:4", "f")],
                vec![retract_of("user:4", "f")],
            ]
        }

        let direct_store = store();
        let mut direct = ArtifactTree::empty();
        for batch in batches() {
            let mut delta = ArchiveDelta::zero();
            direct
                .apply_versioned(&direct_store, &mut delta, None, stream::iter(batch))
                .await?;
            delta.flush_into(&direct_store);
        }

        let buffered_store = store();
        let mut buffered = ArtifactTree::empty();
        for batch in batches() {
            let mut delta = ArchiveDelta::zero();
            apply_buffered(
                &mut buffered,
                &buffered_store,
                &mut delta,
                None,
                stream::iter(batch),
                true,
            )
            .await?;
            delta.flush_into(&buffered_store);
        }

        assert_eq!(
            direct.root(),
            buffered.root(),
            "the buffered path must produce the byte-identical canonical root"
        );
        Ok(())
    }

    /// A single batch that asserts a fact and retracts it again must cancel
    /// through the buffer: the retract sees the same-batch assert on the
    /// write target, so reads find nothing afterward, and with
    /// canonicalization the root is identical to the canonical path's root
    /// for the same batch. Covers both the flushed and the still-buffered
    /// form; the deeper cross-level variant is not reachable through
    /// `apply_buffered` (it opens the hitchhiker with default buffer sizes,
    /// so a two-instruction batch never cascades) and is pinned at the
    /// hitchhiker level instead.
    #[dialog_common::test]
    async fn it_cancels_a_same_batch_assert_and_retract_through_the_buffer() -> Result<()> {
        let the = "test/field".parse().unwrap();
        let of = "user:1".parse().unwrap();

        for canonicalize in [true, false] {
            let buffered_store = store();
            let mut tree = ArtifactTree::empty();
            let mut delta = ArchiveDelta::zero();
            let changed = apply_buffered(
                &mut tree,
                &buffered_store,
                &mut delta,
                None,
                stream::iter(vec![
                    assert_of("user:1", "resident"),
                    retract_of("user:1", "resident"),
                ]),
                canonicalize,
            )
            .await?;
            delta.flush_into(&buffered_store);

            assert!(
                changed,
                "canonicalize {canonicalize}: the batch wrote and unwrote, which is a change"
            );
            assert!(
                tree.select_data(buffered_store.clone(), &of, &the)
                    .await?
                    .is_empty(),
                "canonicalize {canonicalize}: the retract must cancel the same-batch assert"
            );

            if canonicalize {
                // The same batch through the canonical path on a fresh tree
                // must land on the identical root.
                let direct_store = store();
                let mut direct = ArtifactTree::empty();
                let mut direct_delta = ArchiveDelta::zero();
                direct
                    .apply_versioned(
                        &direct_store,
                        &mut direct_delta,
                        None,
                        stream::iter(vec![
                            assert_of("user:1", "resident"),
                            retract_of("user:1", "resident"),
                        ]),
                    )
                    .await?;
                assert_eq!(
                    tree.root(),
                    direct.root(),
                    "the cancelling pair must land on the canonical root"
                );
            }
        }
        Ok(())
    }

    /// Record entries appended through [`BufferedBatch::record`] are ordinary
    /// buffered ops covered by the batch's single seal.
    ///
    /// Two pins: sealing with canonicalize lands on the identical root the
    /// canonical path (`apply_versioned` + `record`) produces for the same
    /// data and entries, so the record's placement is not path-dependent; and
    /// sealing the buffered form keeps the record readable through the
    /// novelty-aware read path. `apply` itself must leave the base tree
    /// untouched, which is what makes dropping an unsealed no-op batch free.
    #[dialog_common::test]
    async fn it_seals_record_entries_with_the_batch_onto_the_canonical_root() -> Result<()> {
        fn version() -> Version {
            Version::new(Origin::from([7u8; 32]), Edition::new(1))
        }
        fn data() -> Vec<Instruction> {
            vec![assert_of("user:1", "resident"), assert_of("user:2", "b")]
        }
        fn entries() -> Vec<(crate::Key, State<Datum>)> {
            let artifact = Artifact {
                the: "test/record".parse().unwrap(),
                of: "rev:1".parse().unwrap(),
                is: Value::String("record".to_string()),
                cause: None,
            };
            let entity_key =
                EntityKey::from_artifact(&artifact, &dialog_search_tree::Manifest::default());
            let attribute_key = AttributeKey::from_key(&entity_key);
            let added = State::Added(Datum::for_artifact(&artifact));
            vec![
                (entity_key.into_key(), added.clone()),
                (attribute_key.into_key(), added),
            ]
        }

        // The canonical reference: data through the canonical edit path, then
        // the record through the canonical `record` surface.
        let direct_store = store();
        let mut direct = ArtifactTree::empty();
        let mut direct_delta = ArchiveDelta::zero();
        direct
            .apply_versioned(
                &direct_store,
                &mut direct_delta,
                Some(version()),
                stream::iter(data()),
            )
            .await?;
        direct
            .record(&direct_store, &mut direct_delta, entries().into())
            .await?;

        // The batch surface, sealed canonical: the same fact set must land on
        // the byte-identical root.
        let batch_store = store();
        let base = ArtifactTree::empty();
        let mut delta = ArchiveDelta::zero();
        let batch = BufferedBatch::apply(
            &base,
            &batch_store,
            Some(version()),
            stream::iter(data()),
            WriteScope::Application,
        )
        .await?;
        assert!(batch.changed(), "the data writes change the indexes");
        let batch = batch.record(&batch_store, entries()).await?;
        let sealed = batch.seal(&batch_store, &mut delta, true).await?;
        assert_eq!(
            sealed.root(),
            direct.root(),
            "the batch-carried record must land on the canonical root"
        );
        assert_eq!(
            base.root(),
            ArtifactTree::empty().root(),
            "applying a batch must not touch the base tree"
        );

        // The batch surface, sealed buffered: the record must read back
        // through the novelty-aware read path.
        let buffered_store = store();
        let mut delta = ArchiveDelta::zero();
        let batch = BufferedBatch::apply(
            &ArtifactTree::empty(),
            &buffered_store,
            Some(version()),
            stream::iter(data()),
            WriteScope::Application,
        )
        .await?;
        let batch = batch.record(&buffered_store, entries()).await?;
        let sealed = batch.seal(&buffered_store, &mut delta, false).await?;
        delta.flush_into(&buffered_store);
        let records = sealed
            .select_record(
                buffered_store.clone(),
                &"rev:1".parse()?,
                &"test/record".parse()?,
            )
            .await?;
        assert_eq!(records.len(), 1, "the record reads back from the buffers");
        assert_eq!(records[0].is, Value::String("record".to_string()));
        assert!(
            !sealed
                .select_data(buffered_store, &"user:1".parse()?, &"test/field".parse()?)
                .await?
                .is_empty(),
            "the batch's data survives alongside the record"
        );
        Ok(())
    }

    fn asset_version() -> Version {
        Version::new(Origin::from([3u8; 32]), Edition::new(0))
    }

    /// A commit writes the facts it derives under machinery scope after the
    /// application's own instructions, in one batch. An asset's fact then
    /// vouches for its content: the tree answers its size, and the
    /// differential names its bytes for push to ship.
    #[dialog_common::test]
    async fn it_records_an_asset_fact_under_machinery_scope() -> Result<()> {
        let store = store();
        let mut delta = ArchiveDelta::zero();
        let asset = Asset::new(b"asset bytes".to_vec());

        let batch = BufferedBatch::apply(
            &ArtifactTree::empty(),
            &store,
            Some(asset_version()),
            stream::iter(vec![assert_of("user:alice", "Alice")]),
            WriteScope::Application,
        )
        .await?;
        let batch = batch
            .then_apply(
                &store,
                Stamp::Amend(asset_version()),
                stream::iter(vec![Instruction::Assert(asset.fact()?)]),
                WriteScope::Machinery,
            )
            .await?;
        assert!(batch.changed());
        let tree = batch.seal(&store, &mut delta, false).await?;
        delta.flush_into(&store);

        assert_eq!(
            tree.content_size(&store, asset.hash()).await?,
            Some(asset.size())
        );
        assert_eq!(tree.content_size(&store, &[0u8; 32]).await?, None);

        let changes: Vec<_> = blob_changes(ArtifactTree::empty(), tree, store.clone())
            .try_collect()
            .await?;
        assert_eq!(changes, vec![BlobChange::Added(*asset.hash())]);
        Ok(())
    }

    /// A sealed asset's one fact vouches for its size and ships as its
    /// sealed copy: the blob it names is the copy, never the plaintext.
    #[dialog_common::test]
    async fn it_ships_a_sealed_asset_as_its_copy() -> Result<()> {
        let store = store();
        let mut delta = ArchiveDelta::zero();
        let copy = SealedCopy {
            address: [9u8; 32],
            length: 99,
        };
        let asset = Asset::sealed([7u8; 32], 34, copy);

        let batch = BufferedBatch::apply(
            &ArtifactTree::empty(),
            &store,
            Some(asset_version()),
            stream::iter(vec![Instruction::Assert(asset.sealed_fact(&copy)?)]),
            WriteScope::Machinery,
        )
        .await?;
        let tree = batch.seal(&store, &mut delta, false).await?;
        delta.flush_into(&store);

        assert_eq!(tree.content_size(&store, asset.hash()).await?, Some(34));
        assert_eq!(tree.asset_size(&store, asset.hash()).await?, None);
        assert_eq!(
            tree.sealed_asset(&store, asset.hash()).await?,
            Some((copy, 34))
        );
        let changes: Vec<_> = blob_changes(ArtifactTree::empty(), tree, store.clone())
            .try_collect()
            .await?;
        assert_eq!(changes, vec![BlobChange::Added(copy.address)]);
        Ok(())
    }

    /// Application instructions cannot write an asset's fact, so its size
    /// is always the one a commit derived from the bytes.
    #[dialog_common::test]
    async fn it_refuses_an_asset_fact_from_an_application() -> Result<()> {
        let store = store();
        let asset = Asset::new(b"asset bytes".to_vec());
        let refused = BufferedBatch::apply(
            &ArtifactTree::empty(),
            &store,
            Some(asset_version()),
            stream::iter(vec![Instruction::Assert(asset.fact()?)]),
            WriteScope::Application,
        )
        .await;
        assert!(matches!(
            refused,
            Err(DialogArtifactsError::ReservedAttribute(_))
        ));
        Ok(())
    }
}
