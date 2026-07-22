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
//! `commit(..).canonicalize()`), rather than a precondition for publishing.

use async_trait::async_trait;
use dialog_common::ConditionalSend;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSync};
use dialog_search_tree::{
    Buffer, Cache, ContentAddressedStorage, Delta, DialogSearchTreeError, Entry, HitchhikerTree,
    Manifest, TransientTree,
};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::Stream;
use std::ops::RangeInclusive;

use crate::history::Version;
use crate::tree::{
    ArtifactTree, TreeStorageBridge, privileged_entries, privileged_keys, write_instructions,
};
use crate::{Artifact, Datum, DialogArtifactsError, Instruction, Key, State};

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
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync;

    /// Remove `key`, if present.
    async fn erase<S>(
        self,
        key: &Key,
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync;

    /// Read the value at `key`, seeing this batch's own pending writes.
    async fn read<S>(
        &self,
        key: &Key,
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Option<State<Datum>>, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync;

    /// Scan `range` in key order, seeing this batch's own pending writes.
    ///
    /// Load-bearing for `Replace` and `Retract`, which must find every prior at
    /// a slot in order to supersede it and cite it. A scan that missed a pending
    /// write would leave a superseded value live at a cardinality-one slot and
    /// emit a claim whose lineage skips what it replaced.
    fn scan<'a, S>(
        &'a self,
        range: RangeInclusive<Key>,
        storage: &'a ContentAddressedStorage<S>,
    ) -> impl Stream<Item = Result<Entry<Key, State<Datum>>, DialogSearchTreeError>> + 'a
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ArtifactWriter for TransientTree<Key, State<Datum>> {
    async fn write<S>(
        self,
        key: Key,
        value: State<Datum>,
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.insert(key, value, storage).await
    }

    async fn erase<S>(
        self,
        key: &Key,
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.delete(key, storage).await
    }

    async fn read<S>(
        &self,
        key: &Key,
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Option<State<Datum>>, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.get(key, storage).await
    }

    fn scan<'a, S>(
        &'a self,
        range: RangeInclusive<Key>,
        storage: &'a ContentAddressedStorage<S>,
    ) -> impl Stream<Item = Result<Entry<Key, State<Datum>>, DialogSearchTreeError>> + 'a
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.stream_range(range, storage)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ArtifactWriter for BufferedArtifactTree {
    async fn write<S>(
        self,
        key: Key,
        value: State<Datum>,
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.insert(key, value, storage).await
    }

    async fn erase<S>(
        self,
        key: &Key,
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.delete(key.clone(), storage).await
    }

    async fn read<S>(
        &self,
        key: &Key,
        storage: &ContentAddressedStorage<S>,
    ) -> Result<Option<State<Datum>>, DialogSearchTreeError>
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.get(key, storage).await
    }

    fn scan<'a, S>(
        &'a self,
        range: RangeInclusive<Key>,
        storage: &'a ContentAddressedStorage<S>,
    ) -> impl Stream<Item = Result<Entry<Key, State<Datum>>, DialogSearchTreeError>> + 'a
    where
        S: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
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
    cache: Cache<NodeHash, Buffer>,
    manifest: Manifest,
    changed: bool,
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
        store: &mut S,
        version: Option<Version>,
        instructions: I,
    ) -> Result<Self, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
        // Keys are built under the target tree's own format, read from the
        // manifest its root node carries. Writes preserve that format, so the
        // manifest captured here also governs the record entries appended
        // later and the root `seal` produces.
        let manifest = tree.manifest(&storage).await?;
        let (buffered, changed) = write_instructions(
            HitchhikerTree::open(tree),
            store,
            &storage,
            version,
            &manifest,
            instructions,
        )
        .await?;
        Ok(Self {
            tree: buffered,
            cache: tree.node_cache(),
            manifest,
            changed,
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
        entries: Vec<(Key, State<Datum>)>,
    ) -> Result<Self, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
        for (key, value) in entries {
            self.tree = self.tree.write(key, value, &storage).await?;
        }
        Ok(self)
    }

    /// Installs privileged facts (deductive rules — see
    /// [`privileged_entries`](crate::tree::privileged_entries)) into the open
    /// buffered tree, spilling any large value block into `store` first.
    ///
    /// This is the sanctioned rule-write rail. Rules live under the reserved
    /// `dialog.rule/*` namespace, which the public instruction path refuses;
    /// routing them through this surface — the same one revision records use —
    /// is what lets an app install a rule without being able to forge one
    /// through an ordinary `assert`. The facts get all three index orderings so
    /// the `conclusion` (by value) and `source` (by entity) scans both find
    /// them, and marks the batch changed so a commit whose only writes are rule
    /// installs still mints a revision.
    #[tracing::instrument(skip_all, name = "install_rules")]
    pub async fn install<S>(
        mut self,
        store: &mut S,
        rules: &[Artifact],
    ) -> Result<Self, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
        for artifact in rules {
            let entries = privileged_entries(store, artifact, &self.manifest).await?;
            for (key, value) in entries {
                self.tree = self.tree.write(key, value, &storage).await?;
                self.changed = true;
            }
        }
        Ok(self)
    }

    /// Removes privileged facts installed by [`install`](Self::install),
    /// erasing every index key of each fact from the open buffered tree.
    ///
    /// A removal that erased nothing (the rule was never installed) leaves the
    /// tree untouched and does not mark the batch changed, so a commit whose
    /// only instruction is such a removal stays a no-op — the same shape a
    /// retract of an absent fact takes on the public path.
    #[tracing::instrument(skip_all, name = "uninstall_rules")]
    pub async fn uninstall<S>(
        mut self,
        store: &S,
        rules: &[Artifact],
    ) -> Result<Self, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
        for artifact in rules {
            for key in privileged_keys(artifact, &self.manifest) {
                if self.tree.read(&key, &storage).await?.is_some() {
                    self.tree = self.tree.erase(&key, &storage).await?;
                    self.changed = true;
                }
            }
        }
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
        self,
        store: &S,
        delta: &mut Delta<NodeHash, Buffer>,
        canonicalize: bool,
    ) -> Result<ArtifactTree, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
        Ok(if canonicalize {
            self.tree.canonicalize(&storage, delta).await?
        } else {
            // Serialize the spine with its buffers intact and seal the
            // resulting root: the hash covers the buffered ops, so this is a
            // complete identity for the tree's content.
            let root = self.tree.persist(delta)?;
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
    store: &mut S,
    delta: &mut Delta<NodeHash, Buffer>,
    version: Option<Version>,
    instructions: I,
    canonicalize: bool,
) -> Result<bool, DialogArtifactsError>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + ConditionalSync,
    I: Stream<Item = Instruction> + ConditionalSend,
{
    let batch = BufferedBatch::apply(tree, store, version, instructions).await?;
    let changed = batch.changed();
    *tree = batch.seal(store, delta, canonicalize).await?;
    Ok(changed)
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_search_tree::Delta;
    use dialog_storage::{CborEncoder, MemoryStorageBackend, Storage, StorageBackend as _};
    use futures_util::stream;

    use super::{BufferedBatch, apply_buffered};
    use crate::history::{Edition, Origin, Version};
    use crate::key::{FromKey as _, default_manifest};
    use crate::tree::{ArtifactTree, ArtifactTreeExt as _};
    use crate::{Artifact, AttributeKey, Datum, EntityKey, Instruction, State, Value};

    fn store() -> Storage<CborEncoder, MemoryStorageBackend<[u8; 32], Vec<u8>>> {
        Storage {
            encoder: CborEncoder,
            backend: MemoryStorageBackend::default(),
        }
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

        let mut store = store();
        let mut tree = ArtifactTree::empty();
        for i in 0..COMMITS {
            let mut delta = Delta::zero();
            apply_buffered(
                &mut tree,
                &mut store,
                &mut delta,
                Some(Version::new(
                    Origin::from([7u8; 32]),
                    Edition::new(i as u64 + 1),
                )),
                stream::iter(vec![assert_of(&format!("user:{i}"), "resident")]),
                false,
            )
            .await?;
            for (hash, buffer) in delta.flush() {
                use dialog_storage::StorageBackend as _;
                store
                    .set(*hash.as_bytes(), buffer.as_ref().to_vec())
                    .await?;
            }
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
        use dialog_storage::StorageBackend as _;

        let mut store = store();
        let mut tree = ArtifactTree::empty();

        // Enough commits that the spine grows and the buffers matter, each one
        // buffering its batch and then writing a record entry, as a commit does.
        const COMMITS: usize = 300;
        for i in 0..COMMITS {
            let mut delta = Delta::zero();
            apply_buffered(
                &mut tree,
                &mut store,
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
            let entity_key = crate::EntityKey::from_artifact(&artifact, &default_manifest());
            let attribute_key = crate::AttributeKey::from_key(&entity_key);
            let added = crate::State::Added(crate::Datum::for_artifact(&artifact));
            tree.record(
                &mut store,
                &mut delta,
                vec![
                    (entity_key.into_key(), added.clone()),
                    (attribute_key.into_key(), added),
                ],
            )
            .await?;

            for (hash, buffer) in delta.flush() {
                store
                    .set(*hash.as_bytes(), buffer.as_ref().to_vec())
                    .await?;
            }
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

        let mut direct_store = store();
        let mut direct = ArtifactTree::empty();
        for batch in batches() {
            let mut delta = Delta::zero();
            direct
                .apply_versioned(&mut direct_store, &mut delta, None, stream::iter(batch))
                .await?;
            for (hash, buffer) in delta.flush() {
                use dialog_storage::StorageBackend as _;
                direct_store
                    .set(*hash.as_bytes(), buffer.as_ref().to_vec())
                    .await?;
            }
        }

        let mut buffered_store = store();
        let mut buffered = ArtifactTree::empty();
        for batch in batches() {
            let mut delta = Delta::zero();
            apply_buffered(
                &mut buffered,
                &mut buffered_store,
                &mut delta,
                None,
                stream::iter(batch),
                true,
            )
            .await?;
            for (hash, buffer) in delta.flush() {
                use dialog_storage::StorageBackend as _;
                buffered_store
                    .set(*hash.as_bytes(), buffer.as_ref().to_vec())
                    .await?;
            }
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
            let mut buffered_store = store();
            let mut tree = ArtifactTree::empty();
            let mut delta = Delta::zero();
            let changed = apply_buffered(
                &mut tree,
                &mut buffered_store,
                &mut delta,
                None,
                stream::iter(vec![
                    assert_of("user:1", "resident"),
                    retract_of("user:1", "resident"),
                ]),
                canonicalize,
            )
            .await?;
            for (hash, buffer) in delta.flush() {
                buffered_store
                    .set(*hash.as_bytes(), buffer.as_ref().to_vec())
                    .await?;
            }

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
                let mut direct_store = store();
                let mut direct = ArtifactTree::empty();
                let mut direct_delta = Delta::zero();
                direct
                    .apply_versioned(
                        &mut direct_store,
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
            let entity_key = EntityKey::from_artifact(&artifact, &default_manifest());
            let attribute_key = AttributeKey::from_key(&entity_key);
            let added = State::Added(Datum::for_artifact(&artifact));
            vec![
                (entity_key.into_key(), added.clone()),
                (attribute_key.into_key(), added),
            ]
        }

        // The canonical reference: data through the canonical edit path, then
        // the record through the canonical `record` surface.
        let mut direct_store = store();
        let mut direct = ArtifactTree::empty();
        let mut direct_delta = Delta::zero();
        direct
            .apply_versioned(
                &mut direct_store,
                &mut direct_delta,
                Some(version()),
                stream::iter(data()),
            )
            .await?;
        direct
            .record(&mut direct_store, &mut direct_delta, entries())
            .await?;

        // The batch surface, sealed canonical: the same fact set must land on
        // the byte-identical root.
        let mut batch_store = store();
        let base = ArtifactTree::empty();
        let mut delta = Delta::zero();
        let batch = BufferedBatch::apply(
            &base,
            &mut batch_store,
            Some(version()),
            stream::iter(data()),
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
        let mut buffered_store = store();
        let mut delta = Delta::zero();
        let batch = BufferedBatch::apply(
            &ArtifactTree::empty(),
            &mut buffered_store,
            Some(version()),
            stream::iter(data()),
        )
        .await?;
        let batch = batch.record(&buffered_store, entries()).await?;
        let sealed = batch.seal(&buffered_store, &mut delta, false).await?;
        for (hash, buffer) in delta.flush() {
            buffered_store
                .set(*hash.as_bytes(), buffer.as_ref().to_vec())
                .await?;
        }
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
}
