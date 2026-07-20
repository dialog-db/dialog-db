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
    Buffer, ContentAddressedStorage, Delta, DialogSearchTreeError, Entry, HitchhikerTree,
    TransientTree,
};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::Stream;
use std::ops::RangeInclusive;

use crate::history::Version;
use crate::tree::{ArtifactTree, TreeStorageBridge, write_instructions};
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

/// Applies `instructions` to `tree` through the write buffer, returning whether
/// the batch changed the indexes.
///
/// The buffered counterpart of
/// [`ArtifactTreeExt::apply_versioned`](crate::ArtifactTreeExt::apply_versioned),
/// running the very same instruction semantics (they share
/// [`write_instructions`](crate::tree::write_instructions)). The difference is
/// where the writes land: instead of reshaping the tree per batch, they
/// accumulate in bounded per-node buffers, and the reshape happens only when a
/// buffer overflows and cascades.
///
/// With `canonicalize` the buffers are flushed to the leaves before returning,
/// so `tree` ends as the deterministic canonical form of its fact set. Without
/// it the buffers are left in place and `tree` is the buffered form.
///
/// **Both forms are publishable.** A node's hash covers its buffers as well as
/// its links, so a buffered root identifies its content exactly: it reads
/// (buffers merge over stored entries), diffs (the differential is
/// novelty-aware), and pushes (its blocks carry the ops) like any other root.
/// What the buffered form gives up is *canonicality*, meaning two replicas with
/// the same facts hash differently if they flushed at different points. That
/// costs convergence detection, not correctness: a fast-forward check compares
/// roots, so such replicas fail to recognize each other as equal and do merge
/// work that finds nothing.
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
    let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
    let (buffered, changed) = write_instructions(
        HitchhikerTree::open(tree),
        store,
        &storage,
        version,
        instructions,
    )
    .await?;
    *tree = if canonicalize {
        buffered.canonicalize(&storage, delta).await?
    } else {
        // Serialize the spine with its buffers intact and seal the resulting
        // root: the hash covers the buffered ops, so this is a complete
        // identity for the tree's content.
        let root = buffered.persist(delta)?;
        ArtifactTree::from_hash_with_cache(root, tree.node_cache())
    };
    Ok(changed)
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_search_tree::Delta;
    use dialog_storage::{CborEncoder, MemoryStorageBackend, Storage};
    use futures_util::stream;

    use super::apply_buffered;
    use crate::history::{Edition, Origin, Version};
    use crate::tree::{ArtifactTree, ArtifactTreeExt as _};
    use crate::{Artifact, Instruction, Value};

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
            let entity_key = crate::EntityKey::from(&artifact);
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
}
