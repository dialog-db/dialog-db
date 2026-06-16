//! An artifact-level novelty buffer over an [`ArtifactTree`].
//!
//! [`BufferedArtifactTree`] wraps the generic
//! [`Buffered`](dialog_search_tree::Buffered) with the artifact key/value
//! types and the EAV/AEV/VAE index derivation that lives on
//! [`ArtifactTreeExt`]. It is the hitchhiker tree of the novelty-buffer design:
//! a canonical base [`ArtifactTree`] plus a small sorted buffer of pending
//! artifact-level ops held at the root.
//!
//! The buffer stores EAV-sorted facts only, one entry per fact, keyed by the
//! artifact's [`EntityKey`] bytes. The AEV/VAE index keys are derived locally,
//! exactly as [`ArtifactTreeExt::apply`] does, at flush time (when the buffer
//! drains into the base) and at read time (when the buffer is merged over a base
//! scan). This keeps a write to a single buffer insert and keeps the base nodes
//! byte-stable until a flush.
//!
//! See `notes/novelty-buffer.md` for the full design.

use async_stream::try_stream;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSync};
use dialog_search_tree::{
    Buffer, Buffered, ContentAddressedStorage, Delta, DialogSearchTreeError, Op,
};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::Stream;
use std::collections::HashSet;

use crate::tree::{ArtifactTree, ArtifactTreeExt, TreeStorageBridge};
use crate::{
    Artifact, ArtifactSelector, AttributeKey, Datum, DialogArtifactsError, EntityKey, FromKey,
    Instruction, Key, KeyBytes, MatchCandidate, State, ValueKey, selector::Constrained, sort_key,
};
use dialog_search_tree::Entry;

/// An [`ArtifactTree`] carrying a root-level novelty buffer of pending
/// artifact-level ops.
///
/// The buffer is EAV-keyed (one op per fact); the AEV/VAE index views are
/// derived at flush and read time. Writes append to the buffer; a flush drains
/// the whole buffer into the base in one batch and resets to an empty buffer.
pub struct BufferedArtifactTree {
    inner: Buffered<KeyBytes, State<Datum>>,
}

impl BufferedArtifactTree {
    /// Creates a buffered artifact tree over `base` with an empty buffer.
    pub fn new(base: ArtifactTree) -> Self {
        Self {
            inner: Buffered::new(base),
        }
    }

    /// The canonical base tree (unaffected by buffering).
    pub fn base(&self) -> &ArtifactTree {
        self.inner.base()
    }

    /// Returns `true` when the buffer holds no pending ops.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of buffered ops.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// The tree hash: the base root hash with the sorted novelty folded in.
    pub fn tree_hash(&self) -> Result<NodeHash, DialogSearchTreeError> {
        self.inner.tree_hash()
    }

    /// Buffers an [`Instruction`](crate::Instruction) as a single EAV-keyed op.
    ///
    /// The op is stored at the artifact's [`EntityKey`] only; the AEV/VAE index
    /// keys are derived from it at flush and read time. Last writer wins per
    /// EAV key.
    ///
    /// `Replace` is buffered the same as `Assert` (the asserted fact is stored).
    /// Cardinality-one supersession of a different-valued prior at the same
    /// `(entity, attribute)` requires reading the base, which the buffered write
    /// path deliberately avoids; that resolution is deferred to flush, where
    /// [`ArtifactTreeExt::apply`](crate::tree::ArtifactTreeExt::apply)-style
    /// supersession would run over the loaded base. In this increment the flush
    /// performs a plain assert of the buffered fact, so the buffered `Replace`
    /// is exactly an `Assert`.
    pub fn write(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::Assert(artifact) | Instruction::Replace(artifact) => {
                let entity_key = EntityKey::from(&artifact);
                let datum = Datum::from(artifact);
                self.inner.write(
                    entity_key.into_key().into(),
                    Op::Assert(State::Added(datum)),
                );
            }
            Instruction::Retract(artifact) => {
                let entity_key = EntityKey::from(&artifact);
                self.inner.write(entity_key.into_key().into(), Op::Retract);
            }
        }
    }

    /// Flushes the whole buffer into the base in one batch, then drains to empty.
    ///
    /// Each buffered EAV fact is expanded into its three EAV/AEV/VAE index keys
    /// and applied to a single transient edit over the base, mirroring
    /// [`ArtifactTreeExt::apply`](crate::tree::ArtifactTreeExt::apply). The
    /// rebuilt nodes are persisted into `delta` (caller-owned), and the buffer
    /// resets over the new base so the tree ends as `(new_base, empty)`.
    pub async fn flush<S>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));
        let mut transient = self.inner.base().edit();

        for (eav_key_bytes, op) in self.inner.novelty() {
            let entity_key = EntityKey(Key::from(*eav_key_bytes));
            let value_key = ValueKey::from_key(&entity_key);
            let attribute_key = AttributeKey::from_key(&entity_key);

            let state: State<Datum> = match op {
                Op::Assert(state) => state.clone(),
                Op::Retract => State::Removed,
            };

            transient = transient
                .insert(entity_key.into_key().into(), state.clone(), &storage)
                .await?;
            transient = transient
                .insert(attribute_key.into_key().into(), state.clone(), &storage)
                .await?;
            transient = transient
                .insert(value_key.into_key().into(), state, &storage)
                .await?;
        }

        let new_base = transient.persist(delta)?;
        self.inner.reset(new_base);
        Ok(())
    }

    /// Scans the buffered tree for [`Artifact`]s matching `selector`, merging the
    /// buffer's pending facts over the base scan.
    ///
    /// The base is scanned exactly as
    /// [`ArtifactTreeExt::scan`](crate::tree::ArtifactTreeExt::scan) does. The
    /// buffer's matching facts are materialized (the buffer is small by design)
    /// into the newer overlay: a buffered retract tombstones the matching base
    /// fact, and a buffered assert shadows or adds it. The merged output is
    /// sorted by [`sort_key`], the one total order consistent with all three
    /// index layouts, so it matches what a single tree containing both would
    /// yield.
    pub fn scan<'s, S>(
        &'s self,
        store: S,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's,
    {
        // Materialize the buffer's overlay for this selector. The buffer is
        // EAV-keyed; for each buffered op that matches the selector, an assert
        // contributes an artifact to overlay and a tombstone at its exact EAV
        // key, and a retract contributes only a tombstone at its EAV key.
        //
        // Two suppression rules apply to the base scan, both because the buffer
        // is newer than the base:
        //
        // - Exact-fact tombstone (EAV key bytes): a buffered op at the exact
        //   same fact (entity, attribute, value) suppresses that base fact. For
        //   a retract this removes it; for an assert this dedups the base's copy
        //   so the buffer's copy stands in its place.
        // - Pair shadow (entity, attribute): a buffered assert at an
        //   (entity, attribute) suppresses base facts at the same pair with a
        //   different value (cardinality-one supersession at read time).
        let mut buffer_asserts: Vec<Artifact> = Vec::new();
        let mut tombstoned_keys: HashSet<KeyBytes> = HashSet::new();
        let mut shadowed_pairs: HashSet<(Vec<u8>, Vec<u8>)> = HashSet::new();

        for (eav_key_bytes, op) in self.inner.novelty() {
            let entity_key = EntityKey(Key::from(*eav_key_bytes));
            let value: State<Datum> = match op {
                Op::Assert(state) => state.clone(),
                Op::Retract => State::Removed,
            };
            let entry = Entry {
                key: entity_key.into_key(),
                value,
            };
            if !entry.matches_selector(&selector) {
                continue;
            }
            tombstoned_keys.insert(*eav_key_bytes);
            match &entry.value {
                State::Added(datum) => {
                    let artifact = match Artifact::try_from(datum.clone()) {
                        Ok(artifact) => artifact,
                        Err(_) => continue,
                    };
                    shadowed_pairs.insert((
                        artifact.the.key_bytes().to_vec(),
                        artifact.of.key_bytes().to_vec(),
                    ));
                    buffer_asserts.push(artifact);
                }
                State::Removed => {}
            }
        }

        buffer_asserts.sort_by_key(sort_key);

        let base = self.inner.base().clone();
        let base_scan = base.scan(store, selector);

        try_stream! {
            tokio::pin!(base_scan);
            // Yield base facts that are neither tombstoned by an identical
            // buffered op nor shadowed by a buffered assert at the same
            // (entity, attribute), interleaved with the buffer's asserts in
            // sort_key order so the merged output is in tree order.
            let mut pending = buffer_asserts.into_iter().peekable();
            for await item in base_scan {
                let base_artifact = item?;
                let base_key = sort_key(&base_artifact);
                while let Some(next) = pending.peek() {
                    if sort_key(next) < base_key {
                        yield pending.next().expect("peeked");
                    } else {
                        break;
                    }
                }
                let base_eav: KeyBytes = EntityKey::from(&base_artifact).into_key().into();
                if tombstoned_keys.contains(&base_eav) {
                    continue;
                }
                let pair = (
                    base_artifact.the.key_bytes().to_vec(),
                    base_artifact.of.key_bytes().to_vec(),
                );
                if shadowed_pairs.contains(&pair) {
                    continue;
                }
                yield base_artifact;
            }
            for artifact in pending {
                yield artifact;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash as NodeHash;
    use dialog_search_tree::{Buffer, Delta};
    use dialog_storage::{Blake3Hash, MemoryStorageBackend, StorageBackend};
    use futures_util::{TryStreamExt, stream};

    use super::BufferedArtifactTree;
    use crate::selector::Constrained;
    use crate::tree::{ArtifactTree, ArtifactTreeExt};
    use crate::{Artifact, ArtifactSelector, Attribute, Entity, Instruction, Value};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type Store = MemoryStorageBackend<Blake3Hash, Vec<u8>>;

    fn entity(name: &str) -> Entity {
        name.parse().expect("valid entity")
    }

    fn attribute(name: &str) -> Attribute {
        name.parse().expect("valid attribute")
    }

    fn artifact(of: &str, the: &str, is: &str) -> Artifact {
        Artifact {
            the: attribute(the),
            of: entity(of),
            is: Value::String(is.into()),
            cause: None,
        }
    }

    /// Persist a delta's pending nodes into the store so subsequent scans can
    /// read them back, mirroring `Artifacts::commit`.
    async fn persist(store: &mut Store, mut delta: Delta<NodeHash, Buffer>) -> Result<()> {
        for (_, buffer) in delta.flush() {
            let digest = *buffer.blake3_hash().as_bytes();
            store.set(digest, buffer.into_vec()).await?;
        }
        Ok(())
    }

    /// Apply instructions to a plain base tree, persisting the result.
    async fn base_with(store: &mut Store, instructions: Vec<Instruction>) -> Result<ArtifactTree> {
        let mut tree = ArtifactTree::empty();
        let mut delta: Delta<NodeHash, Buffer> = Delta::zero();
        tree.apply(store, &mut delta, stream::iter(instructions))
            .await?;
        persist(store, delta).await?;
        Ok(tree)
    }

    async fn collect(
        tree: &BufferedArtifactTree,
        store: &Store,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<Vec<Artifact>> {
        let stream = tree.scan(store.clone(), selector);
        let collected = stream.try_collect::<Vec<_>>().await?;
        Ok(collected)
    }

    #[dialog_common::test]
    async fn it_reads_an_asserted_fact_from_the_buffer_before_flush() -> Result<()> {
        let store = Store::default();
        let mut tree = BufferedArtifactTree::new(ArtifactTree::empty());

        tree.write(Instruction::Assert(artifact(
            "id:alice",
            "test/name",
            "Alice",
        )));

        let results = collect(
            &tree,
            &store,
            ArtifactSelector::new().the(attribute("test/name")),
        )
        .await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of, entity("id:alice"));
        assert_eq!(results[0].is, Value::String("Alice".into()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_tombstones_a_base_fact_with_a_buffered_retract() -> Result<()> {
        let mut store = Store::default();
        let base = base_with(
            &mut store,
            vec![Instruction::Assert(artifact(
                "id:alice",
                "test/name",
                "Alice",
            ))],
        )
        .await?;

        let mut tree = BufferedArtifactTree::new(base);

        // Confirm the base fact is visible before the retract.
        let before = collect(
            &tree,
            &store,
            ArtifactSelector::new().the(attribute("test/name")),
        )
        .await?;
        assert_eq!(before.len(), 1);

        tree.write(Instruction::Retract(artifact(
            "id:alice",
            "test/name",
            "Alice",
        )));

        let after = collect(
            &tree,
            &store,
            ArtifactSelector::new().the(attribute("test/name")),
        )
        .await?;
        assert!(after.is_empty(), "retract should tombstone the base fact");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_shadows_a_different_valued_base_fact() -> Result<()> {
        let mut store = Store::default();
        let base = base_with(
            &mut store,
            vec![Instruction::Assert(artifact(
                "id:alice",
                "test/name",
                "Alice",
            ))],
        )
        .await?;

        let mut tree = BufferedArtifactTree::new(base);
        tree.write(Instruction::Assert(artifact(
            "id:alice",
            "test/name",
            "Alicia",
        )));

        let results = collect(
            &tree,
            &store,
            ArtifactSelector::new().the(attribute("test/name")),
        )
        .await?;
        // The buffered assert at the same (entity, attribute) shadows the
        // different-valued base fact.
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, Value::String("Alicia".into()));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_preserves_the_queryable_view_across_flush() -> Result<()> {
        let mut store = Store::default();
        let mut tree = BufferedArtifactTree::new(ArtifactTree::empty());

        tree.write(Instruction::Assert(artifact(
            "id:alice",
            "test/name",
            "Alice",
        )));
        tree.write(Instruction::Assert(artifact("id:bob", "test/name", "Bob")));
        tree.write(Instruction::Assert(artifact(
            "id:alice",
            "test/role",
            "Engineer",
        )));

        let selector = ArtifactSelector::new().the(attribute("test/name"));
        let before = collect(&tree, &store, selector.clone()).await?;

        let mut delta: Delta<NodeHash, Buffer> = Delta::zero();
        tree.flush(&mut store, &mut delta).await?;
        persist(&mut store, delta).await?;

        assert!(tree.is_empty(), "buffer must drain to empty on flush");

        let after = collect(&tree, &store, selector).await?;
        assert_eq!(
            before, after,
            "scan before flush must equal scan after flush"
        );
        assert_eq!(before.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_flushes_to_the_same_root_as_direct_apply() -> Result<()> {
        // `Instruction` is not `Clone`, so rebuild the same ordered batch twice.
        let instructions = || {
            vec![
                Instruction::Assert(artifact("id:alice", "test/name", "Alice")),
                Instruction::Assert(artifact("id:bob", "test/name", "Bob")),
                Instruction::Assert(artifact("id:alice", "test/role", "Engineer")),
                Instruction::Retract(artifact("id:bob", "test/name", "Bob")),
                Instruction::Assert(artifact("id:carol", "test/name", "Carol")),
            ]
        };

        // Direct apply via ArtifactTreeExt.
        let mut direct_store = Store::default();
        let direct = base_with(&mut direct_store, instructions()).await?;

        // Buffered flush of the same instructions in the same order.
        let mut buffered_store = Store::default();
        let mut tree = BufferedArtifactTree::new(ArtifactTree::empty());
        for instruction in instructions() {
            tree.write(instruction);
        }
        let mut delta: Delta<NodeHash, Buffer> = Delta::zero();
        tree.flush(&mut buffered_store, &mut delta).await?;
        persist(&mut buffered_store, delta).await?;

        assert_eq!(
            tree.base().root(),
            direct.root(),
            "buffered flush root must equal direct apply root"
        );

        Ok(())
    }
}
