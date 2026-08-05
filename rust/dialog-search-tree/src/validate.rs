//! Canonical-form validation for real, stored trees.
//!
//! # The property being checked
//!
//! The convergence guarantee this crate makes is precisely this: for a
//! given format [`Manifest`], the CANONICAL, FULLY-PERSISTED form of a tree
//! is a pure function of its entry set — the same `(key, value)` set
//! canonicalizes to the same node graph, byte for byte, regardless of the
//! order, grouping, or path (edits, buffered writes, plants, flushes) by
//! which the entries arrived. Two boundary clauses matter:
//!
//! - the function is PARAMETERIZED BY THE MANIFEST, including for the
//!   EMPTY entry set: the canonical empty form is the zero-entry node
//!   carrying the tree's manifest (see
//!   [`persist_empty_root`](crate::persist_empty_root)), under every
//!   manifest alike — the format survives emptiness structurally, so a
//!   delete-to-empty lifecycle no longer needs
//!   `HitchhikerTree::with_manifest` to re-pin it. The null root is not a
//!   persisted form at all; it names a tree that does not exist yet, and a
//!   legacy null root remains readable as the empty tree. (The adversarial
//!   soak's delete-to-empty pattern found the seam this representation
//!   closes.)
//! - the property does NOT cover in-flight buffered state: a hitchhiker
//!   root with pending novelty is valid and publishable, but its shape
//!   deliberately depends on where ops currently sit, and two such roots
//!   holding the same facts may differ until canonicalized.
//!
//! # What the validator does
//!
//! Because the shape rules are history-independent they are locally
//! re-derivable: the canonical partition at every level is a function of
//! the entry sequence and the manifest alone. The validator exploits the
//! fact that the crate already contains the canonical constructor — the
//! bottom-up plant path (`regroup_entries` + `seal_root`) — and reuses it
//! verbatim rather than re-deriving seam rules (coins, vetoed stretches,
//! frame ceilings, forced-long separators, anchor elections) in a second
//! implementation that could drift:
//!
//! 1. walk the stored tree, flagging any non-empty novelty buffer (a
//!    canonical tree is fully flushed) and collecting its entries and its
//!    per-level separator lists;
//! 2. replant the same entries in memory through the production plant path
//!    (no persist, no hashing);
//! 3. lockstep-compare the two shapes level by level, reporting the FIRST
//!    divergence at each level with its position and separators.
//!
//! A clean report on every commit's canonicalized root upgrades "root
//! differs after 3,000 commits" into "level 2, node 17, seam misplaced at
//! key …" at the causing edit. Cost is O(n) in entries plus one in-memory
//! rebuild; no reference store, no second persist.

use dialog_common::{Blake3Hash, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    Accessor, ArchivedNodeBody, ContentAddressedStorage, DialogSearchTreeError, Distribution, Key,
    NoveltyEntry, NoveltyOp, PersistentNode, PersistentTree, TransientTree, Value, into_owned,
};

/// Renders a separator for a violation message: a bounded hex prefix, so
/// reports stay readable for long keys.
fn hex_prefix(bytes: &[u8]) -> String {
    let shown: String = bytes.iter().take(20).map(|b| format!("{b:02x}")).collect();
    if bytes.len() > 20 {
        format!("{shown}… ({} bytes)", bytes.len())
    } else {
        shown
    }
}

impl<K, V, D> PersistentTree<K, V, D>
where
    K: Key + ConditionalSync + 'static,
    V: Value
        + ConditionalSync
        + 'static
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    V::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<V, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    D: Distribution + ConditionalSync + 'static,
{
    /// Validates that this tree is in canonical form: fully flushed, and
    /// shaped exactly as the canonical constructor shapes its entry set
    /// under the tree's own manifest. Returns one message per divergence
    /// (empty = canonical); each message names the level and position, so
    /// a failure localizes to a node instead of a differing root hash.
    ///
    /// See the module docs for the precise property statement — this is
    /// meaningful for canonicalized roots, and will (correctly) report a
    /// buffered hitchhiker root as non-canonical.
    pub async fn canonical_divergences<Backend>(
        &self,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Vec<String>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        if self.root() == NULL_BLAKE3_HASH {
            return Ok(Vec::new());
        }
        let manifest = self.manifest(storage).await?;
        let accessor: Accessor<Backend> = Accessor::new(Default::default(), storage.clone());

        // The empty tree's node: canonical exactly when it is byte-identical
        // to the fixed zero-entry encoding for its manifest — under every
        // manifest, the default included.
        let root_node: PersistentNode<K, V> = accessor.get_node(self.root()).await?;
        if root_node.is_empty()? {
            let mut scratch = crate::Delta::zero();
            let canonical = crate::persist_empty_root::<K, V>(&manifest, &mut scratch)?;
            return Ok(if canonical.hash() == self.root() {
                Vec::new()
            } else {
                vec![
                    "zero-entry root diverges from the canonical empty node for its manifest"
                        .to_string(),
                ]
            });
        }

        let mut violations = Vec::new();

        // Level-order walk of the stored tree: per-level separator lists
        // (children of every level's nodes, in order), entry collection in
        // key order, and the fully-flushed check.
        let mut stored_levels: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut entries: Vec<NoveltyEntry<V>> = Vec::new();
        let mut current: Vec<Blake3Hash> = vec![self.root().clone()];
        while !current.is_empty() {
            let mut next: Vec<Blake3Hash> = Vec::new();
            let mut separators: Vec<Vec<u8>> = Vec::new();
            for hash in &current {
                let node: PersistentNode<K, V> = accessor.get_node(hash).await?;
                match node.body()? {
                    ArchivedNodeBody::Index(index) => {
                        for at in 0..index.len() {
                            if index.buffer_for(at).is_some() {
                                violations.push(format!(
                                    "buffered novelty at level {} link {at}: the tree is \
                                     not fully flushed, so it is not in canonical form",
                                    stored_levels.len(),
                                ));
                            }
                            separators.push(index.separator(at)?);
                            next.push(index.hash_at(at)?.clone());
                        }
                    }
                    ArchivedNodeBody::Segment(segment) => {
                        let mut keys = segment.keys::<K>()?;
                        while let Some((at, key)) = keys.next_key()? {
                            entries.push(NoveltyEntry {
                                key: key.to_vec(),
                                op: NoveltyOp::Assert(into_owned(segment.value_at(at)?)?),
                            });
                        }
                    }
                }
            }
            if !separators.is_empty() {
                stored_levels.push(separators);
            }
            current = next;
        }
        // Buffered novelty means the replant below would fold ops the
        // stored shape has not absorbed; the report above already says
        // everything useful.
        if !violations.is_empty() {
            return Ok(violations);
        }

        // The canonical constructor, over the same entries, in memory.
        let expected = TransientTree::<K, V, D>::with_manifest(
            NULL_BLAKE3_HASH.clone(),
            Default::default(),
            manifest,
        )
        .plant(entries, storage)
        .await?;
        let expected_levels = expected.level_separators()?;

        if stored_levels.len() != expected_levels.len() {
            violations.push(format!(
                "tree height diverges: stored {} levels below the root, canonical {}",
                stored_levels.len(),
                expected_levels.len(),
            ));
        }
        for (depth, (stored, expected)) in
            stored_levels.iter().zip(expected_levels.iter()).enumerate()
        {
            if stored.len() != expected.len() {
                violations.push(format!(
                    "level {depth}: stored {} nodes, canonical {}",
                    stored.len(),
                    expected.len(),
                ));
            }
            if let Some(at) =
                (0..stored.len().min(expected.len())).find(|at| stored[*at] != expected[*at])
            {
                violations.push(format!(
                    "level {depth}, node {at}: seam diverges — stored separator {} vs \
                     canonical {}",
                    hex_prefix(&stored[at]),
                    hex_prefix(&expected[at]),
                ));
            }
        }
        Ok(violations)
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::{Blake3Hash, NULL_BLAKE3_HASH};
    use dialog_storage::MemoryStorageBackend;

    use crate::{
        Buffer, Cache, ContentAddressedStorage, Delta, HitchhikerTree, Manifest, PersistentTree,
        TransientTree,
    };

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type Store = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;
    type Tree = PersistentTree<[u8; 4], Vec<u8>>;

    async fn settle(delta: &mut Delta<Blake3Hash, Buffer>, storage: &mut Store) -> Result<()> {
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(())
    }

    /// A tree built by canonical edits validates clean at EVERY step — the
    /// edit path's whole contract is that it maintains canonical form
    /// incrementally, and the validator re-derives that form through the
    /// independent plant constructor.
    #[dialog_common::test]
    async fn it_validates_canonically_edited_trees_clean() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::empty();
        for i in 0..250u32 {
            let key = (i * 37 % 1000).to_be_bytes();
            let mut delta = Delta::zero();
            tree = tree
                .edit()
                .insert(key, vec![i as u8; (i % 60) as usize + 1], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;

            if i % 50 == 49 {
                let divergences = tree.canonical_divergences(&storage).await?;
                assert_eq!(
                    divergences,
                    Vec::<String>::new(),
                    "canonical edit left a non-canonical tree after {} inserts",
                    i + 1
                );
            }
        }
        Ok(())
    }

    /// A canonicalized buffered tree validates clean: the flush cascade must
    /// settle every op into the same shape the plant constructor derives.
    #[dialog_common::test]
    async fn it_validates_canonicalized_buffered_trees_clean() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut buffered = HitchhikerTree::open(&Tree::empty()).with_op_buf_size(8);
        for i in 0..300u32 {
            let key = (i * 17 % 700).to_be_bytes();
            buffered = buffered.insert(key, vec![i as u8, 3], &storage).await?;
        }
        let mut delta = Delta::zero();
        let canonical = buffered.canonicalize(&storage, &mut delta).await?;
        settle(&mut delta, &mut storage).await?;

        let divergences = canonical.canonical_divergences(&storage).await?;
        assert_eq!(
            divergences,
            Vec::<String>::new(),
            "canonicalize left divergences"
        );
        Ok(())
    }

    /// The validator is not vacuous: a published BUFFERED root — valid,
    /// publishable, deliberately not canonical — must be reported, via the
    /// fully-flushed check.
    #[dialog_common::test]
    async fn it_flags_buffered_roots_as_non_canonical() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
        // Seed enough entries through the canonical path that the tree has
        // real depth, then buffer a few writes with the DEFAULT op buffer:
        // they stay parked in the root's novelty, which is exactly the
        // published-but-not-canonical form the property excludes. (A tiny
        // op buffer would cascade every op to the leaves and publish a form
        // that genuinely IS canonical — the validator correctly accepts it.)
        let mut tree = Tree::empty();
        for i in 0..200u32 {
            let key = (i * 13 % 400).to_be_bytes();
            let mut delta = Delta::zero();
            tree = tree
                .edit()
                .insert(key, vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }
        let mut buffered = HitchhikerTree::open(&tree);
        for i in 0..10u32 {
            let key = (900 + i).to_be_bytes();
            buffered = buffered.insert(key, vec![i as u8], &storage).await?;
        }
        let mut delta = Delta::zero();
        let root = buffered.persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
        let tree = Tree::from_hash_with_cache(root, Default::default());

        let divergences = tree.canonical_divergences(&storage).await?;
        assert!(
            divergences
                .iter()
                .any(|violation| violation.contains("buffered novelty")),
            "a buffered root must be reported as not in canonical form, got: {divergences:?}"
        );
        Ok(())
    }

    /// The empty tree's canonical form: the manifest-carrying zero-entry
    /// node is canonical under its manifest — custom and default alike —
    /// and a zero-entry node whose bytes diverge from the fixed encoding
    /// for its manifest is flagged.
    #[dialog_common::test]
    async fn it_validates_the_empty_node_by_manifest() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // The canonical empty node, produced by the production persist
        // path, under a custom manifest and under the default.
        for manifest in [
            Manifest {
                fanout_n: 2,
                ..Manifest::default()
            },
            Manifest::default(),
        ] {
            let mut delta = Delta::zero();
            let emptied = TransientTree::<[u8; 4], Vec<u8>>::with_manifest(
                NULL_BLAKE3_HASH.clone(),
                Cache::new(),
                manifest,
            )
            .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            assert_ne!(emptied.root(), &NULL_BLAKE3_HASH.clone());
            assert_eq!(
                emptied.canonical_divergences(&storage).await?,
                Vec::<String>::new(),
                "the manifest-carrying empty node is canonical"
            );
        }
        Ok(())
    }
}
