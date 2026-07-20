//! The spilled-value differential: surfacing spilled value blocks for upload.
//!
//! A value larger than the inline threshold does not live in the key or the
//! payload; its raw bytes are a content-addressed block in the archive block
//! store, keyed by the value's 32-byte reference (written on commit by
//! [`ArtifactTreeExt::apply`](crate::tree::ArtifactTreeExt::apply)). Those
//! blocks are addressed independently of the tree nodes, so the tree-node
//! differential push already runs does not surface them. [`spilled_refs`]
//! mirrors [`blob_changes`](crate::blob_changes): it walks the tree
//! differential and names exactly the spilled value blocks newly referenced
//! between two tree versions, so push can ship them alongside the novel nodes.

use std::collections::HashSet;

use async_stream::try_stream;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSend, ConditionalSync};
use dialog_search_tree::{Change, ContentAddressedStorage, TreeDifference};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::Stream;

use crate::{
    BLOB_KEY_TAG, BlobChange, BlobKey, BlobRecord, Datum, DialogArtifactsError, ENTITY_KEY_TAG,
    EntityKey, Key, KeyView, State,
    tree::{ArtifactTree, TreeStorageBridge},
};

/// One content-addressed block a push must ship to the remote before
/// publishing: a blob-index change or a spilled value block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShipmentRef {
    /// A blob-index change (see [`BlobChange`]); only additions ship bytes.
    Blob(BlobChange),
    /// A spilled value block newly referenced, by its 32-byte reference.
    SpilledValue(Blake3Hash),
}

/// Stream everything a push must ship from ONE walk of an already-computed
/// tree differential: blob-index changes (`BLOB` tag) and newly-referenced
/// spilled value blocks (EAV-tagged additions whose key carries a reference,
/// deduplicated). Push runs the node-level differential anyway to upload
/// novel nodes; draining this from the same [`TreeDifference`] means the
/// changed paths are read once instead of once per concern.
pub fn shipment_refs<'a, Backend>(
    difference: &'a TreeDifference<'a, Key, State<Datum>, Backend>,
) -> impl Stream<Item = Result<ShipmentRef, DialogArtifactsError>> + 'a + ConditionalSend
where
    Backend: StorageBackend<Key = NodeHash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    try_stream! {
        let changes = difference.changes();
        tokio::pin!(changes);
        let mut seen: HashSet<Blake3Hash> = HashSet::new();
        for await change in changes {
            let (entry, removed) = match change? {
                Change::Add(entry) => (entry, false),
                Change::Remove(entry) => (entry, true),
            };
            let key = entry.key;
            match key.tag() {
                BLOB_KEY_TAG => {
                    let hash = BlobKey(key).blob_hash();
                    // Decoding rejects a malformed record; a `None` decode is
                    // a retraction tombstone, a reference only when removed.
                    match (removed, BlobRecord::from_state(&entry.value)?) {
                        (false, Some(_)) => yield ShipmentRef::Blob(BlobChange::Added(hash)),
                        (true, _) => yield ShipmentRef::Blob(BlobChange::Removed(hash)),
                        (false, None) => {}
                    }
                }
                // Count each spilled value once, via the EAV ordering only:
                // a value shared by the EAV/AEV/VAE orderings surfaces once,
                // and the `HashSet` dedups a block shared by many facts.
                ENTITY_KEY_TAG if !removed => {
                    // Only asserted facts need their block shipped. A
                    // retraction writes a TOMBSTONE at the same spilled key:
                    // it is never read through the spill store (readers check
                    // `State::Added` before fetching), and a replica can
                    // legitimately hold a tombstone for a block it never
                    // replicated (pull ships tree nodes, not value blocks) —
                    // requiring the block would wedge that replica's push
                    // forever.
                    if !matches!(entry.value, State::Added(_)) {
                        continue;
                    }
                    let view = EntityKey(&key);
                    let Some(hash) = view.value_spill_hash() else {
                        continue;
                    };
                    let reference: Blake3Hash = hash.try_into().map_err(|_| {
                        DialogArtifactsError::InvalidKey(
                            "spilled value reference is not 32 bytes".to_string(),
                        )
                    })?;
                    if seen.insert(reference) {
                        yield ShipmentRef::SpilledValue(reference);
                    }
                }
                _ => {}
            }
        }
    }
}

/// Stream the spilled value block references newly added between two tree
/// versions, deduplicated.
///
/// Runs the search-tree differential over `base -> current` and keeps the
/// spilled-value half of [`shipment_refs`]. Removals are ignored (their
/// blocks are GC candidates, out of scope for upload). Both trees must be
/// readable from `store`.
pub fn spilled_refs<'s, S>(
    base: ArtifactTree,
    current: ArtifactTree,
    store: S,
) -> impl Stream<Item = Result<Blake3Hash, DialogArtifactsError>> + 's + ConditionalSend
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + Clone
        + ConditionalSync
        + 's,
{
    let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
    try_stream! {
        let difference = TreeDifference::compute(&base, &current, &storage, &storage).await?;
        let refs = shipment_refs(&difference);
        tokio::pin!(refs);
        for await item in refs {
            if let ShipmentRef::SpilledValue(reference) = item? {
                yield reference;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::tree::ArtifactTreeExt;
    use crate::{Artifact, Instruction, Value};
    use dialog_search_tree::{Buffer, Delta};
    use dialog_storage::MemoryStorageBackend;
    use futures_util::{TryStreamExt, stream};

    async fn flush(
        store: &mut MemoryStorageBackend<Blake3Hash, Vec<u8>>,
        delta: &mut Delta<dialog_common::Blake3Hash, Buffer>,
    ) -> Result<(), DialogArtifactsError> {
        for (_, buffer) in delta.flush() {
            store
                .set(*buffer.blake3_hash().as_bytes(), buffer.as_ref().to_vec())
                .await?;
        }
        Ok(())
    }

    #[dialog_common::test]
    async fn it_surfaces_each_spilled_value_once() -> Result<(), DialogArtifactsError> {
        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let value = Value::String("z".repeat(inline_n + 1));
        let reference = value.to_reference();

        let mut store = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let mut delta = Delta::zero();

        // Base: empty.
        let base = ArtifactTree::empty();

        // Current: one spilling fact (touches all three EAV/AEV/VAE orderings).
        let mut current = base.clone();
        current
            .apply(
                &mut store,
                &mut delta,
                stream::iter(vec![Instruction::Assert(Artifact {
                    the: "doc/body".parse().unwrap(),
                    of: "doc:1".parse().unwrap(),
                    is: value.clone(),
                    cause: None,
                })]),
            )
            .await?;
        flush(&mut store, &mut delta).await?;

        let refs: Vec<_> = spilled_refs(base, current, store).try_collect().await?;
        assert_eq!(
            refs,
            vec![reference],
            "a spilled value shared by EAV/AEV/VAE surfaces exactly once"
        );
        Ok(())
    }

    /// Retracting a spilled fact writes tombstones at the same spilled keys;
    /// those additions must NOT surface the value reference. A tombstone is
    /// never read through the spill store, and a replica can legitimately
    /// hold one for a block it never replicated (pull ships tree nodes, not
    /// value blocks) — requiring the block at push would wedge that replica's
    /// push forever.
    #[dialog_common::test]
    async fn it_ignores_tombstones_at_spilled_keys() -> Result<(), DialogArtifactsError> {
        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let artifact = Artifact {
            the: "doc/body".parse().unwrap(),
            of: "doc:1".parse().unwrap(),
            is: Value::String("z".repeat(inline_n + 1)),
            cause: None,
        };

        let mut store = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let mut delta = Delta::zero();

        // Base: the spilled fact is asserted.
        let mut base = ArtifactTree::empty();
        base.apply(
            &mut store,
            &mut delta,
            stream::iter(vec![Instruction::Assert(artifact.clone())]),
        )
        .await?;
        flush(&mut store, &mut delta).await?;

        // Current: the fact is retracted (tombstones at the spilled keys).
        let mut current = base.clone();
        current
            .apply(
                &mut store,
                &mut delta,
                stream::iter(vec![Instruction::Retract(artifact)]),
            )
            .await?;
        flush(&mut store, &mut delta).await?;

        let refs: Vec<_> = spilled_refs(base, current, store).try_collect().await?;
        assert!(
            refs.is_empty(),
            "a retraction ships no spilled blocks: {refs:?}"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_ignores_inline_values() -> Result<(), DialogArtifactsError> {
        let mut store = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
        let mut delta = Delta::zero();

        let base = ArtifactTree::empty();
        let mut current = base.clone();
        current
            .apply(
                &mut store,
                &mut delta,
                stream::iter(vec![Instruction::Assert(Artifact {
                    the: "user/name".parse().unwrap(),
                    of: "user:1".parse().unwrap(),
                    is: Value::String("Alice".to_string()),
                    cause: None,
                })]),
            )
            .await?;
        flush(&mut store, &mut delta).await?;

        let refs: Vec<_> = spilled_refs(base, current, store).try_collect().await?;
        assert!(refs.is_empty(), "inline values surface no spilled refs");
        Ok(())
    }
}
