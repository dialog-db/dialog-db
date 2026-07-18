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
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_search_tree::{Change, ContentAddressedStorage, TreeDifference};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::Stream;

use crate::{
    DialogArtifactsError, ENTITY_KEY_TAG, EntityKey, KeyView,
    tree::{ArtifactTree, TreeStorageBridge},
};

/// Stream the spilled value block references newly added between two tree
/// versions, deduplicated.
///
/// Runs the search-tree differential over `base -> current`, and for each
/// added entry whose key carries a spilled value, yields the value's 32-byte
/// reference. Only the EAV (`ENTITY_KEY_TAG`) ordering is counted, so a spilled
/// value shared by the EAV/AEV/VAE orderings surfaces once rather than three
/// times; a `HashSet` further dedups a reference shared by many facts, so each
/// distinct spilled block ships exactly once. Removals are ignored (their blocks
/// are GC candidates, out of scope for upload). Both trees must be readable from
/// `store`.
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
        let changes = difference.changes();
        tokio::pin!(changes);
        let mut seen: HashSet<Blake3Hash> = HashSet::new();
        for await change in changes {
            // Only additions are shipped; removed blocks stay on the remote.
            let Change::Add(entry) = change? else {
                continue;
            };
            let key = entry.key;
            // Count each spilled value once, via the EAV ordering only.
            if key.tag() != ENTITY_KEY_TAG {
                continue;
            }
            let view = EntityKey(&key);
            if !view.value_is_spilled() {
                continue;
            }
            let reference: Blake3Hash = view.value_payload().try_into().map_err(|_| {
                DialogArtifactsError::InvalidKey(
                    "spilled value reference is not 32 bytes".to_string(),
                )
            })?;
            if seen.insert(reference) {
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
