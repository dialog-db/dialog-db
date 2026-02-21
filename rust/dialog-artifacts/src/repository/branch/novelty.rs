use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_prolly_tree::{Node, Tree, TreeDifference};
use dialog_storage::Blake3Hash;
use futures_util::Stream;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::Index;
use crate::artifacts::Datum;
use crate::repository::archive::ContentAddressedStore;
use crate::{DialogArtifactsError, Key, State};

/// Create a stream of novel nodes representing local changes since the last sync.
///
/// These are tree nodes that exist in the current tree but not in the base tree.
/// Used during push to send only the new nodes to the remote.
pub fn novelty<Env>(
    base_hash: Blake3Hash,
    current_hash: Blake3Hash,
    env: Arc<Mutex<Env>>,
    catalog: dialog_capability::Capability<archive_fx::Catalog>,
) -> impl Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, DialogArtifactsError>>
where
    Env: Provider<archive_fx::Get> + Provider<archive_fx::Put> + ConditionalSync + 'static,
{
    async_stream::try_stream! {
        let archive = ContentAddressedStore::new(env, catalog);

        let base: Index<Env> = Tree::from_hash(&base_hash, archive.clone())
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to load base tree: {:?}", e))
            })?;

        let current: Index<Env> = Tree::from_hash(&current_hash, archive)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to load current tree: {:?}", e))
            })?;

        let difference = TreeDifference::compute(&base, &current)
            .await
            .map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to compute diff: {:?}", e))
            })?;

        for await node in difference.novel_nodes() {
            yield node.map_err(|e| {
                DialogArtifactsError::Storage(format!("Failed to load node: {:?}", e))
            })?;
        }
    }
}
