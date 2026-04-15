use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_prolly_tree::{Node, Tree, TreeDifference};
use dialog_storage::Blake3Hash;
use futures_util::Stream;

use super::Index;
use crate::repository::archive::local::LocalIndex;
use crate::{DialogArtifactsError, Key, State};
use dialog_artifacts::Datum;

/// Create a stream of novel nodes representing local changes since the last sync.
///
/// These are tree nodes that exist in the current tree but not in the base tree.
/// Used during push to send only the new nodes to the remote.
pub fn novelty<'a, Env>(
    base_hash: Blake3Hash,
    current_hash: Blake3Hash,
    env: &'a Env,
    catalog: Capability<archive_fx::Catalog>,
) -> impl Stream<Item = Result<Node<Key, State<Datum>, Blake3Hash>, DialogArtifactsError>> + 'a
where
    Env: Provider<archive_fx::Get> + Provider<archive_fx::Put> + ConditionalSync + 'static,
{
    async_stream::try_stream! {
        let store = LocalIndex::new(env, catalog);

        let base: Index = Tree::from_hash(&base_hash, &store).await?;

        let current: Index = Tree::from_hash(&current_hash, &store).await?;

        let difference = TreeDifference::compute(&base, &current, &store, &store).await?;

        for await node in difference.novel_nodes() {
            yield node?;
        }
    }
}
