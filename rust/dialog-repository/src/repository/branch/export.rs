use dialog_artifacts::KeyBytes;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{
    Artifact, DialogArtifactsError, EntityKey, Exporter, Key, KeyViewConstruct, State,
};
use dialog_capability::{Fork, Provider};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_search_tree::ContentAddressedStorage as TreeStorage;
use futures_util::TryStreamExt;

use crate::{
    Branch, EMPTY_TREE_HASH, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _,
    RepositoryMemoryExt, Upstream,
};

/// Command struct for exporting all artifacts from a branch.
pub struct Export<'a, E> {
    branch: &'a Branch,
    exporter: E,
}

impl<'a, E> Export<'a, E> {
    pub(super) fn new(branch: &'a Branch, exporter: E) -> Self {
        Self { branch, exporter }
    }
}

impl<E: Exporter> Export<'_, E> {
    /// Execute the export, writing all artifacts to the exporter.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), DialogArtifactsError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let mut exporter = self.exporter;

        let remote = match branch.upstream() {
            Some(Upstream::Remote { remote: name, .. }) => {
                branch.subject().remote(name).load().perform(env).await.ok()
            }
            _ => None,
        };

        let catalog = branch.subject().archive().index();
        let store = NetworkedIndex::new(env, catalog, remote);

        let tree_hash = branch
            .revision()
            .as_ref()
            .map(|rev| *rev.tree.hash())
            .unwrap_or(EMPTY_TREE_HASH);

        let tree = Index::from_hash(NodeHash::from(tree_hash));

        let range = KeyBytes::from(<EntityKey<Key> as KeyViewConstruct>::min().into_key())
            ..=KeyBytes::from(<EntityKey<Key> as KeyViewConstruct>::max().into_key());

        let tree_store = TreeStorage::new(TreeStorageBridge(store));
        let stream = tree.stream_range(range, &tree_store);
        tokio::pin!(stream);

        while let Some(entry) = stream.try_next().await? {
            if let State::Added(datum) = entry.value {
                let artifact = Artifact::try_from(datum)?;
                exporter.write(&artifact).await?;
            }
        }

        exporter.close().await?;

        Ok(())
    }
}
