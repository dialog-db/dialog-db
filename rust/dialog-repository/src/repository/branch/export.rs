use dialog_artifacts::{
    Artifact, DialogArtifactsError, EntityKey, Exporter, Key, KeyViewConstruct, State,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_prolly_tree::{EMPT_TREE_HASH, Entry, Tree};
use futures_util::TryStreamExt;

use crate::{
    Branch, Index, NetworkedIndex, RemoteSite, RepositoryArchiveExt as _, RepositoryMemoryExt,
    Upstream,
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
            .unwrap_or(EMPT_TREE_HASH);

        let tree: Index = Tree::from_hash(&tree_hash, &store).await?;

        let range = <EntityKey<Key> as KeyViewConstruct>::min().into_key()
            ..<EntityKey<Key> as KeyViewConstruct>::max().into_key();

        let stream = tree.stream_range(range, &store);
        tokio::pin!(stream);

        while let Some(entry) = stream.try_next().await? {
            let Entry { value, .. } = entry;
            if let State::Added(datum) = value {
                let artifact = Artifact::try_from(datum)?;
                exporter.write(&artifact).await?;
            }
        }

        exporter.close().await?;

        Ok(())
    }
}
