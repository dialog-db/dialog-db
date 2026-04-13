use dialog_artifacts::Exporter;
use dialog_artifacts::{Artifact, DialogArtifactsError, EntityKey, Key, KeyViewConstruct, State};
use dialog_capability::fork::Fork;
use dialog_capability::{Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::{Entry, Tree};
use dialog_remote_s3::S3;
use futures_util::TryStreamExt;

use super::{Branch, Index};
use crate::repository::archive::Archive;
use crate::repository::archive::fallback::FallbackStore;
use crate::repository::branch::state::UpstreamState;

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
        Env: Provider<archive_fx::Get>
            + Provider<archive_fx::Put>
            + Provider<memory_fx::Resolve>
            + Provider<Fork<S3, archive_fx::Get>>
            + Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, archive_fx::Get>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
            + ConditionalSync
            + 'static,
    {
        let branch = self.branch;
        let mut exporter = self.exporter;

        let remote = match branch.upstream() {
            Some(UpstreamState::Remote { name, .. }) => {
                branch.remote(name).load().perform(env).await.ok()
            }
            _ => None,
        };

        let catalog = Archive::new(Subject::from(branch.subject().clone())).index();
        let store = FallbackStore::new(env, catalog, remote);

        let tree_hash = branch
            .revision()
            .as_ref()
            .map(|rev| *rev.tree().hash())
            .unwrap_or(dialog_prolly_tree::EMPT_TREE_HASH);

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
