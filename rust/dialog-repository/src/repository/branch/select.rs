use dialog_capability::fork::Fork;
use dialog_capability::{Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::{Entry, Tree};
use dialog_remote_s3::S3;
use dialog_storage::{Blake3Hash, ContentAddressedStorage, DialogStorageError};
use futures_util::Stream;
use std::ops::Range;

use super::{Branch, Index};
use crate::repository::archive::Archive;
use crate::repository::archive::fallback::FallbackStore;
use crate::repository::branch::state::UpstreamState;
use crate::{
    AttributeKey, DialogArtifactsError, EntityKey, Key, KeyViewConstruct, KeyViewMut, State,
    ValueKey,
};
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{Artifact, ArtifactSelector, Datum, MatchCandidate};

/// Command struct for selecting artifacts from a branch.
pub struct Select<'a> {
    branch: &'a Branch,
    selector: ArtifactSelector<Constrained>,
}

impl<'a> Select<'a> {
    pub(super) fn new(branch: &'a Branch, selector: ArtifactSelector<Constrained>) -> Self {
        Self { branch, selector }
    }

    fn tree_hash(&self) -> Blake3Hash {
        self.branch
            .revision()
            .as_ref()
            .map(|rev| *rev.tree().hash())
            .unwrap_or(dialog_prolly_tree::EMPT_TREE_HASH)
    }

    pub(crate) fn catalog(&self) -> dialog_capability::Capability<archive_fx::Catalog> {
        Archive::new(Subject::from(self.branch.subject().clone())).index()
    }
}

impl Select<'_> {
    /// Execute the select, using fallback to remote if the branch has
    /// a remote upstream.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>>, DialogArtifactsError>
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
        let remote = match self.branch.upstream() {
            Some(UpstreamState::Remote { name, .. }) => {
                self.branch.remote(name).load().perform(env).await.ok()
            }
            _ => None,
        };

        let store = FallbackStore::new(env, self.catalog(), remote);
        self.execute(store).await
    }

    /// Execute with a custom content-addressed store.
    pub(crate) async fn execute<'s, S>(
        self,
        store: S,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's, DialogArtifactsError>
    where
        S: ContentAddressedStorage<Hash = Blake3Hash, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's,
    {
        let tree: Index = Tree::from_hash(&self.tree_hash(), &store)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Failed to load tree: {:?}", e)))?;

        let selector = self.selector;

        Ok(async_stream::try_stream! {
            if selector.entity().is_some() {
                let start = <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();
                let stream = tree.stream_range(Range { start, end }, &store);
                tokio::pin!(stream);
                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else if selector.value().is_some() {
                let start = <ValueKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <ValueKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();
                let stream = tree.stream_range(Range { start, end }, &store);
                tokio::pin!(stream);
                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else if selector.attribute().is_some() {
                let start = <AttributeKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <AttributeKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();
                let stream = tree.stream_range(Range { start, end }, &store);
                tokio::pin!(stream);
                for await item in stream {
                    let entry: Entry<Key, State<Datum>> = item?;
                    if entry.matches_selector(&selector)
                        && let Entry { value: State::Added(datum), .. } = entry
                    {
                        yield Artifact::try_from(datum)?;
                    }
                }
            } else {
                unreachable!("ArtifactSelector will always have at least one field specified")
            };
        })
    }
}
