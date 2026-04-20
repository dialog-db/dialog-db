use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, AttributeKey, Datum, DialogArtifactsError, EntityKey, Key,
    KeyViewConstruct, KeyViewMut, MatchCandidate, State, ValueKey,
};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::archive::{Catalog, Get, Put};
use dialog_effects::memory::Resolve;
use dialog_prolly_tree::{EMPT_TREE_HASH, Entry, Tree};
use dialog_storage::{Blake3Hash, ContentAddressedStorage, DialogStorageError};
use futures_util::Stream;
use std::ops::Range;

use super::{Branch, Index, UpstreamState};
use crate::repository::archive::RepositoryArchiveExt as _;
use crate::repository::archive::networked::NetworkedIndex;
use crate::repository::error::RepositoryError;
use crate::repository::memory::RepositoryMemoryExt;
use crate::repository::remote::RemoteSite;

/// Command struct for selecting artifacts from a branch.
pub struct Select<'a> {
    branch: &'a Branch,
    selector: ArtifactSelector<Constrained>,
}

impl<'a> Select<'a> {
    /// Create a select command for the given branch and artifact selector.
    pub fn new(branch: &'a Branch, selector: ArtifactSelector<Constrained>) -> Self {
        Self { branch, selector }
    }

    fn tree_hash(&self) -> Blake3Hash {
        self.branch
            .revision()
            .as_ref()
            .map(|rev| *rev.tree.hash())
            .unwrap_or(EMPT_TREE_HASH)
    }

    fn catalog(&self) -> Capability<Catalog> {
        self.branch.subject().archive().index()
    }
}

impl Select<'_> {
    /// Execute the select, using fallback to remote if the branch has
    /// a remote upstream.
    ///
    /// The per-item error type remains [`DialogArtifactsError`] because
    /// stream items surface artifact-decoding errors that the caller may
    /// want to inspect directly.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>>, RepositoryError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let remote = match self.branch.upstream() {
            Some(UpstreamState::Remote { name, .. }) => self
                .branch
                .subject()
                .remote(name)
                .load()
                .perform(env)
                .await
                .ok(),
            _ => None,
        };

        let store = NetworkedIndex::new(env, self.catalog(), remote);
        self.execute(store).await
    }

    async fn execute<'s, S>(
        self,
        store: S,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's, RepositoryError>
    where
        S: ContentAddressedStorage<Hash = Blake3Hash, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's,
    {
        let tree: Index = Tree::from_hash(&self.tree_hash(), &store).await?;

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
