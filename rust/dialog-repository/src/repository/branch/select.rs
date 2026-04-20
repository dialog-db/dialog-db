use crate::repository::memory::MemoryExt;
use crate::repository::remote::address::RemoteSite;
use dialog_capability::Fork;
use dialog_capability::Subject;
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::archive::prelude::ArchiveSubjectExt as _;
use dialog_effects::memory as memory_fx;
use dialog_prolly_tree::{EMPT_TREE_HASH, Entry, Tree};
use dialog_storage::{Blake3Hash, ContentAddressedStorage, DialogStorageError};
use futures_util::Stream;
use std::ops::Range;

use super::{Branch, Index};
use crate::repository::archive::ArchiveExt as _;
use crate::repository::archive::networked::NetworkedIndex;
use crate::repository::branch::upstream::UpstreamState;
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

    fn catalog(&self) -> Capability<archive_fx::Catalog> {
        Subject::from(self.branch.subject().clone())
            .archive()
            .index()
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
            + Provider<Fork<RemoteSite, archive_fx::Get>>
            + Provider<Fork<RemoteSite, memory_fx::Resolve>>
            + ConditionalSync
            + 'static,
    {
        let remote = match self.branch.upstream() {
            Some(UpstreamState::Remote { name, .. }) => {
                Subject::from(self.branch.subject().clone())
                    .remote(name)
                    .load()
                    .perform(env)
                    .await
                    .ok()
            }
            _ => None,
        };

        let store = NetworkedIndex::new(env, self.catalog(), remote);
        self.execute(store).await
    }

    async fn execute<'s, S>(
        self,
        store: S,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's, DialogArtifactsError>
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
