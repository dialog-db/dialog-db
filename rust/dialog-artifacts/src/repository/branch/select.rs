use dialog_capability::{Did, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_prolly_tree::{Entry, Tree};
use futures_util::Stream;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::state::BranchState;
use super::{Index, archive};
use crate::artifacts::selector::Constrained;
use crate::artifacts::{Artifact, ArtifactSelector, Datum, MatchCandidate};
use crate::repository::archive::ContentAddressedStore;
use crate::{
    AttributeKey, DialogArtifactsError, EntityKey, Key, KeyViewConstruct, KeyViewMut,
    State, ValueKey,
};

/// Command struct for selecting artifacts from a branch.
pub struct Select {
    pub(super) subject: Did,
    pub(super) state: BranchState,
    pub(super) selector: ArtifactSelector<Constrained>,
}

impl Select {
    /// Execute the select operation, returning a stream of matching artifacts.
    ///
    /// Takes `Arc<Mutex<Env>>` because the prolly tree requires an owned
    /// `ContentAddressedStorage` implementation via `ContentAddressedStore`.
    pub async fn perform<Env>(
        self,
        env: Arc<Mutex<Env>>,
    ) -> Result<impl Stream<Item = Result<Artifact, DialogArtifactsError>>, DialogArtifactsError>
    where
        Env: Provider<archive_fx::Get> + Provider<archive_fx::Put> + ConditionalSync + 'static,
    {
        let archive = ContentAddressedStore::new(
            env,
            archive::Archive::new(Subject::from(self.subject.clone())).index(),
        );

        let tree: Index<Env> = Tree::from_hash(self.state.revision.tree.hash(), archive)
            .await
            .map_err(|e| DialogArtifactsError::Storage(format!("Failed to load tree: {:?}", e)))?;

        let selector = self.selector;

        Ok(async_stream::try_stream! {
            if selector.entity().is_some() {
                let start = <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(&selector).into_key();
                let end = <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(&selector).into_key();

                let stream = tree.stream_range(Range { start, end });
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

                let stream = tree.stream_range(Range { start, end });
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

                let stream = tree.stream_range(Range { start, end });
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
