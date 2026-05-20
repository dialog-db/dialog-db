//! Shared tree-ops on the artifact prolly tree.
//!
//! Both [`Artifacts`](crate::Artifacts) and the higher-level branch
//! abstractions in `dialog-repository` and `dialog-query` operate on the
//! same EAV/AEV/VAE prolly tree. The per-instruction mutation loop and the
//! selector → key-range scan dispatch are identical across all of them, so
//! they live here as an extension trait on [`ArtifactTree`], parameterized
//! over any [`ContentAddressedStorage<Hash=Blake3Hash, Error=DialogStorageError>`].
//!
//! Callers responsible for revisions, upstreams, remote fallback, or any
//! other branch specifics keep that logic on their side and call
//! [`ArtifactTreeExt::apply`] / [`ArtifactTreeExt::scan`] for the actual
//! key writes and range scans.
//!
//! `ArtifactTree` is a type alias for a `dialog_prolly_tree::Tree`, so the
//! orphan rule rules out inherent methods — the operations are exposed as
//! an extension trait instead.

use std::future::Future;
use std::ops::Range;

use async_stream::try_stream;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_prolly_tree::{Entry, GeometricDistribution, Tree};
use dialog_storage::{Blake3Hash, ContentAddressedStorage, DialogStorageError};
use futures_util::{Stream, StreamExt, TryStreamExt};

use crate::{
    Artifact, ArtifactSelector, AttributeKey, Datum, DialogArtifactsError, EntityKey, FromKey,
    Instruction, Key, KeyView, KeyViewConstruct, KeyViewMut, MatchCandidate, State, ValueKey,
    selector::Constrained,
};

/// The concrete prolly-tree type the artifact indexes use.
pub type ArtifactTree = Tree<GeometricDistribution, Key, State<Datum>, Blake3Hash>;

/// Shared mutation + scan operations on an [`ArtifactTree`].
///
/// An extension trait rather than inherent methods because
/// `ArtifactTree` aliases a foreign `dialog_prolly_tree::Tree` — the
/// orphan rule forbids `impl ArtifactTree { .. }`.
pub trait ArtifactTreeExt {
    /// Drain a stream of [`Instruction`]s into the tree, applying the
    /// same key writes that a branch commit or `Artifacts::commit`
    /// would.
    ///
    /// Each instruction touches all three EAV/AEV/VAE indexes;
    /// `Replace` additionally scans the `(entity, attribute)` range to
    /// supersede any different-valued priors (and skips inserting when
    /// a same-valued prior is already in place — that's the
    /// cardinality-one no-op).
    ///
    /// Callers own everything else: building the change stream,
    /// choosing a base tree root, persisting a `Revision`, etc.
    fn apply<S, I>(
        &mut self,
        store: &mut S,
        instructions: I,
    ) -> impl Future<Output = Result<(), DialogArtifactsError>> + ConditionalSend
    where
        S: ContentAddressedStorage<Hash = Blake3Hash, Error = DialogStorageError> + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend;

    /// Scan the tree for [`Artifact`]s matching the given constrained
    /// selector.
    ///
    /// Picks the EAV/AEV/VAE index based on which field of the
    /// selector is constrained (entity / value / attribute, in that
    /// priority order), then streams the matching key range. Items in
    /// the range that don't fully satisfy the selector and items in
    /// the `Removed` state are filtered out.
    ///
    /// Consumes `self` (the tree is moved into the returned stream to
    /// pin its root); `store` is the [`ContentAddressedStorage`]
    /// backing it.
    fn scan<'s, S>(
        self,
        store: S,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's + ConditionalSend
    where
        Self: Sized,
        S: ContentAddressedStorage<Hash = Blake3Hash, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's;
}

impl ArtifactTreeExt for ArtifactTree {
    fn apply<S, I>(
        &mut self,
        store: &mut S,
        instructions: I,
    ) -> impl Future<Output = Result<(), DialogArtifactsError>> + ConditionalSend
    where
        S: ContentAddressedStorage<Hash = Blake3Hash, Error = DialogStorageError> + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        async move {
            tokio::pin!(instructions);
            while let Some(instruction) = instructions.next().await {
                match instruction {
                    Instruction::Assert(artifact) => {
                        let entity_key = EntityKey::from(&artifact);
                        let value_key = ValueKey::from_key(&entity_key);
                        let attribute_key = AttributeKey::from_key(&entity_key);

                        let datum = Datum::from(artifact);
                        self.set(entity_key.into_key(), State::Added(datum.clone()), store)
                            .await?;
                        self.set(
                            attribute_key.into_key(),
                            State::Added(datum.clone()),
                            store,
                        )
                        .await?;
                        self.set(value_key.into_key(), State::Added(datum), store)
                            .await?;
                    }
                    Instruction::Replace(artifact) => {
                        let entity_key = EntityKey::from(&artifact);

                        // Scan priors at this (entity, attribute).
                        // Same-valued priors already represent the
                        // desired state; only different-valued ones
                        // need superseding.
                        let mut superseded_keys: Vec<Key> = Vec::new();
                        let mut found_same_value = false;
                        {
                            let search_start = <EntityKey<Key> as KeyViewConstruct>::min()
                                .set_entity(entity_key.entity())
                                .set_attribute(entity_key.attribute())
                                .into_key();
                            let search_end = <EntityKey<Key> as KeyViewConstruct>::max()
                                .set_entity(entity_key.entity())
                                .set_attribute(entity_key.attribute())
                                .into_key();
                            let search_stream =
                                self.stream_range(search_start..search_end, store);
                            tokio::pin!(search_stream);
                            while let Some(candidate) = search_stream.try_next().await? {
                                if let State::Added(current_element) = candidate.value {
                                    let current = Artifact::try_from(current_element)?;
                                    if current.is == artifact.is {
                                        found_same_value = true;
                                    } else {
                                        superseded_keys.push(candidate.key);
                                    }
                                }
                            }
                        }

                        for key in superseded_keys {
                            let entity_key = EntityKey(key);
                            let value_key = ValueKey::from_key(&entity_key);
                            let attribute_key = AttributeKey::from_key(&entity_key);

                            self.delete(&entity_key.into_key(), store).await?;
                            self.delete(&value_key.into_key(), store).await?;
                            self.delete(&attribute_key.into_key(), store).await?;
                        }

                        if found_same_value {
                            continue;
                        }

                        let entity_key = EntityKey::from(&artifact);
                        let value_key = ValueKey::from_key(&entity_key);
                        let attribute_key = AttributeKey::from_key(&entity_key);
                        let datum = Datum::from(artifact);
                        self.set(entity_key.into_key(), State::Added(datum.clone()), store)
                            .await?;
                        self.set(
                            attribute_key.into_key(),
                            State::Added(datum.clone()),
                            store,
                        )
                        .await?;
                        self.set(value_key.into_key(), State::Added(datum), store)
                            .await?;
                    }
                    Instruction::Retract(artifact) => {
                        let entity_key = EntityKey::from(&artifact);
                        let value_key = ValueKey::from_key(&entity_key);
                        let attribute_key = AttributeKey::from_key(&entity_key);

                        self.set(entity_key.into_key(), State::Removed, store)
                            .await?;
                        self.set(attribute_key.into_key(), State::Removed, store)
                            .await?;
                        self.set(value_key.into_key(), State::Removed, store)
                            .await?;
                    }
                }
            }
            Ok(())
        }
    }

    fn scan<'s, S>(
        self,
        store: S,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's + ConditionalSend
    where
        S: ContentAddressedStorage<Hash = Blake3Hash, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's,
    {
        let tree = self;
        try_stream! {
            if selector.entity().is_some() {
                let start = <EntityKey<Key> as KeyViewConstruct>::min()
                    .apply_selector(&selector)
                    .into_key();
                let end = <EntityKey<Key> as KeyViewConstruct>::max()
                    .apply_selector(&selector)
                    .into_key();
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
                let start = <ValueKey<Key> as KeyViewConstruct>::min()
                    .apply_selector(&selector)
                    .into_key();
                let end = <ValueKey<Key> as KeyViewConstruct>::max()
                    .apply_selector(&selector)
                    .into_key();
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
                let start = <AttributeKey<Key> as KeyViewConstruct>::min()
                    .apply_selector(&selector)
                    .into_key();
                let end = <AttributeKey<Key> as KeyViewConstruct>::max()
                    .apply_selector(&selector)
                    .into_key();
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
                // `Constrained` guarantees at least one field is set.
                unreachable!("ArtifactSelector will always have at least one field specified")
            }
        }
    }
}
