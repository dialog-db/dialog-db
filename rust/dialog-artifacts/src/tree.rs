//! Shared tree-ops on the artifact search tree.
//!
//! Both [`Artifacts`](crate::Artifacts) and the higher-level branch
//! abstractions in `dialog-repository` and `dialog-query` operate on the
//! same EAV/AEV/VAE search tree. The per-instruction mutation loop and the
//! selector → key-range scan dispatch are identical across all of them, so
//! they live here as an extension trait on [`ArtifactTree`], parameterized
//! over any store that exposes the raw hash-addressed
//! [`StorageBackend<Key = Blake3Hash, Value = Vec<u8>>`].
//!
//! Callers responsible for revisions, upstreams, remote fallback, or any
//! other branch specifics keep that logic on their side and call
//! [`ArtifactTreeExt::apply`] / [`ArtifactTreeExt::scan`] for the actual
//! key writes and range scans. Mutations accumulate in the tree's delta;
//! callers must flush and persist the buffers when they mint a revision.
//!
//! The tree stores raw fixed-size key bytes and CBOR-encoded values:
//! [`Key`] is a transparent newtype over [`KeyBytes`] and passes through
//! unchanged, while [`State<Datum>`] crosses the boundary through
//! [`encode_state`] / [`decode_state`].
//!
//! `ArtifactTree` is a type alias for a `dialog_search_tree::Tree`, so the
//! orphan rule rules out inherent methods — the operations are exposed as
//! an extension trait instead.

use std::ops::Range;

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSend, ConditionalSync};
use dialog_search_tree::{ContentAddressedStorage, Entry, Tree};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::{Stream, StreamExt};

use crate::{
    Artifact, ArtifactSelector, AttributeKey, Datum, DialogArtifactsError, EntityKey, FromKey,
    Instruction, Key, KeyBytes, KeyView, KeyViewConstruct, KeyViewMut, MatchCandidate, State,
    ValueKey, selector::Constrained,
};

/// The concrete search-tree type the artifact indexes use.
///
/// Keys are the raw fixed-size bytes of [`Key`]; values are CBOR-encoded
/// [`State<Datum>`] blocks.
pub type ArtifactTree = Tree<KeyBytes, Vec<u8>>;

/// Adapts a [`StorageBackend`] keyed by raw `[u8; 32]` hashes (the
/// [`dialog_storage::Blake3Hash`] alias used throughout the artifact
/// stores) to the [`dialog_common::Blake3Hash`] newtype keys the search
/// tree addresses nodes by. The conversion is a transparent byte copy.
#[derive(Clone, Debug)]
pub struct TreeStorageBridge<S>(pub S);

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> StorageBackend for TreeStorageBridge<S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    type Key = NodeHash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.0.set(*key.as_bytes(), value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.0.get(key.as_bytes()).await
    }
}

/// Encodes a [`State`] for storage as a tree value.
pub fn encode_state(state: &State<Datum>) -> Result<Vec<u8>, DialogArtifactsError> {
    serde_ipld_dagcbor::to_vec(state)
        .map_err(|error| DialogArtifactsError::InvalidValue(format!("{error}")))
}

/// Decodes a tree value back into a [`State`].
pub fn decode_state(bytes: &[u8]) -> Result<State<Datum>, DialogArtifactsError> {
    serde_ipld_dagcbor::from_slice(bytes)
        .map_err(|error| DialogArtifactsError::InvalidValue(format!("{error}")))
}

/// Shared mutation + scan operations on an [`ArtifactTree`].
///
/// An extension trait rather than inherent methods because
/// `ArtifactTree` aliases a foreign `dialog_search_tree::Tree` — the
/// orphan rule forbids `impl ArtifactTree { .. }`. Uses
/// `#[async_trait]` (matching [`ArtifactStore`](crate::ArtifactStore))
/// so the async `apply` desugars to a boxed future rather than a
/// bound-less native `async fn`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
    /// choosing a base tree root, persisting a `Revision`, flushing
    /// the tree's delta, etc.
    async fn apply<S, I>(
        &mut self,
        store: &mut S,
        instructions: I,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 'static,
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
    /// pin its root); `store` is the storage backing it.
    fn scan<'s, S>(
        self,
        store: S,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's + ConditionalSend
    where
        Self: Sized,
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's
            + 'static;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ArtifactTreeExt for ArtifactTree {
    async fn apply<S, I>(
        &mut self,
        store: &mut S,
        instructions: I,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 'static,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));

        tokio::pin!(instructions);
        while let Some(instruction) = instructions.next().await {
            match instruction {
                Instruction::Assert(artifact) => {
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    let datum = Datum::from(artifact);
                    let added = encode_state(&State::Added(datum))?;
                    *self = self
                        .insert(entity_key.into_key().into(), added.clone(), &storage)
                        .await?;
                    *self = self
                        .insert(attribute_key.into_key().into(), added.clone(), &storage)
                        .await?;
                    *self = self
                        .insert(value_key.into_key().into(), added, &storage)
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
                        let search_stream = self.stream_range(
                            Range {
                                start: KeyBytes::from(search_start),
                                end: KeyBytes::from(search_end),
                            },
                            &storage,
                        );
                        tokio::pin!(search_stream);
                        while let Some(candidate) = search_stream.next().await {
                            let candidate = candidate?;
                            if let State::Added(current_element) = decode_state(&candidate.value)? {
                                let current = Artifact::try_from(current_element)?;
                                if current.is == artifact.is {
                                    found_same_value = true;
                                } else {
                                    superseded_keys.push(Key::from(candidate.key));
                                }
                            }
                        }
                    }

                    for key in superseded_keys {
                        let entity_key = EntityKey(key);
                        let value_key = ValueKey::from_key(&entity_key);
                        let attribute_key = AttributeKey::from_key(&entity_key);

                        *self = self
                            .delete(&entity_key.into_key().into(), &storage)
                            .await?;
                        *self = self.delete(&value_key.into_key().into(), &storage).await?;
                        *self = self
                            .delete(&attribute_key.into_key().into(), &storage)
                            .await?;
                    }

                    if found_same_value {
                        continue;
                    }

                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);
                    let datum = Datum::from(artifact);
                    let added = encode_state(&State::Added(datum))?;
                    *self = self
                        .insert(entity_key.into_key().into(), added.clone(), &storage)
                        .await?;
                    *self = self
                        .insert(attribute_key.into_key().into(), added.clone(), &storage)
                        .await?;
                    *self = self
                        .insert(value_key.into_key().into(), added, &storage)
                        .await?;
                }
                Instruction::Retract(artifact) => {
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    let removed = encode_state(&State::Removed)?;
                    *self = self
                        .insert(entity_key.into_key().into(), removed.clone(), &storage)
                        .await?;
                    *self = self
                        .insert(attribute_key.into_key().into(), removed.clone(), &storage)
                        .await?;
                    *self = self
                        .insert(value_key.into_key().into(), removed, &storage)
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn scan<'s, S>(
        self,
        store: S,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 's + ConditionalSend
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync
            + 's
            + 'static,
    {
        let tree = self;
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
        try_stream! {
            let range = if selector.entity().is_some() {
                Range {
                    start: KeyBytes::from(
                        <EntityKey<Key> as KeyViewConstruct>::min()
                            .apply_selector(&selector)
                            .into_key(),
                    ),
                    end: KeyBytes::from(
                        <EntityKey<Key> as KeyViewConstruct>::max()
                            .apply_selector(&selector)
                            .into_key(),
                    ),
                }
            } else if selector.value().is_some() {
                Range {
                    start: KeyBytes::from(
                        <ValueKey<Key> as KeyViewConstruct>::min()
                            .apply_selector(&selector)
                            .into_key(),
                    ),
                    end: KeyBytes::from(
                        <ValueKey<Key> as KeyViewConstruct>::max()
                            .apply_selector(&selector)
                            .into_key(),
                    ),
                }
            } else if selector.attribute().is_some() {
                Range {
                    start: KeyBytes::from(
                        <AttributeKey<Key> as KeyViewConstruct>::min()
                            .apply_selector(&selector)
                            .into_key(),
                    ),
                    end: KeyBytes::from(
                        <AttributeKey<Key> as KeyViewConstruct>::max()
                            .apply_selector(&selector)
                            .into_key(),
                    ),
                }
            } else {
                // `Constrained` guarantees at least one field is set.
                unreachable!("ArtifactSelector will always have at least one field specified")
            };

            let stream = tree.stream_range(range, &storage);
            tokio::pin!(stream);
            for await item in stream {
                let raw = item?;
                let entry = Entry {
                    key: Key::from(raw.key),
                    value: decode_state(&raw.value)?,
                };
                if entry.matches_selector(&selector)
                    && let Entry { value: State::Added(datum), .. } = entry
                {
                    yield Artifact::try_from(datum)?;
                }
            }
        }
    }
}
