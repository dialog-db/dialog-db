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
//! The tree stores raw fixed-size key bytes and rkyv-native values:
//! [`Key`] is a transparent newtype over [`KeyBytes`] and passes through
//! unchanged, while [`State<Datum>`] is the tree's value type directly,
//! serialized into node buffers by the tree itself.
//!
//! `ArtifactTree` is a type alias for a `dialog_search_tree::PersistentTree`, so the
//! orphan rule rules out inherent methods — the operations are exposed as
//! an extension trait instead.

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSend, ConditionalSync};
use dialog_search_tree::{
    Buffer, ContentAddressedStorage, Delta, Entry, PersistentTree, Value as TreeValue,
};
use dialog_storage::{Blake3Hash, DialogStorageError, StorageBackend};
use futures_util::{Stream, StreamExt};

use crate::{
    ATTRIBUTE_LENGTH, Artifact, ArtifactSelector, AttributeKey, AttributeKeyPart, Datum,
    DialogArtifactsError, ENTITY_LENGTH, ENTITY_RAW_HEAD, EntityKey, EntityKeyPart, FromKey,
    Instruction, Key, KeyBytes, KeyView, KeyViewConstruct, KeyViewMut, MatchCandidate, State,
    ValueKey, selector::Constrained,
};

/// The concrete search-tree type the artifact indexes use.
///
/// Keys are the raw fixed-size bytes of [`Key`]; values are [`State`]
/// payloads stored in the tree's native (rkyv) encoding.
pub type ArtifactTree = PersistentTree<KeyBytes, State<Datum>>;

impl TreeValue for State<Datum> {}

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

/// A fixed-width key segment bounding a string prefix: the prefix's
/// raw bytes (capped at `head` — the order-preserving span of the
/// segment) followed by `fill`. With `fill = 0x00` this is the
/// smallest segment any matching value can have, with `fill = 0xFF`
/// the largest, so the pair brackets the prefix's key range.
fn prefix_segment<const N: usize>(prefix: &str, head: usize, fill: u8) -> [u8; N] {
    let mut segment = [fill; N];
    let raw = prefix.as_bytes();
    let take = raw.len().min(head).min(N);
    segment[..take].copy_from_slice(&raw[..take]);
    segment
}

/// Tighten a scan's `(start, end)` key pair with the selector's
/// prefix bounds. A prefix on a field that also has an exact
/// constraint is skipped — the exact value is already in the keys
/// and is strictly tighter. Applying a prefix to a non-leading key
/// dimension is sound (the range stays a superset of the matches;
/// [`MatchCandidate::matches_selector`] filters the rest) and
/// tightens the range whenever every more-significant dimension is
/// exact.
fn apply_prefix_bounds<K: KeyViewMut>(
    start: K,
    end: K,
    selector: &ArtifactSelector<Constrained>,
) -> (K, K) {
    let mut start = start;
    let mut end = end;
    if selector.attribute().is_none()
        && let Some(prefix) = selector.attribute_prefix()
    {
        let lo = prefix_segment::<ATTRIBUTE_LENGTH>(prefix, ATTRIBUTE_LENGTH, u8::MIN);
        let hi = prefix_segment::<ATTRIBUTE_LENGTH>(prefix, ATTRIBUTE_LENGTH, u8::MAX);
        start = start.set_attribute(AttributeKeyPart(&lo));
        end = end.set_attribute(AttributeKeyPart(&hi));
    }
    if selector.entity().is_none()
        && let Some(prefix) = selector.entity_prefix()
    {
        let lo = prefix_segment::<ENTITY_LENGTH>(prefix, ENTITY_RAW_HEAD, u8::MIN);
        let hi = prefix_segment::<ENTITY_LENGTH>(prefix, ENTITY_RAW_HEAD, u8::MAX);
        start = start.set_entity(EntityKeyPart(&lo));
        end = end.set_entity(EntityKeyPart(&hi));
    }
    (start, end)
}

/// Shared mutation + scan operations on an [`ArtifactTree`].
///
/// An extension trait rather than inherent methods because
/// `ArtifactTree` aliases a foreign `dialog_search_tree::PersistentTree` — the
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
    /// The batch's new nodes are written into `delta`, the caller-owned
    /// accumulator. Callers own everything else: building the change stream,
    /// choosing a base tree root, persisting a `Revision`, and flushing
    /// `delta`.
    async fn apply<S, I>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        instructions: I,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
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
            + 's;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ArtifactTreeExt for ArtifactTree {
    async fn apply<S, I>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        instructions: I,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));

        // Snapshot the committed tree before editing. A retract consults it to
        // tell a fact that existed *before this batch* (tombstone it, so the
        // removal propagates on merge) from one that only appears within the
        // batch or not at all (drop it, leaving no tombstone). `edit()` borrows
        // `self`, so this cheap Arc-backed clone stays readable alongside the
        // in-flight `transient`.
        let base = self.clone();

        // Open one transient edit batch over this tree's spine and apply every
        // instruction's writes to it in flight, so the whole instruction stream
        // costs a single persist instead of one full tree rebuild per key.
        let mut transient = self.edit();

        tokio::pin!(instructions);
        while let Some(instruction) = instructions.next().await {
            match instruction {
                Instruction::Assert(artifact) => {
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    let datum = Datum::from(artifact);
                    let added = State::Added(datum);
                    transient = transient
                        .insert(entity_key.into_key().into(), added.clone(), &storage)
                        .await?;
                    transient = transient
                        .insert(attribute_key.into_key().into(), added.clone(), &storage)
                        .await?;
                    transient = transient
                        .insert(value_key.into_key().into(), added, &storage)
                        .await?;
                }
                Instruction::Replace(artifact) => {
                    let entity_key = EntityKey::from(&artifact);

                    // Scan priors at this (entity, attribute) against the
                    // in-flight transient tree, so writes from earlier
                    // instructions in this batch are visible. Same-valued priors
                    // already represent the desired state; only different-valued
                    // ones need superseding. The scan borrows `transient`
                    // immutably, so collect into owned vectors in a scope that
                    // ends before the subsequent mutating reassignments.
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
                        let search_stream = transient.stream_range(
                            KeyBytes::from(search_start)..=KeyBytes::from(search_end),
                            &storage,
                        );
                        tokio::pin!(search_stream);
                        while let Some(candidate) = search_stream.next().await {
                            let candidate = candidate?;
                            if let State::Added(current_element) = candidate.value {
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

                        transient = transient
                            .delete(&entity_key.into_key().into(), &storage)
                            .await?;
                        transient = transient
                            .delete(&value_key.into_key().into(), &storage)
                            .await?;
                        transient = transient
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
                    let added = State::Added(datum);
                    transient = transient
                        .insert(entity_key.into_key().into(), added.clone(), &storage)
                        .await?;
                    transient = transient
                        .insert(attribute_key.into_key().into(), added.clone(), &storage)
                        .await?;
                    transient = transient
                        .insert(value_key.into_key().into(), added, &storage)
                        .await?;
                }
                Instruction::Retract(artifact) => {
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    // Was this exact fact committed *before* this batch? Read the
                    // value key (the fact identity) from the base snapshot, not
                    // the transient tree — so an assert earlier in this same
                    // batch doesn't count as a prior.
                    let committed = matches!(
                        base.get(&value_key.clone().into_key().into(), &storage)
                            .await?,
                        Some(State::Added(_))
                    );

                    if committed {
                        // Retracting a durable fact: replace it with a `Removed`
                        // tombstone across all three orderings so the removal
                        // survives a merge and beats a stale remote assert.
                        let removed: State<Datum> = State::Removed;
                        transient = transient
                            .insert(entity_key.into_key().into(), removed.clone(), &storage)
                            .await?;
                        transient = transient
                            .insert(attribute_key.into_key().into(), removed.clone(), &storage)
                            .await?;
                        transient = transient
                            .insert(value_key.into_key().into(), removed, &storage)
                            .await?;
                    } else {
                        // No committed prior: the fact only exists (if at all) as
                        // an assert earlier in this batch. Delete the keys so the
                        // assert and retract cancel to nothing — no tombstone,
                        // no tree churn. Deleting an absent key is a no-op, so a
                        // retract of a fact that never existed changes nothing.
                        transient = transient
                            .delete(&entity_key.into_key().into(), &storage)
                            .await?;
                        transient = transient
                            .delete(&attribute_key.into_key().into(), &storage)
                            .await?;
                        transient = transient
                            .delete(&value_key.into_key().into(), &storage)
                            .await?;
                    }
                }
            }
        }

        // Seal the whole batch with a single bottom-up persist into the
        // caller's delta.
        *self = transient.persist(delta)?;
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
            + 's,
    {
        let tree = self;
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));
        try_stream! {
            // Index choice: exact fields take priority (entity /
            // value / attribute, as before); prefix bounds pick the
            // index whose leading dimension they constrain when no
            // exact field does. Every branch additionally tightens
            // its key range with whatever prefix bounds the selector
            // carries (sound on any dimension, tight on leading ones)
            // via `apply_prefix_bounds`, and `matches_selector`
            // re-checks per entry. The lower/upper bounds collapse to
            // the same exact key when every component is constrained,
            // and the inclusive range still selects it.
            let range = if selector.entity().is_some()
                || (selector.entity_prefix().is_some()
                    && selector.value().is_none()
                    && selector.attribute().is_none()
                    && selector.attribute_prefix().is_none())
            {
                let (start, end) = apply_prefix_bounds(
                    <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(&selector),
                    <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(&selector),
                    &selector,
                );
                KeyBytes::from(start.into_key())..=KeyBytes::from(end.into_key())
            } else if selector.value().is_some() {
                let (start, end) = apply_prefix_bounds(
                    <ValueKey<Key> as KeyViewConstruct>::min().apply_selector(&selector),
                    <ValueKey<Key> as KeyViewConstruct>::max().apply_selector(&selector),
                    &selector,
                );
                KeyBytes::from(start.into_key())..=KeyBytes::from(end.into_key())
            } else if selector.attribute().is_some() || selector.attribute_prefix().is_some() {
                let (start, end) = apply_prefix_bounds(
                    <AttributeKey<Key> as KeyViewConstruct>::min().apply_selector(&selector),
                    <AttributeKey<Key> as KeyViewConstruct>::max().apply_selector(&selector),
                    &selector,
                );
                KeyBytes::from(start.into_key())..=KeyBytes::from(end.into_key())
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
                    value: raw.value,
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
