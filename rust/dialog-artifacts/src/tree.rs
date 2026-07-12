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
use std::ops::RangeInclusive;

use crate::history::{Cause as HistoryCause, Claim, Record, Version};
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

// Deletion is no longer resolved at the slot: it travels as a history
// record and is applied to the active indexes by the observed-remove
// merge screen (see `crate::merge` and `notes/observed-remove-merge.md`),
// so no `Removed` tombstone ever reaches a data-region `integrate`
// contest. The only remaining contest is `Added` vs `Added` — two
// byte-variants of the *same* value — which the default deterministic
// hash race resolves. No `prevails_over` override is needed.
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

/// Layers a [`Delta`]'s buffered nodes over a backing store for reads, so
/// that a tree persisted into the delta but not yet flushed remains
/// traversable. This lets a caller keep editing a tree across multiple
/// persist points (e.g. [`ArtifactTreeExt::apply_versioned`] followed by
/// [`ArtifactTreeExt::record`]) while the whole batch still travels to
/// storage as a single flush. Writes pass through to the backing store.
struct DeltaReadThrough<'a, S> {
    delta: &'a Delta<NodeHash, Buffer>,
    store: S,
}

impl<S: Clone> Clone for DeltaReadThrough<'_, S> {
    fn clone(&self) -> Self {
        Self {
            delta: self.delta,
            store: self.store.clone(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> StorageBackend for DeltaReadThrough<'_, S>
where
    S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    type Key = NodeHash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.store.set(*key.as_bytes(), value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        if let Some(buffer) = self.delta.get(key) {
            return Ok(Some(buffer.as_ref().to_vec()));
        }
        self.store.get(key.as_bytes()).await
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

    /// Like [`ArtifactTreeExt::apply`], but tags every [`Datum`] written by
    /// the batch with the [`Version`](crate::history::Version) of the
    /// revision that produced it, and records each instruction's history
    /// claim into the tree's history region. This is the write path used
    /// by version-controlled branch commits; [`ArtifactTreeExt::apply`]
    /// leaves the version unset.
    ///
    /// Returns whether the batch changed the indexes at all. A batch made
    /// entirely of cardinality-one no-ops (re-asserting values already in
    /// place) leaves the tree untouched and records no history — there is
    /// nothing a revision could attribute, and callers should not mint one.
    async fn apply_versioned<S, I>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        version: Option<Version>,
        instructions: I,
    ) -> Result<bool, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend;

    /// The currently asserted [`Datum`]s recorded for the given entity and
    /// attribute, scanned from the EAV index. Multiple data are possible for
    /// attributes with more than one asserted value.
    async fn select_data<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Attribute,
    ) -> Result<Vec<Datum>, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync;

    /// Write pre-built entries (e.g. revision lineage records — see
    /// [`Record::into_entry`](crate::history::Record::into_entry)) into the
    /// tree as one edit batch, accumulating new nodes in `delta`
    async fn record<S>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        entries: Vec<(Key, State<Datum>)>,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync;

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
        self.apply_versioned(store, delta, None, instructions)
            .await
            .map(|_| ())
    }

    async fn apply_versioned<S, I>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        version: Option<Version>,
        instructions: I,
    ) -> Result<bool, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
        I: Stream<Item = Instruction> + ConditionalSend,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store.clone()));

        // Open one transient edit batch over this tree's spine and apply every
        // instruction's writes to it in flight, so the whole instruction stream
        // costs a single persist instead of one full tree rebuild per key.
        let mut transient = self.edit();

        // History records are buffered and only written if the batch changed
        // the indexes: a batch of pure no-ops must leave the tree untouched,
        // history region included.
        let mut history_entries: Vec<(Key, State<Datum>)> = Vec::new();
        let mut changed = false;

        tokio::pin!(instructions);
        while let Some(instruction) = instructions.next().await {
            // The `dialog.` namespace is reserved for version-control
            // machinery (revision records — see
            // `history::RevisionRecord`), which writes through
            // [`ArtifactTreeExt::record`] rather than instructions. At the
            // library level lineage therefore cannot be corrupted through
            // the ordinary write path.
            {
                let (Instruction::Assert(artifact)
                | Instruction::Replace(artifact)
                | Instruction::Retract(artifact)) = &instruction;
                if artifact.the.as_str().starts_with("dialog.") {
                    return Err(DialogArtifactsError::ReservedAttribute(
                        artifact.the.to_string(),
                    ));
                }
            }
            match instruction {
                Instruction::Assert(artifact) => {
                    changed = true;
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    // A version-tagged assertion records its history: an
                    // assertion is purely additive, so it supersedes nothing.
                    if let Some(version) = &version {
                        let record = Record::Assert(Claim {
                            the: artifact.the.clone(),
                            of: artifact.of.clone(),
                            is: artifact.is.clone(),
                            cause: HistoryCause::genesis(),
                        });
                        history_entries.push(record.into_entry(version));
                    }

                    let mut datum = Datum::from(artifact);
                    datum.version = version;
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
                    // ones need superseding. Sameness is compared on the raw
                    // stored form — (value_type, bytes) — sparing a full
                    // `Artifact` parse per candidate. The scan borrows
                    // `transient` immutably, so collect into owned vectors in a
                    // scope that ends before the subsequent mutating
                    // reassignments.
                    let replace_type = u8::from(artifact.is.data_type());
                    let replace_value = artifact.is.to_bytes();
                    let mut superseded_keys: Vec<Key> = Vec::new();
                    let mut superseded_versions: Vec<Version> = Vec::new();
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
                                if current_element.value_type == replace_type
                                    && current_element.value == replace_value
                                {
                                    found_same_value = true;
                                } else {
                                    superseded_keys.push(Key::from(candidate.key));
                                    superseded_versions.extend(current_element.version);
                                }
                            }
                        }
                    }

                    // Cardinality-one no-op: the identical claim already
                    // stands, at its original version, and there is nothing
                    // to supersede. Nothing changes in the indexes and no
                    // history is recorded — a fresh record would fork the
                    // claim's lineage away from the version the standing
                    // datum carries.
                    if found_same_value && superseded_keys.is_empty() {
                        continue;
                    }
                    changed = true;

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

                    // A version-tagged replacement records its history: its
                    // cause lists the versions of the claims it superseded —
                    // exactly the data removed from the indexes above. The
                    // record is written even when the insert below is skipped
                    // because a same-valued prior survives; the supersession
                    // of the different-valued claims still happened and must
                    // be attributable.
                    if let Some(version) = &version {
                        let record = Record::Assert(Claim {
                            the: artifact.the.clone(),
                            of: artifact.of.clone(),
                            is: artifact.is.clone(),
                            cause: HistoryCause::new(superseded_versions),
                        });
                        history_entries.push(record.into_entry(version));
                    }

                    if found_same_value {
                        continue;
                    }

                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);
                    let mut datum = Datum::from(artifact);
                    datum.version = version;
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
                    changed = true;
                    let entity_key = EntityKey::from(&artifact);
                    let value_key = ValueKey::from_key(&entity_key);
                    let attribute_key = AttributeKey::from_key(&entity_key);

                    // A version-tagged retraction records its history: its
                    // cause is the version of the assertion it withdraws.
                    // An assertion made earlier in this same batch carries
                    // this batch's own version; a record must not claim
                    // itself as its cause, so that degenerates to a genesis
                    // retraction.
                    if let Some(version) = &version {
                        let withdrawn = match transient
                            .get(&entity_key.clone().into_key().into(), &storage)
                            .await?
                        {
                            Some(State::Added(datum)) => {
                                datum.version.filter(|withdrawn| withdrawn != version)
                            }
                            _ => None,
                        };
                        let record = Record::Retract(Claim {
                            the: artifact.the.clone(),
                            of: artifact.of.clone(),
                            is: artifact.is.clone(),
                            cause: withdrawn.into_iter().collect(),
                        });
                        history_entries.push(record.into_entry(version));
                    }

                    // Observed-remove semantics: retraction deletes the
                    // fact's keys outright — no tombstone. The retract
                    // record written above is the durable carrier of the
                    // deletion (it replicates as history), and a replica's
                    // causal context is what stops a stale peer's copy from
                    // resurrecting the fact at merge time (see
                    // `notes/observed-remove-merge.md`). Deleting an absent
                    // key is a no-op, so a same-batch assert+retract cancels
                    // to nothing and a retract of a fact that never existed
                    // changes nothing in the indexes.
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

        for (key, entry) in history_entries {
            transient = transient.insert(key.into(), entry, &storage).await?;
        }

        // Seal the whole batch with a single bottom-up persist into the
        // caller's delta.
        *self = transient.persist(delta)?;
        Ok(changed)
    }

    async fn select_data<S>(
        &self,
        store: S,
        of: &crate::Entity,
        the: &crate::Attribute,
    ) -> Result<Vec<Datum>, DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let storage = ContentAddressedStorage::new(TreeStorageBridge(store));

        let search_start = <EntityKey<Key> as KeyViewConstruct>::min()
            .set_entity(EntityKeyPart::from(of))
            .set_attribute(AttributeKeyPart::from(the))
            .into_key();
        let search_end = <EntityKey<Key> as KeyViewConstruct>::max()
            .set_entity(EntityKeyPart::from(of))
            .set_attribute(AttributeKeyPart::from(the))
            .into_key();

        let stream = self.stream_range(
            KeyBytes::from(search_start)..=KeyBytes::from(search_end),
            &storage,
        );
        tokio::pin!(stream);

        let mut data = Vec::new();
        while let Some(entry) = stream.next().await {
            if let State::Added(datum) = entry?.value {
                data.push(datum);
            }
        }

        Ok(data)
    }

    async fn record<S>(
        &mut self,
        store: &mut S,
        delta: &mut Delta<NodeHash, Buffer>,
        entries: Vec<(Key, State<Datum>)>,
    ) -> Result<(), DialogArtifactsError>
    where
        S: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + Clone
            + ConditionalSync,
    {
        let mut transient = self.edit();
        {
            // Read through the delta: this tree's latest nodes may only
            // exist there (persisted by an earlier batch, not yet flushed).
            let storage = ContentAddressedStorage::new(DeltaReadThrough {
                delta: &*delta,
                store: store.clone(),
            });
            for (key, entry) in entries {
                transient = transient.insert(key.into(), entry, &storage).await?;
            }
        }
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
            let range = selector_range(&selector);

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

/// The inclusive key range a selector's scan reads.
///
/// Index choice: exact fields take priority (entity / value /
/// attribute); prefix bounds pick the index whose leading dimension
/// they constrain when no exact field does. The range is additionally
/// tightened with whatever prefix bounds the selector carries (sound
/// on any dimension, tight on leading ones) via
/// [`apply_prefix_bounds`]; per-entry re-checking against the full
/// selector happens during the scan, not here. The lower/upper bounds
/// collapse to the same exact key when every component is
/// constrained, and the inclusive range still selects it.
///
/// This is the selector's *demanded range*: everything a scan for it
/// would touch, whether or not entries exist there. Subscriptions use
/// it as the unit of a demand cover — a range that came back empty is
/// still demanded (the emptiness was read), so a later write into it
/// must invalidate the reader.
pub fn selector_range(selector: &ArtifactSelector<Constrained>) -> RangeInclusive<KeyBytes> {
    if selector.entity().is_some()
        || (selector.entity_prefix().is_some()
            && selector.value().is_none()
            && selector.attribute().is_none()
            && selector.attribute_prefix().is_none())
    {
        let (start, end) = apply_prefix_bounds(
            <EntityKey<Key> as KeyViewConstruct>::min().apply_selector(selector),
            <EntityKey<Key> as KeyViewConstruct>::max().apply_selector(selector),
            selector,
        );
        KeyBytes::from(start.into_key())..=KeyBytes::from(end.into_key())
    } else if selector.value().is_some() {
        let (start, end) = apply_prefix_bounds(
            <ValueKey<Key> as KeyViewConstruct>::min().apply_selector(selector),
            <ValueKey<Key> as KeyViewConstruct>::max().apply_selector(selector),
            selector,
        );
        KeyBytes::from(start.into_key())..=KeyBytes::from(end.into_key())
    } else if selector.attribute().is_some() || selector.attribute_prefix().is_some() {
        let (start, end) = apply_prefix_bounds(
            <AttributeKey<Key> as KeyViewConstruct>::min().apply_selector(selector),
            <AttributeKey<Key> as KeyViewConstruct>::max().apply_selector(selector),
            selector,
        );
        KeyBytes::from(start.into_key())..=KeyBytes::from(end.into_key())
    } else {
        // `Constrained` guarantees at least one field is set.
        unreachable!("ArtifactSelector will always have at least one field specified")
    }
}
