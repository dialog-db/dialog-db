use crate::artifacts::query::Select;
use crate::key::value_tail_bytes;
use crate::selector::Constrained;
use crate::{
    Artifact, ArtifactSelector, ArtifactStream, Asset, Attribute, DialogArtifactsError, Entity,
    Instruction, Value,
};
use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_search_tree::Manifest;
use dialog_storage::Blake3Hash;
use futures_util::Stream;
use futures_util::stream;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, HashMap};
use std::mem::take;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

/// A single write operation on an `(entity, attribute)` pair.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Change {
    /// Assert a value for an entity-attribute pair (cardinality-many).
    Assert(Value),
    /// Replace any prior value(s) at this `(entity, attribute)` with this one
    /// (cardinality-one). Supersession of priors happens at commit time.
    Replace(Value),
    /// Retract a value from an entity-attribute pair.
    Retract(Value),
    /// Assert a value and retract the one live claim of the cell the
    /// succession elects: the claim a read under the attribute's policy
    /// returns. The election reads the line, so it is resolved by the
    /// transactor before the batch commits (see
    /// [`Changes::take_successions`]); an unresolved succession commits
    /// as a plain assertion.
    Succeed(Value, Succession),
}

/// How a write under a choosing policy elects the claim it succeeds:
/// the claim a read under the same policy returns. A set (`all`)
/// succeeds nothing. [`Change::Replace`] is the older cardinality-one
/// write, which supersedes every prior of the cell; the engine's
/// machinery still writes it, a statement no longer does.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Succession {
    /// The newest claim.
    Last,
    /// The claim with the greatest value, the newest among equals.
    Max,
    /// The claim with the least value, the newest among equals.
    Min,
    /// The claim whose value is listed first, best first; an unlisted
    /// value ranks last, and the newest wins among equals.
    Top(Vec<Value>),
}

/// The write side of the triple store.
///
/// Implementors accumulate fact changes (associations and dissociations)
/// that can later be committed atomically.
pub trait Update {
    /// Assert that the `attribute` of `entity` is `value`.
    fn associate(&mut self, the: Attribute, of: Entity, is: Value);

    /// Assert with cardinality-one semantics: replaces any previous
    /// value for the same `(attribute, entity)` pair in this batch.
    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        self.associate(the, of, is);
    }

    /// Assert that the `attribute` of `entity` is `value`, succeeding
    /// the one live claim of the cell `succession` elects: that claim
    /// is retracted when the batch commits, and every other claim of
    /// the cell stays. Falls back to a plain assertion for a target
    /// that cannot resolve an election.
    fn succeed(&mut self, the: Attribute, of: Entity, is: Value, succession: Succession) {
        let _ = succession;
        self.associate(the, of, is);
    }

    /// Retract that the `attribute` of `entity` is `value`.
    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value);

    /// Store `asset` when this batch commits.
    ///
    /// The commit writes the asset's bytes through the blob store and
    /// records `asset:<hash> dialog.asset/size <size>` in the same revision
    /// as the batch's facts, so facts may point at [`Asset::entity`] in the
    /// same batch. Staging the same asset twice stages it once.
    fn import(&mut self, asset: Asset);

    /// Drop this line's reference to `asset` when this batch commits: the
    /// commit retracts its `dialog.asset/size` fact, leaving its bytes to be
    /// collected. The later of an import and a discard of the same asset in
    /// one batch wins.
    ///
    /// A discard is keyed on the asset's hash alone: the commit retracts the
    /// size the line records for that hash, whatever size `asset` names, and
    /// changes nothing when the line records none.
    fn discard(&mut self, asset: Asset);
}

/// A domain-level write operation that can be asserted or retracted.
///
/// Types like concept structs and attribute expressions implement this
/// trait. Asserting a statement adds facts; retracting removes them.
pub trait Statement: Sized {
    /// Assert this statement into an update target.
    fn assert(self, update: &mut impl Update);

    /// Retract this statement from an update target.
    fn retract(self, update: &mut impl Update);
}

/// The facts of a [`Changes`] batch, by entity and attribute.
type Facts = HashMap<Entity, HashMap<Attribute, Vec<Change>>>;

/// A change a batch makes to the assets its line stores.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetChange {
    /// Store the asset. See [`Update::import`].
    Import(Asset),
    /// Drop the line's reference to the asset. See [`Update::discard`].
    Discard(Asset),
}

impl AssetChange {
    /// The asset this change concerns.
    pub fn asset(&self) -> &Asset {
        match self {
            AssetChange::Import(asset) | AssetChange::Discard(asset) => asset,
        }
    }
}

/// A batch of pending writes: fact changes organized by entity and
/// attribute, plus the changes the batch makes to the assets its line
/// stores (see [`Update::import`] and [`Update::discard`]).
///
/// Serializes as the fact nesting alone when no asset changes, so such a
/// batch keeps the shape every earlier reader understands, and as
/// `{ facts, assets }` otherwise. Either shape round-trips through any serde
/// format without losing retractions, cardinality-one replacements, or
/// asset bytes.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Changes {
    facts: Facts,
    assets: BTreeMap<Blake3Hash, AssetChange>,
}

/// The serialized shape of a [`Changes`] batch that changes assets.
#[derive(Deserialize)]
struct ChangesWithAssets {
    facts: Facts,
    assets: Vec<AssetChange>,
}

/// [`ChangesWithAssets`] borrowed from a batch, for encoding without
/// copying its facts or any asset's bytes.
#[derive(Serialize)]
struct ChangesWithAssetsRef<'a> {
    facts: &'a Facts,
    assets: Vec<&'a AssetChange>,
}

/// The serialized shape of a [`Changes`] batch that changes no asset.
#[derive(Serialize)]
#[serde(transparent)]
struct FactsOnly<'a>(&'a Facts);

/// Either serialized shape, for decoding.
#[derive(Deserialize)]
#[serde(untagged)]
enum ChangesShape {
    WithAssets(ChangesWithAssets),
    FactsOnly(Facts),
}

impl Serialize for Changes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.assets.is_empty() {
            FactsOnly(&self.facts).serialize(serializer)
        } else {
            ChangesWithAssetsRef {
                facts: &self.facts,
                assets: self.assets.values().collect(),
            }
            .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Changes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match ChangesShape::deserialize(deserializer)? {
            ChangesShape::WithAssets(ChangesWithAssets { facts, assets }) => {
                let mut changes = Changes {
                    facts,
                    assets: BTreeMap::new(),
                };
                for change in assets {
                    changes.change_asset(change);
                }
                changes
            }
            ChangesShape::FactsOnly(facts) => Changes {
                facts,
                assets: BTreeMap::new(),
            },
        })
    }
}

impl Changes {
    /// Create an empty changeset.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assert a claim.
    pub fn assert<C: Statement>(&mut self, claim: C) -> &mut Self {
        claim.assert(self);
        self
    }

    /// Retract a claim.
    pub fn retract<C: Statement>(&mut self, claim: C) -> &mut Self {
        claim.retract(self);
        self
    }

    /// Whether the batch records no fact changes and changes no asset.
    pub fn is_empty(&self) -> bool {
        self.facts.is_empty() && self.assets.is_empty()
    }

    /// Whether the batch changes any asset.
    ///
    /// Only a transaction's commit stores assets. A target that holds facts
    /// alone checks this before taking the batch's
    /// [`into_instructions`](Self::into_instructions), which leaves the
    /// asset changes out, and refuses the batch rather than drop them.
    pub fn has_assets(&self) -> bool {
        !self.assets.is_empty()
    }

    /// The asset changes this batch makes, in hash order.
    pub fn assets(&self) -> impl Iterator<Item = &AssetChange> {
        self.assets.values()
    }

    /// The assets this batch stores, in hash order.
    pub fn imports(&self) -> impl Iterator<Item = &Asset> {
        self.assets.values().filter_map(|change| match change {
            AssetChange::Import(asset) => Some(asset),
            AssetChange::Discard(_) => None,
        })
    }

    /// The assets this batch drops, in hash order.
    pub fn discards(&self) -> impl Iterator<Item = &Asset> {
        self.assets.values().filter_map(|change| match change {
            AssetChange::Discard(asset) => Some(asset),
            AssetChange::Import(_) => None,
        })
    }

    /// Remove and return this batch's asset changes, in hash order, leaving
    /// its fact changes in place. The commit path drains them this way
    /// before turning the facts into instructions.
    pub fn take_assets(&mut self) -> Vec<AssetChange> {
        take(&mut self.assets).into_values().collect()
    }

    /// Record `change`, the later change to an asset winning, except that an
    /// import naming stored bytes never displaces an import carrying them.
    fn change_asset(&mut self, change: AssetChange) {
        let hash = *change.asset().hash();
        if let (AssetChange::Import(incoming), Some(AssetChange::Import(held))) =
            (&change, self.assets.get(&hash))
            && incoming.content().is_none()
            && held.content().is_some()
        {
            return;
        }
        self.assets.insert(hash, change);
    }

    /// Convert to an instruction stream, the form a commit applies.
    ///
    /// A succession is resolved against the line before the batch
    /// commits; a batch still holding one has not been through the
    /// transactor, and committing it raw would land its value without
    /// succeeding anything. Commit such a batch through a transaction
    /// (`transaction().integrate(changes)`), which resolves it.
    pub fn into_stream(self) -> ChangeStream {
        assert!(
            !self.has_successions(),
            "a batch holding successions commits through a transaction, which resolves them"
        );
        ChangeStream::from(self)
    }

    /// Drop every change recorded for entities that fail `keep`,
    /// asserts and retracts alike. Returns `true` when anything was
    /// removed. Unlike [`retract`](Self::retract), which records a
    /// tombstone alongside the prior changes, this removes the
    /// entity's entries from the batch outright — the primitive a
    /// session overlay needs to garbage-collect per-client facts
    /// without growing.
    pub fn retain_entities<F: FnMut(&Entity) -> bool>(&mut self, mut keep: F) -> bool {
        let before = self.facts.len();
        self.facts.retain(|entity, _| keep(entity));
        self.facts.len() != before
    }

    /// Apply every change in `other` after the ones already recorded, with
    /// the same semantics as recording them here directly: a replacement
    /// still supersedes earlier changes to its `(entity, attribute)`, and
    /// `other`'s asset changes follow this batch's.
    pub fn merge(&mut self, other: Changes) {
        for change in other.assets.into_values() {
            self.change_asset(change);
        }
        for (entity, attributes) in other.facts {
            for (attribute, changes) in attributes {
                for change in changes {
                    match change {
                        Change::Assert(value) => {
                            self.associate(attribute.clone(), entity.clone(), value)
                        }
                        Change::Replace(value) => {
                            self.associate_unique(attribute.clone(), entity.clone(), value)
                        }
                        Change::Succeed(value, succession) => {
                            self.succeed(attribute.clone(), entity.clone(), value, succession)
                        }
                        Change::Retract(value) => {
                            self.dissociate(attribute.clone(), entity.clone(), value)
                        }
                    }
                }
            }
        }
    }

    /// Borrowing iterator over every recorded `(entity, attribute,
    /// change)` triple. Use this when you need to inspect the batch
    /// without consuming it — e.g. to extract tombstones from
    /// retracts without cloning the whole structure.
    pub fn iter(&self) -> impl Iterator<Item = (&Entity, &Attribute, &Change)> {
        self.facts.iter().flat_map(|(entity, attrs)| {
            attrs
                .iter()
                .flat_map(move |(attr, changes)| changes.iter().map(move |c| (entity, attr, c)))
        })
    }

    /// Whether any cell of this batch holds a succession the transactor
    /// has yet to resolve.
    pub fn has_successions(&self) -> bool {
        self.iter()
            .any(|(_, _, change)| matches!(change, Change::Succeed(..)))
    }

    /// The cells holding a succession, each once.
    pub fn cells_with_successions(&self) -> Vec<(Attribute, Entity)> {
        let mut cells = Vec::new();
        for (entity, attribute, change) in self.iter() {
            if matches!(change, Change::Succeed(..)) {
                let cell = (attribute.clone(), entity.clone());
                if !cells.contains(&cell) {
                    cells.push(cell);
                }
            }
        }
        cells
    }

    /// The changes recorded for one cell, in order, taken out of the
    /// batch. The transactor settles a cell's successions by taking its
    /// changes, deciding what each write succeeds, and putting the
    /// settled changes back with [`put_cell`](Self::put_cell).
    pub fn take_cell(&mut self, the: &Attribute, of: &Entity) -> Vec<Change> {
        let Some(attributes) = self.facts.get_mut(of) else {
            return Vec::new();
        };
        let changes = attributes.remove(the).unwrap_or_default();
        if attributes.is_empty() {
            self.facts.remove(of);
        }
        changes
    }

    /// Record `changes` for one cell, after whatever the cell holds, in
    /// the order given.
    pub fn put_cell(&mut self, the: Attribute, of: Entity, changes: Vec<Change>) {
        if changes.is_empty() {
            return;
        }
        self.facts
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .extend(changes);
    }

    /// Convert the fact changes to a vec of instructions.
    ///
    /// Asset changes are not instructions and are left out: a caller that
    /// commits the batch drains them first with
    /// [`take_assets`](Self::take_assets), and any other caller refuses a
    /// batch that [`has_assets`](Self::has_assets). A succession reads as
    /// the assertion of its value: what the batch claims, which is what a
    /// caller inspecting the batch asks. The retraction it resolves to
    /// exists only once the transactor has settled it against the line
    /// ([`into_stream`](Self::into_stream) refuses a batch before that).
    pub fn into_instructions(self) -> Vec<Instruction> {
        let mut instructions = Vec::new();
        for (entity, attributes) in self.facts {
            for (attribute, operations) in attributes {
                for operation in operations {
                    let instruction = match operation {
                        Change::Assert(value) | Change::Succeed(value, _) => {
                            Instruction::Assert(Artifact {
                                the: attribute.clone(),
                                of: entity.clone(),
                                is: value,
                                cause: None,
                            })
                        }
                        Change::Replace(value) => Instruction::Replace(Artifact {
                            the: attribute.clone(),
                            of: entity.clone(),
                            is: value,
                            cause: None,
                        }),
                        Change::Retract(value) => Instruction::Retract(Artifact {
                            the: attribute.clone(),
                            of: entity.clone(),
                            is: value,
                            cause: None,
                        }),
                    };
                    instructions.push(instruction);
                }
            }
        }
        instructions
    }
}

impl Update for Changes {
    fn associate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.facts
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .push(Change::Assert(is));
    }

    fn associate_unique(&mut self, the: Attribute, of: Entity, is: Value) {
        self.facts
            .entry(of)
            .or_default()
            .insert(the, vec![Change::Replace(is)]);
    }

    fn succeed(&mut self, the: Attribute, of: Entity, is: Value, succession: Succession) {
        self.facts
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .push(Change::Succeed(is, succession));
    }

    fn dissociate(&mut self, the: Attribute, of: Entity, is: Value) {
        self.facts
            .entry(of)
            .or_default()
            .entry(the)
            .or_default()
            .push(Change::Retract(is));
    }

    fn import(&mut self, asset: Asset) {
        self.change_asset(AssetChange::Import(asset));
    }

    fn discard(&mut self, asset: Asset) {
        self.change_asset(AssetChange::Discard(asset));
    }
}

impl IntoIterator for Changes {
    type Item = Instruction;
    type IntoIter = IntoIter<Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_instructions().into_iter()
    }
}

/// A [`Stream`] adapter that drains [`Changes`] into [`Instruction`]s.
pub struct ChangeStream {
    iter: IntoIter<Instruction>,
}

impl From<Changes> for ChangeStream {
    fn from(changes: Changes) -> Self {
        Self {
            iter: changes.into_iter(),
        }
    }
}

impl Stream for ChangeStream {
    type Item = Instruction;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next())
    }
}

/// The full sort key for an [`Artifact`] — `(the, of, value_tail)`.
///
/// - `the` / `of` — raw attribute / entity key bytes.
/// - `value_tail` — the key's value tail bytes (see below).
///
/// # Why this exact component order
///
/// The artifact prolly tree keeps three indexes, each a byte key (see
/// `dialog_artifacts::key`):
///
/// ```text
///   EAV:  tag | entity    | attribute | value_tail
///   AEV:  tag | attribute | entity    | value_tail
///   VAE:  tag | value_tail | attribute | entity
/// ```
///
/// A query scan pins whichever dimension the selector constrains and
/// streams the rest in that index's byte order. The pinned dimension
/// is constant across the whole scan, so it drops out of the
/// comparison — what's left is the index's *residual* order:
///
/// ```text
///   .of(entity)    → EAV → residual (attribute, value_tail)
///   .the(attr)     → AEV → residual (entity,    value_tail)
///   .is(value)     → VAE → residual (attribute, entity)
/// ```
///
/// `SortKey = (attribute, entity, value_tail)` is the **unique**
/// total order whose restriction (delete the pinned component)
/// reproduces every one of those residuals:
///
/// - lock `entity`  → `attribute` is the next live component ✓ (EAV)
/// - lock `value`   → `value_tail` drops out, `attribute` is next ✓ (VAE)
/// - lock `attribute` → `attribute` itself drops out, `entity` is
///   next ✓ (AEV)
///
/// In every mode the next live component after the pinned one is
/// exactly the dimension that index sorts by next. So sorting any
/// source's output by `SortKey` yields the same order the tree's
/// scan would for that selector — which is what lets the query
/// layer's k-way merge interleave a branch scan and a `Changes`
/// overlay (or two branches) into the order a single physical tree
/// containing all of them would produce. It also holds for
/// multi-constraint selectors:
/// pinning two dimensions just removes both from the comparison.
///
/// The value-tail component (vs. the bare `(the, of)` group key) also
/// fixes interleaving *within* a cardinality-many group: same-`(the,
/// of)` items from different streams order by their value tail rather
/// than by stream index.
///
/// The third component is the key's *value tail* (the type byte followed by
/// the value slot, plus a spilled value's trailing whole-value hash), not a
/// bare type discriminant plus reference: the tree orders same-`(the, of)`
/// facts by exactly those tail bytes. A spilled value's slot holds the encoded
/// prefix of its raw bytes, so it sorts INTO its type band next to inline
/// values, and folding the whole tail into one component reproduces that
/// ordering; splitting the type out and comparing a reference separately would
/// not.
pub type SortKey = (Vec<u8>, Vec<u8>, Vec<u8>);

/// Compute the [`SortKey`] for an artifact.
///
/// Uses the same entity/attribute bytes and value tail the tree's own index
/// keys are built from (`EntityKey::from(&Artifact)` and friends), so a
/// `SortKey` sort reproduces the tree's byte order exactly, not just an
/// approximation of it. In particular the value component is the value tail the
/// key carries, so same-`(the, of, type)` facts order by value exactly as the
/// tree does. See [`SortKey`] for why the component order is correct across all
/// three scan modes.
///
/// `manifest` must be the format of the tree this ordering is compared against:
/// it decides whether the value spills and how much of it the tail carries, so
/// a different manifest would sort a boundary-sized value into a different
/// position than the tree puts it.
pub fn sort_key(artifact: &Artifact, manifest: &Manifest) -> SortKey {
    (
        artifact.the.as_str().as_bytes().to_vec(),
        artifact.of.as_str().as_bytes().to_vec(),
        value_tail_bytes(&artifact.is, manifest),
    )
}

/// `Statement` for a [`Changes`] batch — replays every recorded
/// [`Change`] and asset change into the target [`Update`].
///
/// Lets a `Changes` value act anywhere a single statement does: e.g.
/// folding pre-built changes into another transaction, or asserting
/// a changes-shaped overlay into a query session. `Assert` and
/// `Replace` map to `associate` / `associate_unique` on the target;
/// `Retract` maps to `dissociate`.
///
/// Retracting a batch inverts its asset changes as it inverts its facts:
/// an import becomes a discard and a discard an import.
impl Statement for Changes {
    /// Replay every change into `update` as it was recorded, a
    /// succession as a succession, so a batch asserted into another
    /// keeps what its writes meant.
    fn assert(mut self, update: &mut impl Update) {
        for change in self.take_assets() {
            match change {
                AssetChange::Import(asset) => update.import(asset),
                AssetChange::Discard(asset) => update.discard(asset),
            }
        }
        for (entity, attributes) in self.facts {
            for (attribute, changes) in attributes {
                for change in changes {
                    match change {
                        Change::Assert(value) => {
                            update.associate(attribute.clone(), entity.clone(), value)
                        }
                        Change::Replace(value) => {
                            update.associate_unique(attribute.clone(), entity.clone(), value)
                        }
                        Change::Succeed(value, succession) => {
                            update.succeed(attribute.clone(), entity.clone(), value, succession)
                        }
                        Change::Retract(value) => {
                            update.dissociate(attribute.clone(), entity.clone(), value)
                        }
                    }
                }
            }
        }
    }

    fn retract(mut self, update: &mut impl Update) {
        for change in self.take_assets() {
            match change {
                AssetChange::Import(asset) => update.discard(asset),
                AssetChange::Discard(asset) => update.import(asset),
            }
        }
        // Inverse: asserts/replaces become retracts; existing
        // retracts become asserts. Symmetric so `c.assert(t);
        // c.retract(t);` round-trips when `t` is a fresh target.
        for instruction in self.into_instructions() {
            match instruction {
                Instruction::Assert(a) | Instruction::Replace(a) => {
                    update.dissociate(a.the, a.of, a.is)
                }
                Instruction::Retract(a) => update.associate(a.the, a.of, a.is),
            }
        }
    }
}

/// `Provider<Select>` for an in-memory [`Changes`] batch.
///
/// Treats `Changes` as a queryable source: `Assert` and `Replace`
/// entries surface as [`Artifact`]s matching the [`ArtifactSelector`]'s
/// `the` / `of` / `is` constraints (whichever are present), sorted by
/// [`sort_key`] so the result interleaves cleanly with branch / layer
/// scans in a `merge_grouped`-style union.
///
/// `Retract` entries are **deliberately not yielded**. A retract in a
/// changes batch means "this fact should not appear" — a negative
/// signal that doesn't fit `ArtifactStream`'s positive `Result<Artifact, _>`
/// shape. Tombstone filtering against another source is a separate
/// concern handled by the composition layer that owns the merge.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<'a> Provider<Select<'a>> for Changes {
    /// A batch on its own belongs to no tree, so it orders its rows as a new
    /// tree would ([`Manifest::default`]). A caller merging the batch with a
    /// tree's scan reads it with [`Changes::select`] under that tree's
    /// manifest instead.
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let matched = self.select(&input, &Manifest::default());
        Ok(Box::pin(stream::iter(
            matched.into_iter().map(|artifact| Ok(artifact.into())),
        )))
    }
}

impl Changes {
    /// The asserted facts matching `input`, in [`sort_key`] order under
    /// `manifest`: the order a scan of a tree written under `manifest` would
    /// produce them, so the result merges with that tree's scan (see
    /// [`SortKey`]). Retracts are not yielded, as in the `Provider<Select>`
    /// impl.
    pub fn select(
        &self,
        input: &ArtifactSelector<Constrained>,
        manifest: &Manifest,
    ) -> Vec<Artifact> {
        let the = input.attribute();
        let of = input.entity();
        let is = input.value();

        // Linear filter over the batch. A `Changes` overlay is small
        // by construction — a few auto-injected metadata facts plus
        // whatever the caller asserted via `.with(...)` — so scanning
        // it per query is negligible and not worth indexing.
        let mut matched: Vec<Artifact> = Vec::new();
        for (entity, attrs) in &self.facts {
            if let Some(of_target) = of
                && entity != of_target
            {
                continue;
            }
            for (attribute, changes) in attrs {
                if let Some(the_target) = the
                    && attribute != the_target
                {
                    continue;
                }
                for change in changes {
                    let value = match change {
                        Change::Assert(v) | Change::Replace(v) | Change::Succeed(v, _) => v,
                        // Retracts don't surface from a Changes-as-source
                        // view — see impl docs.
                        Change::Retract(_) => continue,
                    };
                    if let Some(is_target) = is
                        && value != is_target
                    {
                        continue;
                    }
                    matched.push(Artifact {
                        the: attribute.clone(),
                        of: entity.clone(),
                        is: value.clone(),
                        cause: None,
                    });
                }
            }
        }
        // Sort by `sort_key` so this overlay's output is in the same
        // order a scan of a tree written under `manifest` would produce for
        // this selector — see `SortKey` docs. That's the precondition
        // `merge_grouped` relies on when it unions this stream with that
        // tree's scan.
        matched.sort_by_cached_key(|artifact| sort_key(artifact, manifest));
        matched
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use futures_util::StreamExt as _;

    fn alice() -> Entity {
        "id:alice".parse().expect("valid entity")
    }
    fn bob() -> Entity {
        "id:bob".parse().expect("valid entity")
    }
    fn name_attr() -> Attribute {
        "test/name".parse().expect("valid attribute")
    }
    fn role_attr() -> Attribute {
        "test/role".parse().expect("valid attribute")
    }

    /// A batch round-trips through dag-cbor with its retractions and
    /// cardinality-one replacements intact, so a session overlay can be
    /// carried as bytes into another process.
    #[dialog_common::test]
    fn it_round_trips_changes_through_dag_cbor() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));
        changes.associate(name_attr(), alice(), Value::String("Ally".into()));
        changes.associate_unique(role_attr(), alice(), Value::String("admin".into()));
        changes.dissociate(name_attr(), bob(), Value::String("Bob".into()));

        let bytes = serde_ipld_dagcbor::to_vec(&changes).expect("encode changes");
        let decoded: Changes = serde_ipld_dagcbor::from_slice(&bytes).expect("decode changes");

        assert_eq!(decoded, changes);
        let replaced = decoded
            .iter()
            .find(|(entity, attribute, _)| **entity == alice() && **attribute == role_attr())
            .map(|(_, _, change)| change.clone());
        assert_eq!(
            replaced,
            Some(Change::Replace(Value::String("admin".into()))),
            "a replacement stays a replacement"
        );
    }

    /// Staging the same asset twice stages it once, and an asset change
    /// alone makes a batch non-empty without adding any fact.
    #[dialog_common::test]
    fn it_stages_each_asset_once() {
        let mut changes = Changes::new();
        assert!(changes.is_empty());

        changes.import(Asset::new(b"one".to_vec()));
        changes.import(Asset::new(b"one".to_vec()));
        changes.import(Asset::new(b"two".to_vec()));

        assert!(!changes.is_empty());
        assert_eq!(changes.imports().count(), 2);
        assert!(
            changes.into_instructions().is_empty(),
            "asset changes are not facts"
        );
    }

    /// The later of an import and a discard of one asset wins, but naming
    /// stored bytes never displaces an import that carries them.
    #[dialog_common::test]
    fn it_keeps_the_later_change_to_an_asset() {
        let carried = Asset::new(b"one".to_vec());
        let stored = Asset::stored(*carried.hash(), carried.size());

        let mut changes = Changes::new();
        changes.import(carried.clone());
        changes.discard(carried.clone());
        assert_eq!(changes.imports().count(), 0);
        assert_eq!(changes.discards().count(), 1);

        changes.import(carried.clone());
        changes.import(stored);
        let held: Vec<_> = changes.imports().collect();
        assert_eq!(held, vec![&carried], "the carried bytes are kept");
    }

    /// Draining the asset changes leaves the fact changes in place, which is
    /// what the commit path relies on before it turns the facts into
    /// instructions.
    #[dialog_common::test]
    fn it_takes_asset_changes_and_keeps_the_facts() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));
        changes.import(Asset::new(b"avatar".to_vec()));

        let assets = changes.take_assets();
        assert_eq!(
            assets,
            vec![AssetChange::Import(Asset::new(b"avatar".to_vec()))]
        );
        assert_eq!(changes.assets().count(), 0);
        assert_eq!(changes.into_instructions().len(), 1);
    }

    #[dialog_common::test]
    fn it_merges_asset_changes() {
        let mut left = Changes::new();
        left.import(Asset::new(b"one".to_vec()));
        let mut right = Changes::new();
        right.discard(Asset::new(b"one".to_vec()));
        right.import(Asset::new(b"two".to_vec()));

        left.merge(right);
        assert_eq!(left.discards().count(), 1, "the later discard wins");
        assert_eq!(left.imports().count(), 1);
    }

    /// Asserting a batch into another carries its asset changes; retracting
    /// it inverts them, as it inverts its facts.
    #[dialog_common::test]
    fn it_replays_asset_changes_and_inverts_them_on_retract() {
        let mut batch = Changes::new();
        batch.associate(name_attr(), alice(), Value::String("Alice".into()));
        batch.import(Asset::new(b"avatar".to_vec()));

        let mut asserted = Changes::new();
        batch.clone().assert(&mut asserted);
        assert_eq!(asserted.imports().count(), 1);

        let mut retracted = Changes::new();
        batch.retract(&mut retracted);
        assert_eq!(retracted.imports().count(), 0);
        assert_eq!(retracted.discards().count(), 1);
        assert_eq!(retracted.into_instructions().len(), 1);
    }

    /// A batch that changes no asset keeps the plain fact-nesting shape, so
    /// a reader that predates assets still decodes it.
    #[dialog_common::test]
    fn it_encodes_a_batch_without_assets_as_the_plain_fact_nesting() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));

        let bytes = serde_ipld_dagcbor::to_vec(&changes).expect("encode changes");
        let facts: Facts = serde_ipld_dagcbor::from_slice(&bytes).expect("decode as fact nesting");
        assert_eq!(facts, changes.facts);

        let decoded: Changes = serde_ipld_dagcbor::from_slice(&bytes).expect("decode changes");
        assert_eq!(decoded, changes);
    }

    /// A batch with asset changes round-trips its facts and its assets,
    /// carried and stored alike, through dag-cbor and JSON.
    #[dialog_common::test]
    fn it_round_trips_assets_through_dag_cbor_and_json() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));
        changes.dissociate(name_attr(), bob(), Value::String("Bob".into()));
        changes.import(Asset::new(b"avatar".to_vec()));
        changes.import(Asset::stored([4u8; 32], 1 << 20));
        changes.discard(Asset::new(vec![0u8, 255, 7]));

        let bytes = serde_ipld_dagcbor::to_vec(&changes).expect("encode changes");
        let decoded: Changes = serde_ipld_dagcbor::from_slice(&bytes).expect("decode changes");
        assert_eq!(decoded, changes);

        let json = serde_json::to_string(&changes).expect("encode changes as json");
        let decoded: Changes = serde_json::from_str(&json).expect("decode changes from json");
        assert_eq!(decoded, changes);
    }

    /// `sort_key` must reproduce the tree's EAV key byte order exactly,
    /// including when a value spills: the tree orders same-`(the, of)` facts
    /// by the spill-FLAGGED type byte leading the value tail, so a bare
    /// (unflagged) type component would order a spilled String (tail `0x83…`)
    /// before an inline UnsignedInt (tail `0x04…`) while the tree does the
    /// opposite, corrupting the k-way merge order.
    #[dialog_common::test]
    fn it_orders_sort_keys_exactly_as_the_tree_orders_keys() {
        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let facts: Vec<Artifact> = vec![
            Value::String("z".repeat(inline_n + 1)), // spilled: tail 0x83…
            Value::UnsignedInt(1),                   // inline: tail 0x04…
            Value::String("abc".into()),             // inline: tail 0x03…
            Value::Float(1.5),                       // inline: tail 0x06…
        ]
        .into_iter()
        .map(|is| Artifact {
            the: name_attr(),
            of: alice(),
            is,
            cause: None,
        })
        .collect();

        // Both orderings must be built under the SAME manifest: that
        // agreement is the property under test.
        let manifest = Manifest::default();
        let mut by_sort_key = facts.clone();
        by_sort_key.sort_by_key(|fact| sort_key(fact, &manifest));
        let mut by_tree_key = facts;
        by_tree_key.sort_by_key(|fact| crate::EntityKey::from_artifact(fact, &manifest).into_key());

        let sorted: Vec<&Value> = by_sort_key.iter().map(|fact| &fact.is).collect();
        let expected: Vec<&Value> = by_tree_key.iter().map(|fact| &fact.is).collect();
        assert_eq!(
            sorted, expected,
            "sort_key order must equal tree key byte order"
        );
    }

    #[dialog_common::test]
    fn it_replays_changes_into_a_target_via_statement_assert() {
        let mut source = Changes::new();
        source.associate(name_attr(), alice(), Value::String("Alice".into()));
        source.dissociate(name_attr(), bob(), Value::String("Bob".into()));

        let mut target = Changes::new();
        source.assert(&mut target);

        // Replay produced one Assert + one Retract on `target`.
        let instructions: Vec<_> = target.into_instructions();
        assert_eq!(instructions.len(), 2);
        assert!(
            instructions
                .iter()
                .any(|i| matches!(i, Instruction::Assert(_)))
        );
        assert!(
            instructions
                .iter()
                .any(|i| matches!(i, Instruction::Retract(_)))
        );
    }

    #[dialog_common::test]
    fn it_inverts_changes_under_statement_retract() {
        let mut source = Changes::new();
        source.associate(name_attr(), alice(), Value::String("Alice".into()));

        let mut target = Changes::new();
        source.retract(&mut target);

        let instructions: Vec<_> = target.into_instructions();
        assert_eq!(instructions.len(), 1);
        assert!(matches!(instructions[0], Instruction::Retract(_)));
    }

    async fn artifacts(
        changes: &Changes,
        selector: ArtifactSelector<Constrained>,
    ) -> Vec<Artifact> {
        let stream = Provider::<Select<'_>>::execute(changes, selector)
            .await
            .expect("execute");
        stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|row| row.and_then(|view| view.to_owned()))
            .collect::<Result<Vec<_>, _>>()
            .expect("collect")
    }

    #[dialog_common::test]
    async fn it_yields_asserts_as_artifacts() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));

        let results = artifacts(&changes, ArtifactSelector::new().the(name_attr())).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of, alice());
        assert_eq!(results[0].is, Value::String("Alice".into()));
    }

    #[dialog_common::test]
    async fn it_yields_replaces_as_artifacts() {
        let mut changes = Changes::new();
        changes.associate_unique(name_attr(), alice(), Value::String("Alicia".into()));

        let results = artifacts(&changes, ArtifactSelector::new().of(alice())).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, Value::String("Alicia".into()));
    }

    #[dialog_common::test]
    async fn it_omits_retracts_from_the_selection() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));
        changes.dissociate(name_attr(), bob(), Value::String("Bob".into()));

        // Only the assert should surface. Retracts are deliberately
        // dropped because there's no negative-fact channel in
        // ArtifactStream.
        let results = artifacts(&changes, ArtifactSelector::new().the(name_attr())).await;
        let entities: Vec<&Entity> = results.iter().map(|a| &a.of).collect();
        assert_eq!(entities, vec![&alice()]);
    }

    #[dialog_common::test]
    async fn it_filters_by_the_of_and_is() {
        let mut changes = Changes::new();
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));
        changes.associate(name_attr(), bob(), Value::String("Bob".into()));
        changes.associate(role_attr(), alice(), Value::String("Engineer".into()));

        // Filter by `the` only
        let by_attr = artifacts(&changes, ArtifactSelector::new().the(name_attr())).await;
        assert_eq!(by_attr.len(), 2);

        // Filter by `the` + `of`
        let by_attr_entity = artifacts(
            &changes,
            ArtifactSelector::new().the(name_attr()).of(alice()),
        )
        .await;
        assert_eq!(by_attr_entity.len(), 1);
        assert_eq!(by_attr_entity[0].of, alice());

        // Filter by `is`
        let by_value = artifacts(
            &changes,
            ArtifactSelector::new()
                .the(name_attr())
                .is(Value::String("Bob".into())),
        )
        .await;
        assert_eq!(by_value.len(), 1);
        assert_eq!(by_value[0].of, bob());
    }

    #[dialog_common::test]
    async fn it_emits_artifacts_in_sort_key_order() {
        // Insert in deliberately wrong order; expect output sorted by
        // sort_key so cross-source merges interleave consistently.
        let mut changes = Changes::new();
        // Different attributes — sort by attribute key first.
        changes.associate(role_attr(), alice(), Value::String("Engineer".into()));
        changes.associate(name_attr(), alice(), Value::String("Alice".into()));

        let results = artifacts(&changes, ArtifactSelector::new().of(alice())).await;
        assert_eq!(results.len(), 2);
        // Attributes ordered by their key bytes — verify by checking
        // the output is monotonic under sort_key.
        let keys: Vec<_> = results
            .iter()
            .map(|artifact| sort_key(artifact, &Manifest::default()))
            .collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        assert_eq!(keys, sorted_keys);
    }
}
