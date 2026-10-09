use crate::artifacts::ordvalue::encode_value_owned;
use crate::artifacts::query::Select;
use crate::history::Edition;
use crate::key::value_tail_bytes;
use crate::selector::Constrained;
use crate::{
    Artifact, ArtifactSelector, ArtifactStream, Asset, Cause, DialogArtifactsError, Entity,
    Instruction, Relation, Value,
};
use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_search_tree::Manifest;
use dialog_storage::Blake3Hash;
use futures_util::Stream;
use futures_util::stream;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::mem::take;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

/// A single write operation on an `(entity, attribute)` pair.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Change {
    /// Assert a value for an entity-attribute pair under a pick: the
    /// pick the attribute is read under, which says what the write
    /// succeeds. Under [`Pick::All`] the value is appended. Under any
    /// other pick the one live claim of the cell the pick elects,
    /// the claim a read under it returns, is retracted beside the
    /// value. Committed as [`Instruction::Assert`], which elects among
    /// the cell's stored claims in the tree write itself; a transactor
    /// settles it first against the candidates the tree cannot see.
    Assert(Value, Pick),
    /// Retract a value from an entity-attribute pair.
    Retract(Value),
}

impl Change {
    /// The value the change writes or retracts.
    pub fn value(&self) -> &Value {
        match self {
            Change::Assert(value, _) | Change::Retract(value) => value,
        }
    }

    /// Whether the change is an assertion under a pick that elects:
    /// one the transactor or the tree has to settle.
    pub fn elects(&self) -> bool {
        matches!(self, Change::Assert(_, policy) if policy.elects())
    }
}

/// Which of a relation's candidates an attribute picks: the newest
/// (`last`), every one (`all`), the best ranked among listed values
/// (`top`), the greatest (`max`) or the least (`min`). An attribute is a
/// relation qualified by a value type and a pick. Every write through
/// an attribute carries its pick: a write under any pick but `all`
/// succeeds the claim the pick returns, and a write under `all`
/// appends and succeeds nothing.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Pick {
    /// The newest claim.
    Last,
    /// Every claim: a write appends.
    All,
    /// The claim with the greatest value, the newest among equals.
    Max,
    /// The claim with the least value, the newest among equals.
    Min,
    /// The claim whose value is listed first, best first; an unlisted
    /// value ranks last, and the newest wins among equals.
    Top(Vec<Value>),
}

/// The standing of a claim: the deepest revision version it carries, as
/// the edition and the hash of that version, and its cause. Ordered as
/// an election orders claims: a versioned claim beats an unversioned
/// one, a deeper version a shallower, then the cause decides.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Standing {
    /// The deepest revision version the claim carries, as its edition
    /// and the hash of the version. `None` for a claim no revision
    /// carries yet.
    pub version: Option<(Edition, [u8; 32])>,
    /// The claim's cause.
    pub cause: Cause,
}

impl Ord for Standing {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version.cmp(&other.version).then_with(|| {
            self.cause
                .partial_cmp(&other.cause)
                .unwrap_or(Ordering::Equal)
        })
    }
}

impl PartialOrd for Standing {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A claim as an election sees it: its value and its standing, the
/// latter absent for a candidate no fact stands behind.
pub type Contender<'a> = (&'a Value, Option<&'a Standing>);

/// Whether `candidate` orders after `incumbent` by value alone: the
/// values' own order where they have one, their encoding's otherwise.
pub(crate) fn value_beats(candidate: &Value, incumbent: &Value) -> bool {
    match candidate.partial_cmp(incumbent) {
        Some(ordering) => ordering.is_gt(),
        None => encode_value_owned(candidate) > encode_value_owned(incumbent),
    }
}

impl Display for Pick {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(self.name())
    }
}

impl Pick {
    /// The pick's name as the notation spells it: `last`, `all`, `top`,
    /// `max` or `min`.
    pub fn name(&self) -> &'static str {
        match self {
            Pick::Last => "last",
            Pick::All => "all",
            Pick::Top(_) => "top",
            Pick::Max => "max",
            Pick::Min => "min",
        }
    }

    /// Whether the pick returns the whole candidate set (`all`) rather
    /// than one candidate.
    pub fn is_set(&self) -> bool {
        matches!(self, Pick::All)
    }

    /// The values a `top` ranks, best first; empty for every other pick.
    pub fn ranked(&self) -> &[Value] {
        match self {
            Pick::Top(values) => values,
            _ => &[],
        }
    }

    /// Where `value` ranks under this pick: its position among
    /// the values a `top` lists, best first, an unlisted value last;
    /// zero under every other pick and under a `top` listing nothing.
    pub fn rank_of(&self, value: &Value) -> usize {
        match self {
            Pick::Top(among) => Self::rank_among(among, value),
            _ => 0,
        }
    }

    /// Where `value` ranks among `listed`, best first: an unlisted
    /// value ranks last, and every value ranks first when nothing is
    /// listed.
    pub fn rank_among(listed: &[Value], value: &Value) -> usize {
        if listed.is_empty() {
            return 0;
        }
        listed
            .iter()
            .position(|candidate| candidate == value)
            .unwrap_or(usize::MAX)
    }

    /// Whether `candidate` is the newer of two claims: the greater
    /// standing, then the greater value. This is the whole of `last`,
    /// and what every other pick falls back to among equals.
    pub fn newer(candidate: Contender<'_>, incumbent: Contender<'_>) -> bool {
        candidate.1 > incumbent.1
            || (candidate.1 == incumbent.1 && value_beats(candidate.0, incumbent.0))
    }

    /// Whether `candidate` displaces `incumbent` under this pick:
    /// the one a read under the pick returns of the two. `last` takes
    /// the newer; `top` the better rank, then the newer; `max` and `min`
    /// the greater or the lesser value, then the greater standing. This
    /// is the one ordering every election runs on, whether it elects
    /// among a cell's stored claims at the tree or among stored and
    /// derived candidates in a query.
    pub fn prefers(&self, candidate: Contender<'_>, incumbent: Contender<'_>) -> bool {
        match self {
            Pick::Last | Pick::All => Self::newer(candidate, incumbent),
            Pick::Top(_) => {
                let (mine, theirs) = (self.rank_of(candidate.0), self.rank_of(incumbent.0));
                mine < theirs || (mine == theirs && Self::newer(candidate, incumbent))
            }
            Pick::Max | Pick::Min => {
                let ordering = candidate.0.partial_cmp(incumbent.0).unwrap_or_else(|| {
                    encode_value_owned(candidate.0).cmp(&encode_value_owned(incumbent.0))
                });
                let wanted = match self {
                    Pick::Max => Ordering::Greater,
                    _ => Ordering::Less,
                };
                ordering == wanted || (ordering.is_eq() && candidate.1 > incumbent.1)
            }
        }
    }

    /// Whether a write under this pick elects a claim to succeed:
    /// every pick but `all`.
    pub fn elects(&self) -> bool {
        !matches!(self, Pick::All)
    }

    /// The claim this pick elects among `claims`, by index: the one
    /// a read under it returns. `None` over no claims, and under `all`,
    /// which elects nothing.
    pub fn elect<'a, I>(&self, claims: I) -> Option<usize>
    where
        I: IntoIterator<Item = Contender<'a>>,
    {
        if !self.elects() {
            return None;
        }
        let mut best: Option<(usize, Contender<'a>)> = None;
        for (index, claim) in claims.into_iter().enumerate() {
            best = Some(match best {
                Some(incumbent) if !self.prefers(claim, incumbent.1) => incumbent,
                _ => (index, claim),
            });
        }
        best.map(|(index, _)| index)
    }
}

/// The write side of the triple store.
///
/// Implementors accumulate fact changes (associations and dissociations)
/// that can later be committed atomically.
pub trait Update {
    /// Assert that the `attribute` of `entity` is `value`, under
    /// `policy`: the pick the attribute is read under. Under
    /// [`Pick::All`] the value is appended beside the cell's claims.
    /// Under any other pick the one live claim of the cell the
    /// pick elects is retracted when the batch commits, and every
    /// other claim of the cell stays. A batch holds one write of a
    /// cell under [`Pick::Last`]: a later one succeeds the earlier
    /// and takes its place.
    fn associate(&mut self, the: Relation, of: Entity, is: Value, policy: Pick);

    /// Retract that the `attribute` of `entity` is `value`.
    fn dissociate(&mut self, the: Relation, of: Entity, is: Value);

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
type Facts = HashMap<Entity, HashMap<Relation, Vec<Change>>>;

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

    /// Convert to an instruction stream, the form a commit applies. An
    /// assertion under a choosing pick commits as the instruction of
    /// the same shape: the tree elects among the cell's stored claims.
    /// A relation some rule derives has derived candidates the tree
    /// cannot see, so a transaction settles those writes against its
    /// view before it commits.
    pub fn into_stream(self) -> ChangeStream {
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
                        Change::Assert(value, policy) => {
                            self.associate(attribute.clone(), entity.clone(), value, policy)
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
    pub fn iter(&self) -> impl Iterator<Item = (&Entity, &Relation, &Change)> {
        self.facts.iter().flat_map(|(entity, attrs)| {
            attrs
                .iter()
                .flat_map(move |(attr, changes)| changes.iter().map(move |c| (entity, attr, c)))
        })
    }

    /// Whether any cell of this batch holds an assertion under a
    /// choosing pick, which the transactor has yet to resolve.
    pub fn has_successions(&self) -> bool {
        self.iter().any(|(_, _, change)| change.elects())
    }

    /// The cells holding an assertion under a choosing pick, each once.
    pub fn cells_with_successions(&self) -> Vec<(Relation, Entity)> {
        let mut cells = Vec::new();
        for (entity, attribute, change) in self.iter() {
            if change.elects() {
                let cell = (attribute.clone(), entity.clone());
                if !cells.contains(&cell) {
                    cells.push(cell);
                }
            }
        }
        cells
    }

    /// The changes recorded for one cell, in order, taken out of the
    /// batch. The transactor settles a cell's choosing writes by taking
    /// its changes, deciding what each write succeeds, and putting the
    /// settled changes back with [`put_cell`](Self::put_cell).
    pub fn take_cell(&mut self, the: &Relation, of: &Entity) -> Vec<Change> {
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
    pub fn put_cell(&mut self, the: Relation, of: Entity, changes: Vec<Change>) {
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
    /// batch that [`has_assets`](Self::has_assets). An assertion carries
    /// its pick into the instruction: the tree elects among the cell's
    /// stored claims when it applies one under a choosing pick.
    ///
    /// The instructions come in entity then attribute order, a cell's
    /// in the order they were recorded. A batch records its facts in a
    /// hash map, whose order differs from one process to the next, and
    /// the tree buffers the writes it is handed and flushes by what has
    /// accumulated: handed the same facts in another order it wrote
    /// another set of intermediate nodes. In a fixed order, two commits
    /// of the same facts write the same blocks.
    pub fn into_instructions(self) -> Vec<Instruction> {
        let mut instructions = Vec::new();
        for (entity, attributes) in self.facts {
            for (attribute, operations) in attributes {
                for operation in operations {
                    let instruction = match operation {
                        Change::Assert(value, policy) => Instruction::Assert(
                            Artifact {
                                the: attribute.clone(),
                                of: entity.clone(),
                                is: value,
                                cause: None,
                            },
                            policy,
                        ),
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
        let cell = |instruction: &Instruction| {
            let (Instruction::Assert(artifact, _) | Instruction::Retract(artifact)) = instruction;
            (artifact.of.clone(), artifact.the.clone())
        };
        instructions.sort_by_cached_key(cell);
        instructions
    }
}

impl Update for Changes {
    fn associate(&mut self, the: Relation, of: Entity, is: Value, policy: Pick) {
        let cell = self.facts.entry(of).or_default().entry(the).or_default();
        // A write under `last` succeeds the cell's newest claim. The
        // writes of one batch stand at one edition, so the batch's own
        // earlier `last` write is that claim only when it is the cell's
        // latest change and no other assertion of the cell came before
        // it: then it would be retracted the moment it landed, and this
        // write succeeds what it succeeded, so it takes its place. With
        // another assertion of the cell in the batch the two claims
        // stand equal and either may be the one elected, so every write
        // stays, in order, and is replayed as written. Every other
        // pick elects by value, where the earlier write may stand.
        if policy == Pick::Last
            && let Some((Change::Assert(_, Pick::Last), before)) = cell.split_last()
            && before
                .iter()
                .all(|change| matches!(change, Change::Retract(_)))
        {
            cell.pop();
        }
        cell.push(Change::Assert(is, policy));
    }

    fn dissociate(&mut self, the: Relation, of: Entity, is: Value) {
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

/// A batch collected from instructions, each recorded as the change it
/// stands for: what a caller holding instructions hands a transaction.
impl FromIterator<Instruction> for Changes {
    fn from_iter<I: IntoIterator<Item = Instruction>>(instructions: I) -> Self {
        let mut changes = Changes::new();
        for instruction in instructions {
            match instruction {
                Instruction::Assert(a, policy) => changes.associate(a.the, a.of, a.is, policy),
                Instruction::Retract(a) => changes.dissociate(a.the, a.of, a.is),
            }
        }
        changes
    }
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
/// a changes-shaped overlay into a query session. `Assert` maps to
/// `associate` on the target, with its pick; `Retract` maps to
/// `dissociate`.
///
/// Retracting a batch inverts its asset changes as it inverts its facts:
/// an import becomes a discard and a discard an import.
impl Statement for Changes {
    /// Replay every change into `update` as it was recorded, each
    /// assertion under its pick, so a batch asserted into another
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
                        Change::Assert(value, policy) => {
                            update.associate(attribute.clone(), entity.clone(), value, policy)
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
        // Inverse: asserts become retracts; existing retracts become
        // appends. Symmetric so `c.assert(t); c.retract(t);` round-trips
        // when `t` is a fresh target.
        for instruction in self.into_instructions() {
            match instruction {
                Instruction::Assert(a, _) => update.dissociate(a.the, a.of, a.is),
                Instruction::Retract(a) => update.associate(a.the, a.of, a.is, Pick::All),
            }
        }
    }
}

/// `Provider<Select>` for an in-memory [`Changes`] batch.
///
/// Treats `Changes` as a queryable source: `Assert` entries surface as
/// [`Artifact`]s matching the [`ArtifactSelector`]'s
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
                        Change::Assert(v, _) => v,
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
    /// The instructions of a batch come in entity then attribute order,
    /// a cell's in the order they were recorded, however the facts were
    /// recorded: the batch the tree is handed is the same from one
    /// process to the next.
    #[dialog_common::test]
    fn it_orders_a_batchs_instructions() {
        use super::{Changes, Instruction, Pick, Value};
        let fact = |of: &str, the: &str, is: &str| -> Instruction {
            Instruction::Assert(
                super::Artifact {
                    the: the.parse().expect("attribute"),
                    of: of.parse().expect("entity"),
                    is: Value::String(is.into()),
                    cause: None,
                },
                Pick::All,
            )
        };
        let recorded = || {
            [
                fact("id:b", "stuff/role", "x"),
                fact("id:a", "stuff/role", "y"),
                fact("id:b", "stuff/name", "first"),
                fact("id:a", "stuff/name", "z"),
                fact("id:b", "stuff/name", "second"),
            ]
        };
        let forward: Changes = recorded().into_iter().collect();
        let backward: Changes = recorded().into_iter().rev().collect();
        let cells = |changes: Changes| -> Vec<(String, String, String)> {
            changes
                .into_instructions()
                .into_iter()
                .map(|instruction| {
                    let (Instruction::Assert(artifact, _) | Instruction::Retract(artifact)) =
                        instruction;
                    let Value::String(is) = artifact.is else {
                        panic!("a text value")
                    };
                    (artifact.of.to_string(), artifact.the.to_string(), is)
                })
                .collect()
        };
        let expected: Vec<(String, String, String)> = [
            ("id:a", "stuff/name", "z"),
            ("id:a", "stuff/role", "y"),
            ("id:b", "stuff/name", "first"),
            ("id:b", "stuff/name", "second"),
            ("id:b", "stuff/role", "x"),
        ]
        .into_iter()
        .map(|(of, the, is)| (of.into(), the.into(), is.into()))
        .collect();
        assert_eq!(cells(forward), expected);
        let mut reversed = cells(backward);
        // Recorded backwards, a cell's two values come in that order.
        reversed.swap(2, 3);
        assert_eq!(reversed, expected);
    }

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
    fn name_attr() -> Relation {
        "test/name".parse().expect("valid attribute")
    }
    fn role_attr() -> Relation {
        "test/role".parse().expect("valid attribute")
    }

    /// A batch round-trips through dag-cbor with its retractions and
    /// cardinality-one replacements intact, so a session overlay can be
    /// carried as bytes into another process.
    #[dialog_common::test]
    fn it_round_trips_changes_through_dag_cbor() {
        let mut changes = Changes::new();
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Ally".into()),
            crate::Pick::All,
        );
        changes.associate(
            role_attr(),
            alice(),
            Value::String("admin".into()),
            crate::Pick::Last,
        );
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
            Some(Change::Assert(
                Value::String("admin".into()),
                crate::Pick::Last
            )),
            "a replacement stays a replacement"
        );
    }

    /// Two `last` writes of one cell in one batch keep the later alone
    /// when the earlier is the cell's only assertion, the earlier being
    /// the claim the later succeeds. With another assertion of the cell
    /// before them every write stays, since the batch's claims stand
    /// equal and the later write may elect either.
    #[dialog_common::test]
    fn it_keeps_the_later_of_two_last_writes_of_a_cell() {
        let mut changes = Changes::new();
        changes.associate(
            role_attr(),
            alice(),
            Value::String("staff".into()),
            crate::Pick::All,
        );
        changes.associate(
            role_attr(),
            alice(),
            Value::String("member".into()),
            crate::Pick::Last,
        );
        changes.associate(
            role_attr(),
            alice(),
            Value::String("admin".into()),
            crate::Pick::Last,
        );
        changes.associate(
            role_attr(),
            alice(),
            Value::String("owner".into()),
            crate::Pick::Max,
        );
        changes.associate(
            role_attr(),
            bob(),
            Value::String("visitor".into()),
            crate::Pick::Last,
        );
        changes.associate(
            role_attr(),
            bob(),
            Value::String("guest".into()),
            crate::Pick::Last,
        );

        let of_alice: Vec<Change> = changes
            .iter()
            .filter(|(entity, attribute, _)| **entity == alice() && **attribute == role_attr())
            .map(|(_, _, change)| change.clone())
            .collect();
        assert_eq!(
            of_alice,
            vec![
                Change::Assert(Value::String("staff".into()), crate::Pick::All),
                Change::Assert(Value::String("member".into()), crate::Pick::Last),
                Change::Assert(Value::String("admin".into()), crate::Pick::Last),
                Change::Assert(Value::String("owner".into()), crate::Pick::Max),
            ],
            "staff stands at the batch's edition beside member, so admin may elect either: every write is replayed"
        );
        let of_bob: Vec<Change> = changes
            .iter()
            .filter(|(entity, _, _)| **entity == bob())
            .map(|(_, _, change)| change.clone())
            .collect();
        assert_eq!(
            of_bob,
            vec![Change::Assert(
                Value::String("guest".into()),
                crate::Pick::Last
            )]
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
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );
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
        batch.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );
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
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );

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
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );
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
        source.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );
        source.dissociate(name_attr(), bob(), Value::String("Bob".into()));

        let mut target = Changes::new();
        source.assert(&mut target);

        // Replay produced one Assert + one Retract on `target`.
        let instructions: Vec<_> = target.into_instructions();
        assert_eq!(instructions.len(), 2);
        assert!(
            instructions
                .iter()
                .any(|i| matches!(i, Instruction::Assert(.., crate::Pick::All)))
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
        source.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );

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
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );

        let results = artifacts(&changes, ArtifactSelector::new().the(name_attr())).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of, alice());
        assert_eq!(results[0].is, Value::String("Alice".into()));
    }

    #[dialog_common::test]
    async fn it_yields_replaces_as_artifacts() {
        let mut changes = Changes::new();
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alicia".into()),
            crate::Pick::Last,
        );

        let results = artifacts(&changes, ArtifactSelector::new().of(alice())).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, Value::String("Alicia".into()));
    }

    #[dialog_common::test]
    async fn it_omits_retracts_from_the_selection() {
        let mut changes = Changes::new();
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );
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
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );
        changes.associate(
            name_attr(),
            bob(),
            Value::String("Bob".into()),
            crate::Pick::All,
        );
        changes.associate(
            role_attr(),
            alice(),
            Value::String("Engineer".into()),
            crate::Pick::All,
        );

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
        changes.associate(
            role_attr(),
            alice(),
            Value::String("Engineer".into()),
            crate::Pick::All,
        );
        changes.associate(
            name_attr(),
            alice(),
            Value::String("Alice".into()),
            crate::Pick::All,
        );

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
