//! The ephemeral layer: a memory-backed store with a head and an
//! instant log, no history, gone with the process.
//!
//! Every branch and snapshot carries one ([`Branch::overlay`],
//! [`Snapshot::overlay`](crate::Snapshot::overlay)): the store behind
//! the procedural layer (see [`placement`](crate::placement)) and the
//! home of session facts folded into every read of the layer but never
//! committed to its tree. It is built to sit under a reactive UI:
//!
//! - **Reads are range scans in the tree's own order.** Facts are held
//!   under the same three index keys the tree uses (entity, attribute,
//!   and value orders), so a selector's [`selector_range`] applies
//!   unchanged and the rows come out in exactly the order a tree scan
//!   would produce them, which is what lets the query layer's k-way
//!   merge interleave this store with branch scans.
//! - **Writes are set operations with the tree's semantics.** An
//!   assert is idempotent, a replace supersedes the other values at
//!   its `(entity, attribute)` cell, a retract removes the exact
//!   triple. A retract of a fact the store does not hold is a
//!   *tombstone* that hides the same fact in the layers beneath, which
//!   is how a session shadows a committed fact without touching the
//!   tree.
//! - **Every change is an instant.** A write that changes what readers
//!   see mints one [`Instant`]: the facts that became readable, the
//!   facts that stopped being readable, a sequence number, and a
//!   chained hash. Nothing is hashed but the delta, so a commit costs
//!   the delta, never the store.
//! - **Observers see instants, not folds.** There is no shared log.
//!   Anything that wants the instants rather than the fold — a
//!   subscription maintaining its result per touched entity, a command
//!   provider that must see a fact asserted and retracted within one
//!   commit — registers an [`Observer`] with a demand, and every
//!   instant is fanned out at write time into each observer's own
//!   bounded queue, filtered to the facts its demand covers. An
//!   instant nobody demanded costs nothing; memory is the sum of
//!   unconsumed matched instants across observers. An observer that
//!   falls off its queue's bound finds a gap on its next drain and
//!   recomputes from the fold. Dropping the observer unregisters it.
//!   An instant can also be [witnessed](Ephemeral::witness): minted
//!   for observers without changing the store, which is how a
//!   transient that lived for one induction round is seen at all.
//!
//! A [`Revision`](EphemeralRevision) here is an identity, not a
//! persistence claim: the sequence plus the chained hash of every
//! instant so far. Two stores reaching the same state by different
//! paths carry different hashes, which is the trade for never hashing
//! the fold.
//!
//! [`selector_range`]: dialog_artifacts::tree::selector_range
//! [`Branch::overlay`]: crate::Branch::overlay

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Weak};

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::selector_range;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStream, AttributeKey, Changes, DialogArtifactsError,
    Entity, EntityKey, Instruction, Key, Select, SortKey, Statement, ValueKey, default_sort_key,
};
use dialog_capability::Provider;
use dialog_common::Blake3Hash;
use dialog_search_tree::Manifest;
use futures_util::stream;
use parking_lot::{Mutex, RwLock};

use crate::Demand;

/// How many matched instants an observer's queue holds before it
/// gaps. An observer that drains less often than this many matching
/// writes land recomputes from the fold instead of maintaining.
pub(crate) const QUEUE_CAPACITY: usize = 1024;

/// The identity of an ephemeral layer at some instant: how many
/// instants have been minted and the hash chained through all of
/// them. The hash of the empty store is the all-zero hash.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EphemeralRevision {
    /// Instants minted so far; zero for a store nothing has changed.
    pub sequence: u64,
    /// `blake3(previous ‖ sequence ‖ delta)`, chained from zero.
    pub hash: Blake3Hash,
}

/// One change to what readers of the layer see.
#[derive(Clone, Debug, PartialEq)]
pub struct Instant {
    /// The sequence this instant minted; the store's revision after
    /// it is `(sequence, hash)`.
    pub sequence: u64,
    /// The chained hash after this instant.
    pub hash: Blake3Hash,
    /// Facts that became readable: stored, or un-shadowed beneath.
    pub asserted: Vec<Artifact>,
    /// Facts that stopped being readable: removed, or shadowed
    /// beneath by a tombstone.
    pub retracted: Vec<Artifact>,
}

/// A memory-backed layer. Cheap to clone: clones share the store, so a
/// fact asserted through any handle is visible to readers of all of
/// them. See the [module docs](self).
#[derive(Clone, Debug)]
pub struct Ephemeral {
    /// A nonce minted when the store is created: the address a stack
    /// records for this layer. Two stores never share one, and every
    /// clone of a store carries the same.
    entity: Entity,
    state: Arc<RwLock<State>>,
}

impl Default for Ephemeral {
    fn default() -> Self {
        Self {
            entity: Entity::new().expect("the platform can mint a random entity"),
            state: Arc::default(),
        }
    }
}

#[derive(Debug)]
struct State {
    /// Every held fact under each of its three index keys. The key's
    /// tag byte keeps the three orders apart, so one map serves every
    /// selector shape.
    facts: BTreeMap<Key, Artifact>,
    /// Facts held beneath this layer that the session hides, by sort
    /// key, with the fact kept so lifting the tombstone can report
    /// what became readable again. Shared with readers by `Arc` and
    /// rebuilt on change, so a read never copies the set.
    tombstones: Arc<HashSet<SortKey>>,
    shadowed: HashMap<SortKey, Artifact>,
    /// The key format facts are keyed under. Fixed to the default
    /// manifest, the same one every tree carries today and the one
    /// [`Demand`](crate::Demand) ranges are built under.
    manifest: Manifest,
    sequence: u64,
    hash: Blake3Hash,
    /// Every registered observer's queue, weakly: an observer that was
    /// dropped is pruned at the next fan-out.
    observers: Vec<Weak<Mutex<Queue>>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            facts: BTreeMap::new(),
            tombstones: Arc::new(HashSet::new()),
            shadowed: HashMap::new(),
            manifest: Manifest::default(),
            sequence: 0,
            hash: Blake3Hash::from([0u8; 32]),
            observers: Vec::new(),
        }
    }
}

/// The three index keys of a fact under `manifest`.
fn index_keys(fact: &Artifact, manifest: &Manifest) -> [Key; 3] {
    [
        EntityKey::from_artifact(fact, manifest).into_key(),
        AttributeKey::from_artifact(fact, manifest).into_key(),
        ValueKey::from_artifact(fact, manifest).into_key(),
    ]
}

/// The delta one write is accumulating, before it is minted.
#[derive(Default)]
struct Delta {
    asserted: Vec<Artifact>,
    retracted: Vec<Artifact>,
    tombstones_changed: bool,
}

impl Delta {
    fn is_empty(&self) -> bool {
        self.asserted.is_empty() && self.retracted.is_empty()
    }
}

impl State {
    /// Whether the store holds exactly this triple.
    fn holds(&self, fact: &Artifact) -> bool {
        self.facts
            .contains_key(&EntityKey::from_artifact(fact, &self.manifest).into_key())
    }

    fn insert(&mut self, fact: Artifact, delta: &mut Delta) {
        if self.holds(&fact) {
            return;
        }
        for key in index_keys(&fact, &self.manifest) {
            self.facts.insert(key, fact.clone());
        }
        delta.asserted.push(fact);
    }

    fn remove(&mut self, fact: &Artifact, delta: &mut Delta) -> bool {
        if !self.holds(fact) {
            return false;
        }
        for key in index_keys(fact, &self.manifest) {
            self.facts.remove(&key);
        }
        delta.retracted.push(fact.clone());
        true
    }

    /// Every held fact at an `(entity, attribute)` cell.
    fn cell(&self, of: &Entity, the: &dialog_artifacts::Attribute) -> Vec<Artifact> {
        let selector = ArtifactSelector::new().of(of.clone()).the(the.clone());
        self.facts
            .range(selector_range(&selector, &self.manifest))
            .map(|(_, fact)| fact.clone())
            .collect()
    }

    fn apply(&mut self, instruction: Instruction, delta: &mut Delta) {
        match instruction {
            Instruction::Assert(fact) => self.insert(fact, delta),
            Instruction::Replace(fact) => {
                let mut standing = false;
                for prior in self.cell(&fact.of, &fact.the) {
                    if prior.is == fact.is {
                        standing = true;
                    } else {
                        self.remove(&prior, delta);
                    }
                }
                if !standing {
                    self.insert(fact, delta);
                }
            }
            Instruction::Retract(fact) => {
                if self.remove(&fact, delta) {
                    return;
                }
                // Not held here: hide it beneath. A tombstone is a
                // change readers see (the fact disappears), so it is
                // reported as retracted.
                if let Entry::Vacant(slot) = self.shadowed.entry(default_sort_key(&fact)) {
                    slot.insert(fact.clone());
                    delta.tombstones_changed = true;
                    delta.retracted.push(fact);
                }
            }
        }
    }

    /// Mint an instant for a non-empty delta, advancing the sequence
    /// and the chained hash and recording it in the ring. Returns the
    /// instant, or `None` when nothing readers see changed.
    fn mint(&mut self, delta: Delta) -> Option<Instant> {
        if delta.tombstones_changed {
            self.tombstones = Arc::new(self.shadowed.keys().cloned().collect());
        }
        if delta.is_empty() {
            return None;
        }
        self.sequence += 1;
        let mut chunks: Vec<Vec<u8>> =
            Vec::with_capacity(2 + delta.asserted.len() + delta.retracted.len());
        chunks.push(self.hash.as_bytes().to_vec());
        chunks.push(self.sequence.to_be_bytes().to_vec());
        for (polarity, facts) in [(b'+', &delta.asserted), (b'-', &delta.retracted)] {
            for fact in facts {
                let (the, of, tail) = default_sort_key(fact);
                let mut chunk = Vec::with_capacity(1 + the.len() + of.len() + tail.len() + 2);
                chunk.push(polarity);
                chunk.extend(the);
                chunk.push(0);
                chunk.extend(of);
                chunk.push(0);
                chunk.extend(tail);
                chunks.push(chunk);
            }
        }
        self.hash = Blake3Hash::hash_iter(chunks.iter().map(Vec::as_slice));
        let instant = Instant {
            sequence: self.sequence,
            hash: self.hash.clone(),
            asserted: delta.asserted,
            retracted: delta.retracted,
        };
        self.fan_out(&instant);
        Some(instant)
    }

    /// Hand `instant` to every live observer, filtered to the facts
    /// its demand covers; prune observers that were dropped.
    fn fan_out(&mut self, instant: &Instant) {
        self.observers.retain(|weak| weak.strong_count() > 0);
        for weak in &self.observers {
            let Some(queue) = weak.upgrade() else {
                continue;
            };
            let mut queue = queue.lock();
            if queue.gapped {
                continue;
            }
            let Some(matched) = queue.filter.matched(instant, &self.manifest) else {
                continue;
            };
            if queue.instants.len() == QUEUE_CAPACITY {
                queue.instants.clear();
                queue.gapped = true;
            } else {
                queue.instants.push_back(matched);
            }
        }
    }
}

/// What an observer's queue admits.
#[derive(Clone, Debug)]
enum Filter {
    /// Every instant, whole.
    Everything,
    /// The facts whose index keys fall inside the demand's cover.
    Demand(Demand),
}

impl Filter {
    /// The part of `instant` this filter admits, or `None` when it
    /// admits nothing of it.
    fn matched(&self, instant: &Instant, manifest: &Manifest) -> Option<Instant> {
        match self {
            Filter::Everything => Some(instant.clone()),
            Filter::Demand(demand) => {
                let covers = |fact: &Artifact| {
                    index_keys(fact, manifest)
                        .iter()
                        .any(|key| demand.covers(key))
                };
                let asserted: Vec<Artifact> = instant
                    .asserted
                    .iter()
                    .filter(|f| covers(f))
                    .cloned()
                    .collect();
                let retracted: Vec<Artifact> = instant
                    .retracted
                    .iter()
                    .filter(|f| covers(f))
                    .cloned()
                    .collect();
                if asserted.is_empty() && retracted.is_empty() {
                    return None;
                }
                Some(Instant {
                    sequence: instant.sequence,
                    hash: instant.hash.clone(),
                    asserted,
                    retracted,
                })
            }
        }
    }
}

/// One observer's queue: the instants that matched its filter since
/// its last drain, and whether the queue overflowed.
#[derive(Debug)]
struct Queue {
    filter: Filter,
    instants: VecDeque<Instant>,
    gapped: bool,
}

/// What an observer finds when it drains.
#[derive(Clone, Debug, PartialEq)]
pub enum Drained {
    /// The instants that matched since the last drain, oldest first;
    /// empty when nothing did.
    Instants(Vec<Instant>),
    /// The observer fell behind its queue's bound and instants were
    /// dropped: recompute from the fold. `sequence` is where the layer
    /// stands at the drain.
    Gap {
        /// The layer's sequence at the drain.
        sequence: u64,
    },
}

/// A registration to see a layer's instants. Created by
/// [`Ephemeral::observe`]; drained by [`drain`](Self::drain); dropping
/// it unregisters. Each observer owns its queue: what one drains no
/// other misses.
#[derive(Debug)]
pub struct Observer {
    layer: Ephemeral,
    queue: Arc<Mutex<Queue>>,
}

impl Observer {
    /// Take every instant queued since the last drain, or the gap
    /// marker if the queue overflowed. Draining a gap clears it, so
    /// the next drain starts collecting again.
    pub fn drain(&self) -> Drained {
        let mut queue = self.queue.lock();
        if queue.gapped {
            queue.gapped = false;
            queue.instants.clear();
            return Drained::Gap {
                sequence: self.layer.revision().sequence,
            };
        }
        Drained::Instants(queue.instants.drain(..).collect())
    }

    /// Narrow the observer to the facts `demand` covers from now on.
    /// Instants already queued are kept as they were admitted.
    pub fn retarget(&self, demand: Demand) {
        self.queue.lock().filter = Filter::Demand(demand);
    }

    /// Widen the observer to every instant from now on.
    pub fn retarget_everything(&self) {
        self.queue.lock().filter = Filter::Everything;
    }

    /// The layer observed.
    pub fn layer(&self) -> &Ephemeral {
        &self.layer
    }
}

impl Ephemeral {
    /// An empty layer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Assert a statement: its asserts and replaces land in the store
    /// with the tree's semantics, its retracts remove or tombstone.
    /// Chainable; use [`apply`](Self::apply) to get the instant minted.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn assert<S: Statement>(&self, statement: S) -> &Self {
        let mut changes = Changes::new();
        statement.assert(&mut changes);
        self.apply(changes);
        self
    }

    /// Retract a statement: each of its facts is removed from the
    /// store if held here, and otherwise hidden beneath by a
    /// tombstone. Chainable.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn retract<S: Statement>(&self, statement: S) -> &Self {
        let mut changes = Changes::new();
        statement.retract(&mut changes);
        self.apply(changes);
        self
    }

    /// Land a batch of instructions as one instant.
    pub(crate) fn apply(&self, changes: Changes) -> Option<Instant> {
        if changes.is_empty() {
            return None;
        }
        let mut state = self.state.write();
        let mut delta = Delta::default();
        for instruction in changes.into_instructions() {
            state.apply(instruction, &mut delta);
        }
        state.mint(delta)
    }

    /// Drop every fact and tombstone recorded for entities that fail
    /// `keep`, outright rather than by tombstoning. The
    /// garbage-collection primitive for per-client facts keyed by
    /// short-lived entities. Returns whether anything was dropped.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn retain_entities<F: FnMut(&Entity) -> bool>(&self, mut keep: F) -> bool {
        let mut state = self.state.write();
        let mut delta = Delta::default();
        let dropped: Vec<Artifact> = state
            .facts
            .values()
            .filter(|fact| !keep(&fact.of))
            .cloned()
            .collect();
        // Each fact appears under three keys; `remove` is idempotent.
        for fact in dropped {
            state.remove(&fact, &mut delta);
        }
        let lifted: Vec<SortKey> = state
            .shadowed
            .iter()
            .filter(|(_, fact)| !keep(&fact.of))
            .map(|(key, _)| key.clone())
            .collect();
        for key in lifted {
            if let Some(fact) = state.shadowed.remove(&key) {
                delta.tombstones_changed = true;
                delta.asserted.push(fact);
            }
        }
        state.mint(delta).is_some()
    }

    /// Drop every fact and tombstone. Chainable.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn clear(&self) -> &Self {
        let mut state = self.state.write();
        let mut delta = Delta::default();
        let held: Vec<Artifact> = state.facts.values().cloned().collect();
        for fact in held {
            state.remove(&fact, &mut delta);
        }
        let lifted: Vec<Artifact> = state.shadowed.drain().map(|(_, fact)| fact).collect();
        if !lifted.is_empty() {
            delta.tombstones_changed = true;
            delta.asserted.extend(lifted);
        }
        state.mint(delta);
        self
    }

    /// The address of this store: a nonce entity minted when it was
    /// created, shared by every clone of it. A stack records it in
    /// the link facts pointing at this layer.
    pub fn entity(&self) -> &Entity {
        &self.entity
    }

    /// Whether `other` is a handle to this same store.
    pub fn is(&self, other: &Ephemeral) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// The layer's identity now.
    pub fn revision(&self) -> EphemeralRevision {
        let state = self.state.read();
        EphemeralRevision {
            sequence: state.sequence,
            hash: state.hash.clone(),
        }
    }

    /// Register an observer of the facts `demand` covers. Every
    /// instant minted from now on is queued for it, filtered to the
    /// covered facts; see [`Observer`].
    pub fn observe(&self, demand: Demand) -> Observer {
        self.register(Filter::Demand(demand))
    }

    /// Register an observer of every instant, whole.
    pub fn observe_everything(&self) -> Observer {
        self.register(Filter::Everything)
    }

    fn register(&self, filter: Filter) -> Observer {
        let queue = Arc::new(Mutex::new(Queue {
            filter,
            instants: VecDeque::new(),
            gapped: false,
        }));
        self.state.write().observers.push(Arc::downgrade(&queue));
        Observer {
            layer: self.clone(),
            queue,
        }
    }

    /// Mint an instant for observers without changing the store: the
    /// statement's asserted facts appear as both asserted and
    /// retracted, its retracted facts as retracted. This is how a
    /// fact that lived for one induction round — asserted and gone
    /// within a commit, so never in any fold — is seen by whoever
    /// registered to see it. Advances the sequence and the chained
    /// hash like any instant: a revision is an identity of what
    /// happened, not of what is held.
    pub(crate) fn witness(&self, changes: Changes) -> Option<Instant> {
        if changes.is_empty() {
            return None;
        }
        let mut delta = Delta::default();
        for instruction in changes.into_instructions() {
            match instruction {
                Instruction::Assert(fact) | Instruction::Replace(fact) => {
                    delta.asserted.push(fact.clone());
                    delta.retracted.push(fact);
                }
                Instruction::Retract(fact) => delta.retracted.push(fact),
            }
        }
        self.state.write().mint(delta)
    }

    /// How many observers are registered and alive.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn observers(&self) -> usize {
        self.state
            .read()
            .observers
            .iter()
            .filter(|weak| weak.strong_count() > 0)
            .count()
    }

    /// Sort keys of every fact this layer hides beneath it. Shared, so
    /// a read never copies the set.
    pub(crate) fn tombstones(&self) -> Arc<HashSet<SortKey>> {
        self.state.read().tombstones.clone()
    }

    /// Whether the store holds no facts (tombstones aside).
    pub fn is_empty(&self) -> bool {
        self.state.read().facts.is_empty()
    }

    /// The number of facts held.
    pub fn len(&self) -> usize {
        // Every fact sits under exactly three keys.
        self.state.read().facts.len() / 3
    }

    /// The facts a selector matches, in the order a tree scan of the
    /// same selector would produce them.
    pub fn scan(&self, selector: &ArtifactSelector<Constrained>) -> Vec<Artifact> {
        let state = self.state.read();
        state
            .facts
            .range(selector_range(selector, &state.manifest))
            .map(|(_, fact)| fact.clone())
            .collect()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<'a> Provider<Select<'a>> for Ephemeral {
    async fn execute(
        &self,
        input: ArtifactSelector<Constrained>,
    ) -> Result<ArtifactStream<'a>, DialogArtifactsError> {
        let rows = self.scan(&input);
        Ok(Box::pin(stream::iter(
            rows.into_iter().map(|fact| Ok(fact.into())),
        )))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_artifacts::{Update as _, Value};
    use dialog_query::the;

    fn fact(of: &str, the_: &str, is: &str) -> Artifact {
        Artifact {
            the: the_.parse().expect("attribute"),
            of: of.parse().expect("entity"),
            is: Value::String(is.into()),
            cause: None,
        }
    }

    /// A raw fact as a statement.
    struct Claim(Artifact);

    impl Statement for Claim {
        fn assert(self, update: &mut impl dialog_artifacts::Update) {
            update.associate(self.0.the, self.0.of, self.0.is);
        }

        fn retract(self, update: &mut impl dialog_artifacts::Update) {
            update.dissociate(self.0.the, self.0.of, self.0.is);
        }
    }

    fn claim(of: &str, the_: &str, is: &str) -> Claim {
        Claim(fact(of, the_, is))
    }

    /// The single instant an observer of everything saw, or a panic.
    fn only(observer: &Observer) -> Instant {
        match observer.drain() {
            Drained::Instants(mut instants) if instants.len() == 1 => instants.remove(0),
            other => panic!("expected exactly one instant, got {other:?}"),
        }
    }

    fn values(line: &Ephemeral, of: &str, the_: &str) -> Vec<Value> {
        let selector = ArtifactSelector::new()
            .of(of.parse().expect("entity"))
            .the(the_.parse().expect("attribute"));
        line.scan(&selector).into_iter().map(|f| f.is).collect()
    }

    #[dialog_common::test]
    fn it_asserts_idempotently_and_replaces_per_cell() {
        let line = Ephemeral::new();
        let observer = line.observe_everything();
        line.assert(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        );
        let first = only(&observer);
        assert_eq!(first.sequence, 1);
        assert_eq!(first.asserted, vec![fact("id:a", "person/name", "A")]);
        line.assert(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        );
        assert_eq!(
            line.revision().sequence,
            1,
            "re-asserting a held fact changes nothing"
        );
        assert_eq!(line.len(), 1);

        // A second value accumulates; a replace supersedes both.
        line.assert(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("B".to_string()),
        );
        assert_eq!(values(&line, "id:a", "person/name").len(), 2);
        let mut changes = Changes::new();
        changes.associate_unique(
            "person/name".parse().unwrap(),
            "id:a".parse().unwrap(),
            Value::String("C".into()),
        );
        let replaced = line.apply(changes).expect("replace mints");
        assert_eq!(replaced.retracted.len(), 2);
        assert_eq!(replaced.asserted, vec![fact("id:a", "person/name", "C")]);
        assert_eq!(
            values(&line, "id:a", "person/name"),
            vec![Value::String("C".into())]
        );
        assert_eq!(line.revision().sequence, 3);
    }

    #[dialog_common::test]
    fn it_removes_held_facts_and_tombstones_absent_ones() {
        let line = Ephemeral::new();
        line.assert(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        );
        let observer = line.observe_everything();
        line.retract(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        );
        let removed = only(&observer);
        assert_eq!(removed.retracted, vec![fact("id:a", "person/name", "A")]);
        assert!(line.is_empty());
        assert!(
            line.tombstones().is_empty(),
            "a held fact is removed, not shadowed"
        );

        line.retract(
            the!("person/name")
                .of("id:b".parse().unwrap())
                .is("B".to_string()),
        );
        let shadowed = only(&observer);
        assert_eq!(shadowed.retracted, vec![fact("id:b", "person/name", "B")]);
        assert_eq!(line.tombstones().len(), 1);
        line.retract(
            the!("person/name")
                .of("id:b".parse().unwrap())
                .is("B".to_string()),
        );
        assert_eq!(
            line.revision().sequence,
            3,
            "a standing tombstone changes nothing"
        );

        line.clear();
        let lifted = only(&observer);
        assert_eq!(lifted.asserted, vec![fact("id:b", "person/name", "B")]);
        assert!(line.tombstones().is_empty());
    }

    #[dialog_common::test]
    fn it_scans_in_tree_order_for_every_selector_shape() {
        let line = Ephemeral::new();
        for (of, the_, is) in [
            ("id:b", "person/name", "Bob"),
            ("id:a", "person/name", "Alice"),
            ("id:a", "person/role", "Admin"),
            ("id:c", "person/role", "Admin"),
        ] {
            line.assert(claim(of, the_, is));
        }
        // Attribute scan: entity order within the attribute.
        let by_attr: Vec<String> = line
            .scan(&ArtifactSelector::new().the("person/name".parse().unwrap()))
            .into_iter()
            .map(|f| f.of.to_string())
            .collect();
        assert_eq!(by_attr, vec!["id:a", "id:b"]);
        // Entity scan: attribute order within the entity.
        let by_entity: Vec<String> = line
            .scan(&ArtifactSelector::new().of("id:a".parse().unwrap()))
            .into_iter()
            .map(|f| f.the.to_string())
            .collect();
        assert_eq!(by_entity, vec!["person/name", "person/role"]);
        // Value scan: every entity holding the value.
        let by_value: Vec<String> = line
            .scan(
                &ArtifactSelector::new()
                    .the("person/role".parse().unwrap())
                    .is(Value::String("Admin".into())),
            )
            .into_iter()
            .map(|f| f.of.to_string())
            .collect();
        assert_eq!(by_value, vec!["id:a", "id:c"]);
        // Exact triple.
        assert_eq!(
            line.scan(
                &ArtifactSelector::new()
                    .of("id:b".parse().unwrap())
                    .the("person/name".parse().unwrap())
                    .is(Value::String("Bob".into()))
            )
            .len(),
            1
        );
        assert_eq!(line.len(), 4);
    }

    #[dialog_common::test]
    fn it_queues_matched_instants_per_observer_and_gaps_past_the_bound() {
        let line = Ephemeral::new();
        let everything = line.observe_everything();
        let names = {
            let demand = Demand::new();
            demand.record(&ArtifactSelector::new().the("person/name".parse().unwrap()));
            line.observe(demand)
        };
        assert_eq!(everything.drain(), Drained::Instants(Vec::new()));

        line.assert(claim("id:a", "person/name", "A"));
        line.assert(claim("id:b", "person/role", "Admin"));
        let Drained::Instants(all) = everything.drain() else {
            panic!("no gap")
        };
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].sequence, 1);
        assert_eq!(all[1].asserted, vec![fact("id:b", "person/role", "Admin")]);
        let Drained::Instants(matched) = names.drain() else {
            panic!("no gap")
        };
        assert_eq!(
            matched.len(),
            1,
            "an instant outside the demand is never queued"
        );
        assert_eq!(matched[0].asserted, vec![fact("id:a", "person/name", "A")]);
        assert_eq!(
            everything.drain(),
            Drained::Instants(Vec::new()),
            "a drain empties the queue"
        );

        for index in 0..=QUEUE_CAPACITY {
            line.assert(claim(&format!("id:{index}"), "person/tag", "x"));
        }
        let head = line.revision().sequence;
        assert_eq!(
            everything.drain(),
            Drained::Gap { sequence: head },
            "an observer past the bound must recompute"
        );
        assert_eq!(
            names.drain(),
            Drained::Instants(Vec::new()),
            "the tags never matched the names observer"
        );
        line.assert(claim("id:z", "person/name", "Z"));
        assert_eq!(
            only(&everything).asserted,
            vec![fact("id:z", "person/name", "Z")],
            "a drained gap collects again"
        );
    }

    #[dialog_common::test]
    fn it_filters_an_instant_to_the_covered_facts() {
        let line = Ephemeral::new();
        let demand = Demand::new();
        demand.record(&ArtifactSelector::new().the("person/name".parse().unwrap()));
        let names = line.observe(demand);
        let mut changes = Changes::new();
        claim("id:a", "person/name", "A").assert(&mut changes);
        claim("id:a", "person/role", "Admin").assert(&mut changes);
        line.apply(changes);
        let instant = only(&names);
        assert_eq!(instant.asserted, vec![fact("id:a", "person/name", "A")]);
        assert!(instant.retracted.is_empty());
    }

    #[dialog_common::test]
    fn it_unregisters_a_dropped_observer() {
        let line = Ephemeral::new();
        let observer = line.observe_everything();
        assert_eq!(line.observers(), 1);
        drop(observer);
        line.assert(claim("id:a", "person/name", "A"));
        assert_eq!(line.observers(), 0, "pruned at the next fan-out");
    }

    #[dialog_common::test]
    fn it_witnesses_without_changing_the_store() {
        let line = Ephemeral::new();
        let observer = line.observe_everything();
        let before = line.revision();
        let mut transient = Changes::new();
        claim("cmd:1", "cmd.start/target", "doc:1").assert(&mut transient);
        line.witness(transient);
        let instant = only(&observer);
        assert_eq!(
            instant.asserted,
            vec![fact("cmd:1", "cmd.start/target", "doc:1")]
        );
        assert_eq!(
            instant.retracted, instant.asserted,
            "gone within the instant"
        );
        assert!(line.is_empty(), "nothing is held");
        assert_ne!(line.revision(), before, "but it happened");
    }

    #[dialog_common::test]
    fn it_chains_the_hash_through_instants() {
        let a = Ephemeral::new();
        let b = Ephemeral::new();
        assert_eq!(a.revision(), b.revision());
        a.assert(claim("id:a", "person/name", "A"));
        b.assert(claim("id:a", "person/name", "A"));
        assert_eq!(a.revision(), b.revision(), "same instants, same identity");
        b.assert(claim("id:b", "person/name", "B"));
        assert_ne!(a.revision(), b.revision());
        let before = b.revision();
        b.assert(claim("id:b", "person/name", "B"));
        assert_eq!(b.revision(), before, "a no-op mints nothing");
    }

    #[dialog_common::test]
    fn it_retains_entities_and_reports_the_drop() {
        let line = Ephemeral::new();
        line.assert(claim("site:1", "site/path", "/a"));
        line.assert(claim("site:2", "site/path", "/b"));
        line.retract(claim("doc:1", "doc/title", "T"));
        let observer = line.observe_everything();
        assert!(line.retain_entities(|entity| entity.to_string() != "site:1"));
        let dropped = only(&observer);
        assert_eq!(dropped.retracted, vec![fact("site:1", "site/path", "/a")]);
        assert!(
            dropped.asserted.is_empty(),
            "the doc tombstone is unrelated"
        );
        assert_eq!(line.len(), 1);
        assert_eq!(line.tombstones().len(), 1);
        assert!(
            !line.retain_entities(|_| true),
            "keeping everything changes nothing"
        );
    }

    #[dialog_common::test]
    fn it_shares_the_store_across_clones() {
        let line = Ephemeral::new();
        let handle = line.clone();
        handle.assert(claim("id:a", "person/name", "A"));
        assert_eq!(line.len(), 1);
        assert_eq!(line.revision(), handle.revision());
    }
}
