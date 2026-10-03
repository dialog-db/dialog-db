//! The ephemeral layer: a memory-backed store with a head and an
//! instant log, no history, gone with the process.
//!
//! Every branch and snapshot carries one ([`Branch::overlay`],
//! [`Snapshot::overlay`](crate::Snapshot::overlay)): the store behind
//! the session scope (see [`placement`](crate::placement)) and the
//! home of session facts folded into every read of the layer but never
//! committed to its tree. A standalone one is a layer of a
//! [`Stack`](crate::Stack), created through the environment
//! ([`Ephemeral::create`]) so it has an address others can open. It is
//! built to sit under a reactive UI:
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
//!   chained hash. Instants are kept in a bounded ring so a reader
//!   pinned at an earlier sequence reads the exact delta since its pin
//!   ([`Ephemeral::since`]). Nothing is hashed but the delta, so a
//!   commit costs the delta, never the store.
//! - **Observers see instants, not folds.** Anything that wants the
//!   instants rather than the fold — a subscription maintaining its
//!   result per touched entity, a command provider that must see a
//!   fact asserted and retracted within one commit — registers an
//!   [`Observer`] with a demand, and every instant is fanned out at
//!   write time into each observer's own bounded queue, filtered to
//!   the facts its demand covers. An instant nobody demanded costs
//!   nothing; memory is the sum of unconsumed matched instants across
//!   observers. An observer that falls off its queue's bound finds a
//!   gap on its next drain and recomputes from the fold. Dropping the
//!   observer unregisters it. An instant can also be
//!   [witnessed](Ephemeral::witness): minted for observers without
//!   changing the store, which is how a transient that lived for one
//!   induction round is seen at all.
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
    Artifact, ArtifactSelector, AttributeKey, Changes, DialogArtifactsError, Entity, EntityKey,
    Instruction, Key, KeyViewConstruct, SortKey, Statement, Update, ValueKey, sort_key,
};
use dialog_capability::{Command, Provider};
use dialog_common::{Blake3Hash, Holds};
use dialog_search_tree::Manifest;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::Demand;

mod channel;
pub use channel::*;

/// How many instants the ring retains. A reader pinned further back
/// than this recomputes from the fold instead of maintaining.
const LOG_CAPACITY: usize = 1024;

/// How many matched instants an observer's queue holds before it
/// gaps. An observer that drains less often than this many matching
/// writes land recomputes from the fold instead of maintaining.
pub(crate) const QUEUE_CAPACITY: usize = 1024;

/// The key the ephemeral registry is held under in an environment
/// (see [`Holds`]).
const REGISTRY_KEY: &str = "dialog.repository/ephemerals";

/// The identity of an ephemeral layer at some instant: how many
/// instants have been minted and the hash chained through all of
/// them. The hash of the empty store is the all-zero hash.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EphemeralRevision {
    /// Instants minted so far; zero for a store nothing has changed.
    pub sequence: u64,
    /// `blake3(previous ‖ sequence ‖ transient ‖ delta ‖ mutations)`,
    /// chained from zero.
    pub hash: Blake3Hash,
}

/// One change to what readers of the layer see. Serializable: it is
/// what a [`Channel`] carries to a peer.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Instant {
    /// The sequence this instant minted; the store's revision after
    /// it is `(sequence, hash)`.
    pub sequence: u64,
    /// The chained hash after this instant.
    pub hash: Blake3Hash,
    /// Whether these facts were witnessed for one induction round
    /// without changing the store. Replicas preserve the distinction.
    #[serde(default)]
    pub transient: bool,
    /// Exact store mutations, distinct from the reader-visible delta.
    /// Empty for transient witnesses. Replicas replay these so lifting
    /// a tombstone never turns a fact from a lower layer into stored
    /// state.
    pub changes: Vec<StoreChange>,
    /// Facts that became readable: stored, or un-shadowed beneath.
    pub asserted: Vec<Artifact>,
    /// Facts that stopped being readable: removed, or shadowed
    /// beneath by a tombstone.
    pub retracted: Vec<Artifact>,
}

/// One mutation of an ephemeral store, carried by an [`Instant`].
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum StoreChange {
    /// Store a fact in this layer.
    Insert(Artifact),
    /// Remove a held fact without hiding it in lower layers.
    Remove(Artifact),
    /// Hide a fact in lower layers.
    Shadow(Artifact),
    /// Lift a tombstone without storing the fact it hid.
    Unshadow(Artifact),
}

impl StoreChange {
    fn fact(&self) -> &Artifact {
        match self {
            Self::Insert(fact) | Self::Remove(fact) | Self::Shadow(fact) | Self::Unshadow(fact) => {
                fact
            }
        }
    }
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
        Self::detached()
    }
}

/// A handle to an ephemeral layer that does not keep it alive: what
/// an [`EphemeralRegistry`] holds, so a layer dies with the last
/// [`Ephemeral`] handle to it, as a closing tab's should.
#[derive(Clone, Debug)]
pub struct WeakEphemeral {
    entity: Entity,
    state: Weak<RwLock<State>>,
}

impl WeakEphemeral {
    /// The layer, if some strong handle still holds it.
    pub fn upgrade(&self) -> Option<Ephemeral> {
        self.state.upgrade().map(|state| Ephemeral {
            entity: self.entity.clone(),
            state,
        })
    }
}

/// The ephemeral layers open in a process, by address. An environment
/// holds one (see [`Holds`]), so a layer is a process resource opened
/// through the environment like a branch, never constructed. Entries
/// are weak: a layer whose every handle was dropped is gone, and
/// opening its address afterwards fails rather than reviving an empty
/// store under a name something else may still record.
#[derive(Debug, Default)]
pub struct EphemeralRegistry {
    entries: Mutex<HashMap<Entity, WeakEphemeral>>,
}

impl EphemeralRegistry {
    /// The registry `env` holds, created and held on first use. Held
    /// as a shared handle, so every caller clones the one registry out.
    pub fn held_by<Env: Holds + ?Sized>(env: &Env) -> Arc<EphemeralRegistry> {
        if let Some(registry) = env
            .held(REGISTRY_KEY)
            .and_then(|held| held.downcast_ref::<Arc<EphemeralRegistry>>().cloned())
        {
            return registry;
        }
        let registry = Arc::new(EphemeralRegistry::default());
        env.hold(REGISTRY_KEY.into(), Arc::new(registry.clone()));
        registry
    }

    /// Mint a fresh layer and register it under its address.
    pub fn create(&self) -> Ephemeral {
        let layer = Ephemeral::detached();
        let mut entries = self.entries.lock();
        entries.retain(|_, weak| weak.upgrade().is_some());
        entries.insert(layer.entity.clone(), layer.downgrade());
        layer
    }

    /// The layer registered at `address`, if it is still alive.
    pub fn open(&self, address: &Entity) -> Option<Ephemeral> {
        self.entries
            .lock()
            .get(address)
            .and_then(WeakEphemeral::upgrade)
    }

    /// How many registered layers are alive.
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .values()
            .filter(|weak| weak.upgrade().is_some())
            .count()
    }

    /// Whether no registered layer is alive.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<CreateEphemeral> for EphemeralRegistry {
    async fn execute(&self, _: ()) -> Ephemeral {
        self.create()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<OpenEphemeral> for EphemeralRegistry {
    async fn execute(&self, address: Entity) -> Result<Ephemeral, EphemeralError> {
        self.open(&address).ok_or(EphemeralError::NotOpen(address))
    }
}

/// Command minting a fresh ephemeral layer, registered with the
/// environment under its address. Built by [`Ephemeral::create`].
#[derive(Debug, Clone, Copy)]
pub struct CreateEphemeral;

impl Command for CreateEphemeral {
    type Input = ();
    type Output = Ephemeral;
}

impl CreateEphemeral {
    /// Mint the layer and register it with the registry `env` holds.
    /// Async like every command's `perform`, though the registry is
    /// in memory.
    #[allow(clippy::unused_async)]
    pub async fn perform<Env>(self, env: &Env) -> Ephemeral
    where
        Env: Holds,
    {
        EphemeralRegistry::held_by(env).create()
    }
}

/// Command opening the ephemeral layer the environment holds at an
/// address. Built by [`Ephemeral::open`].
#[derive(Debug, Clone)]
pub struct OpenEphemeral {
    /// The layer's address: the nonce entity it was created under.
    pub address: Entity,
}

impl Command for OpenEphemeral {
    type Input = Entity;
    type Output = Result<Ephemeral, EphemeralError>;
}

impl OpenEphemeral {
    /// Resolve the layer from the registry `env` holds, or fail if
    /// nothing holds one at the address. Async like every command's
    /// `perform`, though the registry is in memory.
    #[allow(clippy::unused_async)]
    pub async fn perform<Env>(self, env: &Env) -> Result<Ephemeral, EphemeralError>
    where
        Env: Holds,
    {
        EphemeralRegistry::held_by(env)
            .open(&self.address)
            .ok_or(EphemeralError::NotOpen(self.address))
    }
}

/// Why an ephemeral layer could not be opened.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EphemeralError {
    /// No live layer is registered at the address: it was never
    /// created in this process, or its last handle was dropped.
    #[error("No ephemeral layer is open at {0}")]
    NotOpen(Entity),
}

#[derive(Debug)]
struct State {
    /// Every held fact under each of its three index keys.
    facts: Facts,
    /// Facts held beneath this layer that the session hides, by sort
    /// key, with the fact kept so lifting the tombstone can report
    /// what became readable again. Shared with readers by `Arc` and
    /// updated in place (copied only while a reader holds it), so a
    /// read never copies the set and a write never rebuilds it.
    tombstones: Arc<HashSet<SortKey>>,
    shadowed: HashMap<SortKey, Artifact>,
    sequence: u64,
    hash: Blake3Hash,
    /// The most recent instants, oldest first, at most
    /// [`LOG_CAPACITY`].
    log: VecDeque<Instant>,
    /// Every registered observer's queue, weakly: an observer that was
    /// dropped is pruned at the next fan-out.
    observers: Vec<Weak<Mutex<Queue>>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            facts: Facts::default(),
            tombstones: Arc::new(HashSet::new()),
            shadowed: HashMap::new(),
            sequence: 0,
            hash: Blake3Hash::from([0u8; 32]),
            log: VecDeque::new(),
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

/// Facts held under the tree's three index keys, in one ordered map.
/// The key's tag byte keeps the three orders apart, so one map serves
/// every selector shape and a scan comes out in tree order. Shared by
/// [`Ephemeral`] and a transaction's [`Staged`](crate::Staged) store.
#[derive(Clone, Debug, Default)]
pub(crate) struct Facts {
    map: BTreeMap<Key, Artifact>,
    /// The key format facts are keyed under. Fixed to the default
    /// manifest, the same one every tree carries today and the one
    /// [`Demand`](crate::Demand) ranges are built under.
    manifest: Manifest,
}

impl Facts {
    /// The format facts are keyed under.
    pub(crate) fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Whether this exact triple is held.
    pub(crate) fn holds(&self, fact: &Artifact) -> bool {
        self.map
            .contains_key(&EntityKey::from_artifact(fact, &self.manifest).into_key())
    }

    /// Hold `fact`. Returns whether it was not held before.
    pub(crate) fn insert(&mut self, fact: Artifact) -> bool {
        if self.holds(&fact) {
            return false;
        }
        for key in index_keys(&fact, &self.manifest) {
            self.map.insert(key, fact.clone());
        }
        true
    }

    /// Drop `fact`. Returns whether it was held.
    pub(crate) fn remove(&mut self, fact: &Artifact) -> bool {
        if !self.holds(fact) {
            return false;
        }
        for key in index_keys(fact, &self.manifest) {
            self.map.remove(&key);
        }
        true
    }

    /// Every held fact at an `(entity, attribute)` cell.
    pub(crate) fn cell(&self, of: &Entity, the: &dialog_artifacts::Attribute) -> Vec<Artifact> {
        self.scan(&ArtifactSelector::new().of(of.clone()).the(the.clone()))
    }

    /// The facts a selector matches, in the store's own key order.
    pub(crate) fn scan(&self, selector: &ArtifactSelector<Constrained>) -> Vec<Artifact> {
        self.map
            .range(selector_range(selector, &self.manifest))
            .map(|(_, fact)| fact.clone())
            .collect()
    }

    /// The facts a selector matches, in the order a scan of a tree
    /// written under `manifest` would produce them. A fact's index keys
    /// differ between formats only in their value tail, so under
    /// another format than the store's own the rows are re-keyed, and
    /// only the order of values within a cell can change.
    pub(crate) fn select(
        &self,
        selector: &ArtifactSelector<Constrained>,
        manifest: &Manifest,
    ) -> Vec<Artifact> {
        let mut rows = self.scan(selector);
        if *manifest != self.manifest {
            let range = selector_range(selector, manifest);
            rows.sort_by_cached_key(|fact| {
                index_keys(fact, manifest)
                    .into_iter()
                    .find(|key| range.contains(key))
            });
        }
        rows
    }

    /// Every held fact once, in entity order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Artifact> {
        self.map
            .range(
                <EntityKey<Key> as KeyViewConstruct>::min().into_key()
                    ..=<EntityKey<Key> as KeyViewConstruct>::max().into_key(),
            )
            .map(|(_, fact)| fact)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        // Every fact sits under exactly three keys.
        self.map.len() / 3
    }
}

/// The delta one write is accumulating, before it is minted.
#[derive(Default)]
struct Delta {
    transient: bool,
    changes: Vec<StoreChange>,
    asserted: Vec<Artifact>,
    retracted: Vec<Artifact>,
}

impl Delta {
    fn is_empty(&self) -> bool {
        self.asserted.is_empty() && self.retracted.is_empty()
    }
}

impl State {
    fn key(&self, fact: &Artifact) -> SortKey {
        sort_key(fact, self.facts.manifest())
    }

    fn insert(&mut self, fact: Artifact, delta: &mut Delta) {
        if self.facts.insert(fact.clone()) {
            delta.changes.push(StoreChange::Insert(fact.clone()));
            delta.asserted.push(fact);
        }
    }

    fn remove(&mut self, fact: &Artifact, delta: &mut Delta) -> bool {
        if !self.facts.remove(fact) {
            return false;
        }
        delta.changes.push(StoreChange::Remove(fact.clone()));
        delta.retracted.push(fact.clone());
        true
    }

    /// Hide `fact` beneath this layer. A tombstone is a change readers
    /// see (the fact disappears), so it is reported as retracted.
    fn shadow(&mut self, fact: Artifact, delta: &mut Delta) {
        let key = self.key(&fact);
        if let Entry::Vacant(slot) = self.shadowed.entry(key.clone()) {
            slot.insert(fact.clone());
            Arc::make_mut(&mut self.tombstones).insert(key);
            delta.changes.push(StoreChange::Shadow(fact.clone()));
            delta.retracted.push(fact);
        }
    }

    /// Stop hiding the fact under `key`, if it was hidden.
    fn unshadow(&mut self, key: &SortKey, delta: &mut Delta) {
        if let Some(fact) = self.shadowed.remove(key) {
            Arc::make_mut(&mut self.tombstones).remove(key);
            delta.changes.push(StoreChange::Unshadow(fact.clone()));
            delta.asserted.push(fact);
        }
    }

    fn apply(&mut self, instruction: Instruction, delta: &mut Delta) {
        match instruction {
            Instruction::Assert(fact) => self.insert(fact, delta),
            Instruction::Replace(fact) => {
                let mut standing = false;
                for prior in self.facts.cell(&fact.of, &fact.the) {
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
                // Not held here: hide it beneath.
                self.shadow(fact, delta);
            }
        }
    }

    /// Mint an instant for a non-empty delta, advancing the sequence
    /// and the chained hash, recording it in the ring and handing it
    /// to every observer. Returns the instant, or `None` when nothing
    /// readers see changed.
    fn mint(&mut self, delta: Delta) -> Option<Instant> {
        if delta.is_empty() {
            return None;
        }
        self.sequence += 1;
        let mut chunks: Vec<Vec<u8>> =
            Vec::with_capacity(3 + delta.asserted.len() + delta.retracted.len());
        chunks.push(self.hash.as_bytes().to_vec());
        chunks.push(self.sequence.to_be_bytes().to_vec());
        chunks.push(vec![u8::from(delta.transient)]);
        for (polarity, facts) in [(b'+', &delta.asserted), (b'-', &delta.retracted)] {
            for fact in facts {
                let (the, of, tail) = self.key(fact);
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
        for change in &delta.changes {
            let tag = match change {
                StoreChange::Insert(_) => b'i',
                StoreChange::Remove(_) => b'r',
                StoreChange::Shadow(_) => b's',
                StoreChange::Unshadow(_) => b'u',
            };
            let (the, of, tail) = self.key(change.fact());
            let mut chunk = vec![tag];
            chunk.extend(the);
            chunk.push(0);
            chunk.extend(of);
            chunk.push(0);
            chunk.extend(tail);
            chunks.push(chunk);
        }
        self.hash = Blake3Hash::hash_iter(chunks.iter().map(Vec::as_slice));
        let instant = Instant {
            sequence: self.sequence,
            hash: self.hash.clone(),
            transient: delta.transient,
            changes: delta.changes,
            asserted: delta.asserted,
            retracted: delta.retracted,
        };
        if self.log.len() == LOG_CAPACITY {
            self.log.pop_front();
        }
        self.log.push_back(instant.clone());
        self.fan_out(&instant);
        Some(instant)
    }

    /// Hand `instant` to every live observer, filtered to the facts
    /// its demand covers; prune observers that were dropped.
    fn fan_out(&mut self, instant: &Instant) {
        self.observers.retain(|weak| weak.strong_count() > 0);
        let manifest = self.facts.manifest();
        for weak in &self.observers {
            let Some(queue) = weak.upgrade() else {
                continue;
            };
            let mut queue = queue.lock();
            if queue.gapped {
                continue;
            }
            let Some(matched) = queue.filter.matched(instant, manifest) else {
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
                // Keys are built under the format the cover was recorded
                // in, which is the store's own today; a cover recorded
                // under no format yet covers nothing.
                let keyed = demand.manifest();
                let manifest = keyed.as_ref().unwrap_or(manifest);
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
                    transient: instant.transient,
                    changes: instant
                        .changes
                        .iter()
                        .filter(|change| covers(change.fact()))
                        .cloned()
                        .collect(),
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
        self.drain_at().0
    }

    /// Drain and capture the covered sequence under the same state
    /// lock. Writers lock the layer before its queues; readers use
    /// that order too.
    fn drain_at(&self) -> (Drained, u64) {
        let state = self.layer.state.read();
        let mut queue = self.queue.lock();
        let drained = if queue.gapped {
            queue.gapped = false;
            queue.instants.clear();
            Drained::Gap {
                sequence: state.sequence,
            }
        } else {
            Drained::Instants(queue.instants.drain(..).collect())
        };
        (drained, state.sequence)
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
    /// An empty layer belonging to nothing: a tree layer's session
    /// store, which the layer owns and nothing else addresses. A
    /// standalone layer is created through the environment instead
    /// ([`create`](Self::create)), so it has an address others can
    /// open.
    pub fn new() -> Self {
        Self::detached()
    }

    /// An empty layer with a fresh address, registered nowhere.
    pub(crate) fn detached() -> Self {
        Self {
            entity: Entity::new().expect("the platform can mint a random entity"),
            state: Arc::default(),
        }
    }

    /// Mint a fresh layer through the environment, registered under
    /// its address so [`open`](Self::open) finds it.
    pub fn create() -> CreateEphemeral {
        CreateEphemeral
    }

    /// Open the layer the environment holds at `address`.
    pub fn open(address: Entity) -> OpenEphemeral {
        OpenEphemeral { address }
    }

    /// A handle that does not keep the layer alive.
    pub fn downgrade(&self) -> WeakEphemeral {
        WeakEphemeral {
            entity: self.entity.clone(),
            state: Arc::downgrade(&self.state),
        }
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

    /// Assert a statement: its asserts and replaces land in the store
    /// with the tree's semantics, its retracts remove or tombstone.
    /// Chainable; use [`apply`](Self::apply) to get the instant minted.
    ///
    /// A layer holds facts only, so a statement that changes an asset
    /// is refused (see [`apply`](Self::apply)).
    pub fn assert<S: Statement>(&self, statement: S) -> Result<&Self, DialogArtifactsError> {
        let mut changes = Changes::new();
        statement.assert(&mut changes);
        self.apply(changes)?;
        Ok(self)
    }

    /// Retract a statement: each of its facts is removed from the
    /// store if held here, and otherwise hidden beneath by a
    /// tombstone. Chainable. A statement that changes an asset is
    /// refused.
    pub fn retract<S: Statement>(&self, statement: S) -> Result<&Self, DialogArtifactsError> {
        let mut changes = Changes::new();
        statement.retract(&mut changes);
        self.apply(changes)?;
        Ok(self)
    }

    /// Land a batch of instructions as one instant.
    ///
    /// Assets are stored only by a transaction's commit. A batch that
    /// changes one is refused with
    /// [`AssetsUnsupported`](DialogArtifactsError::AssetsUnsupported)
    /// and nothing in it lands, rather than landing its facts and
    /// dropping its assets.
    pub fn apply(&self, changes: Changes) -> Result<Option<Instant>, DialogArtifactsError> {
        if changes.has_assets() {
            return Err(DialogArtifactsError::AssetsUnsupported(
                "an ephemeral layer".into(),
            ));
        }
        if changes.is_empty() {
            return Ok(None);
        }
        let mut state = self.state.write();
        let mut delta = Delta::default();
        for instruction in changes.into_instructions() {
            state.apply(instruction, &mut delta);
        }
        Ok(state.mint(delta))
    }

    /// Apply scope maintenance and its replacement facts under one
    /// write lock and mint one instant, so a re-stamp has no empty
    /// intermediate: drop everything when `clear`, else every fact and
    /// tombstone of a `forgotten` entity, then land `changes`.
    pub(crate) fn maintain(&self, changes: Changes, clear: bool, forgotten: &HashSet<Entity>) {
        let mut state = self.state.write();
        let mut delta = Delta::default();
        let dropped: Vec<Artifact> = state
            .facts
            .iter()
            .filter(|fact| clear || forgotten.contains(&fact.of))
            .cloned()
            .collect();
        for fact in dropped {
            state.remove(&fact, &mut delta);
        }
        let lifted: Vec<SortKey> = state
            .shadowed
            .iter()
            .filter(|(_, fact)| clear || forgotten.contains(&fact.of))
            .map(|(key, _)| key.clone())
            .collect();
        for key in lifted {
            state.unshadow(&key, &mut delta);
        }
        for instruction in changes.into_instructions() {
            state.apply(instruction, &mut delta);
        }
        state.mint(delta);
    }

    /// Everything this layer holds, as one batch: each tombstone as
    /// the retraction that hides its fact beneath, then each held fact
    /// as an assertion. The batch serializes (see [`Changes`]), so a
    /// session can outlive the process holding it: export, carry the
    /// bytes, and [`apply`](Self::apply) them to a successor's layer,
    /// which reproduces both the facts and the tombstones as one
    /// instant.
    pub fn export(&self) -> Changes {
        let state = self.state.read();
        let mut changes = Changes::new();
        for fact in state.shadowed.values() {
            changes.dissociate(fact.the.clone(), fact.of.clone(), fact.is.clone());
        }
        for fact in state.facts.iter() {
            changes.associate(fact.the.clone(), fact.of.clone(), fact.is.clone());
        }
        changes
    }

    /// Drop every fact and tombstone recorded for entities that fail
    /// `keep`, outright rather than by tombstoning. The
    /// garbage-collection primitive for per-client facts keyed by
    /// short-lived entities. Returns whether anything was dropped.
    pub fn retain_entities<F: FnMut(&Entity) -> bool>(&self, mut keep: F) -> bool {
        let mut state = self.state.write();
        let mut delta = Delta::default();
        let dropped: Vec<Artifact> = state
            .facts
            .iter()
            .filter(|fact| !keep(&fact.of))
            .cloned()
            .collect();
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
            state.unshadow(&key, &mut delta);
        }
        state.mint(delta).is_some()
    }

    /// Drop every fact and tombstone. Chainable.
    pub fn clear(&self) -> &Self {
        let mut state = self.state.write();
        let mut delta = Delta::default();
        let held: Vec<Artifact> = state.facts.iter().cloned().collect();
        for fact in held {
            state.remove(&fact, &mut delta);
        }
        let lifted: Vec<SortKey> = state.shadowed.keys().cloned().collect();
        for key in lifted {
            state.unshadow(&key, &mut delta);
        }
        state.mint(delta);
        self
    }

    /// The layer's identity now.
    pub fn revision(&self) -> EphemeralRevision {
        let state = self.state.read();
        EphemeralRevision {
            sequence: state.sequence,
            hash: state.hash.clone(),
        }
    }

    /// The instants minted after `sequence`, oldest first, or `None`
    /// when the ring no longer reaches back that far and a reader
    /// pinned there must recompute from the fold. An up-to-date pin
    /// yields an empty vector.
    pub fn since(&self, sequence: u64) -> Option<Vec<Instant>> {
        let state = self.state.read();
        if sequence >= state.sequence {
            return Some(Vec::new());
        }
        match state.log.front() {
            Some(oldest) if oldest.sequence > sequence + 1 => None,
            // An empty ring with a moved sequence cannot happen: every
            // sequence advance records an instant, and the ring only
            // drops from the front once full.
            None => None,
            Some(_) => Some(
                state
                    .log
                    .iter()
                    .filter(|instant| instant.sequence > sequence)
                    .cloned()
                    .collect(),
            ),
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
        let mut delta = Delta {
            transient: true,
            ..Delta::default()
        };
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

    /// Sort keys of every fact this layer hides beneath it, keyed
    /// under `manifest`: the format of the tree whose rows they are
    /// checked against. Shared when that is the store's own format, so
    /// a read never copies the set; keyed afresh under another.
    pub(crate) fn tombstones(&self, manifest: &Manifest) -> Arc<HashSet<SortKey>> {
        let state = self.state.read();
        if manifest == state.facts.manifest() {
            return state.tombstones.clone();
        }
        Arc::new(
            state
                .shadowed
                .values()
                .map(|fact| sort_key(fact, manifest))
                .collect(),
        )
    }

    /// Whether the store holds no facts (tombstones aside).
    pub fn is_empty(&self) -> bool {
        self.state.read().facts.is_empty()
    }

    /// The number of facts held.
    pub fn len(&self) -> usize {
        self.state.read().facts.len()
    }

    /// Every fact held, each once, in entity order: the fold a peer
    /// that cannot be caught up from instants applies instead.
    pub fn facts(&self) -> Vec<Artifact> {
        self.fold().0
    }

    /// Capture the fold and its sequence together, so a peer never
    /// skips a write that arrived between reading the facts and
    /// reading the revision.
    fn fold(&self) -> (Vec<Artifact>, u64) {
        let (facts, _, sequence) = self.snapshot();
        (facts, sequence)
    }

    /// The held facts, the shadowed facts and the sequence, read under
    /// one lock.
    fn snapshot(&self) -> (Vec<Artifact>, Vec<Artifact>, u64) {
        let state = self.state.read();
        (
            state.facts.iter().cloned().collect(),
            state.shadowed.values().cloned().collect(),
            state.sequence,
        )
    }

    /// The facts a selector matches, in the order a tree scan of the
    /// same selector would produce them under the store's own format.
    /// For rows merged with a tree's, see [`select`](Self::select).
    pub fn scan(&self, selector: &ArtifactSelector<Constrained>) -> Vec<Artifact> {
        self.state.read().facts.scan(selector)
    }

    /// The facts a selector matches, in the order a scan of a tree
    /// written under `manifest` would produce them: what a query
    /// merges with that tree's rows. A fact's index keys differ
    /// between formats only in their value tail, so under another
    /// format than the store's own the rows are re-keyed, and only the
    /// order of values within a cell can change.
    pub fn select(
        &self,
        selector: &ArtifactSelector<Constrained>,
        manifest: &Manifest,
    ) -> Vec<Artifact> {
        self.state.read().facts.select(selector, manifest)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_artifacts::Value;
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

    fn values(line: &Ephemeral, of: &str, the_: &str) -> Vec<Value> {
        let selector = ArtifactSelector::new()
            .of(of.parse().expect("entity"))
            .the(the_.parse().expect("attribute"));
        line.scan(&selector).into_iter().map(|f| f.is).collect()
    }

    #[dialog_common::test]
    fn it_asserts_idempotently_and_replaces_per_cell() {
        let line = Ephemeral::new();
        line.assert(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        )
        .unwrap();
        let first = &line.since(0).expect("in the ring")[0];
        assert_eq!(first.sequence, 1);
        assert_eq!(first.asserted, vec![fact("id:a", "person/name", "A")]);
        line.assert(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        )
        .unwrap();
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
        )
        .unwrap();
        assert_eq!(values(&line, "id:a", "person/name").len(), 2);
        let mut changes = Changes::new();
        changes.associate_unique(
            "person/name".parse().unwrap(),
            "id:a".parse().unwrap(),
            Value::String("C".into()),
        );
        let replaced = line.apply(changes).unwrap().expect("replace mints");
        assert_eq!(replaced.retracted.len(), 2);
        assert_eq!(replaced.asserted, vec![fact("id:a", "person/name", "C")]);
        assert_eq!(
            values(&line, "id:a", "person/name"),
            vec![Value::String("C".into())]
        );
        assert_eq!(line.revision().sequence, 3);
    }

    #[dialog_common::test]
    fn it_exports_held_facts_and_tombstones_for_another_line() {
        let source = Ephemeral::new();
        source
            .assert(
                the!("person/name")
                    .of("id:a".parse().unwrap())
                    .is("A".to_string()),
            )
            .unwrap();
        source
            .assert(
                the!("person/name")
                    .of("id:a".parse().unwrap())
                    .is("Ann".to_string()),
            )
            .unwrap();
        source
            .retract(
                the!("person/name")
                    .of("id:b".parse().unwrap())
                    .is("B".to_string()),
            )
            .unwrap();

        let exported = source.export();
        let target = Ephemeral::new();
        let restored = target.apply(exported).unwrap().expect("a restore mints");
        assert_eq!(
            restored.sequence, 1,
            "the whole session lands as one instant"
        );
        assert_eq!(target.len(), source.len(), "each held fact once");
        assert_eq!(
            values(&target, "id:a", "person/name"),
            values(&source, "id:a", "person/name")
        );
        assert_eq!(
            *target.tombstones(&Manifest::default()),
            *source.tombstones(&Manifest::default()),
            "the tombstone hiding id:b travels too"
        );
    }

    #[dialog_common::test]
    fn it_removes_held_facts_and_tombstones_absent_ones() {
        let line = Ephemeral::new();
        line.assert(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        )
        .unwrap();
        line.retract(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        )
        .unwrap();
        let removed = &line.since(1).expect("in the ring")[0];
        assert_eq!(removed.retracted, vec![fact("id:a", "person/name", "A")]);
        assert!(line.is_empty());
        assert!(
            line.tombstones(&Manifest::default()).is_empty(),
            "a held fact is removed, not shadowed"
        );

        line.retract(
            the!("person/name")
                .of("id:b".parse().unwrap())
                .is("B".to_string()),
        )
        .unwrap();
        let shadowed = &line.since(2).expect("a tombstone is a visible change")[0];
        assert_eq!(shadowed.retracted, vec![fact("id:b", "person/name", "B")]);
        assert_eq!(line.tombstones(&Manifest::default()).len(), 1);
        line.retract(
            the!("person/name")
                .of("id:b".parse().unwrap())
                .is("B".to_string()),
        )
        .unwrap();
        assert_eq!(
            line.revision().sequence,
            3,
            "a standing tombstone changes nothing"
        );

        line.clear();
        let lifted = &line.since(3).expect("lifting a tombstone is visible")[0];
        assert_eq!(lifted.asserted, vec![fact("id:b", "person/name", "B")]);
        assert!(line.tombstones(&Manifest::default()).is_empty());
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
            line.assert(claim(of, the_, is)).unwrap();
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
    fn it_reports_instants_since_a_pin_and_gaps_past_the_ring() {
        let line = Ephemeral::new();
        assert_eq!(line.since(0), Some(Vec::new()));
        line.assert(claim("id:a", "person/name", "A")).unwrap();
        line.assert(claim("id:b", "person/name", "B")).unwrap();
        let since = line.since(0).expect("within the ring");
        assert_eq!(since.len(), 2);
        assert_eq!(since[0].sequence, 1);
        assert_eq!(since[1].asserted, vec![fact("id:b", "person/name", "B")]);
        assert_eq!(line.since(1).expect("within the ring").len(), 1);
        assert_eq!(line.since(2), Some(Vec::new()));

        for index in 0..LOG_CAPACITY {
            line.assert(claim(&format!("id:{index}"), "person/tag", "x"))
                .unwrap();
        }
        assert!(
            line.since(1).is_none(),
            "a pin older than the ring must recompute"
        );
        let head = line.revision().sequence;
        assert_eq!(line.since(head - 1).expect("the newest instant").len(), 1);
    }

    #[dialog_common::test]
    fn it_chains_the_hash_through_instants() {
        let a = Ephemeral::new();
        let b = Ephemeral::new();
        assert_eq!(a.revision(), b.revision());
        a.assert(claim("id:a", "person/name", "A")).unwrap();
        b.assert(claim("id:a", "person/name", "A")).unwrap();
        assert_eq!(a.revision(), b.revision(), "same instants, same identity");
        b.assert(claim("id:b", "person/name", "B")).unwrap();
        assert_ne!(a.revision(), b.revision());
        let before = b.revision();
        b.assert(claim("id:b", "person/name", "B")).unwrap();
        assert_eq!(b.revision(), before, "a no-op mints nothing");
    }

    #[dialog_common::test]
    fn it_retains_entities_and_reports_the_drop() {
        let line = Ephemeral::new();
        line.assert(claim("site:1", "site/path", "/a")).unwrap();
        line.assert(claim("site:2", "site/path", "/b")).unwrap();
        line.retract(claim("doc:1", "doc/title", "T")).unwrap();
        let pinned = line.revision().sequence;
        assert!(line.retain_entities(|entity| entity.to_string() != "site:1"));
        let dropped = &line.since(pinned).expect("in the ring")[0];
        assert_eq!(dropped.retracted, vec![fact("site:1", "site/path", "/a")]);
        assert!(
            dropped.asserted.is_empty(),
            "the doc tombstone is unrelated"
        );
        assert_eq!(line.len(), 1);
        assert_eq!(line.tombstones(&Manifest::default()).len(), 1);
        assert!(
            !line.retain_entities(|_| true),
            "keeping everything changes nothing"
        );
    }

    #[dialog_common::test]
    fn it_shares_the_store_across_clones() {
        let line = Ephemeral::new();
        let handle = line.clone();
        handle.assert(claim("id:a", "person/name", "A")).unwrap();
        assert_eq!(line.len(), 1);
        assert_eq!(line.revision(), handle.revision());
    }

    /// Reads for a tree of another format are keyed under that format:
    /// a value that spills there and inlines here gets a different sort
    /// key, and the tombstone set and row order follow the reader's
    /// manifest, not the store's own.
    #[dialog_common::test]
    fn it_keys_reads_under_the_readers_manifest() {
        let spilling = Manifest {
            inline_n: 8,
            ..Manifest::default()
        };
        let long = "x".repeat(64);
        let hidden = fact("id:a", "person/bio", &long);
        let line = Ephemeral::new();
        line.retract(claim("id:a", "person/bio", &long)).unwrap();
        line.assert(claim("id:b", "person/bio", &long)).unwrap();
        line.assert(claim("id:b", "person/bio", "short")).unwrap();

        let own = line.tombstones(&Manifest::default());
        let theirs = line.tombstones(&spilling);
        assert!(own.contains(&sort_key(&hidden, &Manifest::default())));
        assert!(theirs.contains(&sort_key(&hidden, &spilling)));
        assert_ne!(
            *own, *theirs,
            "a spilled value keys differently from an inline one"
        );
        assert!(
            Arc::ptr_eq(&own, &line.tombstones(&Manifest::default())),
            "the store's own format shares its set"
        );

        let selector = ArtifactSelector::new().of("id:b".parse().unwrap());
        let rows: Vec<Vec<u8>> = line
            .select(&selector, &spilling)
            .iter()
            .map(|row| sort_key(row, &spilling).2)
            .collect();
        let mut sorted = rows.clone();
        sorted.sort();
        assert_eq!(rows, sorted, "rows come out in the reader's key order");
        assert_eq!(line.select(&selector, &spilling).len(), 2);
    }

    /// A line holds facts only: a batch that changes an asset is refused
    /// whole, facts and all, rather than landing without the asset.
    #[dialog_common::test]
    fn it_refuses_a_batch_that_changes_an_asset() {
        let line = Ephemeral::new();
        let before = line.revision();
        let asset = dialog_artifacts::Asset::from(b"not a fact".to_vec());

        let mut changes = Changes::new();
        the!("person/name")
            .of("id:a".parse().unwrap())
            .is("A".to_string())
            .assert(&mut changes);
        asset.clone().assert(&mut changes);
        assert!(matches!(
            line.apply(changes),
            Err(DialogArtifactsError::AssetsUnsupported(_))
        ));
        assert!(matches!(
            line.assert(asset.clone()),
            Err(DialogArtifactsError::AssetsUnsupported(_))
        ));
        assert!(matches!(
            line.retract(asset),
            Err(DialogArtifactsError::AssetsUnsupported(_))
        ));

        assert_eq!(line.len(), 0, "nothing in the batch landed");
        assert_eq!(line.revision(), before, "no instant was minted");
    }

    /// The single instant an observer of everything saw, or a panic.
    fn only(observer: &Observer) -> Instant {
        match observer.drain() {
            Drained::Instants(mut instants) if instants.len() == 1 => instants.remove(0),
            other => panic!("expected exactly one instant, got {other:?}"),
        }
    }

    #[dialog_common::test]
    fn it_queues_matched_instants_per_observer_and_gaps_past_the_bound() {
        let line = Ephemeral::detached();
        let everything = line.observe_everything();
        let names = {
            let demand = Demand::new();
            demand.record(
                &ArtifactSelector::new().the("person/name".parse().unwrap()),
                &Manifest::default(),
            );
            line.observe(demand)
        };
        assert_eq!(everything.drain(), Drained::Instants(Vec::new()));

        line.assert(claim("id:a", "person/name", "A")).unwrap();
        line.assert(claim("id:b", "person/role", "Admin")).unwrap();
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
            line.assert(claim(&format!("id:{index}"), "person/tag", "x"))
                .unwrap();
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
        line.assert(claim("id:z", "person/name", "Z")).unwrap();
        assert_eq!(
            only(&everything).asserted,
            vec![fact("id:z", "person/name", "Z")],
            "a drained gap collects again"
        );
    }

    #[dialog_common::test]
    fn it_filters_an_instant_to_the_covered_facts() {
        let line = Ephemeral::detached();
        let demand = Demand::new();
        demand.record(
            &ArtifactSelector::new().the("person/name".parse().unwrap()),
            &Manifest::default(),
        );
        let names = line.observe(demand);
        let mut changes = Changes::new();
        claim("id:a", "person/name", "A").assert(&mut changes);
        claim("id:a", "person/role", "Admin").assert(&mut changes);
        line.apply(changes).unwrap();
        let instant = only(&names);
        assert_eq!(instant.asserted, vec![fact("id:a", "person/name", "A")]);
        assert!(instant.retracted.is_empty());
    }

    #[dialog_common::test]
    fn it_unregisters_a_dropped_observer() {
        let line = Ephemeral::detached();
        let observer = line.observe_everything();
        assert_eq!(line.observers(), 1);
        drop(observer);
        line.assert(claim("id:a", "person/name", "A")).unwrap();
        assert_eq!(line.observers(), 0, "pruned at the next fan-out");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn it_drains_overflow_while_another_thread_writes() {
        use std::{sync::mpsc, thread, time::Duration};
        let line = Ephemeral::detached();
        let observer = line.observe_everything();
        for i in 0..=QUEUE_CAPACITY {
            line.assert(claim(&format!("id:{i}"), "person/name", "Before"))
                .unwrap();
        }
        let writer = line.clone();
        let (done, completion) = mpsc::channel();
        let writing = done.clone();
        thread::spawn(move || {
            for i in 0..2048 {
                writer
                    .assert(claim(&format!("next:{i}"), "person/name", "After"))
                    .unwrap();
            }
            writing.send(()).unwrap();
        });
        thread::spawn(move || {
            for _ in 0..2048 {
                observer.drain();
                thread::yield_now();
            }
            done.send(()).unwrap();
        });
        for _ in 0..2 {
            completion
                .recv_timeout(Duration::from_secs(10))
                .expect("overflow draining and writes must not deadlock");
        }
    }

    #[dialog_common::test]
    fn it_witnesses_without_changing_the_store() {
        let line = Ephemeral::detached();
        let observer = line.observe_everything();
        let before = line.revision();
        let mut transient = Changes::new();
        claim("cmd:1", "cmd.start/target", "doc:1").assert(&mut transient);
        line.witness(transient);
        let instant = only(&observer);
        assert!(instant.transient);
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
    fn it_registers_layers_weakly_by_address() {
        let registry = EphemeralRegistry::default();
        let layer = registry.create();
        let address = layer.entity().clone();
        assert!(registry.open(&address).expect("registered").is(&layer));
        assert_eq!(registry.len(), 1);
        let other = registry.create();
        assert!(
            !registry
                .open(other.entity())
                .expect("registered")
                .is(&layer)
        );
        drop(layer);
        assert!(
            registry.open(&address).is_none(),
            "a layer dies with its last handle"
        );
        assert_eq!(registry.len(), 1);
    }

    /// The registry is a process resource the environment holds: the
    /// first use creates it, every later use finds the same one, so a
    /// layer created through the environment opens by its address.
    #[dialog_common::test]
    async fn it_holds_one_registry_per_environment() {
        let env = dialog_common::Holdings::default();
        let layer = Ephemeral::create().perform(&env).await;
        let address = layer.entity().clone();
        let reopened = Ephemeral::open(address.clone())
            .perform(&env)
            .await
            .expect("registered");
        assert!(reopened.is(&layer), "the same store");
        drop(layer);
        drop(reopened);
        let missing = Ephemeral::open(address.clone()).perform(&env).await;
        assert!(
            matches!(&missing, Err(EphemeralError::NotOpen(at)) if *at == address),
            "gone with its last handle: {missing:?}"
        );
    }
}
