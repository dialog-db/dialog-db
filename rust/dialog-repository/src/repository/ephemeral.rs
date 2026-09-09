//! The ephemeral line: a memory-backed store with a head and an
//! instant log, no history, gone with the process.
//!
//! Every branch and snapshot carries one ([`Branch::overlay`],
//! [`Snapshot::overlay`](crate::Snapshot::overlay)): the store behind
//! the procedural layer (see [`placement`](crate::placement)) and the
//! home of session facts folded into every read of the line but never
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
//!   *tombstone* that hides the same fact in the lines beneath, which
//!   is how a session shadows a committed fact without touching the
//!   tree.
//! - **Every change is an instant.** A write that changes what readers
//!   see mints one [`Instant`]: the facts that became readable, the
//!   facts that stopped being readable, a sequence number, and a
//!   chained hash. Instants are kept in a bounded ring so a
//!   subscription pinned at an earlier sequence reads the exact delta
//!   since its pin ([`Ephemeral::since`]) and maintains its result per
//!   touched entity instead of recomputing. Nothing is hashed but the
//!   delta, so a commit costs the delta, never the store.
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
use std::sync::Arc;

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
use parking_lot::RwLock;

/// How many instants the ring retains. A subscription pinned further
/// back than this recomputes from the fold instead of maintaining.
const LOG_CAPACITY: usize = 1024;

/// The identity of an ephemeral line at some instant: how many
/// instants have been minted and the hash chained through all of
/// them. The hash of the empty store is the all-zero hash.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EphemeralRevision {
    /// Instants minted so far; zero for a store nothing has changed.
    pub sequence: u64,
    /// `blake3(previous ‖ sequence ‖ delta)`, chained from zero.
    pub hash: Blake3Hash,
}

/// One change to what readers of the line see.
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

/// A memory-backed line. Cheap to clone: clones share the store, so a
/// fact asserted through any handle is visible to readers of all of
/// them. See the [module docs](self).
#[derive(Clone, Debug)]
pub struct Ephemeral {
    /// A nonce minted when the store is created: the address a stack
    /// records for this line. Two stores never share one, and every
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
    /// Facts held beneath this line that the session hides, by sort
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
    /// The most recent instants, oldest first, at most
    /// [`LOG_CAPACITY`].
    log: VecDeque<Instant>,
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
            log: VecDeque::new(),
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
        if self.log.len() == LOG_CAPACITY {
            self.log.pop_front();
        }
        self.log.push_back(instant.clone());
        Some(instant)
    }
}

impl Ephemeral {
    /// An empty line.
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
    /// the link facts pointing at this line.
    pub fn entity(&self) -> &Entity {
        &self.entity
    }

    /// Whether `other` is a handle to this same store.
    pub fn is(&self, other: &Ephemeral) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// The line's identity now.
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

    /// Sort keys of every fact this line hides beneath it. Shared, so
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
        );
        let first = &line.since(0).expect("in the ring")[0];
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
        line.retract(
            the!("person/name")
                .of("id:a".parse().unwrap())
                .is("A".to_string()),
        );
        let removed = &line.since(1).expect("in the ring")[0];
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
        let shadowed = &line.since(2).expect("a tombstone is a visible change")[0];
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
        let lifted = &line.since(3).expect("lifting a tombstone is visible")[0];
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
    fn it_reports_instants_since_a_pin_and_gaps_past_the_ring() {
        let line = Ephemeral::new();
        assert_eq!(line.since(0), Some(Vec::new()));
        line.assert(claim("id:a", "person/name", "A"));
        line.assert(claim("id:b", "person/name", "B"));
        let since = line.since(0).expect("within the ring");
        assert_eq!(since.len(), 2);
        assert_eq!(since[0].sequence, 1);
        assert_eq!(since[1].asserted, vec![fact("id:b", "person/name", "B")]);
        assert_eq!(line.since(1).expect("within the ring").len(), 1);
        assert_eq!(line.since(2), Some(Vec::new()));

        for index in 0..LOG_CAPACITY {
            line.assert(claim(&format!("id:{index}"), "person/tag", "x"));
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
        let pinned = line.revision().sequence;
        assert!(line.retain_entities(|entity| entity.to_string() != "site:1"));
        let dropped = &line.since(pinned).expect("in the ring")[0];
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
