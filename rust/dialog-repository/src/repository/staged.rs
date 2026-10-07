//! The staged store: a transaction's writes, held so that reading them
//! costs what reading the tree does.
//!
//! A transaction is read "as if committed" before it commits, often
//! once per statement it applies, so its writes are held the way
//! [`Ephemeral`](crate::Ephemeral) holds session facts: under the
//! tree's three index keys ([`Facts`]), so a selector is a range read
//! that comes out in tree order. What differs is what a write means:
//!
//! - **The last write to a fact wins.** An assert holds the fact; a
//!   retract drops it and hides it in the lines beneath (a tombstone),
//!   whether or not this store held it. A retract followed by an
//!   assert keeps both, so the commit retracts then re-asserts, exactly
//!   as the writes were issued.
//! - **A replace claims its cell.** A cardinality-one replace drops
//!   every other value this store holds at its `(entity, attribute)`
//!   and hides every value the lines beneath hold there, so a read
//!   sees only what the commit will leave.
//! - **Asset changes ride along.** An import or discard is held as the
//!   batch holds it, the later change to an asset winning, and handed
//!   to the commit with the facts. It is not read: the asset's fact is
//!   recorded only when the commit stores the asset.
//!
//! Nothing is logged or hashed: nothing subscribes to a transaction.
//! Clones share the store until one of them writes ([`Arc::make_mut`]),
//! so a query takes the transaction's view without copying it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, AssetChange, Attribute, Change, Changes, Entity, Instruction,
    Policy, SortKey, Statement, Update, Value, sort_key,
};
use dialog_search_tree::Manifest;

use super::ephemeral::{EphemeralRevision, Facts};
use crate::Revision;
use crate::repository::branch::ReadSettlement;
use crate::rules::conclusion_attr;

mod electing;

use electing::ElectingCells;

/// A transaction's writes. See the [module docs](self).
#[derive(Clone, Debug, Default)]
pub(crate) struct Staged(Arc<State>);

#[derive(Clone, Debug, Default)]
struct State {
    /// What the transaction asserted and has not retracted since.
    facts: Facts,
    /// What the transaction retracted, by sort key under the store's
    /// format: hidden in the lines beneath, and retracted at commit.
    retracted: BTreeMap<SortKey, Artifact>,
    /// The sort keys of `retracted`, shared with readers.
    tombstones: Arc<HashSet<SortKey>>,
    /// The asset changes, held as a batch with no facts.
    assets: Changes,
    /// Every write in the order the transaction made it, cell by cell:
    /// what the commit applies, so a write that succeeds a claim is
    /// settled against the line and the writes before it, in order.
    log: Vec<(Attribute, Entity, Change)>,
    /// The cells a write under a choosing policy asserted, kept past
    /// their retraction: the cells a read of a range must settle,
    /// found by the range's entity and attribute bounds.
    electing: ElectingCells,
    /// The log's writes by cell, in the order the transaction made
    /// them: what a read settles one cell by.
    by_cell: HashMap<(Attribute, Entity), Vec<Change>>,
    /// The settlement of these writes a read or commit made, with what
    /// it observed of the lines. Shared by the clones a query takes; a
    /// write to a shared store detaches its own copy, which keeps
    /// settling where the shared one stopped, the log being append-only.
    cells: Arc<parking_lot::Mutex<Option<SettlementMemo>>>,
    /// Which writes this store holds, as a number no other store's
    /// writes share: minted afresh by every write, so two stores with
    /// the same number hold the same writes. Zero for a store nothing
    /// was written to.
    generation: u64,
}

/// The generations minted so far, across every staged store.
static GENERATIONS: AtomicU64 = AtomicU64::new(1);

/// A settlement of a store's writes under one observation of the lines.
#[derive(Clone)]
struct SettlementMemo {
    observed: ReadObservation,
    settlement: ReadSettlement,
}

impl std::fmt::Debug for SettlementMemo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettlementMemo")
            .field("observed", &self.observed)
            .finish_non_exhaustive()
    }
}

/// How a read sees one cell's writes settled: the claims the writes
/// succeed, hidden from the line and from the store's own rows (a
/// succeeded claim is the line's, or an earlier write of the cell the
/// commit squashes away); and the values written under a choosing
/// policy the line already held as claims, hidden from the store's own
/// rows, since the commit writes nothing for them and the line's row
/// is the read's.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct CellSettlement {
    /// The claims the cell's writes succeed.
    pub(crate) succeeded: Vec<Artifact>,
    /// The written facts the cell already held as claims.
    pub(crate) held: Vec<Artifact>,
}

/// What a read settlement observed: the lines it settled against and
/// the metadata it read with. A later read with the same observation
/// settles to the same writes.
#[derive(Clone, Debug)]
pub(crate) struct ReadObservation {
    /// Each line's head.
    pub(crate) heads: Vec<Option<Revision>>,
    /// Each line's session overlay.
    pub(crate) overlays: Vec<EphemeralRevision>,
    /// The metadata the read folded in.
    pub(crate) metadata: Arc<Changes>,
}

impl ReadObservation {
    fn matches(&self, other: &ReadObservation) -> bool {
        self.heads == other.heads
            && self.overlays == other.overlays
            && (Arc::ptr_eq(&self.metadata, &other.metadata) || *self.metadata == *other.metadata)
    }
}

impl State {
    /// Apply one recorded write: hold or drop its fact for reads, give
    /// it its place among the transaction's writes, and log it. A
    /// write under a choosing policy holds its value as one more
    /// candidate until the commit settles which claim it succeeds; a
    /// read over the
    /// transaction settles the same way (see
    /// [`succession`](crate::repository::branch::transaction)).
    fn apply_change(&mut self, the: &Attribute, of: &Entity, change: &Change) {
        // A write that repeats the cell's latest write exactly, value and
        // policy alike, is the same write: the facts already hold the
        // value, the tree would fold the two asserts into one claim, and
        // a commit would otherwise settle the cell in the transactor as a
        // cell written twice. tonk seeds a library's schema into a
        // transaction and then evaluates the document that asserts the
        // same facts, so every cell of a library install arrives twice.
        if matches!(change, Change::Assert(..))
            && self
                .by_cell
                .get(&(the.clone(), of.clone()))
                .and_then(|writes| writes.last())
                .is_some_and(|latest| latest == change)
        {
            return;
        }
        // A settlement survives the writes after it: the log is
        // append-only, and the next read settles only what it has not.
        // A clone taken before the write keeps the memo it shares; this
        // store continues in its own copy, since the two logs diverge
        // from here.
        if Arc::get_mut(&mut self.cells).is_none() {
            let memo = self.cells.lock().clone();
            self.cells = Arc::new(parking_lot::Mutex::new(memo));
        }
        self.generation = GENERATIONS.fetch_add(1, Ordering::Relaxed);
        let fact = |value: &Value| Artifact {
            the: the.clone(),
            of: of.clone(),
            is: value.clone(),
            cause: None,
        };
        match change {
            Change::Assert(value, policy) => {
                if policy.elects() {
                    self.electing.insert(the, of);
                }
                self.apply(Instruction::Assert(fact(value), policy.clone()));
            }
            Change::Retract(value) => {
                self.apply(Instruction::Retract(fact(value)));
            }
        }
        self.log.push((the.clone(), of.clone(), change.clone()));
        self.by_cell
            .entry((the.clone(), of.clone()))
            .or_default()
            .push(change.clone());
    }

    fn apply(&mut self, instruction: Instruction) {
        match instruction {
            // A write holds its value here and retires nothing: what a
            // choosing policy succeeds is settled against the line when
            // the store is read or committed.
            Instruction::Assert(fact, _) => {
                self.facts.insert(fact);
            }
            Instruction::Retract(fact) => {
                self.facts.remove(&fact);
                let key = sort_key(&fact, self.facts.manifest());
                if !self.retracted.contains_key(&key) {
                    Arc::make_mut(&mut self.tombstones).insert(key.clone());
                    self.retracted.insert(key, fact);
                }
            }
        }
    }
}

impl Staged {
    /// Assert a statement: its asserts hold, its replaces claim their
    /// cells, its retracts hide.
    pub(crate) fn assert<S: Statement>(&mut self, statement: S) {
        let mut changes = Changes::new();
        statement.assert(&mut changes);
        self.apply(changes);
    }

    /// Retract a statement: each of its facts is dropped and hidden.
    pub(crate) fn retract<S: Statement>(&mut self, statement: S) {
        let mut changes = Changes::new();
        statement.retract(&mut changes);
        self.apply(changes);
    }

    /// Apply a batch, each `(entity, attribute)` cell's writes in the
    /// order they were recorded.
    pub(crate) fn apply(&mut self, mut changes: Changes) {
        if changes.is_empty() {
            return;
        }
        let state = Arc::make_mut(&mut self.0);
        for change in changes.take_assets() {
            match change {
                AssetChange::Import(asset) => state.assets.import(asset),
                AssetChange::Discard(asset) => state.assets.discard(asset),
            }
        }
        // A batch keeps its cells in a hash map: taken in that order,
        // the log, and the order the tree is handed its writes, would
        // differ from run to run, and a buffered tree's shape follows
        // the order of its writes. The cells are taken by entity and
        // attribute instead, each cell's writes in the order they were
        // recorded, so the same batch commits the same tree.
        let mut writes: Vec<(&Entity, &Attribute, &Change)> = changes.iter().collect();
        writes.sort_by(|(of, the, _), (other_of, other_the, _)| {
            (*of, *the).cmp(&(*other_of, *other_the))
        });
        for (entity, attribute, change) in writes {
            state.apply_change(attribute, entity, change);
        }
    }

    /// Apply one write, as [`apply`](Self::apply) does for a batch.
    pub(crate) fn apply_change(&mut self, the: &Attribute, of: &Entity, change: &Change) {
        Arc::make_mut(&mut self.0).apply_change(the, of, change);
    }

    /// The number naming the writes this store holds: equal between
    /// stores holding the same writes, different otherwise.
    pub(crate) fn generation(&self) -> u64 {
        self.0.generation
    }

    /// Every write in the order the transaction made it.
    pub(crate) fn log(&self) -> &[(Attribute, Entity, Change)] {
        &self.0.log
    }

    /// Whether any write succeeds a claim the commit has yet to settle.
    pub(crate) fn has_successions(&self) -> bool {
        !self.0.electing.is_empty()
    }

    /// The writes as the batch a commit applies: each cell's writes in
    /// the order the transaction made them, squashed as one commit's
    /// are ([`squash`]), a choosing write as it was written, for the
    /// transactor or the tree to settle. Applying it to a line leaves
    /// what this store reads as over it.
    pub(crate) fn export(&self) -> Changes {
        let mut changes = self.0.assets.clone();
        let mut cells: Vec<(Attribute, Entity)> = Vec::new();
        let mut writes: HashMap<(Attribute, Entity), Vec<Change>> = HashMap::new();
        for (the, of, change) in &self.0.log {
            let key = (the.clone(), of.clone());
            if !writes.contains_key(&key) {
                cells.push(key.clone());
            }
            writes.entry(key).or_default().push(change.clone());
        }
        for key in cells {
            let cell = writes.remove(&key).unwrap_or_default();
            let (the, of) = key;
            changes.put_cell(the, of, squash(cell, &|_| true));
        }
        changes
    }

    /// Whether nothing was written.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.log.is_empty() && !self.0.assets.has_assets()
    }

    /// The held facts a selector matches, in the store's own key order.
    pub(crate) fn scan(&self, selector: &ArtifactSelector<Constrained>) -> Vec<Artifact> {
        self.0.facts.scan(selector)
    }

    /// The held facts a selector matches, in the order a scan of a tree
    /// written under `manifest` would produce them: what a query merges
    /// with that tree's rows.
    pub(crate) fn select(
        &self,
        selector: &ArtifactSelector<Constrained>,
        manifest: &Manifest,
    ) -> Vec<Artifact> {
        self.0.facts.select(selector, manifest)
    }

    /// Sort keys of every fact this store hides beneath it, keyed under
    /// `manifest`: the format of the tree whose rows they are checked
    /// against. Shared when that is the store's own format, so a read
    /// never copies the set; keyed afresh under another.
    pub(crate) fn tombstones(&self, manifest: &Manifest) -> Arc<HashSet<SortKey>> {
        if manifest == self.0.facts.manifest() {
            return self.0.tombstones.clone();
        }
        Arc::new(
            self.0
                .retracted
                .values()
                .map(|fact| sort_key(fact, manifest))
                .collect(),
        )
    }

    /// The cells written under a choosing policy that `selector`
    /// reaches, retracted since or not: what a read of that range
    /// settles. A bound on the value is no bound on the cell, since a
    /// write may succeed a claim of a value it does not share, so the
    /// cells are found by the entity and attribute bounds alone.
    pub(crate) fn electing_cells_within(
        &self,
        selector: &ArtifactSelector<Constrained>,
    ) -> Vec<(Attribute, Entity)> {
        self.0.electing.within(selector)
    }

    /// The writes of one cell, in the order the transaction made them.
    #[cfg(test)]
    pub(crate) fn writes_of(&self, the: &Attribute, of: &Entity) -> Vec<Change> {
        self.0
            .by_cell
            .get(&(the.clone(), of.clone()))
            .cloned()
            .unwrap_or_default()
    }

    /// The settlement of these writes made under `observed`, taken out
    /// of the store for the caller to advance and put back; `None` when
    /// none was kept under that observation.
    pub(crate) fn take_settlement(&self, observed: &ReadObservation) -> Option<ReadSettlement> {
        let mut memo = self.0.cells.lock();
        if memo
            .as_ref()
            .is_some_and(|memo| memo.observed.matches(observed))
        {
            memo.take().map(|memo| memo.settlement)
        } else {
            None
        }
    }

    /// Keep `settlement`, made under `observed`, for the next read or
    /// the commit.
    pub(crate) fn put_settlement(&self, observed: &ReadObservation, settlement: ReadSettlement) {
        *self.0.cells.lock() = Some(SettlementMemo {
            observed: observed.clone(),
            settlement,
        });
    }

    /// Whether this store holds any rule, for any concept.
    pub(crate) fn holds_rules(&self) -> bool {
        !self
            .scan(&ArtifactSelector::new().the(conclusion_attr()))
            .is_empty()
    }
}

/// One cell's writes squashed as the writes of one commit are: a
/// transaction is a commit that has not been flushed. An assertion a
/// later retraction of the same value cancels is dropped; the
/// retraction stays when `held` says the line holds the value, and is
/// dropped too when it does not, as the value then never reached the
/// line. A write repeating the cell's standing change for its value is
/// dropped. A retraction followed by an assertion of the same value
/// keeps both: the commit retracts the line's claim and asserts afresh.
/// An assertion under a choosing policy is kept as written, for the
/// transactor or the tree to settle.
pub(crate) fn squash(cell: Vec<Change>, held: &dyn Fn(&Value) -> bool) -> Vec<Change> {
    let mut out: Vec<Change> = Vec::with_capacity(cell.len());
    for change in cell {
        let last = out
            .iter()
            .rposition(|prior| prior.value() == change.value());
        match (&change, last.map(|at| &out[at])) {
            (Change::Assert(_, policy), Some(Change::Assert(_, prior))) if policy == prior => {}
            (Change::Retract(_), Some(Change::Retract(_))) => {}
            (Change::Retract(value), Some(Change::Assert(_, Policy::All))) => {
                out.remove(last.expect("a prior write"));
                let before = out
                    .iter()
                    .rposition(|prior| prior.value() == change.value());
                let retracted = matches!(before.map(|at| &out[at]), Some(Change::Retract(_)));
                if !retracted && held(value) {
                    out.push(change);
                }
            }
            _ => out.push(change),
        }
    }
    out
}

impl From<Changes> for Staged {
    fn from(changes: Changes) -> Self {
        let mut staged = Staged::default();
        staged.apply(changes);
        staged
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use dialog_artifacts::{Asset, Change, Policy, Value};

    fn fact(of: &str, the: &str, is: &str) -> Artifact {
        Artifact {
            the: the.parse().expect("attribute"),
            of: of.parse().expect("entity"),
            is: Value::String(is.into()),
            cause: None,
        }
    }

    fn apply(staged: &mut Staged, instruction: Instruction) {
        let mut changes = Changes::new();
        match instruction {
            Instruction::Assert(f, policy) => changes.associate(f.the, f.of, f.is, policy),
            Instruction::Retract(f) => changes.dissociate(f.the, f.of, f.is),
        }
        staged.apply(changes);
    }

    fn held(staged: &Staged, of: &str, the: &str) -> Vec<Value> {
        let selector = ArtifactSelector::new()
            .of(of.parse().expect("entity"))
            .the(the.parse().expect("attribute"));
        staged.scan(&selector).into_iter().map(|f| f.is).collect()
    }

    /// The exported batch, per fact: what the commit does to it.
    fn exported(staged: &Staged) -> Vec<(String, Change)> {
        let mut out: Vec<(String, Change)> = staged
            .export()
            .iter()
            .map(|(of, the, change)| (format!("{of} {the}"), change.clone()))
            .collect();
        out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        out
    }

    /// A read finds the cells it must settle by the log's choosing
    /// writes, a retracted one included; the settlement is kept for a
    /// read observing the same lines, missed by one observing other
    /// metadata, and a write to a shared store continues in a copy.
    #[dialog_common::test]
    fn it_finds_electing_cells_by_their_writes_and_keeps_their_settlement() {
        let mut staged = Staged::default();
        let the: Attribute = "person/name".parse().expect("attribute");
        let a: Entity = "id:a".parse().expect("entity");
        let b: Entity = "id:b".parse().expect("entity");
        staged.apply_change(
            &the,
            &a,
            &Change::Assert(Value::String("A".into()), Policy::Last),
        );
        staged.apply_change(
            &the,
            &b,
            &Change::Assert(Value::String("B".into()), Policy::Last),
        );
        staged.apply_change(&the, &b, &Change::Retract(Value::String("B".into())));
        staged.apply_change(
            &the,
            &a,
            &Change::Assert(Value::String("C".into()), Policy::All),
        );

        let mut cells = staged.electing_cells_within(&ArtifactSelector::new().the(the.clone()));
        cells.sort();
        assert_eq!(
            cells,
            vec![(the.clone(), a.clone()), (the.clone(), b.clone())],
            "both cells, the retracted write's included"
        );
        assert_eq!(
            staged.electing_cells_within(&ArtifactSelector::new().the(the.clone()).of(b.clone())),
            vec![(the.clone(), b.clone())]
        );
        assert_eq!(
            staged.writes_of(&the, &b),
            vec![
                Change::Assert(Value::String("B".into()), Policy::Last),
                Change::Retract(Value::String("B".into()))
            ]
        );

        let overlay = super::super::ephemeral::Ephemeral::new();
        let observed = ReadObservation {
            heads: vec![None],
            overlays: vec![overlay.revision()],
            metadata: Arc::new(Changes::new()),
        };
        assert!(staged.take_settlement(&observed).is_none());
        let settlement = ReadSettlement::new(dialog_artifacts::history::Edition::GENESIS);
        staged.put_settlement(&observed, settlement.clone());

        // Another observation of the lines misses it.
        let elsewhere = ReadObservation {
            metadata: Arc::new({
                let mut changes = Changes::new();
                changes.associate(
                    the.clone(),
                    a.clone(),
                    Value::String("x".into()),
                    Policy::All,
                );
                changes
            }),
            ..observed.clone()
        };
        assert!(staged.take_settlement(&elsewhere).is_none());

        // A clone shares it; a write to the clone continues in a copy of
        // its own, so taking the clone's leaves the original's.
        let mut shared = staged.clone();
        shared.apply_change(
            &the,
            &a,
            &Change::Assert(Value::String("D".into()), Policy::Last),
        );
        assert!(shared.take_settlement(&observed).is_some());
        assert!(
            staged.take_settlement(&observed).is_some(),
            "the store written before the clone keeps its own"
        );
        assert!(staged.take_settlement(&observed).is_none(), "taken once");
    }

    /// A write that repeats the cell's latest write exactly is the same
    /// write: it is recorded once, and the cell is not written twice.
    #[dialog_common::test]
    fn it_records_a_repeated_write_once() {
        let mut staged = Staged::default();
        let the: Attribute = "person/name".parse().expect("attribute");
        let a: Entity = "id:a".parse().expect("entity");
        let write = Change::Assert(Value::String("A".into()), Policy::Last);
        staged.apply_change(&the, &a, &write);
        let written = staged.generation();
        staged.apply_change(&the, &a, &write);
        assert_eq!(staged.writes_of(&the, &a), vec![write.clone()]);
        assert_eq!(staged.generation(), written, "the repeat is no write");
        assert_eq!(
            exported(&staged),
            vec![("id:a person/name".into(), write.clone())]
        );

        let other = Change::Assert(Value::String("A".into()), Policy::All);
        staged.apply_change(&the, &a, &other);
        assert_eq!(
            staged.writes_of(&the, &a),
            vec![write.clone(), other.clone()],
            "the same value under another policy is another write"
        );
        staged.apply_change(&the, &a, &write);
        assert_eq!(
            staged.writes_of(&the, &a),
            vec![write.clone(), other, write],
            "only the latest write folds a repeat"
        );
    }

    /// A store's generation names its writes: a clone shares it, a
    /// write mints a new one, and no two stores written separately
    /// share one.
    #[dialog_common::test]
    fn it_mints_a_generation_per_write() {
        let mut staged = Staged::default();
        assert_eq!(staged.generation(), 0);
        apply(
            &mut staged,
            Instruction::Assert(fact("id:a", "person/name", "A"), Policy::All),
        );
        let written = staged.generation();
        assert_ne!(written, 0);
        let clone = staged.clone();
        assert_eq!(clone.generation(), written);
        apply(
            &mut staged,
            Instruction::Assert(fact("id:a", "person/name", "B"), Policy::All),
        );
        assert_ne!(staged.generation(), written);
        assert_eq!(clone.generation(), written);
        let mut other = Staged::default();
        apply(
            &mut other,
            Instruction::Assert(fact("id:a", "person/name", "A"), Policy::All),
        );
        assert_ne!(other.generation(), written);
        assert_ne!(other.generation(), staged.generation());
    }

    #[dialog_common::test]
    fn it_retracts_what_was_asserted_before() {
        let mut staged = Staged::default();
        let x = fact("id:a", "person/name", "A");
        apply(
            &mut staged,
            Instruction::Assert(x.clone(), dialog_artifacts::Policy::All),
        );
        apply(&mut staged, Instruction::Retract(x.clone()));
        assert!(held(&staged, "id:a", "person/name").is_empty());
        assert!(
            staged
                .tombstones(&Manifest::default())
                .contains(&sort_key(&x, &Manifest::default())),
            "a retract hides the fact beneath even when this store held it"
        );
        assert_eq!(
            exported(&staged),
            vec![(
                "id:a person/name".into(),
                Change::Retract(Value::String("A".into()))
            )]
        );
    }

    #[dialog_common::test]
    fn it_squashes_a_cell_as_one_commit() {
        let a = || Value::String("A".into());
        let b = || Value::String("B".into());
        let unknown = |_: &Value| true;
        // A later retraction cancels the assertion; the retraction stays
        // for the line's claim, unless the line is known not to hold it.
        assert_eq!(
            squash(
                vec![
                    Change::Assert(a(), dialog_artifacts::Policy::All),
                    Change::Retract(a())
                ],
                &unknown
            ),
            vec![Change::Retract(a())]
        );
        assert_eq!(
            squash(
                vec![
                    Change::Assert(a(), dialog_artifacts::Policy::All),
                    Change::Retract(a())
                ],
                &|_| false
            ),
            Vec::<Change>::new()
        );
        // A write repeated under one policy is one write.
        assert_eq!(
            squash(
                vec![
                    Change::Assert(a(), dialog_artifacts::Policy::Last),
                    Change::Assert(a(), dialog_artifacts::Policy::Last)
                ],
                &unknown
            ),
            vec![Change::Assert(a(), dialog_artifacts::Policy::Last)]
        );
        // Retract then assert keeps both: the commit retracts the
        // line's claim and asserts afresh.
        assert_eq!(
            squash(
                vec![
                    Change::Retract(a()),
                    Change::Assert(a(), dialog_artifacts::Policy::All)
                ],
                &unknown
            ),
            vec![
                Change::Retract(a()),
                Change::Assert(a(), dialog_artifacts::Policy::All)
            ]
        );
        // Retract, assert, retract: the second retraction cancels the
        // assertion and repeats the first.
        assert_eq!(
            squash(
                vec![
                    Change::Retract(a()),
                    Change::Assert(a(), dialog_artifacts::Policy::All),
                    Change::Retract(a())
                ],
                &unknown
            ),
            vec![Change::Retract(a())]
        );
        // Repeating a write is dropped; other values are untouched.
        assert_eq!(
            squash(
                vec![
                    Change::Assert(a(), dialog_artifacts::Policy::All),
                    Change::Assert(b(), dialog_artifacts::Policy::All),
                    Change::Assert(a(), dialog_artifacts::Policy::All),
                    Change::Retract(b()),
                    Change::Retract(b())
                ],
                &unknown
            ),
            vec![
                Change::Assert(a(), dialog_artifacts::Policy::All),
                Change::Retract(b())
            ]
        );
        // A choosing write does not reset the cell: it is kept as
        // written beside the writes before it, a later retraction too.
        assert_eq!(
            squash(
                vec![
                    Change::Assert(a(), dialog_artifacts::Policy::All),
                    Change::Assert(b(), dialog_artifacts::Policy::Last),
                    Change::Retract(b())
                ],
                &unknown
            ),
            vec![
                Change::Assert(a(), dialog_artifacts::Policy::All),
                Change::Assert(b(), dialog_artifacts::Policy::Last),
                Change::Retract(b())
            ]
        );
        // A choosing write is kept for the transactor, retraction or not.
        assert_eq!(
            squash(
                vec![Change::Assert(a(), Policy::Last), Change::Retract(a())],
                &unknown
            ),
            vec![Change::Assert(a(), Policy::Last), Change::Retract(a())]
        );
    }

    #[dialog_common::test]
    fn it_reasserts_what_was_retracted_before() {
        let mut staged = Staged::default();
        let x = fact("id:a", "person/name", "A");
        apply(&mut staged, Instruction::Retract(x.clone()));
        apply(
            &mut staged,
            Instruction::Assert(x.clone(), dialog_artifacts::Policy::All),
        );
        assert_eq!(
            held(&staged, "id:a", "person/name"),
            vec![Value::String("A".into())]
        );
        // The commit retracts, then asserts: the fact survives.
        let changes: Vec<Change> = staged
            .export()
            .iter()
            .map(|(_, _, change)| change.clone())
            .collect();
        assert_eq!(
            changes,
            vec![
                Change::Retract(Value::String("A".into())),
                Change::Assert(Value::String("A".into()), dialog_artifacts::Policy::All)
            ]
        );
    }

    /// A write under a choosing policy holds its value beside the
    /// cell's other staged claims and retires nothing here: what it
    /// succeeds is settled against the line when the store is read or
    /// committed. A retraction of a fact the store does not hold still
    /// hides it beneath.
    #[dialog_common::test]
    fn it_holds_a_choosing_write_beside_the_cell() {
        let mut staged = Staged::default();
        apply(
            &mut staged,
            Instruction::Assert(fact("id:a", "person/name", "A"), Policy::All),
        );
        apply(
            &mut staged,
            Instruction::Retract(fact("id:a", "person/name", "old")),
        );
        apply(
            &mut staged,
            Instruction::Assert(fact("id:a", "person/name", "B"), Policy::Last),
        );
        assert_eq!(
            held(&staged, "id:a", "person/name"),
            vec![Value::String("A".into()), Value::String("B".into())]
        );
        assert_eq!(
            staged.tombstones(&Manifest::default()).len(),
            1,
            "the retract of a fact the store does not hold hides it beneath"
        );
        assert_eq!(
            exported(&staged),
            vec![
                (
                    "id:a person/name".into(),
                    Change::Assert(Value::String("A".into()), Policy::All)
                ),
                (
                    "id:a person/name".into(),
                    Change::Assert(Value::String("B".into()), Policy::Last)
                ),
                (
                    "id:a person/name".into(),
                    Change::Retract(Value::String("old".into()))
                ),
            ]
        );
    }

    #[dialog_common::test]
    fn it_keeps_later_writes_to_a_cell_written_under_last() {
        let mut staged = Staged::default();
        apply(
            &mut staged,
            Instruction::Assert(
                fact("id:a", "person/name", "B"),
                dialog_artifacts::Policy::Last,
            ),
        );
        apply(
            &mut staged,
            Instruction::Retract(fact("id:a", "person/name", "B")),
        );
        apply(
            &mut staged,
            Instruction::Assert(
                fact("id:a", "person/name", "C"),
                dialog_artifacts::Policy::All,
            ),
        );
        assert_eq!(
            held(&staged, "id:a", "person/name"),
            vec![Value::String("C".into())]
        );
        // The choosing write is kept as written, then the retract and the assert.
        let changes: Vec<Change> = staged
            .export()
            .iter()
            .map(|(_, _, change)| change.clone())
            .collect();
        assert_eq!(
            changes,
            vec![
                Change::Assert(Value::String("B".into()), dialog_artifacts::Policy::Last),
                Change::Retract(Value::String("B".into())),
                Change::Assert(Value::String("C".into()), dialog_artifacts::Policy::All)
            ]
        );
    }

    #[dialog_common::test]
    fn it_shares_the_store_until_a_clone_writes() {
        let mut staged = Staged::default();
        apply(
            &mut staged,
            Instruction::Assert(
                fact("id:a", "person/name", "A"),
                dialog_artifacts::Policy::All,
            ),
        );
        let view = staged.clone();
        apply(
            &mut staged,
            Instruction::Assert(
                fact("id:b", "person/name", "B"),
                dialog_artifacts::Policy::All,
            ),
        );
        assert!(held(&view, "id:b", "person/name").is_empty());
        assert_eq!(
            held(&staged, "id:b", "person/name"),
            vec![Value::String("B".into())]
        );
    }

    #[dialog_common::test]
    fn it_hands_its_asset_changes_to_the_commit() {
        let kept = Asset::new(b"kept".to_vec());
        let dropped = Asset::new(b"dropped".to_vec());
        let mut staged = Staged::default();

        let mut first = Changes::new();
        first.import(kept.clone());
        first.import(dropped.clone());
        staged.apply(first);
        assert!(!staged.is_empty());

        let mut second = Changes::new();
        second.discard(dropped.clone());
        staged.apply(second);

        let exported = staged.export();
        let assets: Vec<AssetChange> = exported.assets().cloned().collect();
        assert_eq!(assets.len(), 2);
        assert!(assets.contains(&AssetChange::Import(kept)));
        assert!(assets.contains(&AssetChange::Discard(dropped)));
        assert!(exported.iter().next().is_none());
    }
}
