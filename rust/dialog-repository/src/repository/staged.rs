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

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, AssetChange, Attribute, Changes, Entity, Instruction, SortKey,
    Statement, Update, sort_key,
};
use dialog_search_tree::Manifest;

use super::ephemeral::Facts;
use crate::rules::conclusion_attr;

/// Cells a replace claimed, by attribute then entity, in the byte form
/// a [`SortKey`] carries them, so a row's key is checked without
/// allocating.
pub(crate) type Cells = HashMap<Vec<u8>, HashSet<Vec<u8>>>;

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
    /// The value each claimed cell was replaced with, by cell.
    replaced: HashMap<(Attribute, Entity), Artifact>,
    /// The claimed cells, shared with readers.
    cells: Arc<Cells>,
    /// The asset changes, held as a batch with no facts.
    assets: Changes,
}

impl State {
    fn apply(&mut self, instruction: Instruction) {
        match instruction {
            Instruction::Assert(fact) => {
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
            Instruction::Replace(fact) => {
                for prior in self.facts.cell(&fact.of, &fact.the) {
                    if prior.is != fact.is {
                        self.facts.remove(&prior);
                    }
                }
                // The cell is claimed, so what was retracted in it is
                // hidden anyway and the replace retracts it at commit.
                let (the, of, _) = sort_key(&fact, self.facts.manifest());
                let start = (the.clone(), of.clone(), Vec::new());
                let dropped: Vec<SortKey> = self
                    .retracted
                    .range(start..)
                    .take_while(|(key, _)| key.0 == the && key.1 == of)
                    .map(|(key, _)| key.clone())
                    .collect();
                if !dropped.is_empty() {
                    let tombstones = Arc::make_mut(&mut self.tombstones);
                    for key in dropped {
                        tombstones.remove(&key);
                        self.retracted.remove(&key);
                    }
                }
                Arc::make_mut(&mut self.cells)
                    .entry(the)
                    .or_default()
                    .insert(of);
                self.replaced
                    .insert((fact.the.clone(), fact.of.clone()), fact.clone());
                self.facts.insert(fact);
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
        for instruction in changes.into_instructions() {
            state.apply(instruction);
        }
    }

    /// The writes as the batch a commit applies: each replace first, as
    /// it resets its cell, then every retraction, then every held fact.
    /// Applying it to a line leaves what this store reads as over it.
    pub(crate) fn export(&self) -> Changes {
        let mut changes = self.0.assets.clone();
        for fact in self.0.replaced.values() {
            changes.associate_unique(fact.the.clone(), fact.of.clone(), fact.is.clone());
        }
        for fact in self.0.retracted.values() {
            changes.dissociate(fact.the.clone(), fact.of.clone(), fact.is.clone());
        }
        for fact in self.0.facts.iter() {
            let claimed = self
                .0
                .replaced
                .get(&(fact.the.clone(), fact.of.clone()))
                .is_some_and(|replace| replace.is == fact.is);
            if !claimed {
                changes.associate(fact.the.clone(), fact.of.clone(), fact.is.clone());
            }
        }
        changes
    }

    /// Whether nothing was written.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.facts.is_empty() && self.0.retracted.is_empty() && !self.0.assets.has_assets()
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

    /// Whether this store holds any rule, for any concept.
    pub(crate) fn holds_rules(&self) -> bool {
        !self
            .scan(&ArtifactSelector::new().the(conclusion_attr()))
            .is_empty()
    }

    /// The cells whose values beneath this store hides. A cell is an
    /// attribute and an entity, which key the same under every format.
    pub(crate) fn cells(&self) -> Arc<Cells> {
        self.0.cells.clone()
    }
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
    use dialog_artifacts::{Asset, Change, Value};

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
            Instruction::Assert(f) => changes.associate(f.the, f.of, f.is),
            Instruction::Replace(f) => changes.associate_unique(f.the, f.of, f.is),
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

    #[dialog_common::test]
    fn it_retracts_what_was_asserted_before() {
        let mut staged = Staged::default();
        let x = fact("id:a", "person/name", "A");
        apply(&mut staged, Instruction::Assert(x.clone()));
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
    fn it_reasserts_what_was_retracted_before() {
        let mut staged = Staged::default();
        let x = fact("id:a", "person/name", "A");
        apply(&mut staged, Instruction::Retract(x.clone()));
        apply(&mut staged, Instruction::Assert(x.clone()));
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
                Change::Assert(Value::String("A".into()))
            ]
        );
    }

    #[dialog_common::test]
    fn it_claims_a_replaced_cell() {
        let mut staged = Staged::default();
        apply(
            &mut staged,
            Instruction::Assert(fact("id:a", "person/name", "A")),
        );
        apply(
            &mut staged,
            Instruction::Retract(fact("id:a", "person/name", "old")),
        );
        apply(
            &mut staged,
            Instruction::Replace(fact("id:a", "person/name", "B")),
        );
        assert_eq!(
            held(&staged, "id:a", "person/name"),
            vec![Value::String("B".into())]
        );
        assert!(
            staged.tombstones(&Manifest::default()).is_empty(),
            "the claimed cell hides what the retract did"
        );
        let cells = staged.cells();
        assert!(
            cells
                .get("person/name".as_bytes())
                .is_some_and(|entities| entities.contains("id:a".as_bytes()))
        );
        assert_eq!(
            exported(&staged),
            vec![(
                "id:a person/name".into(),
                Change::Replace(Value::String("B".into()))
            )]
        );
    }

    #[dialog_common::test]
    fn it_keeps_later_writes_to_a_replaced_cell() {
        let mut staged = Staged::default();
        apply(
            &mut staged,
            Instruction::Replace(fact("id:a", "person/name", "B")),
        );
        apply(
            &mut staged,
            Instruction::Retract(fact("id:a", "person/name", "B")),
        );
        apply(
            &mut staged,
            Instruction::Assert(fact("id:a", "person/name", "C")),
        );
        assert_eq!(
            held(&staged, "id:a", "person/name"),
            vec![Value::String("C".into())]
        );
        // Replace resets the cell, then the retract and the assert.
        let changes: Vec<Change> = staged
            .export()
            .iter()
            .map(|(_, _, change)| change.clone())
            .collect();
        assert_eq!(
            changes,
            vec![
                Change::Replace(Value::String("B".into())),
                Change::Retract(Value::String("B".into())),
                Change::Assert(Value::String("C".into()))
            ]
        );
    }

    #[dialog_common::test]
    fn it_shares_the_store_until_a_clone_writes() {
        let mut staged = Staged::default();
        apply(
            &mut staged,
            Instruction::Assert(fact("id:a", "person/name", "A")),
        );
        let view = staged.clone();
        apply(
            &mut staged,
            Instruction::Assert(fact("id:b", "person/name", "B")),
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
