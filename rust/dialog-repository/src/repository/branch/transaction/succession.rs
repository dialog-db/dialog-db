//! Policy of a claim under a choosing policy, settled at commit and
//! in a transaction's own reads.
//!
//! A write through an attribute read under `last`, `max`, `min` or
//! `top` succeeds the claim the attribute stands for: the candidate a
//! read under the policy would return, over what the write observed.
//! That is a transactional guarantee: a write observes the line and
//! the writes before it in its own transaction, in order, and succeeds
//! whatever a reader at that point would have observed. A write under
//! `all` asserts and succeeds nothing. An inductive rule's head
//! observes the round view it fired on.
//!
//! What a read returns depends on the line and on the rules deriving
//! the relation, so the statement records a [`Policy`] and the
//! settlement runs here, over the transaction's ordered log: each write
//! is replayed in order over the claims the line holds for its cell;
//! a succession elects among the live claims and the candidates rules
//! derive at that point, retracts the stored claim it elects, and adds
//! its value. A derived candidate is not a claim: when the read elects
//! a derived value nothing is retracted, and the write stands beside it
//! as one more candidate. Writing a value the cell already holds writes
//! nothing. At commit, a cell no rule derives is left to the tree,
//! which elects among the cell's stored claims in the descent that
//! writes the value ([`dialog_artifacts::Instruction::Assert`] under a
//! choosing [`Policy`]); the
//! settlement here is for the cells rules derive, whose candidates the
//! tree cannot see, and for a transaction's own reads. What a cell's
//! writes settle to is squashed as one commit's
//! writes are: an assertion a later retraction cancels leaves only the
//! retraction, so a claim that lived only inside the transaction leaves
//! no tombstone. The same settlement runs for a transaction's own
//! reads, so a read over the transaction sees what the commit will
//! leave.

use crate::repository::CellSettlement;
use crate::repository::branch::session::{Erased, QueryEnv};
use crate::repository::source::Source;
use crate::repository::staged::squash;
use crate::rules::derives_attr;
use crate::{CommitError, Staged};
use dialog_artifacts::history::Edition;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Attribute, Cause, Change, Changes, Entity, Policy, Select, Value,
};
use dialog_capability::Provider;
use dialog_query::attribute::{AttributeDescriptor, Relation, The};
use dialog_query::concept::query::Election;
use dialog_query::query::Output as _;
use dialog_query::rule::statement::Reach;
use dialog_query::source::SelectRules;
use dialog_query::types::Any;
use dialog_query::{
    Binding, Cardinality, Claim, ConceptDescriptor, ConceptFieldDescriptor, ConceptQuery, Match,
    Parameters, Standing, Term,
};
use futures_util::TryStreamExt;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
#[cfg(not(target_arch = "wasm32"))]
use std::env::var_os;
use std::fmt::Display;
use std::sync::Arc;

/// What a settlement is for: the batch a commit applies, or the view a
/// transaction's own reads see.
/// Settle a transaction's writes for its commit against `sources`,
/// read with `overlay`. A write under a choosing policy succeeds the
/// claim the policy elects: where the tree sees every candidate, the
/// write passes through as written and the tree elects in the descent
/// that lands it; where it cannot (some rule derives the relation, the
/// session overlay holds the cell, or the cell is written more than
/// once), the write is settled here to the retraction of the claim it
/// succeeds and the assertion of its value, over the line and the writes
/// before it, in the order the transaction made them.
pub(crate) async fn settle(
    sources: Vec<Source>,
    overlay: Arc<Changes>,
    staged: &Staged,
    env: &Erased,
) -> Result<Changes, CommitError> {
    if !staged.has_successions() {
        return Ok(staged.export());
    }

    // What every write observed of the line: its claims, read once per
    // cell a succession writes.
    let line = QueryEnv::new(sources.clone(), overlay.clone(), env);
    // A commit leaves a cell to the tree unless the tree cannot see
    // every candidate the read elects among: a rule derives the
    // relation (one the line knows, or one this transaction installs,
    // which the line cannot know yet and the settlement below reads
    // through the writes before each succession), or the session
    // overlay holds the cell.
    if !staged.holds_rules() {
        let mut relations: Vec<&Attribute> = Vec::new();
        let mut unseen = false;
        for (the, of, change) in staged.log() {
            if !change.elects() {
                continue;
            }
            if !relations.contains(&the) {
                relations.push(the);
            }
            if overlay_holds(&sources, the, of) {
                unseen = true;
                break;
            }
        }
        if !unseen {
            for the in relations {
                if rules_derive(&line, the).await? {
                    unseen = true;
                    break;
                }
            }
        }
        if !unseen {
            return Ok(staged.export());
        }
    }
    let edition = line.pending_edition();
    let mut cells: HashMap<(Attribute, Entity), Cell> = HashMap::new();
    let mut order: Vec<(Attribute, Entity)> = Vec::new();
    // The writes settled so far, as the view a later write reads the
    // derived candidates through: held back until some write needs the
    // view, which a transaction writing under no derived relation
    // never does.
    let mut prefix = Staged::default();
    let mut unapplied: Vec<(Attribute, Entity, Change)> = Vec::new();
    // Whether some rule derives a relation at each write: a rule the
    // line knows, asked once per relation, or one the prefix installed
    // before the write, found by the relation's probes among the
    // `derives` facts the prefix holds.
    let mut derives = Derives::default();
    // The cells a commit leaves to the tree: no rule derives their
    // relation and no session overlay holds them, so the tree sees
    // every candidate and elects in its own descent. Their writes pass
    // through as written, and the line is never read for them.
    let mut passed: HashSet<(Attribute, Entity)> = HashSet::new();
    // Each cell's writes: whether some write of it elects, and whether
    // every write of it repeats the first.
    let mut writes_per_cell: HashMap<(Attribute, Entity), CellWrites> = HashMap::new();
    for (the, of, change) in staged.log() {
        match writes_per_cell.entry((the.clone(), of.clone())) {
            Entry::Vacant(vacant) => {
                vacant.insert(CellWrites {
                    elects: change.elects(),
                    single: true,
                });
            }
            Entry::Occupied(mut occupied) => {
                let cell = occupied.get_mut();
                cell.elects |= change.elects();
                cell.single = false;
            }
        }
    }

    for (the, of, change) in staged.log() {
        let key = (the.clone(), of.clone());
        if !cells.contains_key(&key) {
            let writes = writes_per_cell.get(&key);
            let settles = writes.is_some_and(|writes| writes.elects);
            // Only a cell written once, or with one write repeated, is
            // left to the tree: the tree settles a batch's writes to
            // one cell as a set, so a value retracted and written back
            // in one transaction must reach it settled. A repeated write
            // is settled here all the same: the tree's election costs
            // more than reading the cell, since it forces the buffered
            // writes down to the leaf it scans (see the settlement
            // measurements in the pull request).
            let leave_to_tree = settles
                && writes.is_some_and(|writes| writes.single)
                && !overlay_holds(&sources, the, of)
                && !derives.at(&line, the).await?
                && !settle_everything();
            if leave_to_tree {
                passed.insert(key.clone());
            }
            let observed = if settles && !leave_to_tree {
                claims_of(&line, the, of).await?
            } else {
                Vec::new()
            };
            cells.insert(key.clone(), Cell::over(observed));
            order.push(key.clone());
        }
        let cell = cells.get_mut(&key).expect("cell loaded above");
        if passed.contains(&key) {
            derives.gained(the, change);
            unapplied.push((the.clone(), of.clone(), change.clone()));
            cell.settled.push(change.clone());
            continue;
        }
        let derived = match change {
            Change::Assert(_, policy) if policy.elects() && derives.at(&line, the).await? => {
                for (the, of, change) in unapplied.drain(..) {
                    prefix.apply_change(&the, &of, &change);
                }
                let view = QueryEnv::new(sources.clone(), overlay.clone(), env)
                    .with_layers(vec![prefix.clone()]);
                let candidates = derived_candidates(&view, the, of).await?;
                drop(view);
                candidates
            }
            _ => Vec::new(),
        };
        for written in cell.write(change, &derived, edition, the, of)? {
            derives.gained(the, &written);
            unapplied.push((the.clone(), of.clone(), written.clone()));
            cell.settled.push(written);
        }
    }
    // What the commit applies: each cell's settled writes, squashed as
    // one commit's are.
    let mut settled = staged.assets().clone();
    for key in order {
        let cell = cells.remove(&key).expect("cell loaded above");
        settled.put_cell(key.0, key.1, cell.squashed());
    }
    Ok(settled)
}

/// Settle every succession `head` holds against `view`, the round view
/// an inductive rule fired on, which is what its head observed: the
/// claims the view holds, the writes before it in the round included,
/// and the candidates rules derive through it.
pub(crate) async fn resolve_against(
    view: &QueryEnv<'_>,
    head: &mut Changes,
) -> Result<(), CommitError> {
    let cells = head.cells_with_successions();
    if cells.is_empty() {
        return Ok(());
    }
    let edition = view.pending_edition();
    for (the, of) in cells {
        let list = head.take_cell(&the, &of);
        let mut cell = Cell::over(claims_of(view, &the, &of).await?);
        let derived = derived_candidates(view, &the, &of).await?;
        for change in &list {
            let written = cell.write(change, &derived, edition, &the, &of)?;
            cell.settled.extend(written);
        }
        head.put_cell(the, of, cell.squashed());
    }
    Ok(())
}

/// Whether some rule `view` knows, or `layer` installs, derives the
/// relation `the` names.
pub(crate) async fn relation_derived(
    view: &QueryEnv<'_>,
    layer: &Staged,
    the: &Attribute,
) -> Result<bool, CommitError> {
    if layer.holds_rules() {
        let staged = layer.staged_derives();
        if !staged.is_empty() {
            let reach = Reach::of(&Relation::Attribute(The::from(the.clone())));
            if reach.probes().iter().any(|probe| staged.contains(probe)) {
                return Ok(true);
            }
        }
    }
    rules_derive(view, the).await
}

/// Settle one cell of `layer` the way a read sees it: against the
/// lines read with `overlay`, and, when `derived`, the candidates rules
/// derive through the whole of `layer`, as a read of the transaction
/// sees every write it made. Returns the line's claims the cell's
/// writes succeed and the written values the cell already held.
pub(crate) async fn settle_cell(
    sources: Vec<Source>,
    overlay: Arc<Changes>,
    layer: &Staged,
    the: &Attribute,
    of: &Entity,
    derived: bool,
    env: &Erased,
) -> Result<CellSettlement, CommitError> {
    let line = QueryEnv::new(sources.clone(), overlay.clone(), env);
    let edition = line.pending_edition();
    let observed = claims_of(&line, the, of).await?;
    let candidates = if derived {
        let view = QueryEnv::new(sources, overlay, env)
            .with_layers(vec![layer.clone()])
            .unsettled();
        derived_candidates(&view, the, of).await?
    } else {
        Vec::new()
    };
    let line_values: Vec<Value> = observed
        .iter()
        .filter(|candidate| candidate.claim)
        .map(|candidate| candidate.value.clone())
        .collect();
    let mut cell = Cell::over(observed);
    let writes = layer.writes_of(the, of);
    // The values the writes staged afresh: a value written back after
    // a retraction is the layer's row, not the line's.
    let mut restaged: Vec<Value> = Vec::new();
    for change in &writes {
        let written = cell.write(change, &candidates, edition, the, of)?;
        if let Change::Assert(value, _) = change
            && !written.is_empty()
        {
            restaged.push(value.clone());
        }
    }
    // What the cell comes to: the claims live after its writes. Every
    // value written or held that is not among them is hidden from the
    // line and the layer alike; a live value the line holds is hidden
    // from the layer's rows, the line's row being the read's.
    let live: Vec<Value> = cell
        .live
        .iter()
        .filter(|candidate| candidate.claim)
        .map(|candidate| candidate.value.clone())
        .collect();
    let fact = |value: &Value| Artifact {
        the: the.clone(),
        of: of.clone(),
        is: value.clone(),
        cause: None,
    };
    let mut settlement = CellSettlement::default();
    let written = writes.iter().filter_map(|change| match change {
        Change::Assert(value, _) => Some(value),
        Change::Retract(_) => None,
    });
    for value in written.chain(line_values.iter()) {
        if !live.contains(value) && !settlement.succeeded.iter().any(|gone| gone.is == *value) {
            settlement.succeeded.push(fact(value));
        }
    }
    for value in &live {
        if line_values.contains(value) && !restaged.contains(value) {
            settlement.held.push(fact(value));
        }
    }
    Ok(settlement)
}

/// What a transaction's writes to one cell amount to, as the commit
/// decides whether the tree can settle them.
struct CellWrites {
    /// Whether some write of the cell elects.
    elects: bool,
    /// Whether the cell is written once.
    single: bool,
}

/// Whether every succession is settled here rather than left to the
/// tree: a measurement switch, native only, for comparing the two
/// paths on one build (temporary).
fn settle_everything() -> bool {
    #[cfg(not(target_arch = "wasm32"))]
    {
        var_os("DIALOG_SETTLE_EVERYTHING").is_some()
    }
    #[cfg(target_arch = "wasm32")]
    {
        false
    }
}

/// A claim or candidate a succession may elect: its value and its
/// standing, and whether it is a stored claim a write can succeed. A
/// candidate a rule derives is not; neither is a session overlay row,
/// which only the session takes back.
struct Candidate {
    value: Value,
    standing: Option<Standing>,
    claim: bool,
}

/// Whether some line's session overlay holds a fact of the cell.
fn overlay_holds(sources: &[Source], the: &Attribute, of: &Entity) -> bool {
    let selector = ArtifactSelector::new().the(the.clone()).of(of.clone());
    sources
        .iter()
        .any(|source| !source.as_ref().overlay().scan(&selector).is_empty())
}

/// The claims a cell holds as `view` reads them: the stored claims a
/// write can succeed, and the session overlay's rows, which it cannot.
async fn claims_of(
    view: &QueryEnv<'_>,
    the: &Attribute,
    of: &Entity,
) -> Result<Vec<Candidate>, CommitError> {
    let failed = |error: &dyn Display| CommitError::Policy(error.to_string());
    let selector = ArtifactSelector::new().the(the.clone()).of(of.clone());
    let rows = Provider::<Select<'_>>::execute(view, selector)
        .await
        .map_err(|error| failed(&error))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| failed(&error))?;
    let mut claims = Vec::with_capacity(rows.len());
    for row in rows {
        let artifact = row.to_owned().map_err(|error| failed(&error))?;
        claims.push(Candidate {
            standing: Some(Standing {
                version: row.standing(),
                cause: Claim::from(artifact.clone()).cause().clone(),
            }),
            value: artifact.is,
            claim: row.key().is_some(),
        });
    }
    Ok(claims)
}

/// The attribute concept over `the`, under which the rules deriving the
/// relation are found.
fn relation_predicate(the: &Attribute) -> ConceptDescriptor {
    let attribute = AttributeDescriptor::over(
        Relation::Attribute(The::from(the.clone())),
        "",
        Cardinality::Many,
        None,
    );
    ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(attribute))
}

/// Which relations rules derive as a commit's writes are settled in
/// order: those the line's rules derive, asked once per relation, and
/// those a rule the writes before installed derives, known by the
/// `derives` facts the settled prefix holds.
#[derive(Default)]
struct Derives {
    /// Whether the line's rules derive the relation, by attribute.
    line: HashMap<Attribute, bool>,
    /// The `on:` entities of the rules the prefix installed.
    staged: HashSet<Entity>,
    /// The trigger entities each relation probes, by attribute.
    probes: HashMap<Attribute, Vec<Entity>>,
}

impl Derives {
    /// Whether some rule derives `the` at this point of the settlement.
    async fn at(&mut self, line: &QueryEnv<'_>, the: &Attribute) -> Result<bool, CommitError> {
        let known = match self.line.get(the) {
            Some(known) => *known,
            None => {
                let known = rules_derive(line, the).await?;
                self.line.insert(the.clone(), known);
                known
            }
        };
        if known {
            return Ok(true);
        }
        if self.staged.is_empty() {
            return Ok(false);
        }
        let probes = self
            .probes
            .entry(the.clone())
            .or_insert_with(|| Reach::of(&Relation::Attribute(The::from(the.clone()))).probes());
        Ok(probes.iter().any(|probe| self.staged.contains(probe)))
    }

    /// Note a write the prefix gained: a `derives` fact names a
    /// relation a rule the prefix installs derives.
    fn gained(&mut self, the: &Attribute, change: &Change) {
        if *the == derives_attr()
            && let Change::Assert(Value::Entity(on), _) = change
        {
            self.staged.insert(on.clone());
        }
    }
}

/// Whether some rule `view` knows derives the relation `the` names:
/// read from the rule index alone, without assembling the relation's
/// bundle.
async fn rules_derive(view: &QueryEnv<'_>, the: &Attribute) -> Result<bool, CommitError> {
    view.rules_derive(the)
        .await
        .map_err(|error| CommitError::Policy(error.to_string()))
}

/// Every candidate a read of the relation sees for the entity through
/// `view`, when some rule derives the relation; nothing otherwise, as
/// the claims are then all there is. A derived one is what the read
/// offers beyond the claims.
async fn derived_candidates(
    view: &QueryEnv<'_>,
    the: &Attribute,
    of: &Entity,
) -> Result<Vec<Candidate>, CommitError> {
    let failed = |error: &dyn Display| CommitError::Policy(error.to_string());
    let predicate = relation_predicate(the);
    let rules = Provider::<SelectRules>::execute(view, predicate.clone())
        .await
        .map_err(|error| failed(&error))?;
    if rules.installed().is_empty() {
        return Ok(Vec::new());
    }
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
    terms.insert(
        ConceptDescriptor::VALUE.to_string(),
        Term::<Any>::var(ConceptDescriptor::VALUE),
    );
    let rows: Vec<Match> = Box::pin(
        ConceptQuery { terms, predicate }
            .evaluate(Match::new().seed(), view)
            .try_vec(),
    )
    .await
    .map_err(|error| failed(&error))?;
    Ok(rows
        .into_iter()
        .filter_map(|row| match row.get(ConceptDescriptor::VALUE) {
            Some(Binding::Present(value)) => Some(Candidate {
                value: value.clone(),
                standing: row
                    .standing_of(ConceptDescriptor::VALUE)
                    .or_else(|| row.standing()),
                claim: false,
            }),
            _ => None,
        })
        .collect())
}

/// One cell's live claims as the writes replayed so far leave them,
/// the values the line held before any of them, and what the writes
/// settled to, in order.
struct Cell {
    live: Vec<Candidate>,
    line: Vec<Value>,
    settled: Vec<Change>,
}

impl Cell {
    /// A cell over the claims the line holds.
    fn over(claims: Vec<Candidate>) -> Self {
        Self {
            line: claims
                .iter()
                .filter(|claim| claim.claim)
                .map(|claim| claim.value.clone())
                .collect(),
            live: claims,
            settled: Vec::new(),
        }
    }

    /// What the writes settled to, squashed as one commit's writes are:
    /// a retraction of a value the line never held, cancelling a staged
    /// assertion of it, leaves nothing.
    fn squashed(self) -> Vec<Change> {
        let line = self.line;
        squash(self.settled, &|value| line.contains(value))
    }

    /// Replay one write: an assertion under `all` adds a claim, standing
    /// at the commit's edition, as every write of the transaction does;
    /// a retraction removes one; an assertion under a choosing policy
    /// elects among the live claims and the derived candidates that are
    /// not claims, retracts the elected claim when it is one and adds
    /// its value, or adds nothing when the cell holds the value already.
    /// Returns what the write settles to.
    fn write(
        &mut self,
        change: &Change,
        derived: &[Candidate],
        edition: Edition,
        the: &Attribute,
        of: &Entity,
    ) -> Result<Vec<Change>, CommitError> {
        let staged = |value: &Value| Candidate {
            standing: Some(Standing {
                version: Some((edition, [0; 32])),
                cause: Cause::from(&Artifact {
                    the: the.clone(),
                    of: of.clone(),
                    is: value.clone(),
                    cause: None,
                }),
            }),
            value: value.clone(),
            claim: true,
        };
        Ok(match change {
            Change::Assert(value, Policy::All) => {
                self.live.retain(|claim| claim.value != *value);
                self.live.push(staged(value));
                vec![change.clone()]
            }
            Change::Retract(value) => {
                self.live.retain(|claim| claim.value != *value);
                vec![change.clone()]
            }
            Change::Assert(value, policy) => {
                if self
                    .live
                    .iter()
                    .any(|claim| claim.claim && claim.value == *value)
                {
                    return Ok(Vec::new());
                }
                let pool: Vec<(Value, Option<Standing>, (Value, bool))> = self
                    .live
                    .iter()
                    .chain(derived.iter().filter(|candidate| {
                        !self.live.iter().any(|claim| claim.value == candidate.value)
                    }))
                    .map(|candidate| {
                        (
                            candidate.value.clone(),
                            candidate.standing.clone(),
                            (candidate.value.clone(), candidate.claim),
                        )
                    })
                    .collect();
                let elected = Election::from(policy)
                    .elect_claims(pool)
                    .map_err(|error| CommitError::Policy(error.to_string()))?;
                let mut settled = Vec::with_capacity(2);
                if let Some((elected, true)) = elected {
                    self.live.retain(|claim| claim.value != elected);
                    settled.push(Change::Retract(elected));
                }
                self.live.push(staged(value));
                settled.push(Change::Assert(value.clone(), Policy::All));
                settled
            }
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::Branch;
    use crate::helpers::test_repo;
    use anyhow::Result;
    use dialog_artifacts::history::Edition;
    use dialog_artifacts::{ArtifactSelector, Attribute, Change, Entity, Policy, Value};
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::The;
    use dialog_query::query::Output as _;
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::types::Any;
    use dialog_query::{
        AttributeStatement, Cardinality, ConceptConclusion, ConceptDescriptor, ConceptQuery,
        DeductiveRule, Parameters, Term,
    };
    use dialog_storage::provider::storage::VolatileSpace;
    use futures_util::StreamExt as _;

    /// Two `last` writes of one cell in one transaction settle to the
    /// later one alone: the second succeeds the first, and the squash
    /// cancels the first's assertion against its retraction, so the
    /// commit carries one assertion and no tombstone.
    #[dialog_common::test]
    fn it_settles_two_last_writes_to_the_later_one() -> Result<()> {
        let the: Attribute = "org/salary".parse()?;
        let of = Entity::new()?;
        let edition = Edition::from(7u64);
        let mut cell = super::Cell::over(Vec::new());
        for value in [200u32, 300] {
            let change = Change::Assert(Value::UnsignedInt(value.into()), Policy::Last);
            let written = cell.write(&change, &[], edition, &the, &of)?;
            cell.settled.extend(written);
        }
        assert_eq!(
            cell.settled,
            vec![
                Change::Assert(Value::UnsignedInt(200), dialog_artifacts::Policy::All),
                Change::Retract(Value::UnsignedInt(200)),
                Change::Assert(Value::UnsignedInt(300), dialog_artifacts::Policy::All),
            ]
        );
        assert_eq!(
            cell.squashed(),
            vec![Change::Assert(
                Value::UnsignedInt(300),
                dialog_artifacts::Policy::All
            )]
        );
        Ok(())
    }

    /// A write of `org/salary` under `max`: the statement a field
    /// reading the relation under that policy writes.
    fn salary(of: &Entity, value: u32) -> AttributeStatement {
        AttributeStatement {
            the: The::from("org/salary".parse::<Attribute>().expect("an attribute")),
            of: of.clone(),
            is: Value::UnsignedInt(value.into()),
            cause: None,
            cardinality: Some(Cardinality::One),
            policy: Some(Policy::Max),
        }
    }

    /// The live claims of `of`'s `org/salary` cell, sorted.
    async fn stored(
        branch: &Branch,
        operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
        of: &Entity,
    ) -> Result<Vec<u128>> {
        let selector = ArtifactSelector::new()
            .the("org/salary".parse()?)
            .of(of.clone());
        let stream = branch.claims().select(selector).perform(operator).await?;
        let mut values: Vec<u128> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|item| item.and_then(|view| view.to_owned()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|artifact| match artifact.is {
                Value::UnsignedInt(value) => Some(value),
                _ => None,
            })
            .collect();
        values.sort();
        Ok(values)
    }

    /// `org/salary` read under `max` for `of`.
    async fn read_max(
        branch: &Branch,
        operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
        of: &Entity,
    ) -> Result<Vec<u64>> {
        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": "max" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = branch
            .select(ConceptQuery { predicate, terms })
            .perform(operator)
            .try_vec()
            .await?;
        rows.iter()
            .map(|row| Ok(row.get::<u64>("salary")?))
            .collect()
    }

    /// A write under `max` succeeds the claim a `max` read returns: the
    /// greatest live claim is retracted beside the written value,
    /// whether the write is greater or smaller than it, so the cell
    /// holds one claim after each write and a read returns the latest.
    #[dialog_common::test]
    async fn it_succeeds_the_claim_a_max_read_returns() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;

        for (write, expected) in [(100u32, 100u128), (200, 200), (150, 150)] {
            branch
                .transaction()
                .assert(salary(&alice, write))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            assert_eq!(
                stored(&branch, &operator, &alice).await?,
                vec![expected],
                "writing {write} succeeds the elected claim"
            );
            assert_eq!(
                read_max(&branch, &operator, &alice).await?,
                vec![expected as u64]
            );
        }

        // Writing the value the cell already holds retracts nothing.
        branch
            .transaction()
            .assert(salary(&alice, 150))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![150]);
        Ok(())
    }

    /// Two writers succeed the same claim concurrently: each retracts
    /// the claim it observed and asserts its own, and the merge keeps
    /// both of theirs, where the read under `max` elects. No write is
    /// lost and no merge is refused.
    #[dialog_common::test]
    async fn it_keeps_both_claims_of_concurrent_successions() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let a = repo.branch("a").open().perform(&operator).await?;
        let b = repo.branch("b").open().perform(&operator).await?;
        let alice = Entity::new()?;

        a.transaction()
            .assert(salary(&alice, 100))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        b.pull().from(&a).perform(&operator).await?;
        assert_eq!(stored(&b, &operator, &alice).await?, vec![100]);

        a.transaction()
            .assert(salary(&alice, 200))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        b.transaction()
            .assert(salary(&alice, 300))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(stored(&a, &operator, &alice).await?, vec![200]);
        assert_eq!(stored(&b, &operator, &alice).await?, vec![300]);

        let mut quiesced = false;
        for _ in 0..4 {
            let pulled_a = a.pull().from(&b).perform(&operator).await?;
            let pulled_b = b.pull().from(&a).perform(&operator).await?;
            if pulled_a.is_none() && pulled_b.is_none() {
                quiesced = true;
                break;
            }
        }
        assert!(quiesced, "mutual pulls reach a fixed point");
        assert_eq!(
            stored(&a, &operator, &alice).await?,
            vec![200, 300],
            "both successions stand; the claim both observed is gone"
        );
        assert_eq!(read_max(&a, &operator, &alice).await?, vec![300]);
        assert_eq!(read_max(&b, &operator, &alice).await?, vec![300]);
        Ok(())
    }

    /// A candidate a rule derives is not a claim: a write under `max`
    /// succeeds the greatest stored claim and leaves the derived
    /// candidate, which the read still elects while it is greater.
    #[dialog_common::test]
    async fn it_succeeds_stored_claims_only_beside_a_derived_candidate() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;

        // `org/salary(x) := bonus :- org/bonus(x) = bonus`.
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "salary": { "the": "org/salary", "as": "UnsignedInteger" }
            }},
            "when": [{
                "assert": { "with": {
                    "bonus": { "the": "org/bonus", "as": "UnsignedInteger" }
                }},
                "where": {
                    "this": { "?": { "name": "this" } },
                    "bonus": { "?": { "name": "salary" } }
                }
            }]
        }))?;
        let rule: DeductiveRule = descriptor.compile()?;
        branch
            .transaction()
            .assert(&rule)
            .assert(salary(&alice, 100))
            .assert(dialog_query::the!("org/bonus").of(alice.clone()).is(500u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(read_max(&branch, &operator, &alice).await?, vec![500]);

        branch
            .transaction()
            .assert(salary(&alice, 200))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![100, 200],
            "the read elected the derived candidate, so no stored claim is succeeded"
        );
        assert_eq!(
            read_max(&branch, &operator, &alice).await?,
            vec![500],
            "the derived candidate stands and still wins the read"
        );

        // Once the derived candidate is gone, the greatest stored
        // claim is what a read returns, and a write succeeds it.
        branch
            .transaction()
            .retract(dialog_query::the!("org/bonus").of(alice.clone()).is(500u32))
            .assert(salary(&alice, 150))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![100, 150],
            "the write succeeds the stored claim the read now elects"
        );
        Ok(())
    }

    /// `org/salary` read for `of` under `select`, every row.
    async fn read_under(
        branch: &Branch,
        operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
        of: &Entity,
        select: &str,
    ) -> Result<Vec<u64>> {
        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": select }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = branch
            .select(ConceptQuery { predicate, terms })
            .perform(operator)
            .try_vec()
            .await?;
        let mut read: Vec<u64> = rows
            .iter()
            .map(|row| row.get::<u64>("salary"))
            .collect::<Result<_, _>>()?;
        read.sort();
        Ok(read)
    }

    /// An `org/salary` fact for `of`, as a session asserts it into the
    /// overlay.
    fn session_salary(of: &Entity, value: u32) -> AttributeStatement {
        AttributeStatement {
            policy: None,
            ..salary(of, value)
        }
    }

    /// The session overlay is the newest facts: an overlay row wins a
    /// `last` read over the committed claim of its cell, and a write
    /// the read resolves to the overlay row retires nothing, since a
    /// commit never takes back what only the session put there. The
    /// written claim stands beside the committed one, and is what a
    /// `last` read elects once the session drops its row.
    #[dialog_common::test]
    async fn it_leaves_an_overlay_row_a_last_read_elects() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let last = |value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(&alice, value)
        };
        branch
            .transaction()
            .assert(last(300))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.overlay().assert(session_salary(&alice, 500))?;
        assert_eq!(
            read_under(&branch, &operator, &alice, "last").await?,
            vec![500],
            "the overlay row is the newest fact of the cell"
        );

        branch
            .transaction()
            .assert(last(400))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![300, 400],
            "the read elected the overlay row, so no stored claim is succeeded"
        );
        assert_eq!(
            read_under(&branch, &operator, &alice, "last").await?,
            vec![500],
            "the overlay row still stands above the written claim"
        );
        assert_eq!(
            read_under(&branch, &operator, &alice, "all").await?,
            vec![300, 400, 500]
        );

        branch.overlay().clear();
        assert_eq!(
            read_under(&branch, &operator, &alice, "last").await?,
            vec![400],
            "with the session row gone the written claim is the newest"
        );
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![300, 400]);
        Ok(())
    }

    /// Under `max` the overlay row ranks by value like any claim: a
    /// write succeeds the stored claim the read elects beneath a
    /// smaller overlay row, and retires nothing when the overlay row is
    /// the greatest.
    #[dialog_common::test]
    async fn it_succeeds_the_stored_claim_a_max_read_elects_beside_the_overlay() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(salary(&alice, 300))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        branch.overlay().assert(session_salary(&alice, 100))?;
        assert_eq!(read_max(&branch, &operator, &alice).await?, vec![300]);
        branch
            .transaction()
            .assert(salary(&alice, 400))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![400],
            "the stored claim the read elected is succeeded; the overlay row is not a claim"
        );
        assert_eq!(read_max(&branch, &operator, &alice).await?, vec![400]);

        branch.overlay().assert(session_salary(&alice, 500))?;
        assert_eq!(read_max(&branch, &operator, &alice).await?, vec![500]);
        branch
            .transaction()
            .assert(salary(&alice, 450))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![400, 450],
            "the read elected the overlay row, so no stored claim is succeeded"
        );
        assert_eq!(read_max(&branch, &operator, &alice).await?, vec![500]);
        Ok(())
    }

    /// A transaction reads the overlay above its own writes, as a read
    /// after its commit will: the overlay row stays the newest fact.
    /// A transaction's writes under a choosing policy read back through
    /// the transaction, by entity and over the whole relation, on a line
    /// holding nothing of them; a cell written twice reads as its later
    /// write alone, and a value written back after another reads once.
    #[dialog_common::test]
    async fn it_reads_its_own_choosing_writes_on_an_empty_line() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let last = |of: &Entity, value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(of, value)
        };
        let carol: Entity = "id:carol".parse()?;
        let transaction = branch
            .transaction()
            .assert(last(&alice, 10))
            .assert(last(&bob, 20))
            .assert(last(&bob, 21))
            .assert(last(&carol, 30))
            .assert(last(&carol, 40))
            .assert(last(&carol, 30));

        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": "last" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(alice.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(ConceptQuery {
                predicate: predicate.clone(),
                terms,
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let read: Vec<u64> = rows
            .iter()
            .map(|row| row.get::<u64>("salary"))
            .collect::<Result<_, _>>()?;
        assert_eq!(read, vec![10], "alice's write reads by entity");

        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("this"));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(ConceptQuery { predicate, terms })
            .perform(&operator)
            .try_vec()
            .await?;
        let mut read: Vec<(String, u64)> = rows
            .iter()
            .map(|row| Ok((row.entity().to_string(), row.get::<u64>("salary")?)))
            .collect::<Result<_>>()?;
        read.sort();
        assert_eq!(
            read,
            vec![
                ("id:alice".to_string(), 10),
                ("id:bob".to_string(), 21),
                ("id:carol".to_string(), 30)
            ],
            "every write reads over the relation: a cell by its later write, a value written back once"
        );
        Ok(())
    }

    /// A value the line holds, retracted and written back under a
    /// choosing policy in one transaction, reads through the transaction
    /// once, and the commit keeps it; one retracted and replaced reads as
    /// the replacement, and the commit lands that.
    #[dialog_common::test]
    async fn it_keeps_a_value_written_back_after_its_retraction() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let last = |of: &Entity, value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(of, value)
        };
        branch
            .transaction()
            .assert(last(&alice, 10))
            .assert(last(&bob, 20))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let transaction = branch
            .transaction()
            .retract(last(&alice, 10))
            .assert(last(&alice, 10))
            .retract(last(&bob, 20))
            .assert(last(&bob, 25));
        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": "all" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var("this"));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(ConceptQuery {
                predicate: predicate.clone(),
                terms: terms.clone(),
            })
            .perform(&operator)
            .try_vec()
            .await?;
        let mut read: Vec<(String, u64)> = rows
            .iter()
            .map(|row| Ok((row.entity().to_string(), row.get::<u64>("salary")?)))
            .collect::<Result<_>>()?;
        read.sort();
        assert_eq!(
            read,
            vec![("id:alice".to_string(), 10), ("id:bob".to_string(), 25)],
            "the transaction reads the value written back once, and the replacement"
        );

        transaction.commit().publish().perform(&operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![10]);
        assert_eq!(stored(&branch, &operator, &bob).await?, vec![25]);
        Ok(())
    }

    /// A commit that installs a rule lands every other cell it writes
    /// under a choosing policy: one written once, which the tree
    /// settles, and one written twice with one value, which the
    /// transactor settles to a single claim.
    #[dialog_common::test]
    async fn it_lands_choosing_writes_beside_a_rule_it_installs() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let last = |of: &Entity, value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(of, value)
        };
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "title": { "the": "org/title", "as": "Text" }
            }},
            "when": [{
                "assert": { "with": {
                    "level": { "the": "org/level", "as": "Text" }
                }},
                "where": {
                    "this": { "?": { "name": "this" } },
                    "level": { "?": { "name": "title" } }
                }
            }]
        }))?;
        let rule: DeductiveRule = descriptor.compile()?;
        branch
            .transaction()
            .assert(&rule)
            .assert(last(&alice, 10))
            .assert(last(&alice, 10))
            .assert(last(&bob, 20))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![10]);
        assert_eq!(stored(&branch, &operator, &bob).await?, vec![20]);

        // Written again with the same values, beside the rule again;
        // bob's cell written once more with another value, so its
        // writes are not one write repeated and settle here.
        branch
            .transaction()
            .assert(&rule)
            .assert(last(&alice, 10))
            .assert(last(&bob, 20))
            .assert(last(&bob, 20))
            .assert(last(&bob, 21))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![10]);
        assert_eq!(stored(&branch, &operator, &bob).await?, vec![21]);
        Ok(())
    }

    /// A read bounded on the value settles the cells it reaches by
    /// their entity and attribute: a succeeded claim is hidden from a
    /// read of its own value, though the write succeeding it wrote
    /// another.
    #[dialog_common::test]
    async fn it_hides_a_succeeded_claim_from_a_read_of_its_value() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        let last = |of: &Entity, value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(of, value)
        };
        branch
            .transaction()
            .assert(last(&alice, 10))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let transaction = branch.transaction().assert(last(&alice, 11));
        let read = |value: u32| {
            let predicate: ConceptDescriptor =
                serde_json::from_value(serde_json::json!({ "with": {
                    "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": "all" }
                }}))
                .expect("a descriptor");
            let mut terms = Parameters::new();
            terms.insert("this".to_string(), Term::<Any>::var("this"));
            terms.insert(
                "salary".to_string(),
                Term::<Any>::Constant(Value::UnsignedInt(value.into())),
            );
            ConceptQuery { predicate, terms }
        };
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(read(10))
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(rows.is_empty(), "the succeeded claim is hidden by value");
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(read(11))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(rows.len(), 1, "the write reads by value");
        Ok(())
    }

    /// A write under a choosing policy reads, through its transaction,
    /// as the commit will leave the cell: the line's claim it succeeds
    /// is gone from an `all` read, and a write of the value the line
    /// holds reads that claim once. A read of another cell settles
    /// nothing of this one.
    #[dialog_common::test]
    async fn it_reads_a_succession_as_the_commit_leaves_it() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        let bob: Entity = "id:bob".parse()?;
        let last = |of: &Entity, value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(of, value)
        };
        branch
            .transaction()
            .assert(last(&alice, 10))
            .assert(last(&bob, 20))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let transaction = branch
            .transaction()
            .assert(last(&alice, 11))
            .assert(last(&bob, 20));
        let read = |of: &Entity| {
            let predicate: ConceptDescriptor =
                serde_json::from_value(serde_json::json!({ "with": {
                    "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": "all" }
                }}))
                .expect("a descriptor");
            let mut terms = Parameters::new();
            terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
            terms.insert("salary".to_string(), Term::<Any>::var("salary"));
            ConceptQuery { predicate, terms }
        };
        let salaries = |rows: Vec<ConceptConclusion>| -> Result<Vec<u64>> {
            let mut read: Vec<u64> = rows
                .iter()
                .map(|row| row.get::<u64>("salary"))
                .collect::<Result<_, _>>()?;
            read.sort();
            Ok(read)
        };
        let rows = transaction
            .query()
            .select(read(&alice))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(
            salaries(rows)?,
            vec![11],
            "the succeeded claim is gone from an all read"
        );
        let rows = transaction
            .query()
            .select(read(&bob))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(salaries(rows)?, vec![20], "the held value reads once");

        transaction.commit().publish().perform(&operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![11]);
        assert_eq!(stored(&branch, &operator, &bob).await?, vec![20]);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_reads_the_overlay_above_its_own_writes() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let last = |value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(&alice, value)
        };
        branch
            .transaction()
            .assert(last(300))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.overlay().assert(session_salary(&alice, 500))?;

        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(alice.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let transaction = branch.transaction().assert(last(400));
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(ConceptQuery { predicate, terms })
            .perform(&operator)
            .try_vec()
            .await?;
        let read: Vec<u64> = rows
            .iter()
            .map(|row| row.get::<u64>("salary"))
            .collect::<Result<_, _>>()?;
        assert_eq!(read, vec![500], "the overlay row outranks the staged write");

        transaction.commit().publish().perform(&operator).await?;
        assert_eq!(
            read_under(&branch, &operator, &alice, "last").await?,
            vec![500]
        );
        Ok(())
    }

    /// Writing the value the overlay holds still lands it: the overlay
    /// row is not a claim of the line, so the cell does not hold the
    /// value yet, and the write stands once the session row is gone.
    #[dialog_common::test]
    async fn it_lands_a_value_only_the_overlay_holds() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let last = |value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(&alice, value)
        };
        branch
            .transaction()
            .assert(last(300))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.overlay().assert(session_salary(&alice, 400))?;
        branch
            .transaction()
            .assert(last(400))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![300, 400]);
        branch.overlay().clear();
        assert_eq!(
            read_under(&branch, &operator, &alice, "last").await?,
            vec![400]
        );
        Ok(())
    }

    /// A rule installed in the transaction that writes counts: the
    /// derived candidate it brings wins the read, so the write succeeds
    /// nothing, where a settlement over the line alone would have
    /// retired the greatest stored claim.
    #[dialog_common::test]
    async fn it_sees_a_rule_the_same_transaction_installs() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(salary(&alice, 100))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "salary": { "the": "org/salary", "as": "UnsignedInteger" }
            }},
            "when": [{
                "assert": { "with": {
                    "bonus": { "the": "org/bonus", "as": "UnsignedInteger" }
                }},
                "where": {
                    "this": { "?": { "name": "this" } },
                    "bonus": { "?": { "name": "salary" } }
                }
            }]
        }))?;
        let rule: DeductiveRule = descriptor.compile()?;
        branch
            .transaction()
            .assert(&rule)
            .assert(dialog_query::the!("org/bonus").of(alice.clone()).is(500u32))
            .assert(salary(&alice, 150))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![100, 150],
            "the derived candidate the transaction brings wins, so nothing is retired"
        );
        assert_eq!(read_max(&branch, &operator, &alice).await?, vec![500]);
        Ok(())
    }

    /// A transaction reads its own `last` write as the commit will: the
    /// staged row stands at the edition the commit mints, so it is
    /// elected over the committed claim, and a second write in the same
    /// transaction succeeds the first, which was never committed and
    /// leaves no trace.
    #[dialog_common::test]
    async fn it_reads_its_own_last_write_before_the_commit() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let last = |value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(&alice, value)
        };
        branch
            .transaction()
            .assert(last(100))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(alice.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let query = ConceptQuery { predicate, terms };

        let transaction = branch.transaction().assert(last(200)).assert(last(300));
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(query.clone())
            .perform(&operator)
            .try_vec()
            .await?;
        let read: Vec<u64> = rows
            .iter()
            .map(|row| row.get::<u64>("salary"))
            .collect::<Result<_, _>>()?;
        assert_eq!(read, vec![300], "the transaction reads its latest write");

        transaction.commit().publish().perform(&operator).await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![300],
            "the committed claim is succeeded and the superseded staged write is gone"
        );
        Ok(())
    }

    /// A write observes the writes before it in its transaction and
    /// none after: `all` written after `last` stands beside it, and
    /// `max` written after `all` succeeds the staged claim it elects,
    /// leaving the committed one the read never returned.
    #[dialog_common::test]
    async fn it_settles_a_cell_in_the_order_the_transaction_wrote_it() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        let last = |value: u32| AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(&alice, value)
        };
        let all = |value: u32| AttributeStatement {
            policy: None,
            cardinality: Some(Cardinality::Many),
            ..salary(&alice, value)
        };

        branch
            .transaction()
            .assert(last(50))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // `last(200)` observes {50} and succeeds it; `all(300)` comes
        // after and appends.
        branch
            .transaction()
            .assert(last(200))
            .assert(all(300))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![200, 300]);

        // `max(150)` observes {200, 300, 400}: 400 was written before it
        // in this transaction and is the greatest, so it is what the
        // write succeeds; 200 and 300 stay.
        branch
            .transaction()
            .assert(all(400))
            .assert(salary(&alice, 150))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![150, 200, 300],
            "the staged claim the policy elected is gone, the others stand"
        );
        Ok(())
    }

    /// A transaction reads what its commit will leave: a `max` write
    /// below the committed claim succeeds it, and a read over the
    /// transaction already returns the write alone.
    #[dialog_common::test]
    async fn it_reads_a_write_as_the_commit_will_settle_it() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;
        branch
            .transaction()
            .assert(salary(&alice, 200))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let transaction = branch.transaction().assert(salary(&alice, 150));
        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": "all" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(alice.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = transaction
            .query()
            .select(ConceptQuery { predicate, terms })
            .perform(&operator)
            .try_vec()
            .await?;
        let mut claims: Vec<u64> = rows
            .iter()
            .map(|row| row.get::<u64>("salary"))
            .collect::<Result<_, _>>()?;
        claims.sort();
        assert_eq!(
            claims,
            vec![150],
            "the claim the write succeeds is gone from the transaction's own view"
        );

        transaction.commit().publish().perform(&operator).await?;
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![150]);
        Ok(())
    }

    /// A write under `last` succeeds the claim a `last` read returns:
    /// of two live claims, the newer. The older stays, as under any
    /// other policy; a `last` read returns the write afterwards, since
    /// it is newer than both.
    #[dialog_common::test]
    async fn it_succeeds_the_newest_claim_under_last() -> Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let alice = Entity::new()?;

        // Two claims written without a policy, one revision apart, so
        // the second is the newer.
        for value in [100u32, 200] {
            branch
                .transaction()
                .assert(dialog_query::the!("org/salary").of(alice.clone()).is(value))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        assert_eq!(stored(&branch, &operator, &alice).await?, vec![100, 200]);

        let last = AttributeStatement {
            policy: Some(Policy::Last),
            ..salary(&alice, 150)
        };
        branch
            .transaction()
            .assert(last)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            stored(&branch, &operator, &alice).await?,
            vec![100, 150],
            "the newer claim is succeeded, the older stays"
        );

        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "UnsignedInteger" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::constant(alice.clone()));
        terms.insert("salary".to_string(), Term::<Any>::var("salary"));
        let rows: Vec<ConceptConclusion> = branch
            .select(ConceptQuery { predicate, terms })
            .perform(&operator)
            .try_vec()
            .await?;
        let read: Vec<u64> = rows
            .iter()
            .map(|row| row.get::<u64>("salary"))
            .collect::<Result<_, _>>()?;
        assert_eq!(read, vec![150], "a last read returns the write");
        Ok(())
    }
}
