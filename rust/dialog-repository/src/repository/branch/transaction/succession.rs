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

use crate::repository::branch::session::{Erased, QueryEnv};
use crate::repository::source::Source;
use crate::repository::staged::squash;
use crate::rules::{conclusion_attr, derives_attr};
use crate::{CommitError, Staged};
use dialog_artifacts::history::Edition;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Attribute, Cause, Change, Changes, Entity, Policy, Select, Value,
};
use dialog_capability::Provider;
use dialog_query::attribute::{AttributeDescriptor, Relation, The};
use dialog_query::concept::query::Election;
use dialog_query::query::Output as _;
use dialog_query::source::SelectRules;
use dialog_query::types::Any;
use dialog_query::{
    Binding, Cardinality, Claim, ConceptDescriptor, ConceptFieldDescriptor, ConceptQuery, Match,
    Parameters, Standing, Term,
};
use futures_util::TryStreamExt;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

/// What a settlement is for: the batch a commit applies, or the view a
/// transaction's own reads see.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settlement {
    /// The tree elects among a cell's stored claims when it applies a
    /// succession, in the same descent that writes the value, so a
    /// commit settles here only the cells some rule derives: those
    /// have candidates the tree cannot see.
    Commit,
    /// A reader sees what the commit will leave, so every succession is
    /// settled to the retraction and assertion it comes to.
    Read,
}

/// Settle a transaction's writes against `sources`, read with `overlay`.
/// For a [`Settlement::Read`], every succession is replaced by the
/// retraction of the claim it succeeds, if any, and the assertion of
/// its value. For a [`Settlement::Commit`], the same happens only when
/// some rule derives a relation a succession writes; otherwise the
/// successions stay as written and the tree settles them. Each write is
/// settled over the line and the writes before it, in the order the
/// transaction made them.
pub(crate) async fn settle(
    sources: Vec<Source>,
    overlay: Arc<Changes>,
    staged: &Staged,
    settlement: Settlement,
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
    if settlement == Settlement::Commit && !staged.holds_rules() {
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
    // derived candidates through.
    let mut prefix = Staged::default();
    // Whether some rule derives a relation, as of the rules the prefix
    // held when it was asked: a write under a relation no rule derives
    // has no candidates beyond the cell's claims, and most writes are
    // such, so the answer is asked once per relation and again only
    // after the prefix gains a rule.
    let mut derives: HashMap<Attribute, (usize, bool)> = HashMap::new();
    let mut rules_in_prefix = 0usize;
    let rule_attributes = [conclusion_attr(), derives_attr()];

    for (position, (the, of, change)) in staged.log().iter().enumerate() {
        let key = (the.clone(), of.clone());
        if !cells.contains_key(&key) {
            let observed = if change.elects()
                || staged.log()[position..]
                    .iter()
                    .any(|(t, o, c)| t == the && o == of && c.elects())
            {
                claims_of(&line, the, of).await?
            } else {
                Vec::new()
            };
            cells.insert(key.clone(), Cell::over(observed));
            order.push(key.clone());
        }
        let cell = cells.get_mut(&key).expect("cell loaded above");
        let derived = match change {
            Change::Assert(_, policy) if policy.elects() => {
                let known = derives
                    .get(the)
                    .filter(|(asked_at, _)| *asked_at == rules_in_prefix)
                    .map(|(_, derived)| *derived);
                let view = QueryEnv::new(sources.clone(), overlay.clone(), env)
                    .with_layers(vec![prefix.clone()]);
                let derived = match known {
                    Some(derived) => derived,
                    None => {
                        let derived = rules_derive(&view, the).await?;
                        derives.insert(the.clone(), (rules_in_prefix, derived));
                        derived
                    }
                };
                let candidates = if derived {
                    derived_candidates(&view, the, of).await?
                } else {
                    Vec::new()
                };
                drop(view);
                candidates
            }
            _ => Vec::new(),
        };
        if rule_attributes.contains(the) {
            rules_in_prefix += 1;
        }
        for written in cell.write(change, &derived, edition, the, of)? {
            prefix.apply_change(the, of, &written);
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

/// Whether some rule `view` knows derives the relation `the` names.
async fn rules_derive(view: &QueryEnv<'_>, the: &Attribute) -> Result<bool, CommitError> {
    let rules = Provider::<SelectRules>::execute(view, relation_predicate(the))
        .await
        .map_err(|error| CommitError::Policy(error.to_string()))?;
    Ok(!rules.installed().is_empty())
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
