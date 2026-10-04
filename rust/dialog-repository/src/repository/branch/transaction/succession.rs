//! Succession of a claim under a choosing policy, settled at commit and
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
//! the relation, so the statement records a [`Succession`] and the
//! settlement runs here, over the transaction's ordered log: each write
//! is replayed in order over the claims the line holds for its cell;
//! a succession elects among the live claims and the candidates rules
//! derive at that point, retracts the stored claim it elects, and adds
//! its value. A derived candidate is not a claim: when the read elects
//! a derived value nothing is retracted, and the write stands beside it
//! as one more candidate. Writing a value the cell already holds writes
//! nothing. The same settlement runs for a transaction's own reads, so
//! a read over the transaction sees what the commit will leave.

use crate::repository::branch::session::QueryEnv;
use crate::repository::source::Source;
use crate::{CommitError, RemoteSite, Staged};
use dialog_artifacts::history::Edition;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Attribute, Cause, Change, Changes, Entity, Select, Value,
    pending_version,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::blob::Read as BlobRead;
use dialog_effects::memory::Resolve;
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

/// Settle a transaction's writes against `sources`, read with `overlay`:
/// the batch a commit applies, every succession replaced by the
/// retraction of the claim it succeeds, if any, and the assertion of
/// its value. Each write is settled over the line and the writes before
/// it, in the order the transaction made them.
pub(crate) async fn settle<Env>(
    sources: Vec<Source>,
    overlay: Arc<Changes>,
    staged: &Staged,
    env: &Env,
) -> Result<Changes, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let mut settled = staged.assets().clone();
    if !staged.has_successions() {
        for (the, of, change) in staged.log() {
            settled.put_cell(the.clone(), of.clone(), vec![change.clone()]);
        }
        return Ok(settled);
    }

    // What every write observed of the line: its claims, read once per
    // cell a succession writes.
    let line = QueryEnv::new(sources.clone(), overlay.clone(), env);
    let edition = line.pending_edition();
    let mut cells: HashMap<(Attribute, Entity), Cell> = HashMap::new();
    // The writes settled so far, as the view a later write reads the
    // derived candidates through.
    let mut prefix = Staged::default();

    for (sequence, (the, of, change)) in staged.log().iter().enumerate() {
        let key = (the.clone(), of.clone());
        if !cells.contains_key(&key) {
            let observed = if matches!(change, Change::Succeed(..))
                || staged.log()[sequence..]
                    .iter()
                    .any(|(t, o, c)| t == the && o == of && matches!(c, Change::Succeed(..)))
            {
                claims_of(&line, the, of).await?
            } else {
                Vec::new()
            };
            cells.insert(key.clone(), Cell { live: observed });
        }
        let cell = cells.get_mut(&key).expect("cell loaded above");
        let derived = match change {
            Change::Succeed(..) => {
                let view = QueryEnv::new(sources.clone(), overlay.clone(), env)
                    .with_layers(vec![prefix.clone()]);
                let candidates = derived_candidates(&view, the, of).await?;
                drop(view);
                candidates
            }
            _ => Vec::new(),
        };
        for written in cell.write(change, sequence, &derived, edition, the, of)? {
            prefix.apply_change(the, of, &written);
            settled.put_cell(the.clone(), of.clone(), vec![written]);
        }
    }
    Ok(settled)
}

/// Settle every succession `head` holds against `view`, the round view
/// an inductive rule fired on, which is what its head observed: the
/// claims the view holds, the writes before it in the round included,
/// and the candidates rules derive through it.
pub(crate) async fn resolve_against<Env>(
    view: &QueryEnv<'_, Env>,
    head: &mut Changes,
) -> Result<(), CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let cells = head.cells_with_successions();
    if cells.is_empty() {
        return Ok(());
    }
    let edition = view.pending_edition();
    for (the, of) in cells {
        let list = head.take_cell(&the, &of);
        let mut cell = Cell {
            live: claims_of(view, &the, &of).await?,
        };
        let derived = derived_candidates(view, &the, &of).await?;
        let mut settled = Vec::with_capacity(list.len());
        for (sequence, change) in list.iter().enumerate() {
            settled.extend(cell.write(change, sequence, &derived, edition, &the, &of)?);
        }
        head.put_cell(the, of, settled);
    }
    Ok(())
}

/// A claim or candidate a succession may elect: its value and its
/// standing, and whether it is a stored claim a write can succeed.
struct Candidate {
    value: Value,
    standing: Option<Standing>,
    claim: bool,
}

/// The claims a cell holds as `view` reads them.
async fn claims_of<Env>(
    view: &QueryEnv<'_, Env>,
    the: &Attribute,
    of: &Entity,
) -> Result<Vec<Candidate>, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let failed = |error: &dyn Display| CommitError::Succession(error.to_string());
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
            claim: true,
        });
    }
    Ok(claims)
}

/// Every candidate a read of the relation sees for the entity through
/// `view`, when some rule derives the relation; nothing otherwise, as
/// the claims are then all there is. A derived one is what the read
/// offers beyond the claims.
async fn derived_candidates<Env>(
    view: &QueryEnv<'_, Env>,
    the: &Attribute,
    of: &Entity,
) -> Result<Vec<Candidate>, CommitError>
where
    Env: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let failed = |error: &dyn Display| CommitError::Succession(error.to_string());
    let attribute = AttributeDescriptor::over(
        Relation::Attribute(The::from(the.clone())),
        "",
        Cardinality::Many,
        None,
    );
    let predicate = ConceptDescriptor::of_attribute(&ConceptFieldDescriptor::required(attribute));
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

/// One cell's live claims as the writes replayed so far leave them.
struct Cell {
    live: Vec<Candidate>,
}

impl Cell {
    /// Replay one write: a plain assertion adds a claim, standing at
    /// the commit's edition and its place among the writes; a
    /// retraction removes one; a replace keeps its value alone; a
    /// succession elects among the live claims and the derived
    /// candidates that are not claims, retracts the elected claim when
    /// it is one and adds its value, or adds nothing when the cell holds
    /// the value already. Returns what the write settles to.
    fn write(
        &mut self,
        change: &Change,
        sequence: usize,
        derived: &[Candidate],
        edition: Edition,
        the: &Attribute,
        of: &Entity,
    ) -> Result<Vec<Change>, CommitError> {
        let staged = |value: &Value| Candidate {
            standing: Some(Standing {
                version: Some((edition, pending_version(sequence as u64))),
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
            Change::Assert(value) => {
                self.live.retain(|claim| claim.value != *value);
                self.live.push(staged(value));
                vec![change.clone()]
            }
            Change::Replace(value) => {
                self.live.clear();
                self.live.push(staged(value));
                vec![change.clone()]
            }
            Change::Retract(value) => {
                self.live.retain(|claim| claim.value != *value);
                vec![change.clone()]
            }
            Change::Succeed(value, succession) => {
                if self.live.iter().any(|claim| claim.value == *value) {
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
                let elected = Election::from(succession)
                    .elect_claims(pool)
                    .map_err(|error| CommitError::Succession(error.to_string()))?;
                let mut settled = Vec::with_capacity(2);
                if let Some((elected, true)) = elected {
                    self.live.retain(|claim| claim.value != elected);
                    settled.push(Change::Retract(elected));
                }
                self.live.push(staged(value));
                settled.push(Change::Assert(value.clone()));
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
    use dialog_artifacts::{ArtifactSelector, Attribute, Entity, Succession, Value};
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

    /// A write of `org/salary` under `max`: the statement a field
    /// reading the relation under that policy writes.
    fn salary(of: &Entity, value: u32) -> AttributeStatement {
        AttributeStatement {
            the: The::from("org/salary".parse::<Attribute>().expect("an attribute")),
            of: of.clone(),
            is: Value::UnsignedInt(value.into()),
            cause: None,
            cardinality: Some(Cardinality::One),
            succession: Some(Succession::Max),
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
            succession: Some(Succession::Last),
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
            succession: Some(Succession::Last),
            ..salary(&alice, value)
        };
        let all = |value: u32| AttributeStatement {
            succession: None,
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
            succession: Some(Succession::Last),
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
