//! A reference election, checked against the engine's.
//!
//! The policies are specified here on their own, over a cell's claims
//! and their standings, apart from the code that implements them: `last`
//! takes the newest claim, the greater standing and then the greater
//! value; `max` and `min` the greater or the lesser value, then the
//! newer; and a write under a choosing policy succeeds the claim the
//! policy elects, leaving the cell alone when that claim holds the
//! value written. A write under `all` succeeds nothing.
//!
//! Generated histories of writes and pulls across three replicas then
//! check every place the engine elects against the reference: the tree,
//! which settles a commit's writes; the transaction's own settlement,
//! which its reads see; and the reads of a branch, under every policy.
//! Finally the replicas pull from each other until nothing changes, and
//! must agree.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::succession::stored_claims;
use crate::Branch;
use crate::helpers::test_repo;
use crate::repository::branch::session::{QueryEnv, QueryLayer};
use crate::repository::source::Source;
use anyhow::{Result, anyhow};
use dialog_artifacts::{Attribute, Entity, Policy, Value};
use dialog_capability::Provider;
use dialog_effects::authority::Identify;
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::attribute::The;
use dialog_query::query::Output as _;
use dialog_query::types::Any;
use dialog_query::{
    AttributeStatement, Cardinality, ConceptConclusion, ConceptDescriptor, ConceptQuery,
    Parameters, Standing, Term,
};
use dialog_storage::provider::storage::VolatileSpace;
use std::collections::BTreeSet;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

/// The policies a history writes and reads under.
const POLICIES: [Policy; 4] = [Policy::Last, Policy::Max, Policy::Min, Policy::All];

/// Where a claim stands: as the line holds it, or staged by the
/// transaction, newer than every claim of the line and equal to every
/// other staged claim, as the claims of one commit are.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stand {
    Line(Standing),
    Staged,
}

/// One claim of a cell, as the reference sees it.
type Entry = (u64, Stand);

/// Whether `candidate` is the newer of two claims: the greater standing,
/// then the greater value.
fn newer(candidate: &Entry, incumbent: &Entry) -> bool {
    candidate.1 > incumbent.1 || (candidate.1 == incumbent.1 && candidate.0 > incumbent.0)
}

/// The claim `policy` elects among `entries`, by index. `None` over no
/// claims and under `all`.
fn elect(policy: &Policy, entries: &[Entry]) -> Option<usize> {
    let better = |candidate: &Entry, incumbent: &Entry| match policy {
        Policy::Last => newer(candidate, incumbent),
        Policy::Max => {
            candidate.0 > incumbent.0 || (candidate.0 == incumbent.0 && newer(candidate, incumbent))
        }
        Policy::Min => {
            candidate.0 < incumbent.0 || (candidate.0 == incumbent.0 && newer(candidate, incumbent))
        }
        _ => false,
    };
    if *policy == Policy::All {
        return None;
    }
    let mut best: Option<usize> = None;
    for (index, entry) in entries.iter().enumerate() {
        match best {
            Some(incumbent) if !better(entry, &entries[incumbent]) => {}
            _ => best = Some(index),
        }
    }
    best
}

/// A write of `value` under `policy` to a cell holding `entries`: under
/// a choosing policy, the claim the policy elects is succeeded unless it
/// holds the value written; the value written stands as staged.
fn write(policy: &Policy, value: u64, entries: &mut Vec<Entry>) {
    if *policy != Policy::All {
        if let Some(elected) = elect(policy, entries) {
            if entries[elected].0 == value {
                return;
            }
            entries.remove(elected);
        }
    }
    entries.retain(|entry| entry.0 != value);
    entries.push((value, Stand::Staged));
}

/// What a read under `policy` returns for a cell holding `entries`: the
/// elected value, or every value under `all`, sorted.
fn read(policy: &Policy, entries: &[Entry]) -> Vec<u64> {
    match policy {
        Policy::All => {
            let mut values: Vec<u64> = entries.iter().map(|entry| entry.0).collect();
            values.sort();
            values
        }
        _ => elect(policy, entries)
            .map(|index| vec![entries[index].0])
            .unwrap_or_default(),
    }
}

/// The spelling a concept field selects `policy` under.
fn select_of(policy: &Policy) -> &'static str {
    match policy {
        Policy::Last => "last",
        Policy::Max => "max",
        Policy::Min => "min",
        _ => "all",
    }
}

fn salary_attribute() -> Attribute {
    "org/salary".parse().expect("an attribute")
}

/// A write of `org/salary` under `policy`.
fn salary(of: &Entity, value: u64, policy: &Policy) -> AttributeStatement {
    AttributeStatement {
        the: The::from(salary_attribute()),
        of: of.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
        cardinality: Some(if *policy == Policy::All {
            Cardinality::Many
        } else {
            Cardinality::One
        }),
        policy: Some(policy.clone()),
    }
}

/// `org/salary` of `of` read under `policy`.
fn salary_query(of: &Entity, policy: &Policy) -> ConceptQuery {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": select_of(policy) }
    }}))
    .expect("a descriptor");
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
    terms.insert("salary".to_string(), Term::<Any>::var("salary"));
    ConceptQuery { predicate, terms }
}

fn salaries(rows: Vec<ConceptConclusion>) -> Result<Vec<u64>> {
    let mut read: Vec<u64> = rows
        .iter()
        .map(|row| row.get::<u64>("salary"))
        .collect::<Result<_, _>>()?;
    read.sort();
    Ok(read)
}

/// The stored claims of `of`'s salary on `branch`, as the reference
/// sees them.
async fn line(branch: &Branch, operator: &Operator, of: &Entity) -> Result<Vec<Entry>> {
    let principal = Identify.perform(operator).await?;
    let overlay = QueryLayer::from(branch).overlay(&principal);
    let view = QueryEnv::new(vec![Source::from(branch.clone())], overlay, operator);
    let claims = stored_claims(&view, &salary_attribute(), of).await?;
    claims
        .into_iter()
        .map(|(value, standing)| match value {
            Value::UnsignedInt(value) => Ok((value as u64, Stand::Line(standing))),
            other => Err(anyhow!("a salary is an unsigned integer, not {other:?}")),
        })
        .collect()
}

fn values(entries: &[Entry]) -> BTreeSet<u64> {
    entries.iter().map(|entry| entry.0).collect()
}

/// A small deterministic generator, so a failing history replays.
struct Seeded(u64);

impl Seeded {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

/// Every read of every cell on `branch`, under every policy, against
/// the reference over the branch's stored claims.
async fn check_reads(
    branch: &Branch,
    name: &str,
    operator: &Operator,
    people: &[Entity],
    history: &[String],
) -> Result<()> {
    for person in people {
        let entries = line(branch, operator, person).await?;
        for policy in &POLICIES {
            let read_back = salaries(
                branch
                    .select(salary_query(person, policy))
                    .perform(operator)
                    .try_vec()
                    .await?,
            )?;
            let expected = read(policy, &entries);
            if read_back != expected {
                return Err(anyhow!(
                    "{name} reads {person} under {policy:?} as {read_back:?}, the reference \
                     over its claims {entries:?} as {expected:?}\nhistory:\n{}",
                    history.join("\n")
                ));
            }
        }
    }
    Ok(())
}

/// One generated history from `seed`, checked step by step.
async fn replay(seed: u64, steps: usize) -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let names = ["a", "b", "c"];
    let mut branches = Vec::new();
    for name in names {
        branches.push(repo.branch(name).open().perform(&operator).await?);
    }
    let people: Vec<Entity> = (0..3)
        .map(|index| format!("id:person-{index}").parse())
        .collect::<Result<_, _>>()?;
    let mut random = Seeded(seed);
    let mut history: Vec<String> = vec![format!("seed {seed}")];

    for _ in 0..steps {
        let at = random.below(branches.len());
        if random.below(10) < 7 {
            // A transaction of one to three writes, each checked against
            // the reference as the transaction reads it and as the
            // commit lands it.
            let branch = &branches[at];
            let count = 1 + random.below(3);
            let mut writes = Vec::new();
            for _ in 0..count {
                let person = random.below(people.len());
                let value = 1 + random.below(6) as u64;
                let policy = POLICIES[random.below(POLICIES.len())].clone();
                writes.push((person, value, policy));
            }
            history.push(format!(
                "{}: {}",
                names[at],
                writes
                    .iter()
                    .map(|(person, value, policy)| format!("{policy:?} {value} to {person}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));

            let mut cells: Vec<Option<Vec<Entry>>> = vec![None; people.len()];
            let mut transaction = branch.transaction();
            for (person, value, policy) in &writes {
                if cells[*person].is_none() {
                    cells[*person] = Some(line(branch, &operator, &people[*person]).await?);
                }
                write(policy, *value, cells[*person].as_mut().expect("loaded"));
                transaction = transaction.assert(salary(&people[*person], *value, policy));
            }
            for (person, entries) in cells.iter().enumerate() {
                let Some(entries) = entries else { continue };
                for policy in &POLICIES {
                    let read_back = salaries(
                        transaction
                            .query()
                            .select(salary_query(&people[person], policy))
                            .perform(&operator)
                            .try_vec()
                            .await?,
                    )?;
                    let expected = read(policy, entries);
                    if read_back != expected {
                        let mut every = Vec::new();
                        for policy in &POLICIES {
                            every.push((
                                policy.clone(),
                                salaries(
                                    transaction
                                        .query()
                                        .select(salary_query(&people[person], policy))
                                        .perform(&operator)
                                        .try_vec()
                                        .await?,
                                )?,
                            ));
                        }
                        history.push(format!("every read: {every:?}"));
                        return Err(anyhow!(
                            "the transaction reads person {person} under {policy:?} as \
                             {read_back:?}, the reference as {expected:?} over {entries:?} \
                             (head edition {:?})\nhistory:\n{}",
                            branch.revision().map(|revision| revision.edition),
                            history.join("\n")
                        ));
                    }
                }
            }
            transaction.commit().publish().perform(&operator).await?;
            for (person, entries) in cells.iter().enumerate() {
                let Some(entries) = entries else { continue };
                let landed = values(&line(branch, &operator, &people[person]).await?);
                if landed != values(entries) {
                    return Err(anyhow!(
                        "the commit leaves person {person} holding {landed:?}, the reference \
                         {:?}\nhistory:\n{}",
                        values(entries),
                        history.join("\n")
                    ));
                }
            }
        } else {
            let from = (at + 1 + random.below(branches.len() - 1)) % branches.len();
            history.push(format!("{} pulls from {}", names[at], names[from]));
            branches[at]
                .pull()
                .from(&branches[from])
                .perform(&operator)
                .await?;
        }
        check_reads(&branches[at], names[at], &operator, &people, &history).await?;
    }

    // The replicas pull from each other until nothing changes, and then
    // hold the same claims and read the same under every policy.
    let mut quiesced = false;
    for _ in 0..8 {
        let mut changed = false;
        for at in 0..branches.len() {
            for from in 0..branches.len() {
                if at != from {
                    changed |= branches[at]
                        .pull()
                        .from(&branches[from])
                        .perform(&operator)
                        .await?
                        .is_some();
                }
            }
        }
        if !changed {
            quiesced = true;
            break;
        }
    }
    if !quiesced {
        return Err(anyhow!(
            "the replicas never stop pulling\nhistory:\n{}",
            history.join("\n")
        ));
    }
    for person in &people {
        let held = values(&line(&branches[0], &operator, person).await?);
        for (at, branch) in branches.iter().enumerate().skip(1) {
            let other = values(&line(branch, &operator, person).await?);
            if other != held {
                return Err(anyhow!(
                    "after every pull, {} holds {other:?} for {person} and a holds {held:?}\n\
                     history:\n{}",
                    names[at],
                    history.join("\n")
                ));
            }
        }
    }
    for (at, branch) in branches.iter().enumerate() {
        check_reads(branch, names[at], &operator, &people, &history).await?;
    }
    Ok(())
}

/// Generated histories of writes under every policy and pulls between
/// three replicas, each step checked against the reference election.
#[dialog_common::test]
async fn every_election_agrees_with_the_reference() -> Result<()> {
    for seed in [0x9E37_79B9_7F4A_7C15, 0xD1B5_4A32_D192_ED03, 0x2545_F491_4F6C_DD1D] {
        replay(seed, 40).await?;
    }
    Ok(())
}

#[dialog_common::test]
async fn scratch_pulled_history() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let a = repo.branch("a").open().perform(&operator).await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    let c = repo.branch("c").open().perform(&operator).await?;
    let p: Entity = "id:p".parse()?;
    fn writes<'b>(
        branch: &'b Branch,
        p: &Entity,
        writes: Vec<(u64, Policy)>,
    ) -> super::Transaction<&'b Branch> {
        let mut transaction = branch.transaction();
        for (value, policy) in &writes {
            transaction = transaction.assert(salary(p, *value, policy));
        }
        transaction
    }
    let commit = |branch, list| writes(branch, &p, list);
    let show = |label: &'static str, entries: Vec<Entry>| {
        eprintln!(
            "SCRATCH {label}: {:?}",
            entries
                .iter()
                .map(|(value, stand)| match stand {
                    Stand::Line(standing) => (*value, standing.version.map(|v| v.0)),
                    Stand::Staged => (*value, None),
                })
                .collect::<Vec<_>>()
        );
    };
    commit(&a, vec![(3, Policy::Max)]).commit().publish().perform(&operator).await?;
    commit(&b, vec![(5, Policy::All)]).commit().publish().perform(&operator).await?;
    b.pull().from(&a).perform(&operator).await?;
    commit(&b, vec![(4, Policy::Max), (5, Policy::All)]).commit().publish().perform(&operator).await?;
    commit(&a, vec![(5, Policy::Min), (2, Policy::Min)]).commit().publish().perform(&operator).await?;
    show("a after min 5, min 2", line(&a, &operator, &p).await?);
    c.pull().from(&b).perform(&operator).await?;
    show("c after pulling b", line(&c, &operator, &p).await?);
    commit(&b, vec![(6, Policy::Max)]).commit().publish().perform(&operator).await?;
    c.pull().from(&a).perform(&operator).await?;
    show("c after pulling a", line(&c, &operator, &p).await?);
    let transaction = commit(&c, vec![(2, Policy::All)]);
    for policy in &POLICIES {
        let rows = salaries(
            transaction
                .query()
                .select(salary_query(&p, policy))
                .perform(&operator)
                .try_vec()
                .await?,
        )?;
        eprintln!("SCRATCH c tx {policy:?}: {rows:?}");
    }
    eprintln!("SCRATCH c head {:?}", c.revision().map(|r| r.edition));
    transaction.commit().publish().perform(&operator).await?;
    show("c after committing all 2", line(&c, &operator, &p).await?);
    let rows = salaries(c.select(salary_query(&p, &Policy::Last)).perform(&operator).try_vec().await?)?;
    eprintln!("SCRATCH c after commit last: {rows:?}");
    // The same on a fresh branch, no pulls.
    let d = repo.branch("d").open().perform(&operator).await?;
    commit(&d, vec![(2, Policy::All)]).commit().publish().perform(&operator).await?;
    commit(&d, vec![(5, Policy::All)]).commit().publish().perform(&operator).await?;
    let transaction = commit(&d, vec![(2, Policy::All)]);
    let rows = salaries(transaction.query().select(salary_query(&p, &Policy::Last)).perform(&operator).try_vec().await?)?;
    eprintln!("SCRATCH d tx last: {rows:?}");
    transaction.commit().publish().perform(&operator).await?;
    show("d after committing all 2", line(&d, &operator, &p).await?);
    Ok(())
}

#[dialog_common::test]
async fn scratch_all_write_beside_two_claims() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("a").open().perform(&operator).await?;
    let other = repo.branch("b").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    let bob: Entity = "id:bob".parse()?;
    branch
        .transaction()
        .assert(salary(&alice, 4, &Policy::All))
        .assert(salary(&alice, 5, &Policy::All))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    for (label, writes) in [
        ("alone", vec![(0usize, 2u64, Policy::All)]),
        ("beside a max write to another cell", vec![(1, 6, Policy::Max), (0, 2, Policy::All)]),
        (
            "beside another cell's succession of the same value",
            vec![(1, 2, Policy::All), (1, 6, Policy::Max), (0, 2, Policy::All)],
        ),
    ] {
        let mut transaction = branch.transaction();
        for (who, value, policy) in &writes {
            let of = if *who == 0 { &alice } else { &bob };
            transaction = transaction.assert(salary(of, *value, policy));
        }
        let rows = salaries(
            transaction
                .query()
                .select(salary_query(&alice, &Policy::Last))
                .perform(&operator)
                .try_vec()
                .await?,
        )?;
        eprintln!("SCRATCH {label}: last {rows:?}");
    }
    let _ = other;
    Ok(())
}

#[dialog_common::test]
async fn scratch_written_back_after_succession() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("a").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(salary(&alice, 3, &Policy::All))
        .assert(salary(&alice, 5, &Policy::All))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let transaction = branch
        .transaction()
        .assert(salary(&alice, 4, &Policy::Max))
        .assert(salary(&alice, 5, &Policy::All));
    for policy in &POLICIES {
        let rows = salaries(
            transaction
                .query()
                .select(salary_query(&alice, policy))
                .perform(&operator)
                .try_vec()
                .await?,
        )?;
        eprintln!("SCRATCH tx {policy:?}: {rows:?}");
    }
    transaction.commit().publish().perform(&operator).await?;
    for policy in &POLICIES {
        let rows = salaries(
            branch
                .select(salary_query(&alice, policy))
                .perform(&operator)
                .try_vec()
                .await?,
        )?;
        eprintln!("SCRATCH branch {policy:?}: {rows:?}");
    }
    eprintln!("SCRATCH line {:?}", line(&branch, &operator, &alice).await?.iter().map(|e| e.0).collect::<Vec<_>>());
    Ok(())
}
