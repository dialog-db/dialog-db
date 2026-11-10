//! Every unit of isolation reads one revision: the one its line stood at
//! when it began.
//!
//! A branch's head lives in a memory cell every clone of the branch
//! shares, so a commit through one handle moves what another handle's
//! reads see. One query performs many selects on its environment (a
//! conjunction looks its second premise up per row of the first, a
//! resolver re-reads a reference per outer row), a transaction performs
//! many queries, and a commit's settlement and induction read the line
//! many times over. A read path that takes a line's root afresh for each
//! read mixes two revisions and yields rows no revision held
//! (dialog-db/dialog-db#757).
//!
//! The harness here puts a commit *under* a read: an [`Interposing`] env
//! commits through another handle of the branch before answering the
//! k-th tree node read (or the k-th spilled-value load), and the path
//! under test must read what it would have read with no commit at all.
//! Sweeping k over the first few dozen reads of each path puts the
//! commit at every point of it. Adding a path is one [`isolated!`] line.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::{Context as _, Result};
use dialog_artifacts::{ArtifactSelector, Entity, Pick, Relation as ArtifactsRelation, Value};
use dialog_capability::Provider;
use dialog_common::ConditionalSend;
use dialog_effects::authority::Identify;
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::attribute::Relation;
use dialog_query::query::Output as _;
use dialog_query::{Attribute, AttributeStatement, Cardinality, Concept, InductiveRule, Query};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::{StreamExt as _, TryStreamExt as _};
use std::future::Future;
use std::pin::Pin;
use std::slice::from_ref;

use super::session::{QueryEnv, shared_metadata};
use super::transaction::{settle, stored_claims};
use crate::helpers::{Interposed, Interposing, Under, test_repo};
use crate::repository::source::{Live, View};
use crate::{Branch, CommitError, PublishError, Staged};

/// The session every read here runs as.
type Operator = Peer<VolatileSpace, Session>;
/// The env the paths run through: the session, with a commit armed
/// under one of its reads.
type Env = Interposing<Operator>;
/// What a path read, rendered so two runs compare.
type Rows = Vec<String>;
/// A read path: what it reads of `branch` through `env`.
type Path = for<'a> fn(&'a Env, &'a Branch) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>>;

/// A person's display name (`person/name`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("person")]
pub struct Name(pub String);

/// A person's age (`person/age`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("person")]
pub struct Age(pub u64);

/// A person's biography (`person/bio`): longer than a row holds inline,
/// so it is read from the blob store when the row is.
#[derive(Attribute, Clone, PartialEq)]
#[domain("person")]
pub struct Bio(pub String);

/// Someone with a name, an age and a biography.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Person {
    /// The person.
    pub this: Entity,
    /// Their name.
    pub name: Name,
    /// Their age.
    pub age: Age,
    /// Their biography.
    pub bio: Bio,
}

/// An age an inductive rule copied (`summary/copied`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("summary")]
pub struct Copied(pub u64);

/// A person whose age a rule summarized.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Summary {
    /// The person.
    pub this: Entity,
    /// Their age, as the rule read it.
    pub copied: Copied,
}

/// How many people the fixture holds.
const PEOPLE: u64 = 5;
/// Longer than the inline threshold (4096 bytes), so a biography spills.
const BIO_LENGTH: usize = 6000;
/// How many reads of each kind a sweep puts the commit under.
const SWEEP: u64 = 40;

fn person(index: u64) -> Entity {
    format!("person:{index}").parse().expect("an entity")
}

fn command(index: u64) -> Entity {
    format!("cmd:{index}").parse().expect("an entity")
}

fn bio(fill: char, index: u64) -> String {
    let mut bio = std::iter::repeat_n(fill, BIO_LENGTH).collect::<String>();
    bio.push_str(&index.to_string());
    bio
}

/// The rule the induction path fires: `summary/copied` of a person is
/// their `person/age` when a `cmd.summarize/person` command names them.
/// The age is read through the commit's view, per command.
fn summarize() -> InductiveRule {
    serde_json::from_value(serde_json::json!({
        "description": "Copy a person's age when asked to summarize them",
        "assert!": {
            "with": {
                "copied": { "the": "summary/copied", "as": "natural:" }
            }
        },
        "when": [
            {
                "assert": {
                    "with": {
                        "person": { "the": "cmd.summarize/person", "as": "entity:" }
                    }
                },
                "where": {
                    "person": { "?": { "name": "this" } }
                }
            },
            {
                "assert": {
                    "with": {
                        "age": { "the": "person/age", "as": "natural:" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "age": { "?": { "name": "copied" } }
                }
            }
        ]
    }))
    .expect("the summarize rule compiles")
}

/// A fresh repository whose `main` holds the fixture at its first
/// revision: the people, and the summarize rule.
async fn fixture(operator: &Operator, profile: &Peer<VolatileSpace>) -> Result<Branch> {
    let repo = test_repo(operator, profile).await;
    let branch = repo.branch("main").open().perform(operator).await?;
    let mut transaction = branch.transaction().assert(summarize());
    for index in 0..PEOPLE {
        transaction = transaction
            .assert(Name::of(person(index)).is(format!("p{index}")))
            .assert(Age::of(person(index)).is(Age(10 + index)))
            .assert(Bio::of(person(index)).is(bio('b', index)));
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(branch)
}

/// Move `branch`'s head through this handle: every person's name, age
/// and biography change, so a read that follows the head reads none of
/// what the first revision held.
async fn move_head(operator: &Operator, branch: &Branch) -> Result<()> {
    let mut transaction = branch.transaction();
    for index in 0..PEOPLE {
        transaction = transaction
            .retract(Name::of(person(index)).is(format!("p{index}")))
            .assert(Name::of(person(index)).is(format!("q{index}")))
            .retract(Age::of(person(index)).is(Age(10 + index)))
            .assert(Age::of(person(index)).is(Age(100 + index)))
            .retract(Bio::of(person(index)).is(bio('b', index)))
            .assert(Bio::of(person(index)).is(bio('c', index)));
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// The action an [`Interposing`] env runs under a read: [`move_head`]
/// through `other`, a handle of the branch under test.
fn moving(other: Branch) -> impl for<'a> FnOnce(&'a Operator) -> Interposed<'a> + ConditionalSend {
    move |operator| {
        Box::pin(async move {
            move_head(operator, &other)
                .await
                .expect("the commit under the read lands");
        })
    }
}

fn people(rows: Vec<Person>) -> Rows {
    let mut rows: Rows = rows
        .into_iter()
        .map(|row| {
            format!(
                "{} {} {} {}",
                row.this,
                row.name.0,
                row.age.0,
                row.bio.0.chars().last().expect("a biography")
            )
        })
        .collect();
    rows.sort();
    rows
}

fn summaries(rows: Vec<Summary>) -> Rows {
    let mut rows: Rows = rows
        .into_iter()
        .map(|row| format!("{} {}", row.this, row.copied.0))
        .collect();
    rows.sort();
    rows
}

fn age() -> ArtifactsRelation {
    "person/age".parse().expect("a relation")
}

/// `branch.query()`: one query, many selects.
fn query<'a>(env: &'a Env, branch: &'a Branch) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        Ok(people(
            branch
                .query()
                .select(Query::<Person>::default())
                .perform(env)
                .try_vec()
                .await?,
        ))
    })
}

/// `branch.select()`: the shortcut for a query over the branch alone.
fn select<'a>(
    env: &'a Env,
    branch: &'a Branch,
) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        Ok(people(
            branch
                .select(Query::<Person>::default())
                .perform(env)
                .try_vec()
                .await?,
        ))
    })
}

/// `branch.claims().select()`: one scan of the artifact index.
fn claims<'a>(
    env: &'a Env,
    branch: &'a Branch,
) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        let stream = branch
            .claims()
            .select(ArtifactSelector::new().the(age()))
            .perform(env)
            .await?;
        let mut rows: Rows = stream
            .map(|item| item.and_then(|view| view.to_owned()))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|artifact| format!("{} {:?}", artifact.of, artifact.is))
            .collect();
        rows.sort();
        Ok(rows)
    })
}

/// Two queries of one transaction: both read the revision the
/// transaction opened at.
fn transaction<'a>(
    env: &'a Env,
    branch: &'a Branch,
) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        let transaction = branch.transaction();
        let mut first = people(
            transaction
                .query()
                .select(Query::<Person>::default())
                .perform(env)
                .try_vec()
                .await?,
        );
        let second = people(
            transaction
                .query()
                .select(Query::<Person>::default())
                .perform(env)
                .try_vec()
                .await?,
        );
        first.extend(second);
        Ok(first)
    })
}

/// Commit-time induction: a staged commit dispatching one summarize
/// command per person, whose rule reads each person's age through the
/// commit's view. What the staged tip holds is what induction read.
fn induction<'a>(
    env: &'a Env,
    branch: &'a Branch,
) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        let mut transaction = branch.transaction();
        for index in 0..PEOPLE {
            transaction = transaction.dispatch(
                dialog_query::the!("cmd.summarize/person")
                    .of(command(index))
                    .is(person(index)),
            );
        }
        let batch = transaction.commit().perform(env).await?;
        Ok(summaries(
            batch
                .query()
                .select(Query::<Summary>::default())
                .perform(env)
                .try_vec()
                .await?,
        ))
    })
}

/// A write of `person/age` under `last`, which succeeds the claim a
/// `last` read elects.
fn succeed_age(of: &Entity, value: u64) -> AttributeStatement {
    AttributeStatement {
        the: Relation::from(age()),
        of: of.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
        cardinality: Some(Cardinality::One),
        pick: Some(Pick::Last),
    }
}

/// The settlement of a transaction's choosing writes: one `last` write
/// per person, settled over the line's claims (the session overlay
/// holding one of the cells is what makes the settlement read them).
/// What each write succeeds is what the settlement read.
fn succession<'a>(
    env: &'a Env,
    branch: &'a Branch,
) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        branch.overlay().assert(Age::of(person(0)).is(Age(1000)))?;
        let mut staged = Staged::default();
        for index in 0..PEOPLE {
            staged.assert(succeed_age(&person(index), 200 + index));
        }
        let view = Live::from(branch).pin();
        let operator = Identify.perform(env).await?;
        let overlay = shared_metadata(from_ref(&view), &operator);
        let settled = settle(vec![view.into_source()], overlay, &staged, env).await?;
        let mut rows: Rows = settled
            .iter()
            .map(|(of, the, change)| format!("{of} {the} {change:?}"))
            .collect();
        rows.sort();
        Ok(rows)
    })
}

/// The oracle's reference read: every person's stored `person/age`
/// claims, as the reference election reads them through one
/// environment.
fn oracle<'a>(
    env: &'a Env,
    branch: &'a Branch,
) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        let view = Live::from(branch).pin();
        let operator = Identify.perform(env).await?;
        let overlay = shared_metadata(from_ref(&view), &operator);
        let line = QueryEnv::new(vec![view.into_source()], overlay, env);
        let mut rows = Rows::new();
        for index in 0..PEOPLE {
            // A claim's standing names a version of this repository's
            // own origin, which differs from fixture to fixture: the
            // value is what the sweep compares.
            for (value, _standing) in stored_claims(&line, &age(), &person(index)).await? {
                rows.push(format!("{} {value:?}", person(index)));
            }
        }
        rows.sort();
        Ok(rows)
    })
}

/// One poll of a subscription: its first evaluation.
fn subscription<'a>(
    env: &'a Env,
    branch: &'a Branch,
) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        let mut standing = branch.subscribe(Query::<Person>::default());
        let delta = standing.poll(env).await?.expect("the first poll evaluates");
        Ok(people(delta.asserted))
    })
}

/// A query over a snapshot of the branch: pinned by construction, the
/// control.
fn snapshot<'a>(
    env: &'a Env,
    branch: &'a Branch,
) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        let snapshot = branch.snapshot().expect("the fixture committed");
        Ok(people(
            snapshot
                .query()
                .select(Query::<Person>::default())
                .perform(env)
                .try_vec()
                .await?,
        ))
    })
}

/// The branch's log: a history read. Rendered by edition and parent
/// count, since every fixture's versions name its own origin.
fn log<'a>(env: &'a Env, branch: &'a Branch) -> Pin<Box<dyn Future<Output = Result<Rows>> + 'a>> {
    Box::pin(async move {
        Ok(branch
            .log(env, 8)
            .await?
            .into_iter()
            .map(|(version, record)| format!("{:?} {}", version.edition, record.parents.len()))
            .collect())
    })
}

/// Run `path` with the head moved under each of its first [`SWEEP`]
/// reads of each kind, and require what it read to be what it reads
/// with no commit under it.
async fn sweep(name: &str, path: Path) -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let env = Interposing::new(operator);

    let branch = fixture(env.inner(), &profile).await?;
    let expected = path(&env, &branch).await?;
    assert!(!expected.is_empty(), "{name}: the path reads something");

    // The commit changes what the path reads, so a path that followed
    // the head would show it.
    let branch = fixture(env.inner(), &profile).await?;
    move_head(env.inner(), &branch).await?;
    let moved = path(&env, &branch).await?;
    assert_ne!(
        moved, expected,
        "{name}: the commit must change what the path reads, or the sweep tests nothing"
    );

    for under in [Under::NodeRead, Under::BlobLoad] {
        let mut fired_at = Vec::new();
        let mut performed = 0;
        for k in 1..=SWEEP {
            let branch = fixture(env.inner(), &profile).await?;
            env.under(under, k, moving(branch.clone()));
            let rows = path(&env, &branch)
                .await
                .with_context(|| format!("{name} with the head moved under {under:?} {k}"))?;
            if env.fired() {
                fired_at.push(k);
            }
            performed = performed.max(match under {
                Under::NodeRead => env.reads(),
                Under::BlobLoad => env.loads(),
            });
            env.disarm();
            assert_eq!(
                rows, expected,
                "{name}: the head moved under {under:?} {k}, and the path read another revision"
            );
        }
        // A path that performs reads of this kind had the commit under
        // its first one; a path that performs none (a log served from
        // the verified-record memo, a path without spilled values) was
        // swept for nothing, which is no failure of the path.
        if performed > 0 {
            assert_eq!(
                fired_at.first(),
                Some(&1),
                "{name}: the first {under:?} puts the commit under the path"
            );
        }
    }
    Ok(())
}

/// One test per read path, each a sweep of the harness.
macro_rules! isolated {
    ($($name:ident),* $(,)?) => {
        $(
            #[dialog_common::test]
            async fn $name() -> Result<()> {
                sweep(stringify!($name), super::$name).await
            }
        )*
    };
}

mod reads_one_revision_under_a_commit {
    use super::*;

    isolated!(
        query,
        select,
        claims,
        transaction,
        induction,
        succession,
        oracle,
        subscription,
        snapshot,
        log,
    );
}

/// A transaction reads the revision it opened at throughout, and its
/// commit is refused when the head is no longer that revision: nothing
/// of it lands, and the branch keeps the head the other commit gave it.
#[dialog_common::test]
async fn a_transaction_reads_and_commits_against_the_revision_it_opened_at() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let branch = fixture(&operator, &profile).await?;
    let first = branch.revision().expect("the fixture committed");
    let expected = people(
        branch
            .select(Query::<Person>::default())
            .perform(&operator)
            .try_vec()
            .await?,
    );

    let transaction = branch.transaction();

    // Another handle moves the head while the transaction is open.
    move_head(&operator, &branch.clone()).await?;
    let second = branch.revision().expect("the head moved");
    assert_ne!(first, second);

    for _ in 0..2 {
        let read = people(
            transaction
                .query()
                .select(Query::<Person>::default())
                .perform(&operator)
                .try_vec()
                .await?,
        );
        assert_eq!(
            read, expected,
            "the transaction reads the revision it opened at"
        );
    }

    let newcomer = person(PEOPLE);
    let refused = transaction
        .assert(Name::of(newcomer.clone()).is("late".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await;
    assert!(
        matches!(
            refused,
            Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
        ),
        "a transaction whose head moved is refused, not rebased: {refused:?}"
    );
    assert_eq!(
        branch.revision(),
        Some(second),
        "the branch keeps the head the other commit gave it"
    );
    let after = branch
        .select(Query::<Person>::default())
        .perform(&operator)
        .try_vec()
        .await?;
    assert!(
        after.iter().all(|row| row.this != newcomer),
        "nothing of the refused transaction landed"
    );
    assert_eq!(after.len(), PEOPLE as usize);
    Ok(())
}

/// Staging is refused the same way: a chain built on a head that moved
/// could never publish, so it is not minted.
#[dialog_common::test]
async fn staging_a_transaction_whose_head_moved_is_refused() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let branch = fixture(&operator, &profile).await?;
    let transaction = branch.transaction();
    move_head(&operator, &branch.clone()).await?;
    let moved = branch.revision();

    let refused = transaction
        .assert(Name::of(person(PEOPLE)).is("late".to_string()))
        .commit()
        .perform(&operator)
        .await
        .map(|_| ());
    assert!(
        matches!(
            refused,
            Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
        ),
        "staging on a moved head is refused: {refused:?}"
    );
    assert_eq!(branch.revision(), moved);

    // A transaction that changes nothing is refused too: its no-op
    // verdict was judged against a head that is gone.
    let empty = branch.transaction();
    branch
        .clone()
        .transaction()
        .assert(Name::of(person(PEOPLE + 1)).is("newer".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_ne!(branch.revision(), moved, "the second commit moved the head");
    let refused = empty.commit().publish().perform(&operator).await;
    assert!(
        matches!(
            refused,
            Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
        ),
        "an empty transaction on a moved head is refused: {refused:?}"
    );
    Ok(())
}

/// A snapshot transaction is pinned the same way: a commit through the
/// same handle while it is open moves the snapshot, its queries keep
/// reading the revision it opened at, and its commit is refused.
#[dialog_common::test]
async fn a_snapshot_transaction_reads_and_commits_against_the_revision_it_opened_at() -> Result<()>
{
    let (operator, profile) = test_session_with_peer().await;
    let branch = fixture(&operator, &profile).await?;
    let snapshot = branch.snapshot().expect("the fixture committed");
    let expected = people(
        snapshot
            .select(Query::<Person>::default())
            .perform(&operator)
            .try_vec()
            .await?,
    );

    let transaction = snapshot.transaction();
    let advanced = snapshot
        .transaction()
        .retract(Age::of(person(0)).is(Age(10)))
        .assert(Age::of(person(0)).is(Age(50)))
        .commit()
        .perform(&operator)
        .await?;
    assert_eq!(snapshot.revision(), advanced);

    let read = people(
        transaction
            .query()
            .select(Query::<Person>::default())
            .perform(&operator)
            .try_vec()
            .await?,
    );
    assert_eq!(
        read, expected,
        "the transaction reads the revision it opened at"
    );

    let refused = transaction
        .assert(Name::of(person(PEOPLE)).is("late".to_string()))
        .commit()
        .perform(&operator)
        .await;
    assert!(
        matches!(
            refused,
            Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
        ),
        "a snapshot transaction whose snapshot moved is refused: {refused:?}"
    );
    assert_eq!(
        snapshot.revision(),
        advanced,
        "the snapshot keeps the commit that moved it"
    );
    Ok(())
}

/// What a read settlement observes of a transaction's environment is
/// the revision the transaction pinned, whatever the head is by then.
#[dialog_common::test]
async fn a_read_observation_records_the_pinned_heads() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let branch = fixture(&operator, &profile).await?;
    let pinned = branch.revision().expect("the fixture committed");
    let view = View::at(Live::from(&branch), Some(&pinned));
    let identity = Identify.perform(&operator).await?;
    let overlay = shared_metadata(from_ref(&view), &identity);
    let line = QueryEnv::new(vec![view.into_source()], overlay, &operator);

    move_head(&operator, &branch.clone()).await?;
    assert_ne!(branch.revision().as_ref(), Some(&pinned));

    let observed = line.observation();
    assert_eq!(observed.heads, vec![Some(pinned.clone())]);
    assert_eq!(
        line.pending_edition(),
        pinned.edition.successor(),
        "a write of the transaction stands at the pinned head's successor"
    );
    Ok(())
}

/// The two selects one query performs on its environment read the same
/// revision, whatever is committed to the branch between them: the one
/// the branch stood at when the query began.
#[dialog_common::test]
async fn a_query_reads_the_revision_it_started_at() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let report: Entity = "id:report".parse()?;
    branch
        .transaction()
        .assert(Name::of(report.clone()).is("Robin".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    // One query's environment, built as `SelectQuery::perform` builds it.
    let layer = branch.query();
    let identity = Identify.perform(&operator).await?;
    let views = layer.pin();
    let overlay = layer.overlay_at(&views, &identity);
    let sources = views.into_iter().map(View::into_source).collect();
    let query = QueryEnv::new(sources, overlay, &operator);

    let name: ArtifactsRelation = "person/name".parse()?;
    let selector = ArtifactSelector::new().the(name).of(report.clone());
    let first = Provider::<dialog_artifacts::Select<'_>>::execute(&query, selector.clone())
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(first.len(), 1, "the query's first select sees the name");

    // Another handle of the same branch moves its head while the query
    // is under way.
    branch
        .transaction()
        .retract(Name::of(report.clone()).is("Robin".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let second = Provider::<dialog_artifacts::Select<'_>>::execute(&query, selector)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        second.len(),
        first.len(),
        "the query's second select read a revision its first did not"
    );
    Ok(())
}

/// Someone with a name: what the subscription below stands over.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Named {
    /// The entity.
    pub this: Entity,
    /// Their name.
    pub name: Name,
}

/// Pinning is per evaluation, not per subscription: a standing query
/// reads one revision within a poll and the branch's current revision
/// at the next, so a commit through another handle of the branch is
/// picked up by the poll after it.
#[dialog_common::test]
async fn a_subscription_follows_the_branch_across_polls() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let report: Entity = "id:report".parse()?;
    branch
        .transaction()
        .assert(Name::of(report.clone()).is("Robin".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let mut standing = branch.subscribe(Query::<Named>::default());
    let first = standing
        .poll(&operator)
        .await?
        .expect("the first poll evaluates");
    assert_eq!(
        first
            .asserted
            .iter()
            .map(|row| row.name.0.as_str())
            .collect::<Vec<_>>(),
        ["Robin"]
    );
    assert!(
        standing.poll(&operator).await?.is_none(),
        "nothing moved, so nothing is re-evaluated"
    );

    // Another handle of the branch moves its head.
    branch
        .clone()
        .transaction()
        .retract(Name::of(report.clone()).is("Robin".to_string()))
        .assert(Name::of(report).is("Sam".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let second = standing.poll(&operator).await?.expect("the head moved");
    assert_eq!(
        second
            .asserted
            .iter()
            .map(|row| row.name.0.as_str())
            .collect::<Vec<_>>(),
        ["Sam"]
    );
    assert_eq!(
        second
            .retracted
            .iter()
            .map(|row| row.name.0.as_str())
            .collect::<Vec<_>>(),
        ["Robin"]
    );
    assert_eq!(
        standing
            .results()
            .iter()
            .map(|row| row.name.0.as_str())
            .collect::<Vec<_>>(),
        ["Sam"],
        "the subscription stands at the branch's current revision"
    );
    Ok(())
}
