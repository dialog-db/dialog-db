//! Invariants the policy-write design claims, pinned as tests.
//!
//! Each test states a property `notes/attribute-heads.md` or the
//! changelog asserts of a write under a choosing policy, and checks it
//! the way a caller would. A failing test here is a place where a
//! transaction does not do what the design says.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::Branch;
use crate::helpers::test_repo;
use anyhow::Result;
use dialog_artifacts::{Attribute, Entity, Policy, Value};
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

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

/// A write of `org/salary` under `policy`.
fn salary(of: &Entity, value: u32, policy: Policy) -> AttributeStatement {
    AttributeStatement {
        the: The::from("org/salary".parse::<Attribute>().expect("an attribute")),
        of: of.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
        cardinality: Some(if policy == Policy::All {
            Cardinality::Many
        } else {
            Cardinality::One
        }),
        policy: Some(policy),
    }
}

/// `org/salary` of `of` read under `select`, sorted.
fn salary_query(of: &Entity, select: &str) -> ConceptQuery {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": select }
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

async fn read_branch(
    branch: &Branch,
    operator: &Operator,
    of: &Entity,
    select: &str,
) -> Result<Vec<u64>> {
    salaries(
        branch
            .select(salary_query(of, select))
            .perform(operator)
            .try_vec()
            .await?,
    )
}

/// `org/salary(x) := bonus :- org/bonus(x) = bonus`.
fn salary_from_bonus() -> Result<DeductiveRule> {
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
    Ok(descriptor.compile()?)
}

/// "A transaction reads what its commit will leave." A rule derives
/// `org/salary` from `org/bonus`; the line holds a stored salary. One
/// transaction writes the salary under `last` and then the bonus. The
/// commit settles the salary write against the writes *before* it, so
/// it sees no derived candidate, elects the stored claim and retracts
/// it. The transaction's own read settles the same cell against the
/// *whole* layer, sees the derived candidate, elects it and retracts
/// nothing. What the transaction reads and what the commit leaves must
/// agree; they differ on whether the stored claim survives.
#[dialog_common::test]
async fn a_transaction_reads_what_its_commit_leaves_when_a_derived_input_follows_the_write()
-> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(&salary_from_bonus()?)
        .assert(salary(&alice, 100, Policy::All))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let transaction = branch
        .transaction()
        .assert(salary(&alice, 200, Policy::Last))
        .assert(dialog_query::the!("org/bonus").of(alice.clone()).is(500u32));
    let read_all = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "all"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    let read_last = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "last"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    transaction.commit().publish().perform(&operator).await?;
    let branch = repo.branch("main").open().perform(&operator).await?;

    assert_eq!(
        read_all,
        read_branch(&branch, &operator, &alice, "all").await?,
        "every candidate the transaction read under `all` is what the commit left"
    );
    assert_eq!(
        read_last,
        read_branch(&branch, &operator, &alice, "last").await?,
        "the candidate the transaction read under `last` is what the commit left"
    );
    Ok(())
}

/// A write is "a claim that succeeds what the attribute currently
/// stands for", and a `last` read after it returns the newest write.
/// The cell holds two live claims, 100 then 200, so `last` reads 200.
/// Writing 100 under `last` is the newest write of the cell; a reader
/// who just wrote it expects to read it back. The changelog admits the
/// write "writes nothing, even when a read elects another claim", so
/// the read keeps returning 200: the write path is not read-your-
/// writes, and no `last` write can ever restore a value the cell
/// still holds as a loser.
#[dialog_common::test]
async fn a_last_write_of_a_value_the_cell_holds_reads_back_as_the_newest() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    for value in [100u32, 200] {
        branch
            .transaction()
            .assert(salary(&alice, value, Policy::All))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    assert_eq!(
        read_branch(&branch, &operator, &alice, "last").await?,
        vec![200]
    );

    branch
        .transaction()
        .assert(salary(&alice, 100, Policy::Last))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "last").await?,
        vec![100],
        "the newest write of the cell is what `last` returns"
    );
    Ok(())
}

/// The same read-your-writes property inside the transaction that made
/// the write: a `last` read over the transaction returns the value it
/// just wrote.
#[dialog_common::test]
async fn a_transaction_reads_back_its_own_last_write_of_a_held_value() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    for value in [100u32, 200] {
        branch
            .transaction()
            .assert(salary(&alice, value, Policy::All))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    let branch = repo.branch("main").open().perform(&operator).await?;
    let transaction = branch
        .transaction()
        .assert(salary(&alice, 100, Policy::Last));
    let read = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "last"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    assert_eq!(
        read,
        vec![100],
        "a transaction reads the value it just wrote"
    );
    Ok(())
}

/// "A cell's writes are replayed in that order at commit." The same
/// three writes, `last` 200, `all` 300, `last` 400, over a stored 100,
/// land the same claims whether a transaction records them one
/// statement at a time or integrates them as one `Changes` batch. The
/// batch drops the earlier `last` write and moves the later one past
/// the `all` write, so the stored 100 is never succeeded on that path.
#[dialog_common::test]
async fn an_integrated_batch_lands_as_the_same_writes_asserted_in_order() -> Result<()> {
    use dialog_artifacts::{Changes, Update as _};

    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let alice: Entity = "id:alice".parse()?;
    let the: Attribute = "org/salary".parse()?;
    let mut lines = Vec::new();
    for by_batch in [false, true] {
        let branch = repo
            .branch(if by_batch { "batch" } else { "statements" })
            .open()
            .perform(&operator)
            .await?;
        branch
            .transaction()
            .assert(salary(&alice, 100, Policy::All))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo
            .branch(if by_batch { "batch" } else { "statements" })
            .open()
            .perform(&operator)
            .await?;
        let transaction = if by_batch {
            let mut changes = Changes::new();
            changes.associate(
                the.clone(),
                alice.clone(),
                Value::UnsignedInt(200),
                Policy::Last,
            );
            changes.associate(
                the.clone(),
                alice.clone(),
                Value::UnsignedInt(300),
                Policy::All,
            );
            changes.associate(
                the.clone(),
                alice.clone(),
                Value::UnsignedInt(400),
                Policy::Last,
            );
            branch.transaction().integrate(changes)
        } else {
            branch
                .transaction()
                .assert(salary(&alice, 200, Policy::Last))
                .assert(salary(&alice, 300, Policy::All))
                .assert(salary(&alice, 400, Policy::Last))
        };
        transaction.commit().publish().perform(&operator).await?;
        let branch = repo
            .branch(if by_batch { "batch" } else { "statements" })
            .open()
            .perform(&operator)
            .await?;
        lines.push(read_branch(&branch, &operator, &alice, "all").await?);
    }
    assert!(
        !lines[0].contains(&100),
        "asserted in order, the first `last` write succeeds the stored claim: {:?}",
        lines[0]
    );
    assert_eq!(
        lines[1], lines[0],
        "the integrated batch lands the same claims as the statements"
    );
    Ok(())
}

/// A write of `the` under `policy`.
fn write(the: &str, of: &Entity, value: u32, policy: Policy) -> AttributeStatement {
    AttributeStatement {
        the: The::from(the.parse::<Attribute>().expect("an attribute")),
        of: of.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
        cardinality: Some(if policy == Policy::All {
            Cardinality::Many
        } else {
            Cardinality::One
        }),
        policy: Some(policy),
    }
}

/// "A transaction reads what its commit will leave." A rule derives
/// `org/salary` from `org/bonus`, and alice has no bonus, so the only
/// candidate of her cell is the stored 100. A `last` write of 200
/// succeeds it: the commit leaves 200 alone. The transaction's own
/// read settles the cell against the derived candidates read through
/// the whole transaction, where its own write of 200 comes back as a
/// candidate at the newest standing; the read elects that and retracts
/// nothing, so the transaction reads 100 beside 200.
#[dialog_common::test]
async fn a_transaction_reads_a_last_write_as_succeeding_the_stored_claim_under_a_rule() -> Result<()>
{
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(&salary_from_bonus()?)
        .assert(salary(&alice, 100, Policy::All))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let transaction = branch
        .transaction()
        .assert(salary(&alice, 200, Policy::Last));
    let read = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "all"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    transaction.commit().publish().perform(&operator).await?;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let committed = read_branch(&branch, &operator, &alice, "all").await?;
    assert_eq!(committed, vec![200], "the write succeeded the stored claim");
    assert_eq!(read, committed, "the transaction read what its commit left");
    Ok(())
}

/// "What a write observes is the line and the writes before it in its
/// own transaction, in order." Alice's bonus is 900 and her stored
/// salary 100, and a rule derives her salary from her bonus. One
/// transaction writes her bonus to 50 under `last`, then her salary to
/// 120 under `max`. A reader after the bonus write sees the salary
/// candidates `{100, 50}`, elects 100 under `max`, and the salary write
/// succeeds it: the cell comes to 120. The settlement reads the derived
/// candidates through the raw staged rows, where the bonus is still
/// 900 beside 50, elects the derived 900 and retracts nothing.
#[dialog_common::test]
async fn a_write_succeeds_what_a_reader_after_the_earlier_writes_observes() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(&salary_from_bonus()?)
        .assert(write("org/bonus", &alice, 900, Policy::Last))
        .assert(salary(&alice, 100, Policy::All))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(write("org/bonus", &alice, 50, Policy::Last))
        .assert(salary(&alice, 120, Policy::Max))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let branch = repo.branch("main").open().perform(&operator).await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "all").await?,
        vec![120],
        "the salary write succeeded the stored 100, the only candidate above the new bonus"
    );
    Ok(())
}

/// "Retracting an overlay row is the session's write, on the overlay
/// itself." A transaction that retracts the overlay's row and writes
/// 400 under `last` reads 400, since its retraction hides the row; its
/// commit writes a retraction of a fact the tree never held, which
/// lands nothing, and the overlay row is back above the committed 400.
/// A transaction reads what its commit leaves, or the retraction is
/// refused; it is neither.
#[dialog_common::test]
async fn a_transaction_that_retracts_an_overlay_row_reads_what_its_commit_leaves() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(salary(&alice, 300, Policy::Last))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let overlay_row = AttributeStatement {
        policy: None,
        ..salary(&alice, 500, Policy::All)
    };
    branch.overlay().assert(overlay_row.clone())?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "last").await?,
        vec![500]
    );

    let transaction =
        branch
            .transaction()
            .retract(overlay_row)
            .assert(salary(&alice, 400, Policy::Last));
    let read = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "last"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    transaction.commit().publish().perform(&operator).await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "last").await?,
        read,
        "what the transaction read under `last` is what its commit left"
    );
    Ok(())
}

/// Two writes under `all` and one under `last` in one transaction, over
/// an empty cell of a relation a rule derives (so the transactor, not
/// the tree, settles it). The two `all` writes stand at the same
/// edition, and the `last` write succeeds the one a `last` read
/// returns among them: "a tie falls to the value's bytes", the greater
/// value. The transactor breaks the tie by the hash of each fact
/// instead, so for some value pairs it retracts the claim a reader
/// would not have returned.
#[dialog_common::test]
async fn a_last_write_succeeds_the_claim_a_last_read_returns_among_equals() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let alice: Entity = "id:alice".parse()?;
    let mut wrong = Vec::new();
    for (index, (first, second)) in [
        (300u32, 400u32),
        (400, 300),
        (123, 321),
        (321, 123),
        (7, 8),
        (8, 7),
        (1000, 2000),
        (2000, 1000),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("pair-{index}");
        let branch = repo.branch(&name).open().perform(&operator).await?;
        branch
            .transaction()
            .assert(&salary_from_bonus()?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch(&name).open().perform(&operator).await?;
        branch
            .transaction()
            .assert(salary(&alice, first, Policy::All))
            .assert(salary(&alice, second, Policy::All))
            .assert(salary(&alice, 1, Policy::Last))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch(&name).open().perform(&operator).await?;
        let stored = read_branch(&branch, &operator, &alice, "all").await?;
        let loser = first.min(second) as u64;
        if stored != vec![1, loser] {
            wrong.push((first, second, stored));
        }
    }
    assert!(
        wrong.is_empty(),
        "the greater of two equal-standing claims is what `last` returns, and what the write succeeds; these pairs retracted the other: {wrong:?}"
    );
    Ok(())
}

/// "A write under `max` succeeds the claim a `max` read returns." The
/// cell holds 100 and 200, both live, so `max` reads 200. A `max` write
/// of 100 succeeds 200 and leaves 100 alone in the cell. The write
/// finds 100 already held and writes nothing, so 200 stays and `max`
/// keeps reading it: the held-value shortcut runs before the election,
/// whatever the policy.
#[dialog_common::test]
async fn a_max_write_of_a_held_value_succeeds_the_claim_a_max_read_returns() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(salary(&alice, 100, Policy::All))
        .assert(salary(&alice, 200, Policy::All))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "max").await?,
        vec![200]
    );
    branch
        .transaction()
        .assert(salary(&alice, 100, Policy::Max))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "all").await?,
        vec![100],
        "the write succeeded the claim the read elected"
    );
    Ok(())
}

/// "The session overlay is the newest facts. An overlay row stands
/// past the edition the next commit mints, above every committed
/// claim." Branch `b` has ten commits and holds a salary of 300; its
/// overlay holds 500. A query over `a` joined with `b` reads `last`
/// over both lines. The overlay's standing is taken from the first
/// line's head, `a`'s, which has one commit, so `b`'s overlay row
/// stands below `b`'s own committed claim and `last` returns 300.
#[dialog_common::test]
async fn an_overlay_row_stands_above_its_own_lines_commits_in_a_join() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let alice: Entity = "id:alice".parse()?;
    let a = repo.branch("a").open().perform(&operator).await?;
    a.transaction()
        .assert(salary(&alice, 1, Policy::All))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    for value in 290..300u32 {
        b.transaction()
            .assert(write("org/other", &alice, value, Policy::All))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    b.transaction()
        .assert(salary(&alice, 300, Policy::All))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let a = repo.branch("a").open().perform(&operator).await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    b.overlay().assert(AttributeStatement {
        policy: None,
        ..salary(&alice, 500, Policy::All)
    })?;
    assert_eq!(
        read_branch(&b, &operator, &alice, "last").await?,
        vec![500],
        "alone, b's overlay row is its newest fact"
    );
    let rows: Vec<ConceptConclusion> = a
        .query()
        .join(&b)
        .select(salary_query(&alice, "last"))
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(
        salaries(rows)?,
        vec![500],
        "joined, b's overlay row is still newer than every committed claim"
    );
    Ok(())
}
