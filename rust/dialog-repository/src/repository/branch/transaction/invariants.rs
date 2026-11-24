//! Invariants the cardinality-one write design claims, pinned as tests.
//!
//! Each test states a property `notes/attribute-heads.md` or the
//! changelog asserts of a write under cardinality one, and checks it
//! the way a caller would. A failing test here is a place where a
//! transaction does not do what the design says.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::Branch;
use crate::helpers::test_repo;
use anyhow::Result;
use dialog_corpus::{Entity, Relation as CorpusRelation, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::attribute::Relation;
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
fn salary(of: &Entity, value: u32, policy: Cardinality) -> AttributeStatement {
    AttributeStatement {
        the: Relation::from(
            "org/salary"
                .parse::<CorpusRelation>()
                .expect("an attribute"),
        ),
        of: of.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
        cardinality: Some(policy),
    }
}

/// `org/salary` of `of` read under the cardinality `select` names, sorted.
/// Of two salaries that stand equal, the one a `last` read returns: the
/// one whose value hashes greater (`Value::tiebreak`).
fn tie_winner(a: u32, b: u32) -> u32 {
    let hash = |salary: u32| Value::UnsignedInt(salary.into()).tiebreak();
    if hash(a) >= hash(b) { a } else { b }
}

fn salary_query(of: &Entity, select: &str) -> ConceptQuery {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "salary": { "the": "org/salary", "as": "natural:", "cardinality": select }
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
            "salary": { "the": "org/salary", "as": "natural:" }
        }},
        "when": [{
            "assert": { "with": {
                "bonus": { "the": "org/bonus", "as": "natural:" }
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
        .assert(
            dialog_query::StorableRule::try_from(&salary_from_bonus()?).expect("a storable rule"),
        )
        .assert(salary(&alice, 100, Cardinality::Many))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let transaction = branch
        .transaction()
        .assert(salary(&alice, 200, Cardinality::One))
        .assert(dialog_query::the!("org/bonus").of(alice.clone()).is(500u32));
    let read_all = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "many"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    let read_last = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "one"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    transaction.commit().publish().perform(&operator).await?;
    let branch = repo.branch("main").open().perform(&operator).await?;

    assert_eq!(
        read_all,
        read_branch(&branch, &operator, &alice, "many").await?,
        "every candidate the transaction read under `all` is what the commit left"
    );
    assert_eq!(
        read_last,
        read_branch(&branch, &operator, &alice, "one").await?,
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
            .assert(salary(&alice, value, Cardinality::Many))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    assert_eq!(
        read_branch(&branch, &operator, &alice, "one").await?,
        vec![200]
    );

    branch
        .transaction()
        .assert(salary(&alice, 100, Cardinality::One))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "one").await?,
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
            .assert(salary(&alice, value, Cardinality::Many))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    let branch = repo.branch("main").open().perform(&operator).await?;
    let transaction = branch
        .transaction()
        .assert(salary(&alice, 100, Cardinality::One));
    let read = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "one"))
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
    use dialog_corpus::{Changes, Update as _};

    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let alice: Entity = "id:alice".parse()?;
    let the: CorpusRelation = "org/salary".parse()?;
    let mut lines = Vec::new();
    for by_batch in [false, true] {
        let branch = repo
            .branch(if by_batch { "batch" } else { "statements" })
            .open()
            .perform(&operator)
            .await?;
        branch
            .transaction()
            .assert(salary(&alice, 100, Cardinality::Many))
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
                Cardinality::One,
            );
            changes.associate(
                the.clone(),
                alice.clone(),
                Value::UnsignedInt(300),
                Cardinality::Many,
            );
            changes.associate(
                the.clone(),
                alice.clone(),
                Value::UnsignedInt(400),
                Cardinality::One,
            );
            branch.transaction().integrate(changes)
        } else {
            branch
                .transaction()
                .assert(salary(&alice, 200, Cardinality::One))
                .assert(salary(&alice, 300, Cardinality::Many))
                .assert(salary(&alice, 400, Cardinality::One))
        };
        transaction.commit().publish().perform(&operator).await?;
        let branch = repo
            .branch(if by_batch { "batch" } else { "statements" })
            .open()
            .perform(&operator)
            .await?;
        lines.push(read_branch(&branch, &operator, &alice, "many").await?);
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
fn write(the: &str, of: &Entity, value: u32, policy: Cardinality) -> AttributeStatement {
    AttributeStatement {
        the: Relation::from(the.parse::<CorpusRelation>().expect("an attribute")),
        of: of.clone(),
        is: Value::UnsignedInt(value.into()),
        cause: None,
        cardinality: Some(policy),
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
        .assert(
            dialog_query::StorableRule::try_from(&salary_from_bonus()?).expect("a storable rule"),
        )
        .assert(salary(&alice, 100, Cardinality::Many))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let transaction = branch
        .transaction()
        .assert(salary(&alice, 200, Cardinality::One));
    let read = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "many"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    transaction.commit().publish().perform(&operator).await?;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let committed = read_branch(&branch, &operator, &alice, "many").await?;
    assert_eq!(committed, vec![200], "the write succeeded the stored claim");
    assert_eq!(read, committed, "the transaction read what its commit left");
    Ok(())
}

/// "What a write observes is the line and the writes before it in its
/// own transaction, in order." Alice's bonus is 900 and her stored
/// salary 100, and a rule derives her salary from her bonus, so the
/// derived 900, standing past the bonus claim it reads, is the newest
/// candidate of her salary. One transaction retracts her bonus, then
/// writes her salary to 120. A reader after the retraction sees the
/// stored 100 alone, which the salary write succeeds: the stored claims
/// come to 120. Settling against the line without the retraction would
/// elect the derived 900, which is no claim, and retract nothing.
#[dialog_common::test]
async fn a_write_succeeds_what_a_reader_after_the_earlier_writes_observes() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(
            dialog_query::StorableRule::try_from(&salary_from_bonus()?).expect("a storable rule"),
        )
        .assert(write("org/bonus", &alice, 900, Cardinality::One))
        .assert(salary(&alice, 100, Cardinality::Many))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .retract(write("org/bonus", &alice, 900, Cardinality::One))
        .assert(salary(&alice, 120, Cardinality::One))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let branch = repo.branch("main").open().perform(&operator).await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "many").await?,
        vec![120],
        "the salary write succeeded the stored 100, the only candidate once the bonus is gone"
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
        .assert(salary(&alice, 300, Cardinality::One))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let overlay_row = salary(&alice, 500, Cardinality::Many);
    branch.overlay().assert(overlay_row.clone())?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "one").await?,
        vec![500]
    );

    let transaction =
        branch
            .transaction()
            .retract(overlay_row)
            .assert(salary(&alice, 400, Cardinality::One));
    let read = salaries(
        transaction
            .query()
            .select(salary_query(&alice, "one"))
            .perform(&operator)
            .try_vec()
            .await?,
    )?;
    transaction.commit().publish().perform(&operator).await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "one").await?,
        read,
        "what the transaction read under `last` is what its commit left"
    );
    Ok(())
}

/// Two writes under `all` and one under `last` in one transaction, over
/// an empty cell of a relation a rule derives (so the transactor, not
/// the tree, settles it). The two `all` writes stand at the same
/// edition, and the `last` write succeeds the one a `last` read
/// returns among them: the one whose value hashes greater. A transactor
/// that broke the tie any other way (by the hash of each fact, or by the
/// value) retracted, for some value pairs, the claim a reader would not
/// have returned. The pairs include one whose hashes order it the other
/// way round from its values.
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
            .assert(
                dialog_query::StorableRule::try_from(&salary_from_bonus()?)
                    .expect("a storable rule"),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch(&name).open().perform(&operator).await?;
        branch
            .transaction()
            .assert(salary(&alice, first, Cardinality::Many))
            .assert(salary(&alice, second, Cardinality::Many))
            .assert(salary(&alice, 1, Cardinality::One))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let branch = repo.branch(&name).open().perform(&operator).await?;
        let stored = read_branch(&branch, &operator, &alice, "many").await?;
        let winner = tie_winner(first, second);
        let loser = if winner == first { second } else { first } as u64;
        if stored != vec![1, loser] {
            wrong.push((first, second, stored));
        }
    }
    assert!(
        wrong.is_empty(),
        "of two equal-standing claims, `last` returns the one whose value hashes greater, and the write succeeds it; these pairs retracted the other: {wrong:?}"
    );
    Ok(())
}

/// "A write under cardinality one succeeds the claim a read returns."
/// The cell holds 100 and, from a later commit, 200, both live, so a
/// read returns 200. A write of 100 succeeds 200 and leaves 100 alone in
/// the cell. The write finds 100 already held, but held is not what the
/// read returns: the held-value shortcut applies only when the claim the
/// read elects holds the value, so 200 is retracted all the same.
#[dialog_common::test]
async fn a_write_of_a_held_value_succeeds_the_claim_a_read_returns() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    for value in [100, 200] {
        branch
            .transaction()
            .assert(salary(&alice, value, Cardinality::Many))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    assert_eq!(
        read_branch(&branch, &operator, &alice, "one").await?,
        vec![200]
    );
    branch
        .transaction()
        .assert(salary(&alice, 100, Cardinality::One))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        read_branch(&branch, &operator, &alice, "many").await?,
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
        .assert(salary(&alice, 1, Cardinality::Many))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    for value in 290..300u32 {
        b.transaction()
            .assert(write("org/other", &alice, value, Cardinality::Many))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    b.transaction()
        .assert(salary(&alice, 300, Cardinality::Many))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let a = repo.branch("a").open().perform(&operator).await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    b.overlay().assert(salary(&alice, 500, Cardinality::Many))?;
    assert_eq!(
        read_branch(&b, &operator, &alice, "one").await?,
        vec![500],
        "alone, b's overlay row is its newest fact"
    );
    let rows: Vec<ConceptConclusion> = a
        .query()
        .join(&b)
        .select(salary_query(&alice, "one"))
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

/// Concurrent `last` writes settle by commit: two replicas each write
/// every one of a dozen cells without seeing the other, and after they
/// merge, every cell reads the value one and the same commit wrote, on
/// both replicas. The order of claims is the order of the revisions that
/// carry them (edition, then the version's hash), never the facts'
/// values or hashes, so no cell is won by one commit while another is
/// won by the other.
#[dialog_common::test]
async fn concurrent_last_writes_settle_by_commit_across_every_cell() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let a = repo.branch("a").open().perform(&operator).await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    let people: Vec<Entity> = (0..12)
        .map(|index| format!("id:person-{index}").parse())
        .collect::<Result<_, _>>()?;
    let mut base = a.transaction();
    for person in &people {
        base = base.assert(salary(person, 1, Cardinality::One));
    }
    base.commit().publish().perform(&operator).await?;
    b.pull(crate::Download::MinimalRanges)
        .from(&a)
        .perform(&operator)
        .await?;

    // Concurrent: values chosen so that neither the value order nor the
    // fact hashes agree with one side across every cell.
    let mut ours = a.transaction();
    let mut theirs = b.transaction();
    for (index, person) in people.iter().enumerate() {
        let (mine, other) = if index % 2 == 0 {
            (100, 200)
        } else {
            (200, 100)
        };
        ours = ours.assert(salary(person, mine + index as u32, Cardinality::One));
        theirs = theirs.assert(salary(person, other + index as u32, Cardinality::One));
    }
    ours.commit().publish().perform(&operator).await?;
    theirs.commit().publish().perform(&operator).await?;
    let mut quiesced = false;
    for _ in 0..4 {
        let pulled_a = a
            .pull(crate::Download::MinimalRanges)
            .from(&b)
            .perform(&operator)
            .await?;
        let pulled_b = b
            .pull(crate::Download::MinimalRanges)
            .from(&a)
            .perform(&operator)
            .await?;
        if pulled_a.is_none() && pulled_b.is_none() {
            quiesced = true;
            break;
        }
    }
    assert!(quiesced, "mutual pulls reach a fixed point");

    for branch in [&a, &b] {
        let mut winners = Vec::new();
        for (index, person) in people.iter().enumerate() {
            let read = read_branch(branch, &operator, person, "one").await?;
            assert_eq!(read.len(), 1, "one value per cell under `last`");
            let ours_value = if index % 2 == 0 { 100 } else { 200 } + index as u64;
            winners.push(read[0] == ours_value);
        }
        assert!(
            winners.iter().all(|won| *won) || winners.iter().all(|won| !*won),
            "every cell is won by the same commit: {winners:?}"
        );
    }
    Ok(())
}

/// Reads `org/salary` of `of` under `select` inside `transaction` and
/// again after its commit.
async fn read_through_commit(
    branch: &Branch,
    transaction: super::Transaction<&Branch>,
    operator: &Operator,
    of: &Entity,
    select: &str,
) -> Result<(Vec<u64>, Vec<u64>)> {
    let read = salaries(
        transaction
            .query()
            .select(salary_query(of, select))
            .perform(operator)
            .try_vec()
            .await?,
    )?;
    transaction.commit().publish().perform(operator).await?;
    Ok((read, read_branch(branch, operator, of, select).await?))
}

/// "A transaction reads what its commit will leave." The line holds 3
/// and, from a later commit, 6. A cardinality one write of 4 succeeds 6,
/// and a cardinality many write then writes 6 back: the commit lands 4
/// and 6 at its own edition, the newest of the cell, standing equal, and
/// 6 hashes greater, so a read returns 6. The transaction read the
/// line's older row of 6 instead and returned 4.
#[dialog_common::test]
async fn a_value_written_back_after_its_succession_reads_as_the_commit_leaves_it() -> Result<()> {
    // The written-back value must win the tie, or a transaction that
    // returned the first write's value would pass.
    assert_eq!(tie_winner(4, 6), 6, "6 hashes greater than 4");
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    for value in [3, 6] {
        branch
            .transaction()
            .assert(salary(&alice, value, Cardinality::Many))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    let transaction = branch
        .transaction()
        .assert(salary(&alice, 4, Cardinality::One))
        .assert(salary(&alice, 6, Cardinality::Many));
    let (read, committed) =
        read_through_commit(&branch, transaction, &operator, &alice, "one").await?;
    assert_eq!(
        committed,
        vec![6],
        "the commit lands 4 and 6 at the newest edition, and 6 wins the tie"
    );
    assert_eq!(read, committed, "the transaction read what its commit left");
    Ok(())
}

/// "A transaction reads what its commit will leave." The line holds 2,
/// then 5 from a later commit. An `all` write of 2 lands it at the
/// commit's edition, newer than 5, so a `last` read returns 2. The
/// transaction's read merged its staged 2 with the line's older row of
/// the same fact and kept the line's, returning 5.
#[dialog_common::test]
async fn an_all_write_of_a_held_value_reads_as_the_commit_leaves_it() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    for value in [2, 5] {
        branch
            .transaction()
            .assert(salary(&alice, value, Cardinality::Many))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    let transaction = branch
        .transaction()
        .assert(salary(&alice, 2, Cardinality::Many));
    let (read, committed) =
        read_through_commit(&branch, transaction, &operator, &alice, "one").await?;
    assert_eq!(
        committed,
        vec![2],
        "the commit refreshes 2 to the newest edition"
    );
    assert_eq!(read, committed, "the transaction read what its commit left");
    Ok(())
}

/// "A transaction reads what its commit will leave." The line holds 6,
/// 1 and 2, committed in that order. A cardinality one write of 6
/// succeeds 2, the newest claim, and refreshes the held 6, which the
/// commit lands at its own edition, so a read returns 6. The settlement took
/// the held 6 for one the write left alone and hid the staged row, so
/// the transaction read the line's older 6 and returned 2.
#[dialog_common::test]
async fn a_held_value_a_write_refreshes_reads_as_the_commit_leaves_it() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    for value in [6, 1, 2] {
        branch
            .transaction()
            .assert(salary(&alice, value, Cardinality::Many))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    let transaction = branch
        .transaction()
        .assert(salary(&alice, 6, Cardinality::One));
    let (read, committed) =
        read_through_commit(&branch, transaction, &operator, &alice, "one").await?;
    assert_eq!(
        committed,
        vec![6],
        "the commit refreshes 6 to the newest edition"
    );
    assert_eq!(read, committed, "the transaction read what its commit left");
    Ok(())
}
