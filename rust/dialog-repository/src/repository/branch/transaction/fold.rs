//! A deductive rule that folds, installed as facts on a branch.
//!
//! `org.dept/total(d) := sum(s), org.dept/heads(d) := count(s) :-
//! org.employee/dept(e) = d, org.employee/salary(e) = s` folds the
//! salaries of a department. `org.employee/salary(e) := t :-
//! org.employee/dept(e) = d, org.dept/total(d) = t` feeds the totals
//! back into what they sum. Either alone is fine; together they close a
//! cycle through the fold, and one of them is set aside by the rule a
//! cycle through `unless` is (`quarantine.rs`).

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::Branch;
use crate::helpers::test_repo;
use anyhow::Result;
use dialog_corpus::{Entity, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::query::Output as _;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::types::Any;
use dialog_query::{ConceptDescriptor, ConceptQuery, DeductiveRule, Parameters, Term};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::StreamExt as _;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn compile(json: serde_json::Value) -> Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json)?;
    Ok(descriptor.compile()?)
}

/// The fold: the sum and the count of a department's salaries.
fn fold() -> Result<DeductiveRule> {
    compile(serde_json::json!({
        "deduce": { "with": {
            "total": { "the": "org.dept/total", "as": "integer:" },
            "heads": { "the": "org.dept/heads", "as": "natural:" }
        }},
        "when": [{
            "assert": { "with": {
                "dept": { "the": "org.employee/dept", "as": "entity:" },
                "salary": { "the": "org.employee/salary", "as": "integer:" }
            }},
            "where": {
                "this": { "?": { "name": "employee" } },
                "dept": { "?": { "name": "this" } },
                "salary": { "?": { "name": "salary" } }
            }
        }],
        "reduce": {
            "total": { "apply": "sum", "of": { "?": { "name": "salary" } } },
            "heads": { "apply": "count", "of": { "?": { "name": "salary" } } }
        }
    }))
}

/// The feedback: a department's total becomes each member's salary.
fn feedback() -> Result<DeductiveRule> {
    compile(serde_json::json!({
        "deduce": { "with": { "salary": { "the": "org.employee/salary", "as": "integer:" } } },
        "when": [{
            "assert": { "with": { "dept": { "the": "org.employee/dept", "as": "entity:" } } },
            "where": { "this": { "?": { "name": "this" } }, "dept": { "?": { "name": "dept" } } }
        }, {
            "assert": { "with": { "total": { "the": "org.dept/total", "as": "integer:" } } },
            "where": { "this": { "?": { "name": "dept" } }, "total": { "?": { "name": "salary" } } }
        }]
    }))
}

/// alice (100) and bob (50) in `dept-a`, carol (7) in `dept-b`.
async fn staff(branch: &Branch, operator: &Operator) -> Result<()> {
    let mut transaction = branch.transaction();
    for (who, dept, salary) in [
        ("id:alice", "id:dept-a", 100i64),
        ("id:bob", "id:dept-a", 50),
        ("id:carol", "id:dept-b", 7),
    ] {
        let who: Entity = who.parse()?;
        let dept: Entity = dept.parse()?;
        transaction = transaction
            .assert(
                dialog_query::the!("org.employee/dept")
                    .of(who.clone())
                    .is(dept),
            )
            .assert(dialog_query::the!("org.employee/salary").of(who).is(salary));
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// The `(entity, value)` pairs `branch` reads for the integer relation
/// `the`, sorted.
async fn pairs(branch: &Branch, operator: &Operator, the: &str) -> Result<Vec<(Entity, i64)>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "value": { "the": the, "as": "integer:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::var("this"));
    terms.insert("value".to_string(), Term::<Any>::var("value"));
    let rows = branch
        .select(ConceptQuery { predicate, terms })
        .perform(operator)
        .try_vec()
        .await?;
    let mut pairs: Vec<(Entity, i64)> = rows
        .iter()
        .map(|row| {
            // A sum narrows to the narrowest integer holding it, so a
            // positive total reads as a natural.
            let read = row.source().lookup(&Term::<Any>::var("value"))?.content()?;
            let value = match read {
                Value::SignedInt(n) => i64::try_from(n)?,
                Value::UnsignedInt(n) => i64::try_from(n)?,
                other => anyhow::bail!("an integer: {other:?}"),
            };
            Ok((row.get::<Entity>("this")?, value))
        })
        .collect::<Result<_>>()?;
    pairs.sort();
    Ok(pairs)
}

fn at(name: &str) -> Entity {
    name.parse().expect("an entity parses")
}

/// What the payroll sums to.
fn totals() -> Vec<(Entity, i64)> {
    vec![(at("id:dept-a"), 150), (at("id:dept-b"), 7)]
}

/// What the payroll stores.
fn salaries() -> Vec<(Entity, i64)> {
    vec![
        (at("id:alice"), 100),
        (at("id:bob"), 50),
        (at("id:carol"), 7),
    ]
}

/// The rules `branch` sets aside, read as `dialog.rule/quarantined`.
async fn quarantined(branch: &Branch, operator: &Operator) -> Result<Vec<Entity>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "concept": { "the": "dialog.rule/quarantined", "as": "entity:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::var("this"));
    terms.insert("concept".to_string(), Term::<Any>::var("concept"));
    let rows = branch
        .select(ConceptQuery { predicate, terms })
        .perform(operator)
        .try_vec()
        .await?;
    let mut rules: Vec<Entity> = rows
        .iter()
        .map(|row| row.get::<Entity>("this"))
        .collect::<Result<_, _>>()?;
    rules.sort();
    Ok(rules)
}

/// The identity of the head of `rule` deriving `the`: what the analysis
/// sets aside when it sets aside that part of a rule of several heads.
fn head(rule: &DeductiveRule, the: &str) -> Result<Entity> {
    let heads = rule.heads()?;
    let head = heads
        .iter()
        .find(|head| head.field.the().to_string() == the)
        .ok_or_else(|| anyhow::anyhow!("a head derives {the}"))?;
    Ok(head.rule.this())
}

/// Commits `rules` on `branch` in one transaction, in the order given.
async fn install(branch: &Branch, operator: &Operator, rules: &[&DeductiveRule]) -> Result<()> {
    let mut transaction = branch.transaction();
    for rule in rules {
        transaction = transaction.assert(*rule);
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// Pulls `from` into `into` until nothing more arrives.
async fn pull(into: &Branch, from: &Branch, operator: &Operator) -> Result<()> {
    for _ in 0..4 {
        if into
            .pull(crate::Download::MinimalRanges)
            .from(from)
            .perform(operator)
            .await?
            .is_none()
        {
            return Ok(());
        }
    }
    anyhow::bail!("pulls reach a fixed point")
}

/// A folding rule installs as facts, replicates to another branch as
/// facts, and reads back there: the stored body decodes to a rule with
/// the same identity and both reduce entries, and the other branch
/// derives the same sums.
#[dialog_common::test]
async fn it_installs_a_folding_rule_that_syncs_as_facts() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let a = repo.branch("a").open().perform(&operator).await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    staff(&a, &operator).await?;
    let fold = fold()?;
    install(&a, &operator, &[&fold]).await?;
    assert_eq!(pairs(&a, &operator, "org.dept/total").await?, totals());

    pull(&b, &a, &operator).await?;
    let selector = Selector::new()
        .the("dialog.rule/source".parse()?)
        .of(fold.this());
    let stored: Vec<Value> = b
        .claims()
        .select(selector)
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|item| item.and_then(|view| view.to_owned()).map(|fact| fact.is))
        .collect::<Result<_, _>>()?;
    let [Value::Bytes(body)] = stored.as_slice() else {
        anyhow::bail!("one stored body: {stored:?}");
    };
    let decoded = DeductiveRule::decode(body).map_err(anyhow::Error::msg)?;
    assert_eq!(decoded.this(), fold.this());
    // The stored body is the canonical spelling, whose head fields are
    // renamed by structure, so the folds are told apart by aggregator.
    let mut reduced: Vec<String> = decoded
        .reduce()
        .iter()
        .map(|entry| format!("{:?}", entry.aggregator))
        .collect();
    reduced.sort();
    assert_eq!(reduced, vec!["Count", "Sum"]);
    assert_eq!(pairs(&b, &operator, "org.dept/total").await?, totals());
    assert_eq!(quarantined(&b, &operator).await?, Vec::<Entity>::new());
    Ok(())
}

/// The fold, then in a later commit the feedback: the feedback is set
/// aside, the totals read as before it, and the salaries are the
/// stored ones. Retracting the feedback lifts the quarantine.
#[dialog_common::test]
async fn it_sets_aside_the_feedback_committed_after_the_fold() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    staff(&branch, &operator).await?;
    let feedback = feedback()?;
    install(&branch, &operator, &[&fold()?]).await?;
    install(&branch, &operator, &[&feedback]).await?;
    assert_eq!(
        quarantined(&branch, &operator).await?,
        vec![feedback.this()]
    );
    assert_eq!(pairs(&branch, &operator, "org.dept/total").await?, totals());
    assert_eq!(
        pairs(&branch, &operator, "org.employee/salary").await?,
        salaries()
    );

    branch
        .transaction()
        .retract(&feedback)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(quarantined(&branch, &operator).await?, Vec::<Entity>::new());
    Ok(())
}

/// The feedback, then in a later commit the fold: the fold is the rule
/// installed last, so it is set aside and nothing derives a total. What
/// is set aside is the fold's `org.dept/total` head, under the head's
/// own identity, not the installed rule's: its `org.dept/heads` head
/// closes no cycle and still counts.
#[dialog_common::test]
async fn it_sets_aside_the_fold_committed_after_the_feedback() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    staff(&branch, &operator).await?;
    let fold = fold()?;
    install(&branch, &operator, &[&feedback()?]).await?;
    install(&branch, &operator, &[&fold]).await?;
    assert_eq!(
        quarantined(&branch, &operator).await?,
        vec![head(&fold, "org.dept/total")?]
    );
    assert_ne!(head(&fold, "org.dept/total")?, fold.this());
    assert_eq!(
        pairs(&branch, &operator, "org.dept/total").await?,
        Vec::<(Entity, i64)>::new()
    );
    assert_eq!(
        pairs(&branch, &operator, "org.employee/salary").await?,
        salaries()
    );
    Ok(())
}

/// Both rules in one commit, asserted in either order: the same rule is
/// set aside, the feedback, which derives what the fold reads, so the
/// fold keeps the meaning it has over the stored salaries.
#[dialog_common::test]
async fn it_sets_aside_the_same_rule_whichever_order_one_commit_asserts_them() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let (fold, feedback) = (fold()?, feedback()?);
    let first = repo.branch("first").open().perform(&operator).await?;
    let second = repo.branch("second").open().perform(&operator).await?;
    staff(&first, &operator).await?;
    staff(&second, &operator).await?;
    install(&first, &operator, &[&fold, &feedback]).await?;
    install(&second, &operator, &[&feedback, &fold]).await?;
    assert_eq!(quarantined(&first, &operator).await?, vec![feedback.this()]);
    assert_eq!(
        quarantined(&second, &operator).await?,
        vec![feedback.this()]
    );
    assert_eq!(pairs(&first, &operator, "org.dept/total").await?, totals());
    assert_eq!(pairs(&second, &operator, "org.dept/total").await?, totals());
    Ok(())
}

/// Two replicas each install one rule of the cycle without seeing the
/// other: `a` the fold, `b` the feedback. `c` merges `a` then `b`, `d`
/// merges `b` then `a`, and `a` and `b` merge each other. All four set
/// aside the same one rule and read the same totals and salaries.
#[dialog_common::test]
async fn it_sets_aside_the_same_rule_whichever_order_replicas_merge() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let (fold, feedback) = (fold()?, feedback()?);
    let a = repo.branch("a").open().perform(&operator).await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    let c = repo.branch("c").open().perform(&operator).await?;
    let d = repo.branch("d").open().perform(&operator).await?;
    staff(&a, &operator).await?;
    pull(&b, &a, &operator).await?;

    install(&a, &operator, &[&fold]).await?;
    install(&b, &operator, &[&feedback]).await?;

    pull(&c, &a, &operator).await?;
    pull(&c, &b, &operator).await?;
    pull(&d, &b, &operator).await?;
    pull(&d, &a, &operator).await?;
    pull(&a, &b, &operator).await?;
    pull(&b, &a, &operator).await?;

    let mut seen = Vec::new();
    for branch in [&a, &b, &c, &d] {
        seen.push((
            quarantined(branch, &operator).await?,
            pairs(branch, &operator, "org.dept/total").await?,
            pairs(branch, &operator, "org.employee/salary").await?,
        ));
    }
    let (set_aside, total, salary) = seen[0].clone();
    for (index, each) in seen.iter().enumerate() {
        assert_eq!(each, &seen[0], "replica {index} agrees with a");
    }
    assert_eq!(set_aside.len(), 1, "one rule is set aside: {set_aside:?}");
    assert!(
        set_aside == vec![head(&fold, "org.dept/total")?] || set_aside == vec![feedback.this()],
        "a rule of the cycle: {set_aside:?}"
    );
    assert_eq!(salary, salaries(), "nothing derives a salary either way");
    if set_aside == vec![feedback.this()] {
        assert_eq!(total, totals());
    } else {
        assert_eq!(total, Vec::<(Entity, i64)>::new());
    }
    Ok(())
}
