//! A fold in a deductive rule: read where any relation is read, and a
//! rule closing a cycle through it set aside as one closing a cycle
//! through `unless` is.
//!
//! The fixture is a payroll: `org.employee/dept` and
//! `org.employee/salary` are stored; `org.dept/total` and
//! `org.dept/heads` are the sum and the count of a department's
//! salaries, folded by one rule.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::{Closure, ProgramAnalysis, RuleRegistry};
use crate::source::test::TestEnv;
use crate::the;
use crate::types::Any;
use crate::{
    ConceptDescriptor, ConceptQuery, DeductiveRule, EvaluationError, Match, Parameters, Term, Value,
};
use dialog_corpus::Entity;
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn compile(json: serde_json::Value) -> anyhow::Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json)?;
    Ok(descriptor.compile()?)
}

/// `org.dept/total(d) := sum(s), org.dept/heads(d) := count(s) :-
/// org.employee/dept(e) = d, org.employee/salary(e) = s`.
fn dept_total() -> anyhow::Result<DeductiveRule> {
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

/// `org.employee/salary(e) := t :- org.employee/dept(e) = d,
/// org.dept/total(d) = t`: feeds the totals back into what they sum,
/// closing a cycle through the fold.
fn feedback() -> anyhow::Result<DeductiveRule> {
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

/// `<head>(e) := v :- <from>(e) = v`, both integer relations.
fn copy(head: &str, from: &str) -> anyhow::Result<DeductiveRule> {
    compile(serde_json::json!({
        "deduce": { "with": { "v": { "the": head, "as": "integer:" } } },
        "when": [{
            "assert": { "with": { "v": { "the": from, "as": "integer:" } } },
            "where": { "this": { "?": { "name": "this" } }, "v": { "?": { "name": "v" } } }
        }]
    }))
}

/// Three employees in two departments: alice (100) and bob (50) in
/// `dept-a`, carol (7) in `dept-b`.
async fn staff(operator: &Operator, branch: &dialog_repository::Branch) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for (who, dept, salary) in [
        ("id:alice", "id:dept-a", 100i64),
        ("id:bob", "id:dept-a", 50),
        ("id:carol", "id:dept-b", 7),
    ] {
        let who: Entity = who.parse()?;
        let dept: Entity = dept.parse()?;
        transaction = transaction
            .assert(the!("org.employee/dept").of(who.clone()).is(dept))
            .assert(the!("org.employee/salary").of(who).is(salary));
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// The concept over the one relation `the`, read as `as`.
fn relation(the: &str, of: &str) -> anyhow::Result<ConceptDescriptor> {
    Ok(serde_json::from_value(serde_json::json!({ "with": {
        "value": { "the": the, "as": of }
    }}))?)
}

/// The `(entity, value)` pairs of the relation `the` read as `of`,
/// sorted.
async fn pairs(source: &TestEnv<'_>, the: &str, of: &str) -> anyhow::Result<Vec<(Value, Value)>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::var("value"));
    let rows = ConceptQuery {
        terms,
        predicate: relation(the, of)?,
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    let mut pairs: Vec<(Value, Value)> = rows
        .iter()
        .map(|row| {
            Ok((
                row.lookup(&Term::<Any>::var("who"))?.content()?,
                row.lookup(&Term::<Any>::var("value"))?.content()?,
            ))
        })
        .collect::<Result<_, EvaluationError>>()?;
    pairs.sort_by_key(|pair| format!("{pair:?}"));
    Ok(pairs)
}

fn at(name: &str) -> Value {
    Value::Entity(name.parse().expect("an entity parses"))
}

/// The totals the payroll sums to. A `sum` accumulates in `i128` and
/// narrows each group's result to the narrowest value that holds it, so
/// a total of positive salaries reads as a natural though its field is
/// declared `integer:`.
fn totals() -> Vec<(Value, Value)> {
    vec![
        (at("id:dept-a"), Value::UnsignedInt(150)),
        (at("id:dept-b"), Value::UnsignedInt(7)),
    ]
}

/// The salaries the payroll stores.
fn salaries() -> Vec<(Value, Value)> {
    vec![
        (at("id:alice"), Value::SignedInt(100)),
        (at("id:bob"), Value::SignedInt(50)),
        (at("id:carol"), Value::SignedInt(7)),
    ]
}

/// A deductive rule folds: `org.dept/total` is the sum and
/// `org.dept/heads` the count of a department's salaries, one value per
/// department, read as any relation is.
#[dialog_common::test]
async fn it_folds_a_sum_and_a_count_in_a_deductive_rule() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    staff(&operator, &branch).await?;
    let mut registry = RuleRegistry::new();
    registry.register(dept_total()?)?;
    let analysis = registry.analysis()?;
    assert!(analysis.quarantined().is_empty());
    assert!(analysis.violations().is_empty());
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        pairs(&source, "org.dept/total", "integer:").await?,
        totals()
    );
    assert_eq!(
        pairs(&source, "org.dept/heads", "natural:").await?,
        vec![
            (at("id:dept-a"), Value::UnsignedInt(2)),
            (at("id:dept-b"), Value::UnsignedInt(1)),
        ]
    );
    Ok(())
}

/// A rule reads what a folding rule derives, a stratum below it:
/// `org.dept/large(d) := t :- org.dept/heads(d) = n, n > 1,
/// org.dept/total(d) = t`.
#[dialog_common::test]
async fn it_reads_a_fold_from_another_rules_premise() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    staff(&operator, &branch).await?;
    let large = compile(serde_json::json!({
        "deduce": { "with": { "large": { "the": "org.dept/large", "as": "integer:" } } },
        "when": [{
            "assert": { "with": { "heads": { "the": "org.dept/heads", "as": "natural:" } } },
            "where": { "this": { "?": { "name": "this" } }, "heads": { "?": { "name": "heads" } } }
        }, {
            "assert": ">",
            "where": { "of": { "?": { "name": "heads" } }, "with": 1 }
        }, {
            "assert": { "with": { "total": { "the": "org.dept/total", "as": "integer:" } } },
            "where": { "this": { "?": { "name": "this" } }, "total": { "?": { "name": "large" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(dept_total()?)?;
    registry.register(large)?;
    assert!(registry.analysis()?.quarantined().is_empty());
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        pairs(&source, "org.dept/large", "integer:").await?,
        vec![(at("id:dept-a"), Value::SignedInt(150))],
        "only the department of two is large, valued with its total"
    );
    Ok(())
}

/// The fold, then the rule feeding its totals back into the salaries
/// it sums: the feedback closes a cycle through the fold and, installed
/// last, is set aside. The analysis records it, reports no violation,
/// and the closure checks as acyclic; the totals read as they did
/// before it arrived, and the salaries are the stored ones.
#[dialog_common::test]
async fn it_quarantines_the_rule_closing_a_cycle_through_a_fold() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    staff(&operator, &branch).await?;
    let feedback = feedback()?;
    let mut registry = RuleRegistry::new();
    registry.register(dept_total()?)?;
    registry.register(feedback.clone())?;
    let analysis = registry.analysis()?;
    let [set_aside] = analysis.quarantined() else {
        anyhow::bail!("one rule is set aside: {:?}", analysis.quarantined());
    };
    assert_eq!(set_aside.rule, feedback.this(), "the rule installed last");
    let salary = ProgramAnalysis::node(&relation("org.employee/salary", "integer:")?);
    let total = ProgramAnalysis::node(&relation("org.dept/total", "integer:")?);
    assert_eq!(set_aside.concept, salary);
    // The cycle runs salary -> the fold's premise concept, which
    // selects `dept` and `salary` -> salary, and total -> that concept
    // (the aggregating edge) and salary -> total.
    let staffed: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "dept": { "the": "org.employee/dept", "as": "entity:" },
        "salary": { "the": "org.employee/salary", "as": "integer:" }
    }}))?;
    let mut cycle = vec![salary, total, ProgramAnalysis::node(&staffed)];
    cycle.sort();
    assert_eq!(set_aside.cycle, cycle);
    assert!(analysis.violations().is_empty());
    assert!(matches!(
        analysis.check(&relation("org.dept/total", "integer:")?),
        Ok(Closure::Acyclic)
    ));
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        pairs(&source, "org.dept/total", "integer:").await?,
        totals()
    );
    assert_eq!(
        pairs(&source, "org.employee/salary", "integer:").await?,
        salaries()
    );
    Ok(())
}

/// The same two rules in the other order: the fold arrives last and is
/// the one set aside, so nothing derives a total, and the feedback,
/// which reads the totals, derives no salary. The salaries are the
/// stored ones. Which rule goes depends on the order of install, as it
/// does for a cycle through `unless`.
#[dialog_common::test]
async fn it_quarantines_the_fold_when_it_arrives_last() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    staff(&operator, &branch).await?;
    let mut registry = RuleRegistry::new();
    registry.register(feedback()?)?;
    registry.register(dept_total()?)?;
    let analysis = registry.analysis()?;
    let [set_aside] = analysis.quarantined() else {
        anyhow::bail!("one rule is set aside: {:?}", analysis.quarantined());
    };
    let total = ProgramAnalysis::node(&relation("org.dept/total", "integer:")?);
    assert_eq!(set_aside.concept, total, "the fold's total head");
    assert!(analysis.violations().is_empty());
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        pairs(&source, "org.dept/total", "integer:").await?,
        Vec::<(Value, Value)>::new()
    );
    assert_eq!(
        pairs(&source, "org.dept/heads", "natural:").await?,
        vec![
            (at("id:dept-a"), Value::UnsignedInt(2)),
            (at("id:dept-b"), Value::UnsignedInt(1)),
        ],
        "the count head reads salaries, which nothing derives now: no cycle runs through it"
    );
    assert_eq!(
        pairs(&source, "org.employee/salary", "integer:").await?,
        salaries()
    );
    Ok(())
}

/// A fold seeding a positive cycle. `org.dept/budget(d) :=
/// org.dept/total(d)` and `org.dept/total(d) := org.dept/budget(d)`
/// put the folded relation on a cycle no absence test runs through, so
/// nothing is set aside and the fixpoint evaluates it, with the fold's
/// rows as a seed (its input sits below the cycle). A reader inside the
/// cycle (the budget rule reads the total) and readers outside it (a
/// query of the total, and `org.dept/report(d) := org.dept/total(d)`)
/// see the same relation: the fold's sums.
#[dialog_common::test]
async fn it_gives_readers_inside_and_outside_a_cycle_the_same_fold() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    staff(&operator, &branch).await?;
    let mut registry = RuleRegistry::new();
    registry.register(dept_total()?)?;
    registry.register(copy("org.dept/budget", "org.dept/total")?)?;
    registry.register(copy("org.dept/total", "org.dept/budget")?)?;
    registry.register(copy("org.dept/report", "org.dept/total")?)?;
    let analysis = registry.analysis()?;
    assert!(analysis.quarantined().is_empty());
    assert!(analysis.violations().is_empty());
    let total = ProgramAnalysis::node(&relation("org.dept/total", "integer:")?);
    let budget = ProgramAnalysis::node(&relation("org.dept/budget", "integer:")?);
    let report = ProgramAnalysis::node(&relation("org.dept/report", "integer:")?);
    assert!(analysis.in_same_cycle(&total, &budget));
    assert!(!analysis.is_recursive(&report));
    assert!(matches!(
        analysis.check(&relation("org.dept/report", "integer:")?),
        Ok(Closure::Recursive)
    ));
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        pairs(&source, "org.dept/total", "integer:").await?,
        totals()
    );
    assert_eq!(
        pairs(&source, "org.dept/budget", "integer:").await?,
        totals(),
        "the reader inside the cycle"
    );
    // The same numbers, typed as the head of the rule that read them:
    // a fold's output is narrowed per group (a natural here) and kept
    // so through the fixpoint, while a plain rule outside the cycle
    // binds it to its own `integer:` head. The same holds with no
    // cycle at all (`it_reads_a_fold_from_another_rules_premise`).
    assert_eq!(
        pairs(&source, "org.dept/report", "integer:").await?,
        vec![
            (at("id:dept-a"), Value::SignedInt(150)),
            (at("id:dept-b"), Value::SignedInt(7)),
        ],
        "the reader outside the cycle"
    );
    Ok(())
}

/// A fold over a relation a cycle derives below it: the cycle is
/// derived in full first, then folded. `org.unit/within(u) := p :-
/// org.unit/parent(u) = p`, and `within(u) := g :- within(u) = p,
/// within(p) = g` under `all`; `org.unit/depth(u) := count(a) :-
/// within(u) = a`.
#[dialog_common::test]
async fn it_folds_over_a_relation_a_cycle_derives_below_it() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut transaction = branch.transaction();
    for (unit, parent) in [("id:c", "id:b"), ("id:b", "id:a")] {
        let unit: Entity = unit.parse()?;
        let parent: Entity = parent.parse()?;
        transaction = transaction.assert(the!("org.unit/parent").of(unit).is(parent));
    }
    transaction.commit().publish().perform(&operator).await?;
    let within = |field: &str| {
        serde_json::json!({ "with": {
            field: { "the": "org.unit/within", "as": "entity:", "pick": "all" }
        }})
    };
    let step = compile(serde_json::json!({
        "deduce": within("within"),
        "when": [{
            "assert": { "with": { "parent": { "the": "org.unit/parent", "as": "entity:" } } },
            "where": { "this": { "?": { "name": "this" } }, "parent": { "?": { "name": "within" } } }
        }]
    }))?;
    let closure = compile(serde_json::json!({
        "deduce": within("within"),
        "when": [{
            "assert": within("within"),
            "where": { "this": { "?": { "name": "this" } }, "within": { "?": { "name": "p" } } }
        }, {
            "assert": within("within"),
            "where": { "this": { "?": { "name": "p" } }, "within": { "?": { "name": "within" } } }
        }]
    }))?;
    let depth = compile(serde_json::json!({
        "deduce": { "with": { "depth": { "the": "org.unit/depth", "as": "natural:" } } },
        "when": [{
            "assert": within("within"),
            "where": { "this": { "?": { "name": "this" } }, "within": { "?": { "name": "a" } } }
        }],
        "reduce": { "depth": { "apply": "count", "of": { "?": { "name": "a" } } } }
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(step)?;
    registry.register(closure)?;
    registry.register(depth)?;
    let analysis = registry.analysis()?;
    assert!(analysis.quarantined().is_empty());
    assert!(analysis.violations().is_empty());
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        pairs(&source, "org.unit/depth", "natural:").await?,
        vec![
            (at("id:b"), Value::UnsignedInt(1)),
            (at("id:c"), Value::UnsignedInt(2)),
        ]
    );
    Ok(())
}

/// A fold over its own conclusion closes a cycle of one rule:
/// `org.dept/total(d) := sum(t) :- org.dept/total(d) = t`. With nothing
/// else to set aside, the rule itself is, and the relation reads what
/// is stored.
#[dialog_common::test]
async fn it_quarantines_a_fold_over_its_own_conclusion() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let dept_a: Entity = "id:dept-a".parse()?;
    branch
        .transaction()
        .assert(the!("org.dept/total").of(dept_a).is(5i64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let refold = compile(serde_json::json!({
        "deduce": { "with": { "total": { "the": "org.dept/total", "as": "integer:" } } },
        "when": [{
            "assert": { "with": { "total": { "the": "org.dept/total", "as": "integer:" } } },
            "where": { "this": { "?": { "name": "this" } }, "total": { "?": { "name": "t" } } }
        }],
        "reduce": { "total": { "apply": "sum", "of": { "?": { "name": "t" } } } }
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(refold.clone())?;
    let analysis = registry.analysis()?;
    let [set_aside] = analysis.quarantined() else {
        anyhow::bail!("one rule is set aside: {:?}", analysis.quarantined());
    };
    assert_eq!(set_aside.rule, refold.this());
    assert!(analysis.violations().is_empty());
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        pairs(&source, "org.dept/total", "integer:").await?,
        vec![(at("id:dept-a"), Value::SignedInt(5))]
    );
    Ok(())
}

/// A stored claim beside a folded value of the same relation, under
/// `last`: what a read returns for `dept-a`, where a total of 5 was
/// written before the salaries. Pinned to show where a folded value
/// stands against a stored one; the notation's design says a stored
/// value should never beat a count.
#[dialog_common::test]
async fn it_elects_between_a_stored_claim_and_a_folded_value() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let dept_a: Entity = "id:dept-a".parse()?;
    branch
        .transaction()
        .assert(the!("org.dept/total").of(dept_a).is(5i64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    staff(&operator, &branch).await?;
    let mut registry = RuleRegistry::new();
    registry.register(dept_total()?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        pairs(&source, "org.dept/total", "integer:").await?,
        vec![
            (at("id:dept-a"), Value::SignedInt(5)),
            (at("id:dept-b"), Value::UnsignedInt(7)),
        ],
        "the folded value for dept-a stands below the stored claim"
    );
    Ok(())
}
