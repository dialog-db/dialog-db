//! Invariants the attribute-heads design claims, pinned as tests.
//!
//! Each test states a property `notes/attribute-heads.md` or the
//! changelog asserts, and checks it the way a reader of those notes
//! would. A failing test here is a place where the engine does not do
//! what the design says.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::{Absence, RuleRegistry};
use crate::source::test::TestEnv;
use crate::the;
use crate::types::Any;
use crate::{ConceptDescriptor, ConceptQuery, DeductiveRule, Match, Parameters, Term, Value};
use dialog_corpus::Entity;
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_storage::provider::storage::VolatileSpace;

fn compile(json: serde_json::Value) -> anyhow::Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json)?;
    Ok(descriptor.compile()?)
}

/// The `(group, role)` pairs a concept over `member/group` and
/// `member/role` yields for `of`, with both fields read under the
/// cardinality `select` names, sorted.
async fn member_rows(
    source: &TestEnv<'_>,
    of: &Entity,
    select: &str,
) -> anyhow::Result<Vec<(Value, Value)>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "group": { "the": "member/group", "as": "entity:", "cardinality": select },
        "role": { "the": "member/role", "as": "text:", "cardinality": select }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::<Any>::constant(of.clone()));
    terms.insert("group".into(), Term::var("group"));
    terms.insert("role".into(), Term::var("role"));
    let rows = ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), source)
        .try_vec()
        .await?;
    let mut pairs: Vec<(Value, Value)> = rows
        .iter()
        .map(|row| {
            Ok((
                row.lookup(&Term::<Any>::var("group"))?.content()?,
                row.lookup(&Term::<Any>::var("role"))?.content()?,
            ))
        })
        .collect::<Result<_, crate::EvaluationError>>()?;
    pairs.sort_by_key(|pair| format!("{pair:?}"));
    Ok(pairs)
}

/// `Member { group, role }` of a person, derived from the membership
/// entities naming the person: one head per attribute, one body.
fn member_rule() -> anyhow::Result<DeductiveRule> {
    compile(serde_json::json!({
        "deduce": { "with": {
            "group": { "the": "member/group", "as": "entity:" },
            "role": { "the": "member/role", "as": "text:" }
        }},
        "when": [{
            "assert": { "with": {
                "person": { "the": "membership/person", "as": "entity:" },
                "group": { "the": "membership/group", "as": "entity:" },
                "role": { "the": "membership/role", "as": "text:" }
            }},
            "where": {
                "this": { "?": { "name": "membership" } },
                "person": { "?": { "name": "this" } },
                "group": { "?": { "name": "group" } },
                "role": { "?": { "name": "role" } }
            }
        }]
    }))
}

/// Two memberships of alice: admin of g1, viewer of g2. Each fact of
/// the two memberships lands in its own commit, in the order given, so
/// every fact has a standing of its own: m1's group and m2's role are
/// the newest facts of their relations, m2's group and m1's role the
/// oldest.
async fn seed_memberships(
    branch: &dialog_repository::Branch,
    operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
    alice: &Entity,
) -> anyhow::Result<(Entity, Entity)> {
    let g1: Entity = "id:g1".parse()?;
    let g2: Entity = "id:g2".parse()?;
    let m1: Entity = "id:m1".parse()?;
    let m2: Entity = "id:m2".parse()?;
    macro_rules! commit {
        ($fact:expr) => {
            branch
                .transaction()
                .assert($fact)
                .commit()
                .publish()
                .perform(operator)
                .await?;
        };
    }
    commit!(the!("membership/person").of(m1.clone()).is(alice.clone()));
    commit!(the!("membership/person").of(m2.clone()).is(alice.clone()));
    commit!(the!("membership/group").of(m2.clone()).is(g2.clone()));
    commit!(
        the!("membership/role")
            .of(m1.clone())
            .is("admin".to_string())
    );
    commit!(the!("membership/group").of(m1.clone()).is(g1.clone()));
    commit!(
        the!("membership/role")
            .of(m2.clone())
            .is("viewer".to_string())
    );
    Ok((g1, g2))
}

/// A rule derives attributes; a concept selects them. `Member { group,
/// role }` reads each attribute through its own relation and joins
/// them on the entity, so alice's memberships, admin of g1 and viewer
/// of g2, read under `all` as every group beside every role: the cross
/// product the design note calls "the EAV model, not a bug". Asserting
/// the same two instances as facts gives the same four rows. This test
/// pins the intended semantics and passes on the head.
#[dialog_common::test]
async fn a_concept_read_under_all_joins_the_attributes_a_rule_derives() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    let (g1, g2) = seed_memberships(&branch, &operator, &alice).await?;
    let mut registry = RuleRegistry::new();
    registry.register(member_rule()?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    let mut expected = Vec::new();
    for group in [&g1, &g2] {
        for role in ["admin", "viewer"] {
            expected.push((Value::Entity(group.clone()), Value::String(role.into())));
        }
    }
    expected.sort_by_key(|pair| format!("{pair:?}"));
    assert_eq!(member_rows(&source, &alice, "many").await?, expected);
    Ok(())
}

/// Under `last` each attribute of `Member` elects one value per entity,
/// by the standing of the fact that bound it: m1's group fact, g1, is
/// the newest group, and m2's role fact, viewer, the newest role. So
/// alice reads as one row, `(g1, viewer)`. While nothing is stored
/// under `member/group` or `member/role` the engine answers through
/// the covering rule, the one rule re-headed onto the concept, which
/// runs no election for a multi-field concept and returns the body's
/// two rows: two values of a `last` attribute for one entity.
#[dialog_common::test]
async fn a_concept_read_under_last_elects_one_value_per_attribute() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    let (g1, _) = seed_memberships(&branch, &operator, &alice).await?;
    let mut registry = RuleRegistry::new();
    registry.register(member_rule()?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        member_rows(&source, &alice, "one").await?,
        vec![(Value::Entity(g1), Value::String("viewer".into()))],
        "one row: each attribute's newest value"
    );
    Ok(())
}

/// A concept's rows are a function of the facts and the rules. The
/// design says `Member`'s answer is its one rule re-headed onto the
/// concept (the covering rule) while nothing is stored under its
/// attributes, and the per-attribute election "the moment a fact lands
/// under one of those attributes". Storing a `member/role` of *bob*
/// says nothing about alice, so alice's rows must not change. Under
/// the default cardinality, one, they do: the covering rule yields the
/// body's two rows (no election runs on that path for a multi-field
/// concept), and the election yields one row, the right one under
/// attribute heads (see `a_concept_read_under_last_elects_one_value_per_attribute`).
#[dialog_common::test]
async fn a_fact_about_another_entity_does_not_change_a_derived_concepts_rows_under_last()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    let bob: Entity = "id:bob".parse()?;
    seed_memberships(&branch, &operator, &alice).await?;
    let mut registry = RuleRegistry::new();
    registry.register(member_rule()?)?;

    let before = {
        let source = TestEnv::new(&branch, &operator, registry.clone());
        member_rows(&source, &alice, "one").await?
    };
    branch
        .transaction()
        .assert(the!("member/role").of(bob.clone()).is("guest".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let after = {
        let source = TestEnv::new(&branch, &operator, registry);
        member_rows(&source, &alice, "one").await?
    };
    assert_eq!(
        after, before,
        "alice's rows under `last` do not depend on a stored fact about bob"
    );
    Ok(())
}

/// `x/p(e) :- x/q(e), unless x/r(e)` and `x/r(e) :- x/p(e)`. A
/// *stored* `x/r` fact is not something the cycle is still deriving: an
/// entity with a stored `x/r` is excluded by the rule as written, and
/// must not come out as `x/p`. The analysis quarantines `x/r(e) :-
/// x/p(e)`, the rule deriving what the negation reads, so the negation
/// reads the stored facts it was written against. (The cycle policy
/// this replaced dropped the premise and derived `p` for `b` anyway.)
#[dialog_common::test]
async fn a_negation_inside_a_cycle_still_sees_stored_facts() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let a: Entity = "id:a".parse()?;
    let b: Entity = "id:b".parse()?;
    branch
        .transaction()
        .assert(the!("x/q").of(a.clone()).is("a".to_string()))
        .assert(the!("x/q").of(b.clone()).is("b".to_string()))
        .assert(the!("x/r").of(b.clone()).is("stored".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let p = compile(serde_json::json!({
        "deduce": { "with": { "p": { "the": "x/p", "as": "text:" } } },
        "when": [{
            "assert": { "with": { "q": { "the": "x/q", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } }, "q": { "?": { "name": "p" } } }
        }],
        "unless": [{
            "assert": { "with": { "r": { "the": "x/r", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } } }
        }]
    }))?;
    let r = compile(serde_json::json!({
        "deduce": { "with": { "r": { "the": "x/r", "as": "text:" } } },
        "when": [{
            "assert": { "with": { "p": { "the": "x/p", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } }, "p": { "?": { "name": "r" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(p)?;
    registry.register(r)?;
    let source = TestEnv::new(&branch, &operator, registry);

    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "p": { "the": "x/p", "as": "text:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("p".into(), Term::var("p"));
    let rows = ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
    let mut who: Vec<Value> = rows
        .iter()
        .map(|row| row.lookup(&Term::<Any>::var("who"))?.content())
        .collect::<Result<_, crate::EvaluationError>>()?;
    who.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(
        who,
        vec![Value::Entity(a)],
        "b has a stored x/r, so the rule's `unless x/r` excludes it from x/p"
    );
    Ok(())
}

/// The rows an attribute concept over `the` yields for every entity,
/// read under the cardinality `select` names: `(this, value)` pairs,
/// sorted.
async fn relation_under(
    source: &TestEnv<'_>,
    the: &str,
    select: &str,
) -> anyhow::Result<Vec<(Value, Value)>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "value": { "the": the, "as": "text:", "cardinality": select }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::var("value"));
    let rows = ConceptQuery { terms, predicate }
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
        .collect::<Result<_, crate::EvaluationError>>()?;
    pairs.sort_by_key(|pair| format!("{pair:?}"));
    Ok(pairs)
}

/// `x/p(e) :- x/q(e), unless x/r(e)` with a stored `x/r(b)` derives
/// `p` for `a` alone: a stratified negation. Installing two rules that
/// neither read `q` nor touch `p`'s body, `x/s(e) :- x/p(e)` and
/// `x/r(e) :- x/s(e)`, closes a cycle through the negation, which a
/// replica merge can do at any time. The analysis quarantines the rule
/// deriving what the negation reads, `x/r(e) :- x/s(e)`, so the meaning
/// of the rule nobody edited stays what it was. (The cycle policy this
/// replaced read the negation as holding, and `p` gained `b`.)
#[dialog_common::test]
async fn a_stratified_negation_keeps_its_meaning_when_another_rule_closes_a_cycle()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let a: Entity = "id:a".parse()?;
    let b: Entity = "id:b".parse()?;
    branch
        .transaction()
        .assert(the!("x/q").of(a.clone()).is("a".to_string()))
        .assert(the!("x/q").of(b.clone()).is("b".to_string()))
        .assert(the!("x/r").of(b.clone()).is("stored".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let derive = |head: &str, from: &str| {
        compile(serde_json::json!({
            "deduce": { "with": { "v": { "the": head, "as": "text:" } } },
            "when": [{
                "assert": { "with": { "v": { "the": from, "as": "text:" } } },
                "where": { "this": { "?": { "name": "this" } }, "v": { "?": { "name": "v" } } }
            }]
        }))
    };
    let p = compile(serde_json::json!({
        "deduce": { "with": { "p": { "the": "x/p", "as": "text:" } } },
        "when": [{
            "assert": { "with": { "q": { "the": "x/q", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } }, "q": { "?": { "name": "p" } } }
        }],
        "unless": [{
            "assert": { "with": { "r": { "the": "x/r", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(p)?;
    let before = {
        let source = TestEnv::new(&branch, &operator, registry.clone());
        relation_under(&source, "x/p", "many").await?
    };
    assert_eq!(
        before,
        vec![(Value::Entity(a.clone()), Value::String("a".into()))],
        "stratified: b's stored x/r excludes it"
    );

    registry.register(derive("x/s", "x/p")?)?;
    registry.register(derive("x/r", "x/s")?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        relation_under(&source, "x/p", "many").await?,
        before,
        "two rules that read neither q nor r's stored facts do not change what p means"
    );
    Ok(())
}

/// `ancestor(this, a) :- parent(this, a)` and `ancestor(this, a) :-
/// parent(this, p), ancestor(p, a)`, read through a field under the
/// default cardinality, one. The design says election "runs once, over
/// the finished fixpoint, where the component's rows leave it" and "a
/// reader outside the component sees one value". Under `last` the
/// exit skips the election and every candidate leaves: a reader of a
/// cardinality-one field gets every ancestor of `c`.
#[dialog_common::test]
async fn a_recursive_relation_read_under_last_yields_one_value_per_entity() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let a: Entity = "id:a".parse()?;
    let b: Entity = "id:b".parse()?;
    let c: Entity = "id:c".parse()?;
    branch
        .transaction()
        .assert(the!("family/parent").of(c.clone()).is(b.clone()))
        .assert(the!("family/parent").of(b.clone()).is(a.clone()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let base = compile(serde_json::json!({
        "deduce": { "with": { "ancestor": { "the": "family/ancestor", "as": "entity:" } } },
        "when": [{
            "assert": { "with": { "parent": { "the": "family/parent", "as": "entity:" } } },
            "where": { "this": { "?": { "name": "this" } }, "parent": { "?": { "name": "ancestor" } } }
        }]
    }))?;
    let step = compile(serde_json::json!({
        "deduce": { "with": { "ancestor": { "the": "family/ancestor", "as": "entity:" } } },
        "when": [
            {
                "assert": { "with": { "parent": { "the": "family/parent", "as": "entity:" } } },
                "where": { "this": { "?": { "name": "this" } }, "parent": { "?": { "name": "p" } } }
            },
            {
                "assert": { "with": { "ancestor": { "the": "family/ancestor", "as": "entity:" } } },
                "where": { "this": { "?": { "name": "p" } }, "ancestor": { "?": { "name": "ancestor" } } }
            }
        ]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(base)?;
    registry.register(step)?;
    let source = TestEnv::new(&branch, &operator, registry);

    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "ancestor": { "the": "family/ancestor", "as": "entity:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::<Any>::constant(c.clone()));
    terms.insert("ancestor".into(), Term::var("ancestor"));
    let rows = ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
    assert_eq!(
        rows.len(),
        1,
        "a field under `last` reads one value; c's ancestors are {{a, b}}, the field elects one"
    );
    Ok(())
}

/// "A derived value stands by the fact that bound it." `x/a(e) := v :-
/// x/src(e) = v`: the derived value stands as the `x/src` fact, which
/// is newer than the stored `x/a`, so a `last` read returns the derived
/// value. The same relation made recursive by a second rule, `x/a(e)
/// := v :- x/a(e) = v`, goes through the fixpoint, whose rows carry the
/// newest standing of their derivations through every round: the exit
/// election under `last` picks the same value.
#[dialog_common::test]
async fn a_derived_value_stands_by_the_fact_that_bound_it_through_a_fixpoint() -> anyhow::Result<()>
{
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let e: Entity = "id:e".parse()?;
    branch
        .transaction()
        .assert(the!("x/a").of(e.clone()).is("old".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch
        .transaction()
        .assert(the!("x/src").of(e.clone()).is("new".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let derive = |from: &str| {
        compile(serde_json::json!({
            "deduce": { "with": { "a": { "the": "x/a", "as": "text:" } } },
            "when": [{
                "assert": { "with": { "v": { "the": from, "as": "text:" } } },
                "where": { "this": { "?": { "name": "this" } }, "v": { "?": { "name": "a" } } }
            }]
        }))
    };
    let expected = vec![(Value::Entity(e.clone()), Value::String("new".into()))];

    let mut registry = RuleRegistry::new();
    registry.register(derive("x/src")?)?;
    {
        let source = TestEnv::new(&branch, &operator, registry.clone());
        assert_eq!(
            relation_under(&source, "x/a", "one").await?,
            expected,
            "the derived value is newer than the stored claim"
        );
    }
    registry.register(derive("x/a")?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        relation_under(&source, "x/a", "one").await?,
        expected,
        "a rule reading the relation back changes nothing about which value is newest"
    );
    Ok(())
}

/// `x/status(e) := to :- x/status(e) = from`: a rule rewriting the
/// value of the cell its body reads.
fn rewrite(from: &str, to: &str) -> anyhow::Result<DeductiveRule> {
    compile(serde_json::json!({
        "deduce": { "with": { "status": { "the": "x/status", "as": "text:" } } },
        "when": [
            {
                "assert": { "with": { "status": { "the": "x/status", "as": "text:" } } },
                "where": { "this": { "?": { "name": "this" } }, "status": from }
            },
            {
                "assert": "==",
                "where": { "this": to, "is": { "?": { "name": "status" } } }
            }
        ]
    }))
}

/// "A conclusion under `last` succeeds the claim its body read."
/// `x/status(e) := "todo" :- x/status(e) = "triage"` over a stored
/// `x/status(e) = "triage"`: the rule rewrites the cell's value, so a
/// `last` read returns `"todo"`, and returns `"triage"` for the
/// rewrite the other way over a stored `"todo"`, though `"triage"`
/// sorts after `"todo"`: the successor stands after the claim, whatever
/// the values' order. An `all` read keeps both.
#[dialog_common::test]
async fn a_derived_value_succeeds_the_claim_its_body_read_under_last() -> anyhow::Result<()> {
    for (stored, derived) in [("triage", "todo"), ("todo", "triage")] {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let e: Entity = "id:e".parse()?;
        branch
            .transaction()
            .assert(the!("x/status").of(e.clone()).is(stored.to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let mut registry = RuleRegistry::new();
        registry.register(rewrite(stored, derived)?)?;
        let source = TestEnv::new(&branch, &operator, registry);
        assert_eq!(
            relation_under(&source, "x/status", "one").await?,
            vec![(Value::Entity(e.clone()), Value::String(derived.into()))],
            "the derived value succeeds the stored claim the rule read"
        );
        let mut both = vec![
            (Value::Entity(e.clone()), Value::String(stored.into())),
            (Value::Entity(e.clone()), Value::String(derived.into())),
        ];
        both.sort_by_key(|pair| format!("{pair:?}"));
        assert_eq!(
            relation_under(&source, "x/status", "many").await?,
            both,
            "under `all` the claim and its successor are both candidates"
        );
    }
    Ok(())
}

/// A rewrite of a rewrite succeeds it in turn: `"triage"` to `"todo"`
/// and `"todo"` to `"done"` over a stored `"triage"` read `"done"`
/// under `last`, the least of the three values. Each conclusion stands
/// one succession past what it read.
#[dialog_common::test]
async fn a_rewrite_of_a_derived_value_succeeds_it_in_turn() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let e: Entity = "id:e".parse()?;
    branch
        .transaction()
        .assert(the!("x/status").of(e.clone()).is("triage".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut registry = RuleRegistry::new();
    registry.register(rewrite("triage", "todo")?)?;
    registry.register(rewrite("todo", "done")?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        relation_under(&source, "x/status", "one").await?,
        vec![(Value::Entity(e), Value::String("done".into()))],
        "the second rewrite succeeds the first"
    );
    Ok(())
}

/// Two rules succeeding one claim elect by which one read newer facts,
/// as concurrent writes do. Over a stored `"triage"` and a later
/// `x/flag`, `x/status(e) := "todo" :- x/status(e) = "triage"` stands
/// just past the claim, and `x/status(e) := "blocked" :- x/status(e) =
/// "triage", x/flag(e) = _` stands at the flag: `last` reads
/// `"blocked"`, though `"todo"` sorts after it. A conclusion whose body
/// does not read the cell displaces nothing: a value derived from the
/// flag alone stands at the flag too, but ranks below the one that
/// read the cell and rewrote it, so `"blocked"` still wins, though
/// `"waiting"` sorts after it.
#[dialog_common::test]
async fn rules_succeeding_one_claim_elect_by_the_newer_reading() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let e: Entity = "id:e".parse()?;
    branch
        .transaction()
        .assert(the!("x/status").of(e.clone()).is("triage".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch
        .transaction()
        .assert(the!("x/flag").of(e.clone()).is("set".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let flagged = |reads_cell: bool, to: &str| {
        let mut when = Vec::new();
        if reads_cell {
            when.push(serde_json::json!({
                "assert": { "with": { "status": { "the": "x/status", "as": "text:" } } },
                "where": { "this": { "?": { "name": "this" } }, "status": "triage" }
            }));
        }
        when.push(serde_json::json!({
            "assert": { "with": { "flag": { "the": "x/flag", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } }, "flag": { "?": { "name": "flag" } } }
        }));
        when.push(serde_json::json!({
            "assert": "==",
            "where": { "this": to, "is": { "?": { "name": "status" } } }
        }));
        compile(serde_json::json!({
            "deduce": { "with": { "status": { "the": "x/status", "as": "text:" } } },
            "when": when
        }))
    };

    let mut registry = RuleRegistry::new();
    registry.register(rewrite("triage", "todo")?)?;
    registry.register(flagged(true, "blocked")?)?;
    {
        let source = TestEnv::new(&branch, &operator, registry.clone());
        assert_eq!(
            relation_under(&source, "x/status", "one").await?,
            vec![(Value::Entity(e.clone()), Value::String("blocked".into()))],
            "the rule that read the newer fact wins, not the greater value"
        );
    }
    registry.register(flagged(false, "waiting")?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        relation_under(&source, "x/status", "one").await?,
        vec![(Value::Entity(e), Value::String("blocked".into()))],
        "at one version, the conclusion that rewrote the cell outranks one that did not read it"
    );
    Ok(())
}

/// Rewrites that read each other back are concurrent candidates.
/// `"triage"` to `"todo"` beside `"todo"` to `"triage"` over a stored
/// `"triage"`: each displaces the other, so no succession ranks them,
/// and the fixpoint converges with both as candidates of one rank. A
/// `last` read then settles them as it settles two concurrent writes,
/// by version, then value: `"triage"`. The cycle still ranks past a
/// value it displaced: over a stored `"old"` that a third rule rewrites
/// to `"triage"`, both members of the cycle beat `"old"`, and the cycle
/// settles the same way. An `all` read keeps every candidate.
#[dialog_common::test]
async fn a_cycle_of_rewrites_elects_as_concurrent_candidates() -> anyhow::Result<()> {
    let e: Entity = "id:e".parse()?;
    let triage_and_todo = |e: &Entity| {
        let mut both = vec![
            (Value::Entity(e.clone()), Value::String("triage".into())),
            (Value::Entity(e.clone()), Value::String("todo".into())),
        ];
        both.sort_by_key(|pair| format!("{pair:?}"));
        both
    };
    {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(the!("x/status").of(e.clone()).is("triage".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let mut registry = RuleRegistry::new();
        registry.register(rewrite("triage", "todo")?)?;
        registry.register(rewrite("todo", "triage")?)?;
        let source = TestEnv::new(&branch, &operator, registry);
        assert_eq!(
            relation_under(&source, "x/status", "one").await?,
            vec![(Value::Entity(e.clone()), Value::String("triage".into()))],
            "the cycle's members tie on rank and the value decides"
        );
        assert_eq!(
            relation_under(&source, "x/status", "many").await?,
            triage_and_todo(&e),
            "under `all` both members of the cycle are candidates"
        );
    }
    {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(the!("x/status").of(e.clone()).is("old".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let mut registry = RuleRegistry::new();
        registry.register(rewrite("old", "triage")?)?;
        registry.register(rewrite("triage", "todo")?)?;
        registry.register(rewrite("todo", "triage")?)?;
        let source = TestEnv::new(&branch, &operator, registry);
        assert_eq!(
            relation_under(&source, "x/status", "one").await?,
            vec![(Value::Entity(e.clone()), Value::String("triage".into()))],
            "the cycle ranks past the value it displaced, and settles by value"
        );
    }
    Ok(())
}

/// A rule reading its own relation under `last` recurses: `x/label(e)
/// := l :- x/next(e) = n, x/label(n) = l` carries the tail's stored
/// label down the chain `a -> b -> c -> tail`. Inside the cycle the
/// `last` read sees every candidate; readers elect once the cycle is
/// derived. This is how a notebook positions a run of inserted blocks
/// from each block's successor, so the rule is not set aside.
#[dialog_common::test]
async fn a_rule_recursing_through_a_last_read_derives_down_the_chain() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let chain: Vec<Entity> = ["id:a", "id:b", "id:c", "id:tail"]
        .iter()
        .map(|id| id.parse())
        .collect::<Result<_, _>>()?;
    let mut transaction = branch.transaction();
    for pair in chain.windows(2) {
        transaction = transaction.assert(the!("x/next").of(pair[0].clone()).is(pair[1].clone()));
    }
    transaction = transaction.assert(the!("x/label").of(chain[3].clone()).is("end".to_string()));
    transaction.commit().publish().perform(&operator).await?;

    let carry = compile(serde_json::json!({
        "deduce": { "with": { "label": { "the": "x/label", "as": "text:" } } },
        "when": [
            {
                "assert": { "with": { "next": { "the": "x/next", "as": "entity:" } } },
                "where": { "this": { "?": { "name": "this" } }, "next": { "?": { "name": "next" } } }
            },
            {
                "assert": { "with": { "label": { "the": "x/label", "as": "text:" } } },
                "where": { "this": { "?": { "name": "next" } }, "label": { "?": { "name": "label" } } }
            }
        ]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(carry)?;
    assert!(registry.analysis()?.quarantined().is_empty());
    let source = TestEnv::new(&branch, &operator, registry);
    let expected: Vec<(Value, Value)> = chain
        .iter()
        .map(|entity| (Value::Entity(entity.clone()), Value::String("end".into())))
        .collect();
    let mut expected = expected;
    expected.sort_by_key(|pair| format!("{pair:?}"));
    assert_eq!(
        relation_under(&source, "x/label", "one").await?,
        expected,
        "every block of the chain takes the tail's label"
    );
    Ok(())
}

/// `ok(p) := name :- name(p) = name, nickname(p) = ?nick, unless
/// banned(?nick)`, over enough people that the negation runs as a
/// bulk anti-join. The person scanned first has no nickname. The bulk
/// path decides which variables the anti-join keys on from the first
/// candidate alone, where `nick` is absent, so it keys on nothing: one
/// banned nickname then excludes everyone.
#[dialog_common::test]
async fn a_bulk_negation_keys_on_the_variables_each_candidate_binds() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let people = 20;
    let mut transaction = branch.transaction();
    for index in 0..people {
        let person: Entity = format!("id:p{index:02}").parse()?;
        transaction = transaction.assert(
            the!("person/name")
                .of(person.clone())
                .is(format!("person {index}")),
        );
        // The first person has no nickname; every other has their own.
        if index > 0 {
            let nick: Entity = format!("id:nick{index:02}").parse()?;
            transaction = transaction.assert(the!("person/nickname").of(person.clone()).is(nick));
        }
    }
    let banned: Entity = "id:nick07".parse()?;
    transaction = transaction.assert(the!("club/banned").of(banned).is("yes".to_string()));
    transaction.commit().publish().perform(&operator).await?;

    let ok = compile(serde_json::json!({
        "deduce": { "with": { "ok": { "the": "club/ok", "as": "text:" } } },
        "when": [{
            "assert": { "with": {
                "name": { "the": "person/name", "as": "text:" },
                "nickname": { "the": "person/nickname", "as": "entity:", "optional": true }
            }},
            "where": {
                "this": { "?": { "name": "this" } },
                "name": { "?": { "name": "ok" } },
                "nickname": { "?": { "name": "nick" } }
            }
        }],
        "unless": [{
            "assert": { "with": { "banned": { "the": "club/banned", "as": "text:" } } },
            "where": { "this": { "?": { "name": "nick" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(ok)?;
    let source = TestEnv::new(&branch, &operator, registry);
    let rows = relation_under(&source, "club/ok", "many").await?;
    assert_eq!(
        rows.len(),
        people - 1,
        "one nickname is banned, so one person is excluded: {rows:?}"
    );
    Ok(())
}

/// "One ordering decides every election, here and in the tree." Two
/// claims of a cell land in one commit, so they stand equal and the
/// tie falls to the value: a `last` concept read returns the greater
/// value. A cardinality-one attribute premise reads the same cell
/// through its own election, which breaks the tie by the hash of each
/// fact instead, so for some value pairs the two reads of one cell
/// under one cardinality disagree.
#[dialog_common::test]
async fn every_last_read_of_a_cell_elects_the_same_claim() -> anyhow::Result<()> {
    use crate::attribute::query::AttributeQuery;
    use crate::{Cardinality, Environment, Planner, Premise, Proposition};
    use futures_util::TryStreamExt as _;

    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let pairs = [
        (300u32, 400u32),
        (123, 321),
        (7, 8),
        (1000, 2000),
        (11, 12),
        (99, 100),
    ];
    let mut transaction = branch.transaction();
    for (index, (first, second)) in pairs.iter().enumerate() {
        let of: Entity = format!("id:e{index}").parse()?;
        transaction = transaction
            .assert(the!("org/salary").of(of.clone()).is(*first))
            .assert(the!("org/salary").of(of.clone()).is(*second));
    }
    transaction.commit().publish().perform(&operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let mut disagree = Vec::new();
    for (index, pair) in pairs.iter().enumerate() {
        let of: Entity = format!("id:e{index}").parse()?;
        let premise = Premise::Assert(Proposition::Attribute(Box::new(AttributeQuery::new(
            Term::from(the!("org/salary")),
            Term::<Entity>::from(of.clone()),
            Term::var("salary"),
            Term::blank(),
            Some(Cardinality::One),
        ))));
        let rows: Vec<Match> = Planner::from(vec![premise])
            .plan(&Environment::new())?
            .evaluate(Match::new().seed(), &source)
            .try_collect()
            .await?;
        let by_attribute: Vec<Value> = rows
            .iter()
            .map(|row| row.lookup(&Term::<Any>::var("salary"))?.content())
            .collect::<Result<_, crate::EvaluationError>>()?;

        let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
            "salary": { "the": "org/salary", "as": "natural:" }
        }}))?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::<Any>::constant(of.clone()));
        terms.insert("salary".into(), Term::var("salary"));
        let rows = ConceptQuery { terms, predicate }
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;
        let by_concept: Vec<Value> = rows
            .iter()
            .map(|row| row.lookup(&Term::<Any>::var("salary"))?.content())
            .collect::<Result<_, crate::EvaluationError>>()?;
        if by_attribute != by_concept {
            disagree.push((pair, by_attribute, by_concept));
        }
    }
    assert!(
        disagree.is_empty(),
        "a cardinality-one attribute read and a `last` concept read elect different claims of one cell: {disagree:?}"
    );
    Ok(())
}

/// A chain `a -> b -> tail` where the tail holds the label `end` and
/// `b` a label of its own written later, with the rule carrying a
/// label down the chain: `b` reads `own`, its newest candidate, and
/// holds `end` as a losing one.
async fn chain_with_a_losing_label(
    operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
    branch: &dialog_repository::Branch,
) -> anyhow::Result<()> {
    let a: Entity = "id:a".parse()?;
    let b: Entity = "id:b".parse()?;
    let tail: Entity = "id:tail".parse()?;
    branch
        .transaction()
        .assert(the!("x/label").of(tail.clone()).is("end".to_string()))
        .commit()
        .publish()
        .perform(operator)
        .await?;
    branch
        .transaction()
        .assert(the!("x/label").of(b.clone()).is("own".to_string()))
        .commit()
        .publish()
        .perform(operator)
        .await?;
    branch
        .transaction()
        .assert(the!("x/next").of(a.clone()).is(b.clone()))
        .assert(the!("x/next").of(b).is(tail))
        .assert(the!("x/mark").of(a).is("yes".to_string()))
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

fn next_premise() -> serde_json::Value {
    serde_json::json!({
        "assert": { "with": { "next": { "the": "x/next", "as": "entity:" } } },
        "where": { "this": { "?": { "name": "this" } }, "next": { "?": { "name": "next" } } }
    })
}

/// `x/label(e) := l :- x/next(e) = n, x/label(n) = l`.
fn carry_label() -> anyhow::Result<DeductiveRule> {
    compile(serde_json::json!({
        "deduce": { "with": { "label": { "the": "x/label", "as": "text:" } } },
        "when": [ next_premise(), {
            "assert": { "with": { "label": { "the": "x/label", "as": "text:" } } },
            "where": { "this": { "?": { "name": "next" } }, "label": { "?": { "name": "label" } } }
        }]
    }))
}

/// `<head>(e) := "saw-end" :- x/mark(e) = "yes", x/next(e) = n,
/// x/label(n) = "end"`: a rule deciding on the label its successor
/// reads.
fn saw_end(head: &str) -> anyhow::Result<DeductiveRule> {
    compile(serde_json::json!({
        "deduce": { "with": { "out": { "the": head, "as": "text:" } } },
        "when": [ next_premise(), {
            "assert": { "with": { "mark": { "the": "x/mark", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } }, "mark": "yes" }
        }, {
            "assert": { "with": { "label": { "the": "x/label", "as": "text:" } } },
            "where": { "this": { "?": { "name": "next" } }, "label": "end" }
        }, {
            "assert": "==",
            "where": { "this": "saw-end", "is": { "?": { "name": "out" } } }
        }]
    }))
}

/// "A rule deciding on a `last` value inside its own cycle is set
/// aside." `b` reads `own`, so a rule firing on `x/label(next) = "end"`
/// concludes nothing for `a` where its head is another relation. With
/// `x/label` as its head the rule sits in the cycle the carrying rule
/// makes, where a read sees every candidate, `end` among them: left in,
/// it concludes `saw-end` for `a` from a label no reader of `b` sees.
/// The analysis sets it aside, and `a` reads what the chain carries.
#[dialog_common::test]
async fn a_rule_deciding_on_a_last_value_inside_its_cycle_is_set_aside() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    chain_with_a_losing_label(&operator, &branch).await?;
    let a = Value::Entity("id:a".parse()?);

    let mut outside = RuleRegistry::new();
    outside.register(carry_label()?)?;
    outside.register(saw_end("x/flag")?)?;
    assert!(
        outside.analysis()?.quarantined().is_empty(),
        "outside the cycle the read is of a relation derived in full"
    );
    let source = TestEnv::new(&branch, &operator, outside);
    assert_eq!(
        relation_under(&source, "x/flag", "one").await?,
        vec![],
        "b reads `own`, so nothing saw `end`"
    );

    let decides = saw_end("x/label")?;
    let mut inside = RuleRegistry::new();
    inside.register(carry_label()?)?;
    inside.register(decides.clone())?;
    let analysis = inside.analysis()?;
    let [set_aside] = analysis.quarantined() else {
        anyhow::bail!("one rule is set aside: {:?}", analysis.quarantined());
    };
    assert_eq!(set_aside.rule, decides.try_this().expect("a storable rule"));
    assert_eq!(
        set_aside
            .tested
            .as_ref()
            .map(|tested| (&tested.by, tested.absence)),
        Some((
            &Some(decides.try_this().expect("a storable rule")),
            Absence::Decided
        )),
        "the record says which rule decided, and that it did"
    );
    let source = TestEnv::new(&branch, &operator, inside);
    let labels = relation_under(&source, "x/label", "one").await?;
    assert!(
        labels.contains(&(a.clone(), Value::String("own".into()))),
        "a reads the label the chain carries from b: {labels:?}"
    );
    assert!(
        !labels.contains(&(a, Value::String("saw-end".into()))),
        "nothing is concluded from a label no reader of b sees: {labels:?}"
    );
    Ok(())
}

/// "A rule passing a `last` value through a formula inside its cycle
/// is not set aside." `x/depth(e) := d + 1 :- x/next(e) = n,
/// x/depth(n) = d` computes from the value it reads and decides
/// nothing on it.
#[dialog_common::test]
async fn a_rule_computing_from_a_last_value_inside_its_cycle_stays() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let a: Entity = "id:a".parse()?;
    let b: Entity = "id:b".parse()?;
    let tail: Entity = "id:tail".parse()?;
    branch
        .transaction()
        .assert(the!("x/next").of(a.clone()).is(b.clone()))
        .assert(the!("x/next").of(b.clone()).is(tail.clone()))
        .assert(the!("x/depth").of(tail.clone()).is(0u32))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let deeper = compile(serde_json::json!({
        "deduce": { "with": { "depth": { "the": "x/depth", "as": "natural:" } } },
        "when": [ next_premise(), {
            "assert": { "with": { "depth": { "the": "x/depth", "as": "natural:" } } },
            "where": { "this": { "?": { "name": "next" } }, "depth": { "?": { "name": "below" } } }
        }, {
            "assert": "math/sum",
            "where": { "of": { "?": { "name": "below" } }, "with": 1, "is": { "?": { "name": "depth" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(deeper)?;
    assert!(registry.analysis()?.quarantined().is_empty());
    let source = TestEnv::new(&branch, &operator, registry);
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "value": { "the": "x/depth", "as": "natural:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::var("value"));
    let rows = ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
    let mut depths: Vec<(Value, Value)> = rows
        .iter()
        .map(|row| {
            Ok((
                row.lookup(&Term::<Any>::var("who"))?.content()?,
                row.lookup(&Term::<Any>::var("value"))?.content()?,
            ))
        })
        .collect::<anyhow::Result<_>>()?;
    depths.sort_by_key(|pair| format!("{pair:?}"));
    assert_eq!(
        depths,
        vec![
            (Value::Entity(a), Value::UnsignedInt(2)),
            (Value::Entity(b), Value::UnsignedInt(1)),
            (Value::Entity(tail), Value::UnsignedInt(0)),
        ]
    );
    Ok(())
}

/// Three employees in two departments: alice (100) and bob (50) in
/// `dept-a`, carol (7) in `dept-b`.
async fn staff(
    operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
    branch: &dialog_repository::Branch,
) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for (who, dept, salary) in [
        ("id:alice", "id:dept-a", 100u32),
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

/// Write `value` to `the` of `of`, added beside the cell's claims or
/// succeeding the newest, in a commit of its own.
async fn write(
    operator: &dialog_peer::Peer<VolatileSpace, dialog_peer::Session>,
    branch: &dialog_repository::Branch,
    the: &str,
    of: &Entity,
    value: &str,
    cardinality: crate::Cardinality,
) -> anyhow::Result<()> {
    use dialog_corpus::{Changes, Fact, Instruction};
    let fact = Fact {
        the: the.parse()?,
        of: of.clone(),
        is: Value::String(value.into()),
        cause: None,
    };
    let changes: Changes = [Instruction::Assert(fact, cardinality)]
        .into_iter()
        .collect();
    branch
        .transaction()
        .integrate(changes)
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

/// "Every state is kept, and the concept elects the status by rank."
/// An account's status is its states read under `top`, which ranks the
/// states its attribute lists from least to greatest: `active` stands
/// alone, `suspended` outranks it, and an `active` written after
/// `suspended` adds a claim but does not displace it. Un-suspending is
/// retracting `suspended`.
#[dialog_common::test]
async fn a_concept_elects_a_status_by_rank_over_the_states_kept() -> anyhow::Result<()> {
    use crate::Cardinality;
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let account: Entity = "id:account".parse()?;
    let concept: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "status": {
            "the": "account/state",
            "as": ["suspended", "active"],
            "pick": "top"
        }
    }}))?;
    let read = || async {
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        let mut terms = Parameters::new();
        terms.insert(
            "this".into(),
            Term::Constant(Value::Entity(account.clone())),
        );
        terms.insert("status".into(), Term::<Any>::var("status"));
        let rows = ConceptQuery {
            terms,
            predicate: concept.clone(),
        }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
        rows.iter()
            .map(|row| Ok(row.lookup(&Term::<Any>::var("status"))?.content()?))
            .collect::<anyhow::Result<Vec<Value>>>()
    };
    let status = |value: &str| vec![Value::String(value.to_string())];

    write(
        &operator,
        &branch,
        "account/state",
        &account,
        "active",
        Cardinality::Many,
    )
    .await?;
    assert_eq!(read().await?, status("active"), "the one state");
    write(
        &operator,
        &branch,
        "account/state",
        &account,
        "suspended",
        Cardinality::Many,
    )
    .await?;
    assert_eq!(
        read().await?,
        status("suspended"),
        "the better ranked state"
    );
    write(
        &operator,
        &branch,
        "account/state",
        &account,
        "active",
        Cardinality::Many,
    )
    .await?;
    assert_eq!(
        read().await?,
        status("suspended"),
        "a later, lesser state does not displace it"
    );
    Ok(())
}

/// "A rule deciding on a field of a concept inside its own cycle is set
/// aside." The same decision as
/// [`a_rule_deciding_on_a_last_value_inside_its_cycle_is_set_aside`],
/// read through a concept of two fields: `x/label(e) := "saw-end" :-
/// x/mark(e) = "yes", x/next(e) = n, Block(n, label: "end", next: _)`.
/// The concept elects each field's claim, so a premise pinning its
/// label tests that election as a read of the label alone does.
#[dialog_common::test]
async fn a_rule_deciding_on_a_concepts_field_inside_its_cycle_is_set_aside() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    chain_with_a_losing_label(&operator, &branch).await?;
    let a = Value::Entity("id:a".parse()?);
    let decides = compile(serde_json::json!({
        "deduce": { "with": { "out": { "the": "x/label", "as": "text:" } } },
        "when": [ next_premise(), {
            "assert": { "with": { "mark": { "the": "x/mark", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } }, "mark": "yes" }
        }, {
            "assert": { "with": {
                "label": { "the": "x/label", "as": "text:" },
                "next": { "the": "x/next", "as": "entity:" }
            }},
            "where": {
                "this": { "?": { "name": "next" } },
                "label": "end",
                "next": { "?": { "name": "after" } }
            }
        }, {
            "assert": "==",
            "where": { "this": "saw-end", "is": { "?": { "name": "out" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(carry_label()?)?;
    registry.register(decides.clone())?;
    let analysis = registry.analysis()?;
    let [set_aside] = analysis.quarantined() else {
        anyhow::bail!("one rule is set aside: {:?}", analysis.quarantined());
    };
    assert_eq!(set_aside.rule, decides.try_this().expect("a storable rule"));
    assert_eq!(
        set_aside.tested.as_ref().map(|tested| tested.absence),
        Some(Absence::Decided)
    );
    let source = TestEnv::new(&branch, &operator, registry);
    let labels = relation_under(&source, "x/label", "one").await?;
    assert!(
        labels.contains(&(a.clone(), Value::String("own".into()))),
        "a reads the label the chain carries from b: {labels:?}"
    );
    assert!(
        !labels.contains(&(a, Value::String("saw-end".into()))),
        "nothing is concluded from a label no reader of b sees: {labels:?}"
    );
    Ok(())
}

/// "A read that names a derived value is answered as one that does not,
/// filtered." `org.employee/outearned-by(e) := more :- salary(e) = mine,
/// dept(e) = d, dept(peer) = d, salary(peer) = more, mine < more` is
/// read for bob and the value `100`. The value a caller names is tested
/// against what the relation's cardinality keeps and never seeds the
/// body, so the body is planned with the value free: a comparison
/// against it waits for the premise that binds it.
#[dialog_common::test]
async fn a_read_naming_a_derived_value_plans_its_rule_with_the_value_free() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    staff(&operator, &branch).await?;
    let var = |name: &str| serde_json::json!({ "?": { "name": name } });
    let outearned = compile(serde_json::json!({
        "deduce": { "with": { "more": {
            "the": "org.employee/outearned-by", "as": "natural:", "cardinality": "many"
        } } },
        "when": [{
            "assert": { "with": { "salary": { "the": "org.employee/salary", "as": "natural:" } } },
            "where": { "this": var("this"), "salary": var("mine") }
        }, {
            "assert": { "with": { "dept": { "the": "org.employee/dept", "as": "entity:" } } },
            "where": { "this": var("this"), "dept": var("dept") }
        }, {
            "assert": { "with": { "dept": { "the": "org.employee/dept", "as": "entity:" } } },
            "where": { "this": var("peer"), "dept": var("dept") }
        }, {
            "assert": { "with": { "salary": { "the": "org.employee/salary", "as": "natural:" } } },
            "where": { "this": var("peer"), "salary": var("more") }
        }, {
            "assert": "<",
            "where": { "of": var("mine"), "with": var("more") }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(outearned)?;
    assert!(registry.analysis()?.quarantined().is_empty());
    let source = TestEnv::new(&branch, &operator, registry);
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "more": { "the": "org.employee/outearned-by", "as": "natural:", "cardinality": "many" }
    }}))?;
    let read = |who: &str, more: u32| {
        let mut terms = Parameters::new();
        terms.insert(
            "this".into(),
            Term::<Any>::constant(who.parse::<Entity>().expect("an entity parses")),
        );
        terms.insert("more".into(), Term::<Any>::constant(more));
        ConceptQuery {
            terms,
            predicate: predicate.clone(),
        }
    };
    let bob = read("id:bob", 100)
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
    assert_eq!(bob.len(), 1, "alice's 100 is more than bob's 50");
    let alice = read("id:alice", 50)
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
    assert!(alice.is_empty(), "nobody in the department outearns alice");
    Ok(())
}

/// "A concluded value is as recent as the newest fact it was concluded
/// from." `report/reports(m) := who :- report/manager(who) = m` derives
/// every report of a manager, and a field picking `last` over it reads
/// the report whose manager was written most recently, whichever
/// entity sorts first.
#[dialog_common::test]
async fn a_field_picking_last_over_a_derived_relation_reads_the_newest_derivation()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let entity = |name: &str| name.parse::<Entity>().expect("an entity parses");
    let var = |name: &str| serde_json::json!({ "?": { "name": name } });
    let reports = compile(serde_json::json!({
        "deduce": { "with": { "reports": {
            "the": "report/reports", "as": "entity:", "cardinality": "many"
        } } },
        "when": [{
            "assert": { "with": { "manager": { "the": "report/manager", "as": "entity:" } } },
            "where": { "this": var("reports"), "manager": var("this") }
        }]
    }))?;
    let headed: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "reports": { "the": "report/reports", "as": "entity:", "cardinality": "many", "pick": "last" }
    }}))?;
    // The later write names the entity that sorts first and the one
    // that sorts last in turn, so no order of entities passes for the
    // order of writes.
    for (first, second) in [("id:p0", "id:p1"), ("id:p1", "id:p0")] {
        let manager = entity(&format!("id:boss-of-{}", &second[3..]));
        for who in [first, second] {
            branch
                .transaction()
                .assert(the!("report/manager").of(entity(who)).is(manager.clone()))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        let mut registry = RuleRegistry::new();
        registry.register(reports.clone())?;
        let source = TestEnv::new(&branch, &operator, registry);
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::<Any>::constant(manager));
        terms.insert("reports".into(), Term::var("reports"));
        let rows = ConceptQuery {
            terms,
            predicate: headed.clone(),
        }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
        let read: Vec<Value> = rows
            .iter()
            .map(|row| row.lookup(&Term::<Any>::var("reports"))?.content())
            .collect::<Result<_, crate::EvaluationError>>()?;
        assert_eq!(
            read,
            vec![Value::Entity(entity(second))],
            "written {first} then {second}"
        );
    }
    Ok(())
}
