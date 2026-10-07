//! Invariants the attribute-heads design claims, pinned as tests.
//!
//! Each test states a property `notes/attribute-heads.md` or the
//! changelog asserts, and checks it the way a reader of those notes
//! would. A failing test here is a place where the engine does not do
//! what the design says.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::types::Any;
use crate::{ConceptDescriptor, ConceptQuery, DeductiveRule, Match, Parameters, Term, Value};
use dialog_artifacts::Entity;
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_storage::provider::storage::VolatileSpace;

fn compile(json: serde_json::Value) -> anyhow::Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json)?;
    Ok(descriptor.compile()?)
}

/// The `(group, role)` pairs a concept over `member/group` and
/// `member/role` yields for `of`, with both fields read under `select`,
/// sorted.
async fn member_rows(
    source: &TestEnv<'_>,
    of: &Entity,
    select: &str,
) -> anyhow::Result<Vec<(Value, Value)>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "group": { "the": "member/group", "as": "Entity", "select": select },
        "role": { "the": "member/role", "as": "Text", "select": select }
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
            "group": { "the": "member/group", "as": "Entity" },
            "role": { "the": "member/role", "as": "Text" }
        }},
        "when": [{
            "assert": { "with": {
                "person": { "the": "membership/person", "as": "Entity" },
                "group": { "the": "membership/group", "as": "Entity" },
                "role": { "the": "membership/role", "as": "Text" }
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
    assert_eq!(member_rows(&source, &alice, "all").await?, expected);
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
        member_rows(&source, &alice, "last").await?,
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
/// the default policy, `last`, they do: the covering rule yields the
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
        member_rows(&source, &alice, "last").await?
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
        member_rows(&source, &alice, "last").await?
    };
    assert_eq!(
        after, before,
        "alice's rows under `last` do not depend on a stored fact about bob"
    );
    Ok(())
}

/// `x/p(e) :- x/q(e), unless x/r(e)` and `x/r(e) :- x/p(e)`. The
/// changelog says that inside a recursive component "a negation holds".
/// Whatever reading one takes of the cycle, a *stored* `x/r` fact is
/// not something the fixpoint is still deriving: an entity with a
/// stored `x/r` is excluded by the rule as written, and must not come
/// out as `x/p`. The cycle policy drops the premise entirely and
/// derives `p` for it anyway, so the result violates the rule's own
/// `unless` against a fact that was in the store before any rule ran.
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
        "deduce": { "with": { "p": { "the": "x/p", "as": "Text" } } },
        "when": [{
            "assert": { "with": { "q": { "the": "x/q", "as": "Text" } } },
            "where": { "this": { "?": { "name": "this" } }, "q": { "?": { "name": "p" } } }
        }],
        "unless": [{
            "assert": { "with": { "r": { "the": "x/r", "as": "Text" } } },
            "where": { "this": { "?": { "name": "this" } } }
        }]
    }))?;
    let r = compile(serde_json::json!({
        "deduce": { "with": { "r": { "the": "x/r", "as": "Text" } } },
        "when": [{
            "assert": { "with": { "p": { "the": "x/p", "as": "Text" } } },
            "where": { "this": { "?": { "name": "this" } }, "p": { "?": { "name": "r" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(p)?;
    registry.register(r)?;
    let source = TestEnv::new(&branch, &operator, registry);

    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "p": { "the": "x/p", "as": "Text" }
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
/// read under `select`: `(this, value)` pairs, sorted.
async fn relation_under(
    source: &TestEnv<'_>,
    the: &str,
    select: &str,
) -> anyhow::Result<Vec<(Value, Value)>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "value": { "the": the, "as": "Text", "select": select }
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
/// `x/r(e) :- x/s(e)`, closes a cycle through the negation, and the
/// cycle policy then reads it as holding: `p` gains `b`. The meaning
/// of a rule nobody edited changes with an install elsewhere, which a
/// replica merge can do at any time.
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
            "deduce": { "with": { "v": { "the": head, "as": "Text" } } },
            "when": [{
                "assert": { "with": { "v": { "the": from, "as": "Text" } } },
                "where": { "this": { "?": { "name": "this" } }, "v": { "?": { "name": "v" } } }
            }]
        }))
    };
    let p = compile(serde_json::json!({
        "deduce": { "with": { "p": { "the": "x/p", "as": "Text" } } },
        "when": [{
            "assert": { "with": { "q": { "the": "x/q", "as": "Text" } } },
            "where": { "this": { "?": { "name": "this" } }, "q": { "?": { "name": "p" } } }
        }],
        "unless": [{
            "assert": { "with": { "r": { "the": "x/r", "as": "Text" } } },
            "where": { "this": { "?": { "name": "this" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(p)?;
    let before = {
        let source = TestEnv::new(&branch, &operator, registry.clone());
        relation_under(&source, "x/p", "all").await?
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
        relation_under(&source, "x/p", "all").await?,
        before,
        "two rules that read neither q nor r's stored facts do not change what p means"
    );
    Ok(())
}

/// `ancestor(this, a) :- parent(this, a)` and `ancestor(this, a) :-
/// parent(this, p), ancestor(p, a)`, read through a field under the
/// default policy, `last`. The design says election "runs once, over
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
        "deduce": { "with": { "ancestor": { "the": "family/ancestor", "as": "Entity" } } },
        "when": [{
            "assert": { "with": { "parent": { "the": "family/parent", "as": "Entity" } } },
            "where": { "this": { "?": { "name": "this" } }, "parent": { "?": { "name": "ancestor" } } }
        }]
    }))?;
    let step = compile(serde_json::json!({
        "deduce": { "with": { "ancestor": { "the": "family/ancestor", "as": "Entity" } } },
        "when": [
            {
                "assert": { "with": { "parent": { "the": "family/parent", "as": "Entity" } } },
                "where": { "this": { "?": { "name": "this" } }, "parent": { "?": { "name": "p" } } }
            },
            {
                "assert": { "with": { "ancestor": { "the": "family/ancestor", "as": "Entity" } } },
                "where": { "this": { "?": { "name": "p" } }, "ancestor": { "?": { "name": "ancestor" } } }
            }
        ]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(base)?;
    registry.register(step)?;
    let source = TestEnv::new(&branch, &operator, registry);

    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "ancestor": { "the": "family/ancestor", "as": "Entity", "select": "last" }
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
/// := v :- x/a(e) = v`, goes through the fixpoint, which drops every
/// row's standing and elects nothing under `last` at its exit: the
/// read returns the stored and the derived value both.
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
            "deduce": { "with": { "a": { "the": "x/a", "as": "Text" } } },
            "when": [{
                "assert": { "with": { "v": { "the": from, "as": "Text" } } },
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
            relation_under(&source, "x/a", "last").await?,
            expected,
            "the derived value is newer than the stored claim"
        );
    }
    registry.register(derive("x/a")?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        relation_under(&source, "x/a", "last").await?,
        expected,
        "a rule reading the relation back changes nothing about which value is newest"
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
        "deduce": { "with": { "ok": { "the": "club/ok", "as": "Text" } } },
        "when": [{
            "assert": { "with": {
                "name": { "the": "person/name", "as": "Text" },
                "nickname": { "the": "person/nickname", "as": "Entity", "optional": true }
            }},
            "where": {
                "this": { "?": { "name": "this" } },
                "name": { "?": { "name": "ok" } },
                "nickname": { "?": { "name": "nick" } }
            }
        }],
        "unless": [{
            "assert": { "with": { "banned": { "the": "club/banned", "as": "Text" } } },
            "where": { "this": { "?": { "name": "nick" } } }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(ok)?;
    let source = TestEnv::new(&branch, &operator, registry);
    let rows = relation_under(&source, "club/ok", "all").await?;
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
/// under one policy disagree.
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
            "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": "last" }
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
