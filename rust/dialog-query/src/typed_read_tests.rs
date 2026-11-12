//! A typed variable over a field that declares no type, pinned as tests.
//!
//! A variable's type says which values it reads, and an attribute's
//! type is part of which attribute it is. So to a variable of a type a
//! value of another type is a fact of another attribute: the row that
//! holds it is passed over, it is not elected among, and an entity
//! that holds nothing else has, to a variable that may be absent, no
//! value. That holds whichever way the read is evaluated: as an
//! attribute scan, as a plain concept read, as a read of a concept a
//! rule derives, or as a read of a recursive concept.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::AttributeStatement;
use crate::attribute::query::all::AttributeQueryAll;
use crate::corpus::Type;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{Binding, ConceptDescriptor, ConceptQuery, Entity, Match, Parameters, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn text(text: &str) -> Value {
    Value::String(text.into())
}

/// `values` in one order on every target: by their debug rendering,
/// since [`Value`] has no total order.
fn sorted(mut values: Vec<Value>) -> Vec<Value> {
    values.sort_by_key(|value| format!("{value:?}"));
    values
}

/// A text, a natural, a negative integer and a boolean, each under
/// `mixed/value` of its own entity. The entities, in that order.
async fn store_mixed(branch: &Branch, operator: &Operator) -> anyhow::Result<Vec<Entity>> {
    let mut entities = Vec::new();
    let mut transaction = branch.transaction();
    for value in [
        text("hello"),
        Value::UnsignedInt(7),
        Value::SignedInt(-3),
        Value::Boolean(true),
    ] {
        let of = Entity::new()?;
        entities.push(of.clone());
        transaction = transaction.assert(AttributeStatement {
            the: the!("mixed/value"),
            of,
            is: value,
            cause: None,
            cardinality: None,
            pick: None,
        });
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(entities)
}

/// The concept of `mixed/value` alone, its field declaring no type,
/// read under `pick` (none for the default).
fn untyped(pick: Option<&str>) -> ConceptDescriptor {
    let mut field = serde_json::Map::new();
    field.insert("the".into(), "mixed/value".into());
    if let Some(pick) = pick {
        field.insert("pick".into(), pick.into());
    }
    serde_json::from_value(serde_json::json!({ "with": { "is": field } }))
        .expect("a one-field concept is well formed")
}

/// A registry whose one rule derives `mixed/value` from itself, which
/// makes the relation recursive.
fn recursive() -> anyhow::Result<RuleRegistry> {
    let field = serde_json::json!({ "the": "mixed/value", "pick": "all:" });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "is": field } },
        "when": [{
            "assert": { "with": { "is": field } },
            "where": {
                "this": { "?": { "name": "this" } },
                "is": { "?": { "name": "is" } }
            }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.compile()?)?;
    assert!(
        registry.analysis()?.quarantined().is_empty(),
        "the self-reading rule makes the relation recursive"
    );
    Ok(registry)
}

/// What an attribute scan of `mixed/value` reads into a variable of
/// `kind`, sorted.
async fn scanned(source: &TestEnv<'_>, kind: Kind) -> anyhow::Result<Vec<Value>> {
    let query = AttributeQueryAll::new(
        Term::from(the!("mixed/value")),
        Term::<Entity>::var("e"),
        Term::var("value").with_kind(kind),
        Term::var("cause"),
    );
    let claims = query.perform(source).try_vec().await?;
    Ok(sorted(
        claims.iter().map(|claim| claim.is().clone()).collect(),
    ))
}

/// What a read of the untyped concept gives a variable of `kind`,
/// sorted.
async fn read(source: &TestEnv<'_>, kind: Kind) -> anyhow::Result<Vec<Value>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("is".into(), Term::var("value").with_kind(kind));
    let rows = ConceptQuery {
        terms,
        predicate: untyped(Some("all:")),
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    let values = rows
        .iter()
        .map(|row| row.lookup(&Term::<Any>::var("value"))?.content())
        .collect::<Result<Vec<Value>, crate::EvaluationError>>()?;
    Ok(sorted(values))
}

/// A variable that reads a text or nothing.
fn optional_text() -> Term<Any> {
    Term::<Option<String>>::var("value").into()
}

/// What each row binds the optional variable to: `None` for absent.
fn optional_values(rows: &[Match]) -> anyhow::Result<Vec<Option<Value>>> {
    rows.iter()
        .map(|row| match row.lookup(&optional_text())? {
            Binding::Present(value) => Ok(Some(value)),
            Binding::Absent => Ok(None),
        })
        .collect()
}

/// What a read of `concept` for `of` gives the optional variable.
async fn read_optional(
    source: &TestEnv<'_>,
    concept: ConceptDescriptor,
    of: &Entity,
) -> anyhow::Result<Vec<Option<Value>>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::Constant(Value::Entity(of.clone())));
    terms.insert("is".into(), optional_text());
    let rows = ConceptQuery {
        terms,
        predicate: concept,
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    optional_values(&rows)
}

/// What each kind of variable reads of the four stored values.
fn cases() -> Vec<(Kind, Vec<Value>)> {
    vec![
        (Kind::from(Type::String), vec![text("hello")]),
        (Kind::from(Type::UnsignedInt), vec![Value::UnsignedInt(7)]),
        (Kind::from(Type::Boolean), vec![Value::Boolean(true)]),
    ]
}

/// A variable of a type reads the values of that type and passes over
/// the others, through a scan of the attribute and through a concept
/// whose field declares no type alike.
#[dialog_common::test]
async fn it_reads_the_values_of_its_type_through_an_untyped_field() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_mixed(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for (kind, expected) in cases() {
        assert_eq!(
            scanned(&source, kind.clone()).await?,
            sorted(expected.clone()),
            "a scan into {kind}"
        );
        assert_eq!(
            read(&source, kind.clone()).await?,
            sorted(expected),
            "a concept read into {kind}"
        );
    }
    Ok(())
}

/// The same holds when the concept is recursive: a rule that derives
/// the field from itself sends the read through the fixpoint, and the
/// variable reads what it reads without it.
#[dialog_common::test]
async fn it_reads_the_values_of_its_type_through_a_recursive_untyped_field() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_mixed(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, recursive()?);

    for (kind, expected) in cases() {
        assert_eq!(
            read(&source, kind.clone()).await?,
            sorted(expected),
            "a recursive concept read into {kind}"
        );
    }
    Ok(())
}

/// A variable that reads a text or nothing is absent for an entity
/// whose only value is a number, as it is for one with no value: to
/// the variable neither has a text. Under the stored rows as they are
/// (`all:` and the default), under a pick that elects on the way out
/// (`max:`), and through the fixpoint.
#[dialog_common::test]
async fn it_reads_a_value_of_another_type_as_absent_when_it_may_be() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let entities = store_mixed(&branch, &operator).await?;
    let (hello, seven) = (&entities[0], &entities[1]);
    let nobody = Entity::new()?;
    let plain = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let recursive = TestEnv::new(&branch, &operator, recursive()?);

    let reads = [
        (&plain, Some("all:"), "all:"),
        (&plain, None, "the default pick"),
        (&plain, Some("max:"), "max:"),
        (&recursive, Some("all:"), "the fixpoint"),
    ];
    for (source, pick, how) in reads {
        assert_eq!(
            read_optional(source, untyped(pick), hello).await?,
            vec![Some(text("hello"))],
            "the text under {how}"
        );
        assert_eq!(
            read_optional(source, untyped(pick), &nobody).await?,
            vec![None],
            "no value under {how}"
        );
        assert_eq!(
            read_optional(source, untyped(pick), seven).await?,
            vec![None],
            "the number under {how}"
        );
    }
    Ok(())
}

/// The same through a concept a rule derives, where an entity's values
/// are elected among: `derived/value` is whatever `source/a` and
/// `source/b` hold. A variable reads the greatest of the values of its
/// type, not the greatest of all when that is of another, and one that
/// may be absent is absent for an entity with none of its type.
#[dialog_common::test]
async fn it_elects_among_the_values_of_its_type_in_a_derived_field() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let (both, number) = (Entity::new()?, Entity::new()?);
    let mut transaction = branch.transaction();
    for (the, of, is) in [
        (the!("source/a"), &both, text("hello")),
        (the!("source/b"), &both, Value::UnsignedInt(7)),
        (the!("source/b"), &number, Value::UnsignedInt(9)),
    ] {
        transaction = transaction.assert(AttributeStatement {
            the,
            of: of.clone(),
            is,
            cause: None,
            cardinality: None,
            pick: None,
        });
    }
    transaction.commit().publish().perform(&operator).await?;

    let derived = serde_json::json!({ "the": "derived/value", "pick": "max:" });
    let mut registry = RuleRegistry::new();
    for from in ["source/a", "source/b"] {
        let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "is": derived } },
            "when": [{
                "assert": { "with": { "is": { "the": from, "pick": "all:" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "is": { "?": { "name": "is" } }
                }
            }]
        }))?;
        registry.register(rule.compile()?)?;
    }
    let source = TestEnv::new(&branch, &operator, registry);
    let concept = || -> ConceptDescriptor {
        serde_json::from_value(serde_json::json!({ "with": { "is": derived } }))
            .expect("a one-field concept is well formed")
    };

    let read = async |kind: Kind| -> anyhow::Result<Vec<Value>> {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::Constant(Value::Entity(both.clone())));
        terms.insert("is".into(), Term::var("value").with_kind(kind));
        let rows = ConceptQuery {
            terms,
            predicate: concept(),
        }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
        Ok(rows
            .iter()
            .map(|row| row.lookup(&Term::<Any>::var("value"))?.content())
            .collect::<Result<Vec<Value>, crate::EvaluationError>>()?)
    };
    assert_eq!(read(Kind::from(Type::String)).await?, vec![text("hello")]);
    assert_eq!(
        read(Kind::from(Type::UnsignedInt)).await?,
        vec![Value::UnsignedInt(7)]
    );

    assert_eq!(
        read_optional(&source, concept(), &both).await?,
        vec![Some(text("hello"))],
        "the text of an entity that has a number too"
    );
    assert_eq!(
        read_optional(&source, concept(), &number).await?,
        vec![None],
        "an entity that has a number alone"
    );
    Ok(())
}

/// One variable in two places reads the rows where the two hold one
/// value and passes over the others: an entity whose value is itself
/// is read, and one whose value is anything else is not.
#[dialog_common::test]
async fn it_passes_over_a_row_one_variable_cannot_hold_twice() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_mixed(&branch, &operator).await?;
    let own = Entity::new()?;
    branch
        .transaction()
        .assert(AttributeStatement {
            the: the!("mixed/value"),
            of: own.clone(),
            is: Value::Entity(own.clone()),
            cause: None,
            cardinality: None,
            pick: None,
        })
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("x"));
    terms.insert("is".into(), Term::var("x"));
    let rows = ConceptQuery {
        terms,
        predicate: untyped(Some("all:")),
    }
    .evaluate(Match::new().seed(), &source)
    .try_vec()
    .await?;
    let values = rows
        .iter()
        .map(|row| row.lookup(&Term::<Any>::var("x"))?.content())
        .collect::<Result<Vec<Value>, crate::EvaluationError>>()?;
    assert_eq!(values, vec![Value::Entity(own)]);
    Ok(())
}
