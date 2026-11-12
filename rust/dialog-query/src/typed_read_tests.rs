//! A typed variable over a field that declares no type, pinned as tests.
//!
//! A variable's type says which values it reads. A value of another
//! type is one the variable does not read, so the row that holds it is
//! passed over, whichever way the read is evaluated: as an attribute
//! scan, as a concept read, or as a read of a recursive concept.

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
use crate::{ConceptDescriptor, ConceptQuery, Entity, Match, Parameters, Term, Value};
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
/// `mixed/value` of its own entity.
async fn store_mixed(branch: &Branch, operator: &Operator) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for value in [
        text("hello"),
        Value::UnsignedInt(7),
        Value::SignedInt(-3),
        Value::Boolean(true),
    ] {
        transaction = transaction.assert(AttributeStatement {
            the: the!("mixed/value"),
            of: Entity::new()?,
            is: value,
            cause: None,
            cardinality: None,
            pick: None,
        });
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// A one-field concept over `mixed/value` that declares no type.
fn untyped() -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": { "value": { "the": "mixed/value", "pick": "all:" } }
    }))
    .expect("a one-field concept is well formed")
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
    terms.insert("value".into(), Term::var("value").with_kind(kind));
    let rows = ConceptQuery {
        terms,
        predicate: untyped(),
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

    let field = serde_json::json!({ "the": "mixed/value", "pick": "all:" });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "value": field } },
        "when": [{
            "assert": { "with": { "value": field } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.compile()?)?;
    assert!(
        registry.analysis()?.quarantined().is_empty(),
        "the self-reading rule makes the relation recursive"
    );
    let source = TestEnv::new(&branch, &operator, registry);

    for (kind, expected) in cases() {
        assert_eq!(
            read(&source, kind.clone()).await?,
            sorted(expected),
            "a recursive concept read into {kind}"
        );
    }
    Ok(())
}
