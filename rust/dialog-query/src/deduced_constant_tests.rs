//! A read of a deduced relation whose value is bound to a number
//! constant, pinned as tests.
//!
//! A number is one value whatever number type holds it: a stored scan
//! bound to the natural `3` finds the float `3.0` a field reads, and a
//! scan bound to `3.0` finds the natural `3`. A read of a relation a
//! rule deduces finds the same facts as the identical read of a stored
//! relation holding the same numbers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::AttributeStatement;
use crate::corpus::Type;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::type_system::Type as Kind;
use crate::{ConceptDescriptor, ConceptQuery, Entity, Match, Parameters, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};

/// How many rows a read of `the` declared `as` and read under
/// `cardinality` answers with its value bound to `constant`.
async fn bound(
    source: &TestEnv<'_>,
    the: &str,
    declared: &str,
    cardinality: Option<&str>,
    constant: &Value,
) -> anyhow::Result<usize> {
    let mut field = serde_json::json!({ "the": the, "as": declared });
    if let Some(cardinality) = cardinality {
        field["cardinality"] = cardinality.into();
    }
    let predicate: ConceptDescriptor =
        serde_json::from_value(serde_json::json!({ "with": { "value": field } }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::Constant(constant.clone()));
    Ok(ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), source)
        .try_vec()
        .await?
        .len())
}

/// The type a field declared `as` holds.
fn declared_type(declared: &str) -> anyhow::Result<Type> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({
        "with": { "value": { "the": "twin/stored", "as": declared } }
    }))?;
    predicate
        .with()
        .iter()
        .next()
        .and_then(|(_, field)| field.content_type())
        .ok_or_else(|| anyhow::anyhow!("{declared} declares a type"))
}

/// The natural `3`, the integer `-3` and the float `2.5`, each of an
/// entity of its own, stored under `twin/stored` and deduced under
/// `twin/deduced`. A read of each number type bound to each number
/// constant finds as many rows in the deduced relation as in the stored
/// one, under each cardinality. A constant the field's type does not
/// read (`-3` under `natural:`) is left to `stored_constant_tests`: the
/// stored read answers it there, wrongly.
#[dialog_common::test]
async fn a_deduced_read_bound_to_a_number_finds_what_a_stored_read_finds() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut transaction = branch.transaction();
    for value in [
        Value::UnsignedInt(3),
        Value::SignedInt(-3),
        Value::Float(2.5),
    ] {
        let of = Entity::new()?;
        for the in [the!("twin/stored"), the!("twin/source")] {
            transaction = transaction.assert(AttributeStatement {
                the,
                of: of.clone(),
                is: value.clone(),
                cause: None,
                cardinality: None,
            });
        }
    }
    transaction.commit().publish().perform(&operator).await?;
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "value": { "the": "twin/deduced", "cardinality": "many" } } },
        "when": [{
            "assert": { "with": { "value": { "the": "twin/source", "cardinality": "many" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.compile()?)?;
    let source = TestEnv::new(&branch, &operator, registry);

    let mut wrong = Vec::new();
    for cardinality in [None, Some("many"), Some("one")] {
        for declared in ["natural:", "integer:", "float:", "number:"] {
            for constant in [
                Value::UnsignedInt(3),
                Value::SignedInt(3),
                Value::Float(3.0),
                Value::SignedInt(-3),
                Value::Float(-3.0),
                Value::Float(2.5),
            ] {
                if !Kind::from(declared_type(declared)?).reads(&constant) {
                    continue;
                }
                let stored =
                    bound(&source, "twin/stored", declared, cardinality, &constant).await?;
                let deduced =
                    bound(&source, "twin/deduced", declared, cardinality, &constant).await?;
                if stored != deduced {
                    wrong.push(format!(
                        "{declared} under {cardinality:?} bound to {constant:?}: stored {stored}, deduced {deduced}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}

/// The text `"3"`, the boolean `true` and an entity, each of an entity
/// of its own, stored and deduced. A number constant finds none of them,
/// in either relation, through a field of no type or of a number type;
/// each finds itself through a field of its own type.
#[dialog_common::test]
async fn a_number_constant_finds_no_value_that_is_not_a_number() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let held = [
        (Value::String("3".into()), "text:"),
        (Value::Boolean(true), "boolean:"),
        (Value::Entity(Entity::new()?), "entity:"),
    ];
    let mut transaction = branch.transaction();
    for (value, _) in &held {
        let of = Entity::new()?;
        for the in [the!("twin/stored"), the!("twin/source")] {
            transaction = transaction.assert(AttributeStatement {
                the,
                of: of.clone(),
                is: value.clone(),
                cause: None,
                cardinality: None,
            });
        }
    }
    transaction.commit().publish().perform(&operator).await?;
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "value": { "the": "twin/deduced", "cardinality": "many" } } },
        "when": [{
            "assert": { "with": { "value": { "the": "twin/source", "cardinality": "many" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.compile()?)?;
    let source = TestEnv::new(&branch, &operator, registry);

    let mut wrong = Vec::new();
    for the in ["twin/stored", "twin/deduced"] {
        for cardinality in [None, Some("many"), Some("one")] {
            for (value, own) in &held {
                if bound(&source, the, own, cardinality, value).await? != 1 {
                    wrong.push(format!(
                        "{the} {own} under {cardinality:?} misses {value:?}"
                    ));
                }
            }
            for constant in [
                Value::UnsignedInt(3),
                Value::SignedInt(3),
                Value::Float(3.0),
                Value::UnsignedInt(1),
            ] {
                for declared in [None, Some("natural:"), Some("integer:"), Some("float:")] {
                    let rows = untyped_or(&source, the, declared, cardinality, &constant).await?;
                    if rows != 0 {
                        wrong.push(format!(
                            "{the} {declared:?} under {cardinality:?} bound to {constant:?} answers {rows}"
                        ));
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}

/// [`bound`], through a field of no type when `declared` is `None`.
async fn untyped_or(
    source: &TestEnv<'_>,
    the: &str,
    declared: Option<&str>,
    cardinality: Option<&str>,
    constant: &Value,
) -> anyhow::Result<usize> {
    if let Some(declared) = declared {
        return bound(source, the, declared, cardinality, constant).await;
    }
    let mut field = serde_json::json!({ "the": the });
    if let Some(cardinality) = cardinality {
        field["cardinality"] = cardinality.into();
    }
    let predicate: ConceptDescriptor =
        serde_json::from_value(serde_json::json!({ "with": { "value": field } }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::Constant(constant.clone()));
    Ok(ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), source)
        .try_vec()
        .await?
        .len())
}
