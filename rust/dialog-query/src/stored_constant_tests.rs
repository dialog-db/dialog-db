//! A typed read of a stored attribute whose value is bound to a
//! constant, pinned as tests.
//!
//! A field's declared type says which values it reads, so a value of
//! another type is no fact of the field: a read whose value is bound to
//! a constant its type does not read answers nothing, as the same read
//! with its value unbound answers no fact holding that constant. A
//! number is one value whatever number type holds it, so a constant of
//! another number type than the stored value still finds it.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::AttributeStatement;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::{ConceptDescriptor, ConceptQuery, Entity, Match, Parameters, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

/// The cardinalities each read is made under.
const CARDINALITIES: [Option<&str>; 3] = [None, Some("many"), Some("one")];

/// A field over `note/value` declared `as`, read under `cardinality`
/// and picking with `pick`.
fn field(declared: &str, cardinality: Option<&str>, pick: Option<&str>) -> serde_json::Value {
    let mut field = serde_json::json!({ "the": "note/value", "as": declared });
    if let Some(cardinality) = cardinality {
        field["cardinality"] = cardinality.into();
    }
    if let Some(pick) = pick {
        field["pick"] = pick.into();
    }
    field
}

/// How many rows a read of `field` answers with its value bound to
/// `constant` and its entity bound to `this`, or unbound when `None`.
async fn bound(
    source: &TestEnv<'_>,
    field: serde_json::Value,
    this: Option<&Entity>,
    constant: &Value,
) -> anyhow::Result<usize> {
    let predicate: ConceptDescriptor =
        serde_json::from_value(serde_json::json!({ "with": { "value": field } }))?;
    let mut terms = Parameters::new();
    terms.insert(
        "this".into(),
        match this {
            Some(of) => Term::Constant(Value::Entity(of.clone())),
            None => Term::var("who"),
        },
    );
    terms.insert("value".into(), Term::Constant(constant.clone()));
    Ok(ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), source)
        .try_vec()
        .await?
        .len())
}

/// Commits each of `values` under `note/value` of an entity of its own.
/// The entities, in that order.
async fn store(
    branch: &Branch,
    operator: &Operator,
    values: &[Value],
) -> anyhow::Result<Vec<Entity>> {
    let mut entities = Vec::new();
    let mut transaction = branch.transaction();
    for value in values {
        let of = Entity::new()?;
        entities.push(of.clone());
        transaction = transaction.assert(AttributeStatement {
            the: the!("note/value"),
            of,
            is: value.clone(),
            cause: None,
            cardinality: None,
        });
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(entities)
}

/// A stored text, integer and boolean, each of an entity of its own,
/// read by a field of a type that does not read the value its read is
/// bound to: no row, under each cardinality, with the entity unbound
/// and bound to the entity holding the value. A field of the value's
/// own type finds it.
#[dialog_common::test]
async fn a_typed_read_bound_to_a_constant_its_type_does_not_read_answers_nothing()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let stored = [
        (Value::String("hello".into()), "text:"),
        (Value::SignedInt(-3), "integer:"),
        (Value::Boolean(true), "boolean:"),
    ];
    let values: Vec<Value> = stored.iter().map(|(value, _)| value.clone()).collect();
    let entities = store(&branch, &operator, &values).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let mut wrong = Vec::new();
    for cardinality in CARDINALITIES {
        for ((constant, own), of) in stored.iter().zip(&entities) {
            for this in [None, Some(of)] {
                let how = if this.is_some() { "bound" } else { "unbound" };
                let rows = bound(&source, field(own, cardinality, None), this, constant).await?;
                if rows != 1 {
                    wrong.push(format!(
                        "{own} under {cardinality:?}, entity {how}, finds {rows} of {constant:?}"
                    ));
                }
                for declared in ["text:", "integer:", "boolean:", "entity:"] {
                    if declared == *own {
                        continue;
                    }
                    let rows =
                        bound(&source, field(declared, cardinality, None), this, constant).await?;
                    if rows != 0 {
                        wrong.push(format!(
                            "{declared} under {cardinality:?}, entity {how}, bound to {constant:?} answers {rows}"
                        ));
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}

/// A field that picks with `max` or `min` over a number type, bound to
/// a text or a boolean, answers nothing, with the entity unbound and
/// bound: a pick reads the values its type reads.
#[dialog_common::test]
async fn a_typed_pick_bound_to_a_constant_its_type_does_not_read_answers_nothing()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let values = [Value::String("hello".into()), Value::Boolean(true)];
    let entities = store(&branch, &operator, &values).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let mut wrong = Vec::new();
    for (constant, of) in values.iter().zip(&entities) {
        for this in [None, Some(of)] {
            for declared in ["natural:", "integer:", "float:"] {
                for pick in ["max", "min"] {
                    let rows =
                        bound(&source, field(declared, None, Some(pick)), this, constant).await?;
                    if rows != 0 {
                        wrong.push(format!(
                            "{declared} {pick}, entity bound {}, bound to {constant:?} answers {rows}",
                            this.is_some()
                        ));
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}

/// A constant of another number type than the stored number finds it,
/// as it does today: a `float:` field bound to the natural `3` finds a
/// stored natural `3`, and `natural:`, `integer:` and `number:` fields
/// bound to the float `3.0` find it too, under each cardinality, with
/// the entity unbound and bound, and through a `max` pick.
#[dialog_common::test]
async fn a_typed_read_bound_to_a_number_of_another_type_finds_the_stored_number()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let entities = store(&branch, &operator, &[Value::UnsignedInt(3)]).await?;
    let of = &entities[0];
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let mut wrong = Vec::new();
    for (declared, constant) in [
        ("float:", Value::UnsignedInt(3)),
        ("float:", Value::SignedInt(3)),
        ("natural:", Value::Float(3.0)),
        ("integer:", Value::Float(3.0)),
        ("number:", Value::Float(3.0)),
    ] {
        for this in [None, Some(of)] {
            let mut reads: Vec<serde_json::Value> = CARDINALITIES
                .iter()
                .map(|cardinality| field(declared, *cardinality, None))
                .collect();
            if declared != "number:" {
                reads.push(field(declared, None, Some("max")));
            }
            for read in reads {
                let rows = bound(&source, read.clone(), this, &constant).await?;
                if rows != 1 {
                    wrong.push(format!(
                        "{read}, entity bound {}, bound to {constant:?} finds {rows}",
                        this.is_some()
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}
