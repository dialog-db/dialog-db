//! A typed read of a stored attribute whose value is bound to a
//! constant, pinned as tests.
//!
//! A field's declared type says which values it reads, so a value of
//! another type is no fact of the field: a read whose value is bound to
//! a constant its type does not read answers nothing, as the same read
//! with its value unbound answers no fact holding that constant.

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

/// How many rows a read of `note/value` declared `as` and read under
/// `cardinality` answers with its value bound to `constant`.
async fn bound(
    source: &TestEnv<'_>,
    declared: &str,
    cardinality: Option<&str>,
    constant: &Value,
) -> anyhow::Result<usize> {
    let mut field = serde_json::json!({ "the": "note/value", "as": declared });
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

/// A stored text, integer and boolean, each of an entity of its own,
/// read by a field of a type that does not read the value its read is
/// bound to: no row, under each cardinality. A field of the value's own
/// type finds it.
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
    let mut transaction = branch.transaction();
    for (value, _) in &stored {
        transaction = transaction.assert(AttributeStatement {
            the: the!("note/value"),
            of: Entity::new()?,
            is: value.clone(),
            cause: None,
            cardinality: None,
        });
    }
    transaction.commit().publish().perform(&operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let mut wrong = Vec::new();
    for cardinality in [None, Some("many"), Some("one")] {
        for (constant, own) in &stored {
            if bound(&source, own, cardinality, constant).await? != 1 {
                wrong.push(format!("{own} under {cardinality:?} misses {constant:?}"));
            }
            for declared in ["text:", "integer:", "boolean:", "entity:"] {
                if declared == *own {
                    continue;
                }
                let rows = bound(&source, declared, cardinality, constant).await?;
                if rows != 0 {
                    wrong.push(format!(
                        "{declared} under {cardinality:?} bound to {constant:?} answers {rows}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}
