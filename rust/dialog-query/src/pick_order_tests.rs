//! The order `max` and `min` pick numbers in, pinned as tests.
//!
//! Numbers are one stratum, keyed by value whatever type wrote them, so
//! a pick over numbers orders them by value: the greatest is the largest
//! number and the least the smallest, whatever types they decode as. A
//! text orders by its characters and a listed type by its listing, as
//! before. Each read is made of a stored relation and of one a rule
//! copies from a stored source, under each cardinality a pick reads
//! under.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::Conclusion as _;
use crate::attribute::{AttributeStatement, Relation};
use crate::corpus::CorpusRelation;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::reduce::{Aggregator, Reduce, ReduceEntry};
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::types::Any;
use crate::{
    AttributeQuery, Cardinality, ConceptDescriptor, ConceptQuery, Entity, Fold, Match, Parameters,
    Term, Value,
};
use dialog_peer::helpers::{test_repo, test_session_with_peer};

fn text(text: &str) -> Value {
    Value::String(text.into())
}

/// A field over `the` declared `as` (`Json::Null` for no type), picking
/// with `pick` under `cardinality`.
fn field(
    the: &str,
    declared: &serde_json::Value,
    pick: &str,
    cardinality: Option<&str>,
) -> serde_json::Value {
    let mut field = serde_json::json!({ "the": the, "pick": pick });
    if !declared.is_null() {
        field["as"] = declared.clone();
    }
    if let Some(cardinality) = cardinality {
        field["cardinality"] = cardinality.into();
    }
    field
}

/// What a read of `field` answers for `of`, each value with its type.
async fn picked(
    source: &TestEnv<'_>,
    field: serde_json::Value,
    of: &Entity,
) -> anyhow::Result<Vec<String>> {
    let predicate: ConceptDescriptor =
        serde_json::from_value(serde_json::json!({ "with": { "value": field } }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::Constant(Value::Entity(of.clone())));
    terms.insert("value".into(), Term::var("value"));
    let rows = ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), source)
        .try_vec()
        .await?;
    rows.iter()
        .map(|row| {
            Ok(format!(
                "{:?}",
                row.lookup(&Term::<Any>::var("value"))?.content()?
            ))
        })
        .collect()
}

/// A case: the field's declared type, the values one entity holds, and
/// what `max` and `min` answer.
struct Case {
    declared: serde_json::Value,
    values: Vec<Value>,
    max: Value,
    min: Value,
}

fn cases() -> Vec<Case> {
    let mixed = || {
        vec![
            Value::UnsignedInt(3),
            Value::SignedInt(-5),
            Value::Float(2.5),
            Value::UnsignedInt(10),
        ]
    };
    vec![
        Case {
            declared: serde_json::Value::Null,
            values: mixed(),
            max: Value::UnsignedInt(10),
            min: Value::SignedInt(-5),
        },
        Case {
            declared: "number:".into(),
            values: mixed(),
            max: Value::UnsignedInt(10),
            min: Value::SignedInt(-5),
        },
        Case {
            declared: "float:".into(),
            values: mixed(),
            max: Value::Float(10.0),
            min: Value::Float(-5.0),
        },
        Case {
            declared: "integer:".into(),
            values: vec![
                Value::UnsignedInt(3),
                Value::SignedInt(-5),
                Value::UnsignedInt(10),
            ],
            max: Value::SignedInt(10),
            min: Value::SignedInt(-5),
        },
        Case {
            declared: "natural:".into(),
            values: vec![
                Value::UnsignedInt(3),
                Value::Float(2.5),
                Value::UnsignedInt(10),
            ],
            max: Value::UnsignedInt(10),
            min: Value::UnsignedInt(2),
        },
        Case {
            declared: "text:".into(),
            values: vec![text("banana"), text("pear"), text("apple")],
            max: text("pear"),
            min: text("apple"),
        },
        Case {
            declared: serde_json::json!(["low", "medium", "high"]),
            values: vec![text("high"), text("low"), text("medium")],
            max: text("high"),
            min: text("low"),
        },
    ]
}

/// `max` and `min` pick numbers by value, stored and deduced, under the
/// default cardinality and under many: over mixed naturals, integers and
/// floats through a field of no type, a `number:` field and each typed
/// number field. A text and a listed type keep their order.
#[dialog_common::test]
async fn a_pick_orders_numbers_by_value() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let cases = cases();
    let mut entities = Vec::new();
    let mut transaction = branch.transaction();
    for (index, case) in cases.iter().enumerate() {
        let of = Entity::new()?;
        for value in &case.values {
            for prefix in ["stored", "source"] {
                transaction = transaction.assert(AttributeStatement {
                    the: Relation::from(format!("{prefix}/case{index}").parse::<CorpusRelation>()?),
                    of: of.clone(),
                    is: value.clone(),
                    cause: None,
                    cardinality: Some(Cardinality::Many),
                });
            }
        }
        entities.push(of);
    }
    transaction.commit().publish().perform(&operator).await?;
    let mut registry = RuleRegistry::new();
    for index in 0..cases.len() {
        let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "value": { "the": format!("deduced/case{index}"), "cardinality": "many" } } },
            "when": [{
                "assert": { "with": { "value": { "the": format!("source/case{index}"), "cardinality": "many" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "value": { "?": { "name": "value" } }
                }
            }]
        }))?;
        registry.register(rule.compile()?)?;
    }
    let source = TestEnv::new(&branch, &operator, registry);

    let mut wrong = Vec::new();
    for (index, (case, of)) in cases.iter().zip(&entities).enumerate() {
        for side in ["stored", "deduced"] {
            let the = format!("{side}/case{index}");
            for cardinality in [None, Some("many")] {
                for (pick, expected) in [("max", &case.max), ("min", &case.min)] {
                    let read = field(&the, &case.declared, pick, cardinality);
                    let got = picked(&source, read, of).await?;
                    let expected = vec![format!("{expected:?}")];
                    if got != expected {
                        wrong.push(format!(
                            "{side} {} {pick} under {cardinality:?} of {:?}: expected {expected:?}, got {got:?}",
                            case.declared, case.values
                        ));
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}

/// What `max` and `min` fold `inputs` to for `of`, grouped by the
/// entities `the` holds a value for: the fold of a rule's `reduce`.
async fn folded<Q>(
    source: &TestEnv<'_>,
    the: &str,
    of: &Entity,
    inputs: Q,
) -> anyhow::Result<Vec<(String, String)>>
where
    Q: crate::query::Application + Clone,
{
    let groups = AttributeQuery::new(
        Term::from(Relation::from(the.parse::<CorpusRelation>()?)),
        Term::<Entity>::var("t"),
        Term::<Any>::var("held"),
        Term::var("cause"),
        None,
    );
    let mut answers = Vec::new();
    for aggregator in [Aggregator::Max, Aggregator::Min] {
        let fold = Fold::new(
            groups.clone(),
            inputs.clone(),
            Reduce::new(
                vec!["t".into()],
                vec![ReduceEntry::new("best", aggregator, Term::var("g"))],
            ),
        );
        let rows = fold.perform(source).try_vec().await?;
        for row in rows.iter().filter(|row| row.this() == of) {
            answers.push((
                format!("{aggregator:?}"),
                format!("{:?}", row.get("best").cloned()),
            ));
        }
    }
    answers.sort();
    answers.dedup();
    Ok(answers)
}

/// A pin of the fold today: the `max` and `min` folds of a rule's
/// `reduce` refuse a group holding numbers of different types
/// ("cannot order"), over stored values and over values a rule deduces,
/// by design: a fold input is data and its numbers are not promoted
/// (`reduce.rs`). A group of one type folds by value: over {3, 10, 7}
/// the greatest is 10 and the least 3. Whether numbers, one stratum in
/// the band, should fold across their types is open; a decision to
/// make them flips the first half of this pin.
#[dialog_common::test]
async fn pin_a_fold_refuses_numbers_of_several_types_and_orders_one_type_by_value()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let of = Entity::new()?;
    let sets = [
        (
            "mixed",
            vec![
                Value::UnsignedInt(3),
                Value::SignedInt(-5),
                Value::Float(2.5),
            ],
        ),
        (
            "whole",
            vec![
                Value::UnsignedInt(3),
                Value::UnsignedInt(10),
                Value::UnsignedInt(7),
            ],
        ),
    ];
    let mut transaction = branch.transaction();
    let mut registry = RuleRegistry::new();
    for (set, values) in &sets {
        for value in values {
            for side in ["stored", "source"] {
                transaction = transaction.assert(AttributeStatement {
                    the: Relation::from(format!("{set}/{side}").parse::<CorpusRelation>()?),
                    of: of.clone(),
                    is: value.clone(),
                    cause: None,
                    cardinality: Some(Cardinality::Many),
                });
            }
        }
        let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "value": { "the": format!("{set}/deduced"), "cardinality": "many" } } },
            "when": [{
                "assert": { "with": { "value": { "the": format!("{set}/source"), "cardinality": "many" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "value": { "?": { "name": "value" } }
                }
            }]
        }))?;
        registry.register(rule.compile()?)?;
    }
    transaction.commit().publish().perform(&operator).await?;
    let source = TestEnv::new(&branch, &operator, registry);

    let stored = |set: &str| -> anyhow::Result<AttributeQuery> {
        Ok(AttributeQuery::new(
            Term::from(Relation::from(
                format!("{set}/stored").parse::<CorpusRelation>()?,
            )),
            Term::<Entity>::var("t"),
            Term::<Any>::var("g"),
            Term::var("cause"),
            None,
        ))
    };
    let deduced = |set: &str| -> anyhow::Result<ConceptQuery> {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("t"));
        terms.insert("value".into(), Term::var("g"));
        Ok(ConceptQuery {
            terms,
            predicate: serde_json::from_value(serde_json::json!({ "with": {
                "value": { "the": format!("{set}/deduced"), "cardinality": "many" }
            }}))?,
        })
    };
    let refused = |result: &anyhow::Result<Vec<(String, String)>>| matches!(result, Err(error) if error.to_string().contains("cannot order"));
    let stored_mixed = folded(&source, "mixed/stored", &of, stored("mixed")?).await;
    assert!(
        refused(&stored_mixed),
        "stored, several types: {stored_mixed:?}"
    );
    let deduced_mixed = folded(&source, "mixed/stored", &of, deduced("mixed")?).await;
    assert!(
        refused(&deduced_mixed),
        "deduced, several types: {deduced_mixed:?}"
    );
    let expected = vec![
        (
            "Max".to_string(),
            format!("{:?}", Some(Value::UnsignedInt(10))),
        ),
        (
            "Min".to_string(),
            format!("{:?}", Some(Value::UnsignedInt(3))),
        ),
    ];
    assert_eq!(
        folded(&source, "whole/stored", &of, stored("whole")?).await?,
        expected,
        "stored, one type"
    );
    assert_eq!(
        folded(&source, "whole/stored", &of, deduced("whole")?).await?,
        expected,
        "deduced, one type"
    );
    Ok(())
}

/// A pin of the order today: a pick over a field of no type that holds
/// values of several types that are not numbers orders them by type
/// first, as [`Value`]'s own order does (an entity below a boolean below
/// a text), whatever the values. Whether such a field reads them all is
/// the typed-election work's to decide (`fix/typed-read-elects-within-its-type`);
/// until then this order holds and this pin says so.
#[dialog_common::test]
async fn pin_a_pick_over_values_of_several_non_number_types_orders_them_by_type()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let of = Entity::new()?;
    let mut transaction = branch.transaction();
    for value in [
        text("apple"),
        Value::Boolean(true),
        Value::Entity(Entity::new()?),
    ] {
        transaction = transaction.assert(AttributeStatement {
            the: Relation::from("mixed/value".parse::<CorpusRelation>()?),
            of: of.clone(),
            is: value,
            cause: None,
            cardinality: Some(Cardinality::Many),
        });
    }
    transaction.commit().publish().perform(&operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let max = picked(
        &source,
        field("mixed/value", &serde_json::Value::Null, "max", None),
        &of,
    )
    .await?;
    assert_eq!(max, vec![format!("{:?}", text("apple"))], "max is the text");
    let min = picked(
        &source,
        field("mixed/value", &serde_json::Value::Null, "min", None),
        &of,
    )
    .await?;
    assert!(
        matches!(min.as_slice(), [only] if only.starts_with("Entity(")),
        "min is the entity: {min:?}"
    );
    Ok(())
}
