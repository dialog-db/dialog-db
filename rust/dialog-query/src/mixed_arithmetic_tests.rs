//! Arithmetic over numbers of different types, pinned as tests. A read of
//! the band yields each number as its narrowest type, so the types a
//! formula's operands arrive in depend on the data.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::{AttributeStatement, Relation};
use crate::corpus::CorpusRelation;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::types::Any;
use crate::{Cardinality, ConceptQuery, Entity, Match, Parameters, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use serde_json::json;

fn var(name: &str) -> serde_json::Value {
    json!({ "?": { "name": name } })
}

fn statement(the: &str, of: &Entity, is: Value) -> anyhow::Result<AttributeStatement> {
    Ok(AttributeStatement {
        the: Relation::from(the.parse::<CorpusRelation>()?),
        of: of.clone(),
        is,
        cause: None,
        cardinality: Some(Cardinality::Many),
    })
}

/// What a rule concludes that applies `formula` to `a` and `b` of one
/// entity, both read through `declared` (`None` for no type).
async fn computed(
    a: Value,
    b: Value,
    declared: Option<&str>,
    formula: &str,
) -> anyhow::Result<Vec<Value>> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let who = Entity::new()?;
    branch
        .transaction()
        .assert(statement("mixed/a", &who, a)?)
        .assert(statement("mixed/b", &who, b)?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let field = |the: &str| {
        let mut field = json!({ "the": the });
        if let Some(declared) = declared {
            field["as"] = declared.into();
        }
        field
    };
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "out": { "the": "mixed/out", "as": "number:", "cardinality": "many" } } },
        "when": [
            { "assert": { "with": { "a": field("mixed/a"), "b": field("mixed/b") } },
              "where": { "this": var("this"), "a": var("a"), "b": var("b") } },
            { "assert": formula, "where": { "of": var("a"), "with": var("b"), "is": var("out") } }
        ]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.compile()?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::Constant(Value::Entity(who)));
    terms.insert("out".into(), Term::var("out"));
    let rows = ConceptQuery {
        terms,
        predicate: serde_json::from_value(json!({ "with": {
            "out": { "the": "mixed/out", "as": "number:", "cardinality": "many" }
        }}))?,
    }
    .evaluate(Match::new().seed(), &source)
    .try_vec()
    .await?;
    rows.iter()
        .map(|row| Ok(row.lookup(&Term::<Any>::var("out"))?.content()?))
        .collect()
}

/// A rule that sums a whole number and a float read untyped or as
/// `number:` derives the float sum; it derived nothing. A natural and an
/// integer sum as the integer they make, as before.
#[dialog_common::test]
async fn a_formula_over_a_whole_number_and_a_float_computes_as_floats() -> anyhow::Result<()> {
    let mut wrong = Vec::new();
    for declared in [None, Some("number:")] {
        for (case, a, b, formula, expected) in [
            (
                "3 + 2.5",
                Value::UnsignedInt(3),
                Value::Float(2.5),
                "math/sum",
                Value::Float(5.5),
            ),
            (
                "-5 + 2.5",
                Value::SignedInt(-5),
                Value::Float(2.5),
                "math/sum",
                Value::Float(-2.5),
            ),
            (
                "3 * 2.5",
                Value::UnsignedInt(3),
                Value::Float(2.5),
                "math/product",
                Value::Float(7.5),
            ),
            (
                "3 + -5",
                Value::UnsignedInt(3),
                Value::SignedInt(-5),
                "math/sum",
                Value::SignedInt(-2),
            ),
        ] {
            let got = computed(a, b, declared, formula).await?;
            if got != vec![expected.clone()] {
                wrong.push(format!(
                    "{case} as {declared:?}: expected {expected:?}, got {got:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}
