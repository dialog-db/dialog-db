//! The range predicates (`<`, `<=`, `>`, `>=`) over numbers of several
//! types, pinned as tests. A read of the band yields each number as its
//! narrowest type, so the variant two compared numbers arrive in depends
//! on the data, not on what declared them.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::{AttributeStatement, Relation};
use crate::corpus::CorpusRelation;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::{Cardinality, ConceptQuery, Entity, Match, Parameters, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use serde_json::json;

fn var(name: &str) -> serde_json::Value {
    json!({ "?": { "name": name } })
}

fn field(the: &str, declared: Option<&str>) -> serde_json::Value {
    let mut field = json!({ "the": the });
    if let Some(declared) = declared {
        field["as"] = declared.into();
    }
    field
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

/// Whether a rule that reads `a` and `b` of one entity, through `a_as`
/// and `b_as`, and requires `a <op> b`, derives anything.
async fn holds(
    a: Value,
    b: Value,
    a_as: Option<&str>,
    b_as: Option<&str>,
    op: &str,
) -> anyhow::Result<bool> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let who = Entity::new()?;
    branch
        .transaction()
        .assert(statement("range/a", &who, a)?)
        .assert(statement("range/b", &who, b)?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "held": { "the": "range/held", "cardinality": "many" } } },
        "when": [
            {
                "assert": { "with": { "a": field("range/a", a_as), "b": field("range/b", b_as) } },
                "where": { "this": var("this"), "a": var("held"), "b": var("b") }
            },
            { "assert": op, "where": { "of": var("held"), "with": var("b") } }
        ]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.compile()?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::Constant(Value::Entity(who)));
    terms.insert("held".into(), Term::var("held"));
    let rows = ConceptQuery {
        terms,
        predicate: serde_json::from_value(json!({ "with": {
            "held": { "the": "range/held", "cardinality": "many" }
        }}))?,
    }
    .evaluate(Match::new().seed(), &source)
    .try_vec()
    .await?;
    Ok(!rows.is_empty())
}

/// Two numbers a rule compares order by value whatever types they are
/// read as: `3 > -5` holds over a natural and an integer, `3 > 2.5` over
/// a natural and a float, read untyped, as `number:` or through two
/// different number types, and `-5 < 3` too. Two numbers of one type
/// order as they did. A text against a number is no match.
#[dialog_common::test]
async fn a_range_predicate_orders_numbers_of_several_types_by_value() -> anyhow::Result<()> {
    let natural = Value::UnsignedInt;
    let integer = Value::SignedInt;
    let float = Value::Float;
    let cases = [
        (
            "3 > -5, untyped",
            natural(3),
            integer(-5),
            None,
            None,
            ">",
            true,
        ),
        (
            "3 > -5, number:",
            natural(3),
            integer(-5),
            Some("number:"),
            Some("number:"),
            ">",
            true,
        ),
        (
            "3 > 2.5, number:",
            natural(3),
            float(2.5),
            Some("number:"),
            Some("number:"),
            ">",
            true,
        ),
        (
            "3 > -5, natural: and integer:",
            natural(3),
            integer(-5),
            Some("natural:"),
            Some("integer:"),
            ">",
            true,
        ),
        (
            "-5 < 3, untyped",
            integer(-5),
            natural(3),
            None,
            None,
            "<",
            true,
        ),
        (
            "2.5 <= 3, untyped",
            float(2.5),
            natural(3),
            None,
            None,
            "<=",
            true,
        ),
        (
            "3 >= 3.5, untyped",
            natural(3),
            float(3.5),
            None,
            None,
            ">=",
            false,
        ),
        (
            "3 > 1, natural:",
            natural(3),
            natural(1),
            Some("natural:"),
            Some("natural:"),
            ">",
            true,
        ),
        (
            "3 > 1, untyped",
            natural(3),
            natural(1),
            None,
            None,
            ">",
            true,
        ),
        (
            "1 > 3, untyped",
            natural(1),
            natural(3),
            None,
            None,
            ">",
            false,
        ),
        (
            "\"a\" > 3, untyped",
            Value::String("a".into()),
            natural(3),
            None,
            None,
            ">",
            false,
        ),
    ];
    let mut wrong = Vec::new();
    for (case, a, b, a_as, b_as, op, expected) in cases {
        let got = holds(a, b, a_as, b_as, op).await?;
        if got != expected {
            wrong.push(format!("{case}: expected {expected}, got {got}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}

/// What `range/v` values a rule keeps that requires `?x > bound`: once
/// with `bound` the constant `0`, which narrows the scan of the band to
/// the numbers past it, and once with `bound` a variable bound to a stored
/// `0`, which filters the rows the scan read. Both keep the same numbers
/// of a mixed set, as the range predicates' module doc promises.
#[dialog_common::test]
async fn a_pushed_range_and_the_residual_filter_keep_the_same_numbers() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let values = [
        Value::UnsignedInt(3),
        Value::SignedInt(-5),
        Value::Float(2.5),
        Value::UnsignedInt(0),
        Value::Float(-0.5),
        Value::Float(0.25),
    ];
    let mut transaction = branch.transaction();
    for value in &values {
        transaction = transaction.assert(statement("range/v", &Entity::new()?, value.clone())?);
    }
    let zero = Entity::new()?;
    transaction = transaction.assert(statement("range/zero", &zero, Value::UnsignedInt(0))?);
    transaction.commit().publish().perform(&operator).await?;

    let kept = |target: &str,
                premises: Vec<serde_json::Value>|
     -> anyhow::Result<DeductiveRuleDescriptor> {
        let mut when = vec![json!({
            "assert": { "with": { "v": field("range/v", None) } },
            "where": { "this": var("this"), "v": var("kept") }
        })];
        when.extend(premises);
        Ok(serde_json::from_value(json!({
            "deduce": { "with": { "kept": { "the": target, "cardinality": "many" } } },
            "when": when
        }))?)
    };
    let pushed = kept(
        "range/pushed",
        vec![json!({ "assert": ">", "where": { "of": var("kept"), "with": 0 } })],
    )?;
    let residual = kept(
        "range/residual",
        vec![
            json!({
                "assert": { "with": { "zero": field("range/zero", None) } },
                "where": { "this": var("z"), "zero": var("bound") }
            }),
            json!({ "assert": ">", "where": { "of": var("kept"), "with": var("bound") } }),
        ],
    )?;
    let mut registry = RuleRegistry::new();
    registry.register(pushed.compile()?)?;
    registry.register(residual.compile()?)?;
    let source = TestEnv::new(&branch, &operator, registry);
    let read = |target: &str| -> anyhow::Result<ConceptQuery> {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("kept".into(), Term::var("kept"));
        Ok(ConceptQuery {
            terms,
            predicate: serde_json::from_value(json!({ "with": {
                "kept": { "the": target, "cardinality": "many" }
            }}))?,
        })
    };
    let mut answers = Vec::new();
    for target in ["range/pushed", "range/residual"] {
        let rows = read(target)?
            .evaluate(Match::new().seed(), &source)
            .try_vec()
            .await?;
        let mut kept = rows
            .iter()
            .map(|row| {
                Ok(format!(
                    "{:?}",
                    row.lookup(&Term::<crate::types::Any>::var("kept"))?
                        .content()?
                ))
            })
            .collect::<anyhow::Result<Vec<String>>>()?;
        kept.sort();
        answers.push(kept);
    }
    let expected = {
        let mut expected = vec![
            format!("{:?}", Value::UnsignedInt(3)),
            format!("{:?}", Value::Float(2.5)),
            format!("{:?}", Value::Float(0.25)),
        ];
        expected.sort();
        expected
    };
    assert_eq!(answers[0], expected, "the pushed range");
    assert_eq!(answers[1], expected, "the residual filter");
    Ok(())
}
