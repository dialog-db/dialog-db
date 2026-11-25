//! Equality and joins over numbers of several types, pinned as tests. A read of the band yields each number as its
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

/// Whether a rule that reads `a` and `b` of one entity through `a_as`
/// and `b_as` derives anything when it requires them equal: by an
/// `==` premise when `join` is unset, and otherwise by binding both
/// to one variable.
async fn equal(
    a: Value,
    b: Value,
    a_as: Option<&str>,
    b_as: Option<&str>,
    join: bool,
) -> anyhow::Result<Outcome> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let who = Entity::new()?;
    branch
        .transaction()
        .assert(statement("same/a", &who, a)?)
        .assert(statement("same/b", &who, b)?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let when = if join {
        json!([
            { "assert": { "with": { "a": field("same/a", a_as) } },
              "where": { "this": var("this"), "a": var("held") } },
            { "assert": { "with": { "b": field("same/b", b_as) } },
              "where": { "this": var("this"), "b": var("held") } }
        ])
    } else {
        json!([
            { "assert": { "with": { "a": field("same/a", a_as), "b": field("same/b", b_as) } },
              "where": { "this": var("this"), "a": var("held"), "b": var("b") } },
            { "assert": "==", "where": { "this": var("held"), "is": var("b") } }
        ])
    };
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "held": { "the": "same/held", "cardinality": "many" } } },
        "when": when
    }))?;
    let compiled = match rule.compile() {
        Ok(compiled) => compiled,
        Err(error) => return Ok(Outcome::Refused(error.to_string())),
    };
    let mut registry = RuleRegistry::new();
    registry.register(compiled)?;
    let source = TestEnv::new(&branch, &operator, registry);
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::Constant(Value::Entity(who)));
    terms.insert("held".into(), Term::var("held"));
    let rows = ConceptQuery {
        terms,
        predicate: serde_json::from_value(json!({ "with": {
            "held": { "the": "same/held", "cardinality": "many" }
        }}))?,
    }
    .evaluate(Match::new().seed(), &source)
    .try_vec()
    .await?;
    Ok(if rows.is_empty() {
        Outcome::NoMatch
    } else {
        Outcome::Match
    })
}

/// What a rule requiring two values equal did: derived its row, derived
/// nothing, or was refused when it compiled.
#[derive(Debug, PartialEq)]
enum Outcome {
    Match,
    NoMatch,
    Refused(String),
}

/// Two numbers of one value are equal whatever types they are read as:
/// `3` read as `natural:` equals `3` read as `integer:` or as `float:`
/// by an `==` premise. Numbers of different values are not equal, and a
/// number of one type equals itself as it did.
///
/// One variable joined across two reads of different number types is
/// refused when the rule compiles, as it is today: whether inference
/// lets two number types meet on one variable is a separate decision,
/// and this pins the refusal until it is made.
#[dialog_common::test]
async fn numbers_of_one_value_are_equal_whatever_type_reads_them() -> anyhow::Result<()> {
    let natural = Value::UnsignedInt;
    let cases = [
        (
            "natural: 3 == integer: 3",
            natural(3),
            natural(3),
            Some("natural:"),
            Some("integer:"),
            Outcome::Match,
        ),
        (
            "natural: 3 == float: 3",
            natural(3),
            natural(3),
            Some("natural:"),
            Some("float:"),
            Outcome::Match,
        ),
        (
            "natural: 3 == natural: 3",
            natural(3),
            natural(3),
            Some("natural:"),
            Some("natural:"),
            Outcome::Match,
        ),
        (
            "natural: 3 == integer: 4",
            natural(3),
            natural(4),
            Some("natural:"),
            Some("integer:"),
            Outcome::NoMatch,
        ),
    ];
    let mut wrong = Vec::new();
    for (case, a, b, a_as, b_as, expected) in cases {
        for join in [false, true] {
            let got = equal(a.clone(), b.clone(), a_as, b_as, join).await?;
            if join && a_as != b_as {
                if !matches!(got, Outcome::Refused(_)) {
                    wrong.push(format!("{case}, joined: expected refused, got {got:?}"));
                }
                continue;
            }
            if got != expected {
                let how = if join { "joined" } else { "==" };
                wrong.push(format!("{case}, {how}: expected {expected:?}, got {got:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    Ok(())
}

/// A formula whose output is a constant derives its row when the value
/// it computes is that number in another type: two `float:` reads of
/// `2.5` and `3.5` sum to the float `6.0`, which is the constant `6`.
/// This held before numbers compared by value (the constant takes the
/// formula's type), and is pinned so it keeps holding.
#[dialog_common::test]
async fn a_formula_output_equals_a_constant_of_one_value() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let who = Entity::new()?;
    branch
        .transaction()
        .assert(statement("sum/a", &who, Value::Float(2.5))?)
        .assert(statement("sum/b", &who, Value::Float(3.5))?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let rule: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "held": { "the": "sum/held", "cardinality": "many" } } },
        "when": [
            { "assert": { "with": { "a": field("sum/a", Some("float:")), "b": field("sum/b", Some("float:")) } },
              "where": { "this": var("this"), "a": var("held"), "b": var("b") } },
            { "assert": "math/sum", "where": { "of": var("held"), "with": var("b"), "is": 6 } }
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
            "held": { "the": "sum/held", "cardinality": "many" }
        }}))?,
    }
    .evaluate(Match::new().seed(), &source)
    .try_vec()
    .await?;
    assert_eq!(rows.len(), 1, "2.5 + 3.5 is 6");
    Ok(())
}
