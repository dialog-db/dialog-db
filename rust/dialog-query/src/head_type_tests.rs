//! A rule whose head declares a type its body does not bind, pinned as
//! tests.
//!
//! A head field's type is part of which attribute the rule concludes,
//! so a rule concludes no value of another type under it. Where the body
//! binds the head's variable to a type the field reads none of (a typed
//! field, a constant, a formula's output), the rule is refused when it
//! compiles. Where the body leaves the variable open (a field of no
//! type), the head's type narrows it, and the rule concludes only the
//! values the field reads.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::AttributeStatement;
use crate::corpus::Type as ValueType;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::types::Any;
use crate::{
    ConceptDescriptor, ConceptQuery, DeductiveRule, Entity, Match, Parameters, Term, TypeError,
    Value,
};
use dialog_peer::helpers::{test_repo, test_session_with_peer};

/// A field over `the`, declared `as` (none for no type), read many.
fn field(the: &str, declared: Option<&str>) -> serde_json::Value {
    let mut field = serde_json::Map::new();
    field.insert("the".into(), the.into());
    if let Some(declared) = declared {
        field.insert("as".into(), declared.into());
    }
    field.insert("cardinality".into(), "many".into());
    serde_json::Value::Object(field)
}

/// The rule concluding `head` from `body`, or why it does not compile.
fn rule(
    head: serde_json::Value,
    body: serde_json::Value,
) -> anyhow::Result<Result<DeductiveRule, String>> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "value": head } },
        "when": [{
            "assert": { "with": { "value": body } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))?;
    Ok(descriptor.compile().map_err(|error| error.to_string()))
}

/// Every value a read of `note/body` that declares no type answers.
async fn concluded(source: &TestEnv<'_>) -> anyhow::Result<Vec<Value>> {
    let predicate: ConceptDescriptor = serde_json::from_value(
        serde_json::json!({ "with": { "value": field("note/body", None) } }),
    )?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::var("value"));
    let rows = ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), source)
        .try_vec()
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.lookup(&Term::<Any>::var("value"))?.content())
        .collect::<Result<Vec<Value>, crate::EvaluationError>>()?)
}

/// A head field over `note/body` declared `declared`, concluded by a
/// body of `when`, compiled.
fn compile(declared: &str, when: serde_json::Value) -> Result<DeductiveRule, TypeError> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "value": field("note/body", Some(declared)) } },
        "when": when
    }))
    .expect("a well-formed rule");
    descriptor.compile()
}

fn var(name: &str) -> serde_json::Value {
    serde_json::json!({ "?": { "name": name } })
}

/// A head declared one type over a body that binds its variable to a
/// type the field reads none of is refused when it compiles, with
/// `TypeError::HeadField` naming the field, its declared type and the
/// type the body binds: a typed body field (text into `integer:`) and a
/// formula's output (a sum into `text:`).
#[dialog_common::test]
fn a_head_of_a_type_the_body_binds_none_of_is_refused() {
    let typed = serde_json::json!([{
        "assert": { "with": { "value": field("source/body", Some("text:")) } },
        "where": { "this": var("this"), "value": var("value") }
    }]);
    let formula = serde_json::json!([
        {
            "assert": { "with": { "n": field("source/n", Some("natural:")) } },
            "where": { "this": var("this"), "n": var("n") }
        },
        { "assert": "math/sum", "where": { "of": var("n"), "with": 1, "is": var("value") } }
    ]);
    for (how, declared, when, bound) in [
        ("a typed field", "integer:", typed, ValueType::String),
        ("a formula", "text:", formula, ValueType::UnsignedInt),
    ] {
        let error = compile(declared, when).expect_err(how);
        assert!(
            matches!(
                &error,
                TypeError::HeadField { field, declared: head, inferred, .. }
                    if field == "value"
                        && head.primitive_part().intersect(inferred.primitive_part()).is_none()
                        && inferred.primitive_part().contains(bound)
            ),
            "{how}: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("\"value\"") && message.contains(&format!("{bound:?}")),
            "{how}: the error names the field and the type the body binds: {message}"
        );
    }
}

/// A head declared a type over a body whose variable is open, or bound
/// to a type the field reads (a number of another number type through
/// the band, or a text, symbol or entity, which convert into one
/// another), compiles.
#[dialog_common::test]
fn a_head_of_a_type_the_body_may_bind_compiles() {
    let untyped = || {
        serde_json::json!([{
            "assert": { "with": { "value": field("source/body", None) } },
            "where": { "this": var("this"), "value": var("value") }
        }])
    };
    let typed = |declared: &str| {
        serde_json::json!([{
            "assert": { "with": { "value": field("source/body", Some(declared)) } },
            "where": { "this": var("this"), "value": var("value") }
        }])
    };
    for (head, when) in [
        ("text:", untyped()),
        ("integer:", untyped()),
        ("boolean:", untyped()),
        ("float:", typed("natural:")),
        ("integer:", typed("natural:")),
        ("text:", typed("entity:")),
        ("entity:", typed("text:")),
    ] {
        if let Err(error) = compile(head, when.clone()) {
            panic!("{head} over {when}: {error}");
        }
    }
}

/// A head declared `text:` over a body that reads with no type concludes
/// the text it reads and not the number: nothing at compile time says
/// what the body binds, so this one is decided value by value. A head
/// declared a number type over the same body already reads its body
/// through that type and concludes no text.
#[dialog_common::test]
async fn a_head_of_a_type_concludes_only_the_values_of_that_type_an_untyped_body_binds()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut transaction = branch.transaction();
    for value in [Value::String("hello".into()), Value::UnsignedInt(7)] {
        transaction = transaction.assert(AttributeStatement {
            the: the!("source/body"),
            of: Entity::new()?,
            is: value,
            cause: None,
            cardinality: None,
        });
    }
    transaction.commit().publish().perform(&operator).await?;

    for (declared, expected) in [
        ("text:", Value::String("hello".into())),
        ("natural:", Value::UnsignedInt(7)),
    ] {
        let compiled = rule(
            field("note/body", Some(declared)),
            field("source/body", None),
        )?
        .map_err(|error| anyhow::anyhow!("a head of {declared} compiles: {error}"))?;
        let mut registry = RuleRegistry::new();
        registry.register(compiled)?;
        let source = TestEnv::new(&branch, &operator, registry);
        assert_eq!(
            concluded(&source).await?,
            vec![expected],
            "a head of {declared} over an untyped body"
        );
    }
    Ok(())
}

/// A head declared `integer:` whose variable the body sets equal to the
/// text `"hello"` compiles: inference types each side of an equality on
/// its own, so the variable is open, and the head narrows it to an
/// integer. The rule then concludes nothing, as no integer is `"hello"`.
#[dialog_common::test]
async fn a_head_set_equal_to_a_constant_of_another_type_concludes_nothing() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(the!("source/flag").of(Entity::new()?).is(true))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let rule = compile(
        "integer:",
        serde_json::json!([
            {
                "assert": { "with": { "flag": field("source/flag", None) } },
                "where": { "this": var("this"), "flag": var("flag") }
            },
            { "assert": "==", "where": { "this": var("value"), "is": "hello" } }
        ]),
    )?;
    let mut registry = RuleRegistry::new();
    registry.register(rule)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(concluded(&source).await?, Vec::<Value>::new());
    Ok(())
}
