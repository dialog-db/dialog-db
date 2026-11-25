//! A rule whose head declares a type its body does not bind, pinned as
//! tests.
//!
//! A head field's type is part of which attribute the rule concludes,
//! so a rule must not conclude a value of another type under it. Whether
//! such a rule is refused when it compiles or concludes nothing for that
//! value is not decided here: each test accepts either, and fails only
//! when the rule installs and concludes the value.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::AttributeStatement;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::types::Any;
use crate::{ConceptDescriptor, ConceptQuery, Entity, Match, Parameters, Term, Value};
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
) -> anyhow::Result<Result<crate::DeductiveRule, String>> {
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

/// A head declared `integer:` over a body that reads text concludes no
/// text: the rule is refused when it compiles, or it concludes nothing
/// for the text. Today it compiles and concludes the text.
#[dialog_common::test]
async fn a_head_of_one_type_concludes_no_value_a_typed_body_binds_of_another() -> anyhow::Result<()>
{
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(
            the!("source/body")
                .of(Entity::new()?)
                .is("hello".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let compiled = rule(
        field("note/body", Some("integer:")),
        field("source/body", Some("text:")),
    )?;
    let Ok(compiled) = compiled else {
        return Ok(());
    };
    let mut registry = RuleRegistry::new();
    registry.register(compiled)?;
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(concluded(&source).await?, Vec::<Value>::new());
    Ok(())
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
