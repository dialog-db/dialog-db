//! Cardinality-one succession across the types of one attribute,
//! pinned as tests.
//!
//! A value of another type under the same attribute belongs to another
//! relation, so asserting a value under cardinality one succeeds the
//! newest claim of its own stratum and leaves the claims of every other
//! stratum standing. Numbers are one stratum, keyed by value whatever
//! type wrote them, so a number succeeds a number of any type.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::AttributeStatement;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::types::Any;
use crate::{Cardinality, ConceptDescriptor, ConceptQuery, Entity, Match, Parameters, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};

/// Every value `of` holds under `note/value`, read through a field of
/// no type under cardinality many, in debug rendering, sorted.
async fn held(source: &TestEnv<'_>, of: &Entity) -> anyhow::Result<Vec<String>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({
        "with": { "value": { "the": "note/value", "cardinality": "many" } }
    }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::Constant(Value::Entity(of.clone())));
    terms.insert("value".into(), Term::var("value"));
    let rows = ConceptQuery { terms, predicate }
        .evaluate(Match::new().seed(), source)
        .try_vec()
        .await?;
    let mut held = rows
        .iter()
        .map(|row| {
            Ok(format!(
                "{:?}",
                row.lookup(&Term::<Any>::var("value"))?.content()?
            ))
        })
        .collect::<anyhow::Result<Vec<String>>>()?;
    held.sort();
    Ok(held)
}

/// Asserting a text under cardinality one leaves the integer the cell
/// held standing, and asserting an integer leaves the text: each is the
/// newest of its own stratum. A float asserted under cardinality one
/// succeeds the natural, numbers being one stratum.
#[dialog_common::test]
async fn a_cardinality_one_assert_succeeds_only_its_own_stratum() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let of = Entity::new()?;
    let assert = |is: Value| AttributeStatement {
        the: the!("note/value"),
        of: of.clone(),
        is,
        cause: None,
        cardinality: Some(Cardinality::One),
    };
    for value in [
        Value::UnsignedInt(3),
        Value::String("hello".into()),
        Value::Float(2.5),
    ] {
        branch
            .transaction()
            .assert(assert(value))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
    }
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    assert_eq!(
        held(&source, &of).await?,
        vec![
            format!("{:?}", Value::Float(2.5)),
            format!("{:?}", Value::String("hello".into())),
        ],
        "the text stands beside the newest number, which succeeded the natural"
    );
    Ok(())
}
