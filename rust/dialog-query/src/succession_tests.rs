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

/// What two replicas of one cell hold once each has merged the other's
/// concurrent cardinality-one write, from either side: a shared base holds
/// `base`, one replica asserts `left` and the other `right`, both under
/// cardinality one, and each then pulls the other. The two merges must
/// hold the same claims, whichever side merged.
async fn merged(
    base: Value,
    left: Value,
    right: Value,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    merged_as(base, left, right, false).await
}

/// [`merged`], where the left replica writes as a release before
/// stratified succession did when `old` is set: its write also retracts
/// the base claim, whatever the base's type.
async fn merged_as(
    base: Value,
    left: Value,
    right: Value,
    old: bool,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let of = Entity::new()?;
    let assert = |is: Value| AttributeStatement {
        the: the!("note/value"),
        of: of.clone(),
        is,
        cause: None,
        cardinality: Some(Cardinality::One),
    };
    let main = repo.branch("main").open().perform(&operator).await?;
    main.transaction()
        .assert(assert(base.clone()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let mut sides = Vec::new();
    for name in ["left", "right"] {
        let side = repo.branch(name).open().perform(&operator).await?;
        side.set_upstream(&main).perform(&operator).await?;
        side.pull(dialog_repository::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        sides.push(side);
    }
    for (index, (side, value)) in sides.iter().zip([left, right]).enumerate() {
        let mut transaction = side.transaction().assert(assert(value));
        if old && index == 0 {
            transaction = transaction.retract(AttributeStatement {
                cardinality: None,
                ..assert(base.clone())
            });
        }
        transaction.commit().publish().perform(&operator).await?;
    }
    // Each merge starts from a copy of one side, then pulls the other.
    let mut held_by = Vec::new();
    for (ours, theirs) in [(&sides[0], &sides[1]), (&sides[1], &sides[0])] {
        let merge = repo
            .branch(format!("merge-{}", held_by.len()))
            .open()
            .perform(&operator)
            .await?;
        merge.set_upstream(ours).perform(&operator).await?;
        merge
            .pull(dialog_repository::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        merge.set_upstream(theirs).perform(&operator).await?;
        merge
            .pull(dialog_repository::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        let source = TestEnv::new(&merge, &operator, RuleRegistry::new());
        held_by.push(held(&source, &of).await?);
    }
    let theirs = held_by.pop().expect("two merges");
    let ours = held_by.pop().expect("two merges");
    Ok((ours, theirs))
}

/// Two replicas' concurrent cardinality-one writes to one cell combine
/// on a pull to the same claims whichever replica merges, for a write of
/// another type and for one of the same type alike: a pull merges claims,
/// and a retraction removes the claim it names wherever it lands.
#[dialog_common::test]
async fn concurrent_cardinality_one_writes_converge_whichever_side_merges() -> anyhow::Result<()> {
    let cases = [
        (
            "a text and a number",
            Value::UnsignedInt(3),
            Value::String("hello".into()),
            Value::Float(2.5),
        ),
        (
            "two numbers",
            Value::UnsignedInt(3),
            Value::UnsignedInt(4),
            Value::Float(2.5),
        ),
        (
            "two texts",
            Value::String("base".into()),
            Value::String("left".into()),
            Value::String("right".into()),
        ),
    ];
    for (case, base, left, right) in cases {
        let (ours, theirs) = merged(base, left, right).await?;
        println!("{case}: {ours:?} / {theirs:?}");
        assert_eq!(ours, theirs, "{case}: both merges hold the same claims");
    }
    // A replica on an older release retracts the claim of any type its
    // write succeeds; merged with a newer replica's write, the cell still
    // converges.
    let (ours, theirs) = merged_as(
        Value::UnsignedInt(3),
        Value::String("hello".into()),
        Value::Float(2.5),
        true,
    )
    .await?;
    println!("an older writer: {ours:?} / {theirs:?}");
    assert_eq!(
        ours, theirs,
        "an older writer: both merges hold the same claims"
    );
    Ok(())
}
