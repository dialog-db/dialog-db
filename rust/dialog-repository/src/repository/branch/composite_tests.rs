//! A composite value is a claim's value like any other: a branch stores
//! it, and a read gives back the bytes it was written in.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::{Composite, Entity, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::query::Output as _;
use dialog_query::{ConceptDescriptor, ConceptQuery, Parameters, RecordBytes, Term, the};

use crate::helpers::test_repo;

fn text(text: &str) -> Value {
    Value::String(text.into())
}

/// A concept reading the one attribute `the`, as records.
fn records(the: &str) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": { "payload": { "the": the, "as": "record:" } }
    }))
    .expect("a concept")
}

/// A composite round-trips through a branch as a claim's value: a map
/// that holds a list and a tuple, and one large enough to be stored
/// apart from its key. What is read is the value that was asserted,
/// and it is the composite it was made from.
#[dialog_common::test]
async fn it_round_trips_a_composite_as_a_claims_value() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let small = Composite::map([
        (text("title"), text("Buy milk")),
        (text("done"), Value::Boolean(false)),
        (
            text("tags"),
            Composite::list([text("home"), text("errand")]).into(),
        ),
        (
            text("at"),
            Composite::tuple([Value::Float(45.5), Value::Float(-122.6)]).into(),
        ),
    ])?;
    let large = Composite::list([text(&"x".repeat(8192)), Value::UnsignedInt(u128::MAX)]);

    let note: Entity = "id:note".parse()?;
    let essay: Entity = "id:essay".parse()?;
    branch
        .transaction()
        .assert(
            the!("note/payload")
                .of(note.clone())
                .is(RecordBytes(small.to_bytes())),
        )
        .assert(
            the!("note/payload")
                .of(essay.clone())
                .is(RecordBytes(large.to_bytes())),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("payload".into(), Term::var("payload"));
    let rows = branch
        .transaction()
        .query()
        .select(ConceptQuery {
            predicate: records("note/payload"),
            terms,
        })
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(rows.len(), 2, "both claims are read");

    for (entity, composite) in [(note, small), (essay, large)] {
        let row = rows
            .iter()
            .find(|row| *row.entity() == entity)
            .expect("the claim is read");
        let read = row
            .source()
            .value_of("payload")
            .expect("the payload is bound");
        assert_eq!(
            *read,
            Value::from(&composite),
            "the bytes that were asserted"
        );
        assert_eq!(read.composite()?, Some(composite.clone()));
        assert_eq!(read.reference(), composite.reference());
    }
    Ok(())
}
