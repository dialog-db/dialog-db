//! A composite value is a claim's value like any other: a branch stores
//! it, and a read gives back the bytes it was written in.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::{Composite, Entity, Fact, Instruction, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::query::Output as _;
use dialog_query::types::Any;
use dialog_query::{
    Changes, ConceptDescriptor, ConceptQuery, DeductiveRule, MapPattern, Parameters, Pattern,
    Premise, Proposition, RecordBytes, Statement, Term, the,
};

use crate::helpers::test_repo;
use crate::rules::source_attr;

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

/// A concept of the one field `field` over `the`, read as `declared`.
fn concept(field: &str, the: &str, declared: &str) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": { field: { "the": the, "as": declared, "pick": "all:" } }
    }))
    .expect("a concept")
}

/// `heading(this, heading) :- payload(this, {title: heading, ...})`: a
/// rule whose premise holds a map pattern, which takes the title out of
/// a note's record whatever else the record holds.
fn heading_rule() -> Result<DeductiveRule> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.set(
        "payload".into(),
        Pattern::Map(
            MapPattern::new([(text("title"), Pattern::Term(Term::var("heading")))])?
                .with_rest(Term::<Any>::blank()),
        ),
    );
    let premise = Premise::Assert(Proposition::Concept(ConceptQuery {
        predicate: concept("payload", "note/payload", "record:"),
        terms,
    }));
    Ok(DeductiveRule::new(
        concept("heading", "note/heading", "text:"),
        vec![premise],
    )?)
}

/// A rule that holds a pattern is laid over a branch for one query and
/// run, with nothing of it stored: the branch holds a note whose
/// record has a title and one whose record has none, and the rule
/// derives a heading for the first alone. The overlay names the rule
/// by its facts and holds no body for it, and the same rule is still
/// refused by a batch that would be committed.
#[dialog_common::test]
async fn it_runs_a_rule_that_holds_a_pattern_laid_over_a_branch() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;

    let titled = Composite::map([
        (text("title"), text("Buy milk")),
        (text("done"), Value::Boolean(false)),
    ])?;
    let untitled = Composite::map([(text("done"), Value::Boolean(true))])?;
    let milk: Entity = "id:milk".parse()?;
    let chore: Entity = "id:chore".parse()?;
    branch
        .transaction()
        .assert(
            the!("note/payload")
                .of(milk.clone())
                .is(RecordBytes(titled.to_bytes())),
        )
        .assert(
            the!("note/payload")
                .of(chore.clone())
                .is(RecordBytes(untitled.to_bytes())),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let rule = heading_rule()?;
    assert!(rule.storable().is_err(), "the rule has no stored body");

    let headings = || {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("heading".into(), Term::var("heading"));
        ConceptQuery {
            predicate: concept("heading", "note/heading", "text:"),
            terms,
        }
    };

    let layer = branch.with(&rule);
    let facts: Vec<Fact> = layer
        .changes()
        .clone()
        .into_instructions()
        .into_iter()
        .map(|instruction| {
            let (Instruction::Assert(fact, _) | Instruction::Retract(fact)) = instruction;
            fact
        })
        .collect();
    assert!(
        facts.iter().any(|fact| fact.of == rule.this()),
        "the overlay names the rule"
    );
    assert!(
        facts.iter().all(|fact| fact.the != source_attr()),
        "nothing is written of the rule's body"
    );
    let rows = layer
        .select(headings())
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(rows.len(), 1, "the note whose record has a title");
    assert_eq!(*rows[0].entity(), milk);
    assert_eq!(
        rows[0].source().value_of("heading"),
        Some(&text("Buy milk"))
    );

    // Without the rule laid over it the branch derives nothing.
    let rows = branch
        .query()
        .select(headings())
        .perform(&operator)
        .try_vec()
        .await?;
    assert!(rows.is_empty(), "the rule was not stored");
    Ok(())
}

/// A batch that would be committed refuses the rule a layer runs: it
/// holds bytes only, and the rule has none. The refusal says why, and
/// nothing is staged.
///
/// Native only: the refusal is a panic, which `wasm32-unknown-unknown`
/// aborts on and cannot catch. The refusal's reason is covered on both
/// targets by `storable`, asserted in
/// `it_runs_a_rule_that_holds_a_pattern_laid_over_a_branch`.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn it_refuses_to_commit_the_rule_a_layer_runs() -> Result<()> {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let rule = heading_rule()?;
    let mut changes = Changes::new();
    let refused = catch_unwind(AssertUnwindSafe(|| {
        (&rule).assert(&mut changes);
    }))
    .expect_err("the assertion is refused");
    let reason = refused
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(reason.contains("no way to write a pattern"), "{reason}");
    assert_eq!(changes, Changes::new(), "nothing was staged");
    Ok(())
}
