//! A cardinality-one write succeeds only the claims of its own stratum,
//! committed one operation at a time and as one commit, pinned as tests.

#![cfg(not(target_arch = "wasm32"))]

use std::str::FromStr as _;

use anyhow::Result;
use dialog_baseline::repo::DialogRepo;
use dialog_corpus::{Cardinality, Entity, Fact, Instruction, Relation, Selector, Value};

fn cell() -> (Relation, Entity) {
    (
        Relation::from_str("stratum/value").expect("valid attribute"),
        Entity::from_str("entity:stratum-cell").expect("valid entity"),
    )
}

fn write(is: Value, cardinality: Option<Cardinality>) -> Instruction {
    let (the, of) = cell();
    let fact = Fact {
        the,
        of,
        is,
        cause: None,
    };
    match cardinality {
        Some(cardinality) => Instruction::Assert(fact, cardinality),
        None => Instruction::Retract(fact),
    }
}

/// What the cell holds once `program` is published one commit per
/// `group` operations, sorted.
async fn held(program: fn() -> Vec<Instruction>, group: usize) -> Result<Vec<Value>> {
    let repo = DialogRepo::volatile().await?;
    repo.publish_grouped(program().into_iter().map(|op| vec![op]), group)
        .await?;
    let (_, of) = cell();
    let mut values: Vec<Value> = repo
        .collect(Selector::new().of(of))
        .await?
        .into_iter()
        .map(|fact| fact.is)
        .collect();
    values.sort_by_key(|value| format!("{value:?}"));
    Ok(values)
}

fn long(count: usize) -> Value {
    Value::String("x".repeat(count))
}

/// The five operations seed 0 of `fact_fuzz` shrinks to: a text, an
/// entity, a second text appended, the entity retracted, and a third text
/// asserted under cardinality one, all of one cell.
fn two_texts_then_one() -> Vec<Instruction> {
    let entity = Value::Entity(Entity::from_str("entity:stratum-other").expect("valid entity"));
    vec![
        write(long(4097), Some(Cardinality::One)),
        write(entity.clone(), Some(Cardinality::One)),
        write(long(4095), Some(Cardinality::Many)),
        write(entity, None),
        write(Value::String("v23".into()), Some(Cardinality::One)),
    ]
}

/// The entity does not succeed the first text, so the last write lands on
/// a cell holding two texts and succeeds one of them. Committed one
/// operation at a time it succeeds the newer, `"x"*4095`. As one commit
/// every write stands at one edition, so the two texts stand equal and
/// the tie between them is broken by the hash of the value, as for any
/// two claims of one commit (see `associate_cell` in
/// `dialog-corpus/src/model/update.rs`): it succeeds `"x"*4097`. Both
/// keep `"v23"` and one of the two texts.
#[tokio::test]
async fn a_write_over_two_claims_of_its_stratum_succeeds_the_newest_or_the_tie() -> Result<()> {
    let text = Value::String("v23".into());
    let mut per_op = vec![text.clone(), long(4097)];
    per_op.sort_by_key(|value| format!("{value:?}"));
    let mut one_commit = vec![text, long(4095)];
    one_commit.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(held(two_texts_then_one, 1).await?, per_op, "per operation");
    assert_eq!(
        held(two_texts_then_one, usize::MAX).await?,
        one_commit,
        "one commit"
    );
    Ok(())
}

/// A text, an entity and a second text, each under cardinality one.
fn text_entity_text() -> Vec<Instruction> {
    vec![
        write(Value::String("first".into()), Some(Cardinality::One)),
        write(
            Value::Entity(Entity::from_str("entity:stratum-other").expect("valid entity")),
            Some(Cardinality::One),
        ),
        write(Value::String("second".into()), Some(Cardinality::One)),
    ]
}

/// The entity stands beside the texts, and the second text succeeds the
/// first, whether the three writes are committed one at a time or as one
/// commit.
#[tokio::test]
async fn a_write_of_another_stratum_leaves_the_claim_it_does_not_succeed() -> Result<()> {
    let mut expected = vec![
        Value::String("second".into()),
        Value::Entity(Entity::from_str("entity:stratum-other").expect("valid entity")),
    ];
    expected.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(held(text_entity_text, 1).await?, expected, "per operation");
    assert_eq!(
        held(text_entity_text, usize::MAX).await?,
        expected,
        "one commit"
    );
    Ok(())
}
