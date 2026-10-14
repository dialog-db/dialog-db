//! Branch-level pins of how numbers are stored and read: a branch's tree
//! keys every number in one band by value, so `3`, `+3` and `3.0` are one
//! fact, a raw read returns the narrowest type holding the number, a typed
//! read is a lens over the number, and a branch an earlier release laid
//! out per type is read but not extended until it is migrated.

#![allow(unexpected_cfgs)]

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::cmp::Ordering;

use anyhow::Result;
use dialog_artifacts::history::HistorySelector;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, DialogArtifactsError, Entity, Instruction, NUMBER_BAND, Pick,
    Relation, Value, encode_key_value_owned,
};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::query::Output as _;
use dialog_query::{Cardinality, ConceptDescriptor, Query, Term, TypeError, the};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::{TryStreamExt as _, stream};

use crate::helpers::test_repo;
use crate::{Branch, CommitError, Repository, Revision};

/// The session every test commits and reads through.
type Operator = Peer<VolatileSpace, Session>;

/// A repository with its `main` branch open.
async fn open() -> Result<(Operator, Repository<dialog_credentials::Credential>, Branch)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    Ok((operator, repo, branch))
}

fn size() -> Relation {
    "item/size".parse().expect("a relation")
}

/// Every row `selector` matches on `branch`, in the order the scan yields
/// them.
async fn select(
    branch: &Branch,
    selector: ArtifactSelector<Constrained>,
    operator: &Operator,
) -> Result<Vec<Artifact>> {
    Ok(branch
        .claims()
        .select(selector)
        .to_owned()
        .perform(operator)
        .await?
        .try_collect::<Vec<Artifact>>()
        .await?)
}

/// The values of `rows`, in the order the scan yielded them.
fn values(rows: &[Artifact]) -> Vec<Value> {
    rows.iter().map(|row| row.is.clone()).collect()
}

/// `a` against `b` as numbers, exactly: through the band's order-preserving
/// encoding, which an `f64` comparison is not (`u64::MAX` and `2^64` are one
/// `f64`).
fn by_number(a: &Value, b: &Value) -> Ordering {
    encode_key_value_owned(a, NUMBER_BAND).cmp(&encode_key_value_owned(b, NUMBER_BAND))
}

/// The values of `rows`, sorted numerically.
fn sorted(rows: &[Artifact]) -> Vec<Value> {
    let mut values = values(rows);
    values.sort_by(by_number);
    values
}

/// The values under `item/size` that `selector` matches, sorted
/// numerically.
async fn sizes(
    branch: &Branch,
    selector: ArtifactSelector<Constrained>,
    operator: &Operator,
) -> Result<Vec<Value>> {
    Ok(sorted(
        &select(branch, selector.the(size()), operator).await?,
    ))
}

/// Every `(entity, value)` under `the` on `branch`, sorted: the fact set a
/// test compares before and after a rewrite or a pull.
async fn facts(branch: &Branch, the: &str, operator: &Operator) -> Result<Vec<(Entity, Value)>> {
    let rows = select(branch, ArtifactSelector::new().the(the.parse()?), operator).await?;
    let mut facts: Vec<(Entity, Value)> = rows.into_iter().map(|row| (row.of, row.is)).collect();
    facts.sort_by(|a, b| {
        a.0.to_string()
            .cmp(&b.0.to_string())
            .then_with(|| by_number(&a.1, &b.1))
    });
    Ok(facts)
}

/// How many history records the tree holds for the cell `(of, the)`: one
/// per assertion or retraction a commit recorded there.
async fn cell_records(
    branch: &Branch,
    of: &Entity,
    the: &str,
    operator: &Operator,
) -> Result<usize> {
    let the: Relation = the.parse()?;
    let records = branch
        .history(operator)
        .select(HistorySelector::All)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(records
        .iter()
        .filter(|(_, record)| record.claim().of == *of && record.claim().the == the)
        .count())
}

/// A fact with no cause, as an application writes it.
fn fact(the: &str, of: &Entity, is: Value) -> Result<Artifact> {
    Ok(Artifact {
        the: the.parse()?,
        of: of.clone(),
        is,
        cause: None,
    })
}

/// Commits `instructions` to `branch` as one batch and refreshes it.
async fn commit(
    branch: &Branch,
    instructions: Vec<Instruction>,
    operator: &Operator,
) -> Result<Revision, CommitError> {
    let revision = branch
        .commit(stream::iter(instructions))
        .perform(operator)
        .await?;
    branch.refresh(operator).await.map_err(CommitError::from)?;
    Ok(revision)
}

/// Asserts `facts` under `pick` as one batch.
async fn assert_all(
    branch: &Branch,
    facts: impl IntoIterator<Item = Artifact>,
    pick: Pick,
    operator: &Operator,
) -> Result<Revision, CommitError> {
    commit(
        branch,
        facts
            .into_iter()
            .map(|artifact| Instruction::Assert(artifact, pick.clone()))
            .collect(),
        operator,
    )
    .await
}

/// Retracts `facts` as one batch.
async fn retract_all(
    branch: &Branch,
    facts: impl IntoIterator<Item = Artifact>,
    operator: &Operator,
) -> Result<Revision, CommitError> {
    commit(
        branch,
        facts.into_iter().map(Instruction::Retract).collect(),
        operator,
    )
    .await
}

/// Commits one `item/size` per value, each on a fresh entity, under `all`.
async fn write_sizes(branch: &Branch, sizes: &[Value], operator: &Operator) -> Result<()> {
    let facts = sizes
        .iter()
        .map(|size| fact("item/size", &Entity::new()?, size.clone()))
        .collect::<Result<Vec<_>>>()?;
    assert_all(branch, facts, Pick::All, operator).await?;
    Ok(())
}

/// The edge numbers, each in every number type that spells it: whole
/// non-negative numbers as naturals, integers and (where a float holds
/// them exactly) floats; negative whole numbers as integers and floats;
/// fractions, huge magnitudes and the infinities as floats.
fn edge_spellings() -> Vec<Value> {
    vec![
        Value::UnsignedInt(0),
        Value::SignedInt(0),
        Value::Float(0.0),
        Value::Float(-0.0),
        Value::UnsignedInt(1),
        Value::SignedInt(1),
        Value::Float(1.0),
        Value::SignedInt(-1),
        Value::Float(-1.0),
        Value::Float(2.5),
        Value::Float(-2.5),
        Value::Float(0.1),
        Value::UnsignedInt(255),
        Value::SignedInt(255),
        Value::Float(255.0),
        Value::UnsignedInt(256),
        Value::SignedInt(256),
        Value::Float(256.0),
        Value::UnsignedInt(u64::MAX as u128),
        Value::SignedInt(u64::MAX as i128),
        Value::UnsignedInt(1u128 << 64),
        Value::SignedInt(1i128 << 64),
        Value::UnsignedInt(u128::MAX),
        Value::SignedInt(i128::MIN),
        Value::Float(1e30),
        Value::Float(f64::MAX),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
    ]
}

/// What a raw read of [`edge_spellings`] returns, in numeric order: the
/// narrowest type holding each number. `-0.0` is the number zero, `1e30`
/// is the natural `10^30`, and `f64::MAX` and the infinities, which no
/// integer type holds, stay floats.
fn edge_readings() -> Vec<Value> {
    vec![
        Value::Float(f64::NEG_INFINITY),
        Value::SignedInt(i128::MIN),
        Value::Float(-2.5),
        Value::SignedInt(-1),
        Value::SignedInt(-1),
        Value::UnsignedInt(0),
        Value::UnsignedInt(0),
        Value::UnsignedInt(0),
        Value::UnsignedInt(0),
        Value::Float(0.1),
        Value::UnsignedInt(1),
        Value::UnsignedInt(1),
        Value::UnsignedInt(1),
        Value::Float(2.5),
        Value::UnsignedInt(255),
        Value::UnsignedInt(255),
        Value::UnsignedInt(255),
        Value::UnsignedInt(256),
        Value::UnsignedInt(256),
        Value::UnsignedInt(256),
        Value::UnsignedInt(u64::MAX as u128),
        Value::UnsignedInt(u64::MAX as u128),
        Value::UnsignedInt(1u128 << 64),
        Value::UnsignedInt(1u128 << 64),
        Value::UnsignedInt(1_000_000_000_000_000_000_000_000_000_000),
        Value::UnsignedInt(u128::MAX),
        Value::Float(f64::MAX),
        Value::Float(f64::INFINITY),
    ]
}

/// A mix of number types on one attribute, each on its own entity.
fn mixed() -> Vec<Value> {
    vec![
        Value::SignedInt(-3),
        Value::Float(-2.5),
        Value::Float(1.5),
        Value::Float(2.5),
        Value::UnsignedInt(3),
        Value::UnsignedInt(5),
    ]
}

/// The raw bound checks every tree holding [`mixed`] under `item/size`
/// answers the same way: a bound is a lens of its type, `across_numbers`
/// compares as is.
async fn assert_lens_bounds(branch: &Branch, operator: &Operator) -> Result<()> {
    let at_least = |bound: Value| ArtifactSelector::new().is_at_least(bound);
    let at_most = |bound: Value| ArtifactSelector::new().is_at_most(bound);
    let more_than = |bound: Value| ArtifactSelector::new().is_greater_than(bound);
    assert_eq!(
        sizes(branch, at_least(Value::UnsignedInt(2)), operator).await?,
        vec![
            Value::Float(2.5),
            Value::UnsignedInt(3),
            Value::UnsignedInt(5)
        ],
        "a natural bound >= 2 sees 2.5 as 2, and does not see -3, -2.5 or 1.5"
    );
    assert_eq!(
        sizes(branch, at_most(Value::UnsignedInt(2)), operator).await?,
        vec![Value::Float(1.5), Value::Float(2.5)],
        "a natural bound <= 2 sees 2.5 as 2 and 1.5 as 1, not 3, and no negative"
    );
    assert_eq!(
        sizes(branch, at_least(Value::SignedInt(-3)), operator).await?,
        mixed_sorted(),
        "an integer bound >= -3 sees -2.5 as -3: every number"
    );
    assert_eq!(
        sizes(branch, more_than(Value::SignedInt(-3)), operator).await?,
        vec![
            Value::Float(1.5),
            Value::Float(2.5),
            Value::UnsignedInt(3),
            Value::UnsignedInt(5)
        ],
        "an integer bound > -3 does not see -2.5, which it reads as -3"
    );
    assert_eq!(
        sizes(branch, at_least(Value::Float(2.5)), operator).await?,
        vec![
            Value::Float(2.5),
            Value::UnsignedInt(3),
            Value::UnsignedInt(5)
        ],
        "a float bound sees every number as it is"
    );
    assert_eq!(
        sizes(branch, at_most(Value::Float(-2.5)), operator).await?,
        vec![Value::SignedInt(-3), Value::Float(-2.5)],
        "a float bound sees every number as it is"
    );
    assert_eq!(
        sizes(
            branch,
            at_most(Value::UnsignedInt(2)).across_numbers(),
            operator
        )
        .await?,
        vec![Value::SignedInt(-3), Value::Float(-2.5), Value::Float(1.5)],
        "across number types <= 2 compares as is: 2.5 is out, the negatives are in"
    );
    assert_eq!(
        sizes(
            branch,
            at_least(Value::UnsignedInt(2)).across_numbers(),
            operator
        )
        .await?,
        vec![
            Value::Float(2.5),
            Value::UnsignedInt(3),
            Value::UnsignedInt(5)
        ],
        "across number types >= 2 compares as is"
    );
    assert_eq!(
        sizes(
            branch,
            ArtifactSelector::new().is_between(Value::SignedInt(-3), Value::Float(2.5)),
            operator
        )
        .await?,
        vec![
            Value::SignedInt(-3),
            Value::Float(-2.5),
            Value::Float(1.5),
            Value::Float(2.5)
        ],
        "bounds of two types form one range, each read through its own lens"
    );
    Ok(())
}

/// [`mixed`] as a raw read returns it, in numeric order.
fn mixed_sorted() -> Vec<Value> {
    vec![
        Value::SignedInt(-3),
        Value::Float(-2.5),
        Value::Float(1.5),
        Value::Float(2.5),
        Value::UnsignedInt(3),
        Value::UnsignedInt(5),
    ]
}

/// `3`, `+3` and `3.0` written at one `(entity, attribute)` are one fact,
/// read as the natural `3`; a fraction re-asserted is one fact; and
/// writing the other spellings does not grow the attribute's fact count.
#[dialog_common::test]
async fn it_holds_one_fact_for_a_number_however_it_is_spelled() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    let three = Entity::new()?;
    let fraction = Entity::new()?;
    let zero = Entity::new()?;
    let minus = Entity::new()?;
    assert_all(
        &branch,
        [
            fact("item/size", &three, Value::UnsignedInt(3))?,
            fact("item/size", &fraction, Value::Float(2.5))?,
            fact("item/size", &zero, Value::UnsignedInt(0))?,
            fact("item/size", &minus, Value::SignedInt(-1))?,
        ],
        Pick::All,
        &operator,
    )
    .await?;
    let before = facts(&branch, "item/size", &operator).await?;
    assert_eq!(before.len(), 4);

    assert_all(
        &branch,
        [
            fact("item/size", &three, Value::Float(3.0))?,
            fact("item/size", &three, Value::SignedInt(3))?,
            fact("item/size", &fraction, Value::Float(2.5))?,
            fact("item/size", &zero, Value::Float(-0.0))?,
            fact("item/size", &zero, Value::SignedInt(0))?,
            fact("item/size", &minus, Value::Float(-1.0))?,
        ],
        Pick::All,
        &operator,
    )
    .await?;

    let after = facts(&branch, "item/size", &operator).await?;
    assert_eq!(after, before, "no spelling adds a fact: {after:?}");
    for (entity, expected) in [
        (&three, Value::UnsignedInt(3)),
        (&fraction, Value::Float(2.5)),
        (&zero, Value::UnsignedInt(0)),
        (&minus, Value::SignedInt(-1)),
    ] {
        let rows = select(
            &branch,
            ArtifactSelector::new().of(entity.clone()),
            &operator,
        )
        .await?;
        assert_eq!(values(&rows), vec![expected], "{entity} holds one fact");
    }
    Ok(())
}

/// A fact written as the natural `3` is retracted by the float `3.0`, and
/// one written as the integer `7` by the natural `7`: the retraction names
/// the number, not the type that wrote it. Retracting `3.5` removes
/// nothing, since no fact holds that number.
#[dialog_common::test]
async fn it_retracts_a_fact_by_another_spelling_of_its_number() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    let three = Entity::new()?;
    let seven = Entity::new()?;
    assert_all(
        &branch,
        [
            fact("item/size", &three, Value::UnsignedInt(3))?,
            fact("item/size", &seven, Value::SignedInt(7))?,
        ],
        Pick::All,
        &operator,
    )
    .await?;

    retract_all(
        &branch,
        [fact("item/size", &three, Value::Float(3.5))?],
        &operator,
    )
    .await?;
    assert_eq!(
        facts(&branch, "item/size", &operator).await?.len(),
        2,
        "3.5 is not a number any fact holds: nothing is retracted"
    );

    retract_all(
        &branch,
        [
            fact("item/size", &three, Value::Float(3.0))?,
            fact("item/size", &seven, Value::UnsignedInt(7))?,
        ],
        &operator,
    )
    .await?;
    let remaining = facts(&branch, "item/size", &operator).await?;
    assert!(
        remaining.is_empty(),
        "the retraction of 3.0 removes the fact written as 3, and 7 the one written as +7: {remaining:?}"
    );
    Ok(())
}

/// Under a cardinality-one attribute, writing `3` and then `3.0` is not a
/// supersession: the cell already holds the number, so the fact set and
/// the cell's history are unchanged. Writing `4.0` then supersedes it, and
/// a reader sees the winner as the natural `4`.
#[dialog_common::test]
async fn it_does_not_supersede_a_cardinality_one_value_rewritten_in_another_type() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    let item = Entity::new()?;
    let minus = Entity::new()?;
    assert_all(
        &branch,
        [
            fact("item/count", &item, Value::UnsignedInt(3))?,
            fact("item/count", &minus, Value::SignedInt(-1))?,
        ],
        Pick::Last,
        &operator,
    )
    .await?;
    let records = cell_records(&branch, &item, "item/count", &operator).await?;
    assert_eq!(records, 1, "the first write is one record");
    let before = facts(&branch, "item/count", &operator).await?;

    assert_all(
        &branch,
        [
            fact("item/count", &item, Value::Float(3.0))?,
            fact("item/count", &item, Value::SignedInt(3))?,
            fact("item/count", &minus, Value::Float(-1.0))?,
        ],
        Pick::Last,
        &operator,
    )
    .await?;
    assert_eq!(
        facts(&branch, "item/count", &operator).await?,
        before,
        "3.0 and +3 are the number the cell holds: nothing supersedes"
    );
    assert_eq!(
        cell_records(&branch, &item, "item/count", &operator).await?,
        records,
        "no supersession is recorded"
    );
    assert_eq!(
        values(&select(&branch, ArtifactSelector::new().of(item.clone()), &operator).await?),
        vec![Value::UnsignedInt(3)]
    );

    assert_all(
        &branch,
        [fact("item/count", &item, Value::Float(4.0))?],
        Pick::Last,
        &operator,
    )
    .await?;
    assert_eq!(
        values(&select(&branch, ArtifactSelector::new().of(item.clone()), &operator).await?),
        vec![Value::UnsignedInt(4)],
        "4.0 supersedes 3 and reads as the natural 4"
    );
    assert!(
        cell_records(&branch, &item, "item/count", &operator).await? > records,
        "the supersession is recorded"
    );
    Ok(())
}

/// A range over mixed number types comes back in numeric order, every
/// number as the narrowest type holding it: `-0.0` is the natural `0`,
/// `1e30` the natural `10^30`, and `f64::MAX` and the infinities stay
/// floats. The order is the same whether the scan is bounded on the
/// attribute or on the value alone.
#[dialog_common::test]
async fn it_reads_a_mixed_range_in_numeric_order_as_narrowest_types() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    write_sizes(&branch, &edge_spellings(), &operator).await?;

    let by_attribute = select(
        &branch,
        ArtifactSelector::new()
            .the(size())
            .is_at_least(Value::Float(f64::NEG_INFINITY)),
        &operator,
    )
    .await?;
    assert_eq!(values(&by_attribute), edge_readings());

    let by_value = select(
        &branch,
        ArtifactSelector::new().is_at_least(Value::Float(f64::NEG_INFINITY)),
        &operator,
    )
    .await?;
    assert_eq!(values(&by_value), edge_readings());

    // The attribute scan, unbounded on the value, holds the same facts.
    assert_eq!(
        sizes(&branch, ArtifactSelector::new().the(size()), &operator).await?,
        edge_readings()
    );
    Ok(())
}

/// A raw value bound reads every number through the lens of its type, as
/// a field declared with that type does; `across_numbers` compares every
/// number as it is.
#[dialog_common::test]
async fn it_reads_raw_bounds_through_the_lens_of_the_bound() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    write_sizes(&branch, &mixed(), &operator).await?;
    assert_lens_bounds(&branch, &operator).await
}

/// A value constant matches the rows its type reads as the constant:
/// `.is(+4)` and `.is(4)` find a stored `4.3` and `4.9` beside the `4`,
/// `.is(4.0)` finds the number 4 alone, and `.is(-5)` finds `-4.5`, which
/// the integer lens reads as `-5`. The entity-pinned route agrees.
#[dialog_common::test]
async fn it_matches_a_constant_through_the_lens_of_its_type() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    let fraction = Entity::new()?;
    let whole = Entity::new()?;
    assert_all(
        &branch,
        [
            fact("item/size", &fraction, Value::Float(4.3))?,
            fact("item/size", &whole, Value::UnsignedInt(4))?,
            fact("item/size", &Entity::new()?, Value::Float(4.9))?,
            fact("item/size", &Entity::new()?, Value::SignedInt(-4))?,
            fact("item/size", &Entity::new()?, Value::Float(-4.5))?,
            fact("item/size", &Entity::new()?, Value::UnsignedInt(5))?,
        ],
        Pick::All,
        &operator,
    )
    .await?;
    let is = |constant: Value| ArtifactSelector::new().is(constant);
    let through_integer = vec![Value::UnsignedInt(4), Value::Float(4.3), Value::Float(4.9)];
    assert_eq!(
        sizes(&branch, is(Value::SignedInt(4)), &operator).await?,
        through_integer,
        "the integer 4 is what 4, 4.3 and 4.9 read as"
    );
    assert_eq!(
        sizes(&branch, is(Value::UnsignedInt(4)), &operator).await?,
        through_integer,
        "the natural 4 is what 4, 4.3 and 4.9 read as"
    );
    assert_eq!(
        sizes(&branch, is(Value::Float(4.0)), &operator).await?,
        vec![Value::UnsignedInt(4)],
        "the float 4.0 is the number 4 alone"
    );
    assert_eq!(
        sizes(&branch, is(Value::SignedInt(-5)), &operator).await?,
        vec![Value::Float(-4.5)],
        "the integer lens reads -4.5 as -5"
    );
    assert_eq!(
        sizes(&branch, is(Value::SignedInt(-4)), &operator).await?,
        vec![Value::SignedInt(-4)],
        "-4.5 reads as -5, not -4"
    );
    assert_eq!(
        sizes(&branch, is(Value::UnsignedInt(5)), &operator).await?,
        vec![Value::UnsignedInt(5)]
    );
    assert_eq!(
        values(
            &select(
                &branch,
                ArtifactSelector::new()
                    .of(fraction.clone())
                    .is(Value::SignedInt(4)),
                &operator
            )
            .await?
        ),
        vec![Value::Float(4.3)],
        "the entity-pinned route reads 4.3 as the integer 4"
    );
    let none = select(
        &branch,
        ArtifactSelector::new()
            .of(fraction.clone())
            .is(Value::Float(4.0)),
        &operator,
    )
    .await?;
    assert!(none.is_empty(), "4.3 is not the float 4.0: {none:?}");
    assert_eq!(
        values(
            &select(
                &branch,
                ArtifactSelector::new()
                    .of(whole.clone())
                    .is(Value::Float(4.0)),
                &operator
            )
            .await?
        ),
        vec![Value::UnsignedInt(4)]
    );
    Ok(())
}

/// One attribute per fixed-width Rust number type, each a lens over the
/// numbers stored under it.
mod lens {
    use dialog_query::Attribute;

    /// A number read through `u8`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct U8(pub u8);
    /// A number read through `u16`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct U16(pub u16);
    /// A number read through `u32`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct U32(pub u32);
    /// A number read through `u64`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct U64(pub u64);
    /// A number read through `u128`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct U128(pub u128);
    /// A number read through `i8`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct I8(pub i8);
    /// A number read through `i16`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct I16(pub i16);
    /// A number read through `i32`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct I32(pub i32);
    /// A number read through `i64`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct I64(pub i64);
    /// A number read through `i128`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct I128(pub i128);
    /// A number read through `f32`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct F32(pub f32);
    /// A number read through `f64`.
    #[derive(Attribute, Clone, PartialEq)]
    #[domain("lens")]
    pub struct F64(pub f64);
}

/// Declares a one-field concept over a [`lens`] attribute.
macro_rules! lens_concept {
    ($(#[$doc:meta])* $concept:ident, $attribute:ident) => {
        $(#[$doc])*
        #[derive(dialog_query::Concept, Debug, Clone, PartialEq)]
        pub struct $concept {
            /// The item.
            pub this: Entity,
            /// Its number, through the lens.
            pub value: lens::$attribute,
        }
    };
}

lens_concept!(
    /// An item whose number is read through `u8`.
    ByU8, U8
);
lens_concept!(
    /// An item whose number is read through `u16`.
    ByU16, U16
);
lens_concept!(
    /// An item whose number is read through `u32`.
    ByU32, U32
);
lens_concept!(
    /// An item whose number is read through `u64`.
    ByU64, U64
);
lens_concept!(
    /// An item whose number is read through `u128`.
    ByU128, U128
);
lens_concept!(
    /// An item whose number is read through `i8`.
    ByI8, I8
);
lens_concept!(
    /// An item whose number is read through `i16`.
    ByI16, I16
);
lens_concept!(
    /// An item whose number is read through `i32`.
    ByI32, I32
);
lens_concept!(
    /// An item whose number is read through `i64`.
    ByI64, I64
);
lens_concept!(
    /// An item whose number is read through `i128`.
    ByI128, I128
);
lens_concept!(
    /// An item whose number is read through `f32`.
    ByF32, F32
);
lens_concept!(
    /// An item whose number is read through `f64`.
    ByF64, F64
);

/// Stores each case's number under `$the` on its own entity, queries the
/// derived concept over the attribute, and checks that exactly the cases
/// the lens sees come back, each as the lens reads it.
macro_rules! assert_lens {
    ($branch:expr, $operator:expr, $concept:ident, $the:literal, [$(($stored:expr, $seen:expr)),* $(,)?]) => {{
        let cases = vec![$(($stored, $seen)),*];
        let mut expected = Vec::new();
        let mut written = Vec::new();
        for (stored, seen) in cases {
            let item = Entity::new()?;
            written.push(fact($the, &item, stored)?);
            if let Some(seen) = seen {
                expected.push((item.to_string(), seen));
            }
        }
        let stored = written.len();
        assert_all($branch, written, Pick::All, $operator).await?;
        assert_eq!(
            facts($branch, $the, $operator).await?.len(),
            stored,
            "{}: every number is stored, whatever the lens sees",
            $the
        );
        let rows: Vec<$concept> = $branch
            .query()
            .select(Query::<$concept> {
                this: Term::var("item"),
                value: Term::var("value"),
            })
            .perform($operator)
            .try_vec()
            .await?;
        let mut observed: Vec<(String, _)> = rows
            .into_iter()
            .map(|row| (row.this.to_string(), row.value.0))
            .collect();
        observed.sort_by(|a, b| a.0.cmp(&b.0));
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(observed, expected, "{}", $the);
    }};
}

/// Through a derived concept whose field is a fixed-width Rust number, a
/// stored number outside the type's range is not a row at all (skipped,
/// never an error, and never clamped), and one inside it is observed at
/// the type's precision: an integer field floors a fraction, `f32` rounds
/// through single precision, `f64` is exact. Boundary values for every
/// width: the maximum, one past it, the minimum, one below it, the
/// maximum plus a half, and fractions of both signs.
#[dialog_common::test]
async fn it_reads_derived_fixed_width_fields_through_their_lens() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    let natural = |n: u128| Value::UnsignedInt(n);
    let integer = |i: i128| Value::SignedInt(i);
    let float = |f: f64| Value::Float(f);

    assert_lens!(
        &branch,
        &operator,
        ByU8,
        "lens/u8",
        [
            (natural(0), Some(0u8)),
            (float(-0.0), Some(0)),
            (natural(255), Some(255)),
            (natural(256), None),
            (float(255.5), Some(255)),
            (float(-0.5), None),
            (float(2.5), Some(2)),
            (float(0.1), Some(0)),
            (integer(-1), None),
            (integer(7), Some(7)),
            (float(f64::INFINITY), None),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByU16,
        "lens/u16",
        [
            (natural(65535), Some(65535u16)),
            (natural(65536), None),
            (float(65535.5), Some(65535)),
            (float(2.5), Some(2)),
            (integer(-1), None),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByU32,
        "lens/u32",
        [
            (natural(4294967295), Some(4294967295u32)),
            (natural(4294967296), None),
            (float(4294967295.5), Some(4294967295)),
            (float(-2.5), None),
            (integer(300), Some(300)),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByU64,
        "lens/u64",
        [
            (natural(u64::MAX as u128), Some(u64::MAX)),
            (natural(1u128 << 64), None),
            (float(2.5), Some(2)),
            (float(1e30), None),
            (integer(-3), None),
            (integer(-1), None),
            (float(f64::INFINITY), None),
            (float(300.0), Some(300)),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByU128,
        "lens/u128",
        [
            (natural(u128::MAX), Some(u128::MAX)),
            (float(u128::MAX as f64), None),
            (float(f64::MAX), None),
            (float(1e30), Some(1_000_000_000_000_000_000_000_000_000_000)),
            (float(2.5), Some(2)),
            (integer(-1), None),
            (integer(i128::MIN), None),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByI8,
        "lens/i8",
        [
            (integer(-128), Some(-128i8)),
            (integer(127), Some(127)),
            (integer(128), None),
            (integer(-129), None),
            (float(127.5), Some(127)),
            (float(-128.5), None),
            (float(-2.5), Some(-3)),
            (float(2.5), Some(2)),
            (natural(300), None),
            (natural(5), Some(5)),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByI16,
        "lens/i16",
        [
            (integer(32767), Some(32767i16)),
            (integer(32768), None),
            (integer(-32768), Some(-32768)),
            (integer(-32769), None),
            (float(32767.5), Some(32767)),
            (float(-0.5), Some(-1)),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByI32,
        "lens/i32",
        [
            (integer(i32::MAX as i128), Some(i32::MAX)),
            (integer(i32::MAX as i128 + 1), None),
            (integer(i32::MIN as i128), Some(i32::MIN)),
            (integer(i32::MIN as i128 - 1), None),
            (float(i32::MAX as f64 + 0.5), Some(i32::MAX)),
            (natural(1 << 40), None),
            (float(2.5), Some(2)),
            (float(-2.5), Some(-3)),
            (natural(3), Some(3)),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByI64,
        "lens/i64",
        [
            (integer(i64::MAX as i128), Some(i64::MAX)),
            (integer(i64::MAX as i128 + 1), None),
            (integer(i64::MIN as i128), Some(i64::MIN)),
            (integer(i64::MIN as i128 - 1), None),
            (natural(u64::MAX as u128), None),
            (float(-2.5), Some(-3)),
            (float(4.3), Some(4)),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByI128,
        "lens/i128",
        [
            (integer(i128::MIN), Some(i128::MIN)),
            (integer(i128::MAX), Some(i128::MAX)),
            (natural(u128::MAX), None),
            (float(2.5), Some(2)),
            (float(-2.5), Some(-3)),
            (float(f64::NEG_INFINITY), None),
            (float(f64::MAX), None),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByF32,
        "lens/f32",
        [
            (float(0.1), Some(0.1f32)),
            (natural(3), Some(3.0)),
            (integer(-1), Some(-1.0)),
            (float(2.5), Some(2.5)),
            (float(f64::from(f32::MAX)), Some(f32::MAX)),
            (float(f64::MAX), None),
            (float(f64::INFINITY), Some(f32::INFINITY)),
            (float(f64::NEG_INFINITY), Some(f32::NEG_INFINITY)),
            (natural(16777217), Some(16777216.0)),
            (natural(u64::MAX as u128), Some(u64::MAX as f32)),
        ]
    );
    assert_lens!(
        &branch,
        &operator,
        ByF64,
        "lens/f64",
        [
            (natural(0), Some(0.0f64)),
            (float(-0.0), Some(0.0)),
            (float(0.1), Some(0.1)),
            (float(2.5), Some(2.5)),
            (float(-2.5), Some(-2.5)),
            (natural(255), Some(255.0)),
            (integer(-1), Some(-1.0)),
            (natural(u64::MAX as u128), Some(u64::MAX as f64)),
            (natural(u128::MAX), Some(u128::MAX as f64)),
            (integer(i128::MIN), Some(i128::MIN as f64)),
            (float(1e30), Some(1e30)),
            (float(f64::MAX), Some(f64::MAX)),
            (float(f64::INFINITY), Some(f64::INFINITY)),
            (float(f64::NEG_INFINITY), Some(f64::NEG_INFINITY)),
        ]
    );
    Ok(())
}

/// A one-field concept `{ value }` over `the`, declared `as` the given
/// type and read under `last`.
fn typed_concept(the: &str, declared: &str) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": { "value": { "the": the, "as": declared, "pick": "last" } }
    }))
    .expect("a one-field concept is well formed")
}

/// NaN is not a value: a commit that would write one, by a raw
/// assertion that no field type checks, is refused with
/// [`CommitError::Artifact`] (`InvalidValue`, "NaN is not a value") and
/// lands nothing: the branch has no revision and holds no fact after it.
/// The infinities are values and land.
#[dialog_common::test]
async fn it_refuses_a_commit_that_would_write_nan() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    let item = Entity::new()?;
    for nan in [f64::NAN, -f64::NAN] {
        let refusal = branch
            .transaction()
            .assert(the!("item/size").of(item.clone()).is(nan))
            .commit()
            .publish()
            .perform(&operator)
            .await
            .err();
        assert!(
            matches!(
                &refusal,
                Some(CommitError::Artifact(DialogArtifactsError::InvalidValue(reason)))
                    if reason == "NaN is not a value"
            ),
            "{refusal:?}"
        );
        assert_eq!(branch.revision(), None, "nothing landed");
        assert!(facts(&branch, "item/size", &operator).await?.is_empty());
    }

    branch
        .transaction()
        .assert(the!("item/size").of(item.clone()).is(f64::INFINITY))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        facts(&branch, "item/size", &operator).await?,
        vec![(item, Value::Float(f64::INFINITY))]
    );
    Ok(())
}

/// A write through a typed concept takes only what the type holds: a
/// fraction into a natural, `256` into `natural:8`, a negative into a
/// natural, `2^40` into `integer:32` and `f64::MAX` into `float:32` are
/// refused with [`TypeError::TypeMismatch`] when the statement is built,
/// before any transaction exists, so nothing reaches the branch. What the
/// type holds lands and reads as the narrowest type.
#[dialog_common::test]
async fn it_refuses_a_typed_write_outside_the_field_type_before_commit() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    assert_eq!(branch.revision(), None);

    let refused: Vec<(&str, Value)> = vec![
        ("natural:64", Value::Float(2.5)),
        ("natural:8", Value::UnsignedInt(256)),
        ("natural:8", Value::Float(255.5)),
        ("natural:", Value::SignedInt(-1)),
        ("integer:32", Value::UnsignedInt(1 << 40)),
        ("integer:8", Value::Float(-128.5)),
        ("float:32", Value::Float(f64::MAX)),
    ];
    for (declared, value) in refused {
        let concept = typed_concept("item/size", declared);
        let refusal = match value.clone() {
            Value::UnsignedInt(natural) => concept.create().with("value", natural).build(),
            Value::SignedInt(integer) => concept.create().with("value", integer).build(),
            Value::Float(float) => concept.create().with("value", float).build(),
            other => panic!("{other:?} is not a number"),
        }
        .err();
        assert!(
            matches!(refusal, Some(TypeError::TypeMismatch { .. })),
            "{declared} refuses {value:?}: {refusal:?}"
        );
    }
    assert_eq!(
        branch.revision(),
        None,
        "a refused statement reaches no commit"
    );
    assert!(facts(&branch, "item/size", &operator).await?.is_empty());

    let accepted = typed_concept("item/size", "natural:8")
        .create()
        .with("value", 7.0f64)
        .build()?;
    let item = accepted.this.clone();
    branch
        .transaction()
        .assert(accepted)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    assert_eq!(
        values(&select(&branch, ArtifactSelector::new().of(item), &operator).await?),
        vec![Value::UnsignedInt(7)],
        "7.0 is what natural:8 holds, read as the natural 7"
    );
    Ok(())
}

/// Size of an item, read through `i64`.
mod typed {
    /// Size of an item as a signed 64-bit integer.
    #[derive(dialog_query::Attribute, Clone, PartialEq)]
    #[domain("item")]
    pub struct Size(pub i64);
}

/// An item sized through an `i64` lens.
#[derive(dialog_query::Concept, Debug, Clone, PartialEq)]
pub struct Sized {
    /// The item.
    pub this: Entity,
    /// Its size.
    pub size: typed::Size,
}

/// What [`Sized`] observes for `item`.
async fn observed_size(branch: &Branch, item: &Entity, operator: &Operator) -> Result<Vec<i64>> {
    Ok(branch
        .query()
        .select(Query::<Sized> {
            this: Term::from(item.clone()),
            size: Term::var("size"),
        })
        .perform(operator)
        .try_vec()
        .await?
        .into_iter()
        .map(|sized| sized.size.0)
        .collect())
}

/// A cardinality-one write through a typed field of a value the field
/// already observes is a no-op: over a stored `4.3`, an `i64` field
/// observes `4`, and asserting `4` through it (or the raw natural `4`)
/// changes no fact and records nothing, while a raw read still sees
/// `4.3`. Asserting `5` supersedes `4.3` with `5`.
#[dialog_common::test]
async fn it_treats_a_typed_write_the_field_already_observes_as_a_no_op() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    let item = Entity::new()?;
    branch
        .transaction()
        .assert(
            the!("item/size")
                .of(item.clone())
                .is(4.3f64)
                .cardinality(Cardinality::One),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    assert_eq!(observed_size(&branch, &item, &operator).await?, vec![4]);
    let raw = ArtifactSelector::new().of(item.clone());
    assert_eq!(
        values(&select(&branch, raw.clone(), &operator).await?),
        vec![Value::Float(4.3)]
    );
    let records = cell_records(&branch, &item, "item/size", &operator).await?;
    assert_eq!(records, 1);

    branch
        .transaction()
        .assert(Sized {
            this: item.clone(),
            size: typed::Size(4),
        })
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    assert_eq!(
        values(&select(&branch, raw.clone(), &operator).await?),
        vec![Value::Float(4.3)],
        "the field observes 4 already: the write is a no-op and 4.3 stands"
    );
    assert_eq!(observed_size(&branch, &item, &operator).await?, vec![4]);
    assert_eq!(
        cell_records(&branch, &item, "item/size", &operator).await?,
        records,
        "a no-op records nothing"
    );

    assert_all(
        &branch,
        [fact("item/size", &item, Value::UnsignedInt(4))?],
        Pick::Last,
        &operator,
    )
    .await?;
    assert_eq!(
        values(&select(&branch, raw.clone(), &operator).await?),
        vec![Value::Float(4.3)],
        "the raw natural 4 is what the cell reads as too"
    );
    assert_eq!(
        cell_records(&branch, &item, "item/size", &operator).await?,
        records
    );

    branch
        .transaction()
        .assert(Sized {
            this: item.clone(),
            size: typed::Size(5),
        })
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    assert_eq!(
        values(&select(&branch, raw, &operator).await?),
        vec![Value::UnsignedInt(5)],
        "5 is not what the cell reads as: it supersedes 4.3"
    );
    assert_eq!(observed_size(&branch, &item, &operator).await?, vec![5]);
    assert!(
        cell_records(&branch, &item, "item/size", &operator).await? > records,
        "the supersession is recorded"
    );
    Ok(())
}

/// Writes every edge spelling to `branch`, with the zero and the one
/// spelled three ways on one entity each, and returns the fact set.
async fn write_edges(branch: &Branch, operator: &Operator) -> Result<Vec<(Entity, Value)>> {
    write_sizes(branch, &edge_spellings(), operator).await?;
    let zero = Entity::new()?;
    let one = Entity::new()?;
    assert_all(
        branch,
        [
            fact("item/size", &zero, Value::UnsignedInt(0))?,
            fact("item/size", &zero, Value::SignedInt(0))?,
            fact("item/size", &zero, Value::Float(-0.0))?,
            fact("item/size", &one, Value::Float(1.0))?,
            fact("item/size", &one, Value::SignedInt(1))?,
            fact("item/size", &one, Value::UnsignedInt(1))?,
        ],
        Pick::All,
        operator,
    )
    .await?;
    let before = facts(branch, "item/size", operator).await?;
    assert_eq!(
        before.len(),
        edge_spellings().len() + 2,
        "one fact per spelling entity, one each for the zero and the one"
    );
    Ok(before)
}

/// Every edge spelling, plus the zero and the one, in numeric order.
fn edge_readings_with_dedupes() -> Vec<Value> {
    let mut readings = edge_readings();
    readings.push(Value::UnsignedInt(0));
    readings.push(Value::UnsignedInt(1));
    readings.sort_by(by_number);
    readings
}

/// The reads a migrated tree answers like a fresh one: numeric order with
/// narrowest types, and lensed bounds over the edge numbers.
async fn assert_edge_reads(branch: &Branch, operator: &Operator) -> Result<()> {
    assert_eq!(
        values(
            &select(
                branch,
                ArtifactSelector::new().is_at_least(Value::Float(f64::NEG_INFINITY)),
                operator
            )
            .await?
        ),
        edge_readings_with_dedupes(),
        "numeric order, narrowest types"
    );
    assert_eq!(
        sizes(
            branch,
            ArtifactSelector::new().is_at_least(Value::UnsignedInt(1u128 << 64)),
            operator
        )
        .await?,
        vec![
            Value::UnsignedInt(1u128 << 64),
            Value::UnsignedInt(1u128 << 64),
            Value::UnsignedInt(1_000_000_000_000_000_000_000_000_000_000),
            Value::UnsignedInt(u128::MAX),
        ],
        "a natural bound does not see f64::MAX or infinity, which no natural holds"
    );
    assert_eq!(
        sizes(
            branch,
            ArtifactSelector::new().is_at_least(Value::Float(1e30)),
            operator
        )
        .await?,
        vec![
            Value::UnsignedInt(1_000_000_000_000_000_000_000_000_000_000),
            Value::UnsignedInt(u128::MAX),
            Value::Float(f64::MAX),
            Value::Float(f64::INFINITY),
        ],
        "a float bound sees every number"
    );
    assert_eq!(
        sizes(
            branch,
            ArtifactSelector::new().is_at_most(Value::Float(-1.0)),
            operator
        )
        .await?,
        vec![
            Value::Float(f64::NEG_INFINITY),
            Value::SignedInt(i128::MIN),
            Value::Float(-2.5),
            Value::SignedInt(-1),
            Value::SignedInt(-1),
        ],
        "a float bound <= -1.0 sees -infinity"
    );
    assert_eq!(
        sizes(
            branch,
            ArtifactSelector::new().is_at_most(Value::SignedInt(-1)),
            operator
        )
        .await?,
        vec![
            Value::SignedInt(i128::MIN),
            Value::Float(-2.5),
            Value::SignedInt(-1),
            Value::SignedInt(-1),
        ],
        "an integer bound <= -1 reads -2.5 as -3; it floors only finite numbers, so -infinity is unseen"
    );
    assert_eq!(
        sizes(
            branch,
            ArtifactSelector::new().is_at_most(Value::UnsignedInt(0)),
            operator
        )
        .await?,
        vec![
            Value::UnsignedInt(0),
            Value::UnsignedInt(0),
            Value::UnsignedInt(0),
            Value::UnsignedInt(0),
            Value::UnsignedInt(0),
            Value::Float(0.1),
        ],
        "a natural bound <= 0 reads 0.1 as 0 and sees no negative"
    );
    Ok(())
}

/// A branch holding every edge number in every type, laid out the way the
/// release before the band wrote: `needs_migration` is true, a commit is
/// refused with [`CommitError::NeedsMigration`] and writes nothing, and
/// `migrate` rewrites the tree losslessly, after which every read holds
/// (one fact per number however it was spelled, numeric order, lensed
/// bounds), a commit lands, and a second `migrate` finds nothing to do.
#[dialog_common::test]
async fn it_migrates_a_branch_holding_every_edge_number_losslessly() -> Result<()> {
    let (operator, _repo, branch) = open().await?;
    let before = write_edges(&branch, &operator).await?;
    assert_edge_reads(&branch, &operator).await?;

    let rewritten = branch.rekey_storage(0).perform(&operator).await?;
    assert!(rewritten.is_some(), "the head is rewritten per type");
    branch.refresh(&operator).await?;
    assert_eq!(branch.storage_encoding(&operator).await?, Some(0));
    assert!(branch.needs_migration(&operator).await?);
    assert_eq!(
        facts(&branch, "item/size", &operator).await?,
        before,
        "the per-type layout holds the same facts"
    );
    let head = branch.revision();

    let refused = assert_all(
        &branch,
        [fact("item/size", &Entity::new()?, Value::UnsignedInt(7))?],
        Pick::All,
        &operator,
    )
    .await;
    assert!(
        matches!(refused, Err(CommitError::NeedsMigration(_))),
        "{refused:?}"
    );
    branch.refresh(&operator).await?;
    assert_eq!(branch.revision(), head, "a refused write moves nothing");
    assert_eq!(facts(&branch, "item/size", &operator).await?, before);

    let migrated = branch.migrate().perform(&operator).await?;
    branch.refresh(&operator).await?;
    assert!(migrated.storage.is_some(), "the storage is rewritten");
    assert!(!branch.needs_migration(&operator).await?);
    assert_eq!(branch.storage_encoding(&operator).await?, Some(NUMBER_BAND));
    assert_eq!(
        facts(&branch, "item/size", &operator).await?,
        before,
        "every number survives the migration"
    );
    assert_edge_reads(&branch, &operator).await?;

    let again = branch.migrate().perform(&operator).await?;
    assert!(
        again.storage.is_none(),
        "a second run finds nothing: {again:?}"
    );

    assert_all(
        &branch,
        [fact("item/size", &Entity::new()?, Value::UnsignedInt(7))?],
        Pick::All,
        &operator,
    )
    .await?;
    assert_eq!(
        facts(&branch, "item/size", &operator).await?.len(),
        before.len() + 1,
        "the migrated branch takes writes"
    );
    Ok(())
}

/// A branch that pulls numbers of mixed types from its upstream reads
/// them as the upstream does: the same fact set, the same narrowest types,
/// and raw bounds that answer identically on both sides.
#[dialog_common::test]
async fn it_pulls_mixed_numbers_as_the_narrowest_types() -> Result<()> {
    let (operator, repo, main) = open().await?;
    write_sizes(&main, &mixed(), &operator).await?;
    let item = Entity::new()?;
    assert_all(
        &main,
        [
            fact("item/size", &item, Value::Float(3.0))?,
            fact("item/size", &item, Value::UnsignedInt(3))?,
        ],
        Pick::All,
        &operator,
    )
    .await?;
    let upstream = facts(&main, "item/size", &operator).await?;

    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature.set_upstream(&main).perform(&operator).await?;
    feature.pull().perform(&operator).await?;
    feature.refresh(&operator).await?;

    assert_eq!(facts(&feature, "item/size", &operator).await?, upstream);
    assert_eq!(
        values(&select(&feature, ArtifactSelector::new().of(item), &operator).await?),
        vec![Value::UnsignedInt(3)],
        "3.0 and 3 pulled as one fact"
    );
    for branch in [&main, &feature] {
        assert_eq!(
            sizes(
                branch,
                ArtifactSelector::new().is_at_least(Value::Float(f64::NEG_INFINITY)),
                &operator
            )
            .await?,
            {
                let mut all = mixed_sorted();
                all.push(Value::UnsignedInt(3));
                all.sort_by(by_number);
                all
            }
        );
    }
    // The lens bounds, with the extra 3 beside the mixed numbers.
    for branch in [&main, &feature] {
        assert_eq!(
            sizes(
                branch,
                ArtifactSelector::new().is_at_least(Value::UnsignedInt(2)),
                &operator
            )
            .await?,
            vec![
                Value::Float(2.5),
                Value::UnsignedInt(3),
                Value::UnsignedInt(3),
                Value::UnsignedInt(5)
            ]
        );
        assert_eq!(
            sizes(
                branch,
                ArtifactSelector::new()
                    .is_at_most(Value::UnsignedInt(2))
                    .across_numbers(),
                &operator
            )
            .await?,
            vec![Value::SignedInt(-3), Value::Float(-2.5), Value::Float(1.5)]
        );
        assert_eq!(
            sizes(
                branch,
                ArtifactSelector::new().is_at_least(Value::SignedInt(-3)),
                &operator
            )
            .await?
            .len(),
            7
        );
    }
    Ok(())
}

/// A replica on this release pulls an upstream holding every edge number
/// that the release before the band wrote: a first pull adopts the head as
/// it is and the migration reproduces the upstream's facts exactly, every
/// read holding afterwards; a later pull that merges the earlier
/// replica's further writes with this replica's own lands in the band with
/// nothing lost.
#[dialog_common::test]
async fn it_pulls_a_numeric_heavy_upstream_an_earlier_release_wrote() -> Result<()> {
    let (operator, repo, main) = open().await?;
    let first = write_edges(&main, &operator).await?;
    main.rekey_storage(0).perform(&operator).await?;
    main.refresh(&operator).await?;
    assert_eq!(main.storage_encoding(&operator).await?, Some(0));
    assert_eq!(facts(&main, "item/size", &operator).await?, first);

    let feature = repo.branch("feature").open().perform(&operator).await?;
    feature.set_upstream(&main).perform(&operator).await?;
    feature.pull().perform(&operator).await?;
    feature.refresh(&operator).await?;
    assert_eq!(feature.storage_encoding(&operator).await?, Some(0));
    assert!(feature.needs_migration(&operator).await?);
    assert_eq!(facts(&feature, "item/size", &operator).await?, first);

    let migrated = feature.migrate().perform(&operator).await?;
    feature.refresh(&operator).await?;
    assert!(migrated.storage.is_some());
    assert_eq!(
        feature.storage_encoding(&operator).await?,
        Some(NUMBER_BAND)
    );
    assert_eq!(facts(&feature, "item/size", &operator).await?, first);
    assert_edge_reads(&feature, &operator).await?;

    // The earlier replica writes more numbers the earlier way.
    main.migrate_storage().perform(&operator).await?;
    main.refresh(&operator).await?;
    let late = Entity::new()?;
    assert_all(
        &main,
        [fact("item/size", &late, Value::SignedInt(-99))?],
        Pick::All,
        &operator,
    )
    .await?;
    main.rekey_storage(0).perform(&operator).await?;
    main.refresh(&operator).await?;
    assert_eq!(main.storage_encoding(&operator).await?, Some(0));

    // This replica writes too, so the pull merges.
    let own = Entity::new()?;
    assert_all(
        &feature,
        [fact("item/size", &own, Value::Float(0.5))?],
        Pick::All,
        &operator,
    )
    .await?;
    feature.pull().perform(&operator).await?;
    feature.refresh(&operator).await?;
    assert_eq!(
        feature.storage_encoding(&operator).await?,
        Some(NUMBER_BAND)
    );
    let merged = facts(&feature, "item/size", &operator).await?;
    assert_eq!(merged.len(), first.len() + 2, "{merged:?}");
    assert!(merged.contains(&(late, Value::SignedInt(-99))));
    assert!(merged.contains(&(own, Value::Float(0.5))));
    for fact in &first {
        assert!(merged.contains(fact), "{fact:?} survives the merge");
    }
    Ok(())
}
