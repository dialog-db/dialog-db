//! Numbers in the index: a commit of numeric facts, point reads, a scan,
//! and value ranges narrow, wide and across number types. They measure
//! how numbers are keyed: run them as they are for a band per type, and
//! with `DIALOG_TREE_VALUE_ENCODING=1` for every number in one band.

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use dialog_artifacts::{Pick, Value};
use dialog_query::attribute::query::all::AttributeQueryAll;
use dialog_query::query::Output as _;
use dialog_query::type_system::{Primitive, Type as Kind};
use dialog_query::{ConceptDescriptor, Entity, Term};

use super::read::read;
use super::{Outcome, Prepared};
use crate::env::{Env, assert_all, attribute, concept, entity, fact};

/// How many point reads, or narrow ranges, a scenario makes.
const PROBES: usize = 200;

/// How many narrow ranges a scenario makes.
const WINDOWS: usize = 20;

/// The first item's timestamp, in milliseconds.
const EPOCH: i128 = 1_760_000_000_000;

/// Milliseconds between two items' timestamps.
const STEP: i128 = 60_000;

/// The `index`th item's timestamp.
fn at(index: usize) -> i128 {
    EPOCH + index as i128 * STEP
}

/// The `index`th item's price: cents spread over 0.01 to 999.99, never a
/// whole number, so it reads back as a float in either encoding.
fn price(index: usize) -> f64 {
    let mut cents = (index * 7919) % 99_999 + 1;
    if cents.is_multiple_of(100) {
        cents += 1;
    }
    cents as f64 / 100.0
}

/// The `index`th item's count: a small integer.
fn count(index: usize) -> i128 {
    ((index * 31) % 1000) as i128
}

/// The `index`th item's size, a number written as a natural, an integer
/// or a float in turn: one relation holding all three, as JSON writes it.
fn magnitude(index: usize) -> Value {
    match index % 3 {
        0 => Value::UnsignedInt(index as u128),
        1 => Value::SignedInt(index as i128),
        _ => Value::Float(index as f64 + 0.5),
    }
}

/// The `item` population's price alone.
fn item_price() -> ConceptDescriptor {
    concept(serde_json::json!({ "with": {
        "price": { "the": "item/price", "as": "float:" }
    }}))
}

/// Commit `count` items, each with a timestamp, a price and a count.
async fn seed_items(env: &Env, size: usize) -> Result<()> {
    let facts = (0..size).flat_map(|index| {
        let this = entity("item", index);
        [
            fact(&this, "item/at", Value::SignedInt(at(index))),
            fact(&this, "item/price", Value::Float(price(index))),
            fact(&this, "item/count", Value::SignedInt(count(index))),
            fact(&this, "item/size", magnitude(index)),
        ]
    });
    env.commit(assert_all(facts, Pick::Last)).await
}

/// Every claim of `the` whose value lies in `from..=to` (or from `from` on).
async fn range(env: &Env, the: &str, from: Value, to: Option<Value>) -> Result<u64> {
    ranged(env, the, Kind::from(from.data_type()), from, to).await
}

/// Every claim of `the` read as `kind` whose value lies in `from..=to`.
async fn ranged(env: &Env, the: &str, kind: Kind, from: Value, to: Option<Value>) -> Result<u64> {
    let mut kind = kind
        .with_interval(&from, true, true)
        .expect("a number admits an interval");
    if let Some(to) = &to {
        kind = kind
            .with_interval(to, true, false)
            .expect("the meet keeps both bounds");
    }
    let query = AttributeQueryAll::new(
        Term::from(dialog_query::Relation::from(attribute(the))),
        Term::<Entity>::var("item"),
        Term::var("value").with_kind(kind),
        Term::var("cause"),
    );
    let rows = env
        .branch
        .select(query)
        .perform(&env.operator)
        .try_vec()
        .await?;
    // A bound the scan could not take is the comparison's to apply, as a
    // query's `>=` and `<=` would, so every encoding reports the same rows.
    let (from, to) = (number(&from), to.as_ref().map(number));
    Ok(rows
        .iter()
        .map(|claim| number(claim.is()))
        .filter(|value| *value >= from && to.is_none_or(|to| *value <= to))
        .count() as u64)
}

/// A number as a float, close enough to compare these scenarios' values.
fn number(value: &Value) -> f64 {
    match value {
        Value::UnsignedInt(value) => *value as f64,
        Value::SignedInt(value) => *value as f64,
        Value::Float(value) => *value,
        _ => f64::NAN,
    }
}

struct Commit {
    env: Env,
    size: usize,
}

impl Prepared for Commit {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            seed_items(&self.env, self.size).await?;
            Ok(Outcome::from([("facts".into(), 4 * self.size as u64)]))
        })
    }
}

/// `number-commit`: an empty branch, then one transaction of `size`
/// items, three numbers each.
pub async fn commit(size: usize) -> Result<Box<dyn Prepared>> {
    let env = Env::open().await?;
    Ok(Box::new(Commit { env, size }))
}

/// A scenario over `size` seeded items.
async fn seeded(size: usize) -> Result<Env> {
    let mut env = Env::open().await?;
    seed_items(&env, size).await?;
    env.reopen().await?;
    Ok(env)
}

struct Point {
    env: Env,
    size: usize,
}

impl Prepared for Point {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let step = (self.size / PROBES).max(1);
            let mut rows = 0u64;
            for probe in 0..PROBES {
                let this = entity("item", (probe * step) % self.size);
                rows += read(&self.env, &item_price(), &["price"], Some(&this))
                    .await?
                    .len() as u64;
            }
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `number-point`: `size` items, then `PROBES` single-item price reads.
pub async fn point(size: usize) -> Result<Box<dyn Prepared>> {
    let env = seeded(size).await?;
    Ok(Box::new(Point { env, size }))
}

struct Scan {
    env: Env,
}

impl Prepared for Scan {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = read(&self.env, &item_price(), &["price"], None).await?;
            Ok(Outcome::from([("rows".into(), rows.len() as u64)]))
        })
    }
}

/// `number-scan`: `size` items, then one read of every price.
pub async fn scan(size: usize) -> Result<Box<dyn Prepared>> {
    let env = seeded(size).await?;
    Ok(Box::new(Scan { env }))
}

struct Narrow {
    env: Env,
    size: usize,
}

impl Prepared for Narrow {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let step = (self.size / WINDOWS).max(1);
            let mut rows = 0u64;
            for window in 0..WINDOWS {
                let first = (window * step) % self.size.saturating_sub(10).max(1);
                rows += range(
                    &self.env,
                    "item/at",
                    Value::SignedInt(at(first)),
                    Some(Value::SignedInt(at(first + 9))),
                )
                .await?;
            }
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `number-range-narrow`: `size` items, then `WINDOWS` reads of the ten
/// items in a window of timestamps.
pub async fn narrow(size: usize) -> Result<Box<dyn Prepared>> {
    let env = seeded(size).await?;
    Ok(Box::new(Narrow { env, size }))
}

struct Wide {
    env: Env,
}

impl Prepared for Wide {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let rows = range(&self.env, "item/price", Value::Float(500.0), None).await?;
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `number-range-wide`: `size` items, then one read of every item priced
/// at 500 or more, about half of them.
pub async fn wide(size: usize) -> Result<Box<dyn Prepared>> {
    let env = seeded(size).await?;
    Ok(Box::new(Wide { env }))
}

struct Mixed {
    env: Env,
    size: usize,
}

impl Prepared for Mixed {
    fn env(&self) -> &Env {
        &self.env
    }

    fn measure<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = Result<Outcome>> + 'a>> {
        Box::pin(async move {
            let from = Value::SignedInt((self.size - self.size / 10) as i128);
            let kind = Kind::from(Primitive::NUMERIC);
            let rows = ranged(&self.env, "item/size", kind, from, None).await?;
            Ok(Outcome::from([("rows".into(), rows)]))
        })
    }
}

/// `number-range-mixed`: `size` items, then one read of the tenth of them
/// with the largest size, a relation holding naturals, integers and floats.
pub async fn mixed(size: usize) -> Result<Box<dyn Prepared>> {
    let env = seeded(size).await?;
    Ok(Box::new(Mixed { env, size }))
}
