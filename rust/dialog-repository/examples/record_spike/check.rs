//! The canonical form's properties, over generated records. Each check is
//! a claim the topic makes; a failure names the seed that broke it.

use super::Seeded;
use super::codec::{self, Field, Record};
use anyhow::{Result, anyhow, ensure};
use dialog_corpus::Value;
use std::cmp::Ordering;

const ATTRIBUTES: [&str; 6] = [
    "geo/lat",
    "geo/lon",
    "tonk.event/name",
    "tonk.event/source",
    "x/tag",
    "x.y/zero",
];

fn value(random: &mut Seeded) -> Value {
    match random.below(8) {
        0 => Value::UnsignedInt(random.below(1000) as u128),
        1 => Value::SignedInt(random.below(1000) as i128 - 500),
        2 => Value::Float((random.below(2000) as f64 - 1000.0) / 8.0),
        3 => Value::Boolean(random.below(2) == 1),
        4 => Value::String(format!("s{}", random.below(40))),
        // A string with a zero byte, which the encoding escapes.
        5 => Value::String(format!("a\0{}", random.below(5))),
        6 => Value::Bytes(
            (0..random.below(5))
                .map(|_| random.below(3) as u8)
                .collect(),
        ),
        _ => Value::Entity(format!("id:e{}", random.below(30)).parse().expect("entity")),
    }
}

/// A record of up to six pairs, a pair in four holding a nested record
/// while `depth` allows, attributes repeating (a set).
pub fn record(random: &mut Seeded, depth: usize) -> Record {
    let count = random.below(7);
    let mut pairs = Vec::new();
    for _ in 0..count {
        let attribute = ATTRIBUTES[random.below(ATTRIBUTES.len())].to_string();
        let field = if depth > 0 && random.below(4) == 0 {
            Field::Record(record(random, depth - 1))
        } else {
            Field::Value(value(random))
        };
        pairs.push((attribute, field));
    }
    Record(pairs)
}

/// The order the encoding should agree with: pairs sorted and deduplicated
/// by meaning, compared one by one, a record that is a prefix of another
/// first. Attributes by their names, fields by type then value, numbers as
/// numbers whatever type holds them, records recursively.
fn semantic(record: &Record) -> Vec<(String, Field)> {
    let mut pairs: Vec<(String, Field)> = record
        .0
        .iter()
        .map(|(a, f)| (a.clone(), normal(f)))
        .collect();
    pairs.sort_by(compare_pair);
    pairs.dedup_by(|a, b| compare_pair(a, b) == Ordering::Equal);
    pairs
}

fn normal(field: &Field) -> Field {
    match field {
        Field::Record(record) => Field::Record(Record(semantic(record))),
        Field::Value(value) => Field::Value(value.clone()),
    }
}

fn number(value: &Value) -> Option<f64> {
    match value {
        Value::UnsignedInt(n) => Some(*n as f64),
        Value::SignedInt(n) => Some(*n as f64),
        Value::Float(n) => Some(*n),
        _ => None,
    }
}

fn rank(field: &Field) -> u8 {
    match field {
        Field::Value(v) if number(v).is_some() => dialog_corpus::ValueDataType::Number as u8,
        Field::Value(v) => v.data_type() as u8,
        Field::Record(_) => dialog_corpus::ValueDataType::Record as u8,
    }
}

fn compare_field(a: &Field, b: &Field) -> Ordering {
    rank(a).cmp(&rank(b)).then_with(|| match (a, b) {
        (Field::Record(a), Field::Record(b)) => compare_records(&a.0, &b.0),
        (Field::Value(a), Field::Value(b)) => match (number(a), number(b)) {
            (Some(x), Some(y)) => x.partial_cmp(&y).expect("no NaN"),
            _ => match (a, b) {
                (Value::String(x), Value::String(y)) => x.as_bytes().cmp(y.as_bytes()),
                (Value::Bytes(x), Value::Bytes(y)) => x.cmp(y),
                (Value::Entity(x), Value::Entity(y)) => x.as_str().cmp(y.as_str()),
                (Value::Boolean(x), Value::Boolean(y)) => x.cmp(y),
                _ => Ordering::Equal,
            },
        },
        _ => Ordering::Equal,
    })
}

fn compare_pair(a: &(String, Field), b: &(String, Field)) -> Ordering {
    a.0.as_bytes()
        .cmp(b.0.as_bytes())
        .then_with(|| compare_field(&a.1, &b.1))
}

fn compare_records(a: &[(String, Field)], b: &[(String, Field)]) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        match compare_pair(x, y) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

/// Runs every check over `seeds` generated records; returns how many
/// records and pairs of records it checked.
pub fn run(seeds: u64) -> Result<(u64, u64)> {
    let mut checked = (0, 0);
    let mut previous: Option<Record> = None;
    for seed in 1..=seeds {
        let mut random = Seeded(seed);
        let record = record(&mut random, 2);
        let bytes = codec::encode(&record);
        let fail = |what: &str| anyhow!("seed {seed}: {what}: {record:?}");

        // It reads back, and writing what it read gives the same bytes.
        let read = codec::decode(&bytes).ok_or_else(|| fail("does not decode"))?;
        ensure!(
            codec::encode(&read) == bytes,
            fail("does not re-encode the same")
        );
        // It delimits itself.
        ensure!(
            codec::record_len(&bytes) == Some(bytes.len()),
            fail("its length is not its own")
        );
        // Order and repeats do not change it.
        let mut shuffled = record.0.clone();
        shuffled.reverse();
        if let Some(first) = record.0.first().cloned() {
            shuffled.push(first);
        }
        ensure!(
            codec::encode(&Record(shuffled)) == bytes,
            fail("pair order or a repeat changes the bytes")
        );
        // A number is one value whatever type holds it.
        let as_float = Record(
            record
                .0
                .iter()
                .map(|(a, f)| match f {
                    Field::Value(Value::UnsignedInt(n)) => {
                        (a.clone(), Field::Value(Value::Float(*n as f64)))
                    }
                    other => (a.clone(), other.clone()),
                })
                .collect(),
        );
        ensure!(
            codec::encode(&as_float) == bytes,
            fail("a natural and the float of it encode differently")
        );
        // A spilled prefix is a record that sorts before the whole.
        let prefix = codec::prefix(&bytes, bytes.len() / 2 + 1).ok_or_else(|| fail("no prefix"))?;
        ensure!(
            codec::decode(&prefix).is_some(),
            fail("the prefix is not a record")
        );
        ensure!(prefix <= bytes, fail("the prefix sorts after the record"));
        // Looking a field up agrees with decoding the whole.
        for (attribute, _) in &record.0 {
            let found = codec::slots(&bytes, &codec::attribute_key(attribute))
                .ok_or_else(|| fail("lookup fails"))?;
            let want = semantic(&record)
                .into_iter()
                .filter(|(a, _)| a == attribute)
                .count();
            ensure!(found.len() == want, fail("lookup finds another count"));
        }
        checked.0 += 1;

        // The bytes compare as the pairs do.
        if let Some(previous) = previous.replace(record.clone()) {
            let by_bytes = codec::encode(&previous).cmp(&bytes);
            let by_meaning = compare_records(&semantic(&previous), &semantic(&record));
            ensure!(
                by_bytes == by_meaning,
                anyhow!(
                    "seed {seed}: bytes order {by_bytes:?}, meaning {by_meaning:?}:\n{previous:?}\n{record:?}"
                )
            );
            checked.1 += 1;
        }
    }
    Ok(checked)
}
