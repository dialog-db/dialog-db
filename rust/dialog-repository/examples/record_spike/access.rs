//! Reaching one part of a composite (`design/composite-value.md`): a field
//! of a map and an item of a list by a scan of their slots against a table
//! of offsets, and a field three composites down by writing each nested
//! composite again (draft #812) against a view of the one buffer. Then the
//! notation's syntax trees, by size.
//!
//! Three readers of each map:
//!
//! - **#812**: [`super::pairs812`], which parses every name and walks the
//!   fields before the one it wants, a nested composite by reading it;
//! - **scan**: the composite with no offsets anywhere, stepping over the
//!   entries before the one it wants, a nested composite by its size;
//! - **offsets**: the composite with offsets in every sequence of two or
//!   more, halving over them.

use super::pairs812::{Field, Pairs};
use anyhow::{Context, Result, anyhow};
use dialog_corpus::{Composite, Relation, Value};
use std::hint::black_box;
use std::time::Instant;

const SIZES: [usize; 9] = [2, 4, 8, 12, 16, 32, 64, 128, 256];

fn relation(name: &str) -> Relation {
    name.parse().expect("a relation")
}

fn attribute(index: usize) -> Relation {
    relation(&format!("syntax/f{index:03}"))
}

/// Nanoseconds a call of `run`, the median of `rounds` rounds of `repeat`
/// calls each, with the least and the greatest.
fn time(rounds: usize, repeat: usize, mut run: impl FnMut() -> usize) -> (f64, f64, f64) {
    let mut samples: Vec<f64> = (0..rounds)
        .map(|_| {
            let start = Instant::now();
            let mut found = 0;
            for _ in 0..repeat {
                found += run();
            }
            black_box(found);
            start.elapsed().as_nanos() as f64 / repeat as f64
        })
        .collect();
    samples.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
    (
        samples[0],
        samples[samples.len() / 2],
        samples[samples.len() - 1],
    )
}

/// A syntax node's shape: a map of three fields, `type`, `name` and
/// `value`.
fn node_fields(index: usize) -> Vec<(Relation, Field)> {
    vec![
        (
            relation("syntax/type"),
            Field::Value(Value::Entity("constant:".parse().expect("an entity"))),
        ),
        (
            relation("syntax/name"),
            Field::Value(Value::String(format!("n{index}"))),
        ),
        (
            relation("syntax/value"),
            Field::Value(Value::UnsignedInt(index as u128)),
        ),
    ]
}

fn to_value(field: &Field) -> Value {
    match field {
        Field::Value(value) => value.clone(),
        Field::Nested(fields) => Value::Composite(
            Composite::record(fields.iter().map(|(a, f)| (a.clone(), to_value(f))))
                .expect("a record"),
        ),
        Field::Read(_) => unreachable!("only content to write is made a value"),
    }
}

/// The three readers of a map of `size` fields, each field's value made by
/// `value`; each row the ns to reach one field, over every field in turn.
fn maps(rounds: usize, what: &str, value: &dyn Fn(usize) -> Field) {
    println!("### One field of a map of n fields, {what}\n");
    println!("ns a read, median (min to max) over every field in turn.\n");
    println!("| n | #812 | scan | offsets | bytes: #812 / scan / offsets |");
    println!("|--:|--:|--:|--:|---|");
    for size in SIZES {
        let fields: Vec<(Relation, Field)> = (0..size).map(|i| (attribute(i), value(i))).collect();
        let pairs = Pairs::new(fields.clone());
        let composite = Composite::record(fields.iter().map(|(a, f)| (a.clone(), to_value(f))))
            .expect("a record");
        let scan_bytes = composite.layout(usize::MAX);
        let offset_bytes = composite.layout(0);
        let scan = Composite::from_layout(&scan_bytes).expect("reads");
        let offsets = Composite::from_layout(&offset_bytes).expect("reads");
        let keys: Vec<Value> = (0..size).map(|i| Value::Symbol(attribute(i))).collect();
        let repeat = (20_000 / size).max(20);
        let mut at = 0;
        let a = time(rounds, repeat, || {
            at = (at + 1) % size;
            pairs.get(&fields[at].0).len()
        });
        let b = time(rounds, repeat, || {
            at = (at + 1) % size;
            scan.first(&keys[at]).is_some() as usize
        });
        let c = time(rounds, repeat, || {
            at = (at + 1) % size;
            offsets.first(&keys[at]).is_some() as usize
        });
        println!(
            "| {size} | {:.0} ({:.0} to {:.0}) | {:.0} ({:.0} to {:.0}) | {:.0} ({:.0} to {:.0}) | {} / {} / {} |",
            a.1,
            a.0,
            a.2,
            b.1,
            b.0,
            b.2,
            c.1,
            c.0,
            c.2,
            pairs.0.len(),
            scan_bytes.len(),
            offset_bytes.len()
        );
    }
    println!();
}

/// A scan against offsets for one item of a list of `size` items.
fn lists(rounds: usize, what: &str, value: &dyn Fn(usize) -> Value) {
    println!("### One item of a list of n items, {what}\n");
    println!("ns a read, median (min to max) over every item in turn. #812 has no list.\n");
    println!("| n | scan | offsets | bytes: scan / offsets |");
    println!("|--:|--:|--:|---|");
    for size in SIZES {
        let list = Composite::list((0..size).map(value)).expect("a list");
        let scan_bytes = list.layout(usize::MAX);
        let offset_bytes = list.layout(0);
        let scan = Composite::from_layout(&scan_bytes).expect("reads");
        let offsets = Composite::from_layout(&offset_bytes).expect("reads");
        let repeat = (20_000 / size).max(20);
        let mut at = 0;
        let a = time(rounds, repeat, || {
            at = (at + 1) % size;
            scan.item(at).is_some() as usize
        });
        let b = time(rounds, repeat, || {
            at = (at + 1) % size;
            offsets.item(at).is_some() as usize
        });
        println!(
            "| {size} | {:.0} ({:.0} to {:.0}) | {:.0} ({:.0} to {:.0}) | {} / {} |",
            a.1,
            a.0,
            a.2,
            b.1,
            b.0,
            b.2,
            scan_bytes.len(),
            offset_bytes.len()
        );
    }
    println!();
}

/// A field three composites down, each level beside `siblings` fields
/// holding a syntax node: #812 writes each nested composite again to read
/// the next level; a view reads the one buffer in place.
fn nested(rounds: usize) {
    println!("### A field three composites down\n");
    println!(
        "ns a read, median (min to max). Each level holds the next under `syntax/f000` and s siblings, each a three-field node.\n"
    );
    println!("| s | #812, written again at each level | view | bytes |");
    println!("|--:|--:|--:|--:|");
    for siblings in [1, 3, 8, 32] {
        let mut level: Vec<(Relation, Field)> = vec![(
            relation("syntax/leaf"),
            Field::Value(Value::String("here".into())),
        )];
        for _ in 0..3 {
            let mut outer = vec![(attribute(0), Field::Nested(level))];
            for s in 1..=siblings {
                outer.push((attribute(s), Field::Nested(node_fields(s))));
            }
            level = outer;
        }
        let pairs = Pairs::new(level.clone());
        let composite = Composite::record(level.iter().map(|(a, f)| (a.clone(), to_value(f))))
            .expect("a record");
        let first = attribute(0);
        let leaf = relation("syntax/leaf");
        let a = time(rounds, 2_000, || {
            let mut at = pairs.clone();
            for _ in 0..3 {
                let Some(Field::Read(next)) = at.get(&first).pop() else {
                    return 0;
                };
                at = next;
            }
            at.get(&leaf).len()
        });
        let key = Value::Symbol(first.clone());
        let leaf_key = Value::Symbol(leaf.clone());
        let b = time(rounds, 2_000, || {
            let mut at = composite.clone();
            for _ in 0..3 {
                let Some(Value::Composite(next)) = at.first(&key) else {
                    return 0;
                };
                at = next;
            }
            at.first(&leaf_key).is_some() as usize
        });
        println!(
            "| {siblings} | {:.0} ({:.0} to {:.0}) | {:.0} ({:.0} to {:.0}) | {} |",
            a.1,
            a.0,
            a.2,
            b.1,
            b.0,
            b.2,
            composite.to_bytes().len()
        );
    }
    println!();
}

fn tree_value(json: &serde_json::Value) -> Result<Value> {
    let (kind, inner) = json
        .as_object()
        .and_then(|o| o.iter().next())
        .context("a tagged node")?;
    Ok(match kind.as_str() {
        "map" => Value::Composite(Composite::map(
            inner
                .as_array()
                .context("entries")?
                .iter()
                .map(|pair| {
                    let key = pair[0].as_str().context("a text key")?;
                    Ok((Value::String(key.into()), tree_value(&pair[1])?))
                })
                .collect::<Result<Vec<_>>>()?,
        )?),
        "list" => Value::Composite(Composite::list(
            inner
                .as_array()
                .context("items")?
                .iter()
                .map(tree_value)
                .collect::<Result<Vec<_>>>()?,
        )?),
        "text" => Value::String(inner.as_str().context("text")?.into()),
        "bool" => Value::Boolean(true),
        "entity" => Value::Entity(inner.as_str().context("entity")?.parse()?),
        "symbol" => Value::Symbol(inner.as_str().context("symbol")?.parse()?),
        "num" => {
            let text = inner.as_str().context("number")?;
            if text.contains('.') {
                Value::Float(text.parse()?)
            } else {
                let n: i128 = text.parse()?;
                if n < 0 {
                    Value::SignedInt(n)
                } else {
                    Value::UnsignedInt(n as u128)
                }
            }
        }
        other => return Err(anyhow!("no node `{other}`")),
    })
}

/// The notation's syntax trees (dumped by `design/composite-value/census.py`):
/// bytes as the composite against dag-cbor, and what offsets would add.
fn trees(path: &str) -> Result<()> {
    let dumped: Vec<serde_json::Value> = serde_json::from_slice(&std::fs::read(path)?)?;
    println!("### The notation's syntax trees\n");
    println!("| document | forms | dag-cbor | composite | ÷ dag-cbor | offsets everywhere |");
    println!("|---|--:|--:|--:|--:|--:|");
    let mut rows: Vec<(String, usize, usize, usize, usize)> = Vec::new();
    for form in &dumped {
        let document = form["document"].as_str().context("document")?.to_string();
        let cbor = form["cbor"].as_u64().context("cbor")? as usize;
        let Value::Composite(tree) = tree_value(&form["tree"])? else {
            return Err(anyhow!("a tree is a composite"));
        };
        let bytes = tree.to_bytes().len();
        let everywhere = tree.layout(1).len();
        match rows.iter_mut().find(|row| row.0 == document) {
            Some(row) => {
                row.1 += 1;
                row.2 += cbor;
                row.3 += bytes;
                row.4 += everywhere;
            }
            None => rows.push((document, 1, cbor, bytes, everywhere)),
        }
    }
    let total = rows.iter().fold((0, 0, 0, 0), |t, r| {
        (t.0 + r.1, t.1 + r.2, t.2 + r.3, t.3 + r.4)
    });
    for (document, forms, cbor, bytes, everywhere) in &rows {
        println!(
            "| `{document}` | {forms} | {cbor} | {bytes} | {:.2} | {everywhere} |",
            *bytes as f64 / *cbor as f64
        );
    }
    println!(
        "| all | {} | {} | {} | {:.2} | {} |\n",
        total.0,
        total.1,
        total.2,
        total.2 as f64 / total.1 as f64,
        total.3
    );
    Ok(())
}

pub fn run(rounds: usize, trees_path: Option<&str>) -> Result<()> {
    maps(rounds, "each value text", &|i| {
        Field::Value(Value::String(format!("value {i}")))
    });
    maps(rounds, "each value a three-field node", &|i| {
        Field::Nested(node_fields(i))
    });
    lists(rounds, "each item text", &|i| {
        Value::String(format!("item {i}"))
    });
    lists(rounds, "each item a three-field node", &|i| {
        to_value(&Field::Nested(node_fields(i)))
    });
    nested(rounds);
    if let Some(path) = trees_path {
        trees(path)?;
    }
    Ok(())
}
