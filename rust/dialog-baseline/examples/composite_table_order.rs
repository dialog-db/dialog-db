//! Research for `research/composite-table-order.md`: a composite's table of
//! atoms sorted by bytes (A, today) against the same table with the map keys
//! first (B), in the leaves of a tree and in the time to write and read one.
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example composite_table_order -- size [scratch]
//! cargo run --release -p dialog-baseline --example composite_table_order -- time [rounds]
//! ```
//!
//! `size` commits each corpus to a branch on the volatile provider, once per
//! layout, walks the canonical tree and reports its bytes; `scratch` is
//! where the Chronicle files go. `time` times encoding and reading, A and B
//! interleaved. B is the research switch of `dialog-corpus`'s
//! `research-table-order` feature; nothing here is used by a crate.

// Commits through the one-shot `.commit().publish().perform(..)`, as the
// rest of this crate's harness does.
#![allow(deprecated)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

use anyhow::{Context, Result};
use dialog_baseline::changes_of;
use dialog_baseline::nodes::walk;
use dialog_baseline::repo::DialogRepo;
use dialog_common::Buffer;
use dialog_corpus::table_order_research::set_keys_first;
use dialog_corpus::{
    ATTRIBUTE_KEY_TAG, Cardinality, Composite, ENTITY_KEY_TAG, Entity, Fact, HISTORY_KEY_TAG,
    Instruction, Key, Relation, VALUE_KEY_TAG, Value,
};
use dialog_effects::archive::Load;
use dialog_search_tree::NodeBody;

/// A small deterministic generator, so a corpus replays from its seed.
struct Seeded(u64);

impl Seeded {
    fn step(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.step() % bound as u64) as usize
    }

    fn pick<'a>(&mut self, from: &[&'a str]) -> &'a str {
        from[self.below(from.len())]
    }
}

const SEED: u64 = 0x5eed_c0de_7ab1_e0d3;
const ENTITIES: usize = 4000;

const NAMES: &[&str] = &[
    "Ada", "Grace", "Alan", "Barbara", "Edsger", "Frances", "Donald", "Margaret", "Ken", "Radia",
    "Leslie", "Shafi", "Tim", "Sophie", "Robin", "Yukihiro", "Guido", "Anders", "Bjarne",
    "Niklaus",
];
const CITIES: &[&str] = &[
    "Berlin",
    "Lisbon",
    "Oslo",
    "Tbilisi",
    "Kyoto",
    "Lagos",
    "Lima",
    "Quebec",
    "Hanoi",
    "Perth",
    "Reykjavik",
    "Montevideo",
];
const WORDS: &[&str] = &[
    "todo", "state", "open", "tags", "errand", "home", "where", "select", "count", "sum",
    "concept", "rule", "match", "of", "is", "the", "person", "name", "title", "due",
];

fn relation(name: &str) -> Relation {
    Relation::from_str(name).expect("a relation")
}

fn entity(i: usize) -> Entity {
    // A spread of identifiers, so the entity ordering is not the order of
    // generation.
    let mixed = (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    Entity::from_str(&format!("id:e{mixed:016x}")).expect("an entity")
}

fn fact(the: &str, of: Entity, is: Value) -> Fact {
    Fact {
        the: relation(the),
        of,
        is,
        cause: None,
    }
}

/// A person record of five fields; with `skewed`, its city and its flag are
/// the same on nine entities in ten.
fn person(random: &mut Seeded, skewed: bool) -> Vec<(Relation, Value)> {
    let common = skewed && random.below(10) != 0;
    let city = if common {
        "Berlin"
    } else {
        random.pick(CITIES)
    };
    let active = if common { true } else { random.below(2) == 0 };
    vec![
        (
            relation("person/name"),
            Value::String(random.pick(NAMES).into()),
        ),
        (relation("person/city"), Value::String(city.into())),
        (
            relation("person/age"),
            Value::UnsignedInt(18 + random.below(70) as u128),
        ),
        (relation("person/active"), Value::Boolean(active)),
        (
            relation("person/manager"),
            Value::Entity(entity(random.below(ENTITIES))),
        ),
    ]
}

fn record(fields: Vec<(Relation, Value)>) -> Value {
    Value::Composite(Composite::record(fields).expect("a record"))
}

/// Same-shape records, one per entity, under one attribute.
fn records(skewed: bool) -> Vec<Fact> {
    let mut random = Seeded(SEED);
    (0..ENTITIES)
        .map(|i| {
            let value = record(person(&mut random, skewed));
            fact("app/profile", entity(i), value)
        })
        .collect()
}

/// A node of the notation's syntax tree as `design/composite-value/census.py`
/// builds it: a map with text keys, its kind under `type` as an entity.
fn syntax(kind: &str, fields: Vec<(&str, Value)>) -> Value {
    let mut entries = vec![(
        Value::String("type".into()),
        Value::Entity(Entity::from_str(&format!("{kind}:")).expect("a kind")),
    )];
    entries.extend(
        fields
            .into_iter()
            .map(|(key, value)| (Value::String(key.into()), value)),
    );
    Value::Composite(Composite::map(entries).expect("a syntax node"))
}

/// A random form in the notation's syntax-tree shape: applications with
/// labelled terms, tuples, constants, references and variables.
fn form(random: &mut Seeded, depth: usize) -> Value {
    let choice = if depth == 0 { 6 } else { random.below(10) };
    match choice {
        0..=3 => {
            let operator = syntax(
                "reference",
                vec![(
                    "name",
                    Value::Symbol(relation(&format!("/{}", random.pick(WORDS)))),
                )],
            );
            let items: Vec<Value> = (0..random.below(3))
                .map(|_| form(random, depth - 1))
                .collect();
            let mut labels: BTreeMap<&str, Value> = BTreeMap::new();
            for _ in 0..random.below(4) {
                let label = random.pick(WORDS);
                labels.insert(label, form(random, depth - 1));
            }
            let terms: Vec<(Value, Value)> = labels
                .into_iter()
                .map(|(label, value)| (Value::String(label.into()), value))
                .collect();
            let operand = syntax(
                "parameters",
                vec![
                    (
                        "items",
                        Value::Composite(Composite::list(items).expect("items")),
                    ),
                    (
                        "terms",
                        Value::Composite(Composite::map(terms).expect("terms")),
                    ),
                ],
            );
            syntax(
                "application",
                vec![("operator", operator), ("operand", operand)],
            )
        }
        4 => {
            let items: Vec<Value> = (0..1 + random.below(3))
                .map(|_| form(random, depth - 1))
                .collect();
            syntax(
                "tuple",
                vec![(
                    "items",
                    Value::Composite(Composite::list(items).expect("items")),
                )],
            )
        }
        5 => syntax(
            "variable",
            vec![("name", Value::String(random.pick(WORDS).into()))],
        ),
        _ => match random.below(3) {
            0 => syntax(
                "constant",
                vec![("value", Value::UnsignedInt(random.below(1000) as u128))],
            ),
            1 => syntax(
                "constant",
                vec![("value", Value::String(random.pick(WORDS).into()))],
            ),
            _ => syntax(
                "reference",
                vec![(
                    "name",
                    Value::Symbol(relation(&format!("/{}", random.pick(WORDS)))),
                )],
            ),
        },
    }
}

/// One syntax tree per entity under one attribute, each small enough to
/// stay inline.
fn syntax_trees() -> Vec<Fact> {
    let mut random = Seeded(SEED ^ 1);
    let mut facts = Vec::new();
    let mut i = 0;
    while facts.len() < ENTITIES / 2 {
        let tree = form(&mut random, 4);
        let Value::Composite(composite) = &tree else {
            unreachable!("a syntax node is a map")
        };
        if composite.to_bytes().len() <= 4000 {
            facts.push(fact("notation/form", entity(i), tree));
        }
        i += 1;
    }
    facts
}

/// Three record shapes interleaved under one attribute, and a list of
/// scalars per entity under another.
fn mixed() -> Vec<Fact> {
    let mut random = Seeded(SEED ^ 2);
    let mut facts = Vec::new();
    for i in 0..ENTITIES {
        let fields = match random.below(3) {
            0 => person(&mut random, false),
            1 => vec![
                (
                    relation("task/title"),
                    Value::String(random.pick(WORDS).into()),
                ),
                (relation("task/done"), Value::Boolean(random.below(2) == 0)),
                (
                    relation("task/due"),
                    Value::UnsignedInt(20_000 + random.below(400) as u128),
                ),
            ],
            _ => vec![
                (
                    relation("place/name"),
                    Value::String(random.pick(CITIES).into()),
                ),
                (
                    relation("place/lat"),
                    Value::Float(random.below(18_000) as f64 / 100.0 - 90.0),
                ),
                (
                    relation("place/lon"),
                    Value::Float(random.below(36_000) as f64 / 100.0 - 180.0),
                ),
                (
                    relation("place/country"),
                    Value::String(random.pick(CITIES).into()),
                ),
                (
                    relation("place/population"),
                    Value::UnsignedInt(random.below(10_000_000) as u128),
                ),
                (
                    relation("place/capital"),
                    Value::Boolean(random.below(5) == 0),
                ),
                (
                    relation("place/owner"),
                    Value::Entity(entity(random.below(ENTITIES))),
                ),
            ],
        };
        facts.push(fact("app/item", entity(i), record(fields)));
        let items: Vec<Value> = (0..3 + random.below(6))
            .map(|_| match random.below(2) {
                0 => Value::UnsignedInt(random.below(500) as u128),
                _ => Value::String(random.pick(WORDS).into()),
            })
            .collect();
        facts.push(fact(
            "app/tags",
            entity(i),
            Value::Composite(Composite::list(items).expect("a list")),
        ));
    }
    facts
}

/// The person records' fields as scalar facts of their own: no composites.
fn control() -> Vec<Fact> {
    let mut random = Seeded(SEED);
    let mut facts = Vec::new();
    for i in 0..ENTITIES {
        for (the, is) in person(&mut random, false) {
            facts.push(Fact {
                the,
                of: entity(i),
                is,
                cause: None,
            });
        }
    }
    facts
}

/// A corpus: its name and what makes its facts.
type Corpus = (&'static str, fn() -> Vec<Fact>);

const CORPORA: &[Corpus] = &[
    ("records", || records(false)),
    ("records, 2 of 5 shared", || records(true)),
    ("syntax trees (stand-in)", syntax_trees),
    ("mixed shapes and lists", mixed),
    ("control, no composites", control),
];

fn tag_name(tag: u8) -> &'static str {
    match tag {
        ENTITY_KEY_TAG => "EAV",
        ATTRIBUTE_KEY_TAG => "AEV",
        VALUE_KEY_TAG => "VAE",
        HISTORY_KEY_TAG => "history",
        _ => "other",
    }
}

#[derive(Default, Debug, Clone)]
struct Census {
    leaves: usize,
    leaf_bytes: usize,
    node_bytes: usize,
    by_tag: BTreeMap<&'static str, (usize, usize)>,
    composite_bytes: usize,
    shared: Vec<usize>,
    fraction: Vec<f64>,
    same_header: usize,
    neighbours: usize,
    chronicle: u64,
}

/// Bytes two composites share at their front.
fn common(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

/// The table's `head · count · size`: the bytes before its first atom.
fn header(bytes: &[u8]) -> &[u8] {
    let mut at = 1;
    for _ in 0..2 {
        while bytes[at] & 0x80 != 0 {
            at += 1;
        }
        at += 1;
    }
    &bytes[..at]
}

async fn census(facts: &[Fact], scratch: &std::path::Path, label: &str) -> Result<Census> {
    let mut census = Census::default();

    // The mechanism: what each composite shares with the one before it in
    // the attribute ordering (attribute, then entity).
    let mut ordered: Vec<(String, String, Vec<u8>)> = facts
        .iter()
        .filter_map(|fact| match &fact.is {
            Value::Composite(composite) => Some((
                fact.the.to_string(),
                fact.of.as_str().to_string(),
                composite.to_bytes().into_owned(),
            )),
            _ => None,
        })
        .collect();
    ordered.sort();
    for pair in ordered.windows(2) {
        census.composite_bytes += pair[1].2.len();
        if pair[0].0 != pair[1].0 {
            continue;
        }
        let (a, b) = (&pair[0].2, &pair[1].2);
        let shared = common(a, b);
        census.shared.push(shared);
        census.fraction.push(shared as f64 / b.len() as f64);
        census.neighbours += 1;
        if header(a) == header(b) {
            census.same_header += 1;
        }
    }
    if let Some(first) = ordered.first() {
        census.composite_bytes += first.2.len();
    }

    let repo = DialogRepo::volatile().await?;
    let instructions = facts
        .iter()
        .cloned()
        .map(|fact| Instruction::Assert(fact, Cardinality::Many));
    repo.branch()
        .transaction()
        .integrate(changes_of(instructions))
        .commit()
        .publish()
        .perform(repo.operator())
        .await?;
    repo.canonicalize().await?;
    let root = repo.root().context("a committed tree")?;
    let catalog = repo.catalog();
    let mut hashes = Vec::new();
    walk(repo.operator(), &catalog, root, |visit| {
        census.node_bytes += visit.size;
        hashes.push(visit.hash.clone());
        if let NodeBody::Segment(segment) = visit.node.body() {
            census.leaves += 1;
            census.leaf_bytes += visit.size;
            let mut keys = segment.keys::<Key>()?;
            let tag = keys
                .next_key()?
                .and_then(|(_, key)| key.first().copied())
                .unwrap_or(0);
            let slot = census.by_tag.entry(tag_name(tag)).or_default();
            slot.0 += 1;
            slot.1 += visit.size;
        }
        Ok(())
    })
    .await?;

    let mut blocks: Vec<Buffer> = Vec::with_capacity(hashes.len());
    for hash in hashes {
        let block = Load::new(catalog.clone(), hash)
            .perform(repo.operator())
            .await?
            .context("a reachable block")?;
        blocks.push(block);
    }
    let path = scratch.join(format!(
        "{}.dialog",
        label.replace([' ', ',', '(', ')'], "_")
    ));
    std::fs::remove_file(&path).ok();
    {
        let chronicle = dialog_chronicle::Chronicle::open(&path)?;
        chronicle.append("index", &blocks)?;
        chronicle.sync()?;
        census.chronicle = chronicle.stats().file_bytes;
    }
    std::fs::remove_file(&path).ok();
    Ok(census)
}

fn median<T: Copy + PartialOrd>(values: &[T]) -> Option<T> {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("ordered"));
    sorted.get(sorted.len() / 2).copied()
}

/// Under B, every composite of `facts` reads back, its fields read as under
/// A, and its merkle reference is A's.
fn check(name: &str, a: &[Fact], b: &[Fact]) -> Result<()> {
    for (left, right) in a.iter().zip(b) {
        let (Value::Composite(left), Value::Composite(right)) = (&left.is, &right.is) else {
            continue;
        };
        anyhow::ensure!(
            left.reference() == right.reference(),
            "{name}: reference moved"
        );
        let bytes = right.to_bytes();
        let back = Composite::from_bytes(&bytes).context("B reads back as canonical")?;
        anyhow::ensure!(&back == right, "{name}: B does not round-trip");
        for (key, _) in left.entries() {
            anyhow::ensure!(left.get(&key).len() == right.get(&key).len(), "{name}: get");
        }
        anyhow::ensure!(
            left.entries().count() == right.entries().count(),
            "{name}: entries"
        );
        anyhow::ensure!(
            left.entries().zip(right.entries()).all(|(x, y)| x == y),
            "{name}: entries differ"
        );
    }
    Ok(())
}

async fn sizes(scratch: PathBuf) -> Result<()> {
    std::fs::create_dir_all(&scratch)?;
    println!(
        "| corpus | layout | facts | leaves | leaf bytes | node bytes | EAV | AEV | VAE | history | composite bytes | chronicle |"
    );
    println!("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|");
    let mut mechanism = Vec::new();
    for (name, make) in CORPORA {
        let mut runs = Vec::new();
        set_keys_first(false);
        let a = make();
        set_keys_first(true);
        let b = make();
        check(name, &a, &b)?;
        // A, B, A, B, A, B: each repository signs with an identity of its
        // own, so the history and the leaf boundaries it moves vary a
        // little from run to run; the spread between runs of one layout is
        // the noise.
        for _ in 0..3 {
            for (layout, facts, keys_first) in [("A", &a, false), ("B", &b, true)] {
                set_keys_first(keys_first);
                let census = census(facts, &scratch, &format!("{name}-{layout}")).await?;
                let tag = |t: &str| census.by_tag.get(t).map_or(0, |slot| slot.1);
                println!(
                    "| {name} | {layout} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    facts.len(),
                    census.leaves,
                    census.leaf_bytes,
                    census.node_bytes,
                    tag("EAV"),
                    tag("AEV"),
                    tag("VAE"),
                    tag("history"),
                    census.composite_bytes,
                    census.chronicle
                );
                runs.push((layout, census));
            }
        }
        mechanism.push((name, runs));
    }
    set_keys_first(false);

    println!("\nMedian of three runs per layout, and the spread (max - min) of leaf bytes:\n");
    println!(
        "| corpus | layout | leaf bytes | spread | node bytes | EAV | AEV | VAE | history | chronicle |"
    );
    println!("|---|---|--:|--:|--:|--:|--:|--:|--:|--:|");
    for (name, runs) in &mechanism {
        for layout in ["A", "B"] {
            let of: Vec<&Census> = runs
                .iter()
                .filter(|(l, _)| *l == layout)
                .map(|(_, c)| c)
                .collect();
            let pick = |f: &dyn Fn(&Census) -> usize| -> usize {
                median(&of.iter().map(|c| f(c)).collect::<Vec<_>>()).unwrap_or(0)
            };
            let leaf: Vec<usize> = of.iter().map(|c| c.leaf_bytes).collect();
            let spread = leaf.iter().max().unwrap_or(&0) - leaf.iter().min().unwrap_or(&0);
            let tag = |t: &'static str| move |c: &Census| c.by_tag.get(t).map_or(0, |slot| slot.1);
            println!(
                "| {name} | {layout} | {} | {spread} | {} | {} | {} | {} | {} | {} |",
                pick(&|c| c.leaf_bytes),
                pick(&|c| c.node_bytes),
                pick(&tag("EAV")),
                pick(&tag("AEV")),
                pick(&tag("VAE")),
                pick(&tag("history")),
                pick(&|c| c.chronicle as usize),
            );
        }
    }

    println!("\nShared prefix with the neighbour in the attribute ordering (composites only):\n");
    println!(
        "| corpus | layout | neighbours | mean bytes | median bytes | mean fraction | median fraction | same table header |"
    );
    println!("|---|---|--:|--:|--:|--:|--:|--:|");
    for (name, runs) in &mechanism {
        for (layout, census) in runs.iter().take(2) {
            if census.neighbours == 0 {
                continue;
            }
            let mean = census.shared.iter().sum::<usize>() as f64 / census.shared.len() as f64;
            let fraction = census.fraction.iter().sum::<f64>() / census.fraction.len() as f64;
            println!(
                "| {name} | {layout} | {} | {mean:.1} | {} | {fraction:.3} | {:.3} | {:.3} |",
                census.neighbours,
                median(&census.shared).unwrap_or(0),
                median(&census.fraction).unwrap_or(0.0),
                census.same_header as f64 / census.neighbours as f64
            );
        }
    }
    Ok(())
}

/// Nanoseconds per call of `work` over `count` items, the median of
/// `rounds` rounds and the spread between the lowest and the highest.
fn record_time(samples: &mut Vec<f64>, count: usize, work: impl FnOnce() -> usize) {
    let started = Instant::now();
    let sink = work();
    let elapsed = started.elapsed().as_nanos() as f64 / count as f64;
    std::hint::black_box(sink);
    samples.push(elapsed);
}

fn shaped(fields: usize, random: &mut Seeded) -> Vec<(Relation, Value)> {
    let mut out = Vec::new();
    for at in 0..fields {
        let value = match at % 5 {
            0 => Value::String(random.pick(NAMES).into()),
            1 => Value::String(random.pick(CITIES).into()),
            2 => Value::UnsignedInt(random.below(100) as u128),
            3 => Value::Boolean(random.below(2) == 0),
            _ => Value::Entity(entity(random.below(ENTITIES))),
        };
        out.push((relation(&format!("shape/field{at}")), value));
    }
    out
}

fn times(rounds: usize) -> Result<()> {
    const COUNT: usize = 20_000;
    println!("| map | measure | A ns (min..max) | B ns (min..max) | B / A |");
    println!("|---|---|--:|--:|--:|");
    for fields in [2usize, 5, 9] {
        let mut random = Seeded(SEED ^ fields as u64);
        let inputs: Vec<Vec<(Relation, Value)>> =
            (0..COUNT).map(|_| shaped(fields, &mut random)).collect();
        let keys: Vec<Value> = (0..fields)
            .map(|at| Value::Symbol(relation(&format!("shape/field{at}"))))
            .collect();
        let lists: Vec<Composite> = inputs
            .iter()
            .map(|fields| Composite::list(fields.iter().map(|(_, v)| v.clone())).expect("a list"))
            .collect();
        let mut encode: [Vec<f64>; 2] = Default::default();
        let mut get: [Vec<f64>; 2] = Default::default();
        let mut item: [Vec<f64>; 2] = Default::default();
        for _ in 0..rounds {
            for (variant, keys_first) in [(0, false), (1, true)] {
                set_keys_first(keys_first);
                let mut built = Vec::with_capacity(COUNT);
                record_time(&mut encode[variant], COUNT, || {
                    for fields in &inputs {
                        built.push(Composite::record(fields.clone()).expect("a record"));
                    }
                    built.len()
                });
                record_time(&mut get[variant], COUNT * fields, || {
                    let mut found = 0;
                    for composite in &built {
                        for key in &keys {
                            found += composite.get(key).len();
                        }
                    }
                    assert_eq!(found, COUNT * fields);
                    found
                });
                record_time(&mut item[variant], COUNT * fields, || {
                    let mut found = 0;
                    for list in &lists {
                        for at in 0..fields {
                            found += usize::from(list.item(at).is_some());
                        }
                    }
                    found
                });
            }
        }
        set_keys_first(false);
        for (what, samples) in [
            ("encode a record", &encode),
            ("get one field", &get),
            ("item of a list", &item),
        ] {
            let spread = |s: &Vec<f64>| {
                let low = s.iter().copied().fold(f64::MAX, f64::min);
                let high = s.iter().copied().fold(0.0, f64::max);
                (median(s).unwrap_or(0.0), low, high)
            };
            let (a, a_low, a_high) = spread(&samples[0]);
            let (b, b_low, b_high) = spread(&samples[1]);
            println!(
                "| {fields} fields | {what} | {a:.1} ({a_low:.1}..{a_high:.1}) | {b:.1} ({b_low:.1}..{b_high:.1}) | {:.3} |",
                b / a
            );
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("size") => {
            let scratch = args
                .get(2)
                .map(PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("composite-table-order"));
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(sizes(scratch))
        }
        Some("time") => times(args.get(2).and_then(|r| r.parse().ok()).unwrap_or(7)),
        _ => anyhow::bail!("usage: composite_table_order size [scratch] | time [rounds]"),
    }
}
