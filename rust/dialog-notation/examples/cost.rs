//! What one change costs as the page grows: the to-do card of design
//! 2026-11-18 maintained on a branch of N to-dos, each with two tags,
//! beside a plain Dialog-DB subscription of the same concept.
//!
//! ```sh
//! cargo run -p dialog-notation --example cost -- 5
//! cargo run --release -p dialog-notation --example cost -- 5 100 10000
//! ```
//!
//! The first argument is how many runs, and the ones after it the sizes N
//! (10, 100, 1000 and 10000 when none is given); each run measures every N
//! and every variant once, interleaved, on a branch built afresh. Reads are counted
//! as the tree node reads and blob loads the reading env performs, by
//! dialog-repository's `Interposing` env; writes go around it, so only
//! reads are counted. Time is from after the write has landed (committed
//! and the branch refreshed) to the patches, so it is the work the change
//! causes in the view and not the write.
//!
//! What it prints: for each measurement and N, the median time in
//! microseconds with the least and the most, the median node reads, blob
//! loads, rows the layers delivered and patches, and the ratio of the
//! median at the largest N to the smallest.

#![allow(deprecated)]

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::live::Live;
use dialog_notation::{Analyzer, Plan, Query, Selection, Step, check, parse};
use dialog_query::ConceptConclusion;
use dialog_query::query::Output as _;
use dialog_repository::helpers::Interposing;

#[path = "notebook/session.rs"]
mod session;
use session::Notebook;

type Env = Interposing<
    dialog_peer::Peer<dialog_storage::provider::storage::VolatileSpace, dialog_peer::Session>,
>;

const SIZES: [usize; 4] = [10, 100, 1000, 10000];

const SCHEMA: &str = r#"
    (concept (list ?l name: ?name)
      (xyz.list/name of: ?l is: ?name as: :text cardinality: :one))
    (concept (todo ?t list: ?l title: ?title state: ?s)
      (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
      (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)
      (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))
    (concept (todo/tag ?t tag: ?g)
      (xyz.todo/tag of: ?t is: ?g as: :text cardinality: :many))"#;

/// The design note's views: a chip a tag, a row a to-do with its chips
/// collected, and the card that counts the open ones and collects the
/// rows; and the to-do's card as data.
const QUERIES: &str = r#"
    (query (todo/chip ?t tag: ?g is: ?chip)
      (todo/tag ?t tag: ?g)
      (view ?chip [:span {:class "tag"} ?g]))
    (query (todo/row ?t list: ?l is: ?row)
      (todo ?t list: ?l title: ?title)
      (todo/chip ?t is: (distinct ?chips))
      (view ?row [:li ?title ?chips]))
    (query (list/card ?l is: ?card)
      (list ?l name: ?name)
      (todo (distinct ?open) list: ?l state: :open)
      (todo/row _ list: ?l is: (distinct ?rows))
      (is ?count (count ?open))
      (view ?card [:article [:h2 ?name] [:p ?count " open"] [:ul ?rows]]))
    (query (todo/card ?t is: ?card)
      (todo ?t title: ?title)
      (todo/tag ?t tag: (distinct ?tags))
      (is ?card {:title ?title :tags ?tags}))"#;

/// The seed: one list of `n` to-dos, each open, each with two tags.
fn seed(n: usize) -> String {
    let mut text = String::from(r#"(assert (list list:home name: "Home"))"#);
    for i in 0..n {
        text.push_str(&format!(
            "\n(assert (todo todo:{i} list: list:home title: \"Task {i}\" state: :open))\
             \n(assert (todo/tag todo:{i} tag: \"a{}\"))\
             \n(assert (todo/tag todo:{i} tag: \"b{}\"))",
            i % 7,
            i % 5
        ));
    }
    text
}

/// One measurement: time, reads, rows delivered, patches.
#[derive(Clone, Debug, Default)]
struct Sample {
    micros: u128,
    nodes: u64,
    blobs: u64,
    rows: usize,
    patches: usize,
}

/// A counter of what the env reads between two points.
fn mark(env: &Env) -> (u64, u64) {
    (env.reads(), env.loads())
}

fn since(env: &Env, from: (u64, u64)) -> (u64, u64) {
    (env.reads() - from.0, env.loads() - from.1)
}

/// The plan `asked` compiles to after the schema and the queries.
fn plan(asked: &str) -> Result<Plan> {
    let analysis = Analyzer::new()?.analyze(&parse(&format!("{SCHEMA}\n{QUERIES}\n{asked}"))?);
    for tree in &analysis.analyzed {
        if let [Step::View(plan)] = dialog_notation::compile(tree)?.as_slice() {
            return Ok((**plan).clone());
        }
    }
    Err(anyhow::anyhow!("no plan: {:?}", analysis.diagnostics))
}

/// The selection `asked` compiles to.
fn selection(asked: &str) -> Result<Selection> {
    let analysis = Analyzer::new()?.analyze(&parse(&format!("{SCHEMA}\n{QUERIES}\n{asked}"))?);
    for tree in &analysis.analyzed {
        if let [Step::Select(selection)] = dialog_notation::compile(tree)?.as_slice() {
            return Ok((**selection).clone());
        }
    }
    Err(anyhow::anyhow!("no selection: {:?}", analysis.diagnostics))
}

/// A fresh branch with the schema and `n` to-dos, and the reading env.
async fn branch(n: usize) -> Result<(Notebook, Env)> {
    let mut notebook = Notebook::open().await?;
    notebook.submit(SCHEMA).await?;
    notebook.submit(&seed(n)).await?;
    let env = Interposing::new(notebook.operator.clone());
    Ok((notebook, env))
}

/// Rows of a query asked once through `env`.
async fn asked(
    notebook: &Notebook,
    env: &Env,
    query: &Query,
) -> Result<Vec<BTreeMap<String, Value>>> {
    let mut layer = notebook.branch.query();
    if let Some(rule) = &query.rule {
        layer = layer.with(rule.clone());
    }
    let rows: Vec<ConceptConclusion> = layer
        .select(query.select.clone())
        .perform(env)
        .try_vec()
        .await?;
    rows.iter()
        .map(|row| query.values(row).map_err(anyhow::Error::from))
        .collect()
}

type Results = BTreeMap<(String, usize), Vec<Sample>>;

fn record(results: &mut Results, name: &str, n: usize, sample: Sample) {
    results
        .entry((name.to_string(), n))
        .or_default()
        .push(sample);
}

/// The notation's card: first render, then the three changes; and the
/// to-do's card asked by its entity and all cards.
async fn notation(
    n: usize,
    results: &mut Results,
    layers: &mut BTreeMap<usize, Vec<usize>>,
) -> Result<()> {
    let (mut notebook, env) = branch(n).await?;
    let card = plan("(select (list/card ?l is: ?card))")?;
    let live = Live::new(notebook.branch.clone());
    let mut watched = live.watch(card, Some(entity("list:home")))?;

    let from = mark(&env);
    let start = Instant::now();
    let patches = watched.poll(&env).await?;
    let micros = start.elapsed().as_micros();
    let (nodes, blobs) = since(&env, from);
    layers.insert(n, watched.delivered().to_vec());
    record(
        results,
        "1 first render",
        n,
        Sample {
            micros,
            nodes,
            blobs,
            rows: watched.delivered().iter().sum(),
            patches: patches.len(),
        },
    );

    let k = n / 2;
    for (name, write) in [
        (
            "2 rename",
            format!(r#"(assert (todo todo:{k} list: list:home title: "Renamed" state: :open))"#),
        ),
        (
            "3 tag added",
            format!(r#"(assert (todo/tag todo:{k} tag: "new"))"#),
        ),
        (
            "4 to-do added",
            r#"(assert (todo todo:new list: list:home title: "New" state: :open))"#.to_string(),
        ),
    ] {
        notebook.submit(&write).await?;
        let recomputed = watched.recomputes();
        let from = mark(&env);
        let start = Instant::now();
        let patches = watched.poll(&env).await?;
        let micros = start.elapsed().as_micros();
        let (nodes, blobs) = since(&env, from);
        record(
            results,
            name,
            n,
            Sample {
                micros,
                nodes,
                blobs,
                rows: watched.delivered().iter().sum(),
                patches: patches.len(),
            },
        );
        // A layer evaluated whole again, not maintained from the change.
        record(
            results,
            &format!("{name}, layers recomputed"),
            n,
            Sample {
                patches: watched
                    .recomputes()
                    .iter()
                    .zip(&recomputed)
                    .filter(|(now, before)| now > before)
                    .count(),
                ..Sample::default()
            },
        );
    }

    // A selection asked once through the driver.
    for (name, asked_for) in [
        (
            "5 select one card",
            format!("(select (todo/card todo:{k} is: ?card))"),
        ),
        (
            "5 select all cards",
            "(select (todo/card ?t is: ?card))".to_string(),
        ),
    ] {
        let selection = selection(&asked_for)?;
        let from = mark(&env);
        let start = Instant::now();
        let answers = live.select(&selection, &env).await?;
        let micros = start.elapsed().as_micros();
        let (nodes, blobs) = since(&env, from);
        record(
            results,
            name,
            n,
            Sample {
                micros,
                nodes,
                blobs,
                rows: 0,
                patches: answers.len(),
            },
        );
    }
    Ok(())
}

/// The baseline: a plain subscription of `(todo ?t list: ?l title:
/// ?title)` with the same three changes, and one to-do's title asked by
/// its entity.
async fn baseline(n: usize, results: &mut Results) -> Result<()> {
    let (mut notebook, env) = branch(n).await?;
    let query = query_of(
        &mut Analyzer::new()?,
        "(select (todo ?t list: ?l title: ?title))",
    )?;
    if let Some(rule) = &query.rule {
        notebook.branch.overlay().assert(rule.clone())?;
    }
    let mut subscription = notebook.branch.subscribe(query.select.clone());
    let from = mark(&env);
    let start = Instant::now();
    let delta = subscription.poll(&env).await?;
    let micros = start.elapsed().as_micros();
    let (nodes, blobs) = since(&env, from);
    record(
        results,
        "B1 first poll",
        n,
        Sample {
            micros,
            nodes,
            blobs,
            rows: delta.map(|delta| delta.asserted.len()).unwrap_or_default(),
            patches: 0,
        },
    );
    let k = n / 2;
    for (name, write) in [
        (
            "B2 rename",
            format!(r#"(assert (todo todo:{k} list: list:home title: "Renamed" state: :open))"#),
        ),
        (
            "B3 tag added",
            format!(r#"(assert (todo/tag todo:{k} tag: "new"))"#),
        ),
        (
            "B4 to-do added",
            r#"(assert (todo todo:new list: list:home title: "New" state: :open))"#.to_string(),
        ),
    ] {
        notebook.submit(&write).await?;
        let from = mark(&env);
        let start = Instant::now();
        let delta = subscription.poll(&env).await?;
        let micros = start.elapsed().as_micros();
        let (nodes, blobs) = since(&env, from);
        record(
            results,
            name,
            n,
            Sample {
                micros,
                nodes,
                blobs,
                rows: delta
                    .map(|delta| delta.asserted.len() + delta.retracted.len())
                    .unwrap_or_default(),
                patches: 0,
            },
        );
    }
    // The card's rows and chips layers read the same as rules whose head
    // is the to-do itself, where the layer's head is an entity made of
    // the row's values: what the row-identity rule costs, apart.
    for (name, asked_for, write) in [
        (
            "B6 rows as a rule by to-do, rename",
            "(select (todo ?t title: ?title) (todo ?t list: list:home title: ?title))",
            format!(r#"(assert (todo todo:{k} list: list:home title: "Again" state: :open))"#),
        ),
        (
            "B7 tags as a rule by to-do, tag added",
            "(select (todo/tag ?t tag: ?g) (todo/tag ?t tag: ?g) (todo ?t list: list:home))",
            format!(r#"(assert (todo/tag todo:{k} tag: "newer"))"#),
        ),
    ] {
        let query = query_of(&mut Analyzer::new()?, asked_for)?;
        if let Some(rule) = &query.rule {
            notebook.branch.overlay().assert(rule.clone())?;
        }
        let mut subscription = notebook.branch.subscribe(query.select.clone());
        subscription.poll(&env).await?;
        notebook.submit(&write).await?;
        let recomputed = subscription.recomputes();
        let from = mark(&env);
        let start = Instant::now();
        let delta = subscription.poll(&env).await?;
        let micros = start.elapsed().as_micros();
        let (nodes, blobs) = since(&env, from);
        record(
            results,
            name,
            n,
            Sample {
                micros,
                nodes,
                blobs,
                rows: delta
                    .map(|delta| delta.asserted.len() + delta.retracted.len())
                    .unwrap_or_default(),
                patches: usize::from(subscription.recomputes() > recomputed),
            },
        );
    }
    // The card's tags counted by Dialog-DB: the fold `(select
    // (todo/card ?t is: ?card))` lowers its collected read to, under a
    // subscription, a tag added.
    let card = selection("(select (todo/card ?t is: ?card))")?;
    let folded = card
        .folds
        .first()
        .ok_or_else(|| anyhow::anyhow!("the card's tags are not folded"))?;
    for rule in &folded.rules {
        notebook.branch.overlay().assert(rule.clone())?;
    }
    let mut counted = notebook.branch.subscribe_fold(folded.fold.clone());
    counted.poll(&env).await?;
    // The fold's two queries subscribed alone, beside it: what the
    // standing fold costs beyond keeping them is the difference.
    let mut groups = notebook.branch.subscribe(folded.fold.groups().clone());
    let mut inputs = notebook.branch.subscribe(folded.fold.inputs().clone());
    groups.poll(&env).await?;
    inputs.poll(&env).await?;
    notebook
        .submit(&format!(r#"(assert (todo/tag todo:{k} tag: "newest"))"#))
        .await?;
    let recomputed = counted.recomputes();
    let from = mark(&env);
    let start = Instant::now();
    let delta = counted.poll(&env).await?;
    let micros = start.elapsed().as_micros();
    let (nodes, blobs) = since(&env, from);
    record(
        results,
        "B8 tags folded, tag added",
        n,
        Sample {
            micros,
            nodes,
            blobs,
            rows: delta
                .map(|delta| delta.asserted.len() + delta.retracted.len())
                .unwrap_or_default(),
            patches: usize::from(counted.recomputes() > recomputed),
        },
    );
    let recomputed = groups.recomputes() + inputs.recomputes();
    let from = mark(&env);
    let start = Instant::now();
    let moved = [
        groups
            .poll(&env)
            .await?
            .map(|delta| delta.asserted.len() + delta.retracted.len()),
        inputs
            .poll(&env)
            .await?
            .map(|delta| delta.asserted.len() + delta.retracted.len()),
    ];
    let micros = start.elapsed().as_micros();
    let (nodes, blobs) = since(&env, from);
    record(
        results,
        "B8b tags folded, inner subscriptions alone",
        n,
        Sample {
            micros,
            nodes,
            blobs,
            rows: moved.into_iter().flatten().sum(),
            patches: usize::from(groups.recomputes() + inputs.recomputes() > recomputed),
        },
    );
    let one = query_of(
        &mut Analyzer::new()?,
        &format!("(select (todo todo:{k} title: ?title))"),
    )?;
    let from = mark(&env);
    let start = Instant::now();
    let found = asked(&notebook, &env, &one).await?;
    let micros = start.elapsed().as_micros();
    let (nodes, blobs) = since(&env, from);
    record(
        results,
        "B5 query one title",
        n,
        Sample {
            micros,
            nodes,
            blobs,
            rows: found.len(),
            patches: 0,
        },
    );
    // A row that reads another entity: each to-do with its list's name,
    // a rule by the to-do that joins through its list. A second list
    // holds one to-do; renaming it changes one row, renaming the first
    // list changes all `n`.
    notebook
        .submit(
            r#"(assert (list list:other name: "Other"))
               (assert (todo todo:other list: list:other title: "Elsewhere" state: :open))"#,
        )
        .await?;
    let listed = query_of(
        &mut Analyzer::new()?,
        "(select (todo/listed ?t name: ?name) (todo ?t list: ?l) (list ?l name: ?name))",
    )?;
    if let Some(rule) = &listed.rule {
        notebook.branch.overlay().assert(rule.clone())?;
    }
    let mut subscription = notebook.branch.subscribe(listed.select.clone());
    subscription.poll(&env).await?;
    for (name, write) in [
        (
            "B9 list name by to-do, other list renamed",
            r#"(assert (list list:other name: "Away"))"#,
        ),
        (
            "B9 list name by to-do, home list renamed",
            r#"(assert (list list:home name: "House"))"#,
        ),
    ] {
        notebook.submit(write).await?;
        let recomputed = subscription.recomputes();
        let from = mark(&env);
        let start = Instant::now();
        let delta = subscription.poll(&env).await?;
        let micros = start.elapsed().as_micros();
        let (nodes, blobs) = since(&env, from);
        record(
            results,
            name,
            n,
            Sample {
                micros,
                nodes,
                blobs,
                rows: delta
                    .map(|delta| delta.asserted.len() + delta.retracted.len())
                    .unwrap_or_default(),
                patches: usize::from(subscription.recomputes() > recomputed),
            },
        );
    }
    Ok(())
}

fn query_of(analyzer: &mut Analyzer, text: &str) -> Result<Query> {
    let checked = check(analyzer, &format!("{SCHEMA}\n{text}"));
    for step in checked.steps {
        if let Step::Query(query) = step {
            return Ok(*query);
        }
    }
    Err(anyhow::anyhow!("no query: {:?}", checked.diagnostics))
}

fn entity(uri: &str) -> Value {
    match parse(uri).expect("an entity").remove(0) {
        dialog_notation::Syntax::Constant(value) => value,
        other => panic!("{other} is no entity"),
    }
}

fn median<T: Copy + Ord>(mut values: Vec<T>) -> T {
    values.sort();
    values[values.len() / 2]
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let runs: usize = std::env::args()
        .nth(1)
        .map(|runs| runs.parse())
        .transpose()?
        .unwrap_or(5);
    let mut sizes = std::env::args()
        .skip(2)
        .map(|n| n.parse())
        .collect::<Result<Vec<usize>, _>>()?;
    if sizes.is_empty() {
        sizes = SIZES.to_vec();
    }
    let mut results = Results::new();
    let mut layers = BTreeMap::new();
    for run in 0..runs {
        for &n in &sizes {
            // A, B: the notation's card and the baseline, alternated.
            if run % 2 == 0 {
                notation(n, &mut results, &mut layers).await?;
                baseline(n, &mut results).await?;
            } else {
                baseline(n, &mut results).await?;
                notation(n, &mut results, &mut layers).await?;
            }
        }
        eprintln!("run {} of {runs} done", run + 1);
    }
    println!("the card's layers: 0 the list, 1 the rows, 2 the chips, 3 the open to-dos counted");
    println!(
        "in a `layers recomputed` line, `patches` is how many layers were evaluated whole again"
    );
    println!("rows each layer of the card delivers at its first render, by N:");
    for (n, rows) in &layers {
        println!("  N={n}: {rows:?}");
    }
    println!(
        "{:<26} {:>5} {:>10} {:>10} {:>10} {:>8} {:>7} {:>7} {:>8}",
        "measurement", "N", "median us", "least", "most", "nodes", "blobs", "rows", "patches"
    );
    let names: Vec<String> = {
        let mut names: Vec<String> = results.keys().map(|(name, _)| name.clone()).collect();
        names.dedup();
        names
    };
    for name in &names {
        let mut medians = Vec::new();
        for &n in &sizes {
            let Some(samples) = results.get(&(name.clone(), n)) else {
                continue;
            };
            let times: Vec<u128> = samples.iter().map(|sample| sample.micros).collect();
            let time = median(times.clone());
            let nodes = median(samples.iter().map(|sample| sample.nodes).collect());
            medians.push((n, time, nodes));
            println!(
                "{:<26} {:>5} {:>10} {:>10} {:>10} {:>8} {:>7} {:>7} {:>8}",
                name,
                n,
                time,
                times.iter().min().copied().unwrap_or_default(),
                times.iter().max().copied().unwrap_or_default(),
                nodes,
                median(samples.iter().map(|sample| sample.blobs).collect()),
                median(samples.iter().map(|sample| sample.rows).collect()),
                median(samples.iter().map(|sample| sample.patches).collect()),
            );
        }
        if let (Some(first), Some(last)) = (medians.first(), medians.last()) {
            let ratio = |a: f64, b: f64| if a == 0.0 { f64::NAN } else { b / a };
            println!(
                "{:<26} ratio {}/{}: time {:.1}, nodes {:.1}",
                "",
                last.0,
                first.0,
                ratio(first.1 as f64, last.1 as f64),
                ratio(first.2 as f64, last.2 as f64),
            );
        }
    }
    Ok(())
}
