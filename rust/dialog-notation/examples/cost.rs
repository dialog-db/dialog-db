//! What one change costs as the page grows: the to-do card of design
//! 2026-11-18 maintained on a branch of N to-dos, each with two tags,
//! beside a plain Dialog-DB subscription of the same concept.
//!
//! ```sh
//! cargo run -p dialog-notation --example cost -- 5
//! ```
//!
//! The argument is how many runs; each run measures every N and every
//! variant once, interleaved, on a branch built afresh. Reads are counted
//! as the tree node reads and blob loads the reading env performs, by
//! dialog-repository's `Interposing` env; writes go around it, so only
//! reads are counted. Time is from after the write has landed (committed
//! and the branch refreshed) to the patches, so it is the work the change
//! causes in the view and not the write.
//!
//! What it prints: for each measurement and N, the median time in
//! microseconds with the least and the most, the median node reads, blob
//! loads, rows the layers delivered and patches, and the ratio of the
//! median at N=1000 to N=10.

#![allow(deprecated)]

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::Result;
use dialog_corpus::Value;
use dialog_notation::{
    Analyzer, Change, Maintainer, Patch, Plan, Query, Selection, Step, check, parse,
};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptQuery};
use dialog_repository::Subscription;
use dialog_repository::helpers::Interposing;

#[path = "notebook/session.rs"]
mod session;
use session::Notebook;

type Env = Interposing<
    dialog_peer::Peer<dialog_storage::provider::storage::VolatileSpace, dialog_peer::Session>,
>;

const SIZES: [usize; 3] = [10, 100, 1000];

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
      (todo/chip ?t is: (collect ?chips))
      (view ?row [:li ?title ?chips]))
    (query (list/card ?l is: ?card)
      (list ?l name: ?name)
      (todo (collect ?open) list: ?l state: :open)
      (todo/row _ list: ?l is: (collect ?rows))
      (is ?count (count ?open))
      (view ?card [:article [:h2 ?name] [:p ?count " open"] [:ul ?rows]]))
    (query (todo/card ?t is: ?card)
      (todo ?t title: ?title)
      (todo/tag ?t tag: (collect ?tags))
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

/// A plan's layers, each a subscription, and its maintainer.
struct Driven {
    plan: Plan,
    entity: Option<Value>,
    queries: Vec<Option<Query>>,
    feeds: Vec<Option<Subscription<ConceptQuery>>>,
    maintainer: Maintainer,
}

impl Driven {
    fn new(notebook: &Notebook, plan: Plan, entity: Option<Value>) -> Result<Driven> {
        let mut queries = Vec::new();
        let mut feeds = Vec::new();
        for layer in 0..plan.layers.len() {
            if plan.layers[layer].conditions.is_empty() {
                queries.push(None);
                feeds.push(None);
                continue;
            }
            let query = plan.query(layer, entity.as_ref())?;
            if let Some(rule) = &query.rule {
                notebook.branch.overlay().assert(rule.clone())?;
            }
            feeds.push(Some(notebook.branch.subscribe(query.select.clone())));
            queries.push(Some(query));
        }
        Ok(Driven {
            maintainer: Maintainer::new(plan.clone(), entity.clone()),
            plan,
            entity,
            queries,
            feeds,
        })
    }

    /// Poll every layer and apply what came and went: the patches, the
    /// rows delivered each layer, and the reads each layer's poll took.
    async fn step(&mut self, env: &Env) -> Result<(Vec<Patch>, Vec<Layered>)> {
        let mut changes = Vec::new();
        let mut layers = Vec::new();
        for (layer, feed) in self.feeds.iter_mut().enumerate() {
            let (Some(feed), Some(query)) = (feed, &self.queries[layer]) else {
                layers.push(Layered::default());
                continue;
            };
            let from = mark(env);
            let recomputed = feed.recomputes();
            let start = Instant::now();
            let delta = feed.poll(env).await?;
            let micros = start.elapsed().as_micros();
            let (nodes, _) = since(env, from);
            let recomputed = feed.recomputes() > recomputed;
            let (arrived, left) = match delta {
                Some(delta) => (
                    delta
                        .asserted
                        .iter()
                        .map(|row| self.plan.row(layer, query, row, self.entity.as_ref()))
                        .collect::<Result<Vec<_>, _>>()?,
                    delta
                        .retracted
                        .iter()
                        .map(|row| self.plan.row(layer, query, row, self.entity.as_ref()))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                None => (Vec::new(), Vec::new()),
            };
            layers.push(Layered {
                rows: arrived.len() + left.len(),
                nodes,
                micros,
                recomputed,
            });
            changes.push(Change {
                layer,
                arrived,
                left,
            });
        }
        let start = Instant::now();
        let patches = self.maintainer.apply(&changes);
        let micros = start.elapsed().as_micros();
        layers.push(Layered {
            micros,
            ..Layered::default()
        });
        Ok((patches, layers))
    }
}

/// What one layer's poll took, and, last, the maintainer's apply.
#[derive(Clone, Debug, Default)]
struct Layered {
    rows: usize,
    nodes: u64,
    micros: u128,
    recomputed: bool,
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
    let mut driven = Driven::new(&notebook, card, Some(entity("list:home")))?;

    let from = mark(&env);
    let start = Instant::now();
    let (patches, per) = driven.step(&env).await?;
    let micros = start.elapsed().as_micros();
    let (nodes, blobs) = since(&env, from);
    layers.insert(n, per.iter().map(|layer| layer.rows).collect());
    record(
        results,
        "1 first render",
        n,
        Sample {
            micros,
            nodes,
            blobs,
            rows: per.iter().map(|layer| layer.rows).sum(),
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
        let from = mark(&env);
        let start = Instant::now();
        let (patches, per) = driven.step(&env).await?;
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
                rows: per.iter().map(|layer| layer.rows).sum(),
                patches: patches.len(),
            },
        );
        let last = per.len() - 1;
        for (layer, part) in per.iter().enumerate() {
            let label = if layer == last {
                format!("{name}, maintainer apply")
            } else {
                format!("{name}, layer {layer} poll")
            };
            record(
                results,
                &label,
                n,
                Sample {
                    micros: part.micros,
                    nodes: part.nodes,
                    rows: part.rows,
                    // A layer evaluated whole again, not maintained from
                    // the change.
                    patches: usize::from(part.recomputed),
                    ..Sample::default()
                },
            );
        }
    }

    // Stage 1's evaluation in memory, timed apart from the queries.
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
        let mut results_of = Vec::new();
        let mut rows = 0;
        for query in &selection.queries {
            let found = asked(&notebook, &env, query).await?;
            rows += found.len();
            results_of.push(found);
        }
        let mut folded = Vec::new();
        for fold in &selection.folds {
            let mut layer = notebook.branch.query();
            for rule in &fold.rules {
                layer = layer.with(rule.clone());
            }
            let found = layer
                .select(fold.fold.clone())
                .perform(&env)
                .try_vec()
                .await?;
            rows += found.len();
            folded.push(found.iter().map(|row| fold.values(row)).collect());
        }
        let queried = start.elapsed().as_micros();
        let answering = Instant::now();
        let answers = selection.answer(&results_of, &folded)?;
        let answered = answering.elapsed().as_micros();
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
                rows,
                patches: answers.len(),
            },
        );
        record(
            results,
            &format!("{name}, queries"),
            n,
            Sample {
                micros: queried,
                nodes,
                rows,
                ..Sample::default()
            },
        );
        record(
            results,
            &format!("{name}, answer in memory"),
            n,
            Sample {
                micros: answered,
                patches: answers.len(),
                ..Sample::default()
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
    let mut counted = notebook.branch.subscribe(folded.fold.clone());
    counted.poll(&env).await?;
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
    let mut results = Results::new();
    let mut layers = BTreeMap::new();
    for run in 0..runs {
        for n in SIZES {
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
    println!("in a layer's line, `patches` is 1 where its poll evaluated it whole again");
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
        for n in SIZES {
            let Some(samples) = results.get(&(name.clone(), n)) else {
                continue;
            };
            let times: Vec<u128> = samples.iter().map(|sample| sample.micros).collect();
            let time = median(times.clone());
            let nodes = median(samples.iter().map(|sample| sample.nodes).collect());
            medians.push((time, nodes));
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
                "{:<26} ratio 1000/10: time {:.1}, nodes {:.1}",
                "",
                ratio(first.0 as f64, last.0 as f64),
                ratio(first.1 as f64, last.1 as f64),
            );
        }
    }
    Ok(())
}
