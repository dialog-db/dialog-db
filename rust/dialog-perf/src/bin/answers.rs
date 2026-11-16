// The harness is native-only, as the `perf` binary is.
#![cfg_attr(target_arch = "wasm32", no_main)]
#![cfg(not(target_arch = "wasm32"))]

//! # answers
//!
//! What answering a concept query costs, from the scan to the receiver
//! holding rows it can read, phase by phase, for the zero-copy answers
//! research (`research/zero-copy-answers.md`).
//!
//! ```text
//! answers run --dataset issue --rows 3000           # one store, every phase, JSON
//! answers sweep --out-dir target/answers            # every variant under callgrind
//! ```
//!
//! A run seeds one store from a seed, then runs every phase on it, each
//! inside a function of its own (`answers_phase_*`) so callgrind counts
//! each phase's instructions apart, and the counting allocator counts each
//! phase's allocations:
//!
//! - `engine`: the query, to `Vec<ConceptConclusion>` (lazy or eager
//!   bindings, by `DIALOG_LAZY_BINDINGS`);
//! - `json_encode`: tonk's wire path, `project` into `Conclusion { this:
//!   String, fields: BTreeMap<String, Ipld> }` and `serde_json` of a
//!   snapshot frame;
//! - `json_decode`: the receiver's `serde_json` back into `Conclusion`s
//!   (in a page this is `JSON.parse` and `serde_wasm_bindgen`);
//! - `frame_encode`: the conclusions into a `dialog_answer` frame, read
//!   with `ConceptConclusion::view`;
//! - `frame_read_all`: the receiver accepts the frame and reads every
//!   cell, as a client that materializes every field would;
//! - `frame_read_two`: accepts it and reads two fields of every row, as a
//!   template that shows two would.
//!
//! Then a delta: a share of the rows change one field, and the delta's
//! rows are encoded both ways, the frame on the snapshot's session.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use dialog_answer::{Cell, Dictionary, Kind, Scalar, Session};
use dialog_corpus::{Entity, Pick, Value, ValueView};
use dialog_perf::allocator;
use dialog_perf::env::{Env, assert_all, concept, fact, query, text};
use dialog_query::query::Output as _;
use dialog_query::{ConceptConclusion, ConceptDescriptor, Parameters};
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};

#[global_allocator]
static ALLOCATOR: allocator::Counting = allocator::Counting;

#[derive(Debug, Parser)]
#[command(name = "answers", about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Seed one store and run every phase on it; print the report.
    Run {
        /// `stuff` (two text fields) or `issue` (eight fields, tonk's issue list).
        #[arg(long)]
        dataset: String,
        /// How many rows.
        #[arg(long)]
        rows: usize,
        /// Timed repetitions of each phase, after one warm-up.
        #[arg(long, default_value_t = 15)]
        repeat: usize,
    },
    /// Run every dataset, size and binding mode under callgrind, then
    /// natively for wall time, interleaved; write one report each.
    Sweep {
        #[arg(long, default_value = "target/answers")]
        out_dir: PathBuf,
        /// Native runs of each variant (interleaved), for wall time.
        #[arg(long, default_value_t = 3)]
        rounds: usize,
    },
}

/// The phases, in order, by the name of the function each runs in.
const PHASES: [&str; 6] = [
    "engine",
    "json_encode",
    "json_decode",
    "frame_encode",
    "frame_read_all",
    "frame_read_two",
];

/// The variants a sweep runs: dataset, rows.
const DATASETS: [(&str, usize); 4] = [("stuff", 2000), ("stuff", 20000), ("issue", 300), ("issue", 3000)];

fn main() -> ExitCode {
    match run_cli() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("answers: {error:#}");
            ExitCode::FAILURE
        }
    }
}

fn run_cli() -> Result<()> {
    match Cli::parse().command {
        Commands::Run {
            dataset,
            rows,
            repeat,
        } => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let report = measure(&runtime, &dataset, rows, repeat)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Commands::Sweep { out_dir, rounds } => sweep(out_dir, rounds),
    }
}

/// tonk's wire row (`tonk_worker_api::Conclusion`), as it is.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Conclusion {
    this: String,
    fields: BTreeMap<String, Ipld>,
}

/// tonk's wire frame (`tonk_worker_api::Frame`), as it is.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WireFrame {
    Snapshot {
        conclusions: Vec<Conclusion>,
    },
    Delta {
        asserted: Vec<Conclusion>,
        retracted: Vec<Conclusion>,
    },
}

/// tonk's `value_to_ipld`.
fn value_to_ipld(value: &Value) -> Option<Ipld> {
    match value {
        Value::UnsignedInt(u) => Some(match i128::try_from(*u) {
            Ok(i) => Ipld::Integer(i),
            Err(_) => Ipld::String(u.to_string()),
        }),
        other => ipld_core::serde::to_ipld(other.untagged()).ok(),
    }
}

/// tonk's `project` (less the keyed-collection fold, which these
/// concepts have no use for).
fn project(conclusion: &ConceptConclusion, terms: &Parameters) -> Conclusion {
    let mut fields = BTreeMap::new();
    for (name, term) in terms.iter() {
        let value = match term {
            dialog_query::Term::Constant(value) => value_to_ipld(value),
            dialog_query::Term::Variable { .. } => conclusion
                .source()
                .lookup(&dialog_query::Term::<dialog_query::Any>::var(name.clone()))
                .ok()
                .and_then(|binding| binding.as_value().and_then(value_to_ipld)),
        };
        if let Some(value) = value {
            fields.insert(name.clone(), value);
        }
    }
    Conclusion {
        this: conclusion.entity().to_string(),
        fields,
    }
}

/// A value read in place, as a frame cell.
fn scalar<'a>(view: &'a ValueView<'a>) -> Scalar<'a> {
    match view {
        ValueView::Bytes(bytes) => Scalar::Bytes(bytes),
        ValueView::Entity(uri) => Scalar::Entity(uri),
        ValueView::Boolean(b) => Scalar::Boolean(*b),
        ValueView::String(text) => Scalar::Text(text),
        ValueView::UnsignedInt(n) => Scalar::Natural(*n),
        ValueView::SignedInt(n) => Scalar::Integer(*n),
        ValueView::Float(n) => Scalar::Float(*n),
        ValueView::Record(bytes) => Scalar::Record(bytes),
        ValueView::Symbol(name) => Scalar::Symbol(name),
    }
}

/// Writes `conclusions` into `frame`, every column read in place.
fn write_rows(frame: &mut dialog_answer::Frame<'_>, columns: &[&str], conclusions: &[ConceptConclusion]) {
    for conclusion in conclusions {
        let views: Vec<Option<Option<ValueView<'_>>>> =
            columns.iter().map(|column| conclusion.view(column)).collect();
        frame.row_with(views.iter().map(|cell| {
            cell.as_ref()
                .map(|value| value.as_ref().map(scalar))
        }));
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
fn answers_phase_engine(
    runtime: &tokio::runtime::Runtime,
    env: &Env,
    query: dialog_query::ConceptQuery,
) -> Vec<ConceptConclusion> {
    answer(runtime, env, query)
}

/// The query's conclusions.
fn answer(
    runtime: &tokio::runtime::Runtime,
    env: &Env,
    query: dialog_query::ConceptQuery,
) -> Vec<ConceptConclusion> {
    runtime
        .block_on(async {
            env.branch
                .select(query)
                .perform(&env.operator)
                .try_vec()
                .await
        })
        .expect("the query runs")
}

#[unsafe(no_mangle)]
#[inline(never)]
fn answers_phase_json_encode(conclusions: &[ConceptConclusion], terms: &Parameters) -> Vec<u8> {
    let wire = WireFrame::Snapshot {
        conclusions: conclusions
            .iter()
            .map(|conclusion| project(conclusion, terms))
            .collect(),
    };
    serde_json::to_vec(&wire).expect("a frame serializes")
}

#[unsafe(no_mangle)]
#[inline(never)]
fn answers_phase_json_decode(bytes: &[u8]) -> usize {
    let frame: WireFrame = serde_json::from_slice(bytes).expect("a frame parses");
    match frame {
        WireFrame::Snapshot { conclusions } => conclusions.len(),
        WireFrame::Delta { asserted, .. } => asserted.len(),
    }
}

#[unsafe(no_mangle)]
#[inline(never)]
fn answers_phase_frame_encode(
    session: &mut Session,
    columns: &[&str],
    conclusions: &[ConceptConclusion],
) -> Vec<u8> {
    snapshot_frame(session, columns, conclusions)
}

/// The conclusions as a snapshot frame of `session`.
fn snapshot_frame(session: &mut Session, columns: &[&str], conclusions: &[ConceptConclusion]) -> Vec<u8> {
    let mut frame = session.frame(Kind::Snapshot, columns);
    write_rows(&mut frame, columns, conclusions);
    frame.finish()
}

/// Reads every cell of every row, summing what it read so nothing is
/// optimized away.
#[unsafe(no_mangle)]
#[inline(never)]
fn answers_phase_frame_read_all(bytes: Arc<[u8]>) -> usize {
    let mut dictionary = Dictionary::default();
    let answer = dictionary.accept(bytes).expect("a frame reads");
    let columns = answer.columns().count();
    let mut read = 0;
    for row in answer.section(0).iter() {
        for column in 0..columns {
            read += weigh(row.cell(&dictionary, column));
        }
    }
    read
}

/// Reads two fields of every row, as a template showing two does.
#[unsafe(no_mangle)]
#[inline(never)]
fn answers_phase_frame_read_two(bytes: Arc<[u8]>, fields: [&str; 2]) -> usize {
    let mut dictionary = Dictionary::default();
    let answer = dictionary.accept(bytes).expect("a frame reads");
    let [a, b] = fields.map(|field| answer.column(field).expect("the field is a column"));
    let mut read = 0;
    for row in answer.section(0).iter() {
        read += weigh(row.cell(&dictionary, a)) + weigh(row.cell(&dictionary, b));
    }
    read
}

fn weigh(cell: Cell<'_>) -> usize {
    match cell {
        Cell::Value(Scalar::Text(text) | Scalar::Entity(text) | Scalar::Symbol(text)) => text.len(),
        Cell::Value(Scalar::Bytes(bytes) | Scalar::Record(bytes)) => bytes.len(),
        Cell::Value(_) => 1,
        _ => 0,
    }
}

/// The dataset's concept, the fields a query asks for, and the two a
/// template would show.
struct Dataset {
    descriptor: ConceptDescriptor,
    fields: Vec<&'static str>,
    shown: [&'static str; 2],
    /// The field a delta changes.
    changed: &'static str,
}

fn dataset(name: &str) -> Result<Dataset> {
    Ok(match name {
        "stuff" => Dataset {
            descriptor: concept(serde_json::json!({ "with": {
                "name": { "the": "stuff/name", "as": "text:" },
                "role": { "the": "stuff/role", "as": "text:" }
            }})),
            fields: vec!["name", "role"],
            shown: ["name", "role"],
            changed: "role",
        },
        "issue" => Dataset {
            descriptor: concept(serde_json::json!({ "with": {
                "ident": { "the": "issue/ident", "as": "text:" },
                "title": { "the": "issue/title", "as": "text:" },
                "status": { "the": "issue/status", "as": "text:" },
                "priority": { "the": "issue/priority", "as": "natural:" },
                "created": { "the": "issue/created", "as": "natural:" },
                "creator": { "the": "issue/creator", "as": "entity:" },
                "assignee": { "the": "issue/assignee", "as": "entity:" },
                "project": { "the": "issue/project", "as": "entity:" }
            }})),
            fields: vec![
                "ident", "title", "status", "priority", "created", "creator", "assignee",
                "project",
            ],
            shown: ["title", "status"],
            changed: "status",
        },
        other => bail!("no dataset {other:?}: stuff or issue"),
    })
}

/// A deterministic did:key-shaped URI, as long as tonk's entities are
/// (56 bytes): `did:key:z6Mk` and 44 base58 characters from `seed`.
fn did(seed: &str) -> Entity {
    const ALPHABET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let hash = blake3::hash(seed.as_bytes());
    let mut uri = String::from("did:key:z6Mk");
    let mut stream = blake3::Hasher::new();
    stream.update(hash.as_bytes());
    let mut reader = stream.finalize_xof();
    let mut bytes = [0u8; 44];
    reader.fill(&mut bytes);
    uri.extend(bytes.iter().map(|b| ALPHABET[*b as usize % ALPHABET.len()] as char));
    uri.parse().expect("a did:key URI parses")
}

const WORDS: [&str; 16] = [
    "sync", "replica", "query", "subscription", "worker", "page", "render", "commit",
    "branch", "merge", "token", "frame", "index", "leaf", "session", "cache",
];
const STATUSES: [&str; 4] = ["todo", "doing", "review", "done"];

/// The facts of `rows` records of `name`, every value a function of the
/// row's index, so every store and run seeds the same tree.
fn seed(name: &str, rows: usize, round: usize) -> Vec<dialog_corpus::Fact> {
    let mut facts = Vec::new();
    match name {
        "stuff" => {
            for index in 0..rows {
                let this = dialog_perf::env::entity("stuff", index);
                facts.push(fact(&this, "stuff/name", text(format!("name-{index}"))));
                facts.push(fact(
                    &this,
                    "stuff/role",
                    text(format!("role-{}", (index + round) % 8)),
                ));
            }
        }
        _ => {
            let people: Vec<Entity> = (0..20).map(|n| did(&format!("person-{n}"))).collect();
            let projects: Vec<Entity> = (0..8).map(|n| did(&format!("project-{n}"))).collect();
            for index in 0..rows {
                let this = did(&format!("issue-{index}"));
                let title: Vec<&str> = (0..5).map(|w| WORDS[(index * 7 + w * 3) % 16]).collect();
                facts.push(fact(&this, "issue/ident", text(format!("TONK-{index}"))));
                facts.push(fact(&this, "issue/title", text(title.join(" "))));
                facts.push(fact(
                    &this,
                    "issue/status",
                    text(STATUSES[(index + round) % 4]),
                ));
                facts.push(fact(&this, "issue/priority", Value::UnsignedInt((index % 4) as u128)));
                facts.push(fact(
                    &this,
                    "issue/created",
                    Value::UnsignedInt(1_760_000_000_000 + index as u128 * 61_000),
                ));
                facts.push(fact(&this, "issue/creator", Value::Entity(people[index % 20].clone())));
                facts.push(fact(
                    &this,
                    "issue/assignee",
                    Value::Entity(people[(index * 3) % 20].clone()),
                ));
                facts.push(fact(&this, "issue/project", Value::Entity(projects[index % 8].clone())));
            }
        }
    }
    facts
}

#[derive(Debug, Serialize, Deserialize)]
struct Phase {
    allocations: BTreeMap<String, u64>,
    /// Wall time of each repetition, in microseconds, sorted.
    micros: Vec<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Report {
    dataset: String,
    rows: usize,
    lazy: bool,
    phases: BTreeMap<String, Phase>,
    bytes: BTreeMap<String, usize>,
    /// Instructions per phase, from callgrind (filled by `sweep`).
    #[serde(default)]
    instructions: BTreeMap<String, u64>,
}

/// Runs `body` once to warm up, then `repeat` times timed, counting the
/// allocations of the last.
fn phase<T>(repeat: usize, mut body: impl FnMut() -> T) -> (T, Phase) {
    let _ = body();
    let mut micros = Vec::with_capacity(repeat);
    for _ in 1..repeat.max(1) {
        let start = Instant::now();
        let out = body();
        micros.push(start.elapsed().as_secs_f64() * 1e6);
        drop(out);
    }
    allocator::start();
    let start = Instant::now();
    let out = body();
    micros.push(start.elapsed().as_secs_f64() * 1e6);
    let allocations = allocator::stop();
    micros.sort_by(f64::total_cmp);
    (out, Phase { allocations, micros })
}

fn measure(runtime: &tokio::runtime::Runtime, name: &str, rows: usize, repeat: usize) -> Result<Report> {
    let set = dataset(name)?;
    let env = runtime.block_on(async {
        let mut env = Env::open().await?;
        for chunk in seed(name, rows, 0).chunks(8192) {
            env.commit(assert_all(chunk.to_vec(), Pick::Last)).await?;
        }
        env.reopen().await?;
        anyhow::Ok(env)
    })?;
    let mut columns: Vec<&str> = vec!["this"];
    columns.extend(set.fields.iter().copied());
    let query = query(&set.descriptor, &set.fields, None);
    let terms = query.terms.clone();
    let mut phases = BTreeMap::new();
    let mut bytes = BTreeMap::new();

    let (conclusions, engine) = phase(repeat, || answers_phase_engine(runtime, &env, query.clone()));
    if conclusions.len() != rows {
        bail!("{name}: the query answered {} rows of {rows}", conclusions.len());
    }
    phases.insert("engine".into(), engine);

    let (json, encode) = phase(repeat, || answers_phase_json_encode(&conclusions, &terms));
    phases.insert("json_encode".into(), encode);
    let (decoded, decode) = phase(repeat, || answers_phase_json_decode(&json));
    assert_eq!(decoded, rows);
    phases.insert("json_decode".into(), decode);

    let (frame, encode) = phase(repeat, || {
        answers_phase_frame_encode(&mut Session::default(), &columns, &conclusions)
    });
    phases.insert("frame_encode".into(), encode);
    let frame: Arc<[u8]> = frame.into();
    let (_, read_all) = phase(repeat, || answers_phase_frame_read_all(frame.clone()));
    phases.insert("frame_read_all".into(), read_all);
    let (_, read_two) = phase(repeat, || answers_phase_frame_read_two(frame.clone(), set.shown));
    phases.insert("frame_read_two".into(), read_two);
    check_same(&json, &frame, &columns)?;
    bytes.insert("json_snapshot".into(), json.len());
    bytes.insert("frame_snapshot".into(), frame.len());

    // A delta: every 20th row's `changed` field moves on, as a page sees
    // after a round of edits. Both encodings carry the changed rows'
    // new versions and the old ones they replace; the frame is written
    // on the session that sent the snapshot.
    let mut session = Session::default();
    let snapshot = snapshot_frame(&mut session, &columns, &conclusions);
    let before: BTreeMap<String, usize> = conclusions
        .iter()
        .enumerate()
        .map(|(at, conclusion)| (conclusion.entity().to_string(), at))
        .collect();
    let changed: Vec<dialog_corpus::Fact> = seed(name, rows, 1)
        .into_iter()
        .enumerate()
        .filter(|(at, fact)| {
            let per_row = set.fields.len();
            (at / per_row) % 20 == 0 && fact.the.name() == set.changed
        })
        .map(|(_, fact)| fact)
        .collect();
    runtime.block_on(env.commit(assert_all(changed.clone(), Pick::Last)))?;
    let after = answer(runtime, &env, query.clone());
    let entities: std::collections::BTreeSet<String> =
        changed.iter().map(|fact| fact.of.to_string()).collect();
    let asserted: Vec<ConceptConclusion> = after
        .into_iter()
        .filter(|conclusion| entities.contains(&conclusion.entity().to_string()))
        .collect();
    let retracted: Vec<ConceptConclusion> = entities
        .iter()
        .map(|entity| conclusions[before[entity]].clone())
        .collect();
    let json_delta = serde_json::to_vec(&WireFrame::Delta {
        asserted: asserted.iter().map(|c| project(c, &terms)).collect(),
        retracted: retracted.iter().map(|c| project(c, &terms)).collect(),
    })?;
    let mut delta = session.frame(Kind::Delta, &columns);
    write_rows(&mut delta, &columns, &asserted);
    delta.retracted();
    write_rows(&mut delta, &columns, &retracted);
    let delta = delta.finish();
    let mut dictionary = Dictionary::default();
    dictionary.accept(snapshot.into())?;
    let answer = dictionary.accept(delta.clone().into())?;
    if answer.section(0).len() != entities.len() || answer.section(1).len() != entities.len() {
        bail!("the delta's sections disagree with the change");
    }
    bytes.insert("delta_rows".into(), entities.len());
    bytes.insert("json_delta".into(), json_delta.len());
    bytes.insert("frame_delta".into(), delta.len());

    Ok(Report {
        dataset: name.into(),
        rows,
        lazy: dialog_query::lazy_bindings(),
        phases,
        bytes,
        instructions: BTreeMap::new(),
    })
}

/// Refuses a run whose frame does not say what the JSON says: every row,
/// every field, the same value.
fn check_same(json: &[u8], frame: &Arc<[u8]>, columns: &[&str]) -> Result<()> {
    let WireFrame::Snapshot { conclusions } = serde_json::from_slice(json)? else {
        bail!("not a snapshot");
    };
    let mut dictionary = Dictionary::default();
    let answer = dictionary.accept(frame.clone())?;
    let rows = answer.section(0);
    if rows.len() != conclusions.len() {
        bail!("the frame has {} rows, the JSON {}", rows.len(), conclusions.len());
    }
    for (row, conclusion) in rows.iter().zip(&conclusions) {
        for (at, column) in columns.iter().enumerate() {
            let cell = row.get(&dictionary, at);
            let expected = if *column == "this" {
                Some(Ipld::String(conclusion.this.clone()))
            } else {
                conclusion.fields.get(*column).cloned()
            };
            let got = cell.map(|scalar| match scalar {
                Scalar::Text(t) | Scalar::Entity(t) | Scalar::Symbol(t) => Ipld::String(t.into()),
                Scalar::Natural(n) => Ipld::Integer(n as i128),
                Scalar::Integer(n) => Ipld::Integer(n),
                Scalar::Float(n) => Ipld::Float(n),
                Scalar::Boolean(b) => Ipld::Bool(b),
                Scalar::Bytes(b) | Scalar::Record(b) => Ipld::Bytes(b.to_vec()),
            });
            if got != expected {
                bail!("{column}: the frame says {got:?}, the JSON {expected:?}");
            }
        }
    }
    Ok(())
}

/// Every variant under callgrind for instructions, then natively for wall
/// time, the variants interleaved round by round.
fn sweep(out_dir: PathBuf, rounds: usize) -> Result<()> {
    std::fs::create_dir_all(&out_dir)?;
    let binary = std::env::current_exe()?;
    let variants: Vec<(&str, usize, &str)> = DATASETS
        .iter()
        .flat_map(|(name, rows)| [(*name, *rows, "0"), (*name, *rows, "1")])
        .collect();
    let mut reports: BTreeMap<String, Report> = BTreeMap::new();
    for round in 0..rounds {
        for (name, rows, lazy) in &variants {
            let key = format!("{name}-{rows}-{}", if *lazy == "1" { "lazy" } else { "eager" });
            eprintln!("round {round}: {key}");
            let output = Command::new(&binary)
                .args(["run", "--dataset", name, "--rows", &rows.to_string()])
                .env("DIALOG_LAZY_BINDINGS", lazy)
                .output()?;
            if !output.status.success() {
                bail!("{key}: {}", String::from_utf8_lossy(&output.stderr));
            }
            let report: Report = serde_json::from_slice(&output.stdout)?;
            match reports.get_mut(&key) {
                None => {
                    reports.insert(key, report);
                }
                Some(held) => {
                    for (phase, run) in report.phases {
                        if let Some(held) = held.phases.get_mut(&phase) {
                            held.micros.extend(run.micros);
                            held.micros.sort_by(f64::total_cmp);
                        }
                    }
                }
            }
        }
    }
    for (name, rows, lazy) in &variants {
        let key = format!("{name}-{rows}-{}", if *lazy == "1" { "lazy" } else { "eager" });
        eprintln!("callgrind: {key}");
        let out = out_dir.join(format!("{key}.callgrind"));
        let status = Command::new("valgrind")
            .args(["--tool=callgrind", "--quiet", "--collect-atstart=no"])
            .args(PHASES.map(|phase| format!("--toggle-collect=answers_phase_{phase}")))
            .arg(format!("--callgrind-out-file={}", out.display()))
            .arg(&binary)
            .args(["run", "--dataset", name, "--rows", &rows.to_string(), "--repeat", "1"])
            .env("DIALOG_LAZY_BINDINGS", lazy)
            .stdout(std::process::Stdio::null())
            .status()
            .context("valgrind")?;
        if !status.success() {
            bail!("{key}: callgrind failed");
        }
        let annotated = Command::new("callgrind_annotate")
            .args(["--inclusive=yes", "--threshold=100"])
            .arg(&out)
            .output()?;
        let text = String::from_utf8_lossy(&annotated.stdout);
        let report = reports.get_mut(&key).expect("every variant ran natively");
        for phase in PHASES {
            let symbol = format!("answers_phase_{phase} ");
            let total = text
                .lines()
                .find(|line| line.contains(&format!(":{}", symbol.trim_end())) && line.contains(" ["))
                .and_then(|line| line.split_whitespace().next())
                .map(|count| count.replace(',', ""))
                .and_then(|count| count.parse::<u64>().ok())
                .unwrap_or(0);
            // A phase that runs twice in a run (warm-up and the counted
            // repetition) is halved; `--repeat 1` runs each twice.
            report.instructions.insert(phase.into(), total / 2);
        }
    }
    for (key, report) in &reports {
        std::fs::write(
            out_dir.join(format!("{key}.json")),
            serde_json::to_string_pretty(report)?,
        )?;
    }
    eprintln!("{} reports in {}", reports.len(), out_dir.display());
    Ok(())
}
