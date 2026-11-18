// The harness is native-only, as the `perf` binary is.
#![cfg_attr(target_arch = "wasm32", no_main)]
#![cfg(not(target_arch = "wasm32"))]

//! # answers
//!
//! What answering a concept query costs, from the scan to tonk's JSON and
//! back, phase by phase, for the zero-copy answers research
//! (`research/zero-copy-answers.md`). The binary answer frame's phases
//! live with the frame, in its own pull request.
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
//! - `engine`: the query, to `Vec<ConceptConclusion>`;
//! - `json_encode`: tonk's wire path, `project` into `Conclusion { this:
//!   String, fields: BTreeMap<String, Ipld> }` and `serde_json` of a
//!   snapshot frame;
//! - `json_decode`: the receiver's `serde_json` back into `Conclusion`s
//!   (in a page this is `JSON.parse` and `serde_wasm_bindgen`);
//! - `json_view_encode`: the same JSON written from the rows in place,
//!   checked byte for byte against `json_encode`'s.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
#[cfg(not(dialog_main))]
use dialog_corpus::ValueView;
use dialog_corpus::{Entity, Pick, Value};
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
        /// Main's build of this harness (`--cfg dialog_main`), run as the
        /// `main` variant beside the branch's eager and lazy ones.
        #[arg(long)]
        main: Option<PathBuf>,
    },
}

/// The phases, in order, by the name of the function each runs in.
const PHASES: [&str; 4] = ["json_view_encode", "engine", "json_encode", "json_decode"];

/// The variants a sweep runs: dataset, rows.
const DATASETS: [(&str, usize); 4] = [
    ("stuff", 2000),
    ("issue", 3000),
    ("tonk-issue", 300),
    ("tonk-issue", 3000),
];

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
        Commands::Sweep {
            out_dir,
            rounds,
            main,
        } => sweep(out_dir, rounds, main),
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

/// Tonk's snapshot JSON, written from the values the rows hold in place:
/// nothing is hydrated into a `Value` or an `Ipld` to be serialized. The
/// same bytes as [`answers_phase_json_encode`] (checked every run).
#[cfg(not(dialog_main))]
#[unsafe(no_mangle)]
#[inline(never)]
fn answers_phase_json_view_encode(conclusions: &[ConceptConclusion], fields: &[&str]) -> Vec<u8> {
    use std::io::Write as _;
    // `project` writes every term, `this` among them, in its `BTreeMap`'s
    // order.
    let mut sorted: Vec<&str> = fields.to_vec();
    sorted.push("this");
    sorted.sort_unstable();
    let mut out = Vec::with_capacity(conclusions.len() * 256);
    out.extend_from_slice(br#"{"kind":"snapshot","conclusions":["#);
    for (at, conclusion) in conclusions.iter().enumerate() {
        if at > 0 {
            out.push(b',');
        }
        out.extend_from_slice(br#"{"this":"#);
        match conclusion.view("this") {
            Some(Some(ValueView::Entity(uri))) => {
                serde_json::to_writer(&mut out, uri.as_ref()).expect("a string writes")
            }
            _ => serde_json::to_writer(&mut out, conclusion.entity().as_str())
                .expect("a string writes"),
        }
        out.extend_from_slice(br#","fields":{"#);
        let mut first = true;
        for field in &sorted {
            let Some(Some(view)) = conclusion.view(field) else {
                continue;
            };
            if !first {
                out.push(b',');
            }
            first = false;
            serde_json::to_writer(&mut out, field).expect("a string writes");
            out.push(b':');
            match &view {
                ValueView::String(text) | ValueView::Entity(text) | ValueView::Symbol(text) => {
                    serde_json::to_writer(&mut out, text.as_ref())
                }
                ValueView::UnsignedInt(n) => match i128::try_from(*n) {
                    Ok(n) => serde_json::to_writer(&mut out, &n),
                    Err(_) => serde_json::to_writer(&mut out, &n.to_string()),
                },
                ValueView::SignedInt(n) => serde_json::to_writer(&mut out, n),
                ValueView::Float(n) => serde_json::to_writer(&mut out, n),
                ValueView::Boolean(b) => serde_json::to_writer(&mut out, b),
                ValueView::Bytes(bytes) | ValueView::Record(bytes) => {
                    serde_json::to_writer(&mut out, &Ipld::Bytes(bytes.to_vec()))
                }
            }
            .expect("a value writes");
        }
        let _ = out.write_all(b"}}");
    }
    out.extend_from_slice(b"]}");
    out
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

/// The dataset's concept and the fields a query asks for.
struct Dataset {
    descriptor: ConceptDescriptor,
    fields: Vec<&'static str>,
}

fn dataset(name: &str) -> Result<Dataset> {
    Ok(match name {
        "stuff" => Dataset {
            descriptor: concept(serde_json::json!({ "with": {
                "name": { "the": "stuff/name", "as": "text:" },
                "role": { "the": "stuff/role", "as": "text:" }
            }})),
            fields: vec!["name", "role"],
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
                "ident", "title", "status", "priority", "created", "creator", "assignee", "project",
            ],
        },
        // tonk's own issue concept (`v2/issue`): three required fields and
        // ten optional ones, present at the rates `seed` writes them.
        "tonk-issue" => Dataset {
            descriptor: concept(serde_json::json!({ "with": {
                "ident": { "the": "bug/ident", "as": "text:" },
                "title": { "the": "bug/title", "as": "text:" },
                "status": { "the": "bug/status", "as": "text:" },
                "assignee": { "the": "bug/assignee", "as": "text:", "optional": true },
                "created": { "the": "bug/created-at", "as": "text:", "optional": true },
                "creator": { "the": "bug/creator", "as": "entity:", "optional": true },
                "creator-name": { "the": "bug/creator-name", "as": "text:", "optional": true },
                "detail": { "the": "bug/detail", "as": "text:", "optional": true },
                "due": { "the": "bug/due-date", "as": "text:", "optional": true },
                "effort": { "the": "bug/effort", "as": "natural:", "optional": true },
                "ordering": { "the": "bug/ordering", "as": "float:", "optional": true },
                "priority": { "the": "bug/priority", "as": "text:", "optional": true },
                "project": { "the": "bug/project", "as": "entity:", "optional": true }
            }})),
            fields: vec![
                "ident",
                "title",
                "status",
                "assignee",
                "created",
                "creator",
                "creator-name",
                "detail",
                "due",
                "effort",
                "ordering",
                "priority",
                "project",
            ],
        },
        other => bail!("no dataset {other:?}: stuff, issue or tonk-issue"),
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
    uri.extend(
        bytes
            .iter()
            .map(|b| ALPHABET[*b as usize % ALPHABET.len()] as char),
    );
    uri.parse().expect("a did:key URI parses")
}

const WORDS: [&str; 16] = [
    "sync",
    "replica",
    "query",
    "subscription",
    "worker",
    "page",
    "render",
    "commit",
    "branch",
    "merge",
    "token",
    "frame",
    "index",
    "leaf",
    "session",
    "cache",
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
        "tonk-issue" => {
            let people: Vec<Entity> = (0..6).map(|n| did(&format!("person-{n}"))).collect();
            let projects: Vec<Entity> = (0..22).map(|n| did(&format!("project-{n}"))).collect();
            // Present on `percent` of rows, spread by a stride.
            let on = |index: usize, salt: usize, percent: usize| {
                (index * 37 + salt * 11) % 100 < percent
            };
            for index in 0..rows {
                let this = did(&format!("bug-{index}"));
                let title: Vec<&str> = (0..5).map(|w| WORDS[(index * 7 + w * 3) % 16]).collect();
                facts.push(fact(&this, "bug/ident", text(format!("TONK-{index}"))));
                facts.push(fact(&this, "bug/title", text(title.join(" "))));
                facts.push(fact(
                    &this,
                    "bug/status",
                    text(STATUSES[(index + round) % 4]),
                ));
                if on(index, 1, 96) {
                    facts.push(fact(
                        &this,
                        "bug/assignee",
                        text(format!("person-{}", index % 6)),
                    ));
                }
                facts.push(fact(
                    &this,
                    "bug/created-at",
                    text(format!("2026-10-{:02}T10:00:00Z", index % 28 + 1)),
                ));
                facts.push(fact(
                    &this,
                    "bug/creator",
                    Value::Entity(people[index % 6].clone()),
                ));
                facts.push(fact(
                    &this,
                    "bug/creator-name",
                    text(format!("Person {}", index % 6)),
                ));
                if on(index, 2, 50) {
                    facts.push(fact(
                        &this,
                        "bug/detail",
                        text(format!("{} {}", title.join(" "), title.join(" "))),
                    ));
                }
                if on(index, 3, 10) {
                    facts.push(fact(
                        &this,
                        "bug/due-date",
                        text(format!("2026-12-{:02}", index % 28 + 1)),
                    ));
                }
                if on(index, 4, 30) {
                    facts.push(fact(
                        &this,
                        "bug/effort",
                        Value::UnsignedInt((index % 5) as u128),
                    ));
                }
                facts.push(fact(
                    &this,
                    "bug/ordering",
                    Value::Float(index as f64 * 0.5),
                ));
                if on(index, 5, 60) {
                    facts.push(fact(
                        &this,
                        "bug/priority",
                        text(["low", "medium", "high"][index % 3]),
                    ));
                }
                if on(index, 6, 39) {
                    facts.push(fact(
                        &this,
                        "bug/project",
                        Value::Entity(projects[index % 22].clone()),
                    ));
                }
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
                facts.push(fact(
                    &this,
                    "issue/priority",
                    Value::UnsignedInt((index % 4) as u128),
                ));
                facts.push(fact(
                    &this,
                    "issue/created",
                    Value::UnsignedInt(1_760_000_000_000 + index as u128 * 61_000),
                ));
                facts.push(fact(
                    &this,
                    "issue/creator",
                    Value::Entity(people[index % 20].clone()),
                ));
                facts.push(fact(
                    &this,
                    "issue/assignee",
                    Value::Entity(people[(index * 3) % 20].clone()),
                ));
                facts.push(fact(
                    &this,
                    "issue/project",
                    Value::Entity(projects[index % 8].clone()),
                ));
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
    /// Values hydrated per phase, by the line that asked and what it
    /// made (`trace-hydration` builds only), per row.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    hydration: BTreeMap<String, BTreeMap<String, f64>>,
}

/// What the phase just run hydrated, per row and per run of it.
#[cfg(feature = "trace-hydration")]
fn hydrated(rows: usize, runs: usize) -> BTreeMap<String, f64> {
    dialog_query::hydration::take()
        .into_iter()
        .map(|((site, what), count)| {
            let site = site.rsplit("rust/").next().unwrap_or(&site).to_string();
            (
                format!("{what} @ {site}"),
                count as f64 / rows as f64 / runs as f64,
            )
        })
        .collect()
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
    (
        out,
        Phase {
            allocations,
            micros,
        },
    )
}

fn measure(
    runtime: &tokio::runtime::Runtime,
    name: &str,
    rows: usize,
    repeat: usize,
) -> Result<Report> {
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
    #[allow(unused_mut)]
    let mut hydration = BTreeMap::new();
    let mut bytes = BTreeMap::new();

    let (conclusions, engine) = phase(repeat, || {
        answers_phase_engine(runtime, &env, query.clone())
    });
    if conclusions.len() != rows {
        bail!(
            "{name}: the query answered {} rows of {rows}",
            conclusions.len()
        );
    }
    phases.insert("engine".into(), engine);
    #[cfg(feature = "trace-hydration")]
    hydration.insert("engine".into(), hydrated(rows, repeat.max(1) + 1));

    // Before `project`: it hydrates the rows' values, and the view phase
    // must read rows nothing hydrated.
    #[cfg(not(dialog_main))]
    let (viewed, view_run) = phase(repeat, || {
        answers_phase_json_view_encode(&conclusions, &set.fields)
    });
    let (json, encode) = phase(repeat, || answers_phase_json_encode(&conclusions, &terms));
    phases.insert("json_encode".into(), encode);
    #[cfg(feature = "trace-hydration")]
    // The phase reads the same conclusions every run: a value it hydrates
    // stays hydrated, so its count is the first run's.
    hydration.insert("json_encode".into(), hydrated(rows, 1));
    #[cfg(not(dialog_main))]
    {
        if viewed != json {
            let at = viewed
                .iter()
                .zip(&json)
                .position(|(a, b)| a != b)
                .unwrap_or(0);
            let around = |bytes: &[u8]| {
                String::from_utf8_lossy(&bytes[at.saturating_sub(60)..(at + 60).min(bytes.len())])
                    .into_owned()
            };
            bail!(
                "{name}: the JSON written from views differs from tonk's at byte {at}:\n views: {}\n tonk:  {}",
                around(&viewed),
                around(&json)
            );
        }
        phases.insert("json_view_encode".into(), view_run);
    }
    let (decoded, decode) = phase(repeat, || answers_phase_json_decode(&json));
    assert_eq!(decoded, rows);
    phases.insert("json_decode".into(), decode);
    #[cfg(feature = "trace-hydration")]
    hydration.insert("json_decode".into(), hydrated(rows, 1));

    bytes.insert("json_snapshot".into(), json.len());
    // The answer's identity: every variant of a dataset must agree on it.
    bytes.insert(
        "answer_hash".into(),
        u32::from_le_bytes(blake3::hash(&json).as_bytes()[..4].try_into()?) as usize,
    );

    Ok(Report {
        dataset: name.into(),
        rows,
        lazy: lazy(),
        phases,
        bytes,
        instructions: BTreeMap::new(),
        hydration,
    })
}

/// Whether this build binds scanned rows in place: the branch always
/// does, main never did.
#[cfg(not(dialog_main))]
fn lazy() -> bool {
    true
}

#[cfg(dialog_main)]
fn lazy() -> bool {
    false
}

/// Every variant under callgrind for instructions, then natively for wall
/// time, the variants interleaved round by round.
fn sweep(out_dir: PathBuf, rounds: usize, main: Option<PathBuf>) -> Result<()> {
    std::fs::create_dir_all(&out_dir)?;
    let branch = std::env::current_exe()?;
    // (dataset, rows, "main" for main's build or "lazy" for this one's).
    // This build's variant keeps the label it had when an eager mode of
    // the same build stood beside it, so its reports compare with the
    // ones recorded then.
    let variants: Vec<(&str, usize, &str)> = DATASETS
        .iter()
        .flat_map(|(name, rows)| {
            let mut modes = vec![(*name, *rows, "lazy")];
            if main.is_some() {
                modes.insert(0, (*name, *rows, "main"));
            }
            modes
        })
        .collect();
    let binary = |lazy: &str| -> PathBuf {
        match (lazy, &main) {
            ("main", Some(main)) => main.clone(),
            _ => branch.clone(),
        }
    };
    let mut reports: BTreeMap<String, Report> = BTreeMap::new();
    for round in 0..rounds {
        for (name, rows, lazy) in &variants {
            let key = format!("{name}-{rows}-{lazy}");
            eprintln!("round {round}: {key}");
            let output = Command::new(binary(lazy))
                .args(["run", "--dataset", name, "--rows", &rows.to_string()])
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
        let key = format!("{name}-{rows}-{lazy}");
        eprintln!("callgrind: {key}");
        let out = out_dir.join(format!("{key}.callgrind"));
        let status = Command::new("valgrind")
            .args(["--tool=callgrind", "--quiet", "--collect-atstart=no"])
            .args(PHASES.map(|phase| format!("--toggle-collect=answers_phase_{phase}")))
            .arg(format!("--callgrind-out-file={}", out.display()))
            .arg(binary(lazy))
            .args([
                "run",
                "--dataset",
                name,
                "--rows",
                &rows.to_string(),
                "--repeat",
                "1",
            ])
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
                .find(|line| {
                    line.contains(&format!(":{}", symbol.trim_end())) && line.contains(" [")
                })
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
