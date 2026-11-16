//! The sweep: every scenario in its own child process, under callgrind
//! when asked, one report each.
//!
//! A child process per scenario keeps the counting subscriber and the
//! process-wide caches one scenario's own, and lets valgrind count one
//! measured phase. Collection starts off and toggles on the
//! `dialog_perf_measured` symbol, so the setup is not in the count.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context as _, Result, bail};

use crate::report::Report;
use crate::runner::Runner;
use crate::scenario::catalog;

/// Configuration for one sweep.
#[derive(Debug, Clone)]
pub struct SweepConfig {
    /// Directory the per-scenario reports are written into.
    pub out_dir: PathBuf,
    /// Scenario names to run; empty runs the whole catalog.
    pub only: Vec<String>,
    /// Whether to run each scenario under callgrind for its instruction
    /// count. Off, the reports carry counters alone.
    pub callgrind: bool,
}

/// Run the sweep, writing `<out_dir>/<scenario>.json` per scenario (and
/// `<scenario>.callgrind` beside it under callgrind, for
/// `callgrind_annotate`). Returns the reports in catalog order.
pub fn sweep(config: &SweepConfig) -> Result<Vec<Report>> {
    let exe = std::env::current_exe().context("resolving the perf binary")?;
    fs::create_dir_all(&config.out_dir)
        .with_context(|| format!("creating {}", config.out_dir.display()))?;
    Runner::current().write(&config.out_dir)?;
    let mut reports = Vec::new();
    for spec in catalog() {
        if !config.only.is_empty() && !config.only.iter().any(|name| name == spec.name) {
            continue;
        }
        let report = run_child(&exe, spec.name, config)?;
        let path = config.out_dir.join(format!("{}.json", spec.name));
        let mut text = serde_json::to_string_pretty(&report)?;
        text.push('\n');
        fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
        let blocks = format!(
            "{} blocks read, {} written",
            report.counters.get("archive.get").copied().unwrap_or(0),
            report.counters.get("archive.put").copied().unwrap_or(0),
        );
        match report.instructions {
            Some(instructions) => eprintln!("{}: {instructions} instructions, {blocks}", spec.name),
            None => eprintln!("{}: {blocks}", spec.name),
        }
        reports.push(report);
    }
    Ok(reports)
}

fn run_child(exe: &Path, scenario: &str, config: &SweepConfig) -> Result<Report> {
    let callgrind_out = config.out_dir.join(format!("{scenario}.callgrind"));
    let mut command = if config.callgrind {
        let mut command = Command::new("valgrind");
        command
            .arg("--tool=callgrind")
            .arg("--collect-atstart=no")
            .arg("--toggle-collect=dialog_perf_measured")
            .arg("--cache-sim=no")
            .arg(format!("--callgrind-out-file={}", callgrind_out.display()))
            .arg(exe);
        command
    } else {
        Command::new(exe)
    };
    command.arg("run").arg(scenario);
    let output = match command.output() {
        Ok(output) => output,
        Err(error) if config.callgrind && error.kind() == std::io::ErrorKind::NotFound => {
            bail!("valgrind is not on PATH; install it or sweep with --no-callgrind")
        }
        Err(error) => return Err(error).context(format!("spawning {scenario}")),
    };
    if !output.status.success() {
        bail!(
            "{scenario} failed ({}):\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut report: Report = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("parsing {scenario}'s report"))?;
    if config.callgrind {
        let instructions = collected(&callgrind_out)?;
        if instructions == 0 {
            bail!(
                "{scenario}: callgrind collected nothing; the dialog_perf_measured symbol was not found (stripped?)"
            );
        }
        let text = fs::read_to_string(&callgrind_out)
            .with_context(|| format!("reading {}", callgrind_out.display()))?;
        report.instructions = Some(instructions);
        let allocator =
            allocator(&text).with_context(|| format!("reading {scenario}'s callgrind output"))?;
        // The allocator's calls are part of what was collected; a figure
        // past the total is a misread output, not a cost.
        anyhow::ensure!(
            allocator <= instructions,
            "{scenario}: {allocator} instructions in the allocator, past the {instructions} collected"
        );
        report.instructions_outside_allocator = Some(instructions - allocator);
    }
    Ok(report)
}

/// The allocator's entry points: the C allocator's, and Rust's
/// global-allocator shims, which an over-aligned `realloc` copies in
/// itself rather than in the C one, and in which the harness's counting
/// allocator runs.
const ENTRY: [&str; 22] = [
    "malloc",
    "calloc",
    "posix_memalign",
    "aligned_alloc",
    "memalign",
    "valloc",
    "pvalloc",
    "realloc",
    "free",
    "cfree",
    "__rust_alloc",
    "__rust_alloc_zeroed",
    "__rust_realloc",
    "__rust_dealloc",
    "__rdl_alloc",
    "__rdl_alloc_zeroed",
    "__rdl_realloc",
    "__rdl_dealloc",
    "__rg_alloc",
    "__rg_alloc_zeroed",
    "__rg_realloc",
    "__rg_dealloc",
];

/// The harness's counting allocator ([`crate::allocator::Counting`]):
/// its methods are entry points too.
const ENTRY_TYPE: &str = "<dialog_perf::allocator::Counting as ";

/// The instructions the measured phase spent in the allocator, from a
/// callgrind output: the inclusive cost of every call from a function
/// outside the allocator to one of its entry points, so what the
/// allocator calls, such as the copy a `realloc` makes when it moves a
/// block, is counted with it. A caller is inside the allocator when it
/// is an entry point itself or is defined in glibc's `malloc.c` or
/// `arena.c`, so a call `realloc` makes to `malloc` is not counted
/// twice.
///
/// The format compresses names: `(id) name` the first time, `(id)`
/// after, one table for functions and one for files. A `calls=` line is
/// followed by the line holding the call's inclusive cost: its position
/// (one column, `positions: line`) and then its one event
/// (`events: Ir`). An output in any other shape is refused rather than
/// misread. Recursion suffixes (`'2`) are dropped. Callgrind counts a
/// call's cost only while it collects, so this is the measured phase's;
/// it counts the calls themselves whether it collects or not, so those
/// are counted in the process instead ([`crate::allocator`]).
fn allocator(text: &str) -> Result<u64> {
    let entry = |name: &str| {
        let name = unrecursed(name);
        ENTRY.contains(&name) || name.starts_with(ENTRY_TYPE)
    };
    let mut functions: HashMap<String, String> = HashMap::new();
    let mut files: HashMap<String, String> = HashMap::new();
    let mut file = String::new();
    let mut inside = false;
    let mut into_allocator = false;
    let mut pending = false;
    let mut instructions = 0;
    for line in text.lines() {
        if let Some(positions) = line.strip_prefix("positions:") {
            anyhow::ensure!(
                positions.trim() == "line",
                "callgrind positions `{}`, not `line`",
                positions.trim()
            );
        } else if let Some(events) = line.strip_prefix("events:") {
            anyhow::ensure!(
                events.trim() == "Ir",
                "callgrind events `{}`, not `Ir`",
                events.trim()
            );
        }
        if pending {
            pending = false;
            if !inside {
                instructions += line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|n| n.parse::<u64>().ok())
                    .unwrap_or(0);
            }
            continue;
        }
        if let Some(spec) = line.strip_prefix("fl=") {
            file = resolve(&mut files, spec);
        } else if let Some(spec) = line
            .strip_prefix("fi=")
            .or_else(|| line.strip_prefix("fe="))
            .or_else(|| line.strip_prefix("cfi="))
            .or_else(|| line.strip_prefix("cfl="))
        {
            resolve(&mut files, spec);
        } else if let Some(spec) = line.strip_prefix("fn=") {
            let name = resolve(&mut functions, spec);
            inside = entry(&name)
                || ["/malloc/", "malloc.c", "arena.c"]
                    .iter()
                    .any(|part| file.contains(part));
        } else if let Some(spec) = line.strip_prefix("cfn=") {
            into_allocator = entry(&resolve(&mut functions, spec));
        } else if line.starts_with("calls=") {
            pending = into_allocator;
        }
    }
    Ok(instructions)
}

/// A compressed name: `(id) name` defines `id`, `(id)` refers to it.
fn resolve(table: &mut HashMap<String, String>, spec: &str) -> String {
    let spec = spec.trim();
    if let Some(rest) = spec.strip_prefix('(')
        && let Some((id, name)) = rest.split_once(')')
    {
        let name = name.trim();
        if name.is_empty() {
            return table.get(id).cloned().unwrap_or_default();
        }
        table.insert(id.to_string(), name.to_string());
        return name.to_string();
    }
    spec.to_string()
}

/// A function's name without callgrind's recursion suffix (`'2`).
fn unrecursed(name: &str) -> &str {
    match name.rsplit_once('\'') {
        Some((base, depth)) if !depth.is_empty() && depth.bytes().all(|b| b.is_ascii_digit()) => {
            base
        }
        _ => name,
    }
}

/// The instructions a callgrind output file collected: its `summary:`
/// line (what the toggles enclosed), falling back to `totals:`.
fn collected(path: &Path) -> Result<u64> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let count = |prefix: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(prefix))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|first| first.parse::<u64>().ok())
    };
    count("summary:")
        .or_else(|| count("totals:"))
        .with_context(|| format!("{} carries no summary", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A call into the allocator from outside it is counted with its
    /// inclusive cost, a callee's copy included; calls the allocator
    /// makes to itself, and calls to anything else, are not. Names are
    /// given once and referred to by id after, and a recursion suffix
    /// names the same function.
    #[dialog_common::test]
    fn it_counts_the_allocator_from_a_callgrind_output() {
        let text = "\
fl=(1) src/engine.rs
fn=(1) caller_a
5 1000
cfn=(2) malloc
calls=3 0x10
4 120
cfn=(3) memcpy
calls=5 0x20
4 50
fn=(4) caller_b
cfn=(2)
calls=2 0x10
7 80
cfn=(5) realloc'2
calls=1 0x30
7 900
cfn=(6) <dialog_perf::allocator::Counting as core::alloc::global::GlobalAlloc>::dealloc
calls=4 0x30
7 40
fl=(2) ./malloc/./malloc/malloc.c
fn=(5)
cfn=(2)
calls=1 0x10
9 60
cfn=(3)
calls=1 0x20
9 700
";
        assert_eq!(allocator(text).unwrap(), 120 + 80 + 900 + 40);
    }

    /// Without debug information for the C library the allocator's own
    /// functions carry no file, and the entry points' names alone keep a
    /// call `realloc` makes to `malloc` from being counted twice.
    #[dialog_common::test]
    fn it_counts_the_allocator_without_file_names() {
        let text = "\
positions: line
events: Ir
fl=(1) ???
fn=(1) caller
cfn=(2) realloc
calls=1 0x10
4 500
fn=(2)
cfn=(3) malloc
calls=1 0x20
9 200
";
        assert_eq!(allocator(text).unwrap(), 500);
    }

    /// An output with another position or event layout is refused, not
    /// read by the wrong column.
    #[dialog_common::test]
    fn it_refuses_a_callgrind_output_of_another_shape() {
        assert!(allocator("positions: instr line\nevents: Ir\n").is_err());
        assert!(allocator("positions: line\nevents: Ir Dr\n").is_err());
    }
}
