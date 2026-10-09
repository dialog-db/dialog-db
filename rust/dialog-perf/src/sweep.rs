//! The sweep: every scenario in its own child process, under callgrind
//! when asked, one report each.
//!
//! A child process per scenario keeps the counting subscriber and the
//! process-wide caches one scenario's own, and lets valgrind count one
//! measured phase. Collection starts off and toggles on the
//! `dialog_perf_measured` symbol, so the setup is not in the count.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context as _, Result, bail};

use crate::report::Report;
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
        report.instructions = Some(instructions);
    }
    Ok(report)
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
