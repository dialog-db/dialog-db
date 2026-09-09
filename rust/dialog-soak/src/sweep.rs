//! The sweep: run the join scenario across network profiles (and
//! optionally tree branching factors), one process per configuration,
//! keeping the median run per configuration as its report.
//!
//! Each configuration runs in its own child process because the tree
//! manifest override is read once per process (`DIALOG_TREE_FANOUT_N`).
//! Run-to-run leaf-boundary wobble (randomized identities shifting leaf
//! seams) moves a block or two between phases, so every configuration
//! runs `repeats` times and keeps the run with the median lazy-join
//! modeled time — the report the regression gate compares.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context as _, Result};

use crate::report::Report;

/// Configuration for one sweep.
#[derive(Debug, Clone)]
pub struct SweepConfig {
    /// Directory the per-configuration reports are written into.
    pub out_dir: PathBuf,
    /// Entities to seed per run.
    pub entities: usize,
    /// Commits to split the seed into.
    pub commits: usize,
    /// Runs per configuration; the median by lazy-join time is kept.
    pub repeats: usize,
    /// Network profile names (`none`, `localhost`, `broadband`, ...).
    pub networks: Vec<String>,
    /// Tree branching factors to sweep (`DIALOG_TREE_FANOUT_N`); empty
    /// sweeps only the default.
    pub fanouts: Vec<u8>,
}

/// Modeled lazy-join time: the sum of every phase but `download`.
fn lazy_ms(report: &Report) -> u64 {
    report
        .phases
        .iter()
        .filter(|phase| phase.name != "download")
        .map(|phase| phase.virtual_ms)
        .sum()
}

/// Run one configuration `repeats` times in child processes and return
/// the median run's report.
fn run_config(config: &SweepConfig, network: &str, fanout: Option<u8>) -> Result<Report> {
    let exe = std::env::current_exe().context("resolving the soak binary")?;
    let mut runs = Vec::with_capacity(config.repeats);
    for _ in 0..config.repeats {
        let mut command = Command::new(&exe);
        command
            .arg("run")
            .arg("--network")
            .arg(network)
            .arg("--entities")
            .arg(config.entities.to_string())
            .arg("--commits")
            .arg(config.commits.to_string())
            .arg("--json-only");
        if let Some(fanout) = fanout {
            command.env("DIALOG_TREE_FANOUT_N", fanout.to_string());
        }
        let output = command.output().context("spawning a soak run")?;
        anyhow::ensure!(
            output.status.success(),
            "soak run failed for network {network}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: Report =
            serde_json::from_slice(&output.stdout).context("parsing a soak run's report")?;
        runs.push(report);
    }
    runs.sort_by_key(lazy_ms);
    Ok(runs.swap_remove(runs.len() / 2))
}

/// Run the whole sweep, writing one report per configuration into
/// `config.out_dir` and returning the combined summary table.
pub fn sweep(config: &SweepConfig) -> Result<String> {
    fs::create_dir_all(&config.out_dir)
        .with_context(|| format!("creating {}", config.out_dir.display()))?;

    let fanouts: Vec<Option<u8>> = if config.fanouts.is_empty() {
        vec![None]
    } else {
        config.fanouts.iter().copied().map(Some).collect()
    };

    let mut written = Vec::new();
    for network in &config.networks {
        for fanout in &fanouts {
            let name = match fanout {
                Some(fanout) => format!("join-{network}-fanout{fanout}"),
                None => format!("join-{network}"),
            };
            eprintln!(
                "== {name} (entities={} commits={}, {} repeats)",
                config.entities, config.commits, config.repeats
            );
            let report = run_config(config, network, *fanout)?;
            let path = config.out_dir.join(format!("{name}.json"));
            fs::write(&path, serde_json::to_string_pretty(&report)?)
                .with_context(|| format!("writing {}", path.display()))?;
            written.push((name, report));
        }
    }

    let mut table = String::new();
    use std::fmt::Write as _;
    let _ = writeln!(
        table,
        "| {:<34} | {:<10} | {:>8} | {:>6} | {:>5} | {:>8} |",
        "config", "phase", "virt ms", "reqs", "dup", "KiB"
    );
    let _ = writeln!(
        table,
        "|{:-<36}|{:-<12}|{:-<10}|{:-<8}|{:-<7}|{:-<10}|",
        "", "", "", "", "", ""
    );
    for (name, report) in &written {
        for phase in &report.phases {
            let _ = writeln!(
                table,
                "| {:<34} | {:<10} | {:>8} | {:>6} | {:>5} | {:>8} |",
                name,
                phase.name,
                phase.virtual_ms,
                phase.traffic.requests,
                phase.duplicate_requests,
                phase.traffic.bytes / 1024
            );
        }
    }
    Ok(table)
}
