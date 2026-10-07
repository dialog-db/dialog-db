//! Compare two sweep directories and flag regressions.
//!
//! A report's step counters are exact: the engine took these steps,
//! and the same binary takes the same ones again, so a step count that
//! grows at all is a change in what the engine does. Block counts and
//! bytes wobble a little with where the tree's content-defined seams
//! fall, so they get the small allowance that wobble measures.
//! Instruction counts are callgrind's; the allowance covers toolchain
//! drift and layout, not work.
//!
//! The outcome must match exactly: a workload that answers differently
//! is a different workload (or a wrong answer), and either way the
//! baseline no longer describes it.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context as _, Result};

use crate::report::Report;

/// How much growth the gate allows.
#[derive(Debug, Clone)]
pub struct Tolerance {
    /// Relative growth in instructions allowed, in percent.
    pub instructions_pct: f64,
    /// Absolute growth in instructions always allowed, so a tiny phase
    /// is not gated on noise.
    pub instructions_slack: u64,
    /// Relative growth in byte counts allowed, in percent. A seam that
    /// lands a block earlier or later changes which leaf a read loads,
    /// and with it the bytes, by a tenth with the block count the same;
    /// the count is the signal, the bytes a check on its magnitude.
    pub bytes_pct: f64,
    /// Absolute growth in byte counts always allowed.
    pub bytes_slack: u64,
    /// Absolute growth in archive block counts allowed. The tree's leaf
    /// seams are content-defined, and a scenario's later commits read
    /// and write a block or two more or fewer from one run to the next
    /// with where the seams fall; two blocks is that wobble, measured,
    /// and a commit that costs more than that is a change.
    pub block_slack: u64,
    /// Absolute growth in engine step counts (`span.*`) allowed. The
    /// steps a workload takes do not depend on layout, so none.
    pub step_slack: u64,
}

impl Default for Tolerance {
    fn default() -> Self {
        Self {
            instructions_pct: 3.0,
            instructions_slack: 1_000_000,
            bytes_pct: 15.0,
            bytes_slack: 1024,
            block_slack: 2,
            step_slack: 0,
        }
    }
}

/// The outcome of comparing two sweep directories.
#[derive(Debug, Clone, Default)]
pub struct Comparison {
    /// One line per shared scenario: instructions and what moved.
    pub summary: Vec<String>,
    /// One line per detected regression; empty means the gate passes.
    pub regressions: Vec<String>,
    /// Improvements past the allowance, worth a re-baseline so the gate
    /// holds the new level.
    pub improvements: Vec<String>,
}

impl Comparison {
    /// Whether the gate passes (no regressions).
    pub fn passed(&self) -> bool {
        self.regressions.is_empty()
    }
}

/// Whether `new` grew past `base` by more than both `slack` and
/// `threshold_pct`.
fn regressed(base: u64, new: u64, slack: u64, threshold_pct: f64) -> bool {
    if new.saturating_sub(base) <= slack {
        return false;
    }
    if base == 0 {
        return true;
    }
    (new - base) as f64 / base as f64 * 100.0 > threshold_pct
}

fn change(base: u64, new: u64) -> String {
    if base == 0 {
        return format!("{base} -> {new}");
    }
    let pct = (new as f64 - base as f64) / base as f64 * 100.0;
    format!("{base} -> {new} ({pct:+.1}%)")
}

/// Load every `*.json` report in `dir`, keyed by scenario name.
fn load(dir: &Path) -> Result<BTreeMap<String, Report>> {
    let mut reports = BTreeMap::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            let text =
                fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
            let report: Report = serde_json::from_str(&text)
                .with_context(|| format!("parsing {}", path.display()))?;
            reports.insert(report.scenario.clone(), report);
        }
    }
    Ok(reports)
}

/// Compare the scenarios shared by two sweep directories under
/// `tolerance`.
pub fn compare_dirs(baseline: &Path, new: &Path, tolerance: &Tolerance) -> Result<Comparison> {
    let baseline = load(baseline)?;
    let new = load(new)?;
    let shared: Vec<&String> = baseline
        .keys()
        .filter(|name| new.contains_key(*name))
        .collect();
    anyhow::ensure!(!shared.is_empty(), "no overlapping reports to compare");

    let mut comparison = Comparison::default();
    for name in &shared {
        compare_reports(&baseline[*name], &new[*name], tolerance, &mut comparison);
    }
    for name in new.keys().filter(|name| !baseline.contains_key(*name)) {
        comparison.summary.push(format!(
            "{name}: no baseline (sweep into the baseline directory to add one)"
        ));
    }
    Ok(comparison)
}

fn compare_reports(base: &Report, new: &Report, tolerance: &Tolerance, out: &mut Comparison) {
    let name = &base.scenario;
    if base.size != new.size || base.profile != new.profile {
        out.regressions.push(format!(
            "{name}: compared at size {} / {} against a baseline at size {} / {}",
            new.size, new.profile, base.size, base.profile
        ));
        return;
    }
    for (key, expected) in &base.outcome {
        let actual = new.outcome.get(key).copied().unwrap_or(0);
        if actual != *expected {
            out.regressions.push(format!(
                "{name}: outcome {key} {expected} -> {actual}: the workload answers differently"
            ));
        }
    }

    let mut moved = Vec::new();
    for (key, &before) in &base.counters {
        let after = new.counters.get(key).copied().unwrap_or(0);
        if before == after {
            continue;
        }
        moved.push(format!("{key} {}", change(before, after)));
        let (slack, pct) = if key.ends_with(".bytes") {
            (tolerance.bytes_slack, tolerance.bytes_pct)
        } else if key.starts_with("archive.") {
            (tolerance.block_slack, 0.0)
        } else {
            (tolerance.step_slack, 0.0)
        };
        if regressed(before, after, slack, pct) {
            out.regressions
                .push(format!("{name}: {key} {}", change(before, after)));
        } else if regressed(after, before, slack, pct) {
            out.improvements
                .push(format!("{name}: {key} {}", change(before, after)));
        }
    }

    let instructions = match (base.instructions, new.instructions) {
        (Some(before), Some(after)) if base.volatile || new.volatile => {
            format!(
                "instructions {} (volatile, not gated)",
                change(before, after)
            )
        }
        (Some(before), Some(after)) => {
            if regressed(
                before,
                after,
                tolerance.instructions_slack,
                tolerance.instructions_pct,
            ) {
                out.regressions
                    .push(format!("{name}: instructions {}", change(before, after)));
            } else if regressed(
                after,
                before,
                tolerance.instructions_slack,
                tolerance.instructions_pct,
            ) {
                out.improvements
                    .push(format!("{name}: instructions {}", change(before, after)));
            }
            format!("instructions {}", change(before, after))
        }
        (Some(before), None) => {
            out.regressions.push(format!(
                "{name}: instructions not measured (baseline {before}); sweep under callgrind"
            ));
            "instructions not measured".to_string()
        }
        (None, Some(after)) => format!("instructions {after} (no baseline)"),
        (None, None) => "no instruction counts".to_string(),
    };
    let moved = if moved.is_empty() {
        "counters unchanged".to_string()
    } else {
        moved.join(", ")
    };
    out.summary
        .push(format!("{name}: {instructions} | {moved}"));
}
