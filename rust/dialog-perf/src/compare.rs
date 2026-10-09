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
/// `threshold_pct`. From a baseline of zero any growth is a change in
/// what the workload does, not wobble around a level it already had,
/// so the slack does not apply.
fn regressed(base: u64, new: u64, slack: u64, threshold_pct: f64) -> bool {
    if base == 0 {
        return new > 0;
    }
    if new.saturating_sub(base) <= slack {
        return false;
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
    // A scenario the sweep ran with no baseline is ungated, and a
    // baseline no scenario answers gates nothing: both fail, so a
    // scenario cannot drop out of the gate by being renamed or by its
    // baseline being deleted.
    for name in new.keys().filter(|name| !baseline.contains_key(*name)) {
        comparison.regressions.push(format!(
            "{name}: no baseline (sweep into the baseline directory to add one)"
        ));
    }
    for name in baseline.keys().filter(|name| !new.contains_key(*name)) {
        comparison.regressions.push(format!(
            "{name}: a baseline no scenario in the sweep answers"
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

    // Every counter either report has: a step the baseline never took
    // and the new run takes counts from zero.
    let keys: std::collections::BTreeSet<&String> =
        base.counters.keys().chain(new.counters.keys()).collect();
    let mut moved = Vec::new();
    for key in keys {
        let before = base.counters.get(key).copied().unwrap_or(0);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn report(counters: &[(&str, u64)]) -> Report {
        Report {
            scenario: "s".into(),
            size: 1,
            profile: "release".into(),
            outcome: BTreeMap::new(),
            counters: counters
                .iter()
                .map(|(key, value)| (key.to_string(), *value))
                .collect(),
            instructions: None,
            volatile: false,
        }
    }

    /// A step the baseline never took, taken now, is a change in what
    /// the engine does: a scenario that settled no cell and now settles
    /// five hundred regresses. The comparison walks the baseline's
    /// counters alone, so a counter the baseline lacks is never gated.
    #[test]
    fn a_step_the_baseline_never_took_is_a_regression() {
        let mut comparison = Comparison::default();
        compare_reports(
            &report(&[]),
            &report(&[("span.settle_cell", 500)]),
            &Tolerance::default(),
            &mut comparison,
        );
        assert!(
            !comparison.passed(),
            "500 settlements where the baseline had none: {:?}",
            comparison.summary
        );
    }

    /// A read scenario that starts writing blocks is a change in what
    /// the engine does, not seam wobble: the block allowance is for a
    /// commit landing a block earlier or later, and a baseline of zero
    /// has no seams to wobble.
    #[test]
    fn a_read_that_starts_writing_is_a_regression() {
        let mut comparison = Comparison::default();
        compare_reports(
            &report(&[("archive.put", 0)]),
            &report(&[("archive.put", 2)]),
            &Tolerance::default(),
            &mut comparison,
        );
        assert!(
            !comparison.passed(),
            "two blocks written where the baseline wrote none: {:?}",
            comparison.summary
        );
    }
}

#[cfg(test)]
mod counted {
    use crate::counters::{Counters, since};
    use crate::env::{Env, assert_all, attribute, entity, fact, text};
    use dialog_artifacts::Pick;
    use std::sync::OnceLock;

    /// The counting subscriber, installed once for the test process.
    fn counters() -> &'static Counters {
        static COUNTERS: OnceLock<Counters> = OnceLock::new();
        COUNTERS.get_or_init(Counters::install)
    }

    /// The README counts "commits" among the engine steps a scenario
    /// takes. A branch commit is what every write scenario makes; the
    /// `commit` span sits on the snapshot path, which no scenario
    /// takes, so a branch commit counts none and every baseline
    /// records `span.commit: 0`.
    #[tokio::test(flavor = "current_thread")]
    async fn a_branch_commit_counts_a_commit_step() -> anyhow::Result<()> {
        let counters = counters();
        let env = Env::open().await?;
        let before = counters.snapshot(env.tally());
        env.commit(assert_all(
            [fact(&entity("thing", 0), "stuff/name", text("a"))],
            Pick::All,
        ))
        .await?;
        let moved = since(&before, &counters.snapshot(env.tally()));
        assert!(
            moved.get("span.commit").copied().unwrap_or(0) >= 1,
            "one commit, counted: {moved:?}"
        );
        let _ = attribute("stuff/name");
        Ok(())
    }

    /// "Two runs write the same records": the block gate depends on a
    /// scenario's commits landing in the same tree every run. The same
    /// commits from a fresh repository, run several times, mint the
    /// same head every time. Opening the space records its access, a
    /// fresh account key with its encrypted secret and a delegation,
    /// random by design, in `main`; the harness measures on a branch of
    /// its own, which nothing but the scenario writes.
    #[tokio::test(flavor = "current_thread")]
    async fn the_same_commits_mint_the_same_head() -> anyhow::Result<()> {
        let mut heads = Vec::new();
        for _ in 0..6 {
            let env = Env::open().await?;
            for batch in 0..4 {
                let facts = (0..50).map(|index| {
                    fact(
                        &entity("thing", batch * 50 + index),
                        "stuff/name",
                        text(format!("name {index}")),
                    )
                });
                env.commit(assert_all(facts, Pick::Last)).await?;
            }
            heads.push(env.head());
        }
        heads.dedup();
        assert_eq!(heads.len(), 1, "every run minted one head: {heads:#?}");
        Ok(())
    }
}
