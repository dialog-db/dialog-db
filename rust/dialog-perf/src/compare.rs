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
//!
//! The same comparison runs against two kinds of reference. Against
//! the baseline files ([`Against::Baseline`]) every scenario must have
//! one, so none can drop out of the gate. Against the sweep CI recorded
//! for the commit a change lands on ([`Against::Sweep`]) the scenarios
//! may differ, because the change may add, remove or reshape one; those
//! are reported, and the baseline-file comparison gates them. A change
//! that re-records a scenario's baseline file has accepted that
//! scenario's new cost, so its growth against the sweep is reported as
//! accepted rather than failed. Each comparison yields every row it
//! looked at, not only the regressions, so a change's whole delta can be
//! shown ([`Comparison::markdown`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context as _, Result};

use crate::report::Report;
use crate::runner::Runner;
use crate::scenario::{catalog, spec};

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

/// What the new sweep is compared against.
#[derive(Debug, Clone)]
pub enum Against {
    /// Checked-in baseline files: every scenario must have one.
    Baseline,
    /// The sweep of the commit the change lands on. Scenarios present on
    /// one side only, or whose workload changed, are reported and not
    /// gated; growth in a scenario named in `rebaselined` (one whose
    /// baseline file the change re-records) is accepted, not failed.
    Sweep {
        /// Scenarios whose baseline file the change re-records.
        rebaselined: BTreeSet<String>,
    },
}

/// How one row of a comparison came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Unchanged, or moved within the allowance.
    Within,
    /// Grew past the allowance: the gate fails.
    Regressed,
    /// Shrank past the allowance.
    Improved,
    /// Grew past the allowance in a scenario whose baseline file the
    /// change re-records.
    Accepted,
    /// Not gated: volatile instructions, or a scenario present on one
    /// side only or whose workload changed.
    Ungated,
}

impl Verdict {
    fn marker(self) -> &'static str {
        match self {
            Verdict::Within => "",
            Verdict::Regressed => "❌ past tolerance",
            Verdict::Improved => "✅ improved past tolerance",
            Verdict::Accepted => "⚠️ past tolerance, re-baselined",
            Verdict::Ungated => "not gated",
        }
    }
}

/// One measured quantity of one scenario, on both sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The scenario.
    pub scenario: String,
    /// `instructions` or a counter name.
    pub metric: String,
    /// The reference's value; `None` when it has none.
    pub base: Option<u64>,
    /// The new sweep's value; `None` when it has none.
    pub new: Option<u64>,
    /// How the row came out.
    pub verdict: Verdict,
}

impl Row {
    /// Whether the value differs between the two sides.
    pub fn moved(&self) -> bool {
        self.base != self.new
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
    /// Growth past the allowance in re-baselined scenarios, compared
    /// against a sweep: shown, not failed.
    pub accepted: Vec<String>,
    /// Scenario-level differences that are not rows: a scenario on one
    /// side only, or one whose workload changed.
    pub notes: Vec<String>,
    /// Every quantity compared, in scenario order.
    pub rows: Vec<Row>,
    /// The CPU each side's sweep ran on, where it recorded one.
    pub runners: (Option<Runner>, Option<Runner>),
    /// Whether this compares against a sweep that ran on another CPU
    /// model or feature set, or one either side did not record. Each
    /// scenario's instructions are then allowed its measured cross-CPU
    /// spread ([`Spec::cross_cpu_pct`](crate::Spec::cross_cpu_pct)) on top
    /// of the tolerance.
    pub cross_cpu: bool,
}

impl Comparison {
    /// Whether the gate passes (no regressions).
    pub fn passed(&self) -> bool {
        self.regressions.is_empty()
    }

    /// Against the baseline files, the rows that beat their file by more
    /// than the allowance, as a Markdown warning; `None` when there are
    /// none. An improvement that lands without re-recording the file
    /// leaves room under it that a later regression can fill unseen (#625
    /// left commit-derived 20% under its file), so it is flagged, never
    /// failed: improvements must not block a change.
    pub fn stale_markdown(&self) -> Option<String> {
        if self.improvements.is_empty() {
            return None;
        }
        let mut out = format!(
            "⚠ **Stale baseline, re-record:** {} row(s) beat their baseline file by more than \
             the allowance. Re-record the file in the change that explains the move, so the \
             gate holds the new level.\n\n",
            self.improvements.len()
        );
        for line in &self.improvements {
            let _ = writeln!(out, "- {line}");
        }
        Some(out)
    }

    /// The comparison as one Markdown table: the rows that moved, then
    /// the rows that did not, folded into a details block.
    pub fn markdown(&self) -> String {
        let mut out = String::new();
        let past = |verdict| self.rows.iter().filter(|r| r.verdict == verdict).count();
        let _ = writeln!(
            out,
            "{} rows: {} past tolerance, {} improved past tolerance, {} re-baselined.\n",
            self.rows.len(),
            past(Verdict::Regressed),
            past(Verdict::Improved),
            past(Verdict::Accepted),
        );
        let runner = |runner: &Option<Runner>| {
            runner
                .as_ref()
                .map_or_else(|| "a CPU it did not record".to_string(), Runner::to_string)
        };
        let _ = writeln!(
            out,
            "Base swept on {}; this sweep on {}.\n",
            runner(&self.runners.0),
            runner(&self.runners.1),
        );
        if self.cross_cpu {
            let allowances: Vec<String> = catalog()
                .iter()
                .filter(|spec| spec.cross_cpu_pct > 0.0)
                .map(|spec| format!("{} +{}%", spec.name, spec.cross_cpu_pct))
                .collect();
            let _ = writeln!(
                out,
                "Different or unrecorded CPUs: instructions are allowed each scenario's \
                 cross-CPU spread on top of the tolerance ({}). The allowances are rough, \
                 from eight identical sweeps.\n",
                allowances.join(", ")
            );
        } else {
            out.push_str("Same CPU model and flags: instructions are held to the tolerance.\n\n");
        }
        for note in &self.notes {
            let _ = writeln!(out, "- {note}");
        }
        if !self.notes.is_empty() {
            out.push('\n');
        }
        let (moved, still): (Vec<&Row>, Vec<&Row>) = self.rows.iter().partition(|r| r.moved());
        if moved.is_empty() {
            out.push_str("Nothing moved.\n");
        } else {
            table(&mut out, &moved);
        }
        if !still.is_empty() {
            let _ = writeln!(
                out,
                "\n<details><summary>{} rows unchanged</summary>\n",
                still.len()
            );
            table(&mut out, &still);
            out.push_str("\n</details>\n");
        }
        out
    }
}

fn table(out: &mut String, rows: &[&Row]) {
    out.push_str("| scenario | metric | base | new | Δ | Δ% | |\n");
    out.push_str("|---|---|--:|--:|--:|--:|---|\n");
    for row in rows {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} |",
            row.scenario,
            row.metric,
            count(row.base),
            count(row.new),
            delta(row.base, row.new),
            percent(row.base, row.new),
            row.verdict.marker(),
        );
    }
}

/// A count with thousands separated, or a dash for none.
fn count(value: Option<u64>) -> String {
    let Some(value) = value else {
        return "–".to_string();
    };
    let digits = value.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

fn delta(base: Option<u64>, new: Option<u64>) -> String {
    match (base, new) {
        (Some(base), Some(new)) if new >= base => format!("+{}", count(Some(new - base))),
        (Some(base), Some(new)) => format!("-{}", count(Some(base - new))),
        _ => "–".to_string(),
    }
}

fn percent(base: Option<u64>, new: Option<u64>) -> String {
    match (base, new) {
        (Some(base), Some(new)) if base == new => "0%".to_string(),
        (Some(0), Some(_)) => "new".to_string(),
        (Some(base), Some(new)) => {
            format!("{:+.2}%", (new as f64 - base as f64) / base as f64 * 100.0)
        }
        _ => "–".to_string(),
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

/// Compare the scenarios of two sweep directories under `tolerance`,
/// the first being what `against` says it is.
pub fn compare_dirs(
    baseline: &Path,
    new: &Path,
    tolerance: &Tolerance,
    against: &Against,
) -> Result<Comparison> {
    let (baseline_dir, new_dir) = (baseline, new);
    let baseline = load(baseline)?;
    let new = load(new)?;
    anyhow::ensure!(
        baseline.keys().any(|name| new.contains_key(name)),
        "no overlapping reports to compare"
    );

    let runners = (Runner::read(baseline_dir), Runner::read(new_dir));
    let same_cpu = matches!(&runners, (Some(base), Some(new)) if base == new);
    let mut comparison = Comparison {
        cross_cpu: matches!(against, Against::Sweep { .. }) && !same_cpu,
        runners,
        ..Comparison::default()
    };
    let names: BTreeSet<&String> = baseline.keys().chain(new.keys()).collect();
    for name in names {
        match (baseline.get(name), new.get(name), against) {
            (Some(base), Some(new), _) => {
                compare_reports(base, new, tolerance, against, &mut comparison)
            }
            // A scenario the sweep ran with no baseline is ungated, and
            // a baseline no scenario answers gates nothing: both fail,
            // so a scenario cannot drop out of the gate by being renamed
            // or by its baseline being deleted.
            (None, Some(_), Against::Baseline) => comparison.regressions.push(format!(
                "{name}: no baseline (sweep into the baseline directory to add one)"
            )),
            (Some(_), None, Against::Baseline) => comparison.regressions.push(format!(
                "{name}: a baseline no scenario in the sweep answers"
            )),
            (None, Some(report), Against::Sweep { .. }) => {
                comparison
                    .notes
                    .push(format!("`{name}`: new, not in the base sweep"));
                one_sided(&mut comparison, report, false);
            }
            (Some(report), None, Against::Sweep { .. }) => {
                comparison
                    .notes
                    .push(format!("`{name}`: in the base sweep, not in this one"));
                one_sided(&mut comparison, report, true);
            }
            (None, None, _) => unreachable!("a name comes from one side or the other"),
        }
    }
    Ok(comparison)
}

/// The rows of a scenario only one side ran, ungated.
fn one_sided(out: &mut Comparison, report: &Report, base_side: bool) {
    let values = report
        .instructions
        .map(|value| ("instructions".to_string(), value))
        .into_iter()
        .chain(report.counters.iter().map(|(k, v)| (k.clone(), *v)));
    for (metric, value) in values {
        let (base, new) = if base_side {
            (Some(value), None)
        } else {
            (None, Some(value))
        };
        out.rows.push(Row {
            scenario: report.scenario.clone(),
            metric,
            base,
            new,
            verdict: Verdict::Ungated,
        });
    }
}

/// One scenario's rows: how each is judged and where its verdict goes.
struct Gate<'a> {
    scenario: &'a str,
    /// Whether the two sides ran the same workload.
    comparable: bool,
    /// Whether the change re-records this scenario's baseline file.
    rebaselined: bool,
}

impl Gate<'_> {
    /// Record a row and route its verdict: growth past the allowance is
    /// a regression, or accepted in a re-baselined scenario.
    fn check(
        &self,
        out: &mut Comparison,
        metric: &str,
        before: u64,
        after: u64,
        slack: u64,
        pct: f64,
    ) {
        let name = self.scenario;
        let verdict = if !self.comparable {
            Verdict::Ungated
        } else if regressed(before, after, slack, pct) {
            let line = format!("{name}: {metric} {}", change(before, after));
            if self.rebaselined {
                out.accepted.push(line);
                Verdict::Accepted
            } else {
                out.regressions.push(line);
                Verdict::Regressed
            }
        } else if regressed(after, before, slack, pct) {
            out.improvements
                .push(format!("{name}: {metric} {}", change(before, after)));
            Verdict::Improved
        } else {
            Verdict::Within
        };
        self.push(out, metric, before, after, verdict);
    }

    fn push(&self, out: &mut Comparison, metric: &str, before: u64, after: u64, verdict: Verdict) {
        out.rows.push(Row {
            scenario: self.scenario.to_string(),
            metric: metric.to_string(),
            base: Some(before),
            new: Some(after),
            verdict,
        });
    }
}

fn compare_reports(
    base: &Report,
    new: &Report,
    tolerance: &Tolerance,
    against: &Against,
    out: &mut Comparison,
) {
    let name = &base.scenario;
    let sweep = matches!(against, Against::Sweep { .. });
    let rebaselined = match against {
        Against::Sweep { rebaselined } => rebaselined.contains(name),
        Against::Baseline => false,
    };

    // Against a sweep, a scenario run at another size or answering
    // differently is a workload the change reshaped: its counts are not
    // comparable, and the baseline file the change re-records gates it.
    let mut comparable = true;
    if base.size != new.size || base.profile != new.profile {
        let line = format!(
            "{name}: compared at size {} / {} against a baseline at size {} / {}",
            new.size, new.profile, base.size, base.profile
        );
        if !sweep {
            out.regressions.push(line);
            return;
        }
        out.notes.push(format!(
            "`{line}`: workload changed, not gated against the base"
        ));
        comparable = false;
    }
    let outcomes: BTreeSet<&String> = base.outcome.keys().chain(new.outcome.keys()).collect();
    for key in outcomes {
        let expected = base.outcome.get(key).copied().unwrap_or(0);
        let actual = new.outcome.get(key).copied().unwrap_or(0);
        if actual == expected {
            continue;
        }
        if sweep {
            out.notes.push(format!(
                "`{name}`: outcome {key} {expected} -> {actual}, workload changed, not gated against the base"
            ));
            comparable = false;
        } else if base.outcome.contains_key(key) {
            out.regressions.push(format!(
                "{name}: outcome {key} {expected} -> {actual}: the workload answers differently"
            ));
        }
    }

    let gate = Gate {
        scenario: name,
        comparable,
        rebaselined,
    };

    let instructions = match (base.instructions, new.instructions) {
        (Some(before), Some(after)) if base.volatile || new.volatile => {
            gate.push(out, "instructions", before, after, Verdict::Ungated);
            format!(
                "instructions {} (volatile, not gated)",
                change(before, after)
            )
        }
        (Some(before), Some(after)) => {
            let cross_cpu_pct = if out.cross_cpu {
                spec(name).map_or(0.0, |spec| spec.cross_cpu_pct)
            } else {
                0.0
            };
            gate.check(
                out,
                "instructions",
                before,
                after,
                tolerance.instructions_slack,
                tolerance.instructions_pct + cross_cpu_pct,
            );
            format!("instructions {}", change(before, after))
        }
        (Some(before), None) => {
            if !sweep {
                out.regressions.push(format!(
                    "{name}: instructions not measured (baseline {before}); sweep under callgrind"
                ));
            }
            "instructions not measured".to_string()
        }
        (None, Some(after)) => format!("instructions {after} (no baseline)"),
        (None, None) => "no instruction counts".to_string(),
    };

    // Every counter either report has: a step the baseline never took
    // and the new run takes counts from zero.
    let keys: BTreeSet<&String> = base.counters.keys().chain(new.counters.keys()).collect();
    let mut moved = Vec::new();
    for key in keys {
        let before = base.counters.get(key).copied().unwrap_or(0);
        let after = new.counters.get(key).copied().unwrap_or(0);
        if before != after {
            moved.push(format!("{key} {}", change(before, after)));
        }
        let (slack, pct) = if key.ends_with(".bytes") {
            (tolerance.bytes_slack, tolerance.bytes_pct)
        } else if key.starts_with("archive.") {
            (tolerance.block_slack, 0.0)
        } else {
            (tolerance.step_slack, 0.0)
        };
        gate.check(out, key, before, after, slack, pct);
    }

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
            &Against::Baseline,
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
            &Against::Baseline,
            &mut comparison,
        );
        assert!(
            !comparison.passed(),
            "two blocks written where the baseline wrote none: {:?}",
            comparison.summary
        );
    }

    fn sweep(rebaselined: &[&str]) -> Against {
        Against::Sweep {
            rebaselined: rebaselined.iter().map(|name| name.to_string()).collect(),
        }
    }

    fn measured(instructions: u64, counters: &[(&str, u64)]) -> Report {
        Report {
            instructions: Some(instructions),
            ..report(counters)
        }
    }

    /// The motivating case: the baseline file sits above where the base
    /// commit measures, so growth that lands back under the file passes
    /// it, and the same growth against the base sweep fails.
    #[test]
    fn growth_hidden_under_the_file_fails_against_the_base() {
        let file = measured(100_000_000, &[]);
        let base = measured(80_000_000, &[]);
        let new = measured(90_000_000, &[]);
        let tolerance = Tolerance::default();

        let mut against_file = Comparison::default();
        compare_reports(
            &file,
            &new,
            &tolerance,
            &Against::Baseline,
            &mut against_file,
        );
        assert!(against_file.passed(), "{:?}", against_file.regressions);

        let mut against_base = Comparison::default();
        compare_reports(&base, &new, &tolerance, &sweep(&[]), &mut against_base);
        assert!(!against_base.passed(), "+12.5% against the base");
        assert_eq!(against_base.rows[0].verdict, Verdict::Regressed);
    }

    /// Against a sweep from another CPU, a scenario's instructions get
    /// its measured cross-CPU spread on top of the tolerance; on the same
    /// CPU, none. rule-point's 2.4% lets +4% through across CPUs only.
    #[test]
    fn a_sweep_from_another_cpu_widens_instructions_by_the_scenario() {
        let mut base = measured(48_000_000, &[]);
        base.scenario = "rule-point".into();
        let mut new = measured(49_920_000, &[]);
        new.scenario = "rule-point".into();

        let mut same = Comparison::default();
        compare_reports(&base, &new, &Tolerance::default(), &sweep(&[]), &mut same);
        assert!(!same.passed(), "+4% on one CPU fails");

        let mut cross = Comparison {
            cross_cpu: true,
            ..Comparison::default()
        };
        compare_reports(&base, &new, &Tolerance::default(), &sweep(&[]), &mut cross);
        assert!(cross.passed(), "{:?}", cross.regressions);
        assert!(cross.markdown().contains("rule-point +2.4%"));
    }

    /// A row that beats its file past the allowance is a stale baseline:
    /// flagged in the warning, not failed.
    #[test]
    fn an_improvement_past_the_allowance_flags_a_stale_baseline() {
        let file = measured(150_000_000, &[]);
        let new = measured(116_000_000, &[]);
        let mut comparison = Comparison::default();
        compare_reports(
            &file,
            &new,
            &Tolerance::default(),
            &Against::Baseline,
            &mut comparison,
        );
        assert!(comparison.passed());
        let stale = comparison.stale_markdown().expect("a stale baseline");
        assert!(stale.contains("Stale baseline, re-record"), "{stale}");
        assert!(
            stale.contains("- s: instructions 150000000 -> 116000000"),
            "{stale}"
        );

        let mut comparison = Comparison::default();
        compare_reports(
            &new,
            &new,
            &Tolerance::default(),
            &Against::Baseline,
            &mut comparison,
        );
        assert_eq!(comparison.stale_markdown(), None);
    }

    /// Re-recording a scenario's baseline file accepts its new cost: the
    /// growth against the base is a row marked accepted, not a failure.
    #[test]
    fn a_rebaselined_scenario_is_accepted_against_the_base() {
        let base = measured(80_000_000, &[]);
        let new = measured(90_000_000, &[]);
        let mut comparison = Comparison::default();
        compare_reports(
            &base,
            &new,
            &Tolerance::default(),
            &sweep(&["s"]),
            &mut comparison,
        );
        assert!(comparison.passed(), "{:?}", comparison.regressions);
        assert_eq!(comparison.rows[0].verdict, Verdict::Accepted);
        assert_eq!(comparison.accepted.len(), 1);
    }

    /// Against the base, a workload that answers differently is a
    /// reshaped scenario: reported, its rows ungated. Against the file it
    /// fails, as before.
    #[test]
    fn a_reshaped_workload_is_ungated_against_the_base_only() {
        let base = measured(80_000_000, &[("span.settle", 1)]);
        let mut new = measured(90_000_000, &[("span.settle", 5)]);
        new.outcome.insert("facts".into(), 10);

        let mut comparison = Comparison::default();
        compare_reports(
            &base,
            &new,
            &Tolerance::default(),
            &sweep(&[]),
            &mut comparison,
        );
        assert!(comparison.passed(), "{:?}", comparison.regressions);
        assert!(
            comparison
                .rows
                .iter()
                .all(|row| row.verdict == Verdict::Ungated)
        );
        assert_eq!(comparison.notes.len(), 1);

        let mut comparison = Comparison::default();
        let mut file = base.clone();
        file.outcome.insert("facts".into(), 1);
        compare_reports(
            &file,
            &new,
            &Tolerance::default(),
            &Against::Baseline,
            &mut comparison,
        );
        assert!(!comparison.passed());
    }

    /// Every quantity becomes a row, moved or not, and the table shows
    /// the moved rows first with the rest folded away.
    #[test]
    fn every_quantity_is_a_row_and_the_unmoved_fold() {
        let base = measured(10_000_000, &[("archive.put", 4), ("span.settle", 1)]);
        let new = measured(10_100_000, &[("archive.put", 4), ("span.settle", 1)]);
        let mut comparison = Comparison::default();
        compare_reports(
            &base,
            &new,
            &Tolerance::default(),
            &sweep(&[]),
            &mut comparison,
        );
        let metrics: Vec<&str> = comparison
            .rows
            .iter()
            .map(|row| row.metric.as_str())
            .collect();
        assert_eq!(metrics, ["instructions", "archive.put", "span.settle"]);

        let markdown = comparison.markdown();
        let (moved, folded) = markdown.split_once("<details>").expect("a fold");
        assert!(
            moved.contains("| s | instructions | 10,000,000 | 10,100,000 | +100,000 | +1.00% |  |"),
            "{markdown}"
        );
        assert!(folded.contains("2 rows unchanged"), "{markdown}");
        assert!(
            folded.contains("| s | span.settle | 1 | 1 | +0 | 0% |  |"),
            "{markdown}"
        );
    }

    /// A scenario only one side ran is a note and ungated rows against
    /// the base, and a failure against the files.
    #[test]
    fn a_scenario_on_one_side_fails_only_against_the_files() -> Result<()> {
        let dir = std::env::temp_dir().join(format!("dialog-perf-compare-{}", std::process::id()));
        let (base_dir, new_dir) = (dir.join("base"), dir.join("new"));
        fs::create_dir_all(&base_dir)?;
        fs::create_dir_all(&new_dir)?;
        let write = |dir: &Path, name: &str| -> Result<()> {
            let report = Report {
                scenario: name.into(),
                ..measured(1_000_000, &[("span.settle", 1)])
            };
            fs::write(
                dir.join(format!("{name}.json")),
                serde_json::to_string(&report)?,
            )?;
            Ok(())
        };
        write(&base_dir, "kept")?;
        write(&new_dir, "kept")?;
        write(&new_dir, "added")?;

        let tolerance = Tolerance::default();
        let against_base = compare_dirs(&base_dir, &new_dir, &tolerance, &sweep(&[]))?;
        let against_file = compare_dirs(&base_dir, &new_dir, &tolerance, &Against::Baseline)?;
        fs::remove_dir_all(&dir)?;

        assert!(against_base.passed(), "{:?}", against_base.regressions);
        assert_eq!(against_base.notes.len(), 1);
        assert!(against_base.rows.iter().any(|row| row.scenario == "added"
            && row.base.is_none()
            && row.verdict == Verdict::Ungated));
        assert!(!against_file.passed());
        Ok(())
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
        let before = counters.snapshot(&env);
        env.commit(assert_all(
            [fact(&entity("thing", 0), "stuff/name", text("a"))],
            Pick::All,
        ))
        .await?;
        let moved = since(&before, &counters.snapshot(&env));
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
