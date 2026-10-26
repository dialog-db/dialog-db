//! Compare two soak sweep directories and flag regressions.
//!
//! Two noise sources shape the gate. A run's identities decide its tree's
//! leaf seams, and with them which phase pays for which shared leaf; the
//! harness derives them all from one seed (`DIALOG_SOAK_SEED`), so two
//! runs of one seed write the same tree, and runs of different seeds
//! wobble a block or two between phases. And on *shaped* (delayed)
//! profiles the latency itself moves request counts nondeterministically:
//! fetch windows overlap differently run to run. The `none` profile has no
//! delays, so under the baseline's own seed its counts are exact protocol
//! shape.
//!
//! The gate therefore holds a `none` report run with the baseline's seed
//! tight (threshold, slack 2, in every phase: a new round trip on any
//! phase is a protocol regression) and the shaped reports loose (2.5x
//! threshold on requests, 2x on bytes and modeled time, over lazy-join and
//! download totals). Costs migrating between phases cancel in the totals;
//! real growth does not. Duplicate fetches are gated per phase with the
//! same slack as requests: the baselines carry zero, and any recurrence is
//! replication waste. So are the exact round-trip chain and the blob
//! opens, each per phase: a longer chain is a new dependent request on the
//! critical path, which no total can be allowed to hide, and a blob count
//! that moves is a layout change that moved bytes between blocks and
//! blobs. A baseline recorded before the chain was counted (`round_trips`
//! absent) gates only what it carries.
//!
//! A `none` report run with another seed than its baseline's (the nightly
//! draws a fresh one per run) is not exact: its seams sit elsewhere. It is
//! gated as a shaped report is, on its totals and its duplicate fetches,
//! not phase by phase. A baseline recorded before runs were seeded (`seed`
//! absent) is held tight as before.
//!
//! The same comparison runs against two kinds of reference: the
//! checked-in baseline files ([`Against::Baseline`]) and the sweep CI
//! recorded for the commit a change lands on ([`Against::Sweep`]).
//! Against a sweep, a report on one side only or run over a different
//! seed is reported and not gated (the baseline files gate it), and a
//! report whose baseline file the change re-records has accepted its new
//! cost, so its growth is reported as accepted rather than failed. Each
//! comparison yields every quantity it looked at as a row, gated or not,
//! so a change's whole delta can be shown ([`Comparison::markdown`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context as _, Result};

use crate::report::{PhaseReport, Report};

/// What the new sweep is compared against.
#[derive(Debug, Clone)]
pub enum Against {
    /// Checked-in baseline files. Reports present on one side only are
    /// skipped: the files hold shaped profiles the per-PR gate does not
    /// sweep.
    Baseline,
    /// The sweep of the commit the change lands on.
    Sweep {
        /// Reports (file stems, `join-none`) whose baseline file the
        /// change re-records.
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
    /// Grew past the allowance in a report whose baseline file the change
    /// re-records.
    Accepted,
    /// Shown for context, with no allowance of its own: a phase's bytes
    /// and modeled time are gated through the totals.
    Reported,
    /// Not gated: a report or phase on one side only, or a report run
    /// over a different seed.
    Ungated,
}

impl Verdict {
    fn marker(self) -> &'static str {
        match self {
            Verdict::Within | Verdict::Reported => "",
            Verdict::Regressed => "❌ past tolerance",
            Verdict::Improved => "✅ improved past tolerance",
            Verdict::Accepted => "⚠️ past tolerance, re-baselined",
            Verdict::Ungated => "not gated",
        }
    }
}

/// One measured quantity of one phase (or total) of one report, on both
/// sides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The report's file stem (`join-none`): scenario and profile.
    pub report: String,
    /// A phase name, or `lazy-join total` / `download total`.
    pub phase: String,
    /// The quantity.
    pub metric: &'static str,
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
    /// One human-readable line per shared config: lazy-join and download
    /// modeled times, baseline against new.
    pub summary: Vec<String>,
    /// One line per detected regression; empty means the gate passes.
    pub regressions: Vec<String>,
    /// Growth past the allowance in re-baselined reports, compared
    /// against a sweep: shown, not failed.
    pub accepted: Vec<String>,
    /// Report-level differences that are not rows.
    pub notes: Vec<String>,
    /// Every quantity compared, in report and phase order.
    pub rows: Vec<Row>,
}

impl Comparison {
    /// Whether the gate passes (no regressions).
    pub fn passed(&self) -> bool {
        self.regressions.is_empty()
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
    out.push_str("| report | phase | metric | base | new | Δ | Δ% | |\n");
    out.push_str("|---|---|---|--:|--:|--:|--:|---|\n");
    for row in rows {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            row.report,
            row.phase,
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

/// Totals over one report's phases, split into the lazy join (every phase
/// but `download`) and the download.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Totals {
    requests: u64,
    bytes: u64,
    virtual_ms: u64,
}

fn totals(report: &Report, download: bool) -> Totals {
    report
        .phases
        .iter()
        .filter(|phase| (phase.name == "download") == download)
        .fold(Totals::default(), |mut totals, phase| {
            totals.requests += phase.traffic.requests;
            totals.bytes += phase.traffic.bytes;
            totals.virtual_ms += phase.virtual_ms;
            totals
        })
}

/// Whether `new` regressed against `base`: growth beyond both an absolute
/// `slack` and a relative `threshold_pct`.
fn regressed(base: u64, new: u64, slack: u64, threshold_pct: f64) -> bool {
    if new.saturating_sub(base) <= slack {
        return false;
    }
    if base == 0 {
        return true;
    }
    (new - base) as f64 / base as f64 * 100.0 > threshold_pct
}

/// Load every `*.json` report in `dir`, keyed by file stem.
fn load(dir: &Path) -> Result<BTreeMap<String, Report>> {
    let mut reports = BTreeMap::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            let name = path
                .file_stem()
                .expect("a .json path has a file name")
                .to_string_lossy()
                .into_owned();
            let text =
                fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
            let report: Report = serde_json::from_str(&text)
                .with_context(|| format!("parsing {}", path.display()))?;
            reports.insert(name, report);
        }
    }
    Ok(reports)
}

/// Compare the reports of two sweep directories under a relative growth
/// `threshold_pct`, the first being what `against` says it is.
pub fn compare_dirs(
    baseline: &Path,
    new: &Path,
    threshold_pct: f64,
    against: &Against,
) -> Result<Comparison> {
    let baseline = load(baseline)?;
    let new = load(new)?;
    anyhow::ensure!(
        baseline.keys().any(|name| new.contains_key(name)),
        "no overlapping reports to compare"
    );

    let mut comparison = Comparison::default();
    let names: BTreeSet<&String> = baseline.keys().chain(new.keys()).collect();
    for name in names {
        match (baseline.get(name), new.get(name), against) {
            (Some(base_report), Some(new_report), _) => {
                let rebaselined = match against {
                    Against::Sweep { rebaselined } => rebaselined.contains(name),
                    Against::Baseline => false,
                };
                let comparable = match against {
                    Against::Sweep { .. } => same_seed(base_report, new_report),
                    Against::Baseline => true,
                };
                if !comparable {
                    comparison.notes.push(format!(
                        "`{name}`: seeded or shaped differently from the base, not gated against it"
                    ));
                }
                compare_reports(
                    &mut comparison,
                    &Gate {
                        report: name,
                        comparable,
                        rebaselined,
                    },
                    base_report,
                    new_report,
                    threshold_pct,
                );
                comparison.summary.push(format!(
                    "{name}: lazy join {} ms -> {} ms | download {} ms -> {} ms",
                    totals(base_report, false).virtual_ms,
                    totals(new_report, false).virtual_ms,
                    totals(base_report, true).virtual_ms,
                    totals(new_report, true).virtual_ms,
                ));
            }
            (None, Some(report), Against::Sweep { .. }) => {
                comparison
                    .notes
                    .push(format!("`{name}`: new, not in the base sweep"));
                one_sided(&mut comparison, name, report, false);
            }
            (Some(report), None, Against::Sweep { .. }) => {
                comparison
                    .notes
                    .push(format!("`{name}`: in the base sweep, not in this one"));
                one_sided(&mut comparison, name, report, true);
            }
            (_, _, Against::Baseline) | (None, None, _) => {}
        }
    }
    Ok(comparison)
}

/// Whether two reports ran the same scenario over the same link and seed,
/// so their counts compare.
fn same_seed(base: &Report, new: &Report) -> bool {
    base.scenario == new.scenario
        && base.network == new.network
        && base.latency_ms == new.latency_ms
        && base.auth_ms == new.auth_ms
        && base.bandwidth_mbps == new.bandwidth_mbps
        && base.connections == new.connections
        && base.fanout_n == new.fanout_n
        && base.max_segment == new.max_segment
        && base.entities == new.entities
        && base.facts == new.facts
        && base.commits == new.commits
        && base.seed == new.seed
}

/// The quantities a phase shows, each with whether it is gated per phase.
fn quantities(phase: &PhaseReport) -> [(&'static str, Option<u64>, bool); 7] {
    [
        ("requests", Some(phase.traffic.requests), true),
        ("round trips", phase.round_trips, true),
        ("blob requests", Some(phase.blob_requests), true),
        ("duplicate fetches", Some(phase.duplicate_requests), true),
        ("bytes", Some(phase.traffic.bytes), false),
        ("blob bytes", Some(phase.blob_bytes), false),
        ("modeled ms", Some(phase.virtual_ms), false),
    ]
}

/// The rows of a report only one side ran, ungated.
fn one_sided(out: &mut Comparison, name: &str, report: &Report, base_side: bool) {
    for phase in &report.phases {
        for (metric, value, _) in quantities(phase) {
            if value.is_none() {
                continue;
            }
            let (base, new) = if base_side {
                (value, None)
            } else {
                (None, value)
            };
            out.rows.push(Row {
                report: name.to_string(),
                phase: phase.name.clone(),
                metric,
                base,
                new,
                verdict: Verdict::Ungated,
            });
        }
    }
}

/// One report's rows: how each is judged and where its verdict goes.
struct Gate<'a> {
    report: &'a str,
    /// Whether the two sides ran the same seed over the same link.
    comparable: bool,
    /// Whether the change re-records this report's baseline file.
    rebaselined: bool,
}

impl Gate<'_> {
    /// Record a row and route its verdict: growth past the allowance is a
    /// regression, or accepted in a re-baselined report. `None` for a
    /// tolerance records the row ungated by its own allowance.
    #[allow(clippy::too_many_arguments)]
    fn check(
        &self,
        out: &mut Comparison,
        phase: &str,
        metric: &'static str,
        base: Option<u64>,
        new: Option<u64>,
        tolerance: Option<(u64, f64)>,
        line: impl FnOnce(u64, u64) -> String,
    ) {
        if base.is_none() && new.is_none() {
            return;
        }
        let verdict = match (base, new, tolerance) {
            _ if !self.comparable => Verdict::Ungated,
            (Some(before), Some(after), Some((slack, pct))) => {
                if regressed(before, after, slack, pct) {
                    let line = format!("{}:{phase}: {}", self.report, line(before, after));
                    if self.rebaselined {
                        out.accepted.push(line);
                        Verdict::Accepted
                    } else {
                        out.regressions.push(line);
                        Verdict::Regressed
                    }
                } else if regressed(after, before, slack, pct) {
                    Verdict::Improved
                } else {
                    Verdict::Within
                }
            }
            (Some(_), Some(_), None) => Verdict::Reported,
            _ => Verdict::Ungated,
        };
        out.rows.push(Row {
            report: self.report.to_string(),
            phase: phase.to_string(),
            metric,
            base,
            new,
            verdict,
        });
    }
}

fn compare_reports(
    out: &mut Comparison,
    gate: &Gate<'_>,
    base: &Report,
    new: &Report,
    threshold_pct: f64,
) {
    // Exact: the unshaped profile, run with the seed its baseline was
    // recorded with (or either predates seeding).
    let exact = new.network == "none"
        && match (base.seed, new.seed) {
            (Some(base_seed), Some(new_seed)) => base_seed == new_seed,
            _ => true,
        };
    let loose = !exact;
    let request_slack = if loose { 3 } else { 2 };
    let request_threshold = threshold_pct * if loose { 2.5 } else { 1.0 };
    let request_tolerance = (request_slack, request_threshold);
    // An unshaped run of another seed moved its seams: a block moves
    // between phases, which only the totals see through. Its duplicate
    // fetches are still gated per phase.
    let per_phase = |metric: &str| new.network != "none" || exact || metric == "duplicate fetches";

    let base_phases: BTreeMap<_, _> = base.phases.iter().map(|p| (p.name.as_str(), p)).collect();
    let new_names: BTreeSet<&str> = new.phases.iter().map(|p| p.name.as_str()).collect();
    for new_phase in &new.phases {
        let base_phase = base_phases.get(new_phase.name.as_str());
        let base_values = base_phase.map(|phase| quantities(phase));
        for (index, (metric, after, gated)) in quantities(new_phase).into_iter().enumerate() {
            let before = base_values.and_then(|values| values[index].1);
            gate.check(
                out,
                &new_phase.name,
                metric,
                before,
                after,
                (gated && per_phase(metric)).then_some(request_tolerance),
                |before, after| format!("{metric} {before} -> {after}"),
            );
        }
    }
    for base_phase in base
        .phases
        .iter()
        .filter(|p| !new_names.contains(p.name.as_str()))
    {
        for (metric, before, _) in quantities(base_phase) {
            gate.check(out, &base_phase.name, metric, before, None, None, |_, _| {
                String::new()
            });
        }
    }

    for (scope, download) in [("lazy-join", false), ("download", true)] {
        let base_total = totals(base, download);
        let new_total = totals(new, download);
        let loose = threshold_pct * if loose { 2.0 } else { 1.0 };
        let checks = [
            (
                "requests",
                base_total.requests,
                new_total.requests,
                request_tolerance,
            ),
            (
                "bytes",
                base_total.bytes,
                new_total.bytes,
                (64 * 1024, loose),
            ),
            (
                "virtual_ms",
                base_total.virtual_ms,
                new_total.virtual_ms,
                (100, loose),
            ),
        ];
        for (metric, base_value, new_value, tolerance) in checks {
            gate.check(
                out,
                &format!("{scope} total"),
                metric,
                Some(base_value),
                Some(new_value),
                Some(tolerance),
                |before, after| {
                    let growth = if before > 0 {
                        format!("+{:.1}%", (after - before) as f64 / before as f64 * 100.0)
                    } else {
                        "+inf".to_string()
                    };
                    format!("{metric} {before} -> {after} ({growth})")
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::TallyRows;

    /// The regression lines of one report against its baseline file.
    fn compare_reports(
        _name: &str,
        base: &Report,
        new: &Report,
        threshold_pct: f64,
    ) -> Vec<String> {
        let mut out = Comparison::default();
        let gate = Gate {
            report: "join-none",
            comparable: true,
            rebaselined: false,
        };
        super::compare_reports(&mut out, &gate, base, new, threshold_pct);
        out.regressions
    }

    fn phase(name: &str, requests: u64, bytes: u64, virtual_ms: u64) -> PhaseReport {
        PhaseReport {
            name: name.into(),
            virtual_ms,
            rounds: 0.0,
            round_trips: Some(requests),
            blob_requests: 0,
            blob_bytes: 0,
            unique_blocks: requests,
            duplicate_requests: 0,
            duplicate_bytes: 0,
            empty_requests: 0,
            traffic: TallyRows {
                rows: vec![("archive.get".into(), requests, bytes)],
                requests,
                bytes,
            },
        }
    }

    fn report(network: &str, phases: Vec<PhaseReport>) -> Report {
        Report {
            scenario: "join".into(),
            network: network.into(),
            latency_ms: 0.0,
            auth_ms: 0.0,
            bandwidth_mbps: 0.0,
            connections: None,
            fanout_n: 8,
            max_segment: 65536,
            entities: 100,
            facts: 600,
            commits: 4,
            vault_files: 10,
            vault_bytes: 1000,
            seed: Some(7),
            seeded_root: None,
            phases,
        }
    }

    /// Growth within the absolute slack never regresses; growth past both
    /// the slack and the percentage does; a zero baseline regresses on any
    /// beyond-slack growth.
    #[test]
    fn it_applies_slack_then_threshold() {
        assert!(!regressed(100, 102, 2, 10.0));
        assert!(!regressed(100, 109, 2, 10.0));
        assert!(regressed(100, 120, 2, 10.0));
        assert!(!regressed(0, 2, 2, 10.0));
        assert!(regressed(0, 3, 2, 10.0));
    }

    /// The unshaped profile is gated tight: a modest request growth on a
    /// single phase is a regression. Duplicate fetches recurring over a
    /// zero baseline regress too.
    #[test]
    fn it_flags_request_and_duplicate_growth() {
        let base = report("none", vec![phase("render", 10, 1000, 5)]);
        let mut new = report("none", vec![phase("render", 16, 1000, 5)]);
        let found = compare_reports("join-none.json", &base, &new, 10.0);
        assert!(
            found
                .iter()
                .any(|line| line.contains("render: requests 10 -> 16")),
            "{found:?}"
        );

        new.phases[0].traffic.requests = 10;
        new.phases[0].duplicate_requests = 5;
        let found = compare_reports("join-none.json", &base, &new, 10.0);
        assert!(
            found
                .iter()
                .any(|line| line.contains("duplicate fetches 0 -> 5")),
            "{found:?}"
        );
    }

    /// Under the baseline's seed the unshaped profile is exact, so one
    /// phase growing past the slack regresses even when another phase
    /// shrank by as much. Under another seed the seams sit elsewhere: the
    /// same move is spread, and only growth in the totals regresses.
    #[test]
    fn it_holds_phases_exact_only_under_the_baselines_seed() {
        let base = report(
            "none",
            vec![phase("claim", 5, 1000, 0), phase("seeded", 98, 1000, 0)],
        );
        let moved = vec![phase("claim", 10, 1000, 0), phase("seeded", 93, 1000, 0)];

        let same_seed = report("none", moved.clone());
        let found = compare_reports("join-none.json", &base, &same_seed, 10.0);
        assert!(
            found
                .iter()
                .any(|line| line.contains("claim: requests 5 -> 10")),
            "{found:?}"
        );

        let mut other_seed = report("none", moved);
        other_seed.seed = Some(8);
        let found = compare_reports("join-none.json", &base, &other_seed, 10.0);
        assert!(found.is_empty(), "{found:?}");

        let mut grown = report(
            "none",
            vec![phase("claim", 10, 1000, 0), phase("seeded", 140, 1000, 0)],
        );
        grown.seed = Some(8);
        let found = compare_reports("join-none.json", &base, &grown, 10.0);
        assert!(
            found
                .iter()
                .any(|line| line.contains("lazy-join total: requests")),
            "{found:?}"
        );
    }

    /// A longer dependent chain regresses per phase even when the request
    /// count holds, a baseline without the chain gates nothing on it, and
    /// blob opens appearing where the baseline had none regress too.
    #[test]
    fn it_flags_chain_and_blob_growth() {
        let base = report("none", vec![phase("render", 10, 1000, 5)]);
        let mut new = report("none", vec![phase("render", 10, 1000, 5)]);
        new.phases[0].round_trips = Some(16);
        let found = compare_reports("join-none.json", &base, &new, 10.0);
        assert!(
            found
                .iter()
                .any(|line| line.contains("render: round trips 10 -> 16")),
            "{found:?}"
        );

        let mut unchained = base.clone();
        unchained.phases[0].round_trips = None;
        assert!(
            compare_reports("join-none.json", &unchained, &new, 10.0).is_empty(),
            "a baseline without the chain gates nothing on it"
        );

        new.phases[0].round_trips = Some(10);
        new.phases[0].blob_requests = 5;
        let found = compare_reports("join-none.json", &base, &new, 10.0);
        assert!(
            found
                .iter()
                .any(|line| line.contains("blob requests 0 -> 5")),
            "{found:?}"
        );
    }

    /// Cost migrating between lazy phases cancels in the totals; growing
    /// the total virtual time past the threshold does not.
    #[test]
    fn it_gates_totals_not_migration() {
        let base = report(
            "none",
            vec![phase("probe", 10, 1000, 50), phase("render", 10, 1000, 50)],
        );
        let migrated = report(
            "none",
            vec![phase("probe", 12, 1000, 90), phase("render", 8, 1000, 10)],
        );
        assert!(
            compare_reports("join-none.json", &base, &migrated, 10.0).is_empty(),
            "migration within totals passes"
        );

        let grown = report(
            "none",
            vec![
                phase("probe", 10, 1000, 200),
                phase("render", 10, 1000, 200),
            ],
        );
        let found = compare_reports("join-none.json", &base, &grown, 10.0);
        assert!(
            found
                .iter()
                .any(|line| line.contains("lazy-join total: virtual_ms")),
            "{found:?}"
        );
    }

    fn gate(rebaselined: bool) -> Gate<'static> {
        Gate {
            report: "join-none",
            comparable: true,
            rebaselined,
        }
    }

    /// Every phase quantity and both totals become rows; a phase's bytes
    /// and modeled time carry no verdict of their own, and the moved rows
    /// lead the table with the rest folded away.
    #[test]
    fn every_quantity_is_a_row_and_the_unmoved_fold() {
        let base = report("none", vec![phase("probe", 10, 1000, 5)]);
        let new = report("none", vec![phase("probe", 11, 1000, 5)]);
        let mut out = Comparison::default();
        super::compare_reports(&mut out, &gate(false), &base, &new, 10.0);
        let probe: Vec<_> = out
            .rows
            .iter()
            .filter(|row| row.phase == "probe")
            .map(|row| (row.metric, row.verdict))
            .collect();
        assert_eq!(
            probe,
            [
                ("requests", Verdict::Within),
                ("round trips", Verdict::Within),
                ("blob requests", Verdict::Within),
                ("duplicate fetches", Verdict::Within),
                ("bytes", Verdict::Reported),
                ("blob bytes", Verdict::Reported),
                ("modeled ms", Verdict::Reported),
            ]
        );
        assert_eq!(out.rows.len(), 7 + 6, "seven per phase, three per total");

        let markdown = out.markdown();
        let (moved, folded) = markdown.split_once("<details>").expect("a fold");
        assert!(
            moved.contains("| join-none | probe | requests | 10 | 11 | +1 | +10.00% |  |"),
            "{markdown}"
        );
        assert!(folded.contains("rows unchanged"), "{markdown}");
    }

    /// Against the base, a re-baselined report's growth is accepted, not
    /// failed; a report on one side only or seeded differently is a note
    /// with ungated rows.
    #[test]
    fn against_the_base_reshaped_reports_are_not_failures() -> Result<()> {
        let base = report("none", vec![phase("render", 10, 1000, 5)]);
        let grown = report("none", vec![phase("render", 20, 1000, 5)]);
        let mut out = Comparison::default();
        super::compare_reports(&mut out, &gate(true), &base, &grown, 10.0);
        assert!(out.passed(), "{:?}", out.regressions);
        assert!(out.rows.iter().any(|row| row.verdict == Verdict::Accepted));

        let dir = std::env::temp_dir().join(format!("dialog-soak-compare-{}", std::process::id()));
        let (base_dir, new_dir) = (dir.join("base"), dir.join("new"));
        fs::create_dir_all(&base_dir)?;
        fs::create_dir_all(&new_dir)?;
        let mut reseeded = grown.clone();
        reseeded.entities = 200;
        fs::write(
            base_dir.join("join-none.json"),
            serde_json::to_string(&base)?,
        )?;
        fs::write(
            new_dir.join("join-none.json"),
            serde_json::to_string(&reseeded)?,
        )?;
        fs::write(
            new_dir.join("join-mobile.json"),
            serde_json::to_string(&grown)?,
        )?;
        let against_base = compare_dirs(
            &base_dir,
            &new_dir,
            10.0,
            &Against::Sweep {
                rebaselined: BTreeSet::new(),
            },
        )?;
        let against_file = compare_dirs(&base_dir, &new_dir, 10.0, &Against::Baseline)?;
        fs::remove_dir_all(&dir)?;

        assert!(against_base.passed(), "{:?}", against_base.regressions);
        assert_eq!(against_base.notes.len(), 2, "{:?}", against_base.notes);
        assert!(
            against_base
                .rows
                .iter()
                .all(|row| row.verdict == Verdict::Ungated)
        );
        assert!(!against_file.passed(), "the file still gates the requests");
        Ok(())
    }
}
