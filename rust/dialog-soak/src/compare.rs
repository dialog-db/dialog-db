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

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context as _, Result};

use crate::report::Report;

/// The outcome of comparing two sweep directories.
#[derive(Debug, Clone, Default)]
pub struct Comparison {
    /// One human-readable line per shared config: lazy-join and download
    /// modeled times, baseline against new.
    pub summary: Vec<String>,
    /// One line per detected regression; empty means the gate passes.
    pub regressions: Vec<String>,
}

impl Comparison {
    /// Whether the gate passes (no regressions).
    pub fn passed(&self) -> bool {
        self.regressions.is_empty()
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

/// Load every `*.json` report in `dir`, keyed by file name.
fn load(dir: &Path) -> Result<BTreeMap<String, Report>> {
    let mut reports = BTreeMap::new();
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            let name = path
                .file_name()
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

/// Compare the reports shared by two sweep directories under a relative
/// growth `threshold_pct`.
pub fn compare_dirs(baseline: &Path, new: &Path, threshold_pct: f64) -> Result<Comparison> {
    let baseline = load(baseline)?;
    let new = load(new)?;
    let shared: Vec<_> = baseline
        .keys()
        .filter(|name| new.contains_key(*name))
        .collect();
    anyhow::ensure!(!shared.is_empty(), "no overlapping reports to compare");

    let mut comparison = Comparison::default();
    for name in &shared {
        let base_report = &baseline[*name];
        let new_report = &new[*name];
        comparison.regressions.extend(compare_reports(
            name,
            base_report,
            new_report,
            threshold_pct,
        ));
        comparison.summary.push(format!(
            "{name}: lazy join {} ms -> {} ms | download {} ms -> {} ms",
            totals(base_report, false).virtual_ms,
            totals(new_report, false).virtual_ms,
            totals(base_report, true).virtual_ms,
            totals(new_report, true).virtual_ms,
        ));
    }
    Ok(comparison)
}

fn compare_reports(name: &str, base: &Report, new: &Report, threshold_pct: f64) -> Vec<String> {
    let mut regressions = Vec::new();
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

    let base_phases: BTreeMap<_, _> = base.phases.iter().map(|p| (p.name.as_str(), p)).collect();
    for new_phase in &new.phases {
        let Some(base_phase) = base_phases.get(new_phase.name.as_str()) else {
            continue;
        };
        if regressed(
            base_phase.duplicate_requests,
            new_phase.duplicate_requests,
            request_slack,
            request_threshold,
        ) {
            regressions.push(format!(
                "{name}:{}: duplicate fetches {} -> {}",
                new_phase.name, base_phase.duplicate_requests, new_phase.duplicate_requests
            ));
        }
        // An unshaped run of another seed moved its seams: a block moves
        // between phases, which only the totals below see through.
        if new.network == "none" && !exact {
            continue;
        }
        if regressed(
            base_phase.traffic.requests,
            new_phase.traffic.requests,
            request_slack,
            request_threshold,
        ) {
            regressions.push(format!(
                "{name}:{}: requests {} -> {}",
                new_phase.name, base_phase.traffic.requests, new_phase.traffic.requests
            ));
        }
        if let (Some(base_chain), Some(new_chain)) = (base_phase.round_trips, new_phase.round_trips)
            && regressed(base_chain, new_chain, request_slack, request_threshold)
        {
            regressions.push(format!(
                "{name}:{}: round trips {base_chain} -> {new_chain}",
                new_phase.name
            ));
        }
        if regressed(
            base_phase.blob_requests,
            new_phase.blob_requests,
            request_slack,
            request_threshold,
        ) {
            regressions.push(format!(
                "{name}:{}: blob requests {} -> {}",
                new_phase.name, base_phase.blob_requests, new_phase.blob_requests
            ));
        }
    }

    for (scope, download) in [("lazy-join", false), ("download", true)] {
        let base_total = totals(base, download);
        let new_total = totals(new, download);
        let checks = [
            (
                "requests",
                base_total.requests,
                new_total.requests,
                request_slack,
                request_threshold,
            ),
            (
                "bytes",
                base_total.bytes,
                new_total.bytes,
                64 * 1024,
                threshold_pct * if loose { 2.0 } else { 1.0 },
            ),
            (
                "virtual_ms",
                base_total.virtual_ms,
                new_total.virtual_ms,
                100,
                threshold_pct * if loose { 2.0 } else { 1.0 },
            ),
        ];
        for (metric, base_value, new_value, slack, threshold) in checks {
            if regressed(base_value, new_value, slack, threshold) {
                let growth = if base_value > 0 {
                    format!(
                        "+{:.1}%",
                        (new_value - base_value) as f64 / base_value as f64 * 100.0
                    )
                } else {
                    "+inf".to_string()
                };
                regressions.push(format!(
                    "{name}:{scope}: {metric} {base_value} -> {new_value} ({growth})"
                ));
            }
        }
    }
    regressions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::{PhaseReport, TallyRows};

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
                .any(|line| line.contains("lazy-join: virtual_ms")),
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
                .any(|line| line.contains("lazy-join: requests")),
            "{found:?}"
        );
    }
}
