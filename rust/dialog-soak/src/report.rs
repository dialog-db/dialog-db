//! Report types: what one soak run measured, serializable for baselines.

use dialog_remote_fs::simulation::TransferTally;
use serde::{Deserialize, Serialize};

/// Per-bucket rows of a [`TransferTally`], in a stable serializable shape.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TallyRows {
    /// `(bucket label, invocation count, payload bytes)` per non-empty bucket.
    pub rows: Vec<(String, u64, u64)>,
    /// Total invocations across buckets.
    pub requests: u64,
    /// Total payload bytes across buckets.
    pub bytes: u64,
}

impl From<TransferTally> for TallyRows {
    fn from(tally: TransferTally) -> Self {
        let (requests, bytes) = tally.total();
        Self {
            rows: tally
                .rows()
                .map(|(bucket, count, bytes)| (bucket.label().to_string(), count, bytes))
                .collect(),
            requests,
            bytes,
        }
    }
}

/// One measured phase of a scenario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseReport {
    /// Phase name (`pull`, `probe`, `render`, ...).
    pub name: String,
    /// Modeled wall-clock milliseconds the phase took under the simulated
    /// network (virtual time when the harness runs under the paused
    /// clock).
    pub virtual_ms: u64,
    /// What crossed the simulated wire during the phase.
    pub traffic: TallyRows,
}

/// A complete soak run: configuration, the space's stored shape, and the
/// per-phase measurements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    /// Scenario name.
    pub scenario: String,
    /// Network preset (or `custom` / `none`).
    pub network: String,
    /// Modeled round-trip latency in milliseconds.
    pub latency_ms: f64,
    /// Modeled per-request authorization latency in milliseconds.
    pub auth_ms: f64,
    /// Modeled shared-link bandwidth in megabits per second (0 =
    /// unlimited).
    pub bandwidth_mbps: f64,
    /// Branching parameter `n` the seeded tree was built with (expected
    /// fanout is `2^n`).
    pub fanout_n: u8,
    /// The seeded tree's `max_segment` (leaf weight target, bytes).
    pub max_segment: u32,
    /// Entities seeded into the space.
    pub entities: usize,
    /// Facts seeded into the space.
    pub facts: usize,
    /// Commits the seed was split into.
    pub commits: usize,
    /// Files in the remote vault after seeding (blocks + cells + blobs).
    pub vault_files: u64,
    /// Total bytes in the remote vault after seeding.
    pub vault_bytes: u64,
    /// The measured phases, in execution order.
    pub phases: Vec<PhaseReport>,
}

impl Report {
    /// Render the run as an aligned human-readable table.
    pub fn table(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "scenario={} network={} (latency {}ms, auth {}ms, bandwidth {} Mbps)",
            self.scenario, self.network, self.latency_ms, self.auth_ms, self.bandwidth_mbps
        );
        let _ = writeln!(
            out,
            "tree: fanout 2^{} max_segment {} | space: {} entities, {} facts, {} commits | vault: {} files, {} KiB",
            self.fanout_n,
            self.max_segment,
            self.entities,
            self.facts,
            self.commits,
            self.vault_files,
            self.vault_bytes / 1024
        );
        let _ = writeln!(
            out,
            "| {:<10} | {:>10} | {:>8} | {:>10} | breakdown",
            "phase", "virtual ms", "requests", "bytes"
        );
        let _ = writeln!(
            out,
            "|{:-<12}|{:-<12}|{:-<10}|{:-<12}|----------",
            "", "", "", ""
        );
        for phase in &self.phases {
            let breakdown = phase
                .traffic
                .rows
                .iter()
                .map(|(label, count, bytes)| format!("{label}:{count}({bytes}B)"))
                .collect::<Vec<_>>()
                .join(" ");
            let _ = writeln!(
                out,
                "| {:<10} | {:>10} | {:>8} | {:>10} | {}",
                phase.name,
                phase.virtual_ms,
                phase.traffic.requests,
                phase.traffic.bytes,
                breakdown
            );
        }
        out
    }
}
