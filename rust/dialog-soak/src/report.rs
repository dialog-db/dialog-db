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
    /// Estimated depth of the phase's longest sequential fetch chain:
    /// modeled elapsed time divided by the per-request serial cost
    /// (round-trip latency plus auth latency). An upper bound, since
    /// bandwidth serialization and compute also advance the clock; 0 when
    /// the run is unshaped. Kept beside [`round_trips`](Self::round_trips),
    /// the exact count, because the gap between the two is the share of
    /// modeled time that is not latency: bytes on the link and compute.
    #[serde(default)]
    pub rounds: f64,
    /// The phase's longest chain of dependent requests: how many requests
    /// the client issued one after another, each only once the one before
    /// it had completed (`dialog_remote_fs::simulation::longest_chain`).
    /// Exact and independent of the link model, so the unshaped profile
    /// reports it too. This is the round-trip cost of the cost model:
    /// `requests` may grow while `round_trips` should approach tree depth
    /// times the number of ranges visited. `None` in a report recorded
    /// before the count existed.
    #[serde(default)]
    pub round_trips: Option<u64>,
    /// Blob opens the phase issued (`blob.read` and `blob.import`): the
    /// objects beyond archive blocks and head cells. Counted in
    /// `traffic.requests` too; separate here because a layout change that
    /// moves bytes from blocks into blobs moves cost between the two.
    #[serde(default)]
    pub blob_requests: u64,
    /// Bytes those blobs streamed, in either direction. Counted in
    /// `traffic.bytes` too.
    #[serde(default)]
    pub blob_bytes: u64,
    /// Distinct blocks the phase's `archive.get` requests brought over.
    #[serde(default)]
    pub unique_blocks: u64,
    /// Requests that re-downloaded a block already received in the phase:
    /// pure replication waste, and the count the duplication work drives
    /// to zero.
    #[serde(default)]
    pub duplicate_requests: u64,
    /// Bytes those duplicate requests moved.
    #[serde(default)]
    pub duplicate_bytes: u64,
    /// Requests that returned no block (the remote does not hold the key).
    #[serde(default)]
    pub empty_requests: u64,
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
    /// Connections the modeled client may hold open at once; `None` is
    /// unlimited.
    #[serde(default)]
    pub connections: Option<usize>,
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
    /// The seed the run's identities derived from (`DIALOG_SOAK_SEED`
    /// replays it); `None` in a report recorded before runs were seeded.
    #[serde(default)]
    pub seed: Option<u64>,
    /// The tree root of the seeded space's head, hex: one seed writes one
    /// root, so two runs that differ here did not run the same tree.
    #[serde(default)]
    pub seeded_root: Option<String>,
    /// The measured phases, in execution order.
    pub phases: Vec<PhaseReport>,
}

impl Report {
    /// Render the run as an aligned human-readable table.
    pub fn table(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let connections = self
            .connections
            .map_or_else(|| "unlimited".to_string(), |cap| cap.to_string());
        let _ = writeln!(
            out,
            "scenario={} network={} (latency {}ms, auth {}ms, bandwidth {} Mbps, connections {})",
            self.scenario,
            self.network,
            self.latency_ms,
            self.auth_ms,
            self.bandwidth_mbps,
            connections
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
            "| {:<10} | {:>10} | {:>6} | {:>5} | {:>8} | {:>6} | {:>5} | {:>5} | {:>5} | {:>10} | {:>10} | breakdown",
            "phase",
            "virtual ms",
            "rounds",
            "chain",
            "requests",
            "unique",
            "dup",
            "empty",
            "blobs",
            "bytes",
            "blob bytes"
        );
        let _ = writeln!(
            out,
            "|{:-<12}|{:-<12}|{:-<8}|{:-<7}|{:-<10}|{:-<8}|{:-<7}|{:-<7}|{:-<7}|{:-<12}|{:-<12}|----------",
            "", "", "", "", "", "", "", "", "", "", ""
        );
        for phase in &self.phases {
            let breakdown = phase
                .traffic
                .rows
                .iter()
                .map(|(label, count, bytes)| format!("{label}:{count}({bytes}B)"))
                .collect::<Vec<_>>()
                .join(" ");
            let chain = phase
                .round_trips
                .map_or_else(|| "-".to_string(), |chain| chain.to_string());
            let _ = writeln!(
                out,
                "| {:<10} | {:>10} | {:>6.0} | {:>5} | {:>8} | {:>6} | {:>5} | {:>5} | {:>5} | {:>10} | {:>10} | {}",
                phase.name,
                phase.virtual_ms,
                phase.rounds,
                chain,
                phase.traffic.requests,
                phase.unique_blocks,
                phase.duplicate_requests,
                phase.empty_requests,
                phase.blob_requests,
                phase.traffic.bytes,
                phase.blob_bytes,
                breakdown
            );
        }
        out
    }
}
