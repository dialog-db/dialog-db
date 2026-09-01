#![cfg(not(target_arch = "wasm32"))]

//! # dialog-soak
//!
//! Runs the sync/join soak scenario over a simulated network and prints a
//! JSON report (stdout) plus a human-readable table (stderr).
//!
//! ```text
//! cargo run -p dialog-soak --release -- --network mobile
//! cargo run -p dialog-soak --release -- --network custom \
//!     --latency-ms 120 --auth-ms 200 --bandwidth-mbps 8
//! DIALOG_TREE_FANOUT_N=5 cargo run -p dialog-soak --release -- --network mobile
//! ```
//!
//! By default the harness runs under tokio's paused test clock: simulated
//! delays complete instantly while virtual time advances by exactly the
//! modeled amount, so a run over a 300 ms link finishes in real seconds
//! and reports deterministic modeled times. `--real-time` opts into real
//! sleeps for validation.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use dialog_remote_fs::simulation::NetworkShape;
use dialog_soak::{JoinScenario, run_join};

/// Sync/join soak harness over a simulated network.
#[derive(Debug, Parser)]
#[command(name = "soak", version, about)]
struct Cli {
    /// Network preset: none, localhost, broadband, mobile, intercontinental,
    /// or custom (with --latency-ms/--auth-ms/--bandwidth-mbps).
    #[arg(long, default_value = "broadband")]
    network: String,

    /// Round-trip latency in milliseconds (custom network).
    #[arg(long, default_value_t = 30.0)]
    latency_ms: f64,

    /// Per-request authorization latency in milliseconds (custom network).
    /// Models the per-object access-service redeem a UCAN remote pays.
    #[arg(long, default_value_t = 50.0)]
    auth_ms: f64,

    /// Shared link bandwidth in megabits per second, 0 = unlimited
    /// (custom network).
    #[arg(long, default_value_t = 100.0)]
    bandwidth_mbps: f64,

    /// Entities to seed (6 facts each).
    #[arg(long, default_value_t = 4000)]
    entities: usize,

    /// Commits to split the seed into (history depth).
    #[arg(long, default_value_t = 32)]
    commits: usize,

    /// Members on the seeded roster.
    #[arg(long, default_value_t = 5)]
    members: usize,

    /// Directory for the run's remote vault (a unique subdirectory is
    /// created per run). Defaults to the system temp directory.
    #[arg(long)]
    vault_dir: Option<PathBuf>,

    /// Sleep in real time instead of the paused virtual clock.
    #[arg(long, default_value_t = false)]
    real_time: bool,

    /// Print only the JSON report (suppress the table).
    #[arg(long, default_value_t = false)]
    json_only: bool,
}

/// A named preset link model.
fn preset(name: &str, cli: &Cli) -> Result<(Option<NetworkShape>, String)> {
    let shape = |latency_ms: f64, auth_ms: f64, bandwidth_mbps: f64| {
        Some(NetworkShape {
            latency: Duration::from_secs_f64(latency_ms / 1000.0),
            auth_latency: Duration::from_secs_f64(auth_ms / 1000.0),
            bandwidth: (bandwidth_mbps > 0.0).then(|| (bandwidth_mbps * 1_000_000.0 / 8.0) as u64),
        })
    };
    let network = match name {
        // Counts only: no delays, so virtual times read as pure compute.
        "none" => None,
        // A same-host or LAN service: sub-millisecond everything.
        "localhost" => shape(0.4, 0.4, 1000.0),
        // Home broadband to a nearby region, warm HTTPS connections.
        "broadband" => shape(30.0, 50.0, 100.0),
        // A phone on 4G: the latency budget tonk's field reports describe.
        "mobile" => shape(80.0, 120.0, 20.0),
        // Cross-ocean or degraded link.
        "intercontinental" => shape(250.0, 250.0, 50.0),
        "custom" => shape(cli.latency_ms, cli.auth_ms, cli.bandwidth_mbps),
        other => anyhow::bail!(
            "unknown network {other:?} (use none, localhost, broadband, mobile, \
             intercontinental, or custom)"
        ),
    };
    Ok((network, name.to_string()))
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let (network, network_label) = preset(&cli.network, &cli)?;
    let scenario = JoinScenario {
        entities: cli.entities,
        commits: cli.commits,
        members: cli.members,
        network,
        network_label,
        vault_dir: cli.vault_dir.clone().unwrap_or_else(std::env::temp_dir),
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let report = runtime.block_on(async {
        if !cli.real_time {
            tokio::time::pause();
        }
        run_join(scenario).await
    })?;

    if !cli.json_only {
        eprintln!("{}", report.table());
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
