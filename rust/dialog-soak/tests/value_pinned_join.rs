//! A value-pinned join over a cold remote overlaps its entity reads.
//!
//! Native only, as the whole harness is: the simulated link runs under
//! tokio's paused clock.
#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use anyhow::Result;
use dialog_remote_fs::simulation::NetworkShape;
use dialog_soak::{JoinScenario, PhaseReport, run_join};

/// The exact chain and the request count of the phase named `name`.
fn chain_and_requests(phases: &[PhaseReport], name: &str) -> Result<(u64, u64)> {
    let phase = phases
        .iter()
        .find(|phase| phase.name == name)
        .ok_or_else(|| anyhow::anyhow!("the join reports a {name} phase"))?;
    let chain = phase
        .round_trips
        .ok_or_else(|| anyhow::anyhow!("the {name} phase counts its chain"))?;
    Ok((chain, phase.traffic.requests))
}

/// A join driven by a value-pinned, single-valued scan (`status =
/// "open"`, every other field probed per entity) reads one entity leaf
/// for every run of matching entities it holds. The scan's own leaves
/// arrive together; what decides the round trips is whether the entity
/// leaves do too. Each candidate's winner check names its entity leaf,
/// so the leaves past the one being checked are requested before that
/// check completes, and the phase's chain stays well under its request
/// count. Read one candidate run at a time, every entity leaf is its own
/// round trip and the chain equals the requests (35 of 36 here, before
/// the checks were pipelined; 12 of 36 after).
///
/// Both value-pinned phases are held: `rule`, where the pinned scan is a
/// rule body's, and `filtered`, where it is a concept query's own.
#[tokio::test(start_paused = true)]
async fn it_overlaps_the_entity_reads_of_a_value_pinned_join() -> Result<()> {
    let vault = std::env::temp_dir().join("dialog-soak-value-pinned-join");
    let report = run_join(JoinScenario {
        entities: 1200,
        commits: 4,
        members: 5,
        network: Some(NetworkShape {
            latency: Duration::from_micros(400),
            auth_latency: Duration::from_micros(400),
            bandwidth: Some(125_000_000),
            connections: None,
        }),
        network_label: "localhost".into(),
        vault_dir: vault,
    })
    .await?;

    for name in ["rule", "filtered"] {
        let (chain, requests) = chain_and_requests(&report.phases, name)?;
        assert!(
            requests >= 24,
            "the {name} phase must read enough entity leaves for their overlap to \
             mean anything (requests={requests})"
        );
        assert!(
            chain * 2 <= requests,
            "the {name} phase made {requests} requests in a chain of {chain}: its \
             entity leaves are read one round trip at a time"
        );
    }
    Ok(())
}
