//! A subscription catching up on many touched entities re-derives them
//! together, not one round trip at a time.
//!
//! Native only, as the whole harness is: the simulated link runs under
//! tokio's paused clock.
#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_soak::{DEFAULT_SEED, Kind, PhaseReport, Scenario, run};

/// The exact chain and the request count of the phase named `name`.
fn chain_and_requests(phases: &[PhaseReport], name: &str) -> Result<(u64, u64)> {
    let phase = phases
        .iter()
        .find(|phase| phase.name == name)
        .ok_or_else(|| anyhow::anyhow!("the catch-up reports a {name} phase"))?;
    let chain = phase
        .round_trips
        .ok_or_else(|| anyhow::anyhow!("the {name} phase counts its chain"))?;
    Ok((chain, phase.traffic.requests))
}

/// A poll that catches up on k one-fact commits re-derives each touched
/// entity with a point read of its leaf. Those reads are independent, so
/// they go out together and the poll's chain stays well under its
/// request count. Read one entity at a time, the chain equals the
/// requests: 9 of 9 for `poll-10` and 52 of 52 for `poll-100` at this
/// seed and size, before the re-derivations ran together (2 of 9 and 23
/// of 52 after).
///
/// `poll-10` is all re-derivation: the commits sit in the root's write
/// buffer, so the cover diff reads the root alone. Two thousand entities
/// keep the seed's buffer clear of the overflow that would turn a later
/// poll into a walk of the flushed leaves (#646). The run is unshaped, so
/// the chain is the schedule's own and the counts repeat exactly.
#[tokio::test(start_paused = true)]
async fn it_rederives_the_touched_entities_of_a_catch_up_together() -> Result<()> {
    let report = run(Scenario {
        kind: Kind::CatchUp,
        entities: 2000,
        commits: 32,
        members: 5,
        network: None,
        network_label: "none".into(),
        vault_dir: std::env::temp_dir().join("dialog-soak-catchup-rederivation"),
        seed: DEFAULT_SEED,
    })
    .await?;

    for name in ["poll-10", "poll-100"] {
        let (chain, requests) = chain_and_requests(&report.phases, name)?;
        assert!(
            requests >= 8,
            "the {name} phase must read enough leaves for their overlap to mean \
             anything (requests={requests})"
        );
        assert!(
            chain * 2 <= requests,
            "the {name} phase made {requests} requests in a chain of {chain}: its \
             touched entities are re-derived one round trip at a time"
        );
    }
    Ok(())
}
