// The soak harness is native-only: on wasm the crate is empty.
#![cfg(not(target_arch = "wasm32"))]

//! End-to-end regressions the soak harness pins under a seed: a seed
//! replays the exact tree a bug needed, which no unit oracle reaches.

use dialog_soak::{Kind, Report, Scenario, run};

/// Run `kind` on the unshaped profile under `seed` at the default
/// fixture, on tokio's paused clock as the soak binary does.
fn soak(kind: Kind, seed: u64) -> Report {
    let scenario = Scenario {
        kind,
        entities: 4000,
        commits: 32,
        members: 5,
        network: None,
        network_label: "none".into(),
        vault_dir: std::env::temp_dir(),
        seed,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("a runtime builds")
        .block_on(run(scenario))
        .expect("the scenario runs")
}

/// A one-fact commit landing on the seed's near-full buffer must not
/// cascade into a catch-up that reads the tree (#662, fixed by #669).
///
/// Under this seed the subscribed client's first poll after a one-fact
/// commit read 332 objects over a chain of 22 before the fix, which
/// prunes a subtree both sides route the same buffered ops to instead of
/// descending into it. With the fix it reads 2 over a chain of 2.
#[test]
fn catch_up_after_one_fact_reads_no_more_than_the_root_and_a_leaf() {
    let report = soak(Kind::CatchUp, 885_488_280_369_673_574);
    let poll = report
        .phases
        .iter()
        .find(|phase| phase.name == "poll-1")
        .expect("the catch-up reports poll-1");
    assert!(
        poll.traffic.requests <= 4,
        "poll-1 read {} objects (332 before #669)",
        poll.traffic.requests
    );
    let chain = poll.round_trips.expect("the chain is counted");
    assert!(
        chain <= 4,
        "poll-1 chained {chain} round trips (22 before #669)"
    );
}
