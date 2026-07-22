//! Standalone profiling driver for the transaction-log replay.
//!
//! Replays a Stack Exchange transaction log (see `scripts/se-transform.py`)
//! one commit per source edit, so a sampling profiler sees the commit path
//! without a test harness in the frame. Per-commit cost grows with history
//! depth on this workload, and the query path does not (cardinality-one
//! supersession keeps the queried state one value per entity), so what a
//! profile of this shows is the commit-side history and novelty work.
//!
//! Run under a profiler (macOS):
//!
//! ```sh
//! cargo build -p dialog-query --example profile_replay --features helpers --release
//! xctrace record --template 'Time Profiler' --output replay.trace \
//!   --launch -- target/release/examples/profile_replay <log.csv> 2048
//! ```
//!
//! or with cargo-flamegraph:
//!
//! ```sh
//! cargo flamegraph -p dialog-query --example profile_replay --features helpers \
//!   -- <log.csv> 2048
//! ```
//!
//! Args: `<log.csv> [limit]` (limit defaults to 2048; 0 replays all).

// The replay drives the on-disk BenchEnv and a tokio runtime, neither of
// which exists on wasm; the example is native-only, with a stub main so the
// wasm example build (which compiles every example) still links.
#![cfg_attr(target_arch = "wasm32", allow(unused))]

#[cfg(not(target_arch = "wasm32"))]
#[path = "../src/helpers.rs"]
#[allow(dead_code, unused_imports)]
mod helpers;
#[cfg(not(target_arch = "wasm32"))]
use helpers::BenchEnv;

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .expect("usage: profile_replay <log.csv> [limit]");
    let limit: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(2048);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let env = BenchEnv::temp().await.unwrap();
        let start = std::time::Instant::now();
        let entities = env.import_transaction_log(&path, limit).await.unwrap();
        let elapsed = start.elapsed();
        eprintln!(
            "replayed limit={limit} entities={} in {elapsed:?}",
            entities.len()
        );
    });
}
