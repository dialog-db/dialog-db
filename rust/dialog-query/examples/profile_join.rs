//! Standalone profiling driver for the two-attribute concept join.
//!
//! Seeds a fixed-size fact base once, then runs the public `Stuff` concept
//! query in a tight loop so a sampling profiler (xctrace / Instruments) sees
//! the query-engine hot path without the criterion harness in the frame.
//!
//! Run under Instruments:
//!
//! ```sh
//! cargo build -p dialog-query --example profile_join --features helpers --release
//! xctrace record --template 'Time Profiler' --output join.trace \
//!   --launch -- target/release/examples/profile_join 1000 200
//! ```
//!
//! Args: `<size> <iterations>` (defaults: 1000 size, 200 iterations).

#[path = "../src/helpers.rs"]
#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::BenchEnv;

fn main() {
    let mut args = std::env::args().skip(1);
    let size: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(1_000);
    let iterations: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(200);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let env = BenchEnv::volatile().await.unwrap();
        env.seed_stuff(size).await.unwrap();

        // One untimed warm-up to settle caches / lazy planning.
        let warm = env.query_stuff().await.unwrap();
        eprintln!(
            "warm: results={} reads={} unique_reads={}",
            warm.results_len, warm.reads, warm.unique_reads
        );

        for _ in 0..iterations {
            let run = env.query_stuff().await.unwrap();
            std::hint::black_box(run.results_len);
        }
    });
}
