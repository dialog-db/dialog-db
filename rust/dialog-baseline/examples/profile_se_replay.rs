//! Profile target: memory-backed replay of the Stack Exchange log.
//!
//! Exists to run under a profiler without criterion in the way, pinned to
//! the exact workload behind the headline per-commit number (`dialog_mem`
//! in `se_replay_write`): one buffered commit per real transaction. Set
//! `DIALOG_SE_CSV` to profile against the real dump; the synthetic
//! approximation is used otherwise.
//!
//! ```sh
//! cargo build --release -p dialog-baseline --example profile_se_replay
//! valgrind --tool=callgrind target/release/examples/profile_se_replay 500
//! ```

use dialog_baseline::se::SeLog;
use dialog_baseline::{DialogFacts, DialogMode};

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(500);
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut store = DialogFacts::open(DialogMode::Memory).await?;
        let start = std::time::Instant::now();
        store.replay_se(&log).await?;
        eprintln!(
            "replayed {} txns ({} facts) in {:?} ({:.0} us/txn)",
            log.transactions.len(),
            log.fact_count(),
            start.elapsed(),
            start.elapsed().as_micros() as f64 / log.transactions.len() as f64
        );
        Ok(())
    })
}
