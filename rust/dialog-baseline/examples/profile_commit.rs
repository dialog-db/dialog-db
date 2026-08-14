//! Profile target: memory-backed commits of N entities.
//!
//! Exists to run under a profiler (callgrind, perf) without criterion in
//! the way — the batch-commit benchmark showed superlinear per-entity cost
//! (1.8 ms/entity at N=1000 vs 0.14 ms/entity at N=100), and this pins the
//! run to exactly the commit path.
//!
//! The optional second argument picks the commit shape: `batch` (default)
//! commits every entity in ONE transaction; `small` commits one transaction
//! per entity — the sequential shape where the buffered commit path
//! amortizes, which a single batch cannot show.
//!
//! ```sh
//! cargo build -p dialog-baseline --example profile_commit
//! valgrind --tool=callgrind target/debug/examples/profile_commit 1000
//! valgrind --tool=callgrind target/debug/examples/profile_commit 100 small
//! ```

use dialog_baseline::{DialogFacts, DialogMode, generate_rows};

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(1000);
    let per_row = std::env::args().nth(2).is_some_and(|mode| mode == "small");
    let rows = generate_rows(count);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut store = DialogFacts::open(DialogMode::Memory).await?;
        let start = std::time::Instant::now();
        if per_row {
            store.insert_per_row_transactions(&rows).await?;
        } else {
            store.insert_one_transaction(&rows).await?;
        }
        let shape = if per_row { "per-row txns" } else { "one txn" };
        eprintln!(
            "committed {count} entities ({shape}) in {:?}",
            start.elapsed()
        );
        Ok(())
    })
}
