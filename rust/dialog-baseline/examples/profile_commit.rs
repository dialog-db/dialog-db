//! Profile target: one memory-backed batch commit of N entities.
//!
//! Exists to run under a profiler (callgrind, perf) without criterion in
//! the way — the batch-commit benchmark showed superlinear per-entity cost
//! (1.8 ms/entity at N=1000 vs 0.14 ms/entity at N=100), and this pins the
//! whole run to exactly one commit so the profile is all commit path.
//!
//! ```sh
//! cargo build -p dialog-baseline --example profile_commit
//! valgrind --tool=callgrind target/debug/examples/profile_commit 1000
//! ```

use dialog_baseline::{DialogFacts, DialogMode, generate_rows};

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(1000);
    let rows = generate_rows(count);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut store = DialogFacts::open(DialogMode::Memory).await?;
        let start = std::time::Instant::now();
        store.insert_one_transaction(&rows).await?;
        eprintln!("committed {count} entities in {:?}", start.elapsed());
        Ok(())
    })
}
