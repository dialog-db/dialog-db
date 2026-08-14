//! Profile target: repository-layer commits (`Branch::commit`) of N entities.
//!
//! The repository layer costs ~2.8x the raw `Artifacts::commit` per commit
//! (868 vs 306 us/commit on the real replay); this pins a profiler run to
//! exactly `Branch::commit` so the overhead attributes to its parts (history
//! records, revision record encode, signing, head publication).
//!
//! The optional second argument picks the shape: `small` (default) commits
//! one transaction per entity; `batch` commits every entity in one
//! transaction. Pass `raw` third to drive the same shape through the raw
//! `Artifacts` store instead, for a same-binary A/B.
//!
//! ```sh
//! cargo build -p dialog-baseline --example profile_repo_commit
//! valgrind --tool=callgrind target/debug/examples/profile_repo_commit 200 small
//! valgrind --tool=callgrind target/debug/examples/profile_repo_commit 200 small raw
//! ```

use dialog_baseline::repo::DialogRepo;
use dialog_baseline::{DialogFacts, DialogMode, generate_rows};

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(200);
    let batch = std::env::args().nth(2).is_some_and(|mode| mode == "batch");
    let raw = std::env::args().nth(3).is_some_and(|mode| mode == "raw");
    let rows = generate_rows(count);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let start;
        if raw {
            let mut store = DialogFacts::open(DialogMode::Memory).await?;
            start = std::time::Instant::now();
            if batch {
                store.insert_one_transaction(&rows).await?;
            } else {
                store.insert_per_row_transactions(&rows).await?;
            }
        } else {
            let repo = DialogRepo::volatile().await?;
            start = std::time::Instant::now();
            if batch {
                repo.insert_one_transaction(&rows).await?;
            } else {
                repo.insert_per_row_transactions(&rows).await?;
            }
        }
        let shape = if batch { "one txn" } else { "per-row txns" };
        let layer = if raw { "artifacts" } else { "branch" };
        eprintln!(
            "committed {count} entities ({shape}) through {layer} in {:?}",
            start.elapsed()
        );
        Ok(())
    })
}
