//! Profile target: repository-layer commits (`Branch::commit`) of N entities.
//!
//! Pins a profiler run to exactly `Branch::commit` so its cost attributes
//! to its parts (index writes, history records, revision record encode,
//! signing, head publication).
//!
//! The optional second argument picks the shape: `small` (default) commits
//! one transaction per entity; `batch` commits every entity in one
//! transaction.
//!
//! ```sh
//! cargo build -p dialog-baseline --example profile_repo_commit
//! valgrind --tool=callgrind target/debug/examples/profile_repo_commit 200 small
//! valgrind --tool=callgrind target/debug/examples/profile_repo_commit 200 batch
//! ```

use dialog_baseline::generate_rows;
use dialog_baseline::repo::DialogRepo;

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(200);
    let batch = std::env::args().nth(2).is_some_and(|mode| mode == "batch");
    let rows = generate_rows(count);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let repo = DialogRepo::volatile().await?;
        let start = std::time::Instant::now();
        if batch {
            repo.insert_one_transaction(&rows).await?;
        } else {
            repo.insert_per_row_transactions(&rows).await?;
        }
        let shape = if batch { "one txn" } else { "per-row txns" };
        eprintln!(
            "committed {count} entities ({shape}) through the branch in {:?}",
            start.elapsed()
        );
        Ok(())
    })
}
