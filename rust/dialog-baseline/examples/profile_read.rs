//! Profile target: point reads and scans against a buffered-root store.
//!
//! Exists to run under a profiler (callgrind, perf) without criterion in the
//! way. Commits seal buffered by default now, so every read descends past
//! node novelty buffers; this pins a profiler run to exactly that read path.
//!
//! Arguments: `[entities] [reads] [shape]`. The store is seeded with
//! `entities` rows committed one transaction each (the shape that leaves
//! buffered ops on the spine), then `reads` operations run against it:
//! `point` (default) cycles point gets over the seeded entities, `scan`
//! repeats the attribute scan.
//!
//! ```sh
//! cargo build -p dialog-baseline --example profile_read
//! valgrind --tool=callgrind target/debug/examples/profile_read 1000 2000
//! valgrind --tool=callgrind target/debug/examples/profile_read 1000 20 scan
//! ```

use dialog_baseline::{DialogFacts, DialogMode, generate_rows};

fn main() -> anyhow::Result<()> {
    let entities: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(1000);
    let reads: usize = std::env::args()
        .nth(2)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(2000);
    let scan = std::env::args().nth(3).is_some_and(|mode| mode == "scan");
    let rows = generate_rows(entities);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut store = DialogFacts::open(DialogMode::Memory).await?;
        store.insert_per_row_transactions(&rows).await?;
        let start = std::time::Instant::now();
        let mut found = 0usize;
        if scan {
            for _ in 0..reads {
                found += store.attribute_scan().await?;
            }
        } else {
            for at in 0..reads {
                let row = &rows[at % rows.len()];
                if store.point_get(&row.entity).await?.is_some() {
                    found += 1;
                }
            }
        }
        let shape = if scan { "scans" } else { "point gets" };
        eprintln!(
            "{reads} {shape} over {entities} entities ({found} hits) in {:?}",
            start.elapsed()
        );
        Ok(())
    })
}
