//! Per-commit cost as the dataset grows: dialog vs SQLite, windowed.
//!
//! The headline comparisons run at 500 transactions; this replays the
//! real SE log much further and reports the per-commit wall time of each
//! successive window, so the SCALING of the commit cost — not its
//! small-database constant — is what gets compared. Both stores are
//! in-memory (the CPU curve, no disk noise).
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example scale_curve -- 25000 2500
//! ```

use std::time::Instant;

use dialog_baseline::se::SeLog;
use dialog_baseline::{DialogFacts, DialogMode, SqliteFacts, SqliteMode};

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(25000);
    let window: usize = std::env::args()
        .nth(2)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(2500);
    let log = SeLog::load(count)?;
    println!(
        "txns={} facts={} window={window}",
        log.transactions.len(),
        log.fact_count()
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut sqlite = SqliteFacts::open(SqliteMode::Memory)?;
        let mut dialog = DialogFacts::open(DialogMode::Memory).await?;

        println!(
            "{:>10}  {:>14}  {:>14}  {:>6}",
            "txns", "sqlite us/txn", "dialog us/txn", "ratio"
        );
        let mut done = 0;
        while done < log.transactions.len() {
            let end = (done + window).min(log.transactions.len());
            let slice = SeLog {
                transactions: log.transactions[done..end].to_vec(),
            };
            let n = slice.transactions.len() as f64;

            let started = Instant::now();
            sqlite.replay_se(&slice)?;
            let sqlite_us = started.elapsed().as_micros() as f64 / n;

            let started = Instant::now();
            dialog.replay_se(&slice).await?;
            let dialog_us = started.elapsed().as_micros() as f64 / n;

            done = end;
            println!(
                "{done:>10}  {sqlite_us:>14.1}  {dialog_us:>14.1}  {:>6.1}",
                dialog_us / sqlite_us
            );
        }
        Ok(())
    })
}
