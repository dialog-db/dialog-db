//! Byte-volume measurement: how many bytes each SE commit writes to
//! storage.
//!
//! The callgrind decomposition of the in-memory SE commit shows memcpy +
//! blake3 + allocator as ~60% of all instructions, which reads as "the
//! whole root frame (entries + full novelty buffer) is re-encoded,
//! re-copied, and re-hashed on every commit". This target quantifies
//! that directly by metering the branch's archive traffic
//! ([`Metered`](dialog_baseline::metered::Metered)) and reporting blocks
//! and bytes moved per commit over the replay, windowed so the
//! buffer-fill sawtooth is visible.
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example measure_se_replay -- 500
//! ```

use dialog_baseline::metered::Tally;
use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::{SeLog, se_instructions};
use futures_util::stream;

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(500);
    let window: usize = std::env::args()
        .nth(2)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(50);
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let tally = Tally::default();
        let repo = DialogRepo::metered(tally.clone()).await?;
        let mut committed = 0usize;
        let (mut last_writes, mut last_bytes) = (tally.writes(), tally.write_bytes());
        let (mut last_reads, mut last_read_bytes) = (tally.reads(), tally.read_bytes());
        let mut window_started = std::time::Instant::now();
        println!("commits  writes  set_bytes  (per-commit in window)");
        for commit in &log.transactions {
            repo.branch()
                .commit(stream::iter(se_instructions(commit)?))
                .perform(repo.operator())
                .await?;
            committed += 1;
            if committed.is_multiple_of(window) {
                let (writes, bytes) = (tally.writes(), tally.write_bytes());
                let (reads, read_bytes) = (tally.reads(), tally.read_bytes());
                println!(
                    "{committed:7}  {:.1} blocks / {:.0} B written, {:.1} gets / {:.0} B read, {:.0} us per commit",
                    (writes - last_writes) as f64 / window as f64,
                    (bytes - last_bytes) as f64 / window as f64,
                    (reads - last_reads) as f64 / window as f64,
                    (read_bytes - last_read_bytes) as f64 / window as f64,
                    window_started.elapsed().as_micros() as f64 / window as f64,
                );
                (last_writes, last_bytes) = (writes, bytes);
                (last_reads, last_read_bytes) = (reads, read_bytes);
                window_started = std::time::Instant::now();
            }
        }
        println!(
            "total: {} commits, {} facts, {} blocks, {} bytes written ({:.0} bytes/commit), {} gets, {} bytes read",
            committed,
            log.fact_count(),
            tally.writes(),
            tally.write_bytes(),
            tally.write_bytes() as f64 / committed as f64,
            tally.reads(),
            tally.read_bytes(),
        );
        Ok(())
    })
}
