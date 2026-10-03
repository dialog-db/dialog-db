//! Shaping-hash accounting over the Stack Exchange replay: how many key and
//! separator hashes the tree asks for and computes while committing, and how
//! many blocks the commits read back from the archive.
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example hash_audit -- 2000
//! ```

use dialog_baseline::metered::Tally;
use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::{SeLog, se_instructions};
use futures_util::stream;

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(2000);
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let tally = Tally::default();
        let repo = DialogRepo::metered(tally.clone()).await?;
        let started = std::time::Instant::now();
        for commit in &log.transactions {
            repo.branch()
                .commit(stream::iter(se_instructions(commit)?))
                .perform(repo.operator())
                .await?;
        }
        let elapsed = started.elapsed();
        println!(
            "commits={} facts={} elapsed={elapsed:?} reads={} read_bytes={} writes={} write_bytes={}",
            log.transactions.len(),
            log.fact_count(),
            tally.reads(),
            tally.read_bytes(),
            tally.writes(),
            tally.write_bytes(),
        );
        println!("{}", dialog_search_tree::audit::report());
        Ok(())
    })
}
