//! Delta-debugging shrinker for the convergence break: reduces an SE
//! transaction prefix to a minimal subsequence on which one amended chain
//! canonicalized after every link and the same chain canonicalized only
//! after its last reach different trees, then dumps the surviving
//! instructions so the divergence can be pinned as a deterministic unit
//! test.
//!
//! ```sh
//! DIALOG_SE_CSV=... cargo run --release -p dialog-baseline \
//!   --example converge_shrink -- 200
//! ```

use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::{SeFact, SeLog};

/// Whether canonicalizing after every link and only after the last reach
/// different trees over `transactions`. Both chains stage on the same
/// head, so they mint the same version.
async fn diverges(transactions: &[Vec<SeFact>]) -> anyhow::Result<bool> {
    let log = SeLog {
        transactions: transactions.to_vec(),
    };
    let repo = DialogRepo::volatile().await?;
    let every = repo.stage_se(&log, 1).await?;
    let last = repo.stage_se(&log, usize::MAX).await?;
    Ok(every.revision().tree != last.revision().tree)
}

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(200);
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut kept: Vec<Vec<SeFact>> = log.transactions.clone();
        anyhow::ensure!(
            diverges(&kept).await?,
            "the starting prefix does not diverge"
        );

        // ddmin-style: try dropping chunks, halving granularity until
        // single transactions; restart whenever a drop succeeds.
        let mut chunk = kept.len() / 2;
        while chunk >= 1 {
            let mut at = 0;
            let mut shrunk = false;
            while at < kept.len() {
                let mut candidate = kept.clone();
                let end = (at + chunk).min(candidate.len());
                candidate.drain(at..end);
                if !candidate.is_empty() && diverges(&candidate).await? {
                    kept = candidate;
                    shrunk = true;
                } else {
                    at = end;
                }
            }
            if !shrunk {
                chunk /= 2;
            }
            println!("kept {} transactions (chunk {})", kept.len(), chunk);
        }

        println!("minimal diverging sequence: {} transactions", kept.len());
        for (at, txn) in kept.iter().enumerate() {
            for fact in txn {
                println!("  [{at}] {fact:?}");
            }
        }
        Ok(())
    })
}
