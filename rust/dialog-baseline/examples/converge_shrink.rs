//! Delta-debugging shrinker for the convergence break: reduces an SE
//! transaction prefix to a minimal subsequence on which two commit
//! groupings still canonicalize to different roots, then dumps the
//! surviving instructions so the divergence can be pinned as a
//! deterministic unit test.
//!
//! ```sh
//! DIALOG_SE_CSV=... cargo run --release -p dialog-baseline \
//!   --example converge_shrink -- 200
//! ```

use dialog_artifacts::{ArtifactStoreMut as _, Artifacts, Instruction};
use dialog_baseline::se::{SeFact, SeLog, se_instructions};
use dialog_storage::{Blake3Hash, MemoryStorageBackend};
use futures_util::stream;

async fn replay(transactions: &[Vec<SeFact>], group: usize) -> anyhow::Result<Blake3Hash> {
    let backend = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
    let mut store = Artifacts::anonymous(backend).await?;
    let mut pending: Vec<Instruction> = Vec::new();
    for (at, commit) in transactions.iter().enumerate() {
        pending.extend(se_instructions(commit)?);
        if (at + 1) % group == 0 {
            store
                .commit(stream::iter(std::mem::take(&mut pending)))
                .await?;
        }
    }
    if !pending.is_empty() {
        store.commit(stream::iter(pending)).await?;
    }
    Ok(store.canonicalize().await?)
}

async fn diverges(transactions: &[Vec<SeFact>]) -> anyhow::Result<bool> {
    Ok(replay(transactions, 1).await? != replay(transactions, 5).await?)
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
