//! History-independence check on the real workload: the same SE log
//! replayed under different commit groupings must canonicalize to the
//! same root.
//!
//! The pacing-ramp prototype (`DIALOG_TREE_PACING_RAMP`) makes cut
//! decisions read frame-prefix weight — outcome-dependent context — so
//! its convergence rests on the edit path's rightward fusion re-deciding
//! across every boundary it moves. Adversarial unit fixtures already
//! show residue (four order-convergence tests fail with the ramp on);
//! this measures whether the REAL workload hits it: per-transaction
//! commits vs 5-transaction batches vs one giant commit, canonicalized
//! and compared.
//!
//! ```sh
//! DIALOG_TREE_PACING_RAMP=200 cargo run --release -p dialog-baseline \
//!   --example converge_check -- 10000
//! ```

use dialog_artifacts::{
    ArtifactStoreMut as _, Artifacts, Datum, IndexRoot, Instruction, Key, State,
};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_search_tree::{ArchivedNodeBody, Buffer as TreeBuffer, PersistentNode};
use dialog_storage::{
    Blake3Hash, CborEncoder, Encoder as _, MemoryStorageBackend, StorageBackend as _,
};
use futures_util::stream;

type TreeNode = PersistentNode<Key, State<Datum>>;

/// Canonical root plus a digest over the tree's keys in order — the digest
/// separates "different fact set" (a data bug in batched supersession)
/// from "same facts, different shape" (a history-independence break).
async fn replay_grouped(
    log: &SeLog,
    group: usize,
) -> anyhow::Result<(Blake3Hash, usize, Blake3Hash)> {
    let backend = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
    let mut store = Artifacts::anonymous(backend.clone()).await?;
    let mut pending: Vec<Instruction> = Vec::new();
    for (at, commit) in log.transactions.iter().enumerate() {
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
    let revision = store.canonicalize().await?;

    let bytes = backend
        .get(&revision)
        .await?
        .ok_or_else(|| anyhow::anyhow!("revision block missing"))?;
    let root: IndexRoot = CborEncoder.decode(&bytes).await?;
    let mut stack = vec![*root.index()];
    let mut keyroll: Vec<u8> = Vec::new();
    let mut entries = 0usize;
    while let Some(hash) = stack.pop() {
        let Some(bytes) = backend.get(&hash).await? else {
            anyhow::bail!("reachable node missing");
        };
        let node = TreeNode::new(TreeBuffer::from(bytes));
        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                for at in (0..index.len()).rev() {
                    stack.push(*index.hash_at(at)?.as_bytes());
                }
            }
            ArchivedNodeBody::Segment(segment) => {
                let mut keys = segment.keys::<Key>()?;
                while let Some((_, key)) = keys.next_key()? {
                    keyroll.extend_from_slice(key);
                    entries += 1;
                }
            }
        }
    }
    let digest = *TreeBuffer::from(keyroll).blake3_hash().as_bytes();
    Ok((revision, entries, digest))
}

fn hex(hash: &Blake3Hash) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(10000);
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let per_txn = replay_grouped(&log, 1).await?;
        let by_five = replay_grouped(&log, 5).await?;
        let single = replay_grouped(&log, usize::MAX).await?;
        println!(
            "txns={} facts={}\n  per-txn : root {} / {} entries, key digest {}\n  by-five : root {} / {} entries, key digest {}\n  single  : root {} / {} entries, key digest {}",
            log.transactions.len(),
            log.fact_count(),
            hex(&per_txn.0),
            per_txn.1,
            hex(&per_txn.2),
            hex(&by_five.0),
            by_five.1,
            hex(&by_five.2),
            hex(&single.0),
            single.1,
            hex(&single.2),
        );
        if per_txn.0 == by_five.0 && by_five.0 == single.0 {
            println!("CONVERGED: all groupings canonicalize to the same root");
        } else if per_txn.2 == by_five.2 && by_five.2 == single.2 {
            println!("DIVERGED (shape only): same keys, different canonical roots");
        } else {
            println!("DIVERGED (data): the stored fact sets themselves differ");
        }
        Ok(())
    })
}
