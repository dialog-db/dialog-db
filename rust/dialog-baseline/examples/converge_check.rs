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
use dialog_search_tree::{
    ArchivedNodeBody, Buffer as TreeBuffer, Distribution as _, Manifest, PersistentNode, Value as _,
};
use dialog_storage::{
    Blake3Hash, CborEncoder, Encoder as _, MemoryStorageBackend, StorageBackend as _,
};
use futures_util::stream;

type TreeNode = PersistentNode<Key, State<Datum>>;

/// Canonical root plus a digest over the tree's keys in order — the digest
/// separates "different fact set" (a data bug in batched supersession)
/// from "same facts, different shape" (a history-independence break).
/// Walks the tree under `revision` and returns every stored-leaf invariant
/// violation: a non-final leaf whose terminal coin is unfunded, or an
/// interior entry whose coin cuts. The canonical-edit machinery must keep
/// stored leaves free of both at every step.
async fn leaf_violations(
    backend: &MemoryStorageBackend<Blake3Hash, Vec<u8>>,
    revision: &Blake3Hash,
) -> anyhow::Result<Vec<String>> {
    let Some(bytes) = backend.get(revision).await? else {
        return Ok(Vec::new());
    };
    let root: IndexRoot = CborEncoder.decode(&bytes).await?;
    let mut stack: Vec<Blake3Hash> = vec![*root.index()];
    let mut ordered: Vec<(Vec<u8>, bool, usize)> = Vec::new();
    let mut forced_links = 0usize;
    while let Some(hash) = stack.pop() {
        let Some(bytes) = backend.get(&hash).await? else {
            anyhow::bail!("reachable node missing");
        };
        let node = TreeNode::try_from(TreeBuffer::from(bytes))?;
        match node.body() {
            ArchivedNodeBody::Index(index) => {
                for at in (0..index.len()).rev() {
                    if index.separator(at)?.len() > Manifest::default().max_separator as usize {
                        forced_links += 1;
                    }
                    stack.push(*index.hash_at(at)?.as_bytes());
                }
            }
            ArchivedNodeBody::Segment(segment) => {
                let mut keys = segment.keys::<Key>()?;
                let mut leaf: Vec<(Vec<u8>, bool)> = Vec::new();
                while let Some((at, key)) = keys.next_key()? {
                    let value: State<Datum> =
                        dialog_search_tree::into_owned(segment.value_at(at)?)?;
                    let charge = key.len()
                        + value.payload_weight()
                        + dialog_search_tree::ENTRY_ENCODING_OVERHEAD;
                    let cut =
                        dialog_search_tree::Geometric::leaf_cut(key, charge, &Manifest::default());
                    leaf.push((key.to_vec(), cut));
                }
                let len = leaf.len();
                for (at, (key, cut)) in leaf.into_iter().enumerate() {
                    ordered.push((key, cut, if at + 1 == len { 1 } else { 0 }));
                }
            }
        }
    }
    let mut violations = Vec::new();
    if forced_links > 0 {
        violations.push(format!("forced-links {forced_links}"));
    }
    for (at, (key, cut, terminal)) in ordered.iter().enumerate() {
        let global_last = at + 1 == ordered.len();
        let hexkey: String = key.iter().take(24).map(|b| format!("{b:02x}")).collect();
        if *terminal == 1 && !cut && !global_last {
            violations.push(format!("open-terminal {hexkey}"));
        }
        if *terminal == 0 && *cut {
            violations.push(format!("missing-cut {hexkey}"));
        }
    }
    Ok(violations)
}

async fn replay_grouped(
    log: &SeLog,
    group: usize,
) -> anyhow::Result<(Blake3Hash, usize, Blake3Hash)> {
    let backend = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
    let mut store = Artifacts::anonymous(backend.clone()).await?;
    let mut pending: Vec<Instruction> = Vec::new();
    let scan = std::env::var("DIALOG_CONVERGE_SCAN").is_ok();
    let mut last_violations: Vec<String> = Vec::new();
    for (at, commit) in log.transactions.iter().enumerate() {
        pending.extend(se_instructions(commit)?);
        if (at + 1) % group == 0 {
            store
                .commit(stream::iter(std::mem::take(&mut pending)))
                .await?;
            if scan {
                let revision = store
                    .revision()
                    .await?
                    .expect("the store has commits, so it has a revision");
                let violations = leaf_violations(&backend, &revision).await?;
                if violations != last_violations {
                    println!("  SCAN group={group} txn={at}: {violations:?}");
                    last_violations = violations;
                }
            }
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
    let mut stack: Vec<(Blake3Hash, Vec<u8>)> = vec![(*root.index(), Vec::new())];
    let mut keyroll: Vec<u8> = Vec::new();
    let mut entries = 0usize;
    let diff = std::env::var("DIALOG_CONVERGE_DIFF").is_ok();
    while let Some((hash, separator)) = stack.pop() {
        let Some(bytes) = backend.get(&hash).await? else {
            anyhow::bail!("reachable node missing");
        };
        let size = bytes.len();
        let node = TreeNode::try_from(TreeBuffer::from(bytes))?;
        match node.body() {
            ArchivedNodeBody::Index(index) => {
                for at in (0..index.len()).rev() {
                    stack.push((*index.hash_at(at)?.as_bytes(), index.separator(at)?));
                }
            }
            ArchivedNodeBody::Segment(segment) => {
                let mut leaf_entries = 0usize;
                let mut first: Option<Vec<u8>> = None;
                let mut coins: Vec<(Vec<u8>, bool)> = Vec::new();
                let mut keys = segment.keys::<Key>()?;
                while let Some((at, key)) = keys.next_key()? {
                    if first.is_none() {
                        first = Some(key.to_vec());
                    }
                    keyroll.extend_from_slice(key);
                    // Values ride the digest too: a same-key different-value
                    // divergence changes the coin's weight charge and is a
                    // DATA bug, which the key-only digest misclassified as
                    // shape-only.
                    let value: State<Datum> =
                        dialog_search_tree::into_owned(segment.value_at(at)?)?;
                    keyroll.extend_from_slice(format!("{value:?}").as_bytes());
                    if diff {
                        // The production coin charge: key bytes + payload
                        // weight + per-entry encoding overhead (bank-free —
                        // the veto never fires on this workload).
                        let charge = key.len()
                            + value.payload_weight()
                            + dialog_search_tree::ENTRY_ENCODING_OVERHEAD;
                        let cut = dialog_search_tree::Geometric::leaf_cut(
                            key,
                            charge,
                            &Manifest::default(),
                        );
                        coins.push((key.to_vec(), cut));
                    }
                    entries += 1;
                    leaf_entries += 1;
                }
                if diff {
                    // Canonicality census: with bank-free per-seam coins the
                    // canonical partition is directly computable — every
                    // stored leaf must end at a cutting entry (unless it is
                    // the global last leaf) and contain no interior cutting
                    // entry. Violations name the arm holding a stale shape.
                    for (at, (key, cut)) in coins.iter().enumerate() {
                        let terminal = at + 1 == coins.len();
                        if *cut && !terminal {
                            println!(
                                "  MISSING-CUT group={group} at={at}/{leaf_entries} key={}",
                                key.iter()
                                    .take(16)
                                    .map(|byte| format!("{byte:02x}"))
                                    .collect::<String>()
                            );
                        }
                        if terminal && !*cut {
                            println!(
                                "  OPEN-TERMINAL group={group} entries={leaf_entries} key={}",
                                key.iter()
                                    .take(16)
                                    .map(|byte| format!("{byte:02x}"))
                                    .collect::<String>()
                            );
                        }
                    }
                }
                if diff {
                    // The leaf partition, one line per leaf: the stored
                    // separator length marks forced pieces (longer than the
                    // max_separator bound is the self-identifying forced
                    // seam), the first-key prefix aligns the arms.
                    println!(
                        "  LEAF group={group} sep_len={} entries={leaf_entries} bytes={size} first={}",
                        separator.len(),
                        first
                            .as_deref()
                            .map(|key| key
                                .iter()
                                .take(12)
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>())
                            .unwrap_or_default(),
                    );
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
        if let Ok(only) = std::env::var("DIALOG_CONVERGE_ONLY") {
            let group: usize = only.parse().unwrap_or(5);
            let arm = replay_grouped(&log, group).await?;
            println!("group={group}: root {}", hex(&arm.0));
            return Ok(());
        }
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
