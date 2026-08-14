//! History independence, in-tree: the same fact log must canonicalize to
//! the same root no matter how its transactions were grouped into commits.
//!
//! This is the campaign's convergence oracle promoted from a manually-run
//! example (`converge_check`) to a test that runs with the suite. Every
//! write-path change this repository takes — buffered enqueue structure,
//! flush policy, edit fast paths, bulk plants — is obligated to preserve
//! this property, and several near-misses were only caught because the
//! example happened to be run by hand. The default scale keeps the test in
//! CI budget; set `DIALOG_CONVERGE_TXNS` to sweep larger logs (the manual
//! example remains for the 10k-scale runs).
//!
//! Two distinct failures are distinguished:
//! - different KEY DIGESTS mean the groupings disagree about the fact set
//!   itself (a data bug, e.g. batched supersession dropping a write);
//! - same digests but different roots mean the same facts settled into
//!   different shapes (a history-independence break).

#![cfg(not(target_arch = "wasm32"))]

use anyhow::Result;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{ArtifactStoreMut as _, Artifacts, Datum, IndexRoot, Key, State};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_common::Blake3Hash as NodeHash;
use dialog_search_tree::{
    ArchivedNodeBody, Buffer as TreeBuffer, ContentAddressedStorage, PersistentNode, PersistentTree,
};
use dialog_storage::{
    Blake3Hash, CborEncoder, Encoder as _, MemoryStorageBackend, StorageBackend as _,
};
use futures_util::stream;

type TreeNode = PersistentNode<Key, State<Datum>>;

/// Replays `log` committing every `group` transactions, canonicalizes, and
/// returns the canonical revision hash plus a digest of every key + value in
/// tree order.
async fn replay(log: &SeLog, group: usize) -> Result<(Blake3Hash, Blake3Hash, usize)> {
    let backend = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
    let mut store = Artifacts::anonymous(backend.clone()).await?;

    let mut pending = Vec::new();
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

    // Walk the canonical tree, digesting keys and values in order.
    let bytes = backend
        .get(&revision)
        .await?
        .ok_or_else(|| anyhow::anyhow!("revision block missing"))?;
    let root: IndexRoot = CborEncoder.decode(&bytes).await?;

    // The canonical-form validator localizes any break to a node before the
    // root comparison reports it as an opaque hash difference: the stored
    // tree must be fully flushed and shaped exactly as the canonical
    // constructor shapes its entry set.
    let tree: PersistentTree<Key, State<Datum>> =
        PersistentTree::from_hash_with_cache(NodeHash::from(*root.index()), Default::default());
    let divergences = tree
        .canonical_divergences(&ContentAddressedStorage::new(TreeStorageBridge(
            backend.clone(),
        )))
        .await?;
    assert_eq!(
        divergences,
        Vec::<String>::new(),
        "group={group}: canonical tree failed canonical-form validation"
    );

    let mut stack: Vec<Blake3Hash> = vec![*root.index()];
    let mut keyroll: Vec<u8> = Vec::new();
    let mut entries = 0usize;
    while let Some(hash) = stack.pop() {
        let bytes = backend
            .get(&hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("reachable node missing"))?;
        let node = TreeNode::try_from(TreeBuffer::from(bytes))?;
        match node.body() {
            ArchivedNodeBody::Index(index) => {
                for at in (0..index.len()).rev() {
                    stack.push(*index.hash_at(at)?.as_bytes());
                }
            }
            ArchivedNodeBody::Segment(segment) => {
                let mut keys = segment.keys::<Key>()?;
                while let Some((at, key)) = keys.next_key()? {
                    keyroll.extend_from_slice(key);
                    let value: State<Datum> =
                        dialog_search_tree::into_owned(segment.value_at(at)?)?;
                    keyroll.extend_from_slice(format!("{value:?}").as_bytes());
                    entries += 1;
                }
            }
        }
    }
    Ok((
        revision,
        dialog_artifacts::make_reference(&keyroll),
        entries,
    ))
}

fn txn_count() -> usize {
    std::env::var("DIALOG_CONVERGE_TXNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(120)
}

/// Per-transaction commits, five-transaction batches, and one giant commit
/// must all canonicalize to the same root over the same fact log.
#[tokio::test]
async fn it_converges_across_commit_groupings() -> Result<()> {
    let txns = txn_count();
    let log = SeLog::synthetic(txns);

    let (per_txn, per_txn_digest, per_txn_entries) = replay(&log, 1).await?;
    let (by_five, by_five_digest, by_five_entries) = replay(&log, 5).await?;
    let (single, single_digest, single_entries) = replay(&log, usize::MAX).await?;

    assert_eq!(
        per_txn_digest, by_five_digest,
        "per-txn and by-five groupings disagree on the FACT SET (data bug)"
    );
    assert_eq!(
        per_txn_digest, single_digest,
        "per-txn and single-commit groupings disagree on the FACT SET (data bug)"
    );
    assert_eq!(per_txn_entries, by_five_entries);
    assert_eq!(per_txn_entries, single_entries);

    assert_eq!(
        per_txn, by_five,
        "same facts, different canonical roots between per-txn and by-five \
         groupings: history independence is broken"
    );
    assert_eq!(
        per_txn, single,
        "same facts, different canonical roots between per-txn and \
         single-commit groupings: history independence is broken"
    );
    Ok(())
}

/// Canonicalizing twice must be a fixpoint: the second canonicalize (with
/// no interleaved writes) publishes the same revision.
#[tokio::test]
async fn it_reaches_a_canonical_fixpoint() -> Result<()> {
    let log = SeLog::synthetic(40);
    let backend = MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default();
    let mut store = Artifacts::anonymous(backend).await?;
    for commit in &log.transactions {
        store.commit(stream::iter(se_instructions(commit)?)).await?;
    }
    let first = store.canonicalize().await?;
    let second = store.canonicalize().await?;
    assert_eq!(first, second, "canonicalize must be idempotent");
    Ok(())
}
