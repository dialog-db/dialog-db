//! The tree, written and read through a sealed store.
//!
//! `tests/tree.rs` seals node buffers by hand to pin the sealing layer's
//! properties. These drive the real path: the tree loads its nodes through a
//! [`SealedBlocks`] provider and its writes are sealed into one, with the tree
//! itself untouched and unaware.

use std::sync::Arc;

use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSync};
use dialog_keyring::{LocalKeyring, NodeSealer, SealedBlocks};
use dialog_search_tree::{Delta, LoadBlock, MemoryBlocks, PersistentTree};
use futures_util::StreamExt;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// How many entries to write. Enough to force index levels above the leaves.
const ENTRIES: u32 = 512;

/// A recognisable run of bytes inside every value, so a test can check the
/// store never holds it.
const CANARY: &[u8] = b"canary-plaintext-marker";

/// A sealer over a keyring built from fixed material.
async fn sealer(secret: [u8; 32]) -> Arc<NodeSealer> {
    let keyring = LocalKeyring::genesis(secret, [1u8; 32]);
    Arc::new(NodeSealer::resolve(&keyring).await.expect("resolve"))
}

/// The value stored at `index`, carrying the canary.
fn value(index: u32) -> Vec<u8> {
    let mut value = CANARY.to_vec();
    value.extend_from_slice(&index.to_be_bytes());
    value.resize(64, index as u8);
    value
}

/// Build a tree in one edit, returning it and every block its persist
/// staged. Nothing is loaded while building from empty, so `env` is only
/// what the edit would read through.
async fn build<Env>(env: &Env) -> (PersistentTree<[u8; 4], Vec<u8>>, Vec<Buffer>)
where
    Env: Provider<LoadBlock> + ConditionalSync,
{
    let mut delta = Delta::zero();
    let mut transient = PersistentTree::<[u8; 4], Vec<u8>>::empty().edit();

    for index in 0..ENTRIES {
        transient = transient
            .insert(index.to_be_bytes(), value(index), env)
            .await
            .expect("insert");
    }
    let tree = transient.persist(&mut delta).expect("persist");
    let blocks = delta.flush().map(|(_, block)| block).collect();
    (tree, blocks)
}

/// Build a tree and seal every block it writes into `storage`.
async fn build_sealed(storage: &SealedBlocks) -> PersistentTree<[u8; 4], Vec<u8>> {
    let (tree, blocks) = build(storage).await;
    for block in &blocks {
        storage.store(block).expect("seal");
    }
    tree
}

#[dialog_common::test]
async fn a_tree_written_through_a_sealed_store_reads_back() {
    let storage = SealedBlocks::new(sealer([7u8; 32]).await);
    let tree = build_sealed(&storage).await;

    for index in 0..ENTRIES {
        assert_eq!(
            tree.get(&index.to_be_bytes(), &storage).await.expect("get"),
            Some(value(index)),
            "entry {index}"
        );
    }
}

#[dialog_common::test]
async fn a_sealed_scan_yields_every_entry_in_order() {
    let storage = SealedBlocks::new(sealer([7u8; 32]).await);
    let tree = build_sealed(&storage).await;

    let mut stream = Box::pin(tree.stream(&storage));
    let mut seen = 0u32;
    while let Some(entry) = stream.next().await {
        let entry = entry.expect("entry");
        assert_eq!(entry.key, seen.to_be_bytes());
        seen += 1;
    }

    assert_eq!(seen, ENTRIES);
}

#[dialog_common::test]
async fn the_store_never_holds_plaintext() {
    let storage = SealedBlocks::new(sealer([7u8; 32]).await);
    build_sealed(&storage).await;

    let stored = storage.stored();
    for (_, bytes) in &stored {
        assert!(
            !bytes.windows(CANARY.len()).any(|window| window == CANARY),
            "a value survived into the store in the clear"
        );
    }

    assert!(
        stored.len() > 1,
        "expected a multi-node tree, got {}",
        stored.len()
    );
}

#[dialog_common::test]
async fn the_store_cannot_be_addressed_by_content() {
    // The point of blinding. A node's identity is `blake3` of its bytes, so
    // anyone who could guess a node's contents could look it up — if the
    // store filed it under that identity. It does not.
    let storage = SealedBlocks::new(sealer([7u8; 32]).await);
    let tree = build_sealed(&storage).await;

    let root = tree.root().clone();

    assert!(
        LoadBlock::new(root.clone())
            .perform(&storage)
            .await
            .expect("load")
            .is_some(),
        "a holder of the key finds the root"
    );
    assert!(
        storage.raw(&root).is_none(),
        "the store holds nothing under the root's content identity"
    );
}

#[dialog_common::test]
async fn two_replicas_write_byte_identical_stores() {
    // End to end convergence: two replicas that never spoke, building the
    // same tree through independently constructed sealers, produce the same
    // addresses holding the same bytes. Without this a diff between them
    // would report every node as changed.
    let here = SealedBlocks::new(sealer([7u8; 32]).await);
    let there = SealedBlocks::new(sealer([7u8; 32]).await);

    let mine = build_sealed(&here).await;
    let theirs = build_sealed(&there).await;

    assert_eq!(mine.root(), theirs.root());

    let mut ours: Vec<(Blake3Hash, Vec<u8>)> = here.stored();
    let mut yours: Vec<(Blake3Hash, Vec<u8>)> = there.stored();
    ours.sort();
    yours.sort();

    assert_eq!(ours, yours);
}

#[dialog_common::test]
async fn another_space_cannot_read_the_tree() {
    let storage = SealedBlocks::new(sealer([7u8; 32]).await);
    let tree = build_sealed(&storage).await;

    // The same stored blocks, read through a sealer for a different space.
    let stranger = storage.reading_with(sealer([9u8; 32]).await);

    // The blinded address does not even resolve, let alone open.
    assert!(
        LoadBlock::new(tree.root().clone())
            .perform(&stranger)
            .await
            .expect("load")
            .is_none()
    );
}

#[dialog_common::test]
async fn sealing_is_the_only_difference() {
    // The tree's own shape must not depend on whether its buffers are sealed:
    // boundaries come from hashing keys while a node is built, which happens
    // before sealing. Same entries, same root identity, same node count.
    let plain = MemoryBlocks::new();
    let sealed = SealedBlocks::new(sealer([7u8; 32]).await);

    let (plain_tree, blocks) = build(&plain).await;
    for block in blocks {
        plain.store(block);
    }
    let sealed_tree = build_sealed(&sealed).await;

    assert_eq!(plain_tree.root(), sealed_tree.root());
    assert_eq!(plain.len(), sealed.len());
}

#[dialog_common::test]
async fn sealing_costs_a_fixed_header_per_node() {
    // Quantifies the storage overhead: a 45-byte header and a 16-byte tag on
    // every node, and nothing proportional to node size.
    let sealed = SealedBlocks::new(sealer([7u8; 32]).await);
    let (_, blocks) = build(&sealed).await;
    for block in &blocks {
        sealed.store(block).expect("seal");
    }

    let plain_bytes: usize = blocks.iter().map(|block| block.as_ref().len()).sum();
    let sealed_bytes: usize = sealed.stored().iter().map(|(_, bytes)| bytes.len()).sum();

    assert_eq!(
        sealed_bytes - plain_bytes,
        61 * blocks.len(),
        "expected exactly 61 bytes of overhead per node"
    );

    // Printed rather than asserted: node sizes are a property of the tree's
    // shaping, not of sealing, and pinning them here would make this test
    // fail for reasons that have nothing to do with encryption.
    println!(
        "{ENTRIES} entries: {} nodes, {} plain bytes ({} avg), {} sealed bytes (+{:.2}%)",
        blocks.len(),
        plain_bytes,
        plain_bytes / blocks.len(),
        sealed_bytes,
        (sealed_bytes as f64 / plain_bytes as f64 - 1.0) * 100.0
    );
}
