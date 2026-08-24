//! The tree, written and read through a sealed store.
//!
//! `tests/tree.rs` seals node buffers by hand to pin the sealing layer's
//! properties. These drive the real path: a `ContentAddressedStorage` with a
//! `NodeSealer` attached, with the tree above it untouched and unaware.

use std::sync::Arc;

use dialog_common::Blake3Hash;
use dialog_keyring::{LocalKeyring, NodeSealer};
use dialog_search_tree::{ContentAddressedStorage, Delta, PersistentTree};
use dialog_storage::{MemoryStorageBackend, StorageBackend, StorageSource};
use futures_util::StreamExt;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The store the tree writes through.
type Backend = MemoryStorageBackend<Blake3Hash, Vec<u8>>;

/// How many entries to write. Enough to force index levels above the leaves.
const ENTRIES: u32 = 512;

/// A recognisable run of bytes inside every value, so a test can check the
/// backend never holds it.
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

/// Build a tree through `storage`, returning it and every node identity the
/// tree assigned.
async fn build(storage: &mut ContentAddressedStorage<Backend>) -> PersistentTree<[u8; 4], Vec<u8>> {
    let mut delta = Delta::zero();
    let mut transient = PersistentTree::<[u8; 4], Vec<u8>>::empty().edit();

    for index in 0..ENTRIES {
        transient = transient
            .insert(index.to_be_bytes(), value(index), storage)
            .await
            .expect("insert");
    }
    let tree = transient.persist(&mut delta).expect("persist");

    for (_, buffer) in delta.flush() {
        storage
            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
            .await
            .expect("store");
    }

    tree
}

#[dialog_common::test]
async fn a_tree_written_through_a_sealed_store_reads_back() {
    let mut storage =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);
    assert!(storage.is_sealed());

    let tree = build(&mut storage).await;

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
    let mut storage =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);
    let tree = build(&mut storage).await;

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
async fn the_backend_never_holds_plaintext() {
    let mut storage =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);
    build(&mut storage).await;

    let mut stored = Box::pin(storage.backend().read());
    let mut nodes = 0usize;
    while let Some(entry) = stored.next().await {
        let (_, bytes) = entry.expect("entry");
        assert!(
            !bytes.windows(CANARY.len()).any(|window| window == CANARY),
            "a value survived into the backend in the clear"
        );
        nodes += 1;
    }

    assert!(nodes > 1, "expected a multi-node tree, got {nodes}");
}

#[dialog_common::test]
async fn the_backend_cannot_be_addressed_by_content() {
    // The point of blinding. A node's identity is `blake3` of its bytes, so
    // anyone who could guess a node's contents could look it up — if the
    // backend filed it under that identity. It does not.
    let mut storage =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);
    let tree = build(&mut storage).await;

    let root = tree.root().clone();

    assert!(
        storage.retrieve(&root).await.expect("retrieve").is_some(),
        "a holder of the key finds the root"
    );
    assert!(
        storage.backend().get(&root).await.expect("get").is_none(),
        "the backend holds nothing under the root's content identity"
    );
}

#[dialog_common::test]
async fn two_replicas_write_byte_identical_stores() {
    // End to end convergence: two replicas that never spoke, building the
    // same tree through independently constructed sealers, produce the same
    // addresses holding the same bytes. Without this a diff between them
    // would report every node as changed.
    let mut here =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);
    let mut there =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);

    let mine = build(&mut here).await;
    let theirs = build(&mut there).await;

    assert_eq!(mine.root(), theirs.root());

    let mut ours: Vec<(Blake3Hash, Vec<u8>)> = collect(here.backend()).await;
    let mut yours: Vec<(Blake3Hash, Vec<u8>)> = collect(there.backend()).await;
    ours.sort();
    yours.sort();

    assert_eq!(ours, yours);
}

#[dialog_common::test]
async fn another_space_cannot_read_the_tree() {
    let mut storage =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);
    let tree = build(&mut storage).await;

    // Same backend, a sealer for a different space.
    let stranger =
        ContentAddressedStorage::with_cipher(storage.backend().clone(), sealer([9u8; 32]).await);

    // The blinded address does not even resolve, let alone open.
    assert!(
        stranger
            .retrieve(tree.root())
            .await
            .expect("retrieve")
            .is_none()
    );
}

#[dialog_common::test]
async fn sealing_is_the_only_difference() {
    // The tree's own shape must not depend on whether its buffers are sealed:
    // boundaries come from hashing keys while a node is built, which happens
    // before sealing. Same entries, same root identity, same node count.
    let mut plain = ContentAddressedStorage::new(Backend::default());
    let mut sealed =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);

    let plain_tree = build(&mut plain).await;
    let sealed_tree = build(&mut sealed).await;

    assert_eq!(plain_tree.root(), sealed_tree.root());
    assert_eq!(
        collect(plain.backend()).await.len(),
        collect(sealed.backend()).await.len()
    );
}

#[dialog_common::test]
async fn sealing_costs_a_fixed_header_per_node() {
    // Quantifies the storage overhead: a 45-byte header and a 16-byte tag on
    // every node, and nothing proportional to node size.
    let mut plain = ContentAddressedStorage::new(Backend::default());
    let mut sealed =
        ContentAddressedStorage::with_cipher(Backend::default(), sealer([7u8; 32]).await);
    build(&mut plain).await;
    build(&mut sealed).await;

    let plain_nodes = collect(plain.backend()).await;
    let sealed_nodes = collect(sealed.backend()).await;

    let plain_bytes: usize = plain_nodes.iter().map(|(_, bytes)| bytes.len()).sum();
    let sealed_bytes: usize = sealed_nodes.iter().map(|(_, bytes)| bytes.len()).sum();

    assert_eq!(
        sealed_bytes - plain_bytes,
        61 * plain_nodes.len(),
        "expected exactly 61 bytes of overhead per node"
    );

    // Printed rather than asserted: node sizes are a property of the tree's
    // shaping, not of sealing, and pinning them here would make this test
    // fail for reasons that have nothing to do with encryption.
    println!(
        "{ENTRIES} entries: {} nodes, {} plain bytes ({} avg), {} sealed bytes (+{:.2}%)",
        plain_nodes.len(),
        plain_bytes,
        plain_bytes / plain_nodes.len(),
        sealed_bytes,
        (sealed_bytes as f64 / plain_bytes as f64 - 1.0) * 100.0
    );
}

/// Everything a backend holds.
async fn collect(backend: &Backend) -> Vec<(Blake3Hash, Vec<u8>)> {
    let mut entries = Vec::new();
    let mut stream = Box::pin(backend.read());
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("entry"));
    }
    entries
}
