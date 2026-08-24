//! Sealing composed with the shapes the search tree actually produces.
//!
//! The unit tests pin the sealing layer's properties against synthetic bytes.
//! These pin the two claims that only hold if it composes with a real prolly
//! tree: that sealing does not disturb how the tree chunks itself, and that
//! two replicas which independently build the same tree still agree on every
//! address.
//!
//! What is deliberately *not* done here is threading the sealed address back
//! into each `Link`, so that a parent points at its children's ciphertext
//! rather than their plaintext. That is the invasive half of the change and it
//! belongs in the search tree itself. These tests establish that the layer
//! underneath it behaves, which is what has to be true first.

use dialog_common::Blake3Hash;
use dialog_keyring::{KeyringExt, LocalKeyring, Sealed};
use dialog_search_tree::{ContentAddressedStorage, Delta, PersistentTree};
use dialog_storage::{MemoryStorageBackend, StorageBackend};

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// How many entries to write. Enough that the tree is several nodes deep
/// rather than a single leaf, so the boundary claim is actually exercised.
const ENTRIES: u32 = 512;

/// Build a tree over a deterministic set of entries and return its node
/// buffers, keyed by the plaintext hash the tree assigned them.
///
/// Nothing here knows about encryption: this is the tree behaving exactly as
/// it does today, and it is the input the sealing layer has to preserve.
async fn node_buffers() -> Vec<(Blake3Hash, Vec<u8>)> {
    let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
    let mut delta = Delta::zero();
    let mut transient = PersistentTree::<[u8; 4], Vec<u8>>::empty().edit();

    for index in 0..ENTRIES {
        transient = transient
            .insert(index.to_be_bytes(), vec![index as u8; 48], &storage)
            .await
            .expect("insert");
    }
    transient.persist(&mut delta).expect("persist");

    let mut buffers: Vec<_> = delta
        .flush()
        .map(|(hash, buffer)| (hash, buffer.as_ref().to_vec()))
        .collect();
    buffers.sort_by(|(left, _), (right, _)| left.cmp(right));
    buffers
}

#[dialog_common::test]
async fn node_buffers_survive_a_round_trip_through_storage() {
    let keyring = LocalKeyring::genesis([7u8; 32], [1u8; 32]);
    let buffers = node_buffers().await;
    assert!(
        buffers.len() > 1,
        "expected a multi-node tree, got {}",
        buffers.len()
    );

    // Seal every node and store it under the address of its ciphertext. The
    // store never sees a plaintext hash: addressing by one would let anyone
    // who could guess a node's contents confirm the guess against the store.
    let mut store = MemoryStorageBackend::default();
    let mut addresses = Vec::new();
    for (_, plain) in &buffers {
        let sealed = keyring.seal(plain).await.expect("seal");
        let address = sealed.address();
        store
            .set(address.clone(), sealed.to_bytes())
            .await
            .expect("store");
        addresses.push(address);
    }

    for (address, (_, plain)) in addresses.iter().zip(&buffers) {
        let bytes = store.get(address).await.expect("read").expect("present");
        let sealed = Sealed::from_bytes(&bytes).expect("decode");
        assert_eq!(&keyring.open(&sealed).await.expect("open"), plain);
    }
}

#[dialog_common::test]
async fn two_replicas_seal_the_same_tree_to_the_same_addresses() {
    // The property the whole design rests on. Chunk boundaries come from
    // hashing keys while a node is built, which happens strictly before the
    // buffer is sealed — so encryption cannot move a boundary, and two
    // replicas that never spoke still produce byte-identical blobs at
    // identical addresses. Without this, a diff between replicas would report
    // every node as changed.
    let here = LocalKeyring::genesis([7u8; 32], [1u8; 32]);
    let there = LocalKeyring::genesis([7u8; 32], [1u8; 32]);

    let mine = node_buffers().await;
    let theirs = node_buffers().await;

    assert_eq!(
        mine.iter().map(|(hash, _)| hash).collect::<Vec<_>>(),
        theirs.iter().map(|(hash, _)| hash).collect::<Vec<_>>(),
        "the tree itself must be deterministic before encryption can be"
    );

    for ((_, plain_here), (_, plain_there)) in mine.iter().zip(&theirs) {
        let sealed_here = here.seal(plain_here).await.expect("seal");
        let sealed_there = there.seal(plain_there).await.expect("seal");
        assert_eq!(sealed_here.address(), sealed_there.address());
    }
}

#[dialog_common::test]
async fn rotation_moves_every_address_without_touching_the_tree() {
    // Quantifies what the note calls the cost that is easy to miss. The tree
    // is byte-for-byte the same; every sealed address is different. A diff
    // across this boundary prunes nothing and transfers the lot.
    //
    // Nothing is lost — the old blobs are still there and still readable —
    // but this is why rotation is `OnDemand` in production and aggressive
    // only in tests.
    let mut keyring = LocalKeyring::genesis([7u8; 32], [1u8; 32]);
    let buffers = node_buffers().await;

    let mut before = Vec::new();
    for (_, plain) in &buffers {
        before.push(keyring.seal(plain).await.expect("seal"));
    }

    keyring.rotate_with([2u8; 32]);

    let mut after = Vec::new();
    for (_, plain) in &buffers {
        after.push(keyring.seal(plain).await.expect("seal"));
    }

    let shared = before
        .iter()
        .filter(|sealed| {
            after
                .iter()
                .any(|other| other.address() == sealed.address())
        })
        .count();
    assert_eq!(shared, 0, "an epoch boundary shares no addresses");

    // Both generations remain readable from the one keyring.
    for (sealed, (_, plain)) in before.iter().chain(&after).zip(buffers.iter().cycle()) {
        assert_eq!(&keyring.open(sealed).await.expect("open"), plain);
    }
}
