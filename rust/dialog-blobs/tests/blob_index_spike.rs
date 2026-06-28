//! Spike: blobs that replicate through the existing tree differential.
//!
//! Self-contained demonstration (dev-dependencies only — it does not change the
//! production crate graph) of the design in `notes/blob-replication.md`. It
//! implements a **content-addressed blob index**: a `dialog-search-tree` with
//! two key namespaces, distinguished by a tag byte:
//!
//! ```text
//! chunk:    CHUNK_TAG ‖ chunk_hash (32)  → chunk bytes
//! manifest: BLOB_TAG  ‖ blob_hash  (32)  → length ‖ [chunk_hash; n]
//! ```
//!
//! A blob is split into chunks; each chunk is stored under *its own* content
//! hash, and the blob's manifest lists the chunk hashes in order. Because a
//! blob is now ordinary tree content, the existing `TreeDifference::novel_nodes`
//! — which yields "exactly the blocks a holder of the source tree is missing" —
//! replicates it with no new machinery.
//!
//! Keying chunks by the *chunk* hash (not by `(blob_hash, index)`) is the
//! crucial detail: identical chunks across different blobs collapse to the same
//! tree entry, so content-defined dedup falls out and the differential ships a
//! shared chunk only once. (An earlier version of this spike keyed chunks by
//! `blob_hash ‖ index`; that embeds the blob hash in every chunk key, so two
//! blobs never shared a leaf even when they shared bytes — no dedup. The test
//! `shared_chunks_deduplicate_across_blobs` exists to pin this property.)
//!
//! Run with: `cargo test -p dialog-blobs --test blob_index_spike`

use anyhow::Result;
use blake3::Hasher;
use dialog_common::Blake3Hash;
use dialog_search_tree::{Buffer, ContentAddressedStorage, Delta, PersistentTree, TreeDifference};
use dialog_storage::MemoryStorageBackend;
use futures_util::StreamExt;

/// Tag byte for chunk entries (`chunk_hash → bytes`).
const CHUNK_TAG: u8 = 0x0c;
/// Tag byte for manifest entries (`blob_hash → manifest`).
const BLOB_TAG: u8 = 0x0b;

/// Chunk size. Deliberately tiny so modest fixtures still produce multi-node
/// trees and exercise the differential. Production would use kilobytes.
const CHUNK_SIZE: usize = 64;

/// Tag ‖ 32-byte hash. Both namespaces share one fixed-width key.
const KEY_LEN: usize = 1 + 32;
type BlobKey = [u8; KEY_LEN];

type BlobTree = PersistentTree<BlobKey, Vec<u8>>;
/// `persist` writes new nodes keyed by their hash into this delta; `flush`
/// drains them as `(hash, buffer)` pairs to store.
type BlobDelta = Delta<Blake3Hash, Buffer>;
type Store = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn tagged_key(tag: u8, hash: &[u8; 32]) -> BlobKey {
    let mut key = [0u8; KEY_LEN];
    key[0] = tag;
    key[1..].copy_from_slice(hash);
    key
}

fn fresh_store() -> Store {
    ContentAddressedStorage::new(MemoryStorageBackend::default())
}

/// Flush every node the last `persist` produced into `storage` so the next edit
/// (and later reads) can resolve them. Mirrors the search-tree crate's tests.
async fn flush(delta: &mut BlobDelta, storage: &mut Store) -> Result<()> {
    for (_, buffer) in delta.flush() {
        storage
            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
            .await?;
    }
    Ok(())
}

async fn insert(
    tree: BlobTree,
    storage: &mut Store,
    delta: &mut BlobDelta,
    key: BlobKey,
    value: Vec<u8>,
) -> Result<BlobTree> {
    let tree = tree.edit().insert(key, value, storage).await?.persist(delta)?;
    flush(delta, storage).await?;
    Ok(tree)
}

/// Chunk `bytes`, store each chunk under its own content hash, and store a
/// manifest (`length ‖ chunk_hashes`) under the blob hash. Returns the blob
/// hash. Re-storing a chunk that already exists is a no-op insert (same key,
/// same value), so shared chunks neither duplicate storage nor create nodes.
async fn put_blob(
    mut tree: BlobTree,
    storage: &mut Store,
    delta: &mut BlobDelta,
    bytes: &[u8],
) -> Result<(BlobTree, [u8; 32])> {
    let blob_hash = hash_bytes(bytes);

    // length (u64 BE) ‖ chunk_hash (32) per chunk.
    let mut manifest = Vec::new();
    manifest.extend_from_slice(&(bytes.len() as u64).to_be_bytes());

    for chunk in bytes.chunks(CHUNK_SIZE) {
        let chunk_hash = hash_bytes(chunk);
        manifest.extend_from_slice(&chunk_hash);
        tree = insert(
            tree,
            storage,
            delta,
            tagged_key(CHUNK_TAG, &chunk_hash),
            chunk.to_vec(),
        )
        .await?;
    }

    tree = insert(tree, storage, delta, tagged_key(BLOB_TAG, &blob_hash), manifest).await?;
    Ok((tree, blob_hash))
}

/// Reconstruct a blob from its manifest and chunks. Works against any storage
/// holding the blob's nodes — including a *remote* hydrated only from the
/// differential.
async fn read_blob(tree: &BlobTree, storage: &Store, blob_hash: &[u8; 32]) -> Result<Option<Vec<u8>>> {
    let manifest = match tree.get(&tagged_key(BLOB_TAG, blob_hash), storage).await? {
        Some(manifest) => manifest,
        None => return Ok(None),
    };
    let length = u64::from_be_bytes(manifest[0..8].try_into().unwrap()) as usize;

    let mut bytes = Vec::with_capacity(length);
    for chunk_hash in manifest[8..].chunks_exact(32) {
        let key = tagged_key(CHUNK_TAG, chunk_hash.try_into().unwrap());
        let chunk = tree
            .get(&key, storage)
            .await?
            .expect("manifest references a chunk that must exist");
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes.len(), length, "reassembled length must match manifest");
    Ok(Some(bytes))
}

/// Stream the novel nodes between `base` and `target` into `remote`, returning
/// how many nodes crossed the wire. This is exactly what `push` does with the
/// index tree (`branch/push.rs`): compute the difference, upload novel nodes.
async fn replicate(
    base: &BlobTree,
    target: &BlobTree,
    local: &Store,
    remote: &mut Store,
) -> Result<usize> {
    let difference = TreeDifference::compute(base, target, local, local).await?;
    let nodes = difference.novel_nodes();
    futures_util::pin_mut!(nodes);
    let mut uploaded = 0;
    while let Some(node) = nodes.next().await {
        let node = node?;
        remote
            .store(node.buffer().as_ref().to_vec(), node.hash())
            .await?;
        uploaded += 1;
    }
    Ok(uploaded)
}

/// A blob written into the index reads back byte-for-byte, including a blob
/// large enough to span several chunks and several tree nodes.
#[tokio::test]
async fn blob_round_trips_through_the_index() -> Result<()> {
    let mut storage = fresh_store();
    let mut delta = Delta::zero();
    let tree = BlobTree::empty();

    // ~10 chunks worth, with a non-chunk-aligned tail.
    let payload: Vec<u8> = (0..(CHUNK_SIZE * 10 + 7) as u32)
        .map(|i| (i % 251) as u8)
        .collect();

    let (tree, hash) = put_blob(tree, &mut storage, &mut delta, &payload).await?;

    let restored = read_blob(&tree, &storage, &hash).await?;
    assert_eq!(restored.as_deref(), Some(payload.as_slice()));

    // Unknown hash → None, not an error.
    assert!(read_blob(&tree, &storage, &[0u8; 32]).await?.is_none());
    Ok(())
}

/// The differential identifies exactly the blocks a remote is missing: after
/// uploading the novel nodes, a remote that started empty can reconstruct and
/// read the blob. This is the property the whole plan rests on.
#[tokio::test]
async fn differential_replicates_a_new_blob() -> Result<()> {
    let mut local = fresh_store();
    let mut delta = Delta::zero();

    let base = BlobTree::empty();
    let payload: Vec<u8> = (0..(CHUNK_SIZE * 5) as u32).map(|i| (i % 97) as u8).collect();
    let (target, hash) = put_blob(base.clone(), &mut local, &mut delta, &payload).await?;

    // A remote that has the (empty) base tree. Upload only the novelty.
    let mut remote = fresh_store();
    let uploaded = replicate(&base, &target, &local, &mut remote).await?;
    assert!(uploaded > 0, "a new blob must produce novel nodes to upload");

    // The remote, knowing only the target root and the uploaded nodes, can
    // reconstruct the blob with no further communication.
    let remote_tree = BlobTree::from_hash(target.root().clone());
    let restored = read_blob(&remote_tree, &remote, &hash).await?;
    assert_eq!(
        restored.as_deref(),
        Some(payload.as_slice()),
        "remote must read the blob purely from differential-uploaded nodes"
    );
    Ok(())
}

/// A blob that shares most of its chunks with an already-replicated blob
/// uploads strictly fewer novel nodes than a fully-disjoint blob of the same
/// size: the shared chunks' leaves stay put and cross the wire zero times.
/// Content-addressing the chunks is what makes this dedup real.
#[tokio::test]
async fn shared_chunks_deduplicate_across_blobs() -> Result<()> {
    let mut local = fresh_store();
    let mut delta = Delta::zero();

    // Blob A: many distinct chunks, enough to span several leaf nodes.
    const N: usize = 200;
    let make = |salt: u8, count: usize| -> Vec<u8> {
        // `count` distinct chunks; each chunk's bytes are unique per (salt, i).
        let mut v = Vec::with_capacity(count * CHUNK_SIZE);
        for i in 0..count {
            for b in 0..CHUNK_SIZE {
                v.push(((i as u32 * 131 + b as u32 + salt as u32 * 977) % 251) as u8);
            }
        }
        v
    };

    let blob_a = make(0, N);
    let base = BlobTree::empty();
    let (after_a, _hash_a) = put_blob(base.clone(), &mut local, &mut delta, &blob_a).await?;

    // Pre-seed the remote with A so later diffs measure only B's novelty.
    let mut remote = fresh_store();
    replicate(&base, &after_a, &local, &mut remote).await?;

    // Blob B-shared: A's chunks except the last one differs → shares N-1 chunks.
    let mut blob_b_shared = make(0, N - 1);
    blob_b_shared.extend(make(9, 1)); // one fresh trailing chunk
    let (after_shared, _) = put_blob(after_a.clone(), &mut local, &mut delta, &blob_b_shared).await?;
    let shared_upload = replicate(&after_a, &after_shared, &local, &mut remote).await?;

    // Blob B-disjoint: N entirely different chunks, same size.
    let blob_b_disjoint = make(7, N);
    let (after_disjoint, _) =
        put_blob(after_a.clone(), &mut local, &mut delta, &blob_b_disjoint).await?;
    let disjoint_upload = replicate(&after_a, &after_disjoint, &local, &mut remote).await?;

    assert!(
        shared_upload < disjoint_upload,
        "sharing chunks must upload fewer novel nodes than a disjoint blob \
         (shared={shared_upload}, disjoint={disjoint_upload})"
    );
    Ok(())
}
