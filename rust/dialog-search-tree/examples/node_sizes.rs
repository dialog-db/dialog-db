//! One-off: reports per-level node byte sizes (min / mean / max) for the
//! threshold distribution across branch factors, so the index/root node size
//! (the per-insert hotspot and the largest single block fetch) is visible
//! rather than blended into an average.
//!
//! Run with:
//!
//! ```sh
//! cargo run --release --example node_sizes --features helpers
//! ```

use std::collections::BTreeMap;

use dialog_common::Blake3Hash;
use dialog_common::helpers::BenchData;
use dialog_search_tree::{
    ContentAddressedStorage, Delta, Distribution, PersistentNode, PersistentTree, Rank,
};
use dialog_storage::MemoryStorageBackend;

const BENCH_SEED: u64 = 42;
const SIZES: [usize; 2] = [10_000, 50_000];

struct Threshold<const M: u64>;

impl<const M: u64> Distribution for Threshold<M> {
    fn rank(key: &[u8], _manifest: &dialog_search_tree::Manifest) -> Rank {
        let hash = Blake3Hash::hash(key);
        let [b0, b1, b2, b3, b4, b5, b6, b7, ..] = *hash.as_bytes();
        let prefix = u64::from_le_bytes([b0, b1, b2, b3, b4, b5, b6, b7]);
        let mut rank: Rank = 1;
        let mut threshold = u64::MAX / M;
        while prefix < threshold {
            rank += 1;
            threshold /= M;
        }
        rank
    }
}

type Backend = MemoryStorageBackend<Blake3Hash, Vec<u8>>;

async fn build<D: Distribution>(
    size: usize,
) -> (
    PersistentTree<[u8; 16], Vec<u8>, D>,
    ContentAddressedStorage<Backend>,
) {
    let mut data = BenchData::new(BENCH_SEED);
    let keys = data.random_buffers::<16>(size);
    let values = data.random_buffers::<32>(size);
    let mut storage = ContentAddressedStorage::new(Backend::default());
    let mut tree = PersistentTree::<[u8; 16], Vec<u8>, D>::empty();
    let mut delta = Delta::zero();
    for (k, v) in keys.iter().zip(values.iter()) {
        tree = tree
            .edit()
            .insert(*k, v.to_vec(), &storage)
            .await
            .unwrap()
            .persist(&mut delta)
            .unwrap();
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await
                .unwrap();
        }
    }
    (tree, storage)
}

/// Walks the tree top-down by level, recording the byte size of every node and
/// the fan-out of the root.
async fn report<const M: u64>(size: usize) {
    let (tree, storage) = build::<Threshold<M>>(size).await;

    // depth 0 = root. Collect node byte sizes per depth.
    let mut sizes_by_depth: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut frontier = vec![tree.root().clone()];
    let mut depth = 0usize;
    let mut root_fanout = 0usize;

    loop {
        let mut next = Vec::new();
        for hash in &frontier {
            let bytes = storage.retrieve(hash).await.unwrap().unwrap();
            sizes_by_depth.entry(depth).or_default().push(bytes.len());
            let node = PersistentNode::<[u8; 16], Vec<u8>>::new(bytes.into());
            if let Ok(index) = node.as_index() {
                if depth == 0 {
                    root_fanout = index.len();
                }
                for at in 0..index.len() {
                    next.push(index.hash_at(at).unwrap().clone());
                }
            }
        }
        if next.is_empty() {
            break;
        }
        frontier = next;
        depth += 1;
    }

    let height = depth + 1;
    let root_bytes = sizes_by_depth[&0][0];
    println!(
        "  m={M:<4} size={size:<6} height={height} root_fanout={root_fanout:<4} root_bytes={root_bytes}"
    );
    for (d, sizes) in &sizes_by_depth {
        let level = depth - d; // level 0 = leaves
        let min = *sizes.iter().min().unwrap();
        let max = *sizes.iter().max().unwrap();
        let mean = sizes.iter().sum::<usize>() / sizes.len();
        let kind = if *d == depth { "leaf " } else { "index" };
        println!(
            "      L{level} ({kind}) count={:<5} bytes min={:<7} mean={:<7} max={:<7}",
            sizes.len(),
            min,
            mean,
            max
        );
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!(
        "Threshold node sizes per level (level 0 = leaves; entry ~= 16B key + 32B value + overhead)"
    );
    for size in SIZES {
        for m in [32u64, 64, 128, 254] {
            match m {
                32 => report::<32>(size).await,
                64 => report::<64>(size).await,
                128 => report::<128>(size).await,
                254 => report::<254>(size).await,
                _ => unreachable!(),
            }
        }
        println!();
    }
}
