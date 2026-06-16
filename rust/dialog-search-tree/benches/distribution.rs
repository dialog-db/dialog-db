//! Compares the bit-batch rank derivation (the distribution on `main`) against
//! the threshold-based geometric distribution (this branch), swept across
//! several declared branch factors.
//!
//! The bug the threshold distribution fixes shrank the *effective* branch
//! factor: the bit-batch algorithm reads batches that straddle byte
//! boundaries, so promotion past the first level happens with probability
//! 1/2, 1/4, ... instead of 1/m. The smaller the declared branch factor, the
//! more that skew distorts the tree, so the sweep covers 32, 64, 128 and the
//! production value 254.
//!
//! Both distributions hash the key with blake3 exactly as the production
//! [`Geometric`] distribution does; the only variable is how the hash becomes
//! a rank.
//!
//! Criterion measures the wall-clock cost of insert, point-get and range
//! scans. The two distributions share one benchmark group per (operation,
//! branch factor), so criterion overlays them on a single comparison line
//! chart across input sizes: see `target/criterion/<op>/m=<m>/report/lines.svg`
//! and the top-level `target/criterion/report/index.html`.
//!
//! A separate `shape` group prints, once per (distribution, branch factor,
//! size), the tree height, node counts, per-level fan-out and persisted byte
//! size: the structural evidence that criterion's timing cannot show directly.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-search-tree --bench distribution
//! ```

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::Blake3Hash;
use dialog_common::helpers::BenchData;
use dialog_search_tree::helpers::{Traversable as _, TraversalOrder};
use dialog_search_tree::{
    ContentAddressedStorage, Distribution, PersistentNode, PersistentTree, Rank,
};
use dialog_storage::MemoryStorageBackend;
use futures_util::StreamExt;

const BENCH_SEED: u64 = 42;

// Declared branch factors swept by `*_sweep`: 32, 64, 128 and the production
// value 254. Const generics require literal type arguments, so the sweep
// functions list them explicitly rather than iterating this as data. The bug
// bites hardest at the small end.

/// Entry counts for the read-side timing sweeps and the shape report. The
/// trees are built once during setup, so larger sizes are cheap to measure.
const SIZES: [usize; 3] = [1_000, 10_000, 50_000];

/// Entry counts for the insert sweep. Each criterion sample rebuilds the whole
/// tree from empty, so inserting is capped lower than the read sweeps and run
/// with a small sample count to keep wall-clock reasonable.
const INSERT_SIZES: [usize; 2] = [1_000, 10_000];

/// Criterion sample count for the insert sweep; a full rebuild per sample is
/// expensive, so fewer samples are taken than criterion's default of 100.
const INSERT_SAMPLES: usize = 10;

/// The bit-batch rank derivation that lived on `main`, parameterized by the
/// declared branch factor `M`.
///
/// It groups the 256 fair Bernoulli trials of the hash into batches of
/// `ceil(log2(M))` bits and returns the index of the first all-zero batch.
/// Because batches straddle byte boundaries and the algorithm reads a single
/// byte per batch, higher levels see inflated promotion probabilities, which
/// skews the tree taller and narrower than `M` declares.
struct BitBatch<const M: u64>;

impl<const M: u64> Distribution for BitBatch<M> {
    fn rank(key: &[u8]) -> Rank {
        let hash = Blake3Hash::hash(key);
        let bytes = hash.as_bytes();
        let m = M as u32;

        let k = (m + 1).ilog2();
        let batch_count = 256 / k;
        let mask = (1u8 << k) - 1;
        for i in 0..batch_count {
            let byte_index = (k * i) / 8;
            let bit_index = (k * i) % 8;
            let batch = (bytes[byte_index as usize] >> bit_index) & mask;
            if batch != 0 {
                return Rank::from(i + 1);
            }
        }
        Rank::from(batch_count + 1)
    }
}

/// The threshold-based geometric distribution from this branch, parameterized
/// by the declared branch factor `M`.
///
/// It reads the first 8 bytes of the hash as a little-endian `u64` and counts
/// how many geometrically decreasing thresholds (`u64::MAX / M^k`) the prefix
/// falls below, giving an exact 1/M split probability at every level.
struct Threshold<const M: u64>;

impl<const M: u64> Distribution for Threshold<M> {
    fn rank(key: &[u8]) -> Rank {
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
type Storage = ContentAddressedStorage<Backend>;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

/// Builds and persists a tree of `size` random entries under distribution `D`.
async fn build_tree<D>(
    size: usize,
) -> (PersistentTree<[u8; 16], Vec<u8>, D>, Storage, Vec<[u8; 16]>)
where
    D: Distribution,
{
    let mut data = BenchData::new(BENCH_SEED);
    let keys = data.random_buffers::<16>(size);
    let values = data.random_buffers::<32>(size);

    let mut storage = ContentAddressedStorage::new(Backend::default());
    let mut tree = PersistentTree::<[u8; 16], Vec<u8>, D>::empty();

    for (key, value) in keys.iter().zip(values.iter()) {
        tree = tree
            .edit()
            .insert(*key, value.to_vec(), &storage)
            .await
            .unwrap()
            .persist()
            .unwrap();
    }
    for buffer in tree.flush() {
        storage
            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
            .await
            .unwrap();
    }

    (tree, storage, keys)
}

/// Inserts every entry into a fresh tree. This is the routine timed by the
/// insert benchmark.
async fn insert_all<D>(keys: &[[u8; 16]], values: &[Vec<u8>])
where
    D: Distribution,
{
    let storage = ContentAddressedStorage::new(Backend::default());
    let mut tree = PersistentTree::<[u8; 16], Vec<u8>, D>::empty();
    for (key, value) in keys.iter().zip(values.iter()) {
        tree = tree
            .edit()
            .insert(*key, value.clone(), &storage)
            .await
            .unwrap()
            .persist()
            .unwrap();
    }
}

/// The two distribution function names compared within every group. Using the
/// same two names across input sizes is what makes criterion overlay them on a
/// single comparison line chart (`<group>/report/lines.svg`).
const BIT_BATCH: &str = "bit-batch";
const THRESHOLD: &str = "threshold";

/// Group name for one operation at one branch factor, e.g. `insert/m=254`. One
/// group per (operation, branch factor) gives one comparison chart per cell,
/// each overlaying bit-batch against threshold across the input sizes.
fn group_name(op: &str, m: u64) -> String {
    format!("{op}/m={m}")
}

/// Times bulk insertion of every entry, for both distributions, at one branch
/// factor `M` and across the size sweep. The two distributions share a group so
/// criterion overlays them on one comparison line chart.
fn bench_insert<const M: u64>(c: &mut Criterion) {
    let mut group = c.benchmark_group(group_name("insert", M));
    group.sample_size(INSERT_SAMPLES);
    for size in INSERT_SIZES {
        let mut data = BenchData::new(BENCH_SEED);
        let keys = data.random_buffers::<16>(size);
        let values: Vec<Vec<u8>> = data
            .random_buffers::<32>(size)
            .into_iter()
            .map(|v| v.to_vec())
            .collect();

        group.bench_with_input(BenchmarkId::new(BIT_BATCH, size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| insert_all::<BitBatch<M>>(&keys, &values));
        });
        group.bench_with_input(BenchmarkId::new(THRESHOLD, size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| insert_all::<Threshold<M>>(&keys, &values));
        });
    }
    group.finish();
}

/// Number of point lookups timed per sample. Bounded so the get sweep isolates
/// per-lookup walk cost (which the distribution controls via tree depth)
/// without scaling the sample's work with the tree size.
const LOOKUPS_PER_SAMPLE: usize = 256;

/// Times a fixed batch of point lookups, for both distributions, at one branch
/// factor `M` and across the size sweep. Fewer lookups than the tree holds
/// keeps each sample cheap; the per-lookup cost is what the distribution
/// changes, via the number of levels a lookup must walk.
fn bench_get<const M: u64>(c: &mut Criterion) {
    let mut group = c.benchmark_group(group_name("get", M));
    for size in SIZES {
        let (old_tree, old_storage, old_keys) = runtime().block_on(build_tree::<BitBatch<M>>(size));
        let (new_tree, new_storage, new_keys) =
            runtime().block_on(build_tree::<Threshold<M>>(size));

        let probe = LOOKUPS_PER_SAMPLE.min(size);

        group.bench_with_input(BenchmarkId::new(BIT_BATCH, size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                for key in &old_keys[..probe] {
                    old_tree.get(key, &old_storage).await.unwrap();
                }
            });
        });
        group.bench_with_input(BenchmarkId::new(THRESHOLD, size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                for key in &new_keys[..probe] {
                    new_tree.get(key, &new_storage).await.unwrap();
                }
            });
        });
    }
    group.finish();
}

/// Times a full range scan over the whole tree, for both distributions, at one
/// branch factor `M` and across the size sweep.
fn bench_range<const M: u64>(c: &mut Criterion) {
    let mut group = c.benchmark_group(group_name("range_scan", M));
    group.sample_size(20);
    for size in SIZES {
        let (old_tree, old_storage, _) = runtime().block_on(build_tree::<BitBatch<M>>(size));
        let (new_tree, new_storage, _) = runtime().block_on(build_tree::<Threshold<M>>(size));

        group.bench_with_input(BenchmarkId::new(BIT_BATCH, size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                let stream = old_tree.stream(&old_storage);
                futures_util::pin_mut!(stream);
                let mut count = 0usize;
                while let Some(entry) = stream.next().await {
                    entry.unwrap();
                    count += 1;
                }
                count
            });
        });
        group.bench_with_input(BenchmarkId::new(THRESHOLD, size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                let stream = new_tree.stream(&new_storage);
                futures_util::pin_mut!(stream);
                let mut count = 0usize;
                while let Some(entry) = stream.next().await {
                    entry.unwrap();
                    count += 1;
                }
                count
            });
        });
    }
    group.finish();
}

/// Structural shape of a persisted tree.
struct Shape {
    height: usize,
    total_nodes: usize,
    index_nodes: usize,
    leaf_nodes: usize,
    avg_fanout_by_level: BTreeMap<usize, f64>,
    total_bytes: usize,
}

/// Walks the persisted tree to recover its height, node counts, per-level
/// fan-out and total persisted byte size.
async fn shape_of<D>(tree: &PersistentTree<[u8; 16], Vec<u8>, D>, storage: &Storage) -> Shape
where
    D: Distribution,
{
    let mut total_nodes = 0usize;
    let mut index_nodes = 0usize;
    let mut leaf_nodes = 0usize;
    let mut total_bytes = 0usize;
    let mut seen = std::collections::HashSet::new();

    let stream = tree.traverse(TraversalOrder::BreadthFirst, storage);
    futures_util::pin_mut!(stream);
    while let Some(node) = stream.next().await {
        let node = node.unwrap();
        total_nodes += 1;
        if seen.insert(node.hash().clone()) {
            total_bytes += node.buffer().as_ref().len();
        }
        match node.as_index() {
            Ok(_) => index_nodes += 1,
            Err(_) => leaf_nodes += 1,
        }
    }

    // Depth-tagged BFS over child hashes for per-level fan-out and height.
    let mut by_depth: BTreeMap<usize, (usize, usize)> = BTreeMap::new();
    let mut frontier = vec![tree.root().clone()];
    let mut depth = 0usize;
    let is_empty = total_nodes == 0;

    if !is_empty {
        loop {
            let mut next = Vec::new();
            let mut fanout_sum = 0usize;
            let mut node_count = 0usize;

            for hash in &frontier {
                let bytes = storage.retrieve(hash).await.unwrap().unwrap();
                let node = PersistentNode::<[u8; 16], Vec<u8>>::new(bytes.into());
                node_count += 1;
                match node.as_index() {
                    Ok(index) => {
                        fanout_sum += index.links.len();
                        for link in index.links.iter() {
                            next.push(<&Blake3Hash>::from(&link.node).clone());
                        }
                    }
                    Err(_) => fanout_sum += node.as_segment().unwrap().entries.len(),
                }
            }

            by_depth.insert(depth, (fanout_sum, node_count));
            if next.is_empty() {
                break;
            }
            frontier = next;
            depth += 1;
        }
    }

    let height = if is_empty { 0 } else { depth + 1 };

    // Re-key depth (root = 0) to level (leaves = 0) so the table reads bottom-up.
    let mut avg_fanout_by_level = BTreeMap::new();
    for (d, (fanout_sum, node_count)) in &by_depth {
        let level = depth - d;
        let avg = if *node_count == 0 {
            0.0
        } else {
            *fanout_sum as f64 / *node_count as f64
        };
        avg_fanout_by_level.insert(level, avg);
    }

    Shape {
        height,
        total_nodes,
        index_nodes,
        leaf_nodes,
        avg_fanout_by_level,
        total_bytes,
    }
}

fn print_shape_row(distribution: &str, m: u64, size: usize, shape: &Shape) {
    let fanout: Vec<String> = shape
        .avg_fanout_by_level
        .iter()
        .map(|(level, avg)| format!("L{level}:{avg:.1}"))
        .collect();
    println!(
        "{:<10} {:>4} {:>7}  height={:<2} nodes={:<5} (idx={:<4} leaf={:<5}) bytes={:<9} fanout[{}]",
        distribution,
        m,
        size,
        shape.height,
        shape.total_nodes,
        shape.index_nodes,
        shape.leaf_nodes,
        shape.total_bytes,
        fanout.join(" "),
    );
}

/// Emits the shape/storage table for one branch factor `M`. This is the
/// structural evidence the timing plots cannot show: it walks each persisted
/// tree and prints height, node counts, per-level fan-out and byte size.
///
/// It is wired as a criterion bench so it runs under `cargo bench`, but the
/// timed routine is trivial; the value is the table printed during setup.
fn bench_shape<const M: u64>(c: &mut Criterion) {
    let rt = runtime();
    println!(
        "\n=== tree shape @ branch factor m={M} (level 0 = leaves; fanout = avg children/entries) ==="
    );
    for size in SIZES {
        let (old_tree, old_storage, _) = rt.block_on(build_tree::<BitBatch<M>>(size));
        let old_shape = rt.block_on(shape_of(&old_tree, &old_storage));
        print_shape_row("bit-batch", M, size, &old_shape);

        let (new_tree, new_storage, _) = rt.block_on(build_tree::<Threshold<M>>(size));
        let new_shape = rt.block_on(shape_of(&new_tree, &new_storage));
        print_shape_row("threshold", M, size, &new_shape);
    }

    // A trivial measured routine keeps criterion happy without adding noise to
    // the report; the shape table above is what this group exists to produce.
    let mut group = c.benchmark_group("shape_marker");
    group.sample_size(10);
    group.bench_function(format!("m={M}"), |b| b.iter(|| M * 2));
    group.finish();
}

fn insert_sweep(c: &mut Criterion) {
    bench_insert::<32>(c);
    bench_insert::<64>(c);
    bench_insert::<128>(c);
    bench_insert::<254>(c);
}

fn get_sweep(c: &mut Criterion) {
    bench_get::<32>(c);
    bench_get::<64>(c);
    bench_get::<128>(c);
    bench_get::<254>(c);
}

fn range_sweep(c: &mut Criterion) {
    bench_range::<32>(c);
    bench_range::<64>(c);
    bench_range::<128>(c);
    bench_range::<254>(c);
}

fn shape_sweep(c: &mut Criterion) {
    bench_shape::<32>(c);
    bench_shape::<64>(c);
    bench_shape::<128>(c);
    bench_shape::<254>(c);
}

criterion_group!(benches, shape_sweep, insert_sweep, get_sweep, range_sweep);
criterion_main!(benches);
