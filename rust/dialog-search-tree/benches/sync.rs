//! Sync-cost comparison: canonical [`PersistentTree`] vs buffered
//! [`HitchhikerTree`].
//!
//! The hypothesis: a hitchhiker tree concentrates recent novelty in the upper
//! nodes and cascades it lazily, so its lower nodes stay stable across commits.
//! Two replicas that diverge by a handful of commits therefore differ in fewer
//! nodes, so syncing them transfers fewer blocks and (because the differential
//! walk fetches a node per differing subtree) takes fewer round-trips.
//!
//! This bench measures that at the search-tree layer, the quantity that drives
//! repository sync round-trips, without the repository plumbing. For each
//! configuration it reports, per sync:
//!
//! - **round-trips**: storage `get` calls the differential walk makes against
//!   the other replica's store (each is one fetch a real remote would serve);
//! - **novel blocks** and **novel bytes**: the node set a push must transfer
//!   (`TreeDifference::novel_nodes`);
//! - **commit churn**: new node hashes written per commit, summed over the
//!   divergence (storage write amplification).
//!
//! Two scenarios, both starting from a shared, flushed base:
//!
//! - **pull**: one replica commits `n` more; the other (still at base) pulls,
//!   i.e. diffs base against the advanced tree.
//! - **concurrent**: both replicas commit `n` more independently; each diffs its
//!   own tree against the other's (the bidirectional reconcile cost).
//!
//! Neither tree is force-canonicalized: the hitchhiker tree just buffers each
//! commit and persists with its novelty intact, so cascades happen only on
//! overflow, per the algorithm. The structural counts are printed once at
//! startup; criterion then times the differential walk itself.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::{Blake3Hash, helpers::BenchData};
use dialog_search_tree::{
    Buffer, ContentAddressedStorage, Delta, FlushPolicy, HitchhikerTree, PersistentTree,
};
use dialog_storage::{DialogStorageError, MemoryStorageBackend, StorageBackend};
use futures_util::StreamExt;

const BENCH_SEED: u64 = 42;

/// The size of the shared base each scenario starts from.
const BASE_SIZE: usize = 10_000;

/// The per-node novelty capacity for the hitchhiker tree.
const OP_BUF_SIZE: usize = 1024;

/// A [`StorageBackend`] that counts `get` calls and the bytes they return,
/// wrapping an in-memory backend. Each `get` models one block a remote would
/// have to serve, so the count is the sync round-trip proxy.
#[derive(Clone)]
struct CountingBackend {
    inner: MemoryStorageBackend<Blake3Hash, Vec<u8>>,
    gets: Arc<AtomicU64>,
    get_bytes: Arc<AtomicU64>,
}

impl CountingBackend {
    fn new() -> Self {
        Self {
            inner: MemoryStorageBackend::default(),
            gets: Arc::new(AtomicU64::new(0)),
            get_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Resets the counters and returns a handle to read them later.
    fn reset(&self) {
        self.gets.store(0, Ordering::Relaxed);
        self.get_bytes.store(0, Ordering::Relaxed);
    }

    fn gets(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }

    fn get_bytes(&self) -> u64 {
        self.get_bytes.load(Ordering::Relaxed)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl StorageBackend for CountingBackend {
    type Key = Blake3Hash;
    type Value = Vec<u8>;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.inner.set(key, value).await
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let result = self.inner.get(key).await?;
        self.gets.fetch_add(1, Ordering::Relaxed);
        if let Some(bytes) = &result {
            self.get_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(result)
    }
}

type Tree = PersistentTree<[u8; 16], Vec<u8>>;

/// Flushes a delta's nodes into storage, counting how many distinct new nodes it
/// produced (the commit's write churn).
async fn flush_counting(delta: &mut Delta<Blake3Hash, Buffer>, storage: &mut Storage) -> u64 {
    let mut count = 0u64;
    for (_, buffer) in delta.flush() {
        storage
            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
            .await
            .unwrap();
        count += 1;
    }
    count
}

type Storage = ContentAddressedStorage<CountingBackend>;

/// Builds a shared base tree of `base_size` sequential entries, flushed into a
/// fresh counting store, and returns the tree and its store.
async fn build_base(base_size: usize) -> (Tree, Storage) {
    let data = BenchData::new(BENCH_SEED);
    let keys = data.sequential_buffers::<16>(base_size);

    let mut storage = ContentAddressedStorage::new(CountingBackend::new());
    let mut tree = Tree::empty();
    let mut delta = Delta::zero();
    for key in &keys {
        tree = tree
            .edit()
            .insert(*key, key.to_vec(), &storage)
            .await
            .unwrap()
            .persist(&mut delta)
            .unwrap();
        flush_counting(&mut delta, &mut storage).await;
    }
    (tree, storage)
}

/// The keys a replica commits to diverge from the base: `n` fresh keys past the
/// base range, deterministic per `replica` so two replicas diverge differently.
fn divergence_keys(base_size: usize, n: usize, replica: u64) -> Vec<[u8; 16]> {
    let mut data = BenchData::new(BENCH_SEED ^ (replica.wrapping_mul(0x9E3779B97F4A7C15)));
    let mut keys = data.random_buffers::<16>(n);
    // Bias into a high range so divergence keys do not collide with the base's
    // sequential low range; the exact values stay random within it.
    for key in &mut keys {
        key[15] = 0xFF;
        let _ = base_size;
    }
    keys
}

/// Advances the canonical tree by committing `keys` one at a time (edit +
/// persist + flush per commit), returning the new tree and the total commit
/// churn (new nodes written across all commits).
async fn advance_persistent(base: &Tree, keys: &[[u8; 16]], storage: &mut Storage) -> (Tree, u64) {
    let mut tree = base.clone();
    let mut delta = Delta::zero();
    let mut churn = 0u64;
    for key in keys {
        tree = tree
            .edit()
            .insert(*key, key.to_vec(), storage)
            .await
            .unwrap()
            .persist(&mut delta)
            .unwrap();
        churn += flush_counting(&mut delta, storage).await;
    }
    (tree, churn)
}

/// Advances a buffered hitchhiker tree over the base by buffering `keys` one at a
/// time on a single live spine (no canonicalize, no per-commit reload), then
/// persists once with novelty intact to materialize the divergent root. Returns
/// the divergent root tree and the churn (nodes the persist wrote: the touched
/// spine, deduplicated by content address against the shared base).
///
/// Persisting once at the end is the faithful buffered-commit cost: a buffered
/// commit writes nothing to storage on its own (it buffers in memory); the
/// touched nodes are serialized only when the divergent root is needed (here,
/// for sync). `persist` re-emits untouched `Node::Persistent` children as links
/// without re-serializing, so the delta holds exactly the nodes the divergence
/// changed.
async fn advance_hitchhiker(base: &Tree, keys: &[[u8; 16]], storage: &mut Storage) -> (Tree, u64) {
    let mut hh = HitchhikerTree::<[u8; 16], Vec<u8>>::open(base)
        .with_op_buf_size(OP_BUF_SIZE)
        .with_flush_policy(FlushPolicy::Amortized);
    for key in keys {
        hh = hh.insert(*key, key.to_vec(), storage).await.unwrap();
    }
    let mut delta = Delta::zero();
    let root = hh.persist(&mut delta).unwrap();
    let churn = flush_counting(&mut delta, storage).await;
    (Tree::from_hash(root), churn)
}

/// Runs the full differential between `from` and `to` (consuming the change
/// stream so the prune-expand walk completes), returning the round-trips (gets
/// against `to`'s store) and the novel block count and bytes.
async fn sync_cost(
    from: &Tree,
    from_storage: &Storage,
    to: &Tree,
    to_storage: &Storage,
) -> (u64, u64, u64) {
    to_storage.backend().reset();

    // Drive the entry-level differential, which runs the prune-expand walk over
    // both trees: this is what a pull does to find what changed.
    let differential = from.differentiate(to, from_storage, to_storage);
    futures_util::pin_mut!(differential);
    while let Some(change) = differential.next().await {
        let _ = change.unwrap();
    }

    let round_trips = to_storage.backend().gets();
    let bytes = to_storage.backend().get_bytes();

    // Count the novel block set a push would transfer.
    let difference =
        dialog_search_tree::TreeDifference::compute(from, to, from_storage, to_storage)
            .await
            .unwrap();
    let novel = difference.novel_nodes();
    futures_util::pin_mut!(novel);
    let mut novel_blocks = 0u64;
    while let Some(node) = novel.next().await {
        node.unwrap();
        novel_blocks += 1;
    }

    (round_trips, novel_blocks, bytes)
}

/// Prints the structural sync metrics (round-trips, novel blocks, churn) for both
/// tree kinds across divergence sizes, once before timing.
fn report_metrics() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        println!("\n=== Sync cost: canonical PersistentTree vs buffered HitchhikerTree ===");
        println!(
            "base = {BASE_SIZE} entries, op_buf_size = {OP_BUF_SIZE}, divergence keys are fresh\n"
        );

        // Divergence sizes span both regimes: below op_buf_size everything stays
        // in the root buffer (sync cost is flat at 1 block); past op_buf_size the
        // root overflows and cascades, so the hitchhiker cost grows but stays far
        // below the canonical tree's per-commit path churn.
        for n in [1usize, 16, 256, 1024, 4096] {
            // --- pull scenario: replica B at base pulls replica A (base + n). ---
            let keys_a = divergence_keys(BASE_SIZE, n, 1);

            let (base_p, mut store_p) = build_base(BASE_SIZE).await;
            let (tree_p, churn_p) = advance_persistent(&base_p, &keys_a, &mut store_p).await;
            let (pull_rt_p, pull_blocks_p, pull_bytes_p) =
                sync_cost(&base_p, &store_p, &tree_p, &store_p).await;

            let (base_h, mut store_h) = build_base(BASE_SIZE).await;
            let (tree_h, churn_h) = advance_hitchhiker(&base_h, &keys_a, &mut store_h).await;
            let (pull_rt_h, pull_blocks_h, pull_bytes_h) =
                sync_cost(&base_h, &store_h, &tree_h, &store_h).await;

            println!("n = {n} commits of divergence");
            println!("  pull (one replica ahead, other catches up):");
            println!(
                "    PersistentTree: round_trips={pull_rt_p:>4}  novel_blocks={pull_blocks_p:>4}  novel_get_bytes={pull_bytes_p:>7}  commit_churn={churn_p:>5}"
            );
            println!(
                "    HitchhikerTree: round_trips={pull_rt_h:>4}  novel_blocks={pull_blocks_h:>4}  novel_get_bytes={pull_bytes_h:>7}  commit_churn={churn_h:>5}"
            );

            // --- concurrent scenario: both diverge, each diffs against the other. ---
            let keys_b = divergence_keys(BASE_SIZE, n, 2);

            let (base_p2, mut store_p2) = build_base(BASE_SIZE).await;
            let (a_p, _) = advance_persistent(&base_p2, &keys_a, &mut store_p2).await;
            let (b_p, _) = advance_persistent(&base_p2, &keys_b, &mut store_p2).await;
            let (conc_rt_p, conc_blocks_p, _) = sync_cost(&a_p, &store_p2, &b_p, &store_p2).await;

            let (base_h2, mut store_h2) = build_base(BASE_SIZE).await;
            let (a_h, _) = advance_hitchhiker(&base_h2, &keys_a, &mut store_h2).await;
            let (b_h, _) = advance_hitchhiker(&base_h2, &keys_b, &mut store_h2).await;
            let (conc_rt_h, conc_blocks_h, _) = sync_cost(&a_h, &store_h2, &b_h, &store_h2).await;

            println!("  concurrent (both diverge, reconcile one direction):");
            println!(
                "    PersistentTree: round_trips={conc_rt_p:>4}  novel_blocks={conc_blocks_p:>4}"
            );
            println!(
                "    HitchhikerTree: round_trips={conc_rt_h:>4}  novel_blocks={conc_blocks_h:>4}\n"
            );
        }
    });
}

fn bench_sync(c: &mut Criterion) {
    report_metrics();

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Time the pull differential walk for each tree kind across divergence sizes,
    // spanning both the in-buffer regime and the overflow/cascade regime.
    let mut group = c.benchmark_group("sync_pull_walk");
    for n in [16usize, 256, 4096] {
        let keys = divergence_keys(BASE_SIZE, n, 1);

        let (base_p, mut store_p) = rt.block_on(build_base(BASE_SIZE));
        let (tree_p, _) = rt.block_on(advance_persistent(&base_p, &keys, &mut store_p));
        group.bench_with_input(BenchmarkId::new("persistent", n), &n, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { sync_cost(&base_p, &store_p, &tree_p, &store_p).await });
        });

        let (base_h, mut store_h) = rt.block_on(build_base(BASE_SIZE));
        let (tree_h, _) = rt.block_on(advance_hitchhiker(&base_h, &keys, &mut store_h));
        group.bench_with_input(BenchmarkId::new("hitchhiker", n), &n, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { sync_cost(&base_h, &store_h, &tree_h, &store_h).await });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_sync);
criterion_main!(benches);
