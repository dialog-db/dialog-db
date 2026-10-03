//! What sealing costs.
//!
//! Every benchmark runs the same workload three times against the same tree
//! code: through plain [`MemoryBlocks`], through [`SealedBlocks`] (flat
//! sealing), and through [`LayeredBlocks`] (layered sealing). The deltas
//! between the arms are the whole answer — nothing else differs.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_capability::Provider;
use dialog_common::helpers::BenchData;
use dialog_common::{Blake3Hash, Buffer};
use dialog_keyring::layered::{Access, LayeredBlocks, LayeredRoot, Level, LevelSecret, Writer};
use dialog_keyring::{EpochId, LocalKeyring, NodeSealer, SealedBlocks};
use dialog_search_tree::{Delta, DialogSearchTreeError, LoadBlock, MemoryBlocks, PersistentTree};
use futures_util::StreamExt;

const BENCH_SEED: u64 = 42;

type Tree = PersistentTree<[u8; 16], Vec<u8>>;

/// Which way the tree's blocks are stored.
#[derive(Clone, Copy)]
enum Arm {
    Plain,
    Sealed,
    Layered,
}

impl Arm {
    const ALL: [Arm; 3] = [Arm::Plain, Arm::Sealed, Arm::Layered];

    fn label(self) -> &'static str {
        match self {
            Arm::Plain => "plain",
            Arm::Sealed => "sealed",
            Arm::Layered => "layered",
        }
    }
}

/// The storage the tree reads and writes through, in each arm.
enum Store {
    Plain(MemoryBlocks),
    Sealed(SealedBlocks),
    /// The layered envelopes, and where the last write left the tree.
    Layered(LayeredBlocks<[u8; 16], Vec<u8>>, Mutex<Option<LayeredRoot>>),
}

impl Store {
    /// Keep every block `delta` stages for the tree rooted at `root`,
    /// sealing them in the sealed arms.
    fn flush(&self, delta: &mut Delta<Blake3Hash, Buffer>, root: &Blake3Hash) {
        match self {
            Store::Plain(blocks) => blocks.flush(delta),
            Store::Sealed(blocks) => blocks.flush(delta).unwrap(),
            Store::Layered(blocks, last) => {
                let written = blocks.write(&writer(), delta, root).unwrap();
                *last.lock().unwrap() = Some(written);
            }
        }
    }

    /// A reader with nothing cached or learned, and the tree rooted at
    /// `root` as it reads it. A layered reader starts from the layered root,
    /// so opening it is part of what a cold read costs.
    fn cold(&self, root: &Blake3Hash) -> (Store, Tree) {
        match self {
            Store::Plain(blocks) => (Store::Plain(blocks.clone()), Tree::from_hash(root.clone())),
            Store::Sealed(blocks) => (Store::Sealed(blocks.clone()), Tree::from_hash(root.clone())),
            Store::Layered(blocks, last) => {
                let start = last.lock().unwrap().clone().expect("written");
                let reader = blocks.as_party(member());
                let identity = reader.open_root(&start).unwrap();
                (
                    Store::Layered(reader, Mutex::new(Some(start))),
                    Tree::from_hash(identity),
                )
            }
        }
    }

    /// Bytes stored, and how many blocks hold them.
    fn footprint(&self) -> (usize, usize) {
        let sizes: Vec<usize> = match self {
            Store::Plain(_) => return (0, 0),
            Store::Sealed(blocks) => blocks.stored().iter().map(|(_, b)| b.len()).collect(),
            Store::Layered(blocks, _) => blocks.stored().iter().map(|(_, b)| b.len()).collect(),
        };
        (sizes.iter().sum(), sizes.len())
    }
}

#[async_trait]
impl Provider<LoadBlock> for Store {
    async fn execute(&self, load: LoadBlock) -> Result<Option<Buffer>, DialogSearchTreeError> {
        match self {
            Store::Plain(blocks) => blocks.execute(load).await,
            Store::Sealed(blocks) => blocks.execute(load).await,
            Store::Layered(blocks, _) => blocks.execute(load).await,
        }
    }
}

fn level(tag: u8) -> LevelSecret {
    LevelSecret::new(EpochId::from([tag; 32]), [tag.wrapping_mul(31); 32])
}

/// The layered arm's writer: one range and one content generation.
fn writer() -> Writer {
    Writer::new(level(1), level(2))
}

/// A member holding the layered arm's generations.
fn member() -> Access {
    Access::content(Level::new().with(level(1)), Level::new().with(level(2)))
}

/// A sealer over a fixed keyring, resolved once outside any benchmark.
///
/// Resolution is async and must not happen inside the measured closure —
/// both because it would be measuring the wrong thing and because criterion
/// already holds a runtime there.
fn sealer(runtime: &tokio::runtime::Runtime) -> Arc<NodeSealer> {
    let keyring = LocalKeyring::genesis([7u8; 32], [1u8; 32]);
    Arc::new(
        runtime
            .block_on(NodeSealer::resolve(&keyring))
            .expect("resolve"),
    )
}

/// Storage for one run of `arm`.
fn storage(arm: Arm, sealer: &Arc<NodeSealer>) -> Store {
    match arm {
        Arm::Plain => Store::Plain(MemoryBlocks::new()),
        Arm::Sealed => Store::Sealed(SealedBlocks::new(sealer.clone())),
        Arm::Layered => Store::Layered(LayeredBlocks::new(member()), Mutex::new(None)),
    }
}

/// Build a tree in one batch: every insert into a single transient, one
/// persist, one flush. This is the shape a commit actually has, and the one
/// where sealing cost is proportional to the nodes a commit writes.
async fn commit(
    storage: &Store,
    keys: &[[u8; 16]],
    values: &[[u8; 32]],
) -> PersistentTree<[u8; 16], Vec<u8>> {
    let mut delta = Delta::zero();
    let mut transient = PersistentTree::<[u8; 16], Vec<u8>>::empty().edit();

    for (key, value) in keys.iter().zip(values.iter()) {
        transient = transient
            .insert(*key, value.to_vec(), storage)
            .await
            .unwrap();
    }
    let tree = transient.persist(&mut delta).unwrap();
    storage.flush(&mut delta, tree.root());
    tree
}

/// Build a tree one insert at a time, flushing after each, the way the
/// existing `insert` benchmark does. Every insert rewrites the whole
/// root-to-leaf path, so this seals far more nodes per entry than a commit
/// does — the pessimistic end of the range.
async fn build(
    storage: &Store,
    keys: &[[u8; 16]],
    values: &[[u8; 32]],
) -> PersistentTree<[u8; 16], Vec<u8>> {
    let mut tree = PersistentTree::<[u8; 16], Vec<u8>>::empty();
    let mut delta = Delta::zero();

    for (key, value) in keys.iter().zip(values.iter()) {
        tree = tree
            .edit()
            .insert(*key, value.to_vec(), storage)
            .await
            .unwrap()
            .persist(&mut delta)
            .unwrap();
        storage.flush(&mut delta, tree.root());
    }

    tree
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    let mut data = BenchData::new(BENCH_SEED);
    let setup = tokio::runtime::Runtime::new().unwrap();
    let sealer = sealer(&setup);

    for size in [100usize, 1000] {
        let keys = data.random_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        for arm in Arm::ALL {
            group.bench_with_input(BenchmarkId::new(arm.label(), size), &size, |b, _| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let store = storage(arm, &sealer);
                        build(&store, &keys, &values).await;
                    });
            });
        }
    }

    group.finish();
}

fn bench_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit");
    let mut data = BenchData::new(BENCH_SEED);
    let setup = tokio::runtime::Runtime::new().unwrap();
    let sealer = sealer(&setup);

    for size in [1000usize, 10_000] {
        let keys = data.random_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        for arm in Arm::ALL {
            group.bench_with_input(BenchmarkId::new(arm.label(), size), &size, |b, _| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let store = storage(arm, &sealer);
                        commit(&store, &keys, &values).await;
                    });
            });
        }
    }

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    let mut data = BenchData::new(BENCH_SEED);
    let setup = tokio::runtime::Runtime::new().unwrap();
    let sealer = sealer(&setup);
    let size = 10_000;
    let keys = data.random_buffers::<16>(size);
    let values = data.random_buffers::<32>(size);

    for arm in Arm::ALL {
        let label = arm.label();
        let (store, tree) = setup.block_on(async {
            let store = storage(arm, &sealer);
            let tree = build(&store, &keys, &values).await;
            (store, tree)
        });

        let root = tree.root().clone();
        let (bytes, blocks) = store.footprint();
        if blocks > 0 {
            println!("{label}: {blocks} blocks, {bytes} bytes stored");
        }
        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    // A cold tree every iteration. The node cache holds
                    // decrypted buffers, so a warm read costs the same either
                    // way and would measure nothing; what sealing charges is
                    // the miss.
                    let (reader, tree) = store.cold(&root);
                    for key in keys.iter().step_by(size / 64) {
                        tree.get(key, &reader).await.unwrap();
                    }
                });
        });
    }

    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan");
    let mut data = BenchData::new(BENCH_SEED);
    let setup = tokio::runtime::Runtime::new().unwrap();
    let sealer = sealer(&setup);
    let size = 10_000;
    let keys = data.random_buffers::<16>(size);
    let values = data.random_buffers::<32>(size);

    for arm in Arm::ALL {
        let label = arm.label();
        let (store, tree) = setup.block_on(async {
            let store = storage(arm, &sealer);
            let tree = build(&store, &keys, &values).await;
            (store, tree)
        });

        let root = tree.root().clone();
        let (bytes, blocks) = store.footprint();
        if blocks > 0 {
            println!("{label}: {blocks} blocks, {bytes} bytes stored");
        }
        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    // Cold, for the same reason.
                    let (reader, tree) = store.cold(&root);
                    let mut stream = Box::pin(tree.stream(&reader));
                    let mut seen = 0usize;
                    while let Some(entry) = stream.next().await {
                        entry.unwrap();
                        seen += 1;
                    }
                    assert_eq!(seen, size);
                });
        });
    }

    group.finish();
}

/// Per-node cost, isolated from the tree.
///
/// Attributes the deltas above: everything else in a sealed run is the tree
/// doing what it already did. Throughput here also says whether the AES
/// backend is the hardware one — a software fallback lands an order of
/// magnitude lower and would make the whole approach worth reconsidering.
fn bench_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("node");
    let setup = tokio::runtime::Runtime::new().unwrap();
    let sealer = sealer(&setup);
    let mut data = BenchData::new(BENCH_SEED);

    for size in [1024usize, 4096, 16_384, 65_536] {
        let plain: Vec<u8> = data
            .random_buffers::<32>(size / 32)
            .into_iter()
            .flatten()
            .collect();
        let sealed = sealer.seal(&plain).unwrap();

        group.throughput(criterion::Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("seal", size), &size, |b, _| {
            b.iter(|| sealer.seal(std::hint::black_box(&plain)).unwrap());
        });
        group.bench_with_input(BenchmarkId::new("open", size), &size, |b, _| {
            b.iter(|| sealer.open(std::hint::black_box(&sealed)).unwrap());
        });
        group.bench_with_input(BenchmarkId::new("address", size), &size, |b, _| {
            let identity = Blake3Hash::hash(&plain);
            b.iter(|| sealer.address(std::hint::black_box(&identity)));
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_node,
    bench_commit,
    bench_insert,
    bench_get,
    bench_scan
);
criterion_main!(benches);
