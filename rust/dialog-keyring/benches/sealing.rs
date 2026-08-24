//! What sealing costs.
//!
//! Every benchmark runs the same workload twice against the same tree code:
//! once through a plain [`ContentAddressedStorage`], once through one with a
//! [`NodeSealer`] attached. The delta between the two pairs is the whole
//! answer — nothing else differs.

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::Blake3Hash;
use dialog_common::helpers::BenchData;
use dialog_keyring::{LocalKeyring, NodeSealer};
use dialog_search_tree::{ContentAddressedStorage, Delta, NodeCipher, PersistentTree};
use dialog_storage::MemoryStorageBackend;
use futures_util::StreamExt;

const BENCH_SEED: u64 = 42;

/// The storage the tree writes through, in both arms of every comparison.
type Store = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

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

/// Storage for one run, sealed or not.
fn storage(sealer: Option<&Arc<NodeSealer>>) -> Store {
    match sealer {
        Some(sealer) => {
            ContentAddressedStorage::with_cipher(MemoryStorageBackend::default(), sealer.clone())
        }
        None => ContentAddressedStorage::new(MemoryStorageBackend::default()),
    }
}

/// Build a tree in one batch: every insert into a single transient, one
/// persist, one flush. This is the shape a commit actually has, and the one
/// where sealing cost is proportional to the nodes a commit writes.
async fn commit(
    storage: &mut Store,
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

    for (_, buffer) in delta.flush() {
        storage
            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
            .await
            .unwrap();
    }

    tree
}

/// Build a tree one insert at a time, flushing after each, the way the
/// existing `insert` benchmark does. Every insert rewrites the whole
/// root-to-leaf path, so this seals far more nodes per entry than a commit
/// does — the pessimistic end of the range.
async fn build(
    storage: &mut Store,
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
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await
                .unwrap();
        }
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

        for sealed in [false, true] {
            let label = if sealed { "sealed" } else { "plain" };
            let sealer = sealed.then(|| sealer.clone());
            group.bench_with_input(BenchmarkId::new(label, size), &size, |b, _| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let mut store = storage(sealer.as_ref());
                        build(&mut store, &keys, &values).await;
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

        for sealed in [false, true] {
            let label = if sealed { "sealed" } else { "plain" };
            let sealer = sealed.then(|| sealer.clone());
            group.bench_with_input(BenchmarkId::new(label, size), &size, |b, _| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let mut store = storage(sealer.as_ref());
                        commit(&mut store, &keys, &values).await;
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

    for sealed in [false, true] {
        let label = if sealed { "sealed" } else { "plain" };
        let sealer = sealed.then(|| sealer.clone());
        let (store, tree) = setup.block_on(async {
            let mut store = storage(sealer.as_ref());
            let tree = build(&mut store, &keys, &values).await;
            (store, tree)
        });

        let root = tree.root().clone();
        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    // A cold tree every iteration. The node cache holds
                    // decrypted buffers, so a warm read costs the same either
                    // way and would measure nothing; what sealing charges is
                    // the miss.
                    let tree = PersistentTree::<[u8; 16], Vec<u8>>::from_hash(root.clone());
                    for key in keys.iter().step_by(size / 64) {
                        tree.get(key, &store).await.unwrap();
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

    for sealed in [false, true] {
        let label = if sealed { "sealed" } else { "plain" };
        let sealer = sealed.then(|| sealer.clone());
        let (store, tree) = setup.block_on(async {
            let mut store = storage(sealer.as_ref());
            let tree = build(&mut store, &keys, &values).await;
            (store, tree)
        });

        let root = tree.root().clone();
        group.bench_with_input(BenchmarkId::new(label, size), &size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    // Cold, for the same reason.
                    let tree = PersistentTree::<[u8; 16], Vec<u8>>::from_hash(root.clone());
                    let mut stream = Box::pin(tree.stream(&store));
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
