//! Compares batched insertion through a [`Transient`] against the same entries
//! inserted one at a time with [`Tree::insert`].
//!
//! Sequential insert rebuilds and re-hashes the path on every operation; the
//! transient copies and seals each touched node once for the whole batch. This
//! measures the amortization across batch sizes.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-search-tree --bench batch
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_search_tree::{ContentAddressedStorage, Tree};
use dialog_storage::MemoryStorageBackend;

const BENCH_SEED: u64 = 42;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn bench_batch_vs_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_batch");
    let mut data = BenchData::new(BENCH_SEED);

    for size in [100usize, 1_000, 10_000] {
        let keys = data.random_buffers::<16>(size);
        let values: Vec<Vec<u8>> = data
            .random_buffers::<32>(size)
            .into_iter()
            .map(|value| value.to_vec())
            .collect();

        group.bench_with_input(BenchmarkId::new("sequential", size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                let mut tree = Tree::<[u8; 16], Vec<u8>>::empty();
                for (key, value) in keys.iter().zip(values.iter()) {
                    tree = tree.insert(*key, value.clone(), &storage).await.unwrap();
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("transient", size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                let tree = Tree::<[u8; 16], Vec<u8>>::empty();
                let mut transient = tree.transient(&storage).await.unwrap();
                for (key, value) in keys.iter().zip(values.iter()) {
                    transient.insert(*key, value.clone()).await.unwrap();
                }
                let (_root, _delta) = transient.persist().unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_batch_vs_sequential);
criterion_main!(benches);
