//! Compares batched edits through [`Tree::edit`] against the same operations
//! applied one at a time with [`Tree::insert`] / [`Tree::delete`].
//!
//! Sequential edits rebuild and re-hash the path on every operation; the
//! batched edit shares untouched subtrees and serializes the touched spine once
//! at `persist`. This measures the amortization across batch sizes.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-search-tree --bench batch
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_search_tree::{ContentAddressedStorage, Delta, PersistentTree};
use dialog_storage::MemoryStorageBackend;

const BENCH_SEED: u64 = 42;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn bench_insert_batch_vs_sequential(c: &mut Criterion) {
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
                let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                let mut tree = PersistentTree::<[u8; 16], Vec<u8>>::empty();
                let mut delta = Delta::zero();
                for (key, value) in keys.iter().zip(values.iter()) {
                    tree = tree
                        .edit()
                        .insert(*key, value.clone(), &storage)
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
            });
        });

        group.bench_with_input(BenchmarkId::new("batched", size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                let mut edit = PersistentTree::<[u8; 16], Vec<u8>>::empty().edit();
                for (key, value) in keys.iter().zip(values.iter()) {
                    edit = edit.insert(*key, value.clone(), &storage).await.unwrap();
                }
                let mut delta = Delta::zero();
                let _tree = edit.persist(&mut delta).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_mixed_batch_vs_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_batch");
    let mut data = BenchData::new(BENCH_SEED);

    // Pre-build a base tree of `base` keys, then apply a batch of `ops` updates
    // (a mix of inserts of fresh keys and deletes of existing keys).
    for size in [100usize, 1_000, 10_000] {
        let base_keys = data.random_buffers::<16>(size);
        let base_values: Vec<Vec<u8>> = data
            .random_buffers::<32>(size)
            .into_iter()
            .map(|value| value.to_vec())
            .collect();
        let fresh_keys = data.random_buffers::<16>(size / 2);
        let fresh_values: Vec<Vec<u8>> = data
            .random_buffers::<32>(size / 2)
            .into_iter()
            .map(|value| value.to_vec())
            .collect();
        // Delete the first half of the base keys.
        let delete_keys: Vec<[u8; 16]> = base_keys.iter().take(size / 2).copied().collect();

        group.bench_with_input(BenchmarkId::new("sequential", size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                let mut tree = PersistentTree::<[u8; 16], Vec<u8>>::empty();
                let mut delta = Delta::zero();
                for (key, value) in base_keys.iter().zip(base_values.iter()) {
                    tree = tree
                        .edit()
                        .insert(*key, value.clone(), &storage)
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
                for (key, value) in fresh_keys.iter().zip(fresh_values.iter()) {
                    tree = tree
                        .edit()
                        .insert(*key, value.clone(), &storage)
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
                for key in delete_keys.iter() {
                    tree = tree
                        .edit()
                        .delete(key, &storage)
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
            });
        });

        group.bench_with_input(BenchmarkId::new("batched", size), &size, |b, _| {
            b.to_async(runtime()).iter(|| async {
                let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                let mut edit = PersistentTree::<[u8; 16], Vec<u8>>::empty().edit();
                for (key, value) in base_keys.iter().zip(base_values.iter()) {
                    edit = edit.insert(*key, value.clone(), &storage).await.unwrap();
                }
                for (key, value) in fresh_keys.iter().zip(fresh_values.iter()) {
                    edit = edit.insert(*key, value.clone(), &storage).await.unwrap();
                }
                for key in delete_keys.iter() {
                    edit = edit.delete(key, &storage).await.unwrap();
                }
                let mut delta = Delta::zero();
                let _tree = edit.persist(&mut delta).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_batch_vs_sequential,
    bench_mixed_batch_vs_sequential
);
criterion_main!(benches);
