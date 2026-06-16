use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_search_tree::{ContentAddressedStorage, PersistentTree};
use dialog_storage::MemoryStorageBackend;

const BENCH_SEED: u64 = 42;

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete");
    let mut data = BenchData::new(BENCH_SEED);

    for size in [10, 100, 1000, 10000] {
        let keys = data.random_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                    let mut tree = PersistentTree::<[u8; 16], Vec<u8>>::empty();

                    // Insert all keys
                    for (key, value) in keys.iter().zip(values.iter()) {
                        tree = tree
                            .edit()
                            .insert(*key, value.to_vec(), &storage)
                            .await
                            .unwrap()
                            .persist()
                            .unwrap();
                    }

                    // Delete half of them
                    for key in keys.iter().take(size / 2) {
                        tree = tree
                            .edit()
                            .delete(key, &storage)
                            .await
                            .unwrap()
                            .persist()
                            .unwrap();
                    }
                });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_delete);
criterion_main!(benches);
