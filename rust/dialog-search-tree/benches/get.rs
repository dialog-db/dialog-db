use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_search_tree::{ContentAddressedStorage, Tree};
use dialog_storage::MemoryStorageBackend;

const BENCH_SEED: u64 = 42;

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    let mut data = BenchData::new(BENCH_SEED);

    for size in [10, 100, 1000, 10000] {
        let keys = data.random_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        // Setup: create a tree with data
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (tree, storage, keys) = runtime.block_on(async {
            let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
            let mut tree = Tree::<[u8; 16], Vec<u8>>::empty();

            for (key, value) in keys.iter().zip(values.iter()) {
                tree = tree.insert(*key, value.to_vec(), &storage).await.unwrap();
            }

            (tree, storage, keys)
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            // Benchmark: get the first key
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async { tree.get(&keys[0], &storage).await.unwrap() });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_get);
criterion_main!(benches);
