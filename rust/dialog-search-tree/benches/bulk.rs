use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_search_tree::{ContentAddressedStorage, Tree};
use dialog_storage::MemoryStorageBackend;

const BENCH_SEED: u64 = 42;

/// Compares materializing a batch through a per-key insert loop against
/// the one-pass bulk build (`Tree::from_entries`). The insert loop
/// rewrites every touched segment once per insert; the bulk build ranks
/// every key and serializes every node exactly once. Both produce the
/// same canonical tree, so the gap between the two lines is pure
/// redundant work.
fn bench_build_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_sequential");
    let mut data = BenchData::new(BENCH_SEED);

    for size in [10, 100, 1000, 10000] {
        let keys = data.sequential_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        group.bench_with_input(BenchmarkId::new("insert_loop", size), &size, |b, _size| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                    let mut tree = Tree::<[u8; 16], Vec<u8>>::empty();

                    for (key, value) in keys.iter().zip(values.iter()) {
                        tree = tree.insert(*key, value.to_vec(), &storage).await.unwrap();
                    }
                });
        });

        group.bench_with_input(BenchmarkId::new("from_entries", size), &size, |b, _size| {
            b.iter(|| {
                Tree::<[u8; 16], Vec<u8>>::from_entries(
                    keys.iter()
                        .zip(values.iter())
                        .map(|(key, value)| (*key, value.to_vec())),
                )
                .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_build_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_random");
    let mut data = BenchData::new(BENCH_SEED);

    for size in [10, 100, 1000, 10000] {
        let keys = data.random_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        group.bench_with_input(BenchmarkId::new("insert_loop", size), &size, |b, _size| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                    let mut tree = Tree::<[u8; 16], Vec<u8>>::empty();

                    for (key, value) in keys.iter().zip(values.iter()) {
                        tree = tree.insert(*key, value.to_vec(), &storage).await.unwrap();
                    }
                });
        });

        group.bench_with_input(BenchmarkId::new("from_entries", size), &size, |b, _size| {
            b.iter(|| {
                Tree::<[u8; 16], Vec<u8>>::from_entries(
                    keys.iter()
                        .zip(values.iter())
                        .map(|(key, value)| (*key, value.to_vec())),
                )
                .unwrap()
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_build_sequential, bench_build_random);
criterion_main!(benches);
