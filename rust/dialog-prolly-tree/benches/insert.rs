use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_prolly_tree::{GeometricDistribution, Tree};
use dialog_storage::{CborEncoder, MemoryStorageBackend, Storage};
use std::sync::Arc;
use tokio::sync::Mutex;

const BENCH_SEED: u64 = 42;

fn bench_insert_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_sequential");
    let mut data = BenchData::new(BENCH_SEED);

    for size in [10, 100, 1000, 10000] {
        let keys = data.sequential_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let storage = Arc::new(Mutex::new(Storage {
                        backend: MemoryStorageBackend::default(),
                        encoder: CborEncoder,
                    }));
                    let mut tree = Tree::<GeometricDistribution, _, _, _, _>::new(storage);

                    for (key, value) in keys.iter().zip(values.iter()) {
                        tree.set(key.to_vec(), value.to_vec()).await.unwrap();
                    }
                });
        });
    }

    group.finish();
}

fn bench_insert_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_random");
    let mut data = BenchData::new(BENCH_SEED);

    for size in [10, 100, 1000, 10000] {
        let keys = data.random_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let storage = Arc::new(Mutex::new(Storage {
                        backend: MemoryStorageBackend::default(),
                        encoder: CborEncoder,
                    }));
                    let mut tree = Tree::<GeometricDistribution, _, _, _, _>::new(storage);

                    for (key, value) in keys.iter().zip(values.iter()) {
                        tree.set(key.to_vec(), value.to_vec()).await.unwrap();
                    }
                });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_insert_sequential, bench_insert_random);
criterion_main!(benches);
