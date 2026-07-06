//! Compares seeding an empty tree through a batched insert loop
//! ([`TransientTree::insert`] per key, one persist) against the one-pass
//! bottom-up bulk build ([`TransientTree::seed`]).
//!
//! The insert loop descends the transient spine and re-cuts the touched
//! path once per key; the bulk build ranks every key once and constructs
//! each node exactly once. Both produce the same canonical tree, so the
//! gap between the two lines is pure redundant work.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-search-tree --bench bulk
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_search_tree::{ContentAddressedStorage, Delta, PersistentTree};
use dialog_storage::MemoryStorageBackend;

const BENCH_SEED: u64 = 42;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn bench_seed(c: &mut Criterion, group_name: &str, sequential: bool) {
    let mut group = c.benchmark_group(group_name);
    let mut data = BenchData::new(BENCH_SEED);

    for size in [10usize, 100, 1_000, 10_000] {
        let keys: Vec<[u8; 16]> = if sequential {
            data.sequential_buffers(size)
        } else {
            data.random_buffers(size)
        };
        let values: Vec<Vec<u8>> = data
            .random_buffers::<32>(size)
            .into_iter()
            .map(|value| value.to_vec())
            .collect();

        group.bench_with_input(BenchmarkId::new("insert_loop", size), &size, |b, _| {
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

        group.bench_with_input(BenchmarkId::new("seed", size), &size, |b, _| {
            b.iter(|| {
                let edit = PersistentTree::<[u8; 16], Vec<u8>>::empty()
                    .edit()
                    .seed(keys.iter().zip(values.iter()).map(|(k, v)| (*k, v.clone())))
                    .unwrap();
                let mut delta = Delta::zero();
                let _tree = edit.persist(&mut delta).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_seed_sequential(c: &mut Criterion) {
    bench_seed(c, "seed_sequential", true);
}

fn bench_seed_random(c: &mut Criterion) {
    bench_seed(c, "seed_random", false);
}

criterion_group!(benches, bench_seed_sequential, bench_seed_random);
criterion_main!(benches);
