use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_search_tree::helpers::Blocks;
use dialog_search_tree::{Delta, PersistentTree};

const BENCH_SEED: u64 = 42;

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    let mut data = BenchData::new(BENCH_SEED);

    for size in [10, 100, 1000, 10000] {
        let keys = data.random_buffers::<16>(size);
        let values = data.random_buffers::<32>(size);

        // Setup: create a tree with data
        let runtime = tokio::runtime::Runtime::new().unwrap();
        // The nodes the build seals are kept, as a tree read through a
        // store with a node cache finds them.
        let blocks = Blocks::new();
        let storage = dialog_search_tree::helpers::Holding::new(&blocks);
        let (tree, keys) = runtime.block_on(async {
            let mut tree = PersistentTree::<[u8; 16], Vec<u8>>::empty_in(
                dialog_search_tree::helpers::test_subject(),
            );
            let mut delta = Delta::zero();

            for (key, value) in keys.iter().zip(values.iter()) {
                tree = tree
                    .edit()
                    .insert(*key, value.to_vec(), &storage)
                    .await
                    .unwrap()
                    .persist(&mut delta)
                    .unwrap();
                // Flush after each persist so the next edit can load the nodes this persist created.
                for (_, node) in delta.flush() {
                    storage.store(node.buffer().clone());
                }
            }

            (tree, keys)
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
