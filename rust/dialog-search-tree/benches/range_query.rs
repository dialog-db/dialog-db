use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_common::helpers::BenchData;
use dialog_search_tree::{ContentAddressedStorage, Tree};
use dialog_storage::MemoryStorageBackend;
use futures_util::StreamExt;

const BENCH_SEED: u64 = 42;

fn make_key(index: usize) -> [u8; 16] {
    let mut key = [0; 16];
    let index_bytes = (index as u32).to_le_bytes();
    key[..index_bytes.len()].copy_from_slice(&index_bytes);
    key
}

fn bench_range_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query");
    let mut data = BenchData::new(BENCH_SEED);

    const TREE_SIZE: usize = 10100;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (tree, storage) = runtime.block_on(async {
        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 16], Vec<u8>>::empty();

        let keys = data.random_buffers(TREE_SIZE);
        let values = data.random_buffers::<32>(TREE_SIZE);

        for (key, value) in keys.iter().zip(values.iter()) {
            tree = tree.insert(*key, value.to_vec(), &storage).await.unwrap();
        }

        (tree, storage)
    });

    for range_size in [10usize, 100, 1000, 10000] {
        let start = make_key(100);
        let end = make_key(100 + range_size);

        group.bench_with_input(
            BenchmarkId::from_parameter(range_size),
            &range_size,
            |b, _| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let stream = tree.stream_range(start..end, &storage);
                        futures_util::pin_mut!(stream);
                        let mut count = 0;
                        while let Some(result) = stream.next().await {
                            result.unwrap();
                            count += 1;
                        }
                        count
                    });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_range_query);
criterion_main!(benches);
