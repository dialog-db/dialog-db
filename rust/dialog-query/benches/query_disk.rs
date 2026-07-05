//! On-disk wall-clock benchmark for the query engine.
//!
//! Times the same select-by-attribute query as `query_memory`, but
//! against an on-disk environment rooted in the platform temp directory
//! (`NativeTempSpace`). This is the real-world latency signal where I/O
//! dominates.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-query --bench query_disk --features helpers
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

// The helper module is shared across benches; each bench exercises only
// part of its surface, so unused items are expected here.
#[path = "../src/helpers.rs"]
#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::BenchEnv;

const SIZES: &[usize] = &[100, 1_000];

const QUERY_ATTRIBUTE: &str = "item/name";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn bench_query_disk(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_disk");
    let rt = runtime();

    for &size in SIZES {
        let env = rt.block_on(async {
            let env = BenchEnv::temp().await.unwrap();
            env.seed(size).await.unwrap();
            env
        });

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| async { env.run_query(QUERY_ATTRIBUTE).await.unwrap() });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_query_disk);
criterion_main!(benches);
