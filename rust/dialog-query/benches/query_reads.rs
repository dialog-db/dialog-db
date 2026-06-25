//! Read-count benchmark for the query engine.
//!
//! The headline metric: how many block fetches a query triggers. This is
//! the planner's true objective (minimize round-trips) and is
//! deterministic and machine-independent. We seed a volatile environment,
//! run a select-by-attribute query through the real branch-select path
//! wrapped in a `JournaledStorage`, print the recorded read counts once
//! per fact-base size, and also let criterion time the journaled query.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-query --bench query_reads --features helpers
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

// The helper module is shared across benches; each bench exercises only
// part of its surface, so unused items are expected here.
#[path = "../src/helpers.rs"]
#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::BenchEnv;

const SIZES: &[usize] = &[100, 1_000, 10_000];

const QUERY_ATTRIBUTE: &str = "item/name";

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn bench_query_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_reads");
    let rt = runtime();

    for &size in SIZES {
        let env = rt.block_on(async {
            let env = BenchEnv::volatile().await.unwrap();
            env.seed(size).await.unwrap();
            env
        });

        // Report the read counts once per size — criterion measures time,
        // not counts, so surface the headline metric directly.
        let run = rt.block_on(env.run_query(QUERY_ATTRIBUTE)).unwrap();
        println!(
            "query_reads size={size} results={} reads={} unique_reads={}",
            run.len(),
            run.reads,
            run.unique_reads,
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| async { env.run_query(QUERY_ATTRIBUTE).await.unwrap() });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_query_reads);
criterion_main!(benches);
