//! Read-count benchmark for a multi-premise concept query (a join).
//!
//! Where `query_reads` times a single attribute scan — one premise, no
//! ordering choice for the planner — this bench drives the public
//! `Query::<Stuff>` concept query: a `Stuff` joins `stuff/name` and
//! `stuff/role` on a shared entity via the planner's implicit two-attribute
//! rule. Evaluating it issues many `Provider<Select>` calls (per premise,
//! and once per outer binding for the inner premise); every one routes
//! through a `CountingStore` over a single shared `ReadJournal`, so the
//! headline metric — total block fetches across the *whole* planned query —
//! is the signal the planner's round-trip optimization actually moves.
//!
//! A join touches more blocks than a single scan, so the printed read
//! counts here are expected to exceed the `query_reads` counts at the same
//! fact-base size.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-query --bench query_join --features helpers
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

// The helper module is shared across benches; each bench exercises only
// part of its surface, so unused items are expected here.
#[path = "../src/helpers.rs"]
#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::BenchEnv;

const SIZES: &[usize] = &[100, 1_000];

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn bench_query_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_join");
    let rt = runtime();

    for &size in SIZES {
        // Seed once per size, off the timed path; the query itself does not
        // mutate the branch, so it can be re-run deterministically.
        let env = rt.block_on(async {
            let env = BenchEnv::volatile().await.unwrap();
            env.seed_stuff(size).await.unwrap();
            env
        });

        // Report the read counts once per size — criterion measures time,
        // not counts, so surface the headline metric directly.
        let run = rt.block_on(env.query_stuff()).unwrap();
        println!(
            "query_join size={size} results={} reads={} unique_reads={}",
            run.results_len, run.reads, run.unique_reads,
        );

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| async { env.query_stuff().await.unwrap() });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_query_join);
criterion_main!(benches);
