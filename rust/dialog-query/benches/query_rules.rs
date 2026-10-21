//! Read-count benchmark for concept queries answered by a deductive rule.
//!
//! `query_join` measures the implicit rule over stored facts. This bench
//! registers one rule, `Member { title, level } :- Stuff { name, role }`,
//! and drives four public queries against a seeded `Stuff` fact base:
//!
//! - `stuff`: the `Stuff` join itself. The rule reads `stuff/*` but
//!   derives nothing the query selects, so its reads must match
//!   `query_join` exactly — the rule-free path must not pay for rules
//!   that exist elsewhere.
//! - `member`: the exact head. Every row is derived; this is the cost
//!   of one rule evaluation.
//! - `titled`: a subset of the head (`member/title` only). Under
//!   attribute-level resolution this sees the same rows as `member`
//!   for the same reads; under concept-keyed resolution it saw nothing.
//! - `member-of`: the point-shaped derived query, one entity bound.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-query --bench query_rules --features helpers
//! ```

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

#[path = "../src/helpers.rs"]
#[allow(dead_code, unused_imports)]
mod helpers;
use helpers::BenchEnv;

const SIZES: &[usize] = &[100, 1_000];

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

fn bench_query_rules(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_rules");
    let rt = runtime();

    for &size in SIZES {
        let (env, probe) = rt.block_on(async {
            let env = BenchEnv::volatile().await.unwrap();
            let entities = env.seed_stuff_returning(size).await.unwrap();
            (env, entities[size / 2].clone())
        });

        let stuff = rt.block_on(env.query_stuff_with_rule()).unwrap();
        let member = rt.block_on(env.query_member()).unwrap();
        let titled = rt.block_on(env.query_titled()).unwrap();
        let point = rt.block_on(env.query_member_of(&probe)).unwrap();
        for (name, run) in [
            ("stuff", &stuff),
            ("member", &member),
            ("titled", &titled),
            ("member-of", &point),
        ] {
            println!(
                "query_rules/{name} size={size} results={} reads={} unique_reads={}",
                run.results_len, run.reads, run.unique_reads,
            );
        }

        group.bench_with_input(BenchmarkId::new("stuff", size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| async { env.query_stuff_with_rule().await.unwrap() });
        });
        group.bench_with_input(BenchmarkId::new("member", size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| async { env.query_member().await.unwrap() });
        });
        group.bench_with_input(BenchmarkId::new("titled", size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| async { env.query_titled().await.unwrap() });
        });
        group.bench_with_input(BenchmarkId::new("member-of", size), &size, |b, _| {
            b.to_async(runtime())
                .iter(|| async { env.query_member_of(&probe).await.unwrap() });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_query_rules);
criterion_main!(benches);
