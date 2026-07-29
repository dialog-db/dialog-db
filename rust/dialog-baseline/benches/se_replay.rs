//! Realistic-workload benchmark: replay of the Stack Exchange fact log.
//!
//! Replays a transaction-ordered fact log (real commit boundaries, real
//! supersession churn — see `notes/benchmark-dataset.md`) into both stores,
//! one commit per transaction, then measures reads against the replayed
//! store: a value-indexed lookup (`se.post/kind = "question"`, the VAE
//! shape) and a point read of a heavily-superseded pair (`se.post/title`).
//!
//! Data source: the deterministic synthetic approximation by default;
//! point `DIALOG_SE_CSV` at a transformed dump (produced by
//! `scripts/se-transform.py`) to run against real data. `DIALOG_SE_TXNS`
//! overrides the replayed transaction count (default 500 for writes,
//! 2000 for the read-side seed).
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-baseline --bench se_replay
//! DIALOG_SE_CSV=path/to/retro-facts.csv cargo bench -p dialog-baseline --bench se_replay
//! ```

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::SeLog;
use dialog_baseline::{DialogFacts, DialogMode, SqliteFacts, SqliteMode};

const SQLITE_MODES: &[(&str, SqliteMode)] = &[
    ("sqlite_mem", SqliteMode::Memory),
    ("sqlite_disk", SqliteMode::Disk),
    ("sqlite_disk_nosync", SqliteMode::DiskNoSync),
];

const DIALOG_MODES: &[(&str, DialogMode)] = &[
    ("dialog_mem", DialogMode::Memory),
    ("dialog_disk", DialogMode::Disk),
];

/// A fresh multi-threaded tokio runtime for driving the async dialog side.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime")
}

/// Transaction count from `DIALOG_SE_TXNS`, or the given default.
fn txn_count(default: usize) -> usize {
    std::env::var("DIALOG_SE_TXNS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(default)
}

fn bench_replay_write(c: &mut Criterion) {
    let rt = runtime();
    let size = txn_count(500);
    let log = SeLog::load(size).expect("load SE log");
    println!(
        "se_replay_write txns={} facts={}",
        log.transactions.len(),
        log.fact_count()
    );

    let mut group = c.benchmark_group("se_replay_write");
    group.throughput(criterion::Throughput::Elements(
        log.transactions.len() as u64
    ));
    group.sample_size(10);

    for (label, mode) in SQLITE_MODES {
        group.bench_with_input(BenchmarkId::new(*label, size), &log, |b, log| {
            b.iter_batched(
                || SqliteFacts::open(*mode).expect("open sqlite"),
                |mut store| {
                    store.replay_se(log).expect("replay");
                    store
                },
                BatchSize::PerIteration,
            );
        });
    }

    for (label, mode) in DIALOG_MODES {
        group.bench_with_input(BenchmarkId::new(*label, size), &log, |b, log| {
            b.iter_batched(
                || rt.block_on(async { DialogFacts::open(*mode).await.expect("open dialog") }),
                |mut store| {
                    rt.block_on(async { store.replay_se(log).await.expect("replay") });
                    store
                },
                BatchSize::PerIteration,
            );
        });
    }

    // The repository layer: the same replay through `Branch::commit`, the
    // surface applications actually write through.
    group.bench_with_input(BenchmarkId::new("repo_mem", size), &log, |b, log| {
        b.iter_batched(
            || rt.block_on(async { DialogRepo::volatile().await.expect("open repo") }),
            |repo| {
                rt.block_on(async { repo.replay_se(log).await.expect("replay") });
                repo
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_with_input(BenchmarkId::new("repo_disk", size), &log, |b, log| {
        b.iter_batched(
            || {
                dialog_baseline::repo::clean_temp_storage();
                rt.block_on(async { DialogRepo::temp().await.expect("open repo") })
            },
            |repo| {
                rt.block_on(async { repo.replay_se(log).await.expect("replay") });
                repo
            },
            BatchSize::PerIteration,
        );
    });
    // DCAA single-file archive. Durability caveat for honest comparison:
    // this row fsyncs once per commit, while repo_disk's file-per-block
    // archive never fsyncs (sqlite_disk is the durable control there).
    group.bench_with_input(BenchmarkId::new("repo_dcaa", size), &log, |b, log| {
        b.iter_batched(
            || {
                dialog_baseline::repo::clean_temp_storage();
                rt.block_on(async { DialogRepo::dcaa().await.expect("open repo") })
            },
            |repo| {
                rt.block_on(async { repo.replay_se(log).await.expect("replay") });
                repo
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn bench_replay_reads(c: &mut Criterion) {
    let rt = runtime();
    let size = txn_count(2000);
    let log = SeLog::load(size).expect("load SE log");
    // A post that carries a title (titles are edited, so this pair is
    // superseded in the log with high likelihood).
    let titled_post = log
        .transactions
        .iter()
        .flatten()
        .find(|fact| fact.the == "se.post/title")
        .map(|fact| fact.of.clone())
        .expect("log contains a titled post");

    let sqlite: Vec<(&str, SqliteFacts)> = SQLITE_MODES
        .iter()
        .map(|(label, mode)| {
            let mut store = SqliteFacts::open(*mode).expect("open sqlite");
            store.replay_se(&log).expect("seed sqlite");
            (*label, store)
        })
        .collect();
    let dialog: Vec<(&str, DialogFacts)> = DIALOG_MODES
        .iter()
        .map(|(label, mode)| {
            let store = rt.block_on(async {
                let mut store = DialogFacts::open(*mode).await.expect("open dialog");
                store.replay_se(&log).await.expect("seed dialog");
                store
            });
            (*label, store)
        })
        .collect();

    let mut group = c.benchmark_group("se_kind_lookup");
    for (label, store) in &sqlite {
        group.bench_with_input(BenchmarkId::new(*label, size), store, |b, store| {
            b.iter(|| store.se_by_kind("question").expect("kind lookup"));
        });
    }
    for (label, store) in &dialog {
        group.bench_with_input(BenchmarkId::new(*label, size), store, |b, store| {
            b.iter(|| rt.block_on(async { store.se_by_kind("question").await.expect("kind") }));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("se_title_get");
    for (label, store) in &sqlite {
        group.bench_with_input(BenchmarkId::new(*label, size), store, |b, store| {
            b.iter(|| store.se_title(&titled_post).expect("title"));
        });
    }
    for (label, store) in &dialog {
        group.bench_with_input(BenchmarkId::new(*label, size), store, |b, store| {
            b.iter(|| rt.block_on(async { store.se_title(&titled_post).await.expect("title") }));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_replay_write, bench_replay_reads);
criterion_main!(benches);
