//! Side-by-side criterion benchmarks: dialog-db vs SQLite on identical
//! fact workloads.
//!
//! Five operations × five store configurations, all in shared criterion
//! groups so each report page shows the stores next to each other:
//!
//! - `write_small_txns`: N entities committed one transaction each (the
//!   interactive-edit shape; the Stack Exchange dataset's p50 is 1 fact
//!   per transaction).
//! - `write_batch`: N entities in one transaction (bulk load).
//! - `point_get`: value of `(entity, stuff/name)` for a rotating entity.
//! - `attr_scan`: all `stuff/name` facts.
//! - `join`: `(entity, name, role)` two-attribute join.
//!
//! Store configurations: `sqlite_mem`, `sqlite_disk` (WAL +
//! `synchronous=NORMAL`), `sqlite_disk_nosync` (`synchronous=OFF` — the
//! durability semantics dialog's fs backend has today), `dialog_mem`,
//! `dialog_disk`.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p dialog-baseline
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use dialog_baseline::repo::DialogRepo;
use dialog_baseline::{DialogFacts, DialogMode, FactRow, SqliteFacts, SqliteMode, generate_rows};

const WRITE_SMALL_SIZE: usize = 100;
const WRITE_BATCH_SIZE: usize = 1_000;
const READ_SIZE: usize = 1_000;

/// A fresh multi-threaded tokio runtime for driving the async dialog side.
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("tokio runtime")
}

const SQLITE_MODES: &[(&str, SqliteMode)] = &[
    ("sqlite_mem", SqliteMode::Memory),
    ("sqlite_disk", SqliteMode::Disk),
    ("sqlite_disk_nosync", SqliteMode::DiskNoSync),
];

const DIALOG_MODES: &[(&str, DialogMode)] = &[
    ("dialog_mem", DialogMode::Memory),
    ("dialog_disk", DialogMode::Disk),
];

/// A seeded pair of read-side stores, built once per configuration with
/// the same rows so read benches compare identical content.
struct Seeded {
    rows: Vec<FactRow>,
    sqlite: Vec<(&'static str, SqliteFacts)>,
    dialog: Vec<(&'static str, DialogFacts)>,
}

fn seed(size: usize) -> Seeded {
    let rows = generate_rows(size);
    let sqlite = SQLITE_MODES
        .iter()
        .map(|(label, mode)| {
            let mut store = SqliteFacts::open(*mode).expect("open sqlite");
            store.insert_one_transaction(&rows).expect("seed sqlite");
            (*label, store)
        })
        .collect();
    let rt = runtime();
    let dialog = DIALOG_MODES
        .iter()
        .map(|(label, mode)| {
            let store = rt.block_on(async {
                let mut store = DialogFacts::open(*mode).await.expect("open dialog");
                store
                    .insert_one_transaction(&rows)
                    .await
                    .expect("seed dialog");
                store
            });
            (*label, store)
        })
        .collect();
    Seeded {
        rows,
        sqlite,
        dialog,
    }
}

fn bench_writes(c: &mut Criterion) {
    let rt = runtime();
    for (group_name, size, per_row) in [
        ("write_small_txns", WRITE_SMALL_SIZE, true),
        ("write_batch", WRITE_BATCH_SIZE, false),
    ] {
        let mut group = c.benchmark_group(group_name);
        // Each sample writes `size` entities (2 facts each); report
        // throughput in entities so configurations are comparable.
        group.throughput(criterion::Throughput::Elements(size as u64));
        group.sample_size(10);
        let rows = generate_rows(size);

        for (label, mode) in SQLITE_MODES {
            group.bench_with_input(BenchmarkId::new(*label, size), &rows, |b, rows| {
                b.iter_batched(
                    || SqliteFacts::open(*mode).expect("open sqlite"),
                    |mut store| {
                        if per_row {
                            store.insert_per_row_transactions(rows).expect("insert");
                        } else {
                            store.insert_one_transaction(rows).expect("insert");
                        }
                        store
                    },
                    BatchSize::PerIteration,
                );
            });
        }

        for (label, mode) in DIALOG_MODES {
            group.bench_with_input(BenchmarkId::new(*label, size), &rows, |b, rows| {
                b.iter_batched(
                    || rt.block_on(async { DialogFacts::open(*mode).await.expect("open dialog") }),
                    |mut store| {
                        rt.block_on(async {
                            if per_row {
                                store
                                    .insert_per_row_transactions(rows)
                                    .await
                                    .expect("insert");
                            } else {
                                store.insert_one_transaction(rows).await.expect("insert");
                            }
                        });
                        store
                    },
                    BatchSize::PerIteration,
                );
            });
        }

        // The repository layer: the same rows through `Branch::commit`,
        // which is the surface applications actually write through
        // (version tags, history claims, signed revision record, head
        // publication on top of the same index writes).
        group.bench_with_input(BenchmarkId::new("repo_mem", size), &rows, |b, rows| {
            b.iter_batched(
                || rt.block_on(async { DialogRepo::volatile().await.expect("open repo") }),
                |repo| {
                    rt.block_on(async {
                        if per_row {
                            repo.insert_per_row_transactions(rows)
                                .await
                                .expect("insert");
                        } else {
                            repo.insert_one_transaction(rows).await.expect("insert");
                        }
                    });
                    repo
                },
                BatchSize::PerIteration,
            );
        });
        group.bench_with_input(BenchmarkId::new("repo_disk", size), &rows, |b, rows| {
            b.iter_batched(
                || {
                    dialog_baseline::repo::clean_temp_storage();
                    rt.block_on(async { DialogRepo::temp().await.expect("open repo") })
                },
                |repo| {
                    rt.block_on(async {
                        if per_row {
                            repo.insert_per_row_transactions(rows)
                                .await
                                .expect("insert");
                        } else {
                            repo.insert_one_transaction(rows).await.expect("insert");
                        }
                    });
                    repo
                },
                BatchSize::PerIteration,
            );
        });
        // DCAA single-file archive. Durability caveat for honest
        // comparison: this row fsyncs once per commit, while repo_disk's
        // file-per-block archive never fsyncs (sqlite_disk is the durable
        // control there).
        group.bench_with_input(BenchmarkId::new("repo_dcaa", size), &rows, |b, rows| {
            b.iter_batched(
                || {
                    dialog_baseline::repo::clean_temp_storage();
                    rt.block_on(async { DialogRepo::dcaa().await.expect("open repo") })
                },
                |repo| {
                    rt.block_on(async {
                        if per_row {
                            repo.insert_per_row_transactions(rows)
                                .await
                                .expect("insert");
                        } else {
                            repo.insert_one_transaction(rows).await.expect("insert");
                        }
                    });
                    repo
                },
                BatchSize::PerIteration,
            );
        });
        group.finish();
    }
}

fn bench_reads(c: &mut Criterion) {
    let rt = runtime();
    let seeded = seed(READ_SIZE);

    let mut group = c.benchmark_group("point_get");
    let cursor = AtomicUsize::new(0);
    for (label, store) in &seeded.sqlite {
        group.bench_with_input(BenchmarkId::new(*label, READ_SIZE), store, |b, store| {
            b.iter(|| {
                let row = &seeded.rows[cursor.fetch_add(1, Ordering::Relaxed) % seeded.rows.len()];
                store.point_get(&row.entity).expect("point get")
            });
        });
    }
    for (label, store) in &seeded.dialog {
        group.bench_with_input(BenchmarkId::new(*label, READ_SIZE), store, |b, store| {
            b.iter(|| {
                let row = &seeded.rows[cursor.fetch_add(1, Ordering::Relaxed) % seeded.rows.len()];
                rt.block_on(async { store.point_get(&row.entity).await.expect("point get") })
            });
        });
    }
    group.finish();

    let mut group = c.benchmark_group("attr_scan");
    group.throughput(criterion::Throughput::Elements(READ_SIZE as u64));
    for (label, store) in &seeded.sqlite {
        group.bench_with_input(BenchmarkId::new(*label, READ_SIZE), store, |b, store| {
            b.iter(|| store.attribute_scan().expect("scan"));
        });
    }
    for (label, store) in &seeded.dialog {
        group.bench_with_input(BenchmarkId::new(*label, READ_SIZE), store, |b, store| {
            b.iter(|| rt.block_on(async { store.attribute_scan().await.expect("scan") }));
        });
    }
    group.finish();

    let mut group = c.benchmark_group("join");
    group.throughput(criterion::Throughput::Elements(READ_SIZE as u64));
    group.sample_size(20);
    for (label, store) in &seeded.sqlite {
        group.bench_with_input(BenchmarkId::new(*label, READ_SIZE), store, |b, store| {
            b.iter(|| store.join().expect("join"));
        });
    }
    for (label, store) in &seeded.dialog {
        group.bench_with_input(BenchmarkId::new(*label, READ_SIZE), store, |b, store| {
            b.iter(|| rt.block_on(async { store.join().await.expect("join") }));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_writes, bench_reads);
criterion_main!(benches);
