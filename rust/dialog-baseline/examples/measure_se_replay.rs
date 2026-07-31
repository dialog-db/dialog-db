//! Byte-volume measurement: how many bytes each SE commit writes to
//! storage.
//!
//! The callgrind decomposition of the in-memory SE commit shows memcpy +
//! blake3 + allocator as ~60% of all instructions, which reads as "the
//! whole root frame (entries + full novelty buffer) is re-encoded,
//! re-copied, and re-hashed on every commit". This target quantifies
//! that directly by wrapping the memory backend in [`MeasuredStorage`]
//! and reporting written bytes per commit over the replay, windowed so
//! the buffer-fill sawtooth is visible.
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example measure_se_replay -- 500
//! ```

use dialog_artifacts::{ArtifactStoreMut as _, Artifacts};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_storage::{Blake3Hash, MeasuredStorage, MemoryStorageBackend};
use futures_util::stream;

fn main() -> anyhow::Result<()> {
    let count: usize = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(500);
    let window: usize = std::env::args()
        .nth(2)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(50);
    let log = SeLog::load(count)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let backend = MeasuredStorage::new(MemoryStorageBackend::<Blake3Hash, Vec<u8>>::default());
        let mut store = Artifacts::anonymous(backend.clone()).await?;
        let mut committed = 0usize;
        let (mut last_writes, mut last_bytes) = (0usize, 0usize);
        let (mut last_reads, mut last_read_bytes) = (0usize, 0usize);
        let mut window_started = std::time::Instant::now();
        println!("commits  writes  set_bytes  (per-commit in window)");
        for commit in &log.transactions {
            store
                .commit(stream::iter(se_instructions(commit)?))
                .await?;
            committed += 1;
            if committed.is_multiple_of(window) {
                let (writes, bytes) = (backend.writes(), backend.write_bytes());
                let (reads, read_bytes) = (backend.reads(), backend.read_bytes());
                println!(
                    "{committed:7}  {:.1} sets / {:.0} B written, {:.1} gets / {:.0} B read, {:.0} us per commit\n         phases: {} (window totals)",
                    (writes - last_writes) as f64 / window as f64,
                    (bytes - last_bytes) as f64 / window as f64,
                    (reads - last_reads) as f64 / window as f64,
                    (read_bytes - last_read_bytes) as f64 / window as f64,
                    window_started.elapsed().as_micros() as f64 / window as f64,
                    dialog_search_tree::audit::phase_report(),
                );
                (last_writes, last_bytes) = (writes, bytes);
                (last_reads, last_read_bytes) = (reads, read_bytes);
                window_started = std::time::Instant::now();
            }
        }
        println!(
            "total: {} commits, {} facts, {} sets, {} bytes written ({:.0} bytes/commit), {} gets, {} bytes read",
            committed,
            log.fact_count(),
            backend.writes(),
            backend.write_bytes(),
            backend.write_bytes() as f64 / committed as f64,
            backend.reads(),
            backend.read_bytes(),
        );
        {
            use std::sync::atomic::Ordering;

            use dialog_search_tree::audit;
            println!(
                "widen: {} checks, {} runs, {} skipped; rejects: {} novelty, {} interior (min {} / tail {} / last {} / veto-del {}), {} plan",
                audit::WIDEN_CHECKS.load(Ordering::Relaxed),
                audit::WIDEN_RUNS.load(Ordering::Relaxed),
                audit::WIDEN_SKIPS.load(Ordering::Relaxed),
                audit::WIDEN_NOVELTY_REJECTS.load(Ordering::Relaxed),
                audit::WIDEN_INTERIOR_REJECTS.load(Ordering::Relaxed),
                audit::WIDEN_REJECT_MIN.load(Ordering::Relaxed),
                audit::WIDEN_REJECT_TAIL.load(Ordering::Relaxed),
                audit::WIDEN_REJECT_LAST.load(Ordering::Relaxed),
                audit::WIDEN_REJECT_VETO_DELETE.load(Ordering::Relaxed),
                audit::WIDEN_PLAN_REJECTS.load(Ordering::Relaxed),
            );
            println!(
                "widen compressed: {} quiet, {} widen, {} fallback",
                audit::WIDEN_COMPRESSED_QUIET.load(Ordering::Relaxed),
                audit::WIDEN_COMPRESSED_WIDEN.load(Ordering::Relaxed),
                audit::WIDEN_COMPRESSED_FALLBACK.load(Ordering::Relaxed),
            );
        }
        Ok(())
    })
}
