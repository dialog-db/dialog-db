//! Index-amplification measurement for the DCAA archive (review
//! amendment 1 validation).
//!
//! Replays the Stack Exchange log through the repository harness three
//! times — DCAA with the delta chain ON (fold threshold 32), DCAA with the
//! chain OFF (a complete merged index every commit, the pre-amendment spec
//! behavior), and the file-per-block archive as control — then reports the
//! archive bytes each run left on disk. The DCAA file is strictly
//! append-only, so its final length IS the total bytes ever written to it.
//!
//! Run with:
//!
//! ```sh
//! DIALOG_SE_CSV=path/to/retro-facts.csv \
//!     cargo run -p dialog-baseline --release --example dcaa_amplification
//! ```

use std::path::{Path, PathBuf};
use std::time::Instant;

use dialog_baseline::repo::DialogRepo;
use dialog_baseline::se::SeLog;

/// Sum of sizes and count of files under `dir` whose name passes `keep`.
fn walk(dir: &Path, keep: &dyn Fn(&Path) -> bool) -> (u64, usize) {
    let mut bytes = 0;
    let mut count = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if keep(&path) {
                bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
                count += 1;
            }
        }
    }
    (bytes, count)
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

struct Run {
    label: &'static str,
    seconds: f64,
    archive_bytes: u64,
    archive_files: usize,
}

/// One full replay in a fresh TMPDIR; `fold` is the `DIALOG_DCAA_FOLD`
/// setting (`None` = file-per-block control instead of DCAA).
fn replay(label: &'static str, base: &Path, log: &SeLog, fold: Option<&str>) -> Run {
    let scratch = base.join(label.replace(' ', "-"));
    std::fs::create_dir_all(&scratch).expect("scratch dir");
    // Both env vars are read at store-open time. Set while no runtime
    // threads exist yet: each replay builds its runtime after this point.
    unsafe {
        std::env::set_var("TMPDIR", &scratch);
        match fold {
            Some(threshold) => std::env::set_var("DIALOG_DCAA_FOLD", threshold),
            None => std::env::remove_var("DIALOG_DCAA_FOLD"),
        }
    }

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let start = Instant::now();
    rt.block_on(async {
        match fold {
            Some(_) => {
                let repo = DialogRepo::dcaa().await.expect("open dcaa repo");
                repo.replay_se(log).await.expect("replay");
            }
            None => {
                let repo = DialogRepo::temp().await.expect("open temp repo");
                repo.replay_se(log).await.expect("replay");
            }
        }
    });
    let seconds = start.elapsed().as_secs_f64();
    drop(rt);

    let (archive_bytes, archive_files) = match fold {
        // DCAA: the archive is the `.dialog` files; append-only, so final
        // size == total bytes written.
        Some(_) => walk(&scratch, &|path: &Path| {
            path.extension().is_some_and(|e| e == "dialog")
        }),
        // File-per-block: every file under an `archive/` directory.
        None => walk(&scratch, &|path: &Path| {
            path.ancestors()
                .any(|a| a.file_name().is_some_and(|n| n == "archive"))
        }),
    };
    // Remove this run's stores before the next config runs: the merged-
    // index-per-commit configuration alone can write hundreds of MiB and
    // the benchmark host's disk budget is tight.
    std::fs::remove_dir_all(&scratch).ok();
    Run {
        label,
        seconds,
        archive_bytes,
        archive_files,
    }
}

fn main() {
    let txns: usize = std::env::var("DIALOG_SE_TXNS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(500);
    let log = SeLog::load(txns).expect("load SE log");
    println!(
        "SE replay: {} transactions, {} facts",
        log.transactions.len(),
        log.fact_count()
    );

    let base: PathBuf =
        std::env::temp_dir().join(format!("dcaa-amplification-{}", std::process::id()));

    let runs = [
        replay("dcaa fold 32 (delta chain ON)", &base, &log, Some("32")),
        replay(
            "dcaa fold 0 (merged index every commit)",
            &base,
            &log,
            Some("0"),
        ),
        replay("file-per-block control (no fsync)", &base, &log, None),
    ];

    println!();
    println!("| configuration | wall | archive bytes | archive files |");
    println!("|---|---|---|---|");
    for run in &runs {
        println!(
            "| {} | {:.2} s | {:.2} MiB | {} |",
            run.label,
            run.seconds,
            mib(run.archive_bytes),
            run.archive_files
        );
    }
    let on = runs[0].archive_bytes as f64;
    let off = runs[1].archive_bytes as f64;
    println!();
    println!(
        "index amplification: merged-every-commit writes {:.2}x the bytes of the delta chain",
        off / on
    );

    std::fs::remove_dir_all(&base).ok();
}
