//! The single-file archive against the `fs` archive, through a repository.
//!
//! Replays the Stack Exchange log, one branch commit per transaction, into
//! three spaces that differ only in where their blocks go:
//!
//! - `fs`: a file per block (the `fs` provider);
//! - `file/whole`: one append-only file, every block whole;
//! - `file/frames`: one append-only file, tree nodes as frames over tokens.
//!
//! For each it reports the commit time, the bytes the blocks take on disk,
//! then, after dropping every handle and reopening from disk (an empty node
//! cache), a cold pass of reads and the same pass again warm: a scan of
//! every fact of every attribute the log writes, and point reads of single
//! entities. The OS page cache is warm for every store alike.
//!
//! Run it with `DIALOG_SE_CSV=path/to/retro-facts.csv
//! DIALOG_BENCH_COMMITS=4096 cargo run --release -p dialog-baseline
//! --example file_archive_bench`.

use std::collections::BTreeSet;
use std::path::Path;
use std::str::FromStr as _;
use std::time::{Duration, Instant};

use dialog_archive_file::{FILE_NAME, FileArchive, Options};
use dialog_artifacts::{Artifact, ArtifactSelector, Entity, Instruction, Relation};
use dialog_baseline::se::{SeLog, se_instructions};
use dialog_capability::Subject;
use dialog_effects::storage::{Directory, Location};
use dialog_peer::helpers::{open_peer, test_owned};
use dialog_repository::RepositoryExt as _;
use dialog_storage::provider::storage::NativeSpace;
use dialog_storage::provider::{FileSystem, Space, Storage};
use futures_util::TryStreamExt as _;

type FileArchiveSpace = Space<FileArchive, FileSystem, FileSystem, FileSystem, FileSystem>;

/// What one store measured.
#[derive(Default)]
struct Run {
    name: &'static str,
    commit: Duration,
    disk: u64,
    allocated: u64,
    blocks: usize,
    cold_scan: Duration,
    warm_scan: Duration,
    facts: usize,
    cold_points: Duration,
    warm_points: Duration,
    detail: String,
}

/// The bytes the files under `path` hold, and the bytes the filesystem
/// allocates for them.
fn disk_usage(path: &Path) -> std::io::Result<(u64, u64, usize)> {
    use std::os::unix::fs::MetadataExt as _;
    let mut total = (0u64, 0u64, 0usize);
    if path.is_file() {
        let metadata = std::fs::metadata(path)?;
        return Ok((metadata.len(), metadata.blocks() * 512, 1));
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let (bytes, allocated, files) = disk_usage(&entry.path())?;
        total.0 += bytes;
        total.1 += allocated;
        total.2 += files;
    }
    Ok(total)
}

macro_rules! open {
    ($space:ty, $at:expr, $peer:expr, $name:expr) => {{
        let storage = test_owned(Storage::<$space>::new()).await;
        let profile = open_peer(storage, Location::new($at.clone(), $peer.to_string())).await?;
        let operator = profile
            .session(b"file-archive-bench")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let repository = profile
            .space($name.to_string())
            .open()
            .perform(&operator)
            .await?;
        let branch = repository.branch("main").open().perform(&operator).await?;
        (operator, branch)
    }};
}

macro_rules! reads {
    ($operator:expr, $branch:expr, $attributes:expr, $entities:expr) => {{
        let started = Instant::now();
        let mut facts = 0;
        for attribute in $attributes {
            let rows: Vec<Artifact> = $branch
                .claims()
                .select(ArtifactSelector::new().the(Relation::from_str(attribute)?))
                .to_owned()
                .perform(&$operator)
                .await?
                .try_collect()
                .await?;
            facts += rows.len();
        }
        let scan = started.elapsed();
        let started = Instant::now();
        for entity in $entities {
            let rows: Vec<Artifact> = $branch
                .claims()
                .select(ArtifactSelector::new().of(Entity::from_str(entity)?))
                .to_owned()
                .perform(&$operator)
                .await?
                .try_collect()
                .await?;
            anyhow::ensure!(!rows.is_empty(), "{entity} has facts");
        }
        (scan, started.elapsed(), facts)
    }};
}

macro_rules! run {
    ($space:ty, $name:expr, $directory:expr, $commits:expr, $attributes:expr, $entities:expr, $before:expr, $after:expr) => {{
        let at = Directory::At($directory.to_string_lossy().into_owned());
        let mut run = Run {
            name: $name,
            ..Run::default()
        };
        let held = $before(&$directory)?;
        {
            let (operator, branch) = open!($space, at, "peer", "repo");
            let started = Instant::now();
            for commit in $commits {
                branch
                    .transaction()
                    .integrate(se_instructions(commit)?.into_iter().collect())
                    .commit()
                    .publish()
                    .perform(&operator)
                    .await?;
            }
            run.commit = started.elapsed();
        }
        let blocks = $directory.join("repo");
        let blocks = if blocks.join(FILE_NAME).exists() {
            blocks.join(FILE_NAME)
        } else {
            blocks.join("archive")
        };
        (run.disk, run.allocated, run.blocks) = disk_usage(&blocks)?;
        run.detail = $after(&held);
        drop(held);
        {
            let (operator, branch) = open!($space, at, "peer", "repo");
            (run.cold_scan, _, run.facts) = reads!(operator, branch, $attributes, &[] as &[String]);
            (run.warm_scan, _, _) = reads!(operator, branch, $attributes, &[] as &[String]);
        }
        {
            let (operator, branch) = open!($space, at, "peer", "repo");
            (_, run.cold_points, _) = reads!(operator, branch, &[] as &[String], $entities);
            (_, run.warm_points, _) = reads!(operator, branch, &[] as &[String], $entities);
        }
        run
    }};
}

fn mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / (1024.0 * 1024.0))
}

fn ms(duration: Duration) -> String {
    format!("{:.0} ms", duration.as_secs_f64() * 1000.0)
}

/// Opens the archive file ahead of the space, with `options`: the space's
/// open of the same path shares this handle, so it writes with them.
fn hold(options: Options) -> impl Fn(&Path) -> anyhow::Result<Option<FileArchive>> {
    move |directory: &Path| {
        let space = directory.join("repo");
        std::fs::create_dir_all(&space)?;
        Ok(Some(FileArchive::open_with(
            space.join(FILE_NAME),
            options.clone(),
        )?))
    }
}

fn describe(archive: &Option<FileArchive>) -> String {
    let Some(archive) = archive else {
        return String::new();
    };
    let stats = archive.stats();
    format!(
        "{} whole ({}), {} frames ({} against another, {}), {} tokens ({}); index {} in memory (file pages mapped, not counted)\n\
         {:<12} appends: split {}, encode {}, verify {}, write {}",
        stats.blocks,
        mib(stats.block_bytes),
        stats.frames,
        stats.frames_against,
        mib(stats.frame_bytes),
        stats.tokens,
        mib(stats.token_bytes),
        mib(stats.index_memory as u64),
        "",
        ms(stats.split),
        ms(stats.encode),
        ms(stats.verify),
        ms(stats.write),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let count = std::env::var("DIALOG_BENCH_COMMITS")
        .ok()
        .and_then(|count| count.parse().ok())
        .unwrap_or(1024);
    let log = SeLog::load(count)?;
    let commits = log
        .transactions
        .iter()
        .map(|commit| se_instructions(commit))
        .collect::<anyhow::Result<Vec<Vec<Instruction>>>>()?;
    let mut attributes = BTreeSet::new();
    let mut entities = Vec::new();
    for instruction in commits.iter().flatten() {
        if let Instruction::Assert(artifact, _) = instruction {
            attributes.insert(artifact.the.to_string());
            if entities.len() < 4096 {
                entities.push(artifact.of.to_string());
            }
        }
    }
    // Every 16th entity a commit wrote, at most 256.
    let entities: Vec<String> = entities.into_iter().step_by(16).take(256).collect();
    println!(
        "{} commits, {} facts, {} attributes; {} point reads",
        commits.len(),
        log.fact_count(),
        attributes.len(),
        entities.len()
    );

    let nothing = |_: &Path| -> anyhow::Result<Option<FileArchive>> { Ok(None) };
    let mut runs = Vec::new();
    // `DIALOG_BENCH_STORES=fs,file/frames` runs only those.
    let stores = std::env::var("DIALOG_BENCH_STORES")
        .unwrap_or_else(|_| "fs,file/whole,file/frames".to_string());
    for which in stores.split(',') {
        let temporary = tempfile::tempdir()?;
        let directory = temporary.path().to_path_buf();
        let run = match which {
            "fs" => run!(
                NativeSpace,
                "fs",
                directory,
                &log.transactions,
                &attributes,
                &entities,
                nothing,
                describe
            ),
            "file/whole" => run!(
                FileArchiveSpace,
                "file/whole",
                directory,
                &log.transactions,
                &attributes,
                &entities,
                hold(Options {
                    frames: false,
                    ..Options::default()
                }),
                describe
            ),
            _ => run!(
                FileArchiveSpace,
                "file/frames",
                directory,
                &log.transactions,
                &attributes,
                &entities,
                hold(Options::default()),
                describe
            ),
        };
        println!(
            "{:<12} commit {:>9} ({:.2} ms/commit)  disk {:>10} (allocated {:>10}, {} files)  \
             cold scan {:>8}  warm scan {:>8}  ({} facts)  cold points {:>8}  warm points {:>8}",
            run.name,
            ms(run.commit),
            run.commit.as_secs_f64() * 1000.0 / commits.len() as f64,
            mib(run.disk),
            mib(run.allocated),
            run.blocks,
            ms(run.cold_scan),
            ms(run.warm_scan),
            run.facts,
            ms(run.cold_points),
            ms(run.warm_points),
        );
        if !run.detail.is_empty() {
            println!("{:<12} {}", "", run.detail);
        }
        let file = directory.join("repo").join(FILE_NAME);
        if file.exists() {
            // A reader's open, from the checkpoint and the records after it.
            let started = Instant::now();
            let reader = FileArchive::open(&file)?;
            let opened = started.elapsed();
            println!(
                "{:<12} reopened in {}: index {} in memory",
                "",
                ms(opened),
                mib(reader.stats().index_memory as u64)
            );
        }
        runs.push(run);
    }
    Ok(())
}
