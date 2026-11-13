//! The single-file archive against the `fs` archive, through a repository.
//!
//! Replays the Stack Exchange log, one branch commit per transaction, into
//! three spaces that differ only in where their blocks go:
//!
//! - `fs`: a file per block (the `fs` provider);
//! - `file/whole`: one append-only file, every block whole;
//! - `file/frames`: one append-only file, tree nodes as frames over tokens,
//!   each read rebuilding the node's block;
//! - `file/served`: the same file, with the environment holding it as its
//!   node source, so the tree reads frames in place through the file's
//!   mapping and builds no block.
//!
//! For each it reports the commit time, the bytes the blocks take on disk,
//! then, after dropping every handle and reopening from disk (an empty node
//! cache), a cold pass of reads and the same pass again warm: a scan of
//! every fact of every attribute the log writes, and point reads of single
//! entities. The OS page cache is warm for every store alike. It also
//! reports the heap each reopened repository holds once it has read: its
//! node cache above all, whose nodes are blocks for `fs` and rows into the
//! mapping for `file/served`.
//!
//! Run it with `DIALOG_SE_CSV=path/to/retro-facts.csv
//! DIALOG_BENCH_COMMITS=4096 cargo run --release -p dialog-baseline
//! --example file_archive_bench`. `DIALOG_BENCH_GROUP=k` commits every `k`
//! transactions of the log as one, to see what a commit's size costs; the
//! peak heap the commits reach, above the heap before the first, is
//! reported with the commit time.
// Measures the one-shot `.commit().publish().perform(..)` the gates were
// recorded with; moving it to `Branch::transact` is the maintainer's call.
#![allow(deprecated)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::path::Path;
use std::str::FromStr as _;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use dialog_baseline::se::{SeLog, se_instructions};
use dialog_capability::Subject;
use dialog_chronicle::{Chronicle, FILE_NAME, Options};
use dialog_corpus::{Entity, Fact, Instruction, Pick, Relation, Selector, Value};
use dialog_effects::storage::{Directory, Location};
use dialog_peer::helpers::{Seed, open_peer_as, test_owned};
use dialog_repository::RepositoryExt as _;
use dialog_storage::provider::storage::NativeSpace;
use dialog_storage::provider::{FileSystem, Space, Storage};
use futures_util::TryStreamExt as _;

/// Counts the bytes allocated and not yet freed.
struct Counting;

static ALLOCATED: AtomicU64 = AtomicU64::new(0);

/// The most `ALLOCATED` reached since it was last reset.
static PEAK: AtomicU64 = AtomicU64::new(0);

// SAFETY: forwards to the system allocator, only counting.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let now = ALLOCATED.fetch_add(layout.size() as u64, Relaxed) + layout.size() as u64;
        PEAK.fetch_max(now, Relaxed);
        // SAFETY: the caller upholds `alloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOCATED.fetch_sub(layout.size() as u64, Relaxed);
        // SAFETY: the caller upholds `dealloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static COUNTING: Counting = Counting;

type ChronicleSpace = Space<Chronicle, FileSystem, FileSystem, FileSystem, FileSystem>;

/// What one store measured.
#[derive(Default)]
struct Run {
    name: &'static str,
    /// The tree the last commit left, which every store must agree on.
    root: String,
    commit: Duration,
    /// The most heap the commits held above what was held before them.
    commit_heap: u64,
    disk: u64,
    allocated: u64,
    blocks: usize,
    cold_scan: Duration,
    warm_scan: Duration,
    facts: usize,
    cold_points: Duration,
    warm_points: Duration,
    /// Heap opening the repository took: the archive's index among it.
    open_heap: u64,
    /// Heap the scans left held, beyond the open: the node cache, mostly.
    scan_heap: u64,
    /// The same for the point reads.
    points_heap: u64,
    /// The nodes the node cache held after the scans, and what it charged
    /// for them.
    cached: (usize, u64),
    /// Small commits on the built database (five facts on an entity it
    /// holds, one commit each), their median and slowest time.
    small: (Duration, Duration),
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
    ($space:ty, $at:expr, $peer:expr, $name:expr, $create:expr) => {{
        let storage = test_owned(Storage::<$space>::new()).await;
        // Every identity the run mints comes from one seed, the same for
        // every store: a replica's keys and its line's identifier are part
        // of what it writes (a revision's record is keyed by its branch
        // and issuer), so fresh ones would write a differently shaped
        // tree, and the stores would not be doing the same work.
        let seed = Seed::new(733);
        let profile = open_peer_as(
            storage,
            Location::new($at.clone(), $peer.to_string()),
            seed.credential("profile").await?,
        )
        .await?;
        let operator = profile
            .session(b"file-archive-bench")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        let space = profile.space($name.to_string());
        let branch = if $create {
            space
                .create()
                .with_credential(seed.signer("space").await?)
                .perform(&operator)
                .await?
                .branch("main")
                .open()
                .perform(&operator)
                .await?
        } else {
            space
                .open()
                .perform(&operator)
                .await?
                .branch("main")
                .open()
                .perform(&operator)
                .await?
        };
        (operator, branch)
    }};
}

macro_rules! reads {
    ($operator:expr, $branch:expr, $attributes:expr, $entities:expr) => {{
        let started = Instant::now();
        let mut facts = 0;
        for attribute in $attributes {
            let rows: Vec<Fact> = $branch
                .claims()
                .select(Selector::new().the(Relation::from_str(attribute)?))
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
            let rows: Vec<Fact> = $branch
                .claims()
                .select(Selector::new().of(Entity::from_str(entity)?))
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
    ($space:ty, $name:expr, $directory:expr, $commits:expr, $attributes:expr, $entities:expr, $before:expr, $after:expr, $serve:expr) => {{
        let at = Directory::At($directory.to_string_lossy().into_owned());
        let mut run = Run {
            name: $name,
            ..Run::default()
        };
        let held = $before(&$directory)?;
        {
            let _entropy = Seed::new(733).entropy();
            let (operator, branch) = open!($space, at, "peer", "repo", true);
            let started = Instant::now();
            let before = ALLOCATED.load(Relaxed);
            PEAK.store(before, Relaxed);
            let group = std::env::var("DIALOG_BENCH_GROUP")
                .ok()
                .and_then(|group| group.parse::<usize>().ok())
                .unwrap_or(1)
                .max(1);
            for commits in $commits.chunks(group) {
                let mut instructions = Vec::new();
                for commit in commits {
                    instructions.extend(se_instructions(commit)?);
                }
                branch
                    .transaction()
                    .integrate(instructions.into_iter().collect())
                    .commit()
                    .publish()
                    .perform(&operator)
                    .await?;
            }
            run.commit = started.elapsed();
            run.commit_heap = PEAK.load(Relaxed).saturating_sub(before);
            run.root = branch
                .revision()
                .map(|revision| dialog_common::Blake3Hash::from(*revision.tree.hash()).to_string())
                .unwrap_or_default();
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
            let before = ALLOCATED.load(Relaxed);
            let (operator, branch) = open!($space, at, "peer", "repo", false);
            let opened = ALLOCATED.load(Relaxed);
            run.open_heap = opened.saturating_sub(before);
            (run.cold_scan, _, run.facts) = reads!(operator, branch, $attributes, &[] as &[String]);
            (run.warm_scan, _, _) = reads!(operator, branch, $attributes, &[] as &[String]);
            run.scan_heap = ALLOCATED.load(Relaxed).saturating_sub(opened);
            if let Some(cache) = dialog_search_tree::NodeCache::<
                dialog_corpus::Key,
                dialog_corpus::State<dialog_corpus::Datum>,
            >::of(&operator, branch.subject().did())
            {
                run.cached = (cache.len(), cache.bytes() as u64);
            }
        }
        {
            let (operator, branch) = open!($space, at, "peer", "repo", false);
            let opened = ALLOCATED.load(Relaxed);
            (_, run.cold_points, _) = reads!(operator, branch, &[] as &[String], $entities);
            (_, run.warm_points, _) = reads!(operator, branch, &[] as &[String], $entities);
            run.points_heap = ALLOCATED.load(Relaxed).saturating_sub(opened);
        }
        {
            // Small commits on the database as built: what an application's
            // edit costs once the data is there.
            let _entropy = Seed::new(734).entropy();
            let (operator, branch) = open!($space, at, "peer", "repo", false);
            let mut times = Vec::new();
            for (n, entity) in $entities.iter().cycle().take(100).enumerate() {
                let instructions: Vec<Instruction> = (0..5)
                    .map(|field| -> anyhow::Result<Instruction> {
                        Ok(Instruction::Assert(
                            Fact {
                                the: Relation::from_str(&format!("bench/small-{field}"))?,
                                of: Entity::from_str(entity)?,
                                is: Value::String(format!("edit {n}")),
                                cause: None,
                            },
                            Pick::Last,
                        ))
                    })
                    .collect::<anyhow::Result<_>>()?;
                let started = Instant::now();
                branch
                    .transaction()
                    .integrate(instructions.into_iter().collect())
                    .commit()
                    .publish()
                    .perform(&operator)
                    .await?;
                times.push(started.elapsed());
            }
            times.sort();
            run.small = (times[times.len() / 2], times[times.len() - 1]);
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
fn hold(options: Options) -> impl Fn(&Path) -> anyhow::Result<Option<Chronicle>> {
    move |directory: &Path| {
        let space = directory.join("repo");
        std::fs::create_dir_all(&space)?;
        Ok(Some(Chronicle::open_with(
            space.join(FILE_NAME),
            options.clone(),
        )?))
    }
}

fn describe(archive: &Option<Chronicle>) -> String {
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

    let nothing = |_: &Path| -> anyhow::Result<Option<Chronicle>> { Ok(None) };
    let mut runs = Vec::new();
    // `DIALOG_BENCH_STORES=fs,file/frames` runs only those.
    let stores = std::env::var("DIALOG_BENCH_STORES")
        .unwrap_or_else(|_| "fs,file/whole,file/frames,file/served".to_string());
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
                describe,
                false
            ),
            "file/whole" => run!(
                ChronicleSpace,
                "file/whole",
                directory,
                &log.transactions,
                &attributes,
                &entities,
                hold(Options {
                    frames: false,
                    ..Options::default()
                }),
                describe,
                false
            ),
            "file/frames" => run!(
                ChronicleSpace,
                "file/frames",
                directory,
                &log.transactions,
                &attributes,
                &entities,
                hold(Options::default()),
                describe,
                false
            ),
            _ => run!(
                ChronicleSpace,
                "file/served",
                directory,
                &log.transactions,
                &attributes,
                &entities,
                hold(Options::default()),
                describe,
                true
            ),
        };
        println!(
            "{:<12} commit {:>9} ({:.2} ms/transaction, peak heap {})  disk {:>10} (allocated {:>10}, {} files)  \
             cold scan {:>8}  warm scan {:>8}  ({} facts)  cold points {:>8}  warm points {:>8}  \
             heap: open {:>9}, held by scans {:>9}, by points {:>9}; \
             node cache after scans: {} nodes, charged {}",
            run.name,
            ms(run.commit),
            run.commit.as_secs_f64() * 1000.0 / commits.len() as f64,
            mib(run.commit_heap),
            mib(run.disk),
            mib(run.allocated),
            run.blocks,
            ms(run.cold_scan),
            ms(run.warm_scan),
            run.facts,
            ms(run.cold_points),
            ms(run.warm_points),
            mib(run.open_heap),
            mib(run.scan_heap),
            mib(run.points_heap),
            run.cached.0,
            mib(run.cached.1),
        );
        println!(
            "{:<12} small commits on the built database: median {:.1} ms, slowest {:.1} ms",
            "",
            run.small.0.as_secs_f64() * 1e3,
            run.small.1.as_secs_f64() * 1e3
        );
        println!("{:<12} root {}", "", run.root);
        if !run.detail.is_empty() {
            println!("{:<12} {}", "", run.detail);
        }
        let file = directory.join("repo").join(FILE_NAME);
        // `DIALOG_BENCH_KEEP=dir` keeps each store's file there.
        if file.exists()
            && let Ok(keep) = std::env::var("DIALOG_BENCH_KEEP")
        {
            std::fs::create_dir_all(&keep)?;
            std::fs::copy(&file, Path::new(&keep).join(which.replace('/', "-")))?;
        }
        if file.exists() {
            // A reader's open: its slots, and the root of the index in them.
            let started = Instant::now();
            let reader = Chronicle::open(&file)?;
            let opened = started.elapsed();
            println!(
                "{:<12} reopened in {}: index {} in memory",
                "",
                ms(opened),
                mib(reader.stats().index_memory as u64)
            );
        }
        if let Some(first) = runs.first() {
            let first: &Run = first;
            anyhow::ensure!(
                run.root == first.root,
                "{} ended on {}, {} on {}: the stores did not do the same work",
                first.name,
                first.root,
                run.name,
                run.root
            );
        }
        runs.push(run);
    }
    Ok(())
}
