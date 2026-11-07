//! Memory census: what a replica's facts cost in memory while it is
//! queried, today and if entities, attributes and values were shared tokens.
//!
//! The question behind `research/single-file-format/idea.md` is not only disk: the
//! same entity, attribute and value bytes appear in the EAV, AEV and VAE
//! nodes, in every version of a node, and in every row a query returns. This
//! loads a replica's head tree into memory, then reads it through the same
//! paths a query does, under a counting allocator, and reports three layers:
//!
//! - **node cache**: the heap a warm tree holds after every fact has been
//!   scanned once (the blocks it keeps, plus what it derives from them),
//!   next to the bytes of the blocks themselves;
//! - **results**: the heap a materialized result of every fact holds
//!   (`scan_owned` into a `Vec<Artifact>`), next to how many of its bytes
//!   are distinct entity, attribute and value bytes;
//! - **token model**: what the same result would hold if each distinct
//!   entity, attribute and value were one shared token and a row held ids.
//!
//! ```sh
//! cargo run --release -p dialog-baseline --example memory_census -- path/to/replica
//! ```
//!
//! `path/to/replica` holds `archive/` (a file per block) and `blob/`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use dialog_artifacts::tree::{ArtifactTree, ArtifactTreeExt, spill_cache};
use dialog_artifacts::{Artifact, ArtifactSelector, Datum, Key, State, Value};
use dialog_baseline::replica::Replica;
use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{
    NodeBody, PersistentNode,
    helpers::{Holding, test_catalog},
};
use futures_util::{StreamExt as _, TryStreamExt as _};

/// Counts the bytes the process holds on the heap, and the most it held.
struct Counting;

static HELD: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let held = HELD.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(held, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        HELD.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, size) };
        if !moved.is_null() {
            if size > layout.size() {
                let held = HELD.fetch_add(size - layout.size(), Ordering::Relaxed)
                    + (size - layout.size());
                PEAK.fetch_max(held, Ordering::Relaxed);
            } else {
                HELD.fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn held() -> usize {
    HELD.load(Ordering::Relaxed)
}

fn mib(bytes: usize) -> String {
    format!("{:.1}", bytes as f64 / (1024.0 * 1024.0))
}

/// The directory of `archive` that holds tree nodes, and the root of the
/// largest tree among its blocks.
fn load(archive: &Path) -> anyhow::Result<(PathBuf, Blake3Hash, usize)> {
    let directory = archive.join("index");
    let directory = directory.as_path();
    let mut nodes: HashMap<Blake3Hash, PersistentNode<Key, State<Datum>>> = HashMap::new();
    let mut bytes = 0usize;
    let mut stack = vec![directory.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                stack.push(entry.path());
                continue;
            }
            let buffer = Buffer::from(std::fs::read(entry.path())?);
            bytes += buffer.as_ref().len();
            if let Ok(node) = PersistentNode::try_from(buffer.clone()) {
                nodes.insert(buffer.blake3_hash().clone(), node);
            }
        }
    }
    let mut linked = HashSet::new();
    for node in nodes.values() {
        if let NodeBody::Index(index) = node.body() {
            for at in 0..index.len() {
                linked.insert(index.hash_at(at)?.clone());
            }
        }
    }
    let reach = |root: &Blake3Hash| -> anyhow::Result<usize> {
        let (mut stack, mut seen) = (vec![root.clone()], HashSet::new());
        while let Some(hash) = stack.pop() {
            let Some(node) = nodes.get(&hash) else {
                continue;
            };
            if !seen.insert(hash) {
                continue;
            }
            if let NodeBody::Index(index) = node.body() {
                for at in 0..index.len() {
                    stack.push(index.hash_at(at)?.clone());
                }
            }
        }
        Ok(seen.len())
    };
    let mut best: Option<(usize, Blake3Hash)> = None;
    for root in nodes.keys().filter(|hash| !linked.contains(*hash)) {
        let reached = reach(root)?;
        if best.as_ref().is_none_or(|(most, _)| reached > *most) {
            best = Some((reached, root.clone()));
        }
    }
    let (_, root) =
        best.ok_or_else(|| anyhow::anyhow!("no tree root in {}", directory.display()))?;
    Ok((directory.to_path_buf(), root, bytes))
}

/// A value's bytes, for telling distinct values apart and weighing them.
fn value_bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::Bytes(bytes) | Value::Record(bytes) => bytes.clone(),
        Value::String(string) => string.as_bytes().to_vec(),
        Value::Entity(entity) => entity.as_str().as_bytes().to_vec(),
        Value::Symbol(relation) => relation.as_str().as_bytes().to_vec(),
        Value::Boolean(flag) => vec![*flag as u8],
        Value::UnsignedInt(number) => number.to_le_bytes().to_vec(),
        Value::SignedInt(number) => number.to_le_bytes().to_vec(),
        Value::Float(number) => number.to_le_bytes().to_vec(),
    }
}

fn main() -> anyhow::Result<()> {
    let directory = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or_else(|| anyhow::anyhow!("usage: memory_census <replica directory>"))?,
    );
    let (blocks, root, block_bytes) = load(&directory.join("archive"))?;
    // The replica read from disk on every load, as a replica on disk is,
    // through a node cache of its own: the cache's heap is what it holds.
    let replica = Replica::on_disk(blocks, Some(directory.join("blob")));
    let reader = Holding::new(&replica);
    let catalog = test_catalog();
    println!(
        "replica {}: {} MiB of blocks, head {root}",
        directory.display(),
        mib(block_bytes)
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        // Every fact, through the attribute index: the widest scan a query
        // makes.
        let every = || ArtifactSelector::new().the_starting_with("");

        // Layer 1: the node cache a warm tree holds.
        let before = held();
        let started = Instant::now();
        let mut rows = 0usize;
        {
            let stream = ArtifactTree::at(catalog.clone(), root.clone()).scan(
                reader.clone(),
                spill_cache(),
                every(),
            );
            futures_util::pin_mut!(stream);
            while let Some(view) = stream.next().await {
                view?;
                rows += 1;
            }
        }
        let cache_heap = held() - before;
        let cached_nodes = reader.cache::<Key, State<Datum>>().len();
        let cached_bytes = reader.cache::<Key, State<Datum>>().bytes();
        println!(
            "\nnode cache after scanning every fact once ({rows} facts, {:.2}s):",
            started.elapsed().as_secs_f64()
        );
        println!(
            "  {cached_nodes} nodes; the cache counts {} MiB; the heap grew {} MiB",
            mib(cached_bytes),
            mib(cache_heap)
        );

        // Layer 2: a materialized result of every fact.
        let before = held();
        PEAK.store(before, Ordering::Relaxed);
        let artifacts: Vec<Artifact> = ArtifactTree::at(catalog.clone(), root.clone())
            .scan_owned(reader.clone(), spill_cache(), every())
            .try_collect()
            .await?;
        let result_heap = held() - before;
        let peak = PEAK.load(Ordering::Relaxed) - before;

        // What the result is made of: per field, the bytes every row holds
        // and the bytes of the distinct values among them.
        let (mut entity_all, mut attribute_all, mut value_all) = (0usize, 0usize, 0usize);
        let mut entities: HashSet<&str> = HashSet::new();
        let mut attributes: HashSet<&str> = HashSet::new();
        let mut values: HashSet<Vec<u8>> = HashSet::new();
        let mut causes = 0usize;
        for artifact in &artifacts {
            entity_all += artifact.of.as_str().len();
            entities.insert(artifact.of.as_str());
            attribute_all += artifact.the.as_str().len();
            attributes.insert(artifact.the.as_str());
            let bytes = value_bytes(&artifact.is);
            value_all += bytes.len();
            values.insert(bytes);
            causes += artifact.cause.is_some() as usize;
        }
        let distinct = |set: &dyn Fn() -> usize| set();
        let entity_distinct = distinct(&|| entities.iter().map(|s| s.len()).sum());
        let attribute_distinct = distinct(&|| attributes.iter().map(|s| s.len()).sum());
        let value_distinct: usize = values.iter().map(Vec::len).sum();
        println!(
            "\nresult of every fact ({} rows): the heap holds {} MiB ({} bytes a row), peak {} MiB while scanning",
            artifacts.len(),
            mib(result_heap),
            result_heap / artifacts.len().max(1),
            mib(peak),
        );
        println!("  field        bytes in rows     distinct   rows per distinct value");
        for (field, all, distinct, count) in [
            ("entity", entity_all, entity_distinct, entities.len()),
            ("attribute", attribute_all, attribute_distinct, attributes.len()),
            ("value", value_all, value_distinct, values.len()),
        ] {
            println!(
                "  {field:<10} {:>9} MiB {:>9} MiB   {:>10.1}  ({count} distinct)",
                mib(all),
                mib(distinct),
                artifacts.len() as f64 / count.max(1) as f64
            );
        }

        // Layer 3: the same result as shared tokens and rows of ids: each
        // distinct entity, attribute and value once, and a row of three
        // 4-byte ids plus its cause (a 32-byte hash when it has one).
        let tokens = entity_distinct + attribute_distinct + value_distinct;
        let ids = artifacts.len() * 3 * 4 + causes * 32;
        println!(
            "\ntoken model of that result: {} MiB of distinct tokens + {} MiB of rows = {} MiB, against {} MiB held today ({:.1}x)",
            mib(tokens),
            mib(ids),
            mib(tokens + ids),
            mib(result_heap),
            result_heap as f64 / (tokens + ids).max(1) as f64
        );

        drop((entities, attributes, values));
        let before = held() - result_heap;

        // Where a row's heap goes: take the result apart field by field and
        // see what dropping each frees.
        let row = std::mem::size_of::<Artifact>();
        let (mut relations, mut entities, mut values, mut causes) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let count = artifacts.len();
        for artifact in artifacts {
            relations.push(artifact.the);
            entities.push(artifact.of);
            values.push(artifact.is);
            causes.push(artifact.cause);
        }
        let freed = |before: usize| before.saturating_sub(held());
        let mut parts = Vec::new();
        for (name, drop_it) in [
            ("attribute", Box::new(move || drop(relations)) as Box<dyn FnOnce()>),
            ("entity", Box::new(move || drop(entities))),
            ("value", Box::new(move || drop(values))),
            ("cause", Box::new(move || drop(causes))),
        ] {
            let before = held();
            drop_it();
            parts.push((name, freed(before)));
        }
        println!(
            "\nwhere a row's {} bytes go: the row itself is {row} bytes inline in the result;",
            result_heap / count.max(1)
        );
        for (name, bytes) in parts {
            println!(
                "  {name:<10} {} MiB on the heap, {} bytes a row (a Vec of the field alone included)",
                mib(bytes),
                bytes / count.max(1)
            );
        }
        let residual = held().saturating_sub(before);
        println!(
            "  still held once the result is gone: {} MiB ({} bytes a row); the cache counts {} MiB in {} nodes",
            mib(residual),
            residual / count.max(1),
            mib(reader.cache::<Key, State<Datum>>().bytes()),
            reader.cache::<Key, State<Datum>>().len()
        );
        anyhow::Ok(())
    })?;
    Ok(())
}
