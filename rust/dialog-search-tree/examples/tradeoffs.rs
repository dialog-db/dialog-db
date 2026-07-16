//! Storage-footprint and operation-cost report for the search tree under a
//! dialog-shaped workload: 162-byte EAV-style keys (tag, 64-byte entity,
//! 64-byte attribute, value type, 32-byte value reference) and CBOR-sized
//! payloads, written in transaction-sized batches.
//!
//! The example uses only the tree's public API, so the same file runs
//! unmodified against the separator-link and the upper-bound-link
//! implementations; run it on each branch and compare the reports:
//!
//! ```sh
//! cargo run --release --package dialog-search-tree --example tradeoffs
//! ```
#![cfg(not(target_arch = "wasm32"))]

use std::time::Instant;

use dialog_common::Blake3Hash;
use dialog_search_tree::{
    ArchivedNodeBody, Buffer, ContentAddressedStorage, Delta, PersistentNode, PersistentTree,
};
use dialog_storage::{JournaledStorage, MemoryStorageBackend};

const KEY_LENGTH: usize = 162;
const ENTITIES: usize = 5_000;
const ATTRIBUTES_PER_ENTITY: usize = 20;
const FACTS: usize = ENTITIES * ATTRIBUTES_PER_ENTITY;
const BATCH: usize = 100;
const POINT_LOOKUPS: usize = 2_000;
const BOUNDED_SCANS: usize = 200;
const DELETES: usize = 1_000;

type Key = [u8; KEY_LENGTH];
type Backend = JournaledStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;
type Storage = ContentAddressedStorage<Backend>;
type Tree = PersistentTree<Key, Vec<u8>>;

/// Deterministic xorshift so both branches see byte-identical workloads.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// An EAV-shaped key: tag, entity bytes (a shared URI scheme prefix plus
/// hash-like material, zero padded, like `uri.rs` produces), attribute bytes
/// (a short name zero padded, like `attribute.rs` produces), value type and
/// value reference.
fn eav_key(entity: &[u8; 64], attribute: &[u8; 64], value_reference: &[u8; 32]) -> Key {
    let mut key = [0u8; KEY_LENGTH];
    key[0] = 0;
    key[1..65].copy_from_slice(entity);
    key[65..129].copy_from_slice(attribute);
    key[129] = 1;
    key[130..].copy_from_slice(value_reference);
    key
}

fn entities(rng: &mut Rng) -> Vec<[u8; 64]> {
    (0..ENTITIES)
        .map(|_| {
            let mut entity = [0u8; 64];
            entity[..9].copy_from_slice(b"urn:dlg:x");
            let mut tail = [0u8; 23];
            rng.fill(&mut tail);
            // Hash-derived URI text: alphanumeric-ish bytes.
            for (at, byte) in tail.iter().enumerate() {
                entity[9 + at] = b'a' + (byte % 26);
            }
            entity
        })
        .collect()
}

fn attributes() -> Vec<[u8; 64]> {
    let names: [&str; 20] = [
        "person/name",
        "person/email",
        "person/age",
        "person/bio",
        "post/title",
        "post/body",
        "post/created-at",
        "post/author",
        "comment/body",
        "comment/author",
        "comment/parent",
        "tag/label",
        "tag/color",
        "profile/avatar",
        "profile/homepage",
        "task/status",
        "task/assignee",
        "task/due-date",
        "counter/value",
        "counter/origin",
    ];
    names
        .iter()
        .map(|name| {
            let mut attribute = [0u8; 64];
            attribute[..name.len()].copy_from_slice(name.as_bytes());
            attribute
        })
        .collect()
}

struct Workload {
    facts: Vec<(Key, Vec<u8>)>,
}

fn workload() -> Workload {
    let mut rng = Rng::new(42);
    let entities = entities(&mut rng);
    let attributes = attributes();

    let mut facts = Vec::with_capacity(FACTS);
    for entity in &entities {
        for attribute in attributes.iter().take(ATTRIBUTES_PER_ENTITY) {
            // A CBOR State<Datum>-sized payload: 120..=248 bytes.
            let mut value = vec![0u8; 120 + rng.below(129)];
            rng.fill(&mut value);
            let mut value_reference = [0u8; 32];
            value_reference.copy_from_slice(&Blake3Hash::hash(&value).as_bytes()[..32]);
            facts.push((eav_key(entity, attribute, &value_reference), value));
        }
    }
    Workload { facts }
}

async fn flush(delta: &mut Delta<Blake3Hash, Buffer>, storage: &mut Storage) -> (usize, u64) {
    let mut nodes = 0usize;
    let mut bytes = 0u64;
    for (_, buffer) in delta.flush() {
        nodes += 1;
        bytes += buffer.as_ref().len() as u64;
        storage
            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
            .await
            .unwrap();
    }
    (nodes, bytes)
}

/// Walks the live tree from its root, returning per-level (node count, byte
/// total) with level 0 the root, plus segment/index rollups.
async fn live_footprint(tree: &Tree, storage: &Storage) -> Footprint {
    let mut footprint = Footprint::default();
    let mut frontier = vec![(tree.root().clone(), 0usize)];
    while let Some((hash, level)) = frontier.pop() {
        let bytes = storage
            .retrieve(&hash)
            .await
            .unwrap()
            .expect("live node present");
        let size = bytes.len() as u64;
        let node: PersistentNode<Key, Vec<u8>> = PersistentNode::new(Buffer::from(bytes));
        match node.body().unwrap() {
            ArchivedNodeBody::Index(index) => {
                footprint.index_nodes += 1;
                footprint.index_bytes += size;
                footprint.record(level, size);
                for link in index.links.iter() {
                    frontier.push((<&Blake3Hash>::from(&link.node).clone(), level + 1));
                }
            }
            ArchivedNodeBody::Segment(segment) => {
                footprint.segment_nodes += 1;
                footprint.segment_bytes += size;
                footprint.entries += segment.entries.len() as u64;
                footprint.record(level, size);
            }
        }
    }
    footprint
}

#[derive(Default)]
struct Footprint {
    index_nodes: u64,
    index_bytes: u64,
    segment_nodes: u64,
    segment_bytes: u64,
    entries: u64,
    levels: Vec<(u64, u64)>,
}

impl Footprint {
    fn record(&mut self, level: usize, size: u64) {
        if self.levels.len() <= level {
            self.levels.resize(level + 1, (0, 0));
        }
        self.levels[level].0 += 1;
        self.levels[level].1 += size;
    }

    fn total_bytes(&self) -> u64 {
        self.index_bytes + self.segment_bytes
    }
}

fn reads(storage: &Storage) -> usize {
    storage.backend().get_reads().len()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let Workload { facts } = workload();
    let mut storage = ContentAddressedStorage::new(JournaledStorage::new(
        MemoryStorageBackend::default(),
    ));
    storage.backend().disable_journal();

    println!("workload: {FACTS} facts (EAV keys, {KEY_LENGTH}B), batches of {BATCH}");
    println!();

    // Build in transaction-sized batches, tracking write amplification.
    let mut tree = Tree::empty();
    let mut delta = Delta::zero();
    let mut written_nodes = 0usize;
    let mut written_bytes = 0u64;
    let build = Instant::now();
    for batch in facts.chunks(BATCH) {
        let mut edit = tree.edit();
        for (key, value) in batch {
            edit = edit.insert(*key, value.clone(), &storage).await.unwrap();
        }
        tree = edit.persist(&mut delta).unwrap();
        let (nodes, bytes) = flush(&mut delta, &mut storage).await;
        written_nodes += nodes;
        written_bytes += bytes;
    }
    let build_time = build.elapsed();
    println!(
        "build:      {:>8.2?} total, {:>7.0} facts/s",
        build_time,
        FACTS as f64 / build_time.as_secs_f64()
    );
    println!(
        "written:    {written_nodes:>8} nodes, {:>8.1} MiB across all batches (write amplification)",
        written_bytes as f64 / (1024.0 * 1024.0)
    );

    // Live footprint of the final tree.
    let footprint = live_footprint(&tree, &storage).await;
    assert_eq!(footprint.entries as usize, FACTS, "all facts present");
    println!();
    println!(
        "live tree:  {:>8.1} MiB total, {:>6.1} bytes/fact",
        footprint.total_bytes() as f64 / (1024.0 * 1024.0),
        footprint.total_bytes() as f64 / FACTS as f64
    );
    println!(
        "  index:    {:>8} nodes, {:>8.1} KiB ({:>5.1} B/link avg)",
        footprint.index_nodes,
        footprint.index_bytes as f64 / 1024.0,
        footprint.index_bytes as f64
            / (footprint.segment_nodes + footprint.index_nodes.saturating_sub(1)) as f64,
    );
    println!(
        "  segments: {:>8} nodes, {:>8.1} MiB ({:>5.1} entries/segment avg)",
        footprint.segment_nodes,
        footprint.segment_bytes as f64 / (1024.0 * 1024.0),
        footprint.entries as f64 / footprint.segment_nodes as f64,
    );
    for (level, (nodes, bytes)) in footprint.levels.iter().enumerate() {
        println!(
            "  level {level}:  {nodes:>8} nodes, {:>8.1} KiB, {:>7.0} B/node avg",
            *bytes as f64 / 1024.0,
            *bytes as f64 / *nodes as f64
        );
    }

    // Point lookups on a cold tree (fresh cache), counting storage reads.
    let mut rng = Rng::new(7);
    let cold = Tree::from_hash(tree.root().clone());
    storage.backend().enable_journal();
    let reads_before = reads(&storage);
    let point = Instant::now();
    for _ in 0..POINT_LOOKUPS {
        let (key, expected) = &facts[rng.below(FACTS)];
        let value = cold.get(key, &storage).await.unwrap();
        assert_eq!(value.as_ref(), Some(expected));
    }
    let point_time = point.elapsed();
    println!();
    println!(
        "point gets: {POINT_LOOKUPS} lookups in {:>8.2?} ({:>5.1} us/get, {:.2} reads/get cold)",
        point_time,
        point_time.as_micros() as f64 / POINT_LOOKUPS as f64,
        (reads(&storage) - reads_before) as f64 / POINT_LOOKUPS as f64
    );

    // Bounded range scans (~one entity's facts each) on a cold tree.
    let cold = Tree::from_hash(tree.root().clone());
    let reads_before = reads(&storage);
    let scan = Instant::now();
    let mut scanned = 0usize;
    for _ in 0..BOUNDED_SCANS {
        let at = rng.below(FACTS - ATTRIBUTES_PER_ENTITY);
        let mut start = facts[at].0;
        start[65..].fill(0);
        let mut end = start;
        end[65..129].fill(0xFF);
        use futures_util::StreamExt;
        let stream = cold.stream_range(start..=end, &storage);
        futures_util::pin_mut!(stream);
        while let Some(entry) = stream.next().await {
            entry.unwrap();
            scanned += 1;
        }
    }
    let scan_time = scan.elapsed();
    println!(
        "scans:      {BOUNDED_SCANS} entity scans, {scanned} entries in {:>8.2?} ({:.2} reads/scan cold)",
        scan_time,
        (reads(&storage) - reads_before) as f64 / BOUNDED_SCANS as f64
    );

    // Differential after a single-fact change, on cold trees.
    let (changed_key, _) = &facts[FACTS / 2];
    let mut delta = Delta::zero();
    let modified = tree
        .edit()
        .insert(*changed_key, b"modified value".to_vec(), &storage)
        .await
        .unwrap()
        .persist(&mut delta)
        .unwrap();
    flush(&mut delta, &mut storage).await;

    let old = Tree::from_hash(tree.root().clone());
    let new = Tree::from_hash(modified.root().clone());
    let reads_before = reads(&storage);
    let diff = Instant::now();
    let mut changes = 0usize;
    {
        use futures_util::StreamExt;
        let stream = old.differentiate(&new, &storage, &storage);
        futures_util::pin_mut!(stream);
        while let Some(change) = stream.next().await {
            change.unwrap();
            changes += 1;
        }
    }
    println!(
        "diff:       single-fact change: {changes} changes, {} reads, {:>8.2?}",
        reads(&storage) - reads_before,
        diff.elapsed()
    );

    // Deletes in one batch.
    storage.backend().disable_journal();
    let mut delta = Delta::zero();
    let deletes = Instant::now();
    let mut edit = tree.edit();
    for _ in 0..DELETES {
        let (key, _) = &facts[rng.below(FACTS)];
        edit = edit.delete(key, &storage).await.unwrap();
    }
    let _ = edit.persist(&mut delta).unwrap();
    println!(
        "deletes:    {DELETES} random deletes in one batch: {:>8.2?}",
        deletes.elapsed()
    );
}
