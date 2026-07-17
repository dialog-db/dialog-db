//! Isolated columnar-leaf compression report: the same dialog-shaped EAV
//! workload as `tradeoffs.rs`, but with a key type that exposes its component
//! schema, so the leaf stores each component columnar (entity/value in
//! arenas, attribute/tag/value-type in per-leaf dictionaries).
//!
//! Run alongside `tradeoffs.rs` (which uses the opaque `[u8; 162]` key, so
//! its leaf is a single flat front-coded column) to see the effect of
//! recognizing the components: the attribute (20 distinct names recurring
//! non-adjacently across entities in EAV order) is stored once per leaf
//! instead of once per fact.
//!
//! ```sh
//! cargo run --release --package dialog-search-tree --example columnar_tradeoffs
//! ```
#![cfg(not(target_arch = "wasm32"))]

use std::hash::Hash;

use dialog_common::Blake3Hash;
use dialog_search_tree::{
    ArchivedNodeBody, Buffer, Component, ContentAddressedStorage, Delta, DialogSearchTreeError,
    Key as TreeKey, PersistentNode, PersistentTree, Schema,
};
use dialog_storage::{JournaledStorage, MemoryStorageBackend};

const KEY_LENGTH: usize = 162;
const ENTITIES: usize = 5_000;
const ATTRIBUTES_PER_ENTITY: usize = 20;
const FACTS: usize = ENTITIES * ATTRIBUTES_PER_ENTITY;
const BATCH: usize = 100;

const TAG: usize = 1;
const ENTITY: usize = 64;
const ATTRIBUTE: usize = 64;
const VALUE_TYPE: usize = 1;
const VALUE_REF: usize = 32;

/// The EAV component schema: tag and value-type are tiny repeated enums
/// (dictionary), the attribute is a small set of names recurring
/// non-adjacently (dictionary), the entity and value reference are large and
/// mostly distinct (arena).
const EAV_SCHEMA: &[Component] = &[
    Component::dictionary(TAG),
    Component::arena(ENTITY),
    Component::dictionary(ATTRIBUTE),
    Component::dictionary(VALUE_TYPE),
    Component::arena(VALUE_REF),
];

/// A 162-byte EAV key that exposes its component structure.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct EavKey([u8; KEY_LENGTH]);

impl AsRef<[u8]> for EavKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl TreeKey for EavKey {
    fn try_from_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError> {
        bytes
            .try_into()
            .map(EavKey)
            .map_err(|_| DialogSearchTreeError::Encoding("bad EAV key length".into()))
    }

    fn min() -> Self {
        EavKey([u8::MIN; KEY_LENGTH])
    }

    fn max() -> Self {
        EavKey([u8::MAX; KEY_LENGTH])
    }

    fn schema(_layout: u8) -> Schema {
        Schema::new(EAV_SCHEMA)
    }

    fn components<'a>(&'a self, out: &mut Vec<&'a [u8]>) {
        let mut at = 0;
        for width in [TAG, ENTITY, ATTRIBUTE, VALUE_TYPE, VALUE_REF] {
            out.push(&self.0[at..at + width]);
            at += width;
        }
    }
}

type Backend = JournaledStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;
type Storage = ContentAddressedStorage<Backend>;
type Tree = PersistentTree<EavKey, Vec<u8>>;

/// Deterministic xorshift, identical to `tradeoffs.rs` so the workloads match.
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
}

fn eav_key(entity: &[u8; 64], attribute: &[u8; 64], value_reference: &[u8; 32]) -> EavKey {
    let mut key = [0u8; KEY_LENGTH];
    key[0] = 0;
    key[TAG..TAG + ENTITY].copy_from_slice(entity);
    key[TAG + ENTITY..TAG + ENTITY + ATTRIBUTE].copy_from_slice(attribute);
    key[TAG + ENTITY + ATTRIBUTE] = 1;
    key[TAG + ENTITY + ATTRIBUTE + VALUE_TYPE..].copy_from_slice(value_reference);
    EavKey(key)
}

fn entities(rng: &mut Rng) -> Vec<[u8; 64]> {
    (0..ENTITIES)
        .map(|_| {
            let mut entity = [0u8; 64];
            entity[..9].copy_from_slice(b"urn:dlg:x");
            let mut tail = [0u8; 23];
            rng.fill(&mut tail);
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

fn workload() -> Vec<(EavKey, Vec<u8>)> {
    let mut rng = Rng::new(42);
    let entities = entities(&mut rng);
    let attributes = attributes();

    let mut facts = Vec::with_capacity(FACTS);
    for entity in &entities {
        for attribute in attributes.iter().take(ATTRIBUTES_PER_ENTITY) {
            let mut value_reference = [0u8; 32];
            rng.fill(&mut value_reference);
            // Value payload sized like a small CBOR State<Datum>.
            let value = value_reference[..24].to_vec();
            facts.push((eav_key(entity, attribute, &value_reference), value));
        }
    }
    facts.sort_by(|a, b| a.0.cmp(&b.0));
    facts
}

#[derive(Default)]
struct Footprint {
    index_nodes: u64,
    index_bytes: u64,
    segment_nodes: u64,
    segment_bytes: u64,
    entries: u64,
}

async fn live_footprint(tree: &Tree, storage: &Storage) -> Footprint {
    let mut footprint = Footprint::default();
    let mut frontier = vec![tree.root().clone()];
    while let Some(hash) = frontier.pop() {
        let Some(bytes) = storage.retrieve(&hash).await.unwrap() else {
            continue;
        };
        let size = bytes.len() as u64;
        let node: PersistentNode<EavKey, Vec<u8>> = PersistentNode::new(Buffer::from(bytes));
        match node.body().unwrap() {
            ArchivedNodeBody::Index(index) => {
                footprint.index_nodes += 1;
                footprint.index_bytes += size;
                for at in 0..index.len() {
                    frontier.push(index.hash_at(at).unwrap().clone());
                }
            }
            ArchivedNodeBody::Segment(segment) => {
                footprint.segment_nodes += 1;
                footprint.segment_bytes += size;
                footprint.entries += segment.len() as u64;
            }
        }
    }
    footprint
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let facts = workload();
    let mut storage = Storage::new(JournaledStorage::new(MemoryStorageBackend::default()));
    let mut tree = Tree::empty();
    let mut delta = Delta::zero();

    for chunk in facts.chunks(BATCH) {
        let mut edit = tree.edit();
        for (key, value) in chunk {
            edit = edit.insert(key.clone(), value.clone(), &storage).await?;
        }
        tree = edit.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
    }

    let footprint = live_footprint(&tree, &storage).await;
    let live_bytes = footprint.index_bytes + footprint.segment_bytes;

    println!("columnar EAV leaf report");
    println!("workload: {FACTS} facts (EAV keys, 162B), batches of {BATCH}\n");
    println!(
        "live tree:     {:.1} MiB total,  {:.1} bytes/fact",
        live_bytes as f64 / (1024.0 * 1024.0),
        live_bytes as f64 / FACTS as f64,
    );
    println!(
        "  index:       {} nodes, {:.1} KiB",
        footprint.index_nodes,
        footprint.index_bytes as f64 / 1024.0,
    );
    println!(
        "  segments:    {} nodes, {:.1} MiB ({:.1} entries/segment avg)",
        footprint.segment_nodes,
        footprint.segment_bytes as f64 / (1024.0 * 1024.0),
        footprint.entries as f64 / footprint.segment_nodes.max(1) as f64,
    );

    // Sanity: read back a fact so the columnar path is exercised, not just
    // written.
    let (probe_key, probe_value) = &facts[FACTS / 2];
    let got = tree.get(probe_key, &storage).await?;
    assert_eq!(got.as_ref(), Some(probe_value), "columnar read-back failed");
    println!("\nread-back check: ok");

    Ok(())
}
