//! Production storage-footprint report for the artifact index under the
//! columnar leaf codec: real EAV/AEV/VAE facts committed through the full
//! [`Artifacts`] pipeline (all three orderings), then the live tree
//! traversed to sum its on-disk bytes.
//!
//! This is the end-to-end counterpart of the search-tree
//! `columnar_tradeoffs` example: it drives the actual dialog key schema and
//! CBOR-encoded `State<Datum>` payload through the three-index write path,
//! so the bytes/fact it reports is the real production number.
//!
//! ```sh
//! cargo run --release --package dialog-artifacts \
//!   --features debug,helpers --example columnar_footprint
//! ```
#![cfg(all(feature = "debug", feature = "helpers", not(target_arch = "wasm32")))]

use dialog_artifacts::helpers::generate_data;
use dialog_artifacts::tree::TreeStorageBridge;
use dialog_artifacts::{
    ArtifactStoreMutExt, Artifacts, Instruction, Key, MemoryStorageBackend, State,
};
use dialog_search_tree::{
    ArchivedNodeBody, Buffer, ContentAddressedStorage as TreeStorage, PersistentNode,
};

const ENTITIES: usize = 20_000;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // generate_data emits ~5 facts per entity across 5 recurring attributes
    // and mixed value types: the realistic non-adjacent repetition the
    // columnar dictionary targets.
    let data = generate_data(ENTITIES)?;
    let fact_count = data.len();

    let backend = MemoryStorageBackend::default();
    let mut artifacts = Artifacts::anonymous(backend.clone()).await?;
    artifacts
        .commit(data.into_iter().map(Instruction::Assert))
        .await?;

    // Walk the live tree, summing node bytes by kind. Each fact lands in all
    // three EAV/AEV/VAE orderings, so the tree holds ~3x fact_count entries.
    let index = artifacts.index();
    let index = index.read().await;
    let root = index.root().clone();
    let tree_storage = TreeStorage::new(TreeStorageBridge(backend));

    let mut index_nodes = 0u64;
    let mut index_bytes = 0u64;
    let mut segment_nodes = 0u64;
    let mut segment_bytes = 0u64;
    let mut entries = 0u64;
    let mut key_bytes = 0u64;
    let mut flat_key_bytes = 0u64;

    let mut frontier = vec![root];
    while let Some(hash) = frontier.pop() {
        if &hash == dialog_common::NULL_BLAKE3_HASH {
            continue;
        }
        let Some(bytes) = tree_storage.retrieve(&hash).await? else {
            continue;
        };
        let size = bytes.len() as u64;
        let node: PersistentNode<Key, State<dialog_artifacts::Datum>> =
            PersistentNode::new(Buffer::from(bytes));
        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                index_nodes += 1;
                index_bytes += size;
                for at in 0..index.len() {
                    frontier.push(index.hash_at(at)?.clone());
                }
            }
            ArchivedNodeBody::Segment(segment) => {
                segment_nodes += 1;
                segment_bytes += size;
                entries += segment.len() as u64;
                // Approximate the key-column bytes so the payload share is
                // visible: total leaf minus the columns is the value table.
                let mut columns = 0u64;
                for column in segment.columns.iter() {
                    use dialog_search_tree::ArchivedColumnData;
                    columns += match column {
                        ArchivedColumnData::Arena { prefix, stream } => {
                            (prefix.len() + stream.len()) as u64
                        }
                        ArchivedColumnData::Dictionary {
                            table,
                            table_ends,
                            indices,
                        } => (table.len() + table_ends.len() + indices.len()) as u64,
                    };
                }
                key_bytes += columns;

                // Re-encode this leaf's keys as ONE flat whole-key column to
                // compare per-component columns against whole-key front coding
                // on the identical entry set.
                let mut keys = segment.keys::<Key>()?;
                let mut whole: Vec<Vec<u8>> = Vec::new();
                while let Some((_, key)) = keys.next_key()? {
                    whole.push(key.to_vec());
                }
                let refs: Vec<&[u8]> = whole.iter().map(|k| k.as_slice()).collect();
                let (prefix, stream) = dialog_search_tree::encode_keys_public(&refs);
                flat_key_bytes += (prefix.len() + stream.len()) as u64;
            }
        }
    }

    let live_bytes = index_bytes + segment_bytes;

    println!("artifact columnar footprint (production key schema)");
    println!(
        "workload: {ENTITIES} entities, {fact_count} facts, {entries} tree entries (3 orderings)\n"
    );
    println!(
        "live tree:     {:.1} MiB total",
        live_bytes as f64 / (1024.0 * 1024.0)
    );
    println!(
        "  per fact:    {:.1} bytes  (fact stored in 3 orderings)",
        live_bytes as f64 / fact_count as f64
    );
    println!(
        "  per entry:   {:.1} bytes  (one EAV/AEV/VAE row)",
        live_bytes as f64 / entries.max(1) as f64
    );
    println!(
        "  index:       {index_nodes} nodes, {:.1} KiB",
        index_bytes as f64 / 1024.0
    );
    println!(
        "  segments:    {segment_nodes} nodes, {:.1} MiB ({:.1} entries/segment avg)",
        segment_bytes as f64 / (1024.0 * 1024.0),
        entries as f64 / segment_nodes.max(1) as f64
    );
    println!(
        "  key columns:            {:.1} bytes/entry",
        key_bytes as f64 / entries.max(1) as f64
    );
    println!(
        "  value payload + rkyv:   {:.1} bytes/entry (leaf minus key columns)",
        (segment_bytes.saturating_sub(key_bytes)) as f64 / entries.max(1) as f64
    );
    println!(
        "  columnar key total:     {:.1} bytes/entry",
        key_bytes as f64 / entries.max(1) as f64
    );
    println!(
        "  flat whole-key total:   {:.1} bytes/entry (same entries, one arena)",
        flat_key_bytes as f64 / entries.max(1) as f64
    );

    Ok(())
}
