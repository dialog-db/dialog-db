//! Research harness (E3 in design/collaborative-text/hypotheses.md): cold
//! block reads to fetch one document's range of a text region, among 1, 100
//! and 10 000 documents.
//!
//! The region is hypothetical: entries under a spare tag, keyed
//! `tag ‖ document ‖ origin ‖ sequence`, each holding one small change
//! (20 to 50 bytes) as an opaque record, the way the context region holds
//! its watermarks. Unrelated data entries sit under the entity tag beside
//! it. The tree is the production artifact tree (key type, value type and
//! default manifest), so leaf sizes are the real ones.
//!
//! A read from cold goes through a store that holds nothing, so every
//! node the range walk touches is one counted load, the way a partial
//! replica hydrates nodes from a remote on read.
//!
//! ```sh
//! cargo run --release --package dialog-artifacts \
//!   --features helpers --example text_region_reads
//! ```
#![cfg(feature = "helpers")]

#[cfg(not(target_arch = "wasm32"))]
mod harness {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use dialog_artifacts::helpers::generate_data;
    use dialog_artifacts::tree::ArtifactTree;
    use dialog_artifacts::{ArchiveDelta, Datum, Instruction, Key, State, apply_buffered};
    use dialog_capability::Provider;
    use dialog_common::{Buffer, Held, Holds};
    use dialog_effects::archive::{self, ArchiveError};
    use dialog_search_tree::helpers::Blocks;
    use dialog_search_tree::{Change, DialogSearchTreeError, Entry, NodeBody, PersistentNode};
    use futures_util::StreamExt as _;

    /// A spare tag for the hypothetical text region, above the context
    /// region's.
    const TEXT_TAG: u8 = 7;
    /// Changes per document.
    const CHANGES: usize = 500;
    /// Origins that wrote into each document.
    const ORIGINS: usize = 3;
    /// Unrelated data entries per document, real facts in the three data
    /// orderings: `generate_data` writes about five facts per entity, each
    /// in three orderings.
    const DATA_PER_DOCUMENT: usize = 45;
    /// Documents per build batch, so the open edit stays small.
    const BATCH_DOCUMENTS: usize = 500;

    /// Reads that reach the blocks, counted with their bytes and, for
    /// leaves, the entries they hold that fall outside `prefix`.
    #[derive(Clone)]
    struct Counting {
        blocks: Blocks,
        prefix: Arc<Vec<u8>>,
        reads: Arc<AtomicUsize>,
        bytes: Arc<AtomicUsize>,
        leaves: Arc<AtomicUsize>,
        leaf_entries: Arc<AtomicUsize>,
        foreign: Arc<AtomicUsize>,
    }

    impl Counting {
        fn new(blocks: Blocks, prefix: Vec<u8>) -> Self {
            Self {
                blocks,
                prefix: Arc::new(prefix),
                reads: Arc::default(),
                bytes: Arc::default(),
                leaves: Arc::default(),
                leaf_entries: Arc::default(),
                foreign: Arc::default(),
            }
        }
    }

    impl Holds for Counting {
        fn held(&self, _key: &str) -> Option<Held> {
            None
        }

        fn hold(&self, _key: String, _handle: Held) {}
    }

    #[async_trait]
    impl Provider<archive::Load> for Counting {
        async fn execute(&self, load: archive::Load) -> Result<Option<Buffer>, ArchiveError> {
            let block = self.blocks.get(&load.digest);
            if let Some(bytes) = &block {
                self.reads.fetch_add(1, Ordering::SeqCst);
                self.bytes.fetch_add(bytes.as_ref().len(), Ordering::SeqCst);
                if let Ok(node) = PersistentNode::<Key, State<Datum>>::try_from(bytes.clone())
                    && let NodeBody::Segment(segment) = node.body()
                {
                    self.leaves.fetch_add(1, Ordering::SeqCst);
                    let mut keys = segment.keys::<Key>().expect("leaf keys decode");
                    while let Some((_, key)) = keys.next_key().expect("leaf key decodes") {
                        self.leaf_entries.fetch_add(1, Ordering::SeqCst);
                        if !key.starts_with(&self.prefix) {
                            self.foreign.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
            Ok(block)
        }
    }

    fn hash(parts: &[&[u8]]) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for part in parts {
            hasher.update(part);
        }
        *hasher.finalize().as_bytes()
    }

    fn document_id(document: usize) -> [u8; 16] {
        let full = hash(&[b"document", &document.to_be_bytes()]);
        full[..16].try_into().expect("16 bytes")
    }

    fn record(bytes: Vec<u8>) -> State<Datum> {
        State::Added(Datum {
            cause: None,
            blob: Some(bytes),
            version: None,
            collapsed: Vec::new(),
            supersedes: Vec::new(),
            retraction: false,
        })
    }

    /// One document's text changes.
    fn document_entries(document: usize) -> Vec<(Key, State<Datum>)> {
        let id = document_id(document);
        let mut entries = Vec::with_capacity(CHANGES + DATA_PER_DOCUMENT);
        for change in 0..CHANGES {
            let origin = hash(&[b"origin", &id, &(change % ORIGINS).to_be_bytes()]);
            let sequence = (change / ORIGINS) as u64;
            let mut key = Vec::with_capacity(1 + 16 + 32 + 8);
            key.push(TEXT_TAG);
            key.extend_from_slice(&id);
            key.extend_from_slice(&origin);
            key.extend_from_slice(&sequence.to_be_bytes());
            let noise = hash(&[b"change", &key]);
            let length = 20 + (noise[0] as usize % 31);
            let mut bytes = noise.to_vec();
            bytes.extend_from_slice(&noise);
            bytes.truncate(length);
            entries.push((Key::from(key), record(bytes)));
        }
        entries
    }

    async fn build(documents: usize) -> Result<(ArtifactTree, Blocks), DialogSearchTreeError> {
        let blocks = Blocks::new();
        let mut tree = ArtifactTree::empty_in(dialog_search_tree::helpers::test_catalog());
        // Documents in key order, so each batch appends at the right edge
        // instead of rewriting leaves an earlier batch wrote.
        let mut order: Vec<usize> = (0..documents).collect();
        order.sort_by_key(|document| document_id(*document));
        let mut start = 0;
        while start < documents {
            let end = (start + BATCH_DOCUMENTS).min(documents);
            let mut entries: Vec<(Key, State<Datum>)> = order[start..end]
                .iter()
                .flat_map(|document| document_entries(*document))
                .collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let changes = futures_util::stream::iter(
                entries
                    .into_iter()
                    .map(|(key, value)| Ok(Change::Add(Entry::new(key, value)))),
            );
            let edit = tree.edit().integrate(changes, &blocks).await?;
            let mut delta = ArchiveDelta::zero();
            tree = edit.persist(delta.blocks(), &blocks)?;
            delta.flush_into(&blocks);
            start = end;
        }
        let data = generate_data((documents * DATA_PER_DOCUMENT / 15).max(1))
            .map_err(|error| DialogSearchTreeError::Node(error.to_string()))?;
        let mut delta = ArchiveDelta::zero();
        apply_buffered(
            &mut tree,
            &blocks,
            &mut delta,
            None,
            futures_util::stream::iter(
                data.into_iter()
                    .map(|artifact| Instruction::Assert(artifact, dialog_artifacts::Pick::All)),
            ),
            true,
        )
        .await
        .map_err(|error| DialogSearchTreeError::Node(error.to_string()))?;
        delta.flush_into(&blocks);
        Ok((tree, blocks))
    }

    /// Height, leaf count and total node bytes of the live tree.
    fn shape(tree: &ArtifactTree, blocks: &Blocks) -> (usize, usize, usize, usize) {
        let mut frontier = vec![tree.root().clone()];
        let (mut height, mut leaves, mut nodes, mut bytes) = (0, 0, 0, 0);
        while !frontier.is_empty() {
            height += 1;
            let mut next = Vec::new();
            for hash in frontier {
                let Some(block) = blocks.get(&hash) else {
                    continue;
                };
                nodes += 1;
                bytes += block.as_ref().len();
                let node = PersistentNode::<Key, State<Datum>>::try_from(block).expect("node");
                match node.body() {
                    NodeBody::Index(index) => {
                        for at in 0..index.len() {
                            next.push(index.hash_at(at).expect("child").clone());
                        }
                    }
                    NodeBody::Segment(_) => leaves += 1,
                }
            }
            frontier = next;
        }
        (height, leaves, nodes, bytes)
    }

    /// Every entry the live tree holds.
    fn shape_entries(tree: &ArtifactTree, blocks: &Blocks) -> usize {
        let mut frontier = vec![tree.root().clone()];
        let mut entries = 0;
        while let Some(hash) = frontier.pop() {
            let Some(block) = blocks.get(&hash) else {
                continue;
            };
            let node = PersistentNode::<Key, State<Datum>>::try_from(block).expect("node");
            match node.body() {
                NodeBody::Index(index) => {
                    for at in 0..index.len() {
                        frontier.push(index.hash_at(at).expect("child").clone());
                    }
                }
                NodeBody::Segment(segment) => entries += segment.len(),
            }
        }
        entries
    }

    struct Reading {
        entries: usize,
        reads: usize,
        bytes: usize,
        leaves: usize,
        leaf_entries: usize,
        foreign: usize,
    }

    async fn read_document(
        tree: &ArtifactTree,
        blocks: &Blocks,
        document: usize,
    ) -> Result<Reading, DialogSearchTreeError> {
        let mut prefix = vec![TEXT_TAG];
        prefix.extend_from_slice(&document_id(document));
        let mut high = prefix.clone();
        high.extend(std::iter::repeat_n(u8::MAX, 64));
        let counting = Counting::new(blocks.clone(), prefix.clone());
        let stream = tree.stream_range(Key::from(prefix)..=Key::from(high), &counting);
        futures_util::pin_mut!(stream);
        let mut entries = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            entries += 1;
        }
        Ok(Reading {
            entries,
            reads: counting.reads.load(Ordering::SeqCst),
            bytes: counting.bytes.load(Ordering::SeqCst),
            leaves: counting.leaves.load(Ordering::SeqCst),
            leaf_entries: counting.leaf_entries.load(Ordering::SeqCst),
            foreign: counting.foreign.load(Ordering::SeqCst),
        })
    }

    pub async fn run() -> Result<(), DialogSearchTreeError> {
        let sizes: Vec<usize> = match std::env::var("TEXT_REGION_DOCUMENTS") {
            Ok(list) => list
                .split(',')
                .map(|size| size.trim().parse().expect("a document count"))
                .collect(),
            Err(_) => vec![1, 100, 10_000],
        };
        println!(
            "{CHANGES} changes (20 to 50 bytes) per document from {ORIGINS} origins, \
             {DATA_PER_DOCUMENT} unrelated data entries per document"
        );
        println!(
            "docs    entries  height  leaves  tree MiB | sampled  blocks  bytes   leaves  \
             leaf entries  foreign"
        );
        for documents in sizes {
            let (tree, blocks) = build(documents).await?;
            let (height, leaves, _nodes, bytes) = shape(&tree, &blocks);
            let total = shape_entries(&tree, &blocks);
            let samples: Vec<usize> = if documents == 1 {
                vec![0]
            } else {
                vec![0, documents / 3, documents / 2, documents - 1]
            };
            for document in samples {
                let reading = read_document(&tree, &blocks, document).await?;
                assert_eq!(reading.entries, CHANGES, "the range holds the document");
                println!(
                    "{documents:<7} {total:<8} {height:<7} {leaves:<7} {:<8.2} | doc {document:<5} {:<7} {:<7} {:<7} {:<13} {}",
                    bytes as f64 / (1024.0 * 1024.0),
                    reading.reads,
                    reading.bytes,
                    reading.leaves,
                    reading.leaf_entries,
                    reading.foreign,
                );
            }
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    harness::run().await?;
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn main() {}
