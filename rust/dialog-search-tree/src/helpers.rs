//! Tree specification macro and test utilities.
//!
//! This module provides tools to create search trees with deterministic
//! structure for testing the differential algorithm. The [`crate::tree_spec!`]
//! macro describes the exact shape a tree should take; ranks are read
//! straight out of the spec keys by [`DistributionSimulator`] instead of
//! being derived from key hashes, so the resulting tree matches the spec
//! exactly.
//!
//! It also provides utilities for iterating over all nodes in a tree, which
//! is useful for testing, debugging, and advanced introspection.
//!
//! # Tree Specification Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! use dialog_search_tree::helpers::{TestStorage, TreeSpec, test_storage};
//! use dialog_search_tree::tree_spec;
//!
//! let storage: TestStorage = test_storage();
//!
//! let spec = tree_spec![
//!     [                        ..l]  // Height 1 (index nodes)
//!     [..a, c..e, f..f, g..g, h..l]  // Height 0 (segment nodes/leaves)
//! ];
//!
//! let tree_spec: TreeSpec = spec.build(storage).await?;
//! let tree = tree_spec.tree();
//! # Ok(())
//! # }
//! ```

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
};

use async_stream::try_stream;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, JournaledStorage, MemoryStorageBackend, StorageBackend};
use futures_core::Stream;
use futures_util::StreamExt;
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    ArchivedNodeBody, Buffer, ContentAddressedStorage, Delta, DialogSearchTreeError, Distribution,
    Key, PersistentNode, PersistentTree, Rank, Value,
};

/// Traversal order for tree iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TraversalOrder {
    /// Visit children before siblings (uses stack internally).
    #[default]
    DepthFirst,
    /// Visit all nodes at each level before going deeper (uses queue internally).
    BreadthFirst,
}

impl TraversalOrder {
    /// Create a new traversal queue for this order.
    pub fn queue<T>(self) -> TraversalQueue<T> {
        TraversalQueue {
            order: self,
            items: VecDeque::new(),
        }
    }
}

/// A queue that manages traversal order automatically.
///
/// Created via [`TraversalOrder::queue()`].
#[derive(Debug, Clone)]
pub struct TraversalQueue<T> {
    order: TraversalOrder,
    items: VecDeque<T>,
}

impl<T> TraversalQueue<T> {
    /// Remove and return the next item according to traversal order.
    ///
    /// - `DepthFirst`: pops from back (stack/LIFO)
    /// - `BreadthFirst`: pops from front (queue/FIFO)
    pub fn dequeue(&mut self) -> Option<T> {
        match self.order {
            TraversalOrder::DepthFirst => self.items.pop_back(),
            TraversalOrder::BreadthFirst => self.items.pop_front(),
        }
    }

    /// Add items in the appropriate order for this traversal.
    ///
    /// - `DepthFirst`: adds in reverse order so first item is processed first
    /// - `BreadthFirst`: adds in forward order (left-to-right)
    pub fn enqueue<I>(&mut self, items: I)
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: DoubleEndedIterator,
    {
        match self.order {
            TraversalOrder::DepthFirst => {
                for item in items.into_iter().rev() {
                    self.items.push_back(item);
                }
            }
            TraversalOrder::BreadthFirst => {
                for item in items {
                    self.items.push_back(item);
                }
            }
        }
    }
}

/// Trait for traversing all nodes in a tree structure.
///
/// This trait provides the ability to iterate over every node in a tree,
/// which is useful for debugging, testing, and advanced introspection.
pub trait Traversable<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
{
    /// Returns an async stream that traverses all nodes in the specified order.
    ///
    /// This yields every node in the tree, loading each node from storage
    /// lazily as it's visited (including the root, which the tree only holds
    /// by hash).
    ///
    /// # Arguments
    /// * `order` - The traversal order:
    ///   - `DepthFirst`: Visit children before siblings (pre-order)
    ///   - `BreadthFirst`: Visit all nodes at each level before going deeper
    fn traverse<'a, Backend>(
        &'a self,
        order: TraversalOrder,
        storage: &'a ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<PersistentNode<Key, Value>, DialogSearchTreeError>> + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSend;
}

impl<Key, Value, D> Traversable<Key, Value> for PersistentTree<Key, Value, D>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value: for<'b> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'b>, Share>, rkyv::rancor::Error>,
    >,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    D: Distribution,
{
    fn traverse<'a, Backend>(
        &'a self,
        order: TraversalOrder,
        storage: &'a ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<PersistentNode<Key, Value>, DialogSearchTreeError>> + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSend,
    {
        let root = self.root().clone();

        try_stream! {
            if &root != NULL_BLAKE3_HASH {
                let mut queue = order.queue();
                queue.enqueue([root]);

                while let Some(hash) = queue.dequeue() {
                    let node = load_node::<Key, Value, Backend>(storage, &hash).await?;

                    if let ArchivedNodeBody::Index(index) = node.body()? {
                        let children = index
                            .links()?
                            .into_iter()
                            .map(|link| link.node)
                            .collect::<Vec<_>>();
                        queue.enqueue(children);
                    }

                    yield node;
                }
            }
        }
    }
}

/// Reads a node from storage by hash.
async fn load_node<Key, Value, Backend>(
    storage: &ContentAddressedStorage<Backend>,
    hash: &Blake3Hash,
) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend,
{
    let bytes = storage
        .retrieve(hash)
        .await?
        .ok_or_else(|| DialogSearchTreeError::Node(format!("Blob not found in storage: {hash}")))?;
    Ok(PersistentNode::new(Buffer::from(bytes)))
}

/// A stream of tree nodes.
///
/// This trait is implemented for any stream that yields `Result<Node<...>, Error>`.
/// Import this trait to use extension methods like [`into_hash_set`](TreeNodes::into_hash_set).
pub trait TreeNodes<Key, Value>:
    Stream<Item = Result<PersistentNode<Key, Value>, DialogSearchTreeError>>
where
    Key: self::Key,
    Value: self::Value,
{
    /// Collects all node hashes into a `HashSet`, ignoring errors.
    ///
    /// This is useful for comparing sets of nodes between trees.
    fn into_hash_set(self) -> impl std::future::Future<Output = HashSet<Blake3Hash>>;
}

impl<Key, Value, S> TreeNodes<Key, Value> for S
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    S: Stream<Item = Result<PersistentNode<Key, Value>, DialogSearchTreeError>>,
{
    fn into_hash_set(self) -> impl std::future::Future<Output = HashSet<Blake3Hash>> {
        self.filter_map(|result| async { result.ok().map(|node| node.hash().clone()) })
            .collect()
    }
}

/// Fixed width of spec keys: base bytes, a `0x00` delimiter, a leaf-rank
/// byte, a seam-rank byte, and zero padding.
pub const SPEC_KEY_LENGTH: usize = 8;

/// Key type used by spec trees, encoded as
/// `[base.., 0x00, leaf_rank, seam_rank, 0x00..]`.
///
/// The zero delimiter sorts below every alphabetic base byte, so the rank
/// suffix never affects ordering between distinct bases. Two rank bytes are
/// needed because the tree rolls two independent coins: the leaf coin over
/// entry keys (cuts a segment after the entry) and the seam coin over
/// separators (starts an index node before the child). The simulator forces
/// full-key separators (see [`DistributionSimulator::separator`]), so a
/// separator is a whole spec key and both bytes are always present in it.
pub type SpecKey = [u8; SPEC_KEY_LENGTH];

/// Type alias for the journaled storage backend used in tree specs.
pub type JournaledBackend = JournaledStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

/// Type alias for the storage type used in tree specs.
pub type TestStorage = ContentAddressedStorage<JournaledBackend>;

/// Type alias for the tree type used in tree specs.
pub type TestTree = PersistentTree<SpecKey, Vec<u8>, DistributionSimulator>;

/// Creates an empty journaled [`TestStorage`].
pub fn test_storage() -> TestStorage {
    ContentAddressedStorage::new(JournaledStorage::new(MemoryStorageBackend::default()))
}

/// A distribution that reads ranks directly from keys.
///
/// Spec keys are encoded as `[base, 0x00, leaf_rank, seam_rank, padding]`,
/// which makes the coins trivial: find the delimiter and read the byte at
/// the wanted offset. To make seam coins controllable at all, the simulator
/// stores the FULL right-hand key as every separator (the Dolt end of the
/// design space) instead of the shortest-distinguishing prefix: a shortest
/// prefix of distinct alphabetic bases would never include the rank bytes.
/// Both separator rules are canonical (a pure function of the seam keys), so
/// every tree property except byte-level compactness is exercised the same
/// way as production.
#[derive(Clone, Debug)]
pub struct DistributionSimulator;

/// Reads the rank byte at `offset` past the `0x00` delimiter of a spec key
/// (or full-key separator), defaulting to rank 1 when absent or zero.
fn rank_byte(bytes: &[u8], offset: usize) -> Rank {
    match bytes.iter().position(|byte| *byte == 0) {
        Some(delimiter) if delimiter + offset < bytes.len() && bytes[delimiter + offset] != 0 => {
            bytes[delimiter + offset] as Rank
        }
        _ => 1,
    }
}

impl Distribution for DistributionSimulator {
    fn rank(key: &[u8]) -> Rank {
        rank_byte(key, 1)
    }

    fn seam_rank(separator: &[u8]) -> Rank {
        rank_byte(separator, 2)
    }

    fn separator(_left: &[u8], right: &[u8]) -> Vec<u8> {
        right.to_vec()
    }

    fn reseparate(min: &[u8], _floor: &[u8]) -> Vec<u8> {
        min.to_vec()
    }
}

/// Encodes a base key with its leaf and seam ranks into a fixed-width
/// [`SpecKey`].
pub fn encode_key(base: &[u8], leaf_rank: Rank, seam_rank: Rank) -> SpecKey {
    assert!(
        base.len() + 3 <= SPEC_KEY_LENGTH,
        "spec key base too long: {:?}",
        String::from_utf8_lossy(base)
    );
    let mut key = [0u8; SPEC_KEY_LENGTH];
    key[..base.len()].copy_from_slice(base);
    key[base.len() + 1] = leaf_rank as u8;
    key[base.len() + 2] = seam_rank as u8;
    key
}

/// Decodes a spec key back into its base bytes (everything before the
/// `0x00` separator).
pub fn decode_key(encoded: &[u8]) -> Vec<u8> {
    let end = encoded
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(encoded.len());
    encoded[..end].to_vec()
}

/// Get the next alphabetic key (a -> b -> c -> ... -> z -> aa -> ab -> ...)
fn next_alpha_key(key: &[u8]) -> Vec<u8> {
    let mut result = key.to_vec();
    let mut i = result.len();

    loop {
        if i == 0 {
            // Overflow: we need to add a new character
            result.insert(0, b'a');
            break;
        }
        i -= 1;

        if result[i] < b'z' {
            result[i] += 1;
            break;
        } else {
            result[i] = b'a';
            // Continue to carry
        }
    }

    result
}

/// Build the two coin maps from the tree spec's per-level segments (each a
/// `(explicit first key, upper bound)` pair, top level first).
///
/// The leaf coin: every bottom-level upper bound gets leaf rank 2 (it ends
/// its segment); every other key gets the default 1. The seam coin: a node
/// at height `h >= 1` that is not the first of its level starts at a seam
/// that must punch through heights `1..=h`, so its first key gets seam rank
/// `h + 2` (`> BOTTOM_RANK + h`, and not above); a key starting nodes at
/// several heights keeps the highest. First keys are inferred the same way
/// the key inference does: an explicit range start, or the successor of the
/// previous node's bound.
pub fn build_coin_maps(
    levels: &[Vec<(Option<&str>, Vec<u8>)>],
) -> (HashSet<Vec<u8>>, HashMap<Vec<u8>, Rank>) {
    let leaf_boundaries: HashSet<Vec<u8>> = levels
        .last()
        .map(|bottom| bottom.iter().map(|(_, bound)| bound.clone()).collect())
        .unwrap_or_default();

    let mut seam_ranks: HashMap<Vec<u8>, Rank> = HashMap::new();
    for (level_idx, segments) in levels.iter().enumerate() {
        let height = levels.len() - level_idx - 1;
        if height == 0 {
            continue;
        }
        let rank = (height + 2) as Rank;

        let mut expected_next: Option<Vec<u8>> = None;
        for (first_key, bound) in segments {
            let start = match (first_key, &expected_next) {
                (Some(first), _) => first.as_bytes().to_vec(),
                (None, Some(next)) => next.clone(),
                // The first node of a level sits at the global leftmost
                // seam; nothing cuts before it, so it needs no seam rank.
                (None, None) => {
                    expected_next = Some(next_alpha_key(bound));
                    continue;
                }
            };
            if expected_next.is_some() {
                let entry = seam_ranks.entry(start).or_insert(rank);
                *entry = (*entry).max(rank);
            }
            expected_next = Some(next_alpha_key(bound));
        }
    }

    (leaf_boundaries, seam_ranks)
}

/// Expected operation on a node during differentiation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// Node should be read during differentiation
    Read,
    /// Node should be pruned without being read (its hash matched)
    Skip,
}

/// Describes a node in a tree specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeDescriptor {
    /// A range with explicit start and end keys (e.g., `a..b`).
    Range(String, String),
    /// A range with inferred start key (e.g., `..c`).
    OpenRange(String),
    /// A skipped/pruned range with explicit start and end keys (e.g., `(a..b)`).
    SkipRange(String, String),
    /// A skipped/pruned range with inferred start key (e.g., `(..k)`).
    SkipOpenRange(String),
}

/// A descriptor for building a deterministic tree structure.
///
/// Contains levels of node descriptors, from root level to leaf level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeDescriptor(pub Vec<Vec<NodeDescriptor>>);

impl TreeDescriptor {
    /// Validate the tree structure
    fn validate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Empty tree is valid - nothing to validate
        if self.0.is_empty() {
            return Ok(());
        }

        // Extract upper bounds from each level
        let mut levels_bounds: Vec<Vec<&str>> = Vec::new();

        for level_descriptors in &self.0 {
            let mut bounds = Vec::new();
            for descriptor in level_descriptors {
                let upper_bound = match descriptor {
                    NodeDescriptor::Range(_, last) => last.as_str(),
                    NodeDescriptor::OpenRange(last) => last.as_str(),
                    NodeDescriptor::SkipRange(_, last) => last.as_str(),
                    NodeDescriptor::SkipOpenRange(last) => last.as_str(),
                };
                bounds.push(upper_bound);
            }
            levels_bounds.push(bounds);
        }

        // Validate each level
        for (level_idx, bounds) in levels_bounds.iter().enumerate() {
            let height = self.0.len() - 1 - level_idx;

            // Check boundaries are in ascending order
            for i in 1..bounds.len() {
                if bounds[i] <= bounds[i - 1] {
                    return Err(format!(
                        "Boundaries at height {} must be in strictly ascending order: {:?} <= {:?}",
                        height,
                        bounds[i - 1],
                        bounds[i]
                    )
                    .into());
                }
            }

            // If not the bottom level, verify parent boundaries exist in children
            if level_idx + 1 < levels_bounds.len() {
                let child_bounds = &levels_bounds[level_idx + 1];
                for parent_bound in bounds {
                    if !child_bounds.contains(parent_bound) {
                        return Err(format!(
                            "Boundary '{}' at height {} must exist at height {}",
                            parent_bound,
                            height,
                            height - 1
                        )
                        .into());
                    }
                }
            }
        }

        Ok(())
    }

    /// Builds a tree from this descriptor using the provided storage.
    ///
    /// Returns a `TreeSpec` containing the built tree and node specifications
    /// for asserting read patterns during differential operations.
    pub async fn build(
        self,
        mut storage: TestStorage,
    ) -> Result<TreeSpec, Box<dyn std::error::Error + Send + Sync>> {
        // Validate the tree structure first
        self.validate()?;

        // Handle empty tree case
        if self.0.is_empty() {
            return Ok(TreeSpec {
                spec: Vec::new(),
                tree: TestTree::empty(),
                storage,
            });
        }

        // Disable journaling during tree building to avoid polluting with
        // build reads
        storage.backend().disable_journal();

        // First, collect metadata to build the tree
        let mut all_segments = Vec::new();
        // Track expected operations for each boundary
        let mut expected_ops: HashMap<(Vec<u8>, usize), Expect> = HashMap::new();

        for (level_idx, level_descriptors) in self.0.iter().enumerate() {
            let mut level_segment_specs = Vec::new();

            let height = self.0.len() - 1 - level_idx;

            for descriptor in level_descriptors {
                let (first_key, upper_bound, is_skipped) = match descriptor {
                    NodeDescriptor::Range(first, last) => {
                        (Some(first.as_str()), last.as_str(), false)
                    }
                    NodeDescriptor::OpenRange(last) => (None, last.as_str(), false),
                    NodeDescriptor::SkipRange(first, last) => {
                        (Some(first.as_str()), last.as_str(), true)
                    }
                    NodeDescriptor::SkipOpenRange(last) => (None, last.as_str(), true),
                };

                let boundary = upper_bound.as_bytes().to_vec();
                let expected_op = if is_skipped {
                    Expect::Skip
                } else {
                    Expect::Read
                };

                expected_ops.insert((boundary.clone(), height), expected_op);
                level_segment_specs.push((first_key, boundary.clone()));
            }

            all_segments.push(level_segment_specs);
        }

        // Infer all keys from the bottom level
        let bottom_segments = all_segments
            .last()
            .expect("tree_spec requires at least one level");
        let collection = Self::infer_keys_from_segments(bottom_segments);

        // Build the coin maps: leaf boundaries end segments, seam ranks
        // start index nodes.
        let (leaf_boundaries, seam_ranks) = build_coin_maps(&all_segments);

        // Build the tree by inserting every key with its coins encoded into
        // the key bytes, where DistributionSimulator reads them back out.
        // Values carry the decoded base key.
        let mut tree = TestTree::empty();
        let mut delta = Delta::zero();
        for key in &collection {
            let leaf_rank = if leaf_boundaries.contains(key) { 2 } else { 1 };
            let seam_rank = seam_ranks.get(key).copied().unwrap_or(1);
            tree = tree
                .edit()
                .insert(encode_key(key, leaf_rank, seam_rank), key.clone(), &storage)
                .await?
                .persist(&mut delta)?;

            // Flush after each persist so the next edit (and the differentials
            // that read afterwards) can load the nodes this persist created: a
            // persist writes new nodes only into the delta, never into storage.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        let root = tree.root().clone();

        // Now build NodeSpec levels from the actual tree
        let max_height = self.0.len() - 1;
        let mut spec = vec![Vec::new(); self.0.len()];

        if &root != NULL_BLAKE3_HASH {
            Self::build_spec_from_node(&mut spec, &root, &storage, max_height, &expected_ops)
                .await?;
        }

        // Reload the tree from its hash so the root is freshly loaded during
        // differentials (the build tree's node cache is dropped with it), and
        // re-enable journaling to track root and differential reads.
        let tree = TestTree::from_hash(root);
        storage.backend().enable_journal();

        Ok(TreeSpec {
            spec,
            tree,
            storage,
        })
    }

    /// Infer keys from segment specifications
    fn infer_keys_from_segments(segments: &[(Option<&str>, Vec<u8>)]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        let mut expected_next = vec![b'a'];

        for (first_key_opt, upper_bound) in segments {
            let start_key = if let Some(first_key_str) = first_key_opt {
                first_key_str.as_bytes().to_vec()
            } else {
                expected_next.clone()
            };

            let mut current = start_key;
            loop {
                keys.push(current.clone());
                if &current == upper_bound {
                    break;
                }
                current = next_alpha_key(&current);
            }

            expected_next = next_alpha_key(upper_bound);
        }

        keys
    }

    /// Recursively build NodeSpecs from the tree structure, returning the
    /// subtree's upper bound key.
    ///
    /// The spec identifies nodes by their decoded upper bound. Index links no
    /// longer carry bounds (they hold separators), so an index derives its
    /// own bound from its last child's, which the recursion computes anyway;
    /// specs are therefore pushed post-order, which the set-based assertions
    /// do not observe.
    async fn build_spec_from_node(
        spec: &mut [Vec<NodeSpec>],
        hash: &Blake3Hash,
        storage: &TestStorage,
        height: usize,
        expected_ops: &HashMap<(Vec<u8>, usize), Expect>,
    ) -> Result<SpecKey, DialogSearchTreeError> {
        let node = load_node::<SpecKey, Vec<u8>, JournaledBackend>(storage, hash).await?;

        let upper_bound: SpecKey = match node.body()? {
            ArchivedNodeBody::Segment(segment) => {
                SpecKey::try_from_bytes(&segment.last_key::<SpecKey>()?)?
            }
            ArchivedNodeBody::Index(index) => {
                let mut last: Option<SpecKey> = None;
                for link in index.links()? {
                    last = Some(
                        Box::pin(Self::build_spec_from_node(
                            spec,
                            &link.node,
                            storage,
                            height.saturating_sub(1),
                            expected_ops,
                        ))
                        .await?,
                    );
                }
                last.ok_or_else(|| {
                    DialogSearchTreeError::Node("Index was unexpectedly empty".into())
                })?
            }
        };

        let decoded_boundary = decode_key(upper_bound.as_ref());

        // Look up the expected operation for this node
        let expected_op = expected_ops
            .get(&(decoded_boundary.clone(), height))
            .cloned()
            .unwrap_or(Expect::Read);

        // Create and add the NodeSpec
        let level_idx = spec.len() - 1 - height;
        spec[level_idx].push(NodeSpec::new(
            decoded_boundary,
            height,
            node.hash().clone(),
            expected_op,
        ));

        Ok(upper_bound)
    }
}

/// Specification for a single node in the tree.
#[derive(Clone)]
pub struct NodeSpec {
    /// The upper bound key of this node (decoded base bytes).
    pub boundary: Vec<u8>,
    /// The height of this node in the tree (0 for leaves).
    pub height: usize,
    /// The content hash of this node.
    pub hash: Blake3Hash,
    /// The expected operation during differential (Read or Skip).
    pub expect: Expect,
}

impl NodeSpec {
    /// Creates a new NodeSpec with the given parameters.
    pub fn new(boundary: Vec<u8>, height: usize, hash: Blake3Hash, expected_op: Expect) -> Self {
        Self {
            boundary,
            height,
            hash,
            expect: expected_op,
        }
    }
}

impl Debug for NodeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeSpec")
            .field("boundary", &String::from_utf8_lossy(&self.boundary))
            .field("height", &self.height)
            .field("hash", &self.hash.to_string())
            .field("expect", &self.expect)
            .finish()
    }
}

/// Compiled TreeSpec with tree built and hashes populated.
///
/// This struct provides access to the built tree and methods for asserting
/// the expected read patterns during differential operations.
pub struct TreeSpec {
    /// Node specifications organized by level, with actual hashes populated.
    pub spec: Vec<Vec<NodeSpec>>,
    tree: TestTree,
    storage: TestStorage,
}

impl TreeSpec {
    /// Get a reference to the compiled tree
    pub fn tree(&self) -> &TestTree {
        &self.tree
    }

    /// Get a reference to the storage
    pub fn storage(&self) -> &TestStorage {
        &self.storage
    }

    /// Visualize the full tree structure by loading all nodes.
    ///
    /// Temporarily disables journaling during visualization to avoid
    /// polluting read tracking.
    #[allow(dead_code)]
    pub async fn visualize(&self) -> String {
        self.storage.backend().disable_journal();

        let mut output = String::new();

        if self.tree.root() != NULL_BLAKE3_HASH {
            Self::visualize_node(&mut output, self.tree.root(), &self.storage, "", true).await;
        } else {
            output.push_str("(empty tree)\n");
        }

        self.storage.backend().enable_journal();

        output
    }

    /// Produces tree visualization helpful for debugging
    #[allow(dead_code)]
    fn visualize_node<'a>(
        output: &'a mut String,
        hash: &'a Blake3Hash,
        storage: &'a TestStorage,
        prefix: &'a str,
        is_last: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let Ok(node) = load_node::<SpecKey, Vec<u8>, JournaledBackend>(storage, hash).await
            else {
                output.push_str(&format!("{prefix}(missing node {hash})\n"));
                return;
            };
            // Label nodes by their key span where full keys exist (segments)
            // and by child count for indexes, whose links carry only
            // separators.
            let (key_str, rank) = match node.body() {
                Ok(ArchivedNodeBody::Segment(segment)) => match segment.last_key::<SpecKey>() {
                    Ok(upper_bound) => (
                        String::from_utf8_lossy(&decode_key(&upper_bound)).to_string(),
                        DistributionSimulator::rank(&upper_bound),
                    ),
                    Err(_) => {
                        output.push_str(&format!("{prefix}(malformed node {hash})\n"));
                        return;
                    }
                },
                Ok(ArchivedNodeBody::Index(index)) => (format!("({} children)", index.len()), 0),
                Err(_) => {
                    output.push_str(&format!("{prefix}(malformed node {hash})\n"));
                    return;
                }
            };

            let branch = if is_last { "└── " } else { "├── " };

            let bytes = node.hash().as_bytes();
            let hash_str = format!(
                "{:02x}{:02x}{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3]
            );

            if prefix.is_empty() {
                output.push_str(&format!("{} [{}]@{}\n", key_str, rank, hash_str));
            } else {
                output.push_str(&format!(
                    "{}{}{} [{}]@{}\n",
                    prefix, branch, key_str, rank, hash_str
                ));
            }

            if let Ok(ArchivedNodeBody::Index(index)) = node.body() {
                let new_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
                let child_count = index.len();
                let Ok(links) = index.links() else {
                    return;
                };
                for (i, link) in links.iter().enumerate() {
                    let is_last_child = i == child_count - 1;
                    Self::visualize_node(output, &link.node, storage, &new_prefix, is_last_child)
                        .await;
                }
            }
        })
    }

    /// Assert that the expected read pattern matches the actual reads.
    ///
    /// Panics with a detailed diff if the pattern doesn't match.
    #[track_caller]
    pub fn assert(&self) {
        let reads = self.storage.backend().get_reads();

        // Build a set of hashes that were read
        let reads_set: HashSet<Blake3Hash> = reads.iter().cloned().collect();

        // Build expected/actual based on NodeSpecs
        // Use (boundary, height) tuples as keys
        let mut expected_reads = HashSet::new();
        let mut unexpected_reads = HashSet::new();
        let mut actual_reads = HashSet::new();

        for level in &self.spec {
            for node in level {
                let key = (node.boundary.clone(), node.height);

                match node.expect {
                    Expect::Read => {
                        expected_reads.insert(key.clone());
                    }
                    Expect::Skip => {
                        unexpected_reads.insert(key.clone());
                    }
                }

                if reads_set.contains(&node.hash) {
                    actual_reads.insert(key);
                }
            }
        }

        // Compare expected vs actual
        let missing_reads: Vec<_> = expected_reads.difference(&actual_reads).collect();
        let wrongly_read: Vec<_> = actual_reads.intersection(&unexpected_reads).collect();
        let unexpected_reads: Vec<_> = actual_reads
            .difference(&expected_reads)
            .filter(|n| !unexpected_reads.contains(n))
            .collect();

        // If everything matches, return early
        if missing_reads.is_empty() && wrongly_read.is_empty() && unexpected_reads.is_empty() {
            return;
        }

        // Build the comparison output as a string for the panic message
        let mut output = String::from("\n Read Pattern Mismatch \n");

        // ANSI color codes
        const GREEN: &str = "\x1b[32m";
        const RED: &str = "\x1b[31m";
        const RESET: &str = "\x1b[0m";

        // Calculate column widths for alignment
        let max_boundaries = self.spec.iter().map(|l| l.len()).max().unwrap_or(0);
        let mut column_widths = vec![0; max_boundaries];

        for level in &self.spec {
            for (i, node) in level.iter().enumerate() {
                let boundary_str = String::from_utf8_lossy(&node.boundary);
                let width = boundary_str.len() + 4; // "(..x)"
                column_widths[i] = column_widths[i].max(width);
            }
        }

        // Show expected pattern
        output.push_str("\nExpected:\n");
        for level in &self.spec {
            output.push_str("  [");
            for (i, node) in level.iter().enumerate() {
                let boundary_str = String::from_utf8_lossy(&node.boundary);
                let content = match node.expect {
                    Expect::Skip => format!("(..{})", boundary_str),
                    Expect::Read => format!("..{}", boundary_str),
                };
                if i > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format!("{:width$}", content, width = column_widths[i]));
            }
            output.push_str("]\n");
        }

        // Show actual reads with color highlighting
        output.push_str("\nActual:\n");
        for level in &self.spec {
            output.push_str("  [");
            for (i, node) in level.iter().enumerate() {
                let key = (node.boundary.clone(), node.height);
                let boundary_str = String::from_utf8_lossy(&node.boundary);
                let was_read = actual_reads.contains(&key);

                let (content, color_len) = match node.expect {
                    Expect::Skip => {
                        if was_read {
                            (format!("{}(..{}){}", RED, boundary_str, RESET), 9)
                        } else {
                            (format!("(..{})", boundary_str), 0)
                        }
                    }
                    Expect::Read => {
                        if was_read {
                            (format!("{}..{}{}", GREEN, boundary_str, RESET), 9)
                        } else {
                            (format!("{}..{}{}", RED, boundary_str, RESET), 9)
                        }
                    }
                };

                if i > 0 {
                    output.push_str(", ");
                }
                output.push_str(&format!(
                    "{:width$}",
                    content,
                    width = column_widths[i] + color_len
                ));
            }
            output.push_str("]\n");
        }

        // Add detailed errors
        if !missing_reads.is_empty() {
            output.push_str("\n❌ Missing expected reads:\n");
            for node_ref in &missing_reads {
                let boundary = String::from_utf8_lossy(&node_ref.0);
                output.push_str(&format!("  - {} @ height {}\n", boundary, node_ref.1));
            }
        }

        if !wrongly_read.is_empty() {
            output.push_str("\n❌ Expected skips were read:\n");
            for node_ref in &wrongly_read {
                let boundary = String::from_utf8_lossy(&node_ref.0);
                output.push_str(&format!("  - {} @ height {}\n", boundary, node_ref.1));
            }
        }

        if !unexpected_reads.is_empty() {
            output.push_str("\n⚠️  Unexpected reads:\n");
            for node_ref in &unexpected_reads {
                let boundary = String::from_utf8_lossy(&node_ref.0);
                output.push_str(&format!("  - {} @ height {}\n", boundary, node_ref.1));
            }
        }

        panic!("{}", output);
    }
}

/// # Tree Specification Macro
///
/// The `tree_spec!` macro allows you to visually define the exact structure of
/// a search tree for testing purposes. Instead of relying on unpredictable rank
/// distributions, you specify exactly which keys should be boundaries at which
/// heights.
///
/// ## Syntax
///
/// ```text
/// let spec = tree_spec![
///     [                        ..l]  // Height 1 (index nodes)
///     [..a, c..e, f..f, g..g, h..l]  // Height 0 (segment nodes/leaves)
/// ];
///
/// let tree = spec.build(storage).await?;
/// ```
///
/// ### Range Syntax Rules
///
/// 1. **Brackets `[...]`**: Each line represents one height level
///    - Top line = highest height (root/index nodes)
///    - Bottom line = height 0 (segment nodes/leaves)
///
/// 2. **Commas `,`**: Separate sibling segments within a level
///    - `[..a, c..e, ..l]` = three segments with upper bounds a, e, l
///
/// 3. **Range operators**: Define segment boundaries using Rust's range syntax
///    - `..x` = segment ending at 'x' (first key inferred from previous or
///      starts at 'a')
///    - `a..b` = segment explicitly from 'a' to 'b' (inclusive)
///    - Multi-char keys supported: `..aa`, `ab..az` (Excel-style naming: a-z, aa-az, etc.)
///
/// 4. **Parentheses**: Mark nodes expected to be pruned (not read) during a
///    differential: `(..x)` or `(a..b)`
///
/// ## Key Inference
///
/// The macro infers which keys exist in the tree based on specified ranges:
///
/// ```text
/// [..a, c..e, ..f, ..g, ..l]
/// ```
///
/// This creates:
/// - Range `..a`: contains key 'a' only
/// - Range `c..e`: contains keys c, d, e (first key explicit)
/// - Range `..f`: contains key 'f' only (next after 'e')
/// - Range `..g`: contains key 'g' only (next after 'f')
/// - Range `..l`: contains keys h, i, j, k, l (starts after 'g')
/// - Note: 'b' is NOT in the tree (gap between 'a' and 'c')
///
/// ## Structure Validation
///
/// The macro validates that:
/// 1. Every boundary in a parent level has a corresponding child
/// 2. Boundaries are in strictly ascending order
/// 3. Child boundaries don't exceed parent boundaries
#[macro_export]
macro_rules! tree_spec {
    // Empty tree case: tree_spec![]
    () => {{
        use $crate::helpers::*;
        TreeDescriptor(Vec::new())
    }};

    // Match the bracket-based tree format with range syntax
    // Segments can be: ..x (inferred start) or a..b (explicit range)
    // Parentheses indicate pruned nodes: (..x) or (a..b)
    (
        $(
            [$($( .. $end:ident)? $($first:ident .. $last:ident)? $( ( .. $pend:ident ) )? $( ( $pfirst:ident .. $plast:ident ) )?),+ $(,)?]
        )+
    ) => {{
        use $crate::helpers::*;

        // Parse each level - construct NodeDescriptor enums directly
        let mut levels: Vec<Vec<NodeDescriptor>> = Vec::new();

        $(
            let level = vec![
                $(
                    {
                        // Match: ..end, first..last, (..pend), or (pfirst..plast)
                        // Construct the appropriate NodeDescriptor variant
                        let descriptor: NodeDescriptor = {
                            // Normal (non-pruned) segments
                            $(
                                NodeDescriptor::OpenRange(stringify!($end).to_string())
                            )?
                            $(
                                NodeDescriptor::Range(stringify!($first).to_string(), stringify!($last).to_string())
                            )?
                            // Pruned segments
                            $(
                                NodeDescriptor::SkipOpenRange(stringify!($pend).to_string())
                            )?
                            $(
                                NodeDescriptor::SkipRange(stringify!($pfirst).to_string(), stringify!($plast).to_string())
                            )?
                        };
                        descriptor
                    }
                ),+
            ];
            levels.push(level);
        )+

        // Construct TreeDescriptor directly
        TreeDescriptor(levels)
    }};
}
