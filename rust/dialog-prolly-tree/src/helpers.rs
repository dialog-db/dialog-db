//! Tree specification macro and distribution simulator for testing.
//!
//! This module provides tools to create prolly trees with deterministic structure
//! for testing the differential algorithm.
//!
//! It also provides utilities for iterating over all nodes in a tree,
//! which is useful for testing, debugging, and advanced introspection.
//!
//! # Tree Specification Example
//!
//! ```no_run
//! use dialog_prolly_tree::tree_spec;
//! use dialog_storage::{Blake3Hash, CborEncoder, JournaledStorage, MemoryStorageBackend, Storage};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let storage = Storage {
//!     encoder: CborEncoder,
//!     backend: JournaledStorage::new(MemoryStorageBackend::default()),
//! };
//!
//! let spec = tree_spec![
//!     [                        ..l]  // Height 1 (index nodes)
//!     [..a, c..e, f..f, g..g, h..l]  // Height 0 (segment nodes/leaves)
//! ];
//!
//! let tree_spec = spec.build(storage).await?;
//! let tree = tree_spec.tree();
//! # Ok(())
//! # }
//! ```
//!
//! # Traversal Example
//!
//! ```no_run
//! use dialog_prolly_tree::{Tree, GeometricDistribution, Traversable, TraversalOrder, TreeNodes};
//! use dialog_storage::{Blake3Hash, CborEncoder, MemoryStorageBackend, Storage};
//!
//! # type TestTree = Tree<GeometricDistribution, Vec<u8>, Vec<u8>, Blake3Hash,
//! #     Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>>;
//! # async fn example(tree: &TestTree) -> Result<(), Box<dyn std::error::Error>> {
//! // Collect all node hashes for comparison
//! let hashes = tree.traverse(TraversalOrder::DepthFirst).into_hash_set().await;
//! println!("Tree contains {} nodes", hashes.len());
//! # Ok(())
//! # }
//! ```

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
};

use async_stream::try_stream;
use dialog_storage::{ContentAddressedStorage, HashType};
use futures_core::Stream;
use futures_util::StreamExt;

use crate::{DialogProllyTreeError, Distribution, KeyType, Node, Tree, ValueType};

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
pub trait Traversable<Key, Value, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
{
    /// Returns an async stream that traverses all nodes in the specified order.
    ///
    /// This yields every node in the tree, loading each node from storage lazily
    /// as it's visited.
    ///
    /// # Arguments
    /// * `order` - The traversal order:
    ///   - `DepthFirst`: Visit children before siblings (pre-order)
    ///   - `BreadthFirst`: Visit all nodes at each level before going deeper
    fn traverse(
        &self,
        order: TraversalOrder,
    ) -> impl Stream<Item = Result<Node<Key, Value, Hash>, DialogProllyTreeError>>;
}

impl<Distribution, Key, Value, Hash, Storage> Traversable<Key, Value, Hash>
    for Tree<Distribution, Key, Value, Hash, Storage>
where
    Distribution: crate::Distribution<Key, Hash>,
    Key: KeyType,
    Value: ValueType,
    Hash: HashType,
    Storage: ContentAddressedStorage<Hash = Hash>,
{
    fn traverse(
        &self,
        order: TraversalOrder,
    ) -> impl Stream<Item = Result<Node<Key, Value, Hash>, DialogProllyTreeError>> {
        let root = self.root().cloned();
        let storage = self.storage();

        try_stream! {
            if let Some(root) = root {
                // Yield the root first (it's already loaded)
                yield root.clone();

                // Enqueue root's children as references (loaded on demand)
                let mut queue = order.queue();
                if root.is_branch() {
                    queue.enqueue(root.references()?.iter().cloned());
                }

                // Process remaining nodes lazily
                while let Some(reference) = queue.dequeue() {
                    let node = Node::from_reference(reference, storage).await?;
                    yield node.clone();

                    if node.is_branch() {
                        queue.enqueue(node.references()?.iter().cloned());
                    }
                }
            }
        }
    }
}

/// A stream of tree nodes.
///
/// This trait is implemented for any stream that yields `Result<Node<...>, Error>`.
/// Import this trait to use extension methods like [`into_hash_set`](TreeNodes::into_hash_set).
pub trait TreeNodes<Key, Value, Hash>:
    Stream<Item = Result<Node<Key, Value, Hash>, DialogProllyTreeError>>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
{
    /// Collects all node hashes into a `HashSet`, ignoring errors.
    ///
    /// This is useful for comparing sets of nodes between trees.
    fn into_hash_set(self) -> impl std::future::Future<Output = std::collections::HashSet<Hash>>;
}

impl<Key, Value, Hash, S> TreeNodes<Key, Value, Hash> for S
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType + std::hash::Hash + Eq,
    S: Stream<Item = Result<Node<Key, Value, Hash>, DialogProllyTreeError>>,
{
    fn into_hash_set(self) -> impl std::future::Future<Output = std::collections::HashSet<Hash>> {
        self.filter_map(|r| async { r.ok().map(|n| n.hash().clone()) })
            .collect()
    }
}

/// Type alias for the journaled storage backend used in tree specs.
pub type JournaledBackend =
    dialog_storage::JournaledStorage<dialog_storage::MemoryStorageBackend<[u8; 32], Vec<u8>>>;

/// Type alias for the storage type used in tree specs.
pub type TestStorage = dialog_storage::Storage<dialog_storage::CborEncoder, JournaledBackend>;

/// Type alias for the tree type used in tree specs.
pub type TestTree =
    crate::Tree<DistributionSimulator, Vec<u8>, Vec<u8>, dialog_storage::Blake3Hash, TestStorage>;

/// A distribution that reads ranks directly from keys.
/// Keys are encoded as: [actual_key_bytes, 0x00, rank_byte]
/// This makes the distribution trivial - just read the last byte!
#[derive(Clone)]
pub struct DistributionSimulator;

impl<Hash> Distribution<Vec<u8>, Hash> for DistributionSimulator
where
    Hash: HashType,
{
    const BRANCH_FACTOR: u32 = 4;

    fn rank(key: &Vec<u8>) -> u32 {
        // Keys are encoded as [key_bytes, 0x00, rank_byte]
        // Just read the last byte as the rank
        if key.len() >= 2 && key[key.len() - 2] == 0x00 {
            key[key.len() - 1] as u32
        } else {
            1 // Default rank for keys without encoding
        }
    }
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

/// Build a rank map from the tree spec
/// For branching factor BF=4, we use generous rank spacing to ensure boundaries form
/// Height 0 boundaries get rank 2, height 1 get rank 4, height 2 get rank 6, etc.
/// If a boundary appears at multiple heights, it gets the HIGHEST rank.
pub fn build_rank_map(levels: &[Vec<Vec<u8>>]) -> HashMap<Vec<u8>, u32> {
    let mut rank_map = HashMap::new();

    // Process from bottom to top, so higher levels overwrite lower levels
    // This ensures keys appearing at multiple heights get the HIGHEST rank
    for (level_idx, boundaries) in levels.iter().enumerate().rev() {
        let height = levels.len() - level_idx - 1;
        let rank = (height + 2) as u32;

        for boundary in boundaries {
            // Insert or overwrite - higher heights (processed later in reverse) will overwrite
            rank_map.insert(boundary.clone(), rank);
        }
    }

    rank_map
}

/// Expected operation on a node during differentiation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// Node should be read during differentiation
    Read,
    /// Node is in memory and doesn't need to be read (e.g., root nodes)
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
        storage: TestStorage,
    ) -> Result<TreeSpec, Box<dyn std::error::Error + Send + Sync>> {
        use std::collections::BTreeMap;

        // Validate the tree structure first
        self.validate()?;

        // Handle empty tree case
        if self.0.is_empty() {
            let tree = crate::Tree::<
                DistributionSimulator,
                Vec<u8>,
                Vec<u8>,
                dialog_storage::Blake3Hash,
                _,
            >::new(storage.clone());
            return Ok(TreeSpec {
                spec: Vec::new(),
                tree,
                storage,
            });
        }

        // Disable journaling during tree building to avoid polluting with build reads
        storage.backend.disable_journal();

        // First, collect metadata to build the tree
        let mut all_segments = Vec::new();
        let mut boundaries_per_level = Vec::new();
        // Track expected operations for each boundary
        let mut expected_ops: HashMap<(Vec<u8>, usize), Expect> = HashMap::new();

        for (level_idx, level_descriptors) in self.0.iter().enumerate() {
            let mut level_segment_specs = Vec::new();
            let mut level_boundaries = Vec::new();

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
                level_boundaries.push(boundary);
            }

            all_segments.push(level_segment_specs);
            boundaries_per_level.push(level_boundaries);
        }

        // Infer all keys from the bottom level
        let bottom_segments = all_segments
            .last()
            .expect("tree_spec requires at least one level");
        let collection = Self::infer_keys_from_segments(bottom_segments);

        // Build rank map
        let ranks = build_rank_map(&boundaries_per_level);

        // Build tree with encoded keys
        let mut btree_collection = BTreeMap::new();
        for key in &collection {
            let rank = ranks.get(key).copied().unwrap_or(1);
            let mut encoded_key = key.clone();
            encoded_key.push(0x00);
            encoded_key.push(rank as u8);
            btree_collection.insert(encoded_key, key.clone());
        }

        let temp_tree = crate::Tree::from_collection(btree_collection, storage.clone()).await?;

        // Now build NodeSpec levels from the actual tree
        let max_height = self.0.len() - 1;
        let mut spec = vec![Vec::new(); self.0.len()];

        // Disable journaling during spec building to avoid tracking child loads
        storage.backend.disable_journal();

        let root_hash = if let Some(root) = temp_tree.root() {
            Box::pin(Self::build_spec_from_node(
                &mut spec,
                root,
                &storage,
                max_height,
                &expected_ops,
            ))
            .await;
            Some(*root.hash())
        } else {
            None
        };

        // Re-enable journaling to track root and differential reads
        storage.backend.enable_journal();

        // Load tree from hash so root is freshly loaded (not from temp_tree)
        let tree = if let Some(hash) = root_hash {
            crate::Tree::from_hash(&hash, storage.clone()).await?
        } else {
            temp_tree
        };

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

    /// Recursively build NodeSpecs from the tree structure
    async fn build_spec_from_node(
        spec: &mut [Vec<NodeSpec>],
        node: &crate::Node<Vec<u8>, Vec<u8>, dialog_storage::Blake3Hash>,
        storage: &TestStorage,
        height: usize,
        expected_ops: &HashMap<(Vec<u8>, usize), Expect>,
    ) {
        let decoded_boundary = decode_key(node.upper_bound());
        let hash = *node.hash();

        // Look up the expected operation for this node
        let expected_op = expected_ops
            .get(&(decoded_boundary.clone(), height))
            .cloned()
            .unwrap_or(Expect::Read);

        // Create and add the NodeSpec
        let level_idx = spec.len() - 1 - height;
        spec[level_idx].push(NodeSpec::new(decoded_boundary, height, hash, expected_op));

        if node.is_segment() {
            return;
        }

        // Only recurse if we have more levels to go and height won't underflow
        if height > 0
            && let Ok(children) = node.load_children(storage).await
        {
            for child in children {
                Box::pin(Self::build_spec_from_node(
                    spec,
                    &child,
                    storage,
                    height - 1,
                    expected_ops,
                ))
                .await;
            }
        }
    }
}

/// Specification for a single node in the tree.
#[derive(Clone)]
pub struct NodeSpec {
    /// The upper bound key of this node.
    pub boundary: Vec<u8>,
    /// The height of this node in the tree (0 for leaves).
    pub height: usize,
    /// The content hash of this node.
    pub hash: [u8; 32],
    /// The expected operation during differential (Read or Skip).
    pub expect: Expect,
}

impl NodeSpec {
    /// Creates a new NodeSpec with the given parameters.
    pub fn new(boundary: Vec<u8>, height: usize, hash: [u8; 32], expected_op: Expect) -> Self {
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
            .field("hash", &self.hash.display())
            .field("expect", &self.expect)
            .finish()
    }
}

/// Decode a key by removing the [0x00, rank] suffix
fn decode_key(encoded: &[u8]) -> Vec<u8> {
    if encoded.len() >= 2 && encoded[encoded.len() - 2] == 0x00 {
        encoded[..encoded.len() - 2].to_vec()
    } else {
        encoded.to_vec()
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

    /// Visualize the full tree structure by loading all nodes
    /// Temporarily disables journaling during visualization to avoid polluting
    /// read tracking
    #[allow(dead_code)]
    pub async fn visualize(&self) -> String {
        // Disable journaling during visualization
        self.storage.backend.disable_journal();

        let mut output = String::new();

        if let Some(root) = self.tree.root() {
            Self::visualize_node(&mut output, root, &self.storage, "", true).await;
        } else {
            output.push_str("(empty tree)\n");
        }

        // Re-enable journaling after visualization
        self.storage.backend.enable_journal();

        output
    }

    /// Produced tree visualisation helpful for debugging
    #[allow(dead_code)]
    fn visualize_node<'a>(
        output: &'a mut String,
        node: &'a crate::Node<Vec<u8>, Vec<u8>, dialog_storage::Blake3Hash>,
        storage: &'a TestStorage,
        prefix: &'a str,
        is_last: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let branch = if is_last { "└── " } else { "├── " };
            let boundary = node.upper_bound();
            let key_str = String::from_utf8_lossy(boundary).to_string();

            // Extract rank from encoded boundary
            let rank = if boundary.len() >= 2 && boundary[boundary.len() - 2] == 0x00 {
                boundary[boundary.len() - 1]
            } else {
                1
            };

            let hash = node.hash();
            let hash_str = format!(
                "{:02x}{:02x}{:02x}{:02x}",
                hash[0], hash[1], hash[2], hash[3]
            );

            if prefix.is_empty() {
                output.push_str(&format!("{} [{}]@{}\n", key_str, rank, hash_str));
            } else {
                output.push_str(&format!(
                    "{}{}{} [{}]@{}\n",
                    prefix, branch, key_str, rank, hash_str
                ));
            }

            if node.is_branch() {
                // Load children and recurse
                if let Ok(children) = node.load_children(storage).await {
                    let new_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
                    let child_count = children.len();
                    for (i, child) in children.iter().enumerate() {
                        let is_last_child = i == child_count - 1;
                        Self::visualize_node(output, child, storage, &new_prefix, is_last_child)
                            .await;
                    }
                }
            }
        })
    }

    /// Assert that the expected read pattern matches the actual reads.
    ///
    /// Panics with a detailed diff if the pattern doesn't match.
    #[track_caller]
    pub fn assert(&self) {
        let reads = self.storage.backend.get_reads();

        // Build a set of hashes that were read
        let reads_set: HashSet<[u8; 32]> = reads.iter().copied().collect();

        // Build expected/actual based on NodeSpecs
        // Use (boundary, height) tuples as keys
        let mut expected_reads = HashSet::new();
        let mut unexpected_reads = HashSet::new();
        let mut actual_reads = HashSet::new();

        for level in &self.spec {
            for node in level {
                let hash = node.hash;
                let key = (node.boundary.clone(), node.height);

                match node.expect {
                    Expect::Read => {
                        expected_reads.insert(key.clone());
                    }
                    Expect::Skip => {
                        unexpected_reads.insert(key.clone());
                    }
                }

                if reads_set.contains(&hash) {
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

impl std::fmt::Debug for TreeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(root) = self.tree.root() {
            Self::fmt_node(f, root, "", true)
        } else {
            write!(f, "(empty tree)")
        }
    }
}

impl TreeSpec {
    fn fmt_node(
        f: &mut std::fmt::Formatter<'_>,
        node: &crate::Node<Vec<u8>, Vec<u8>, dialog_storage::Blake3Hash>,
        prefix: &str,
        is_last: bool,
    ) -> std::fmt::Result {
        let branch = if is_last { "└── " } else { "├── " };
        let boundary = node.upper_bound();
        let key_str = String::from_utf8_lossy(boundary).to_string();

        // Extract rank from encoded boundary
        let rank = if boundary.len() >= 2 && boundary[boundary.len() - 2] == 0x00 {
            boundary[boundary.len() - 1]
        } else {
            1
        };

        let hash = node.hash();
        let hash_str = format!(
            "{:02x}{:02x}{:02x}{:02x}",
            hash[0], hash[1], hash[2], hash[3]
        );

        if prefix.is_empty() {
            writeln!(f, "{} [{}]@{}", key_str, rank, hash_str)?;
        } else {
            writeln!(f, "{}{}{} [{}]@{}", prefix, branch, key_str, rank, hash_str)?;
        }

        if node.is_branch()
            && let Ok(refs) = node.references()
        {
            let new_prefix = format!("{}{}", prefix, if is_last { "    " } else { "│   " });
            let ref_count = refs.len();
            for (i, reference) in refs.iter().enumerate() {
                let is_last_child = i == ref_count - 1;
                let child_branch = if is_last_child {
                    "└── "
                } else {
                    "├── "
                };

                let ref_boundary = reference.upper_bound();
                let ref_key_str = String::from_utf8_lossy(ref_boundary).to_string();

                let ref_rank = if ref_boundary.len() >= 2
                    && ref_boundary[ref_boundary.len() - 2] == 0x00
                {
                    ref_boundary[ref_boundary.len() - 1]
                } else {
                    1
                };

                let ref_hash = reference.hash();
                let ref_hash_str = format!(
                    "{:02x}{:02x}{:02x}{:02x}",
                    ref_hash[0], ref_hash[1], ref_hash[2], ref_hash[3]
                );

                writeln!(
                    f,
                    "{}{}{} [{}]@{} (ref)",
                    new_prefix, child_branch, ref_key_str, ref_rank, ref_hash_str
                )?;
            }
        }

        Ok(())
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
/// 4. **Key inference**:
///    - `..a` after nothing = starts at 'a', ends at 'a' (contains only 'a')
///    - `..d` after `..a` = starts at 'b' (next after 'a'), ends at 'd' (contains b, c, d)
///    - `c..e` = explicitly starts at 'c', ends at 'e' (contains c, d, e)
///    - `f..f` = starts and ends at 'f' (contains only 'f')
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
///
/// ## Boundary Checking
///
/// Check if boundaries exist at specific heights:
///
/// ```text
/// let spec = tree_spec![
///     [     ..d,      ..g]
///     [..a, ..d, ..f, ..g]
/// ];
///
/// assert!(spec.has_boundary("d", 1));  // Index node at height 1
/// assert!(spec.has_boundary("a", 0));  // Range at height 0
/// assert!(!spec.has_boundary("a", 1)); // 'a' doesn't exist at height 1
/// ```
///
/// ## Example: Overlapping Trees
///
/// ```text
/// let spec_a = tree_spec![
///     [                         ..l]
///     [..a, ..d, ..e, ..f, ..g, ..l]
/// ];
///
/// let spec_b = tree_spec![
///     [             ..s]
///     [f..f, g..g, h..s]
/// ];
///
/// // Build trees
/// let spec_a = spec_a.build(storage.clone()).await?;
/// let spec_b = spec_b.build(storage.clone()).await?;
///
/// // Test differential
/// let tree = spec_a.tree().clone();
/// let delta = tree.differentiate(spec_b.tree());
/// ```
///
/// In this example:
/// - Tree A has keys: a, b, c, d, e, f, g, h, i, j, k, l
/// - Tree B has keys: f, g, h, i, j, k, l, m, n, o, p, q, r, s
/// - Trees overlap in keys f-l, differ in a-e (only in A) and m-s (only in B)
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
