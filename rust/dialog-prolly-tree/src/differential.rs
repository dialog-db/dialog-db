//! Differential synchronization for dialog search trees.
//!
//! This module provides algorithms for computing and applying differences
//! between two dialog search trees. The key insight is that trees sharing
//! structure will have identical subtree hashes, allowing us to skip entire
//! subtrees during comparison.
//!
//! # Core Concepts
//!
//! - [`Change`]: Represents an addition or removal of an entry
//! - [`Differential`]: A stream of changes between two trees
//! - [`TreeDifference`]: Computes differences between trees with three key methods:
//!   - [`compute()`](TreeDifference::compute): Builds the difference structure
//!   - [`changes()`](TreeDifference::changes): Streams entry-level Add/Remove changes
//!   - [`novel_nodes()`](TreeDifference::novel_nodes): Streams nodes in target but not in source
//!
//! # Usage
//!
//! Use [`Tree::differentiate`](crate::Tree::differentiate) to compute changes between trees,
//! and [`Tree::integrate`](crate::Tree::integrate) to apply changes with deterministic conflict
//! resolution.
//!
//! ```text
//! // Compute changes from tree_a to tree_b
//! let changes = tree_a.differentiate(&tree_b);
//!
//! // Apply changes to another tree
//! tree_c.integrate(changes).await?;
//! ```
//!
//! For sync/replication scenarios where you need to push novel nodes to a remote:
//!
//! ```text
//! // Find nodes that local has but remote doesn't
//! let diff = TreeDifference::compute(&remote_tree, &local_tree).await?;
//! let novel = diff.novel_nodes();
//! // Push each novel node to remote storage
//! ```
//!
//! # Conflict Resolution
//!
//! When integrating changes, conflicts are resolved deterministically:
//! - For additions with conflicting values, the value with the higher hash wins
//! - For removals, only exact matches (same key and value) are removed

use std::cmp::Ordering;

use async_stream::try_stream;
use dialog_storage::{ContentAddressedStorage, HashType};
use futures_core::Stream;
use futures_util::StreamExt;

use crate::{DialogProllyTreeError, Entry, KeyType, Node, Reference, Tree, ValueType};

/// Represents a change in the key-value store.
#[derive(Clone)]
pub enum Change<Key, Value>
where
    Key: KeyType + 'static,
    Value: ValueType,
{
    /// Adds an entry to the key-value store.
    Add(Entry<Key, Value>),
    /// Removes an entry from the key-value store.
    Remove(Entry<Key, Value>),
}

/// Represents a differential stream of changes in the key-value store.
pub trait Differential<Key, Value>:
    Stream<Item = Result<Change<Key, Value>, DialogProllyTreeError>>
where
    Key: KeyType + 'static,
    Value: ValueType,
{
}

/// Default implementation for the `Differential` for matching streams.
impl<Key, Value, T> Differential<Key, Value> for T
where
    Key: KeyType + 'static,
    Value: ValueType,
    T: Stream<Item = Result<Change<Key, Value>, DialogProllyTreeError>>,
{
}

/// Represents either a loaded node or an unloaded reference in a sparse tree.
///
/// During tree differentiation, we don't want to eagerly load all nodes from storage.
/// Instead, we maintain a "sparse" representation where nodes can be either:
///
/// - **Loaded (`Node`)**: The full node data is in memory, either because it was
///   the root (already available) or because we needed to inspect its children.
/// - **Unloaded (`Ref`)**: We only have the hash and upper bound (from a parent's
///   child reference list). The actual node data remains in storage until needed.
///
/// This lazy loading is crucial for efficiency: when comparing two trees that share
/// large subtrees, we can detect equality by comparing hashes without ever loading
/// the shared nodes. Only nodes that differ need to be loaded and expanded.
///
/// # Lifecycle
///
/// 1. Roots start as `Node` (already in memory from the `Tree`)
/// 2. When a branch node is expanded, its children become `Ref` entries
/// 3. If a `Ref` needs expansion (it's a branch with differing content), it's
///    loaded from storage, becoming a `Node`, then immediately expanded
/// 4. Leaf nodes (`Ref` or `Node`) remain in the sparse tree until pruning
///    removes shared ones or streaming reads their entries
#[derive(Debug, Clone)]
pub(crate) enum SparseTreeNode<Key, Value, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
{
    /// A fully loaded node (already read from storage or held in memory)
    Node(Node<Key, Value, Hash>),
    /// An unloaded reference (hash + boundary, can be loaded on demand)
    Ref(Reference<Key, Hash>),
}

impl<Key, Value, Hash> SparseTreeNode<Key, Value, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
{
    /// Get the upper bound of this node or reference
    fn upper_bound(&self) -> &Key {
        match self {
            SparseTreeNode::Node(node) => node.upper_bound(),
            SparseTreeNode::Ref(reference) => reference.upper_bound(),
        }
    }

    /// Get the hash of this node or reference
    fn hash(&self) -> &Hash {
        match self {
            SparseTreeNode::Node(node) => node.hash(),
            SparseTreeNode::Ref(referrence) => referrence.hash(),
        }
    }

    /// Load this as a node (returns the node if already loaded, or loads from storage)
    async fn ensure_loaded<Storage>(
        self,
        storage: &Storage,
    ) -> Result<Node<Key, Value, Hash>, DialogProllyTreeError>
    where
        Storage: ContentAddressedStorage<Hash = Hash>,
    {
        match self {
            SparseTreeNode::Node(node) => Ok(node),
            SparseTreeNode::Ref(reference) => Node::from_reference(reference, storage).await,
        }
    }
}

/// A sparse, lazily-loaded view of a tree used for efficient differentiation.
///
/// `SparseTree` represents a tree as a flat list of [`SparseTreeNode`] entries,
/// sorted by their upper bounds. This structure enables efficient comparison
/// between two trees by:
///
/// 1. **Lazy loading**: Nodes start as references and are only loaded when needed
/// 2. **Level-by-level expansion**: Branch nodes can be "expanded" to reveal their
///    children, replacing one entry with multiple child entries
/// 3. **In-place pruning**: Shared nodes (same boundary + same hash) can be removed
///    from both trees simultaneously, leaving only differing nodes
///
/// # Algorithm Overview
///
/// When comparing trees A (source) and B (target):
///
/// ```text
/// Initial state:
///   A.nodes = [root_a]
///   B.nodes = [root_b]
///
/// After expansion (if roots differ):
///   A.nodes = [child_a1, child_a2, child_a3]  (refs to A's children)
///   B.nodes = [child_b1, child_b2]            (refs to B's children)
///   B.expanded = [root_b]                     (B's root is novel)
///
/// After pruning (remove shared nodes):
///   A.nodes = [child_a1]                      (only differing nodes remain)
///   B.nodes = [child_b2]
///
/// Continue until both are empty or contain only leaf segments...
/// ```
///
/// The `expanded` field accumulates all branch nodes from the target tree that
/// were loaded during this process. Combined with the remaining `nodes` (which
/// are leaf segments after full expansion), this gives the complete set of
/// novel nodes in the target tree.
#[derive(Debug, Clone)]
pub(crate) struct SparseTree<'a, Key, Value, Hash, Storage>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
    Storage: ContentAddressedStorage<Hash = Hash>,
{
    /// Reference to the storage backend for loading nodes on demand.
    storage: &'a Storage,
    /// Current frontier of sparse tree nodes, sorted by upper bound.
    /// Contains a mix of loaded nodes and unloaded references.
    nodes: Vec<SparseTreeNode<Key, Value, Hash>>,
    /// Branch nodes that were loaded and expanded during comparison.
    /// These represent novel index nodes in this tree.
    expanded: Vec<Node<Key, Value, Hash>>,
}

impl<Key, Value, Hash, Storage> SparseTree<'_, Key, Value, Hash, Storage>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
    Storage: ContentAddressedStorage<Hash = Hash>,
{
    /// Expands loaded branch nodes and references whose upper bound falls
    /// within the given range.
    ///
    /// For loaded branch nodes, this extracts their child references.
    /// For references, this loads them from storage and then extracts their
    /// child references if they're branches.
    ///
    /// Returns true if any expansion happened.
    pub async fn expand<R>(&mut self, range: R) -> Result<bool, DialogProllyTreeError>
    where
        R: std::ops::RangeBounds<Key>,
        Key: KeyType,
    {
        use std::ops::Bound;

        let mut expanded = false;
        let mut offset = 0;

        while offset < self.nodes.len() {
            let node = &self.nodes[offset];

            // For branch nodes, we need to check if they MIGHT contain children in the range
            // A branch with upper bound "f" might have children "a", "c", "f"
            // So we should expand it if it's >= the range start (it might overlap)
            let is_branch = matches!(node, SparseTreeNode::Node(n) if n.is_branch());

            let in_range = if is_branch {
                // For branches: expand if the node might contain children in the range
                // This means: node.upper_bound() >= range.start (node might have children in range)
                (match range.start_bound() {
                    Bound::Included(start) => node.upper_bound() >= start,
                    Bound::Excluded(start) => node.upper_bound() > start,
                    Bound::Unbounded => true,
                }) && (match range.end_bound() {
                    // Also check we're not completely past the range
                    Bound::Included(_) | Bound::Excluded(_) => true, // We'll filter children later
                    Bound::Unbounded => true,
                })
            } else {
                // For segment nodes: only expand if the upper bound is exactly
                // in the range
                (match range.end_bound() {
                    Bound::Included(end) => node.upper_bound() <= end,
                    Bound::Excluded(end) => node.upper_bound() < end,
                    Bound::Unbounded => true,
                }) && (match range.start_bound() {
                    Bound::Included(start) => node.upper_bound() >= start,
                    Bound::Excluded(start) => node.upper_bound() > start,
                    Bound::Unbounded => true,
                })
            };

            if !in_range {
                offset += 1;
                continue;
            }

            // Load and expand Refs and loaded branch nodes
            match &self.nodes[offset] {
                SparseTreeNode::Ref(reference) => {
                    // Load the Ref from storage
                    let node: Node<Key, Value, Hash> =
                        Node::from_hash(reference.hash().clone(), self.storage).await?;

                    // If it's a branch (index node), expand it
                    if node.is_branch()
                        && let Ok(refs) = node.references()
                    {
                        // Track this loaded node (only for branches that we splice out)
                        self.expanded.push(node.clone());

                        // Convert references to SparseTreeNode::Ref
                        let children: Vec<SparseTreeNode<Key, Value, Hash>> = refs
                            .iter()
                            .map(|r| SparseTreeNode::Ref(r.clone()))
                            .collect();

                        let num_children = children.len();
                        // Replace the Ref with its child references
                        self.nodes.splice(offset..offset + 1, children);
                        offset += num_children; // Skip past the children we just added
                        expanded = true;
                        continue;
                    }
                    // If it's a leaf (segment node), leave it in nodes
                    offset += 1;
                }
                SparseTreeNode::Node(node) => {
                    if node.is_branch()
                        && let Ok(refs) = node.references()
                    {
                        // Track this branch node before splicing out
                        self.expanded.push(node.clone());

                        // Convert references to SparseTreeNode::Ref
                        let children: Vec<SparseTreeNode<Key, Value, Hash>> = refs
                            .iter()
                            .map(|r| SparseTreeNode::Ref(r.clone()))
                            .collect();

                        let count = children.len();
                        // Replace the branch node with its child references
                        self.nodes.splice(offset..offset + 1, children);
                        offset += count; // Skip past the children we just added
                        expanded = true;
                        continue;
                    }
                    offset += 1;
                }
            }
        }

        Ok(expanded)
    }

    /// Removes nodes that are **shared** between this range (`self`) and
    /// another (`other`), keeping only nodes that differ between the two.
    ///
    /// # Behavior
    ///
    /// Both `self` and `other` are assumed to contain sorted sequences of
    /// [`Node`]s ordered by their [`upper_bound`](Node::upper_bound) key.
    /// Two nodes are considered *shared* if:
    ///
    /// - Their `upper_bound` keys are equal **and**
    /// - Their content hashes (`hash()`) are equal.
    ///
    /// When a shared node is detected, it is removed from **both** ranges.
    ///
    /// After this operation:
    ///
    /// - Each range remains sorted by `upper_bound`.
    /// - The relative order of all remaining nodes is preserved.
    ///
    /// # Algorithm
    ///
    /// This uses a **two-pointer merge-like traversal** over the sorted node
    /// lists.
    ///
    /// - `at_left` and `at_right` are *read heads* advancing through the
    ///   current elements.
    /// - `to_left` and `to_right` are *write heads* marking the next position
    ///   to keep.
    ///
    /// The traversal compares nodes’ `upper_bound()` values:
    ///
    /// * `left < right`:
    ///   The left node has a smaller key, so it can’t exist in the right list.
    ///   → Keep it in-place (swap into position if needed).
    ///
    /// * `left > right`:
    ///   The right node has a smaller key.
    ///   → Keep it in-place on the right side.
    ///
    /// * `left == right`:
    ///   Same key range — possible shared node.
    ///   - If the hashes differ → keep both (content changed).
    ///   - If the hashes match → remove both (shared, identical content).
    ///
    /// After the loop, any remaining elements from either side are moved forward intact.
    /// Finally, both vectors are truncated to their new compacted lengths.
    ///
    /// # Example
    ///
    /// Conceptually, if we have two sorted lists with some shared elements:
    ///
    /// ```text
    /// before: [A, B, C]  (nodes with upper bounds A, B, C)
    /// after:  [A, C, D]  (nodes with upper bounds A, C, D)
    ///
    /// After prune():
    /// before: [B]        (A and C were shared, so removed)
    /// after:  [D]        (A and C were shared, so removed)
    /// ```
    pub fn prune(&mut self, other: &mut Self) {
        let left = &mut self.nodes;
        let right = &mut other.nodes;

        // Read indices
        let mut at_left = 0;
        let mut at_right = 0;

        // Write indices — next position to place a node we're keeping
        let mut to_left = 0;
        let mut to_right = 0;

        // Traverse both sorted lists in lockstep
        while at_left < left.len() && at_right < right.len() {
            let left_node = &left[at_left];
            let right_node = &right[at_right];

            match left_node.upper_bound().cmp(right_node.upper_bound()) {
                Ordering::Less => {
                    // left node is unique → keep it
                    if to_left != at_left {
                        left.swap(to_left, at_left);
                    }
                    to_left += 1;
                    at_left += 1;
                }
                Ordering::Greater => {
                    // right node is unique → keep it
                    if to_right != at_right {
                        right.swap(to_right, at_right);
                    }
                    to_right += 1;
                    at_right += 1;
                }
                Ordering::Equal => {
                    // Same key range — possible shared content
                    if left_node.hash() != right_node.hash() {
                        // Different content → keep both
                        if to_left != at_left {
                            left.swap(to_left, at_left);
                        }
                        if to_right != at_right {
                            right.swap(to_right, at_right);
                        }
                        to_left += 1;
                        to_right += 1;
                    }
                    // else identical (shared) → skip both
                    at_left += 1;
                    at_right += 1;
                }
            }
        }

        // Move any remaining nodes from `left` (if right side exhausted)
        if at_left < left.len() && to_left != at_left {
            let remaining = left.len() - at_left;
            for offset in 0..remaining {
                left.swap(to_left + offset, at_left + offset);
            }
            to_left += remaining;
        } else if at_left == left.len() {
            // all done
        } else {
            to_left = left.len();
        }

        // Move any remaining nodes from `right` (if left side exhausted)
        if at_right < right.len() && to_right != at_right {
            let remaining = right.len() - at_right;
            for offset in 0..remaining {
                right.swap(to_right + offset, at_right + offset);
            }
            to_right += remaining;
        } else if at_right == right.len() {
            // all done
        } else {
            to_right = right.len();
        }

        // Drop the unused tail of each vector
        left.truncate(to_left);
        right.truncate(to_right);
    }

    /// Returns an async stream over all entries in this sparse tree.
    pub fn stream(
        &self,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogProllyTreeError>> + '_ {
        try_stream! {
            for sparse_node in &self.nodes {
                // Load the node if it's a reference
                let node = sparse_node.clone().ensure_loaded(self.storage).await?;

                let range = node.get_range(.., self.storage);
                for await entry in range {
                    match entry {
                        Ok(e) => yield e,
                        Err(err) => yield Err(err)?,
                    }
                }
            }
        }
    }
}

/// Represents a difference computed between two trees (source, target).
///
/// TreeDifference contains sparse representations of both trees and can be used
/// to produce either:
///
/// - Entry-level changes via [`changes()`](Self::changes) - can be applied to
///   transform source into target
/// - Node-level novelty via [`novel_nodes()`](Self::novel_nodes) - nodes in target abscent in source
///
/// # Usage
///
/// ```no_run
/// # use dialog_prolly_tree::{TreeDifference, Tree, GeometricDistribution};
/// # use dialog_storage::{Blake3Hash, CborEncoder, MemoryStorageBackend, Storage};
/// # type TestTree = Tree<GeometricDistribution, Vec<u8>, Vec<u8>, Blake3Hash,
/// #     Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>>;
/// # async fn example(source: &TestTree, target: &TestTree) -> Result<(), Box<dyn std::error::Error>> {
/// // Compute the difference (includes expansion)
/// let delta = TreeDifference::compute(&source, &target).await?;
///
/// // Option 1: Get entry-level changes (for replication/sync)
/// let changes = delta.changes();
///
/// // Option 2: Get novel nodes (for pushing to a remote storage)
/// let nodes = delta.novel_nodes();
/// # Ok(())
/// # }
/// ```
pub struct TreeDifference<'a, Key, Value, Hash, Storage>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
    Storage: ContentAddressedStorage<Hash = Hash>,
{
    /// Sparse tree representing the source state (what we're diffing from)
    source: SparseTree<'a, Key, Value, Hash, Storage>,
    /// Sparse tree representing the target state (what we're diffing to)
    target: SparseTree<'a, Key, Value, Hash, Storage>,
}

impl<'a, Key, Value, Hash, Storage> TreeDifference<'a, Key, Value, Hash, Storage>
where
    Key: KeyType + 'static,
    Value: ValueType + PartialEq,
    Hash: HashType,
    Storage: ContentAddressedStorage<Hash = Hash>,
{
    /// Computes the difference between two trees.
    ///
    /// Derives sparse representations of both trees and eliminates nodes that are
    /// identical between them (same boundary and hash). Only nodes that differ are
    /// retained, making it possible to efficiently compute entry-level changes via
    /// [`changes()`](Self::changes) or node-level novelty via [`novel_nodes()`](Self::novel_nodes).
    ///
    /// # Algorithm
    ///
    /// The key insight is that if two trees share nodes, they will share boundaries
    /// for those nodes. By comparing boundaries at each level, we can determine:
    ///
    /// 1. **Same boundary, same hash** → Nodes are identical, prune both
    /// 2. **Same boundary, different hash** → Nodes differ in structure below, expand both
    /// 3. **Different boundaries** → Nodes cover disjoint ranges, expand the larger one
    ///
    /// At each level, ranges grow larger (parent covers more than children). If one
    /// node's boundary is greater than another's, it means that node covers a larger
    /// range and might need expansion to reveal boundaries that match the other side's
    /// granularity.
    ///
    /// ```text
    /// Tree A:                Tree B:
    /// [          ..z]       [         ..z]
    /// [..f, ..m, ..z]       [    ..m, ..z]
    /// ```
    ///
    /// Both roots have boundary 'z' and different hashes → expand both.
    /// After expansion, A has boundaries {f, m, z} while B has {m, z}.
    /// Now we compare:
    /// - 'f' only in A → A covers \[MIN..f\], B starts after 'f' → disjoint
    /// - 'm' in both → compare, if hashes differ expand both
    /// - 'z' in both → compare, if hashes differ expand both
    ///
    /// This avoids expanding B's subtrees unnecessarily when looking for 'f'.
    ///
    /// # Parameters
    /// - `source`: The source tree (base state, e.g., what remote has)
    /// - `target`: The target tree (novel state, e.g., what we have locally)
    ///
    /// # Example
    /// ```no_run
    /// # use dialog_prolly_tree::{TreeDifference, Tree, GeometricDistribution};
    /// # use dialog_storage::{Blake3Hash, CborEncoder, MemoryStorageBackend, Storage};
    /// # type TestTree = Tree<GeometricDistribution, Vec<u8>, Vec<u8>, Blake3Hash,
    /// #     Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>>;
    /// # async fn example(remote_tree: &TestTree, local_tree: &TestTree) -> Result<(), Box<dyn std::error::Error>> {
    /// let diff = TreeDifference::compute(&remote_tree, &local_tree).await?;
    /// // changes() produces transforms remote → local
    /// // novel_nodes() yields nodes local has that remote doesn't
    /// # Ok(())
    /// # }
    /// ```
    pub async fn compute<Distribution: crate::Distribution<Key, Hash>>(
        source: &'a Tree<Distribution, Key, Value, Hash, Storage>,
        target: &'a Tree<Distribution, Key, Value, Hash, Storage>,
    ) -> Result<Self, DialogProllyTreeError> {
        let mut source = SparseTree {
            storage: source.storage(),
            nodes: source
                .root()
                .map(|root| vec![SparseTreeNode::Node(root.clone())])
                .unwrap_or_default(),
            expanded: vec![],
        };
        let mut target = SparseTree {
            storage: target.storage(),
            nodes: target
                .root()
                .map(|root| vec![SparseTreeNode::Node(root.clone())])
                .unwrap_or_default(),
            expanded: vec![],
        };

        // Iteratively expand and prune until only differing leaf nodes remain
        loop {
            // First, prune any nodes with matching boundaries and hashes
            // First, prune any nodes with matching boundaries and hashes
            source.prune(&mut target);

            // Compare boundaries and expand strategically
            // Use two-finger walk to compare sorted node lists
            let mut expanded = false;
            let mut source_idx = 0;
            let mut target_idx = 0;

            while source_idx < source.nodes.len() && target_idx < target.nodes.len() {
                let source_bound = source.nodes[source_idx].upper_bound().clone();
                let target_bound = target.nodes[target_idx].upper_bound().clone();

                match source_bound.cmp(&target_bound) {
                    Ordering::Less => {
                        // Source node has smaller boundary
                        // Expand ONLY the `target` node (larger boundary) to
                        // see if it contains something matching the source node
                        // Use exact range to expand only this specific node
                        if target
                            .expand(target_bound.clone()..=target_bound.clone())
                            .await?
                        {
                            expanded = true;
                            break; // Restart comparison after expansion
                        }
                        // Target couldn't be expanded, so source node is unique
                        source_idx += 1;
                    }
                    Ordering::Greater => {
                        // Target node has smaller boundary
                        // Expand ONLY the source node (larger boundary) to see
                        // if it contains something matching the target node
                        // Use exact range to expand only this specific node
                        if source
                            .expand(source_bound.clone()..=source_bound.clone())
                            .await?
                        {
                            expanded = true;
                            break; // Restart comparison after expansion
                        }
                        // Source couldn't be expanded, so target node is unique
                        target_idx += 1;
                    }
                    Ordering::Equal => {
                        // Same boundary - check if hashes differ
                        if source.nodes[source_idx].hash() != target.nodes[target_idx].hash() {
                            // Different hashes - need to expand both to find
                            // the difference Use exact ranges to expand only
                            // these specific nodes
                            if source
                                .expand(source_bound.clone()..=source_bound.clone())
                                .await?
                            {
                                expanded = true;
                            }
                            if target
                                .expand(target_bound.clone()..=target_bound.clone())
                                .await?
                            {
                                expanded = true;
                            }
                            // Don't increment - need to recompare after
                            // expansion
                            if expanded {
                                break;
                            }
                        }
                        // If hashes match, prune will remove them (at the
                        // begining of the loop)
                        source_idx += 1;
                        target_idx += 1;
                    }
                }
            }

            // If nothing was expanded, we've reached a fixed point
            if !expanded {
                break;
            }
        }

        // After comparison is done, expand any remaining target branch nodes.
        // This is needed for novel_nodes() to return all novel nodes, not just
        // the unexpanded branch references.
        loop {
            let mut expanded = false;
            for idx in 0..target.nodes.len() {
                let bound = target.nodes[idx].upper_bound().clone();
                if target.expand(bound.clone()..=bound).await? {
                    expanded = true;
                    break; // Restart after expansion changes the vec
                }
            }
            if !expanded {
                break;
            }
        }

        Ok(Self { source, target })
    }

    /// Returns a stream of entry-level changes between the two trees.
    ///
    /// This performs a two-cursor walk over the entry streams from both sparse
    /// trees, yielding Add and Remove changes as appropriate.
    ///
    /// # Example
    /// ```no_run
    /// # use dialog_prolly_tree::{TreeDifference, Tree, GeometricDistribution, Change};
    /// # use dialog_storage::{Blake3Hash, CborEncoder, MemoryStorageBackend, Storage};
    /// # use futures_util::{pin_mut, StreamExt};
    /// # type TestTree = Tree<GeometricDistribution, Vec<u8>, Vec<u8>, Blake3Hash,
    /// #     Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>>;
    /// # async fn example(old_tree: &TestTree, new_tree: &TestTree) -> Result<(), Box<dyn std::error::Error>> {
    /// let diff = TreeDifference::compute(&old_tree, &new_tree).await?;
    /// let changes = diff.changes();
    /// pin_mut!(changes);
    /// while let Some(change) = changes.next().await {
    ///     match change? {
    ///         Change::Add(entry) => println!("Added: {:?}", entry),
    ///         Change::Remove(entry) => println!("Removed: {:?}", entry),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn changes(&'a self) -> impl Differential<Key, Value> + 'a {
        let source_stream = self.source.stream();
        let target_stream = self.target.stream();

        try_stream! {
            futures_util::pin_mut!(source_stream);
            futures_util::pin_mut!(target_stream);

            let mut source_next = source_stream.next().await;
            let mut target_next = target_stream.next().await;

            loop {
                match (&source_next, &target_next) {
                    (None, None) => break,
                    (Some(Ok(source_entry)), None) => {
                        // Remaining source entries are removals
                        yield Change::Remove(source_entry.clone());
                        source_next = source_stream.next().await;
                    }
                    (None, Some(Ok(target_entry))) => {
                        // Remaining target entries are additions
                        yield Change::Add(target_entry.clone());
                        target_next = target_stream.next().await;
                    }
                    (Some(Err(_)), _) => {
                        // Propagate error from source stream
                        if let Some(Err(err)) = source_next.take() {
                            Err(err)?;
                        }
                    }
                    (_, Some(Err(_))) => {
                        // Propagate error from target stream
                        if let Some(Err(err)) = target_next.take() {
                            Err(err)?;
                        }
                    }
                    (Some(Ok(source_entry)), Some(Ok(target_entry))) => {
                        match source_entry.key.cmp(&target_entry.key) {
                            Ordering::Less => {
                                // Source key is smaller - it was removed
                                yield Change::Remove(source_entry.clone());
                                source_next = source_stream.next().await;
                            }
                            Ordering::Greater => {
                                // Target key is smaller - it was added
                                yield Change::Add(target_entry.clone());
                                target_next = target_stream.next().await;
                            }
                            Ordering::Equal => {
                                // Same key - check values
                                if source_entry.value != target_entry.value {
                                    // Value changed
                                    yield Change::Remove(source_entry.clone());
                                    yield Change::Add(target_entry.clone());
                                }
                                // If values are equal, skip (no change)
                                source_next = source_stream.next().await;
                                target_next = target_stream.next().await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Returns a stream of novel nodes, which are nodes that are in the
    /// `target` tree but not in `source` tree.
    ///
    /// Unlike [`changes()`](Self::changes) which streams entry-level changes,
    /// this method yields tree nodes. If a subtree is entirely novel (no
    /// corresponding node in source), it yield all nodes in that subtree.
    ///
    /// This is used during sync/replication where we need to send all new
    /// nodes to a remote that doesn't have them.
    ///
    /// # Example
    /// ```no_run
    /// # use dialog_prolly_tree::{TreeDifference, Tree, GeometricDistribution};
    /// # use dialog_storage::{Blake3Hash, CborEncoder, MemoryStorageBackend, Storage};
    /// # use futures_util::{pin_mut, StreamExt};
    /// # type TestTree = Tree<GeometricDistribution, Vec<u8>, Vec<u8>, Blake3Hash,
    /// #     Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>>;
    /// # async fn example(remote_tree: &TestTree, local_tree: &TestTree) -> Result<(), Box<dyn std::error::Error>> {
    /// let diff = TreeDifference::compute(&remote_tree, &local_tree).await?;
    /// let nodes = diff.novel_nodes();
    /// pin_mut!(nodes);
    /// while let Some(node) = nodes.next().await {
    ///     let node = node?;
    ///     // Push node to remote storage
    ///     // remote_storage.put(node.hash(), node.encode()).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn novel_nodes(
        &self,
    ) -> impl Stream<Item = Result<Node<Key, Value, Hash>, DialogProllyTreeError>> + '_ {
        try_stream! {
            // First, yield all (index) nodes that were expanded during
            // comparison.
            for node in &self.target.expanded {
                yield node.clone();
            }

            // Then, yield all the remaining (segment) nodes. They will be
            // segmentns because all index nodes are either pruned when found
            // in source tree or expanded otherwise.
            for node in &self.target.nodes {
                yield node.clone().ensure_loaded(self.target.storage).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GeometricDistribution, Traversable, TraversalOrder, Tree, TreeNodes, tree_spec};
    use anyhow::Result;
    use dialog_storage::{
        Blake3Hash, CborEncoder, JournaledStorage, MemoryStorageBackend, Storage,
    };
    use futures_util::{StreamExt, TryStreamExt, pin_mut, stream::iter};

    type TestTree = Tree<
        GeometricDistribution,
        Vec<u8>,
        Vec<u8>,
        Blake3Hash,
        Storage<CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    >;

    #[dialog_common::test]
    async fn test_differentiate_identical_trees() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree1.set(vec![1], vec![10]).await.unwrap();
        tree1.set(vec![2], vec![20]).await.unwrap();
        tree2.set(vec![1], vec![10]).await.unwrap();
        tree2.set(vec![2], vec![20]).await.unwrap();

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut count = 0;
        while changes.next().await.is_some() {
            count += 1;
        }

        assert_eq!(count, 0, "Identical trees should have no changes");
    }

    #[dialog_common::test]
    async fn test_differentiate_added_entry() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree1.set(vec![1], vec![10]).await.unwrap();
        tree1.set(vec![2], vec![20]).await.unwrap();
        tree2.set(vec![1], vec![10]).await.unwrap();

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut adds = Vec::new();
        let mut removes = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].key, vec![2]);
        assert_eq!(adds[0].value, vec![20]);
        assert_eq!(removes.len(), 0);
    }

    #[dialog_common::test]
    async fn test_differentiate_removed_entry() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree1.set(vec![1], vec![10]).await.unwrap();
        tree2.set(vec![1], vec![10]).await.unwrap();
        tree2.set(vec![2], vec![20]).await.unwrap();

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut adds = Vec::new();
        let mut removes = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(adds.len(), 0);
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].key, vec![2]);
        assert_eq!(removes[0].value, vec![20]);
    }

    #[dialog_common::test]
    async fn test_differentiate_modified_entry() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree1.set(vec![1], vec![10]).await.unwrap();
        tree1.set(vec![2], vec![30]).await.unwrap();
        tree2.set(vec![1], vec![10]).await.unwrap();
        tree2.set(vec![2], vec![20]).await.unwrap();

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut adds = Vec::new();
        let mut removes = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].key, vec![2]);
        assert_eq!(adds[0].value, vec![30]);
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].key, vec![2]);
        assert_eq!(removes[0].value, vec![20]);
    }

    #[dialog_common::test]
    async fn test_differentiate_empty_to_populated() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree1.set(vec![1], vec![10]).await.unwrap();
        tree1.set(vec![2], vec![20]).await.unwrap();

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut adds = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(_) => panic!("Should not have removes"),
            }
        }

        assert_eq!(adds.len(), 2);
    }

    #[dialog_common::test]
    async fn test_differentiate_populated_to_empty() {
        let backend = MemoryStorageBackend::default();
        let tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree2.set(vec![1], vec![10]).await.unwrap();
        tree2.set(vec![2], vec![20]).await.unwrap();

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut removes = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(_) => panic!("Should not have adds"),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(removes.len(), 2);
    }

    #[dialog_common::test]
    async fn test_differentiate_large_tree() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Create a larger tree to test branch handling
        for i in 0..100u8 {
            tree1.set(vec![i], vec![i * 2]).await.unwrap();
            if i != 50 {
                // Skip one entry in tree2
                tree2.set(vec![i], vec![i * 2]).await.unwrap();
            }
        }

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut adds = Vec::new();
        let mut removes = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].key, vec![50]);
        assert_eq!(removes.len(), 0);
    }

    #[dialog_common::test]
    async fn test_integrate_add_new_entry() {
        let backend = MemoryStorageBackend::default();
        let mut tree = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree.set(vec![1], vec![10]).await.unwrap();

        let changes = vec![Change::Add(Entry {
            key: vec![2],
            value: vec![20],
        })];

        tree.integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(vec![10]));
        assert_eq!(tree.get(&vec![2]).await.unwrap(), Some(vec![20]));
    }

    #[dialog_common::test]
    async fn test_integrate_add_idempotent() {
        let backend = MemoryStorageBackend::default();
        let mut tree = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree.set(vec![1], vec![10]).await.unwrap();

        // Add same entry - should be no-op
        let changes = vec![Change::Add(Entry {
            key: vec![1],
            value: vec![10],
        })];

        tree.integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(vec![10]));
    }

    #[dialog_common::test]
    async fn test_integrate_add_conflict_resolution() {
        let backend = MemoryStorageBackend::default();
        let mut tree = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Set initial value
        tree.set(vec![1], vec![10]).await.unwrap();

        // Try to add different value - conflict resolution by hash comparison
        let new_value = vec![20];
        let existing_value = vec![10];

        let changes = vec![Change::Add(Entry {
            key: vec![1],
            value: new_value.clone(),
        })];

        tree.integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        // Check which value won based on hash comparison
        use dialog_storage::Encoder;
        let storage = tree.storage();
        let (existing_hash, _) = storage.encode(&existing_value).await.unwrap();
        let (new_hash, _) = storage.encode(&new_value).await.unwrap();

        if new_hash.as_ref() > existing_hash.as_ref() {
            assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(new_value));
        } else {
            assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(existing_value));
        }
    }

    #[dialog_common::test]
    async fn test_integrate_remove_existing() {
        let backend = MemoryStorageBackend::default();
        let mut tree = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree.set(vec![1], vec![10]).await.unwrap();
        tree.set(vec![2], vec![20]).await.unwrap();

        let changes = vec![Change::Remove(Entry {
            key: vec![1],
            value: vec![10],
        })];

        tree.integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        assert_eq!(tree.get(&vec![1]).await.unwrap(), None);
        assert_eq!(tree.get(&vec![2]).await.unwrap(), Some(vec![20]));
    }

    #[dialog_common::test]
    async fn test_integrate_remove_nonexistent() {
        let backend = MemoryStorageBackend::default();
        let mut tree = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree.set(vec![1], vec![10]).await.unwrap();

        // Remove non-existent entry - should be no-op
        let changes = vec![Change::Remove(Entry {
            key: vec![2],
            value: vec![20],
        })];

        tree.integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(vec![10]));
        assert_eq!(tree.get(&vec![2]).await.unwrap(), None);
    }

    #[dialog_common::test]
    async fn test_integrate_remove_wrong_value() {
        let backend = MemoryStorageBackend::default();
        let mut tree = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree.set(vec![1], vec![10]).await.unwrap();

        // Try to remove with wrong value - should be no-op (concurrent update)
        let changes = vec![Change::Remove(Entry {
            key: vec![1],
            value: vec![20], // Wrong value
        })];

        tree.integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        // Entry should still exist with original value
        assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(vec![10]));
    }

    #[dialog_common::test]
    async fn test_integrate_concurrent_updates() {
        let backend = MemoryStorageBackend::default();

        // Initial state - both replicas start with same value
        let mut tree_a = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree_b = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree_a.set(vec![1], vec![10]).await.unwrap();
        tree_b.set(vec![1], vec![10]).await.unwrap();

        // Replica A updates to value_a
        tree_a.set(vec![1], vec![20]).await.unwrap();

        // Replica B updates to value_b
        tree_b.set(vec![1], vec![30]).await.unwrap();

        // Both replicas exchange their changes
        let empty_tree = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        let host_a = tree_a.clone();
        let changes_a = empty_tree.differentiate(&host_a);
        let host_b = tree_b.clone();
        let changes_b = empty_tree.differentiate(&host_b);

        // Integrate changes
        tree_a.integrate(changes_b).await.unwrap();
        tree_b.integrate(changes_a).await.unwrap();

        // Both should converge to the same value (deterministic by hash)
        let final_a = tree_a.get(&vec![1]).await.unwrap();
        let final_b = tree_b.get(&vec![1]).await.unwrap();

        assert_eq!(final_a, final_b, "Trees should converge to same value");

        // Verify the winner is determined by hash
        use dialog_storage::Encoder;
        let storage = tree_a.storage();
        let (hash_20, _) = storage.encode(&vec![20]).await.unwrap();
        let (hash_30, _) = storage.encode(&vec![30]).await.unwrap();

        if hash_20.as_ref() > hash_30.as_ref() {
            assert_eq!(final_a, Some(vec![20]));
        } else {
            assert_eq!(final_a, Some(vec![30]));
        }
    }

    // Roundtrip tests: Verify differentiate + integrate produces original tree
    #[dialog_common::test]
    async fn test_roundtrip_empty_to_populated() {
        let backend = MemoryStorageBackend::default();
        let mut target = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut start = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Target has entries, start is empty
        target.set(vec![1], vec![10]).await.unwrap();
        target.set(vec![2], vec![20]).await.unwrap();
        target.set(vec![3], vec![30]).await.unwrap();

        // Compute diff and integrate
        // Need to collect changes to avoid borrow checker issues
        // (diff holds immutable ref to start, but integrate needs mutable ref)
        let changes = {
            let diff = start.differentiate(&target);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        // Verify start now matches target
        assert_eq!(start.get(&vec![1]).await.unwrap(), Some(vec![10]));
        assert_eq!(start.get(&vec![2]).await.unwrap(), Some(vec![20]));
        assert_eq!(start.get(&vec![3]).await.unwrap(), Some(vec![30]));
    }

    #[dialog_common::test]
    async fn test_roundtrip_populated_to_empty() {
        let backend = MemoryStorageBackend::default();
        let target = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut start = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Start has entries, target is empty
        start.set(vec![1], vec![10]).await.unwrap();
        start.set(vec![2], vec![20]).await.unwrap();
        start.set(vec![3], vec![30]).await.unwrap();

        // Compute diff and integrate
        // Need to collect changes to avoid borrow checker issues
        // (diff holds immutable ref to start, but integrate needs mutable ref)
        let changes = {
            let diff = start.differentiate(&target);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        // Verify start is now empty
        assert_eq!(start.get(&vec![1]).await.unwrap(), None);
        assert_eq!(start.get(&vec![2]).await.unwrap(), None);
        assert_eq!(start.get(&vec![3]).await.unwrap(), None);
    }

    #[dialog_common::test]
    async fn test_roundtrip_mixed_changes() {
        let backend = MemoryStorageBackend::default();
        let mut target = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut start = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Start state: keys 1, 2, 3
        start.set(vec![1], vec![10]).await.unwrap();
        start.set(vec![2], vec![20]).await.unwrap();
        start.set(vec![3], vec![30]).await.unwrap();

        // Target state: keys 2 (modified), 3, 4
        target.set(vec![2], vec![22]).await.unwrap(); // Modified
        target.set(vec![3], vec![30]).await.unwrap(); // Same
        target.set(vec![4], vec![40]).await.unwrap(); // Added
        // Key 1 removed

        // Compute diff and integrate
        // Need to collect changes to avoid borrow checker issues
        // (diff holds immutable ref to start, but integrate needs mutable ref)
        let changes = {
            let diff = start.differentiate(&target);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        // Verify start now matches target
        assert_eq!(start.get(&vec![1]).await.unwrap(), None);
        assert_eq!(start.get(&vec![2]).await.unwrap(), Some(vec![22]));
        assert_eq!(start.get(&vec![3]).await.unwrap(), Some(vec![30]));
        assert_eq!(start.get(&vec![4]).await.unwrap(), Some(vec![40]));
    }

    #[dialog_common::test]
    async fn test_roundtrip_large_tree() {
        let backend = MemoryStorageBackend::default();
        let mut target = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut start = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Create large trees with many entries
        for i in 0u16..100 {
            start
                .set(vec![i as u8], vec![(i * 10) as u8])
                .await
                .unwrap();
        }

        for i in 50u16..150 {
            target
                .set(vec![i as u8], vec![(i * 20) as u8])
                .await
                .unwrap();
        }

        // Compute diff and integrate
        // Need to collect changes to avoid borrow checker issues
        // (diff holds immutable ref to start, but integrate needs mutable ref)
        let changes = {
            let diff = start.differentiate(&target);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        // Verify start now matches target
        for i in 0u16..50 {
            assert_eq!(start.get(&vec![i as u8]).await.unwrap(), None);
        }
        for i in 50u16..150 {
            assert_eq!(
                start.get(&vec![i as u8]).await.unwrap(),
                Some(vec![(i * 20) as u8])
            );
        }
    }

    // Edge case tests

    #[dialog_common::test]
    async fn test_differentiate_both_empty() {
        let backend = MemoryStorageBackend::default();
        let tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut count = 0;
        while changes.next().await.is_some() {
            count += 1;
        }

        assert_eq!(count, 0, "Both empty trees should have no changes");
    }

    #[dialog_common::test]
    async fn test_differentiate_single_entry_trees() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        tree1.set(vec![1], vec![10]).await.unwrap();
        tree2.set(vec![1], vec![20]).await.unwrap();

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut adds = Vec::new();
        let mut removes = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        // Should have one remove (old value) and one add (new value)
        assert_eq!(removes.len(), 1);
        assert_eq!(removes[0].value, vec![20]);
        assert_eq!(adds.len(), 1);
        assert_eq!(adds[0].value, vec![10]);
    }

    #[dialog_common::test]
    async fn test_differentiate_disjoint_trees() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Completely disjoint key sets
        tree1.set(vec![1], vec![10]).await.unwrap();
        tree1.set(vec![3], vec![30]).await.unwrap();
        tree1.set(vec![5], vec![50]).await.unwrap();

        tree2.set(vec![2], vec![20]).await.unwrap();
        tree2.set(vec![4], vec![40]).await.unwrap();
        tree2.set(vec![6], vec![60]).await.unwrap();

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut adds = Vec::new();
        let mut removes = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        // All of tree2's entries should be removed, all of tree1's added
        assert_eq!(removes.len(), 3);
        assert_eq!(adds.len(), 3);
    }

    #[dialog_common::test]
    async fn test_differentiate_subset_superset() {
        let backend = MemoryStorageBackend::default();
        let mut superset = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut subset = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Subset: keys 2, 3
        subset.set(vec![2], vec![20]).await.unwrap();
        subset.set(vec![3], vec![30]).await.unwrap();

        // Superset: keys 1, 2, 3, 4
        superset.set(vec![1], vec![10]).await.unwrap();
        superset.set(vec![2], vec![20]).await.unwrap();
        superset.set(vec![3], vec![30]).await.unwrap();
        superset.set(vec![4], vec![40]).await.unwrap();

        let changes = subset.differentiate(&superset);
        pin_mut!(changes);
        let mut adds = Vec::new();
        let mut removes = Vec::new();

        while let Some(result) = changes.next().await {
            match result.unwrap() {
                Change::Add(entry) => adds.push(entry),
                Change::Remove(entry) => removes.push(entry),
            }
        }

        // Should add keys 1 and 4
        assert_eq!(adds.len(), 2);
        assert_eq!(removes.len(), 0);
    }

    #[dialog_common::test]
    async fn test_differentiate_all_modified() {
        let backend = MemoryStorageBackend::default();
        let mut tree1 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut tree2 = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Same keys, all different values (except i=0 where both are [0])
        for i in 0u8..10 {
            tree1.set(vec![i], vec![i * 2]).await.unwrap();
            tree2.set(vec![i], vec![i]).await.unwrap();
        }

        let changes = tree2.differentiate(&tree1);
        pin_mut!(changes);
        let mut count = 0;

        while let Some(result) = changes.next().await {
            result.unwrap();
            count += 1;
        }

        // 9 keys modified (i=0 has same value in both) = 9 * 2 = 18 changes total
        assert_eq!(count, 18);
    }

    #[dialog_common::test]
    async fn test_roundtrip_preserves_hash() {
        let backend = MemoryStorageBackend::default();
        let mut target = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });
        let mut start = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        // Set up target state
        for i in 0..20 {
            target.set(vec![i], vec![i * 3]).await.unwrap();
        }
        let target_hash = *target.hash().unwrap();

        // Set up different start state
        for i in 10..30 {
            start.set(vec![i], vec![i * 5]).await.unwrap();
        }

        // Compute diff and integrate
        // Need to collect changes to avoid borrow checker issues
        // (diff holds immutable ref to start, but integrate needs mutable ref)
        let changes = {
            let diff = start.differentiate(&target);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(iter(changes.into_iter().map(Ok)))
            .await
            .unwrap();

        // Hash should match after integration
        assert_eq!(start.hash().unwrap(), &target_hash);
    }

    // ========================================================================
    // Performance tests using JournaledStorage
    // ========================================================================

    #[dialog_common::test]
    async fn test_diff_shared_left_subtree() -> Result<()> {
        let backend = MemoryStorageBackend::default();

        let storage_a = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        let storage_b = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Define tree structures using tree_spec! macro with read/prune expectations
        // () indicates nodes that should be pruned (not read)
        let spec_a = tree_spec![
            [                  ..l]
            [(..e), (f..i),    ..l]
        ]
        .build(storage_a.clone())
        .await
        .unwrap();

        let spec_b = tree_spec![
            [                  ..s]
            [(..e), (f..i), ..m, ..s]
            // [..b, ..e, (f..i), ..m,      ..s]
        ]
        .build(storage_b.clone())
        .await
        .unwrap();

        // Run differentiate (journal is automatically enabled after build)
        let host_b = spec_b.tree().clone();
        let diff = host_b.differentiate(spec_a.tree());
        // consume so we actually perform reads
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_fully_disjoint_trees() -> Result<()> {
        let backend = JournaledStorage::new(MemoryStorageBackend::default());
        let storage = Storage {
            encoder: CborEncoder,
            backend,
        };

        // Scenario: Trees have completely different key ranges - NO shared segments
        // Tree A has keys a-i, Tree B has keys p-x (completely disjoint)
        // All segments from both trees must be read since nothing is shared
        let spec_a = tree_spec![
            [                         ..i]
            [..a, ..d, ..e, ..f, ..g, ..i]
        ]
        .build(storage.clone())
        .await
        .unwrap();

        let spec_b = tree_spec![
            [                         ..x]
            [..p, ..s, ..t, ..u, ..v, ..x]
        ]
        .build(storage.clone())
        .await
        .unwrap();

        let host_b = spec_b.tree().clone();
        let diff = host_b.differentiate(spec_a.tree());
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_subset_superset() -> Result<()> {
        let backend = MemoryStorageBackend::default();

        let storage_a = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        let storage_b = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Scenario: Tree B extends Tree A with additional segment
        // Tree A is the host tree, so its nodes are in memory (not read from storage)
        // Tree B has an additional segment 's' that needs to be read
        // The shared 'n' subtree in Tree B is pruned, so its children (e, n) are not read
        // Only Tree B's additional segment 's' should be read
        let spec_a = tree_spec![
            [         ..n]
            [(..e), (..n)]
        ]
        .build(storage_a.clone())
        .await
        .unwrap();

        let spec_b = tree_spec![
            [(       ..n), ..s]
            [(..e), (..n), ..s]
        ]
        .build(storage_b.clone())
        .await
        .unwrap();

        let host_a = spec_a.tree().clone();
        let diff = host_a.differentiate(spec_b.tree());
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_single_key_change() -> Result<()> {
        let backend = MemoryStorageBackend::default();

        let storage_a = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        let storage_b = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Scenario: Two trees with only one differing segment
        // Tree A has segments ending at 'a', 'e'
        // Tree B has segments ending at 'a', 'f', 'k', 'p', 's' with different 'k' segment
        // Segment 'a' is shared (same boundary, same hash) so should not be read
        // Only the changed segments should be read from both trees
        let spec_a = tree_spec![
            [          ..e]
            [(..a),    ..e]
        ]
        .build(storage_a.clone())
        .await
        .unwrap();

        // Tree B: has more segments, with 'a' shared but different keys in 'k' segment
        let spec_b = tree_spec![
            [                       ..s]
            [(..a), ..f, j..k, ..p, ..s]
        ]
        .build(storage_b.clone())
        .await
        .unwrap();

        let host_b = spec_b.tree().clone();
        let diff = host_b.differentiate(spec_a.tree());
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_different_heights() -> Result<()> {
        let backend = MemoryStorageBackend::default();

        let storage_a = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        let storage_b = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Scenario: Trees of different heights
        // Tree A is shallow (height 1), Tree B is taller (height 2)
        // This tests how differential handles height mismatches
        let spec_a = tree_spec![
            [       ..e]
            [(..a), ..e]
        ]
        .build(storage_a.clone())
        .await
        .unwrap();

        let spec_b = tree_spec![
            [                                ..z]
            [            ..f,        ..p,    ..z]
            [(..a), ..c, ..f, ..k, ..p, ..t, ..z]
        ]
        .build(storage_b.clone())
        .await
        .unwrap();

        let host_a = spec_a.tree().clone();
        let diff = host_a.differentiate(spec_b.tree());
        let _: Vec<_> = diff.collect().await;

        spec_a.assert();
        spec_b.assert();

        Ok(())
    }

    #[dialog_common::test]
    async fn test_diff_different_heights_reverse() -> Result<()> {
        let backend = MemoryStorageBackend::default();

        let storage_a = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        let storage_b = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Scenario: Reverse of test_diff_different_heights
        // Tree B is tall (height 2), Tree A is shallow (height 1)
        // When B.differentiate(A), we still need to read all branches to discover removes
        let spec_a = tree_spec![
            [                                ..z]
            [            ..f,      ..p,      ..z]
            [(..a), ..c, ..f, ..k, ..p, ..t, ..z]
        ]
        .build(storage_b.clone())
        .await
        .unwrap();

        let spec_b = tree_spec![
            [       ..e]
            [(..a), ..e]
        ]
        .build(storage_a.clone())
        .await
        .unwrap();

        // Differentiate B -> A (taller tree to shallow tree)
        // Still need to read all branches to discover remove changes
        let host_a = spec_a.tree().clone();
        let diff = host_a.differentiate(spec_b.tree());
        let _: Vec<_> = diff.collect().await;

        spec_b.assert();
        spec_a.assert();

        Ok(())
    }

    // Novel Nodes Tests
    //
    // These tests verify that novel_nodes() returns exactly the set of nodes
    // that exist in target but not in source. We use tree_spec! to define
    // deterministic tree structures and verify:
    // 1. Read patterns match expectations (via spec.assert())
    // 2. Novel nodes = target nodes - shared nodes with source

    #[dialog_common::test]
    async fn it_returns_all_target_nodes_when_source_is_empty() -> Result<()> {
        // When source is empty, all target nodes are novel
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Empty source tree
        let source = tree_spec![].build(storage_source).await.unwrap();

        // Target has some structure - all nodes loaded since source is empty
        let target = tree_spec![
            [     ..e]
            [..a, ..e]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify that all target nodes were loaded (since source is empty)
        target.assert();

        // All target nodes should be novel - traverse target tree to get all hashes
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        assert_eq!(
            novel_hashes, target_hashes,
            "All target nodes should be novel when source is empty"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_no_nodes_when_target_is_empty() -> Result<()> {
        // When target is empty, no nodes are novel
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Source has structure - root is loaded, children skipped since target is empty
        let source = tree_spec![
            [       ..e]
            [(..a), (..e)]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Empty target tree
        let target = tree_spec![].build(storage_target).await.unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify that no source nodes were loaded (since target is empty)
        source.assert();

        assert!(
            novel_hashes.is_empty(),
            "No nodes should be novel when target is empty"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_no_nodes_when_both_trees_are_empty() -> Result<()> {
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        let source = tree_spec![].build(storage_source).await.unwrap();
        let target = tree_spec![].build(storage_target).await.unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        assert!(
            novel_hashes.is_empty(),
            "No nodes should be novel when both trees are empty"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_no_nodes_for_identical_trees() -> Result<()> {
        // When trees are identical, no nodes are novel (all are shared)
        // and no child nodes should be loaded - identical roots are pruned immediately
        // Note: roots are always read during Tree::from_hash() in build(), so we mark them as ..e
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Both trees have identical structure
        // Root (..e at height 1) is read during build, but children should NOT be loaded
        let source = tree_spec![
            [              ..e]
            [(..a), (..c), (..e)]
        ]
        .build(storage_source)
        .await
        .unwrap();

        let target = tree_spec![
            [              ..e]
            [(..a), (..c), (..e)]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify no child nodes were loaded (identical trees detected at root)
        source.assert();
        target.assert();

        assert!(
            novel_hashes.is_empty(),
            "Identical trees should have no novel nodes"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_excludes_shared_subtrees_from_novel_nodes() -> Result<()> {
        // When trees share a subtree (same hash), that subtree is NOT novel
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Source: segments a, e - root loaded, both segments skipped (shared or no match)
        let source = tree_spec![
            [        ..e]
            [(..a), (..e)]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target: root loaded, segment 'a' is shared (skipped), 'f' and 'k' are loaded
        let target = tree_spec![
            [            ..k]
            [(..a), ..f, ..k]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify read patterns:
        // - Source: only index loaded (segments compared by boundary only)
        // - Target: shared 'a' skipped, novel 'f','k' loaded
        source.assert();
        target.assert();

        // Novel nodes should be: target nodes - nodes shared with source
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        let source_hashes = source
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        let expected_novel: std::collections::HashSet<_> =
            target_hashes.difference(&source_hashes).copied().collect();

        assert_eq!(
            novel_hashes, expected_novel,
            "Novel nodes should be target nodes minus shared nodes"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_trees_with_different_heights() -> Result<()> {
        // Target taller than source
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Shallow source (height 1) - root loaded, segments skipped
        let source = tree_spec![
            [         ..e]
            [(..a), (..e)]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Taller target (height 2) with shared segment 'a'
        // Root and intermediate nodes loaded, segment 'a' shared (skipped)
        let target = tree_spec![
            [                           ..z]
            [       ..f,      ..p,      ..z]
            [(..a), ..f, ..k, ..p, ..t, ..z]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify read patterns - shared segment 'a' should NOT be loaded
        source.assert();
        target.assert();

        // Novel = target - shared
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        let source_hashes = source
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        let expected_novel: std::collections::HashSet<_> =
            target_hashes.difference(&source_hashes).copied().collect();

        assert_eq!(
            novel_hashes, expected_novel,
            "Novel nodes should be target nodes minus shared nodes"
        );

        // Verify uniqueness
        assert_eq!(
            novel_hashes.len(),
            diff.novel_nodes().into_hash_set().await.len(),
            "All novel nodes should be unique"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_no_nodes_when_target_is_subset_of_source() -> Result<()> {
        // Target is a subset of source - should have no novel nodes since
        // all target nodes exist in source.
        //
        // IMPORTANT: Both trees must share the same backend so that identical
        // content produces identical hashes (content-addressed storage).
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Source: 3 segments at height 0, 1 index at height 1
        // Segment 'a' contains entries [a..a]
        let source = tree_spec![
            [            ..k]
            [(..a), ..f, ..k]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target: single segment 'a' with same entries as source's segment 'a'
        // Because they share the same backend, identical content = identical hash
        let target = tree_spec![[(..a)]].build(storage_target).await.unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Target's 'a' segment should match source's 'a' segment (same hash)
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        let source_hashes = source
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        let expected_novel: std::collections::HashSet<_> =
            target_hashes.difference(&source_hashes).copied().collect();

        assert_eq!(
            novel_hashes, expected_novel,
            "Novel nodes should be target - source (empty if target is subset)"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_all_target_nodes_for_disjoint_trees() -> Result<()> {
        // Trees with completely different content - all target nodes are novel
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Source: root loaded, segments skipped (disjoint so no match possible)
        let source = tree_spec![
            [        ..e]
            [(..a), (..e)]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target: root loaded, children loaded for novel_nodes
        let target = tree_spec![
            [      ..z]
            [..p,  ..z]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify read patterns:
        // - Source: only index loaded (segments compared by boundary, no match)
        // - Target: all nodes loaded (all are novel)
        source.assert();
        target.assert();

        // All target nodes should be novel (no overlap)
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        assert_eq!(
            novel_hashes, target_hashes,
            "Disjoint trees should have all target nodes as novel"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_unique_novel_nodes() -> Result<()> {
        // Verify that novel_nodes() never returns duplicates
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Source with some structure
        let source = tree_spec![
            [       ..e]
            [(..a), ..e]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target with more structure
        let target = tree_spec![
            [                           ..z]
            [       ..f,      ..p,      ..z]
            [(..a), ..f, ..k, ..p, ..t, ..z]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();

        // Collect twice - as Vec (preserves duplicates) and HashSet (deduplicates)
        let all_nodes: Vec<_> = diff.novel_nodes().try_collect().await?;
        let unique_hashes = diff.novel_nodes().into_hash_set().await;

        assert_eq!(
            all_nodes.len(),
            unique_hashes.len(),
            "novel_nodes() should not return duplicates"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_novel_nodes_for_different_segments() -> Result<()> {
        // Simplest case: single segment trees with different content
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Source: single segment 'a' (contains key 'a')
        // Marked as read because segment nodes are always loaded during diff
        let source = tree_spec![[..a]].build(storage_source).await.unwrap();

        // Target: single segment 'b' (contains keys 'a', 'b' - different content)
        let target = tree_spec![[..b]].build(storage_target).await.unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // All target nodes should be novel (no overlap)
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;

        assert_eq!(
            novel_hashes, target_hashes,
            "Different segment should produce novel nodes"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_prunes_shared_deep_subtrees() -> Result<()> {
        // 3-level trees where the left subtree is shared but right differs
        let backend = MemoryStorageBackend::default();
        let storage_source = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };
        let storage_target = Storage {
            encoder: CborEncoder,
            backend: JournaledStorage::new(backend.clone()),
        };

        // Source: 3-level tree with segments under two index nodes
        // Left subtree (..f, ..m) should be shared, so pruned
        // Right subtree (..t, ..z) differs - but source segments don't need loading
        // for novel_nodes (we only care about target nodes)
        let source = tree_spec![
            [                     ..z]
            [       (..m),        ..z]
            [(..f), (..m), (..t), ..z]
        ]
        .build(storage_source)
        .await
        .unwrap();

        // Target: same left subtree structure, different right subtree
        // Left (..f, ..m) should be pruned (identical to source)
        // Right (..w, ..z) is novel
        let target = tree_spec![
            [                   ..z]
            [       (..m),      ..z]
            [(..f), (..m), ..w, ..z]
        ]
        .build(storage_target)
        .await
        .unwrap();

        let diff = TreeDifference::compute(source.tree(), target.tree())
            .await
            .unwrap();
        let novel_hashes = diff.novel_nodes().into_hash_set().await;

        // Verify left subtree was pruned (not loaded)
        source.assert();
        target.assert();

        // Novel nodes should be target - source (right subtree differs)
        let target_hashes = target
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        let source_hashes = source
            .tree()
            .traverse(TraversalOrder::default())
            .into_hash_set()
            .await;
        let expected_novel: std::collections::HashSet<_> =
            target_hashes.difference(&source_hashes).copied().collect();

        assert_eq!(
            novel_hashes, expected_novel,
            "Novel nodes should be target nodes minus shared nodes"
        );

        Ok(())
    }
}
