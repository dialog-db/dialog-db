use crate::{DialogProllyTreeError, Entry, KeyType, Node, Tree, ValueType};
use async_stream::try_stream;
use dialog_storage::{ContentAddressedStorage, HashType};
use futures_core::Stream;

/// Represents a change in the key-value store.
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

/// A sparse view of a tree containing only the nodes that differ between two trees.
#[derive(Debug, Clone)]
pub struct SparseTree<'a, const F: u32, const H: usize, S: Settings<F, H>> {
    storage: &'a S::Storage,
    nodes: Vec<Node<F, H, S::Key, S::Value, S::Hash>>,
}

impl<'a, const F: u32, const H: usize, S: Settings<F, H>> SparseTree<'a, F, H, S> {
    /// Creates a sparse view of the given tree.
    pub fn from(
        tree: &'a Tree<F, H, S::Distribution, S::Key, S::Value, S::Hash, S::Storage>,
    ) -> Self {
        let storage = tree.storage();
        let nodes = tree.root().map(|root| vec![root.clone()]).unwrap_or(vec![]);

        SparseTree { storage, nodes }
    }

    /// Returns the upper bound of the key range covered by this tree.
    pub fn upper_bound(&self) -> Option<&S::Key> {
        self.nodes.last().map(|node| node.reference().upper_bound())
    }

    /// Expands this node range into nodes of their children, result is still a
    /// same range except level down. Returns true if we were able to expand
    /// or false if we reached the segment nodes and there is nothing more to
    /// expand.
    pub async fn expand(&mut self) -> Result<bool, DialogProllyTreeError> {
        let mut expanded = false;
        let mut offset = 0;

        while offset < self.nodes.len() {
            if self.nodes[offset].is_branch() {
                let children = self.nodes[offset].load_children(self.storage).await?;
                let children_vec: Vec<_> = Vec::from(children);
                let num_children = children_vec.len();

                // Replace the branch node with its children
                self.nodes.splice(offset..offset + 1, children_vec);

                // Skip past the newly inserted children
                offset += num_children;
                expanded = true;
            } else {
                offset += 1;
            }
        }

        Ok(expanded)
    }

    /// Removes nodes that are **shared** between this range (`self`) and another (`other`),
    /// keeping only nodes that differ between the two.
    ///
    /// # Behavior
    ///
    /// Both `self` and `other` are assumed to contain sorted sequences of [`Node`]s ordered by
    /// their [`upper_bound`](Node::upper_bound) key. Two nodes are considered *shared* if:
    ///
    /// - Their `upper_bound` keys are equal **and**
    /// - Their content hashes (`hash()`) are equal.
    ///
    /// When a shared node is detected, it is removed from **both** ranges.
    ///
    /// After this operation:
    ///
    /// - Each range remains sorted by `upper_bound`.
    /// - The relative order of all remaining nodes is preserved (stable compaction).
    /// - No heap allocations or clones are performed—everything happens fully in-place.
    ///
    /// # Algorithm
    ///
    /// This uses a **two-pointer merge-like traversal** over the sorted node lists.
    /// - `at_left` and `at_right` are *read heads* advancing through the current elements.
    /// - `to_left` and `to_right` are *write heads* marking the next position to keep.
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
    /// ```
    /// // (Pseudocode – assumes appropriate Node types exist)
    /// let mut before = Range::from(vec![A, B, C]);
    /// let mut after  = Range::from(vec![A, C, D]);
    ///
    /// before.discard_shared(&mut after);
    ///
    /// // Result:
    /// // before = [B]
    /// // after  = [D]
    /// ```
    pub fn prune(&mut self, other: &mut Self) {
        use std::cmp::Ordering;
        let left = &mut self.nodes;
        let right = &mut other.nodes;

        // Read indices
        let mut at_left = 0;
        let mut at_right = 0;

        // Write indices — next position to place a node we’re keeping
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
    ) -> impl Stream<Item = Result<Entry<S::Key, S::Value>, DialogProllyTreeError>> + '_ {
        try_stream! {
            for node in &self.nodes {
                let range = node.get_range(.., self.storage);
                for await entry in range {
                    yield entry?;
                }
            }
        }
    }
}

/// Trait encapsulating all the settings so we can carry those as group
/// as opposed to bunch of different parameters.
pub trait Settings<const BRANCH_FACTOR: u32, const HASH_SIZE: usize> {
    const BRANCH_FACTOR: u32;
    const HASH_SIZE: usize;
    type Distribution: crate::Distribution<BRANCH_FACTOR, HASH_SIZE, Self::Key, Self::Hash>;
    type Key: KeyType + 'static;
    type Value: ValueType + PartialEq;
    type Hash: HashType<HASH_SIZE>;
    type Storage: ContentAddressedStorage<HASH_SIZE, Hash = Self::Hash>;
}

/// Represents a difference between two trees.
pub struct Delta<'a, const F: u32, const H: usize, S: Settings<F, H>>(
    SparseTree<'a, F, H, S>,
    SparseTree<'a, F, H, S>,
);

impl<'a, const F: u32, const H: usize, S: Settings<F, H>> Delta<'a, F, H, S> {
    /// Creates a difference between two trees.
    pub fn from(
        pair: (
            &'a Tree<F, H, S::Distribution, S::Key, S::Value, S::Hash, S::Storage>,
            &'a Tree<F, H, S::Distribution, S::Key, S::Value, S::Hash, S::Storage>,
        ),
    ) -> Self {
        Delta(SparseTree::from(pair.0), SparseTree::from(pair.1))
    }

    pub async fn expand(&mut self) -> Result<(), DialogProllyTreeError> {
        let Delta(left, right) = self;

        loop {
            // first we prune to eliminate shared ranges
            left.prune(right);
            // we keep expanding and pruning until neither range can be expanded
            // any further (and we're left with segments)
            if !left.expand().await? && !right.expand().await? {
                break;
            };
        }

        Ok(())
    }

    pub fn stream(&'a self) -> impl Differential<S::Key, S::Value> + 'a
    where
        S::Value: PartialEq,
    {
        let Delta(left, right) = self;
        let left_entries = left.stream();
        let right_entries = right.stream();

        try_stream! {
            futures_util::pin_mut!(left_entries);
            futures_util::pin_mut!(right_entries);

            use futures_util::StreamExt;

            let mut left_next = left_entries.next().await;
            let mut right_next = right_entries.next().await;

            loop {
                match (&left_next, &right_next) {
                    (None, None) => break,
                    (Some(Ok(left_entry)), None) => {
                        // All remaining entries from left are removals
                        yield Change::Remove(left_entry.clone());
                        left_next = left_entries.next().await;
                    }
                    (None, Some(Ok(right_entry))) => {
                        // All remaining entries from right are additions
                        yield Change::Add(right_entry.clone());
                        right_next = right_entries.next().await;
                    }
                    (Some(Err(_)), _) | (_, Some(Err(_))) => {
                        // Error in one of the streams - stop processing
                        break;
                    }
                    (Some(Ok(left_entry)), Some(Ok(right_entry))) => {
                        use std::cmp::Ordering;
                        match left_entry.key.cmp(&right_entry.key) {
                            Ordering::Less => {
                                // Left key is smaller - it was removed
                                yield Change::Remove(left_entry.clone());
                                left_next = left_entries.next().await;
                            }
                            Ordering::Greater => {
                                // Right key is smaller - it was added
                                yield Change::Add(right_entry.clone());
                                right_next = right_entries.next().await;
                            }
                            Ordering::Equal => {
                                // Same key - check values
                                if left_entry.value != right_entry.value {
                                    // Value changed
                                    yield Change::Remove(left_entry.clone());
                                    yield Change::Add(right_entry.clone());
                                }
                                // If values are equal, skip (no change)
                                left_next = left_entries.next().await;
                                right_next = right_entries.next().await;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Compares two entry lists and yields changes using a two-cursor walk.
fn diff_entries<Key, Value>(
    before_entries: Vec<Entry<Key, Value>>,
    after_entries: Vec<Entry<Key, Value>>,
) -> Vec<Change<Key, Value>>
where
    Key: KeyType,
    Value: ValueType + PartialEq,
{
    let mut changes = Vec::new();
    let mut before_cursor = 0;
    let mut after_cursor = 0;

    while before_cursor < before_entries.len() && after_cursor < after_entries.len() {
        let before_entry = &before_entries[before_cursor];
        let after_entry = &after_entries[after_cursor];

        match before_entry.key.cmp(&after_entry.key) {
            std::cmp::Ordering::Equal => {
                // Same key - check if values differ
                if before_entry.value != after_entry.value {
                    // Value changed - emit remove + add
                    changes.push(Change::Remove(before_entry.clone()));
                    changes.push(Change::Add(after_entry.clone()));
                }
                // If values are the same, skip (no change)
                before_cursor += 1;
                after_cursor += 1;
            }
            std::cmp::Ordering::Less => {
                // before_key < after_key: entry was removed
                changes.push(Change::Remove(before_entry.clone()));
                before_cursor += 1;
            }
            std::cmp::Ordering::Greater => {
                // before_key > after_key: entry was added
                changes.push(Change::Add(after_entry.clone()));
                after_cursor += 1;
            }
        }
    }

    // Handle remaining entries
    while before_cursor < before_entries.len() {
        changes.push(Change::Remove(before_entries[before_cursor].clone()));
        before_cursor += 1;
    }

    while after_cursor < after_entries.len() {
        changes.push(Change::Add(after_entries[after_cursor].clone()));
        after_cursor += 1;
    }

    changes
}

/// Helper function to collect all entries from a node and its children.
fn collect_all_entries<
    'a,
    const BRANCH_FACTOR: u32,
    const HASH_SIZE: usize,
    Key,
    Value,
    Hash,
    Storage,
>(
    node: Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>,
    storage: &'a Storage,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<Vec<Entry<Key, Value>>, DialogProllyTreeError>>
            + 'a,
    >,
>
where
    Key: KeyType,
    Value: ValueType + PartialEq + 'a,
    Hash: HashType<HASH_SIZE> + 'a,
    Storage: ContentAddressedStorage<HASH_SIZE, Hash = Hash>,
{
    Box::pin(async move {
        if node.is_segment() {
            Ok(node.into_entries()?.into())
        } else {
            // It's a branch - recursively collect from children
            let children = node.load_children(storage).await?;
            let mut all_entries = Vec::new();
            for child in Vec::from(children) {
                let entries =
                    collect_all_entries::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(child, storage)
                        .await?;
                all_entries.extend(entries);
            }
            Ok(all_entries)
        }
    })
}

/// Computes the difference between two nodes.
/// Returns a stream that yields changes.
pub fn differentiate<
    'a,
    const BRANCH_FACTOR: u32,
    const HASH_SIZE: usize,
    Key,
    Value,
    Hash,
    Storage,
>(
    before: Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>,
    after: Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>,
    storage: &'a Storage,
) -> impl Differential<Key, Value> + 'a
where
    Key: KeyType + 'static,
    Value: ValueType + PartialEq + 'a,
    Hash: HashType<HASH_SIZE> + PartialEq + 'a,
    Storage: ContentAddressedStorage<HASH_SIZE, Hash = Hash>,
{
    try_stream! {
        // Early exit if identical
        if before.hash() == after.hash() {
            return;
        }

        // Create sparse trees (initially just the root nodes)
        let mut before_nodes = vec![before];
        let mut after_nodes = vec![after];

        // Repeatedly prune and expand until we reach segments
        loop {
            // Prune shared nodes using two-cursor walk
            prune_nodes(&mut before_nodes, &mut after_nodes);

            // Try to expand both sides
            let before_expanded = expand_nodes::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(
                &mut before_nodes,
                storage
            ).await?;

            let after_expanded = expand_nodes::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(
                &mut after_nodes,
                storage
            ).await?;

            // If neither side expanded, we're done expanding
            if !before_expanded && !after_expanded {
                break;
            }
        }

        // Final prune after reaching segments
        prune_nodes(&mut before_nodes, &mut after_nodes);

        // Stream entries from both sparse trees and diff them
        let before_stream = stream_nodes(&before_nodes, storage);
        let after_stream = stream_nodes(&after_nodes, storage);

        futures_util::pin_mut!(before_stream);
        futures_util::pin_mut!(after_stream);

        use futures_util::StreamExt;

        let mut before_next = before_stream.next().await;
        let mut after_next = after_stream.next().await;

        loop {
            match (&before_next, &after_next) {
                (None, None) => break,
                (Some(Ok(before_entry)), None) => {
                    // Remaining before entries are removals
                    yield Change::Remove(before_entry.clone());
                    before_next = before_stream.next().await;
                }
                (None, Some(Ok(after_entry))) => {
                    // Remaining after entries are additions
                    yield Change::Add(after_entry.clone());
                    after_next = after_stream.next().await;
                }
                (Some(Err(_)), _) | (_, Some(Err(_))) => {
                    // Error in one of the streams - stop processing
                    break;
                }
                (Some(Ok(before_entry)), Some(Ok(after_entry))) => {
                    use std::cmp::Ordering;
                    match before_entry.key.cmp(&after_entry.key) {
                        Ordering::Less => {
                            // Before key is smaller - it was removed
                            yield Change::Remove(before_entry.clone());
                            before_next = before_stream.next().await;
                        }
                        Ordering::Greater => {
                            // After key is smaller - it was added
                            yield Change::Add(after_entry.clone());
                            after_next = after_stream.next().await;
                        }
                        Ordering::Equal => {
                            // Same key - check values
                            if before_entry.value != after_entry.value {
                                // Value changed
                                yield Change::Remove(before_entry.clone());
                                yield Change::Add(after_entry.clone());
                            }
                            // If values are equal, skip (no change)
                            before_next = before_stream.next().await;
                            after_next = after_stream.next().await;
                        }
                    }
                }
            }
        }
    }
}

/// Prune shared nodes from two node vectors using two-cursor walk.
fn prune_nodes<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>(
    left: &mut Vec<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>>,
    right: &mut Vec<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>>,
) where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    use std::cmp::Ordering;

    let mut at_left = 0;
    let mut at_right = 0;
    let mut to_left = 0;
    let mut to_right = 0;

    while at_left < left.len() && at_right < right.len() {
        let left_node = &left[at_left];
        let right_node = &right[at_right];

        match left_node.upper_bound().cmp(right_node.upper_bound()) {
            Ordering::Less => {
                // Left node is unique - keep it
                if to_left != at_left {
                    left.swap(to_left, at_left);
                }
                to_left += 1;
                at_left += 1;
            }
            Ordering::Greater => {
                // Right node is unique - keep it
                if to_right != at_right {
                    right.swap(to_right, at_right);
                }
                to_right += 1;
                at_right += 1;
            }
            Ordering::Equal => {
                // Same key range - check if shared
                if left_node.hash() != right_node.hash() {
                    // Different content - keep both
                    if to_left != at_left {
                        left.swap(to_left, at_left);
                    }
                    if to_right != at_right {
                        right.swap(to_right, at_right);
                    }
                    to_left += 1;
                    to_right += 1;
                }
                // else identical (shared) - skip both
                at_left += 1;
                at_right += 1;
            }
        }
    }

    // Move remaining nodes from left
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

    // Move remaining nodes from right
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

    left.truncate(to_left);
    right.truncate(to_right);
}

/// Expand all branch nodes in the vector by replacing them with their children.
/// Returns true if any expansion occurred.
async fn expand_nodes<
    'a,
    const BRANCH_FACTOR: u32,
    const HASH_SIZE: usize,
    Key,
    Value,
    Hash,
    Storage,
>(
    nodes: &mut Vec<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>>,
    storage: &'a Storage,
) -> Result<bool, DialogProllyTreeError>
where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
    Storage: ContentAddressedStorage<HASH_SIZE, Hash = Hash>,
{
    let mut expanded = false;
    let mut offset = 0;

    while offset < nodes.len() {
        if nodes[offset].is_branch() {
            let children = nodes[offset].load_children(storage).await?;
            let children_vec: Vec<_> = Vec::from(children);
            let num_children = children_vec.len();

            // Replace the branch node with its children
            nodes.splice(offset..offset + 1, children_vec);

            // Skip past the newly inserted children
            offset += num_children;
            expanded = true;
        } else {
            offset += 1;
        }
    }

    Ok(expanded)
}

/// Stream all entries from a vector of nodes.
fn stream_nodes<'a, const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash, Storage>(
    nodes: &'a [Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>],
    storage: &'a Storage,
) -> impl Stream<Item = Result<Entry<Key, Value>, DialogProllyTreeError>> + 'a
where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
    Storage: ContentAddressedStorage<HASH_SIZE, Hash = Hash>,
{
    try_stream! {
        for node in nodes {
            let range = node.get_range(.., storage);
            for await entry in range {
                yield entry?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GeometricDistribution, Tree};
    use dialog_storage::{Blake3Hash, CborEncoder, MemoryStorageBackend, Storage};
    use futures_util::{StreamExt, pin_mut};

    type TestTree = Tree<
        32,
        32,
        GeometricDistribution,
        Vec<u8>,
        Vec<u8>,
        Blake3Hash,
        Storage<32, CborEncoder, MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    >;

    #[tokio::test]
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

        let changes = tree1.differentiate(tree2);
        pin_mut!(changes);
        let mut count = 0;
        while let Some(_) = changes.next().await {
            count += 1;
        }

        assert_eq!(count, 0, "Identical trees should have no changes");
    }

    #[tokio::test]
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

        let changes = tree1.differentiate(tree2);
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

    #[tokio::test]
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

        let changes = tree1.differentiate(tree2);
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

    #[tokio::test]
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

        let changes = tree1.differentiate(tree2);
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

    #[tokio::test]
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

        let changes = tree1.differentiate(tree2);
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

    #[tokio::test]
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

        let changes = tree1.differentiate(tree2);
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

    #[tokio::test]
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

        let changes = tree1.differentiate(tree2);
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

    #[tokio::test]
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

        tree.integrate(changes).await.unwrap();

        assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(vec![10]));
        assert_eq!(tree.get(&vec![2]).await.unwrap(), Some(vec![20]));
    }

    #[tokio::test]
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

        tree.integrate(changes).await.unwrap();

        assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(vec![10]));
    }

    #[tokio::test]
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

        tree.integrate(changes).await.unwrap();

        // Check which value won based on hash
        use crate::ValueType;
        let existing_hash = existing_value.hash();
        let new_hash = new_value.hash();

        if new_hash > existing_hash {
            assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(new_value));
        } else {
            assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(existing_value));
        }
    }

    #[tokio::test]
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

        tree.integrate(changes).await.unwrap();

        assert_eq!(tree.get(&vec![1]).await.unwrap(), None);
        assert_eq!(tree.get(&vec![2]).await.unwrap(), Some(vec![20]));
    }

    #[tokio::test]
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

        tree.integrate(changes).await.unwrap();

        assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(vec![10]));
        assert_eq!(tree.get(&vec![2]).await.unwrap(), None);
    }

    #[tokio::test]
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

        tree.integrate(changes).await.unwrap();

        // Entry should still exist with original value
        assert_eq!(tree.get(&vec![1]).await.unwrap(), Some(vec![10]));
    }

    #[tokio::test]
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
        let changes_a = {
            let diff_a = tree_a.differentiate(TestTree::new(Storage {
                encoder: CborEncoder,
                backend: backend.clone(),
            }));
            pin_mut!(diff_a);
            let mut changes = Vec::new();
            while let Some(result) = diff_a.next().await {
                changes.push(result.unwrap());
            }
            changes
        };

        let changes_b = {
            let diff_b = tree_b.differentiate(TestTree::new(Storage {
                encoder: CborEncoder,
                backend: backend.clone(),
            }));
            pin_mut!(diff_b);
            let mut changes = Vec::new();
            while let Some(result) = diff_b.next().await {
                changes.push(result.unwrap());
            }
            changes
        };

        // Integrate changes
        tree_a.integrate(changes_b).await.unwrap();
        tree_b.integrate(changes_a).await.unwrap();

        // Both should converge to the same value (deterministic by hash)
        let final_a = tree_a.get(&vec![1]).await.unwrap();
        let final_b = tree_b.get(&vec![1]).await.unwrap();

        assert_eq!(final_a, final_b, "Trees should converge to same value");

        // Verify the winner is determined by hash
        use crate::ValueType;
        let hash_20 = vec![20].hash();
        let hash_30 = vec![30].hash();

        if hash_20 > hash_30 {
            assert_eq!(final_a, Some(vec![20]));
        } else {
            assert_eq!(final_a, Some(vec![30]));
        }
    }
}
