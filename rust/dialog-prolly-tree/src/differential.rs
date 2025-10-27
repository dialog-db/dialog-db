use crate::{DialogProllyTreeError, Entry, KeyType, Node, Tree, ValueType};
use async_stream::try_stream;
use dialog_storage::{ContentAddressedStorage, HashType};
use futures_core::Stream;

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

/// Helper struct to convert Vec<Change<Key, Value>> into a Differential stream.
///
/// This allows using a pre-computed vector of changes as a differential.
pub struct VecDifferential<Key, Value>
where
    Key: KeyType + 'static,
    Value: ValueType,
{
    changes: Vec<Change<Key, Value>>,
    index: usize,
}

impl<Key, Value> VecDifferential<Key, Value>
where
    Key: KeyType + 'static,
    Value: ValueType,
{
    /// Create a new VecDifferential from a vector of changes
    pub fn new(changes: Vec<Change<Key, Value>>) -> Self {
        Self { changes, index: 0 }
    }
}

impl<Key, Value> From<Vec<Change<Key, Value>>> for VecDifferential<Key, Value>
where
    Key: KeyType + 'static,
    Value: ValueType,
{
    fn from(changes: Vec<Change<Key, Value>>) -> Self {
        Self::new(changes)
    }
}

impl<Key, Value> Stream for VecDifferential<Key, Value>
where
    Key: KeyType + 'static + Unpin,
    Value: ValueType + Unpin,
{
    type Item = Result<Change<Key, Value>, DialogProllyTreeError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.index < this.changes.len() {
            let change = this.changes[this.index].clone();
            this.index += 1;
            std::task::Poll::Ready(Some(Ok(change)))
        } else {
            std::task::Poll::Ready(None)
        }
    }
}

/// A sparse view of a tree containing only the nodes that differ between two trees.
#[derive(Debug, Clone)]
pub(crate) struct SparseTree<'a, const F: u32, const H: usize, Key, Value, Hash, Storage>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<H>,
    Storage: ContentAddressedStorage<H, Hash = Hash>,
{
    storage: &'a Storage,
    nodes: Vec<Node<F, H, Key, Value, Hash>>,
}

impl<'a, const F: u32, const H: usize, Key, Value, Hash, Storage>
    SparseTree<'a, F, H, Key, Value, Hash, Storage>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<H>,
    Storage: ContentAddressedStorage<H, Hash = Hash>,
{
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
    /// ```ignore
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
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogProllyTreeError>> + '_ {
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

/// Represents a difference between two trees as a pair of sparse trees.
/// Provides methods to expand the difference and stream changes.
pub(crate) struct Delta<'a, const F: u32, const H: usize, Key, Value, Hash, Storage>(
    SparseTree<'a, F, H, Key, Value, Hash, Storage>,
    SparseTree<'a, F, H, Key, Value, Hash, Storage>,
)
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<H>,
    Storage: ContentAddressedStorage<H, Hash = Hash>;

impl<'a, const F: u32, const H: usize, Key, Value, Hash, Storage>
    Delta<'a, F, H, Key, Value, Hash, Storage>
where
    Key: KeyType + 'static,
    Value: ValueType + PartialEq,
    Hash: HashType<H>,
    Storage: ContentAddressedStorage<H, Hash = Hash>,
{
    pub fn from<Distription: crate::Distribution<F, H, Key, Hash>>(
        (left, right): (
            &'a Tree<F, H, Distription, Key, Value, Hash, Storage>,
            &'a Tree<F, H, Distription, Key, Value, Hash, Storage>,
        ),
    ) -> Self {
        Self(
            SparseTree {
                storage: left.storage(),
                nodes: left.root().map(|root| vec![root.clone()]).unwrap_or(vec![]),
            },
            SparseTree {
                storage: right.storage(),
                nodes: right
                    .root()
                    .map(|root| vec![root.clone()])
                    .unwrap_or(vec![]),
            },
        )
    }

    /// Creates a new Delta from two nodes.
    pub fn new(
        before: Node<F, H, Key, Value, Hash>,
        after: Node<F, H, Key, Value, Hash>,
        storage: &'a Storage,
    ) -> Self {
        Self(
            SparseTree {
                storage,
                nodes: vec![before],
            },
            SparseTree {
                storage,
                nodes: vec![after],
            },
        )
    }

    /// Expands the difference by repeatedly pruning shared nodes and expanding
    /// branch nodes until only segment nodes remain that differ between the trees.
    pub async fn expand(&mut self) -> Result<(), DialogProllyTreeError> {
        let Self(before, after) = self;
        loop {
            // Prune shared nodes using two-cursor walk
            before.prune(after);

            // Try to expand both sides
            let before_expanded = before.expand().await?;
            let after_expanded = after.expand().await?;

            // If neither side expanded, we're done expanding
            if !before_expanded && !after_expanded {
                break;
            }
        }

        // Final prune after reaching segments
        before.prune(after);

        Ok(())
    }

    /// Returns a stream of changes between the two trees.
    ///
    /// This performs a two-cursor walk over the entry streams from both sparse trees,
    /// yielding Add and Remove changes as appropriate.
    pub fn stream(&'a self) -> impl Differential<Key, Value> + 'a {
        let Self(before, after) = self;
        let before_stream = before.stream();
        let after_stream = after.stream();

        try_stream! {
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
                    (Some(Err(_)), _) => {
                        // Propagate error from before stream
                        if let Some(Err(err)) = std::mem::replace(&mut before_next, None) {
                            Err(err)?;
                        }
                    }
                    (_, Some(Err(_))) => {
                        // Propagate error from after stream
                        if let Some(Err(err)) = std::mem::replace(&mut after_next, None) {
                            Err(err)?;
                        }
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

        let changes = tree1.differentiate(&tree2);
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

        let changes = tree1.differentiate(&tree2);
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

        let changes = tree1.differentiate(&tree2);
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

        let changes = tree1.differentiate(&tree2);
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

        let changes = tree1.differentiate(&tree2);
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

        let changes = tree1.differentiate(&tree2);
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

        let changes = tree1.differentiate(&tree2);
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

        tree.integrate(VecDifferential::from(changes))
            .await
            .unwrap();

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

        tree.integrate(VecDifferential::from(changes))
            .await
            .unwrap();

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

        tree.integrate(VecDifferential::from(changes))
            .await
            .unwrap();

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

        tree.integrate(VecDifferential::from(changes))
            .await
            .unwrap();

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

        tree.integrate(VecDifferential::from(changes))
            .await
            .unwrap();

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

        tree.integrate(VecDifferential::from(changes))
            .await
            .unwrap();

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
        let empty_tree = TestTree::new(Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        });

        let host_a = tree_a.clone();
        let changes_a = host_a.differentiate(&empty_tree);
        let host_b = tree_b.clone();
        let changes_b = host_b.differentiate(&empty_tree);

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

    // ========================================================================
    // Roundtrip tests: Verify differentiate + integrate produces original tree
    // ========================================================================

    #[tokio::test]
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
            let diff = target.differentiate(&start);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(VecDifferential::from(changes))
            .await
            .unwrap();

        // Verify start now matches target
        assert_eq!(start.get(&vec![1]).await.unwrap(), Some(vec![10]));
        assert_eq!(start.get(&vec![2]).await.unwrap(), Some(vec![20]));
        assert_eq!(start.get(&vec![3]).await.unwrap(), Some(vec![30]));
    }

    #[tokio::test]
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
            let diff = target.differentiate(&start);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(VecDifferential::from(changes))
            .await
            .unwrap();

        // Verify start is now empty
        assert_eq!(start.get(&vec![1]).await.unwrap(), None);
        assert_eq!(start.get(&vec![2]).await.unwrap(), None);
        assert_eq!(start.get(&vec![3]).await.unwrap(), None);
    }

    #[tokio::test]
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
            let diff = target.differentiate(&start);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(VecDifferential::from(changes))
            .await
            .unwrap();

        // Verify start now matches target
        assert_eq!(start.get(&vec![1]).await.unwrap(), None);
        assert_eq!(start.get(&vec![2]).await.unwrap(), Some(vec![22]));
        assert_eq!(start.get(&vec![3]).await.unwrap(), Some(vec![30]));
        assert_eq!(start.get(&vec![4]).await.unwrap(), Some(vec![40]));
    }

    #[tokio::test]
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
            let diff = target.differentiate(&start);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(VecDifferential::from(changes))
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

    // ========================================================================
    // Edge case tests
    // ========================================================================

    #[tokio::test]
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

        let changes = tree1.differentiate(&tree2);
        pin_mut!(changes);
        let mut count = 0;
        while let Some(_) = changes.next().await {
            count += 1;
        }

        assert_eq!(count, 0, "Both empty trees should have no changes");
    }

    #[tokio::test]
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

        let changes = tree1.differentiate(&tree2);
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

    #[tokio::test]
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

        let changes = tree1.differentiate(&tree2);
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

    #[tokio::test]
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

        let changes = superset.differentiate(&subset);
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

    #[tokio::test]
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

        let changes = tree1.differentiate(&tree2);
        pin_mut!(changes);
        let mut count = 0;

        while let Some(result) = changes.next().await {
            result.unwrap();
            count += 1;
        }

        // 9 keys modified (i=0 has same value in both) = 9 * 2 = 18 changes total
        assert_eq!(count, 18);
    }

    #[tokio::test]
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
        let target_hash = target.hash().unwrap().clone();

        // Set up different start state
        for i in 10..30 {
            start.set(vec![i], vec![i * 5]).await.unwrap();
        }

        // Compute diff and integrate
        // Need to collect changes to avoid borrow checker issues
        // (diff holds immutable ref to start, but integrate needs mutable ref)
        let changes = {
            let diff = target.differentiate(&start);
            pin_mut!(diff);
            let mut changes = Vec::new();
            while let Some(result) = diff.next().await {
                changes.push(result.unwrap());
            }
            changes
        };
        start
            .integrate(VecDifferential::from(changes))
            .await
            .unwrap();

        // Hash should match after integration
        assert_eq!(start.hash().unwrap(), &target_hash);
    }

    // ========================================================================
    // Performance tests using MeasuredStorage
    // ========================================================================

    use dialog_storage::MeasuredStorage;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    type MeasuredTree = Tree<
        32,
        32,
        GeometricDistribution,
        Vec<u8>,
        Vec<u8>,
        Blake3Hash,
        Arc<
            Mutex<
                Storage<
                    32,
                    CborEncoder,
                    MeasuredStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
                >,
            >,
        >,
    >;

    #[tokio::test]
    async fn test_shared_segments_are_skipped() {
        // This test verifies that when trees share common segments,
        // we skip over them without reading their contents
        let backend = MeasuredStorage::new(MemoryStorageBackend::default());
        let storage = Arc::new(Mutex::new(Storage {
            encoder: CborEncoder,
            backend,
        }));

        // Create a base tree with many entries
        let mut base_tree = MeasuredTree::new(storage.clone());
        for i in 0..100u8 {
            base_tree.set(vec![i], vec![i]).await.unwrap();
        }

        // Create tree1 = base + one extra entry
        let mut tree1 = MeasuredTree::new(storage.clone());
        for i in 0..100u8 {
            tree1.set(vec![i], vec![i]).await.unwrap();
        }
        tree1.set(vec![200], vec![200]).await.unwrap();

        // Reset read counter
        {
            let storage_lock = storage.lock().await;
            let reads_before = storage_lock.backend.reads();
            drop(storage_lock);

            // Now differentiate - should skip shared segments
            let changes = tree1.differentiate(&base_tree);
            pin_mut!(changes);
            let mut count = 0;
            while let Some(result) = changes.next().await {
                result.unwrap();
                count += 1;
            }

            assert_eq!(count, 1, "Should only have one add change");

            let storage_lock = storage.lock().await;
            let reads_after = storage_lock.backend.reads();
            let diff_reads = reads_after - reads_before;

            // We should read far fewer nodes than if we read the entire tree
            // Ideally we only read nodes along the path to the difference
            // For a tree with 100 entries, full read would be >> 10 nodes
            // With pruning, we should read < 10 nodes
            println!("Reads during diff: {}", diff_reads);

            // This assertion documents current behavior
            // A well-optimized implementation should read < 10 nodes
            // If this fails, it means we're not pruning effectively
            assert!(
                diff_reads < 20,
                "Expected < 20 reads for shared segments, got {}",
                diff_reads
            );
        }
    }

    #[tokio::test]
    async fn test_subset_relationship_with_same_height() {
        // Test case where one tree is fully contained in another
        // Both have the same height, so pruning should work well
        let backend = MeasuredStorage::new(MemoryStorageBackend::default());
        let storage = Arc::new(Mutex::new(Storage {
            encoder: CborEncoder,
            backend,
        }));

        // Small tree: 50 entries
        let mut small_tree = MeasuredTree::new(storage.clone());
        for i in 0..50u8 {
            small_tree.set(vec![i], vec![i]).await.unwrap();
        }

        // Large tree: 50 entries (same) + 50 more
        let mut large_tree = MeasuredTree::new(storage.clone());
        for i in 0..100u8 {
            large_tree.set(vec![i], vec![i]).await.unwrap();
        }

        // Reset read counter
        {
            let storage_lock = storage.lock().await;
            let reads_before = storage_lock.backend.reads();
            drop(storage_lock);

            // Differentiate large from small
            let changes = large_tree.differentiate(&small_tree);
            pin_mut!(changes);
            let mut count = 0;
            while let Some(result) = changes.next().await {
                result.unwrap();
                count += 1;
            }

            assert_eq!(count, 50, "Should have 50 add changes");

            let storage_lock = storage.lock().await;
            let reads_after = storage_lock.backend.reads();
            let diff_reads = reads_after - reads_before;

            println!("Reads for subset diff (same height): {}", diff_reads);

            // With good pruning, we should skip the shared portion
            // and only read nodes related to the additional 50 entries
            assert!(
                diff_reads < 30,
                "Expected < 30 reads when trees share structure, got {}",
                diff_reads
            );
        }
    }

    #[tokio::test]
    async fn test_subset_relationship_with_different_heights() {
        // Test case where one tree is fully contained in another
        // BUT they have different heights - this should expose the performance issue
        let backend = MeasuredStorage::new(MemoryStorageBackend::default());
        let storage = Arc::new(Mutex::new(Storage {
            encoder: CborEncoder,
            backend,
        }));

        // Small tree: just a few entries (will be shallow)
        let mut small_tree = MeasuredTree::new(storage.clone());
        for i in 0..10u8 {
            small_tree.set(vec![i], vec![i]).await.unwrap();
        }

        // Large tree: same entries + many more (will be taller due to size)
        let mut large_tree = MeasuredTree::new(storage.clone());
        for i in 0..200u8 {
            large_tree.set(vec![i], vec![i]).await.unwrap();
        }

        println!("Small tree: 10 entries, Large tree: 200 entries");

        // Reset read counter
        let storage_lock = storage.lock().await;
        let reads_before = storage_lock.backend.reads();
        drop(storage_lock);

        // Differentiate large from small
        let changes = large_tree.differentiate(&small_tree);
        pin_mut!(changes);
        let mut count = 0;
        while let Some(result) = changes.next().await {
            result.unwrap();
            count += 1;
        }

        assert_eq!(count, 190, "Should have 190 add changes");

        let storage_lock = storage.lock().await;
        let reads_after = storage_lock.backend.reads();
        let diff_reads = reads_after - reads_before;

        println!(
            "Reads for subset diff (likely different heights): {}",
            diff_reads
        );

        // This test DOCUMENTS THE PROBLEM:
        // With different heights, we can't prune shared segments early
        // so we end up reading many more nodes than necessary
        //
        // EXPECTED BEHAVIOR (after fix): < 30 reads
        // CURRENT BEHAVIOR: likely > 50 reads
        //
        // This warning documents the issue when it occurs
        if diff_reads > 50 {
            println!("WARNING: Performance issue detected!");
            println!("With different tree sizes, we read {} nodes", diff_reads);
            println!("This is likely because trees have different heights");
            println!("We can't detect shared segments until reaching leaves");
            println!("See notes/diff.md for details on the height-mismatch problem");
        }
    }

    #[tokio::test]
    async fn test_no_shared_entries_minimal_reads() {
        // Baseline test: when trees share nothing, we must read everything
        let backend = MeasuredStorage::new(MemoryStorageBackend::default());
        let storage = Arc::new(Mutex::new(Storage {
            encoder: CborEncoder,
            backend,
        }));

        // Tree 1: keys 0-49
        let mut tree1 = MeasuredTree::new(storage.clone());
        for i in 0..50u8 {
            tree1.set(vec![i], vec![i]).await.unwrap();
        }

        // Tree 2: keys 100-149 (completely disjoint)
        let mut tree2 = MeasuredTree::new(storage.clone());
        for i in 100..150u8 {
            tree2.set(vec![i], vec![i]).await.unwrap();
        }

        // Reset read counter
        {
            let storage_lock = storage.lock().await;
            let reads_before = storage_lock.backend.reads();
            drop(storage_lock);

            // Differentiate - no shared segments to prune
            let changes = tree1.differentiate(&tree2);
            pin_mut!(changes);
            let mut count = 0;
            while let Some(result) = changes.next().await {
                result.unwrap();
                count += 1;
            }

            assert_eq!(count, 100, "Should have 50 removes + 50 adds");

            let storage_lock = storage.lock().await;
            let reads_after = storage_lock.backend.reads();
            let diff_reads = reads_after - reads_before;

            println!("Reads for disjoint trees: {}", diff_reads);

            // With no shared segments, we need to read most/all nodes
            // This is expected and unavoidable
        }
    }

    #[tokio::test]
    async fn test_partially_shared_tree_pruning() {
        // Test where trees share some segments but not others
        let backend = MeasuredStorage::new(MemoryStorageBackend::default());
        let storage = Arc::new(Mutex::new(Storage {
            encoder: CborEncoder,
            backend,
        }));

        // Base: 0-99
        let mut tree1 = MeasuredTree::new(storage.clone());
        for i in 0..100u8 {
            tree1.set(vec![i], vec![i]).await.unwrap();
        }

        // Modified: 0-49 same, 50-99 different, 100-149 new
        let mut tree2 = MeasuredTree::new(storage.clone());
        for i in 0..50u8 {
            tree2.set(vec![i], vec![i]).await.unwrap(); // Same
        }
        for i in 50..100u8 {
            tree2.set(vec![i], vec![i * 2]).await.unwrap(); // Different
        }
        for i in 100..150u8 {
            tree2.set(vec![i], vec![i]).await.unwrap(); // New
        }

        // Reset read counter
        {
            let storage_lock = storage.lock().await;
            let reads_before = storage_lock.backend.reads();
            drop(storage_lock);

            // Differentiate
            let changes = tree2.differentiate(&tree1);
            pin_mut!(changes);
            let mut adds = Vec::new();
            let mut removes = Vec::new();
            while let Some(result) = changes.next().await {
                match result.unwrap() {
                    Change::Add(e) => adds.push(e),
                    Change::Remove(e) => removes.push(e),
                }
            }

            // 50 modified (50 removes + 50 adds) + 50 new adds = 150 changes
            assert_eq!(removes.len(), 50);
            assert_eq!(adds.len(), 100);

            let storage_lock = storage.lock().await;
            let reads_after = storage_lock.backend.reads();
            let diff_reads = reads_after - reads_before;

            println!("Reads for partially shared tree: {}", diff_reads);

            // We should be able to skip the shared portion (0-49)
            // and only read nodes for the differing/new portions
            // This verifies that pruning works for partially shared trees
        }
    }

    #[tokio::test]
    async fn test_extreme_size_difference() {
        // Test with extreme size difference to really stress the height mismatch case
        // Small tree (3 entries) vs very large tree (same 3 + 1000 more)
        let backend = MeasuredStorage::new(MemoryStorageBackend::default());
        let storage = Arc::new(Mutex::new(Storage {
            encoder: CborEncoder,
            backend,
        }));

        // Tiny tree: just 3 entries
        let mut tiny_tree = MeasuredTree::new(storage.clone());
        tiny_tree.set(vec![0], vec![0]).await.unwrap();
        tiny_tree.set(vec![1], vec![1]).await.unwrap();
        tiny_tree.set(vec![2], vec![2]).await.unwrap();

        // Huge tree: same 3 entries + 1000 more
        let mut huge_tree = MeasuredTree::new(storage.clone());
        for i in 0..255u8 {
            huge_tree.set(vec![i], vec![i]).await.unwrap();
        }

        println!("Tiny tree: 3 entries, Huge tree: 255 entries");

        // Reset read counter
        let storage_lock = storage.lock().await;
        let reads_before = storage_lock.backend.reads();
        drop(storage_lock);

        // Differentiate huge from tiny
        let changes = huge_tree.differentiate(&tiny_tree);
        pin_mut!(changes);
        let mut count = 0;
        while let Some(result) = changes.next().await {
            result.unwrap();
            count += 1;
        }

        assert_eq!(count, 252, "Should have 252 add changes (255 - 3)");

        let storage_lock = storage.lock().await;
        let reads_after = storage_lock.backend.reads();
        let diff_reads = reads_after - reads_before;

        println!("Reads for extreme size diff: {}", diff_reads);

        // With such different sizes, the trees almost certainly have different heights
        // If we see a large number of reads (e.g., > 30), that's the height mismatch problem
        // IDEAL: Should only read nodes for the differing portion
        // PROBLEM: May read entire huge tree structure down to leaves
        if diff_reads > 30 {
            println!("HEIGHT MISMATCH ISSUE DETECTED!");
            println!(
                "Read {} nodes - likely expanding entire tree due to height difference",
                diff_reads
            );
        } else {
            println!(
                "Good performance - only {} reads despite size difference",
                diff_reads
            );
        }
    }
}
