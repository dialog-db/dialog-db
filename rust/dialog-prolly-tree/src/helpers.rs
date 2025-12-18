//! Test and debugging utilities for tree traversal.
//!
//! This module provides utilities for iterating over all nodes in a tree,
//! which is useful for testing, debugging, and advanced introspection.
//!
//! # Example
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

use std::collections::VecDeque;

use async_stream::try_stream;
use dialog_storage::{ContentAddressedStorage, HashType};
use futures_core::Stream;

use crate::{DialogProllyTreeError, KeyType, Node, Tree, ValueType};

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
        use futures_util::StreamExt;
        self.filter_map(|r| async { r.ok().map(|n| n.hash().clone()) })
            .collect()
    }
}
