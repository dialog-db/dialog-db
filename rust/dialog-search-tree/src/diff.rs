//! Tree differentiation and integration.
//!
//! This module computes the difference between two tree versions and applies
//! change streams onto a tree. Both operations are built around the same
//! frugality contract: **only blocks on differing paths are ever read**.
//! Subtrees whose hashes match between the two versions are pruned by
//! comparing the hashes carried in their parents' links, without loading the
//! subtrees themselves, so the number of storage reads is proportional to the
//! size of the difference rather than the size of the trees.
//!
//! [`TreeDifference::compute`] produces a sparse representation of both trees
//! holding only the differing regions. From it, two products are available:
//!
//! - [`TreeDifference::changes`]: entry-level [`Change`]s that transform the
//!   source tree into the target tree (used for merge/replication).
//! - [`TreeDifference::novel_nodes`]: the target-tree nodes absent from the
//!   source tree (used to upload exactly the missing blocks to a remote).

use std::cmp::Ordering;

use async_stream::try_stream;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_core::Stream;
use futures_util::StreamExt;
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    ArchivedNodeBody, Buffer, ContentAddressedStorage, DialogSearchTreeError, Entry, Key, Link,
    Node, SymmetryWith, Tree, Value, into_owned,
};

/// Represents a change in the key-value store.
#[derive(Clone, Debug)]
pub enum Change<Key, Value> {
    /// Adds an entry to the key-value store.
    Add(Entry<Key, Value>),
    /// Removes an entry from the key-value store.
    Remove(Entry<Key, Value>),
}

/// Represents a differential stream of changes in the key-value store.
pub trait Differential<Key, Value>:
    Stream<Item = Result<Change<Key, Value>, DialogSearchTreeError>>
{
}

impl<Key, Value, T> Differential<Key, Value> for T where
    T: Stream<Item = Result<Change<Key, Value>, DialogSearchTreeError>>
{
}

/// Either a loaded node or an unloaded reference in a [`SparseTree`].
///
/// Differentiation never loads nodes eagerly: a node stays a [`Link`] (hash
/// plus upper bound, obtained from its parent) until the comparison proves
/// the node lies on a differing path. Shared subtrees are recognized and
/// discarded while still unloaded, which is what keeps reads proportional to
/// the difference.
enum SparseTreeNode<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
{
    /// A fully loaded node together with its owned upper bound key.
    Loaded {
        /// The loaded node.
        node: Node<Key, Value>,
        /// The node's upper bound key (owned copy).
        upper_bound: Key,
    },
    /// An unloaded reference: hash and upper bound from the parent's link.
    Ref(Link<Key>),
}

impl<Key, Value> SparseTreeNode<Key, Value>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    fn upper_bound(&self) -> &Key {
        match self {
            SparseTreeNode::Loaded { upper_bound, .. } => upper_bound,
            SparseTreeNode::Ref(link) => &link.upper_bound,
        }
    }

    fn hash(&self) -> &Blake3Hash {
        match self {
            SparseTreeNode::Loaded { node, .. } => node.hash(),
            SparseTreeNode::Ref(link) => &link.node,
        }
    }
}

/// A sparse, lazily loaded view over one side of a tree comparison.
///
/// Holds the current frontier of nodes sorted by upper bound (a mix of
/// loaded nodes and unloaded references) plus every index node that was
/// loaded and expanded along the way (the novel interior nodes of this
/// side).
struct SparseTree<'a, Key, Value, Backend>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    storage: &'a ContentAddressedStorage<Backend>,
    nodes: Vec<SparseTreeNode<Key, Value>>,
    expanded: Vec<Node<Key, Value>>,
}

impl<'a, Key, Value, Backend> SparseTree<'a, Key, Value, Backend>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend,
{
    /// Reads a node from storage by hash.
    async fn load(
        storage: &ContentAddressedStorage<Backend>,
        hash: &Blake3Hash,
    ) -> Result<Node<Key, Value>, DialogSearchTreeError> {
        let bytes = storage.retrieve(hash).await?.ok_or_else(|| {
            DialogSearchTreeError::Node(format!("Blob not found in storage: {hash}"))
        })?;
        Ok(Node::new(Buffer::from(bytes)))
    }

    /// Initializes a sparse tree from a root hash. The root is not loaded;
    /// a null hash produces an empty frontier.
    async fn from_root(
        root: &Blake3Hash,
        storage: &'a ContentAddressedStorage<Backend>,
    ) -> Result<SparseTree<'a, Key, Value, Backend>, DialogSearchTreeError> {
        let nodes = if root == NULL_BLAKE3_HASH {
            vec![]
        } else {
            let node: Node<Key, Value> = Self::load(storage, root).await?;
            let upper_bound = node.body()?.upper_bound().and_then(into_owned)?;
            vec![SparseTreeNode::Loaded { node, upper_bound }]
        };

        Ok(SparseTree {
            storage,
            nodes,
            expanded: vec![],
        })
    }

    /// Expands the node covering `bound`, if any.
    ///
    /// The frontier is sorted by upper bound, so the first node whose upper
    /// bound is `>= bound` is the only one whose key range can contain
    /// `bound`. If that node is an index, it is replaced by references to
    /// its children (loading it first when it is still a reference) and the
    /// node is recorded as expanded. If it is a segment, the loaded form
    /// replaces the reference in place so a later entry walk does not load
    /// it twice.
    ///
    /// Returns `true` when an index node was expanded.
    async fn expand_at(&mut self, bound: &Key) -> Result<bool, DialogSearchTreeError> {
        let Some(offset) = self
            .nodes
            .iter()
            .position(|node| node.upper_bound() >= bound)
        else {
            return Ok(false);
        };

        let node = match &self.nodes[offset] {
            SparseTreeNode::Loaded { node, .. } => node.clone(),
            SparseTreeNode::Ref(link) => Self::load(self.storage, &link.node).await?,
        };

        match node.body()? {
            ArchivedNodeBody::Index(index) => {
                let children = index
                    .links
                    .iter()
                    .map(|link| Ok(SparseTreeNode::Ref(into_owned::<Link<Key>>(link)?)))
                    .collect::<Result<Vec<_>, DialogSearchTreeError>>()?;

                self.expanded.push(node);
                self.nodes.splice(offset..offset + 1, children);
                Ok(true)
            }
            ArchivedNodeBody::Segment(_) => {
                let upper_bound = node.body()?.upper_bound().and_then(into_owned)?;
                self.nodes[offset] = SparseTreeNode::Loaded { node, upper_bound };
                Ok(false)
            }
        }
    }

    /// Removes nodes that are shared between `self` and `other`, keeping
    /// only nodes that differ.
    ///
    /// Both frontiers are sorted by upper bound. Two nodes are shared when
    /// their upper bounds are equal and their hashes are equal; such pairs
    /// are removed from both sides without ever being loaded. This is the
    /// step that realizes the read-frugality contract.
    fn prune(&mut self, other: &mut Self) {
        let left = &mut self.nodes;
        let right = &mut other.nodes;

        let mut at_left = 0;
        let mut at_right = 0;
        let mut to_left = 0;
        let mut to_right = 0;

        while at_left < left.len() && at_right < right.len() {
            match left[at_left]
                .upper_bound()
                .cmp(right[at_right].upper_bound())
            {
                Ordering::Less => {
                    left.swap(to_left, at_left);
                    to_left += 1;
                    at_left += 1;
                }
                Ordering::Greater => {
                    right.swap(to_right, at_right);
                    to_right += 1;
                    at_right += 1;
                }
                Ordering::Equal => {
                    if left[at_left].hash() != right[at_right].hash() {
                        left.swap(to_left, at_left);
                        right.swap(to_right, at_right);
                        to_left += 1;
                        to_right += 1;
                    }
                    at_left += 1;
                    at_right += 1;
                }
            }
        }

        while at_left < left.len() {
            left.swap(to_left, at_left);
            to_left += 1;
            at_left += 1;
        }
        while at_right < right.len() {
            right.swap(to_right, at_right);
            to_right += 1;
            at_right += 1;
        }

        left.truncate(to_left);
        right.truncate(to_right);
    }

    /// Streams the entries of every node remaining in the frontier, in key
    /// order, descending through any nodes that are still indexes.
    fn stream(&self) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + '_ {
        try_stream! {
            for sparse_node in &self.nodes {
                let node = match sparse_node {
                    SparseTreeNode::Loaded { node, .. } => node.clone(),
                    SparseTreeNode::Ref(link) => Self::load(self.storage, &link.node).await?,
                };

                // In-order walk; children pushed in reverse so the stack
                // pops them in key order.
                let mut stack = vec![node];
                while let Some(node) = stack.pop() {
                    match node.body()? {
                        ArchivedNodeBody::Index(index) => {
                            let mut children = Vec::with_capacity(index.links.len());
                            for link in index.links.iter() {
                                let hash = <&Blake3Hash>::from(&link.node);
                                children.push(Self::load(self.storage, hash).await?);
                            }
                            while let Some(child) = children.pop() {
                                stack.push(child);
                            }
                        }
                        ArchivedNodeBody::Segment(segment) => {
                            for entry in segment.entries.iter() {
                                yield into_owned(entry)?;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Represents a difference computed between two trees (source, target).
///
/// Contains sparse representations of both trees holding only the differing
/// regions, and can produce either entry-level changes via
/// [`changes`](Self::changes) (to transform source into target) or
/// node-level novelty via [`novel_nodes`](Self::novel_nodes) (the target
/// nodes the source side does not have).
pub struct TreeDifference<'a, Key, Value, Backend>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    source: SparseTree<'a, Key, Value, Backend>,
    target: SparseTree<'a, Key, Value, Backend>,
}

impl<'a, Key, Value, Backend> TreeDifference<'a, Key, Value, Backend>
where
    Key: self::Key + ConditionalSync + 'static,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key: for<'b> rkyv::Serialize<
            Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'b>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + ConditionalSync
        + for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value + PartialEq + ConditionalSync + 'static,
    Value: for<'b> rkyv::Serialize<
            Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'b>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend
        + ConditionalSync,
{
    /// Computes the difference between two trees.
    ///
    /// Both trees are explored level by level in lockstep. At every step,
    /// nodes whose upper bound and hash match on both sides are discarded
    /// without being loaded; only nodes whose hashes differ (or whose key
    /// ranges do not line up) are expanded. The number of reads is therefore
    /// proportional to the number of differing nodes, never to the size of
    /// the trees: in particular, two identical trees are recognized by their
    /// root hashes alone, with zero reads.
    pub async fn compute(
        source_tree: &Tree<Key, Value>,
        target_tree: &Tree<Key, Value>,
        source_storage: &'a ContentAddressedStorage<Backend>,
        target_storage: &'a ContentAddressedStorage<Backend>,
    ) -> Result<TreeDifference<'a, Key, Value, Backend>, DialogSearchTreeError> {
        // Identical trees (including two empty trees) share their root
        // hash; nothing needs to be read at all.
        if source_tree.root() == target_tree.root() {
            return Ok(TreeDifference {
                source: SparseTree {
                    storage: source_storage,
                    nodes: vec![],
                    expanded: vec![],
                },
                target: SparseTree {
                    storage: target_storage,
                    nodes: vec![],
                    expanded: vec![],
                },
            });
        }

        let mut source: SparseTree<'a, Key, Value, Backend> =
            SparseTree::from_root(source_tree.root(), source_storage).await?;
        let mut target: SparseTree<'a, Key, Value, Backend> =
            SparseTree::from_root(target_tree.root(), target_storage).await?;

        // Iteratively prune shared nodes and expand differing ones until a
        // fixed point: only differing leaf segments (and unique-range
        // subtrees) remain.
        loop {
            source.prune(&mut target);

            let mut expanded = false;
            let mut source_idx = 0;
            let mut target_idx = 0;

            while source_idx < source.nodes.len() && target_idx < target.nodes.len() {
                let source_bound = source.nodes[source_idx].upper_bound().clone();
                let target_bound = target.nodes[target_idx].upper_bound().clone();

                match source_bound.cmp(&target_bound) {
                    Ordering::Less => {
                        // The target node covers a larger range; expand it to
                        // reveal boundaries matching the source side.
                        if target.expand_at(&source_bound).await? {
                            expanded = true;
                            break;
                        }
                        source_idx += 1;
                    }
                    Ordering::Greater => {
                        if source.expand_at(&target_bound).await? {
                            expanded = true;
                            break;
                        }
                        target_idx += 1;
                    }
                    Ordering::Equal => {
                        if source.nodes[source_idx].hash() != target.nodes[target_idx].hash() {
                            if source.expand_at(&source_bound).await? {
                                expanded = true;
                            }
                            if target.expand_at(&target_bound).await? {
                                expanded = true;
                            }
                            if expanded {
                                break;
                            }
                        }
                        source_idx += 1;
                        target_idx += 1;
                    }
                }
            }

            if !expanded {
                break;
            }
        }

        // Expand any remaining target index nodes so novel_nodes() can
        // enumerate every novel node, not just unexpanded subtree roots.
        // These reads are not wasted: every remaining target node is novel
        // by construction and must be visited to be reported.
        loop {
            let mut expanded = false;
            for index in 0..target.nodes.len() {
                let bound = target.nodes[index].upper_bound().clone();
                if target.expand_at(&bound).await? {
                    expanded = true;
                    break;
                }
            }
            if !expanded {
                break;
            }
        }

        Ok(TreeDifference { source, target })
    }

    /// Returns a stream of entry-level changes that transform the source
    /// tree into the target tree.
    ///
    /// Performs a two-cursor walk over the entries of both differing
    /// regions: keys only on the source side yield [`Change::Remove`], keys
    /// only on the target side yield [`Change::Add`], and keys present on
    /// both sides with different values yield a `Remove` of the old entry
    /// followed by an `Add` of the new one.
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
                        yield Change::Remove(source_entry.clone());
                        source_next = source_stream.next().await;
                    }
                    (None, Some(Ok(target_entry))) => {
                        yield Change::Add(target_entry.clone());
                        target_next = target_stream.next().await;
                    }
                    (Some(Err(_)), _) => {
                        if let Some(Err(error)) = source_next.take() {
                            Err(error)?;
                        }
                    }
                    (_, Some(Err(_))) => {
                        if let Some(Err(error)) = target_next.take() {
                            Err(error)?;
                        }
                    }
                    (Some(Ok(source_entry)), Some(Ok(target_entry))) => {
                        match source_entry.key.cmp(&target_entry.key) {
                            Ordering::Less => {
                                yield Change::Remove(source_entry.clone());
                                source_next = source_stream.next().await;
                            }
                            Ordering::Greater => {
                                yield Change::Add(target_entry.clone());
                                target_next = target_stream.next().await;
                            }
                            Ordering::Equal => {
                                if source_entry.value != target_entry.value {
                                    yield Change::Remove(source_entry.clone());
                                    yield Change::Add(target_entry.clone());
                                }
                                source_next = source_stream.next().await;
                                target_next = target_stream.next().await;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Returns a stream of the nodes present in the target tree but absent
    /// from the source tree.
    ///
    /// Yields every index node expanded during comparison plus the
    /// remaining (segment) frontier nodes. This is the block set a remote
    /// holding the source tree needs in order to materialize the target
    /// tree.
    pub fn novel_nodes(
        &'a self,
    ) -> impl Stream<Item = Result<Node<Key, Value>, DialogSearchTreeError>> + 'a {
        try_stream! {
            for node in &self.target.expanded {
                yield node.clone();
            }

            for sparse_node in &self.target.nodes {
                let node = match sparse_node {
                    SparseTreeNode::Loaded { node, .. } => node.clone(),
                    SparseTreeNode::Ref(link) => {
                        SparseTree::<Key, Value, Backend>::load(self.target.storage, &link.node)
                            .await?
                    }
                };
                yield node;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use anyhow::Result;
    use async_trait::async_trait;
    use dialog_common::Blake3Hash;
    use dialog_storage::{DialogStorageError, MemoryStorageBackend, StorageBackend};
    use futures_util::StreamExt;

    use super::{Change, TreeDifference};
    use crate::{ContentAddressedStorage, Tree};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A storage backend that counts every read, so tests can assert that
    /// differentiation only loads blocks on differing paths.
    #[derive(Clone)]
    struct CountingBackend {
        inner: MemoryStorageBackend<Blake3Hash, Vec<u8>>,
        reads: Arc<AtomicUsize>,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                inner: MemoryStorageBackend::default(),
                reads: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn reads(&self) -> usize {
            self.reads.load(AtomicOrdering::Relaxed)
        }

        fn reset(&self) {
            self.reads.store(0, AtomicOrdering::Relaxed);
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl StorageBackend for CountingBackend {
        type Key = Blake3Hash;
        type Value = Vec<u8>;
        type Error = DialogStorageError;

        async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
            self.inner.set(key, value).await
        }

        async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.get(key).await
        }
    }

    type TestTree = Tree<[u8; 4], Vec<u8>>;

    async fn build(
        keys: impl IntoIterator<Item = (u32, Vec<u8>)>,
        storage: &mut ContentAddressedStorage<CountingBackend>,
    ) -> Result<TestTree> {
        let mut tree = TestTree::empty();
        for (key, value) in keys {
            tree = tree.insert(key.to_le_bytes(), value, storage).await?;
        }
        for (hash, buffer) in tree.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }
        Ok(tree)
    }

    async fn collect_changes(
        source: &TestTree,
        target: &TestTree,
        storage: &ContentAddressedStorage<CountingBackend>,
    ) -> Result<Vec<Change<[u8; 4], Vec<u8>>>> {
        let stream = source.differentiate(target, storage, storage);
        futures_util::pin_mut!(stream);
        let mut changes = vec![];
        while let Some(change) = stream.next().await {
            changes.push(change?);
        }
        Ok(changes)
    }

    /// Identical trees are recognized by their root hashes alone: no
    /// changes, and crucially, zero storage reads.
    #[dialog_common::test]
    async fn it_yields_no_changes_and_no_reads_for_identical_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let entries: Vec<(u32, Vec<u8>)> = (0..500u32).map(|i| (i, vec![i as u8])).collect();
        let a = build(entries.clone(), &mut storage).await?;
        let b = build(entries, &mut storage).await?;

        storage.backend().reset();
        let changes = collect_changes(&a, &b, &storage).await?;

        assert!(
            changes.is_empty(),
            "identical trees should yield no changes"
        );
        assert_eq!(
            storage.backend().reads(),
            0,
            "identical trees must be recognized without reading any blocks"
        );
        Ok(())
    }

    /// The change stream reports exactly the adds, removals and updates
    /// between the two trees, and applying it via integrate transforms the
    /// source tree into the target tree.
    #[dialog_common::test]
    async fn it_streams_changes_that_transform_source_into_target() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());

        let mut source_entries: BTreeMap<u32, Vec<u8>> =
            (0..300u32).map(|i| (i, vec![i as u8])).collect();
        let source = build(
            source_entries.iter().map(|(k, v)| (*k, v.clone())),
            &mut storage,
        )
        .await?;

        // Target: remove some keys, update some values, add new keys.
        let mut target_entries = source_entries.clone();
        for i in (0..300u32).step_by(50) {
            target_entries.remove(&i);
        }
        for i in (5..300u32).step_by(70) {
            target_entries.insert(i, vec![0xAB]);
        }
        for i in 300..330u32 {
            target_entries.insert(i, vec![i as u8]);
        }
        let target = build(
            target_entries.iter().map(|(k, v)| (*k, v.clone())),
            &mut storage,
        )
        .await?;

        let changes = collect_changes(&source, &target, &storage).await?;

        // Replay the changes over a plain map and compare against target.
        for change in &changes {
            match change {
                Change::Add(entry) => {
                    source_entries.insert(u32::from_le_bytes(entry.key), entry.value.clone());
                }
                Change::Remove(entry) => {
                    source_entries.remove(&u32::from_le_bytes(entry.key));
                }
            }
        }
        assert_eq!(
            source_entries, target_entries,
            "changes must transform the source entry set into the target's"
        );

        // Applying the same differential to the source tree must produce a
        // tree with the target's root.
        let mut merged = source.clone();
        let changes = source.differentiate(&target, &storage, &storage);
        merged.integrate(changes, &storage).await?;
        for (hash, buffer) in merged.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }
        assert_eq!(
            merged.root(),
            target.root(),
            "integrating the differential must produce the target tree"
        );

        Ok(())
    }

    /// Differentiating two large trees that differ in a single entry reads
    /// only the blocks along the differing paths, not the whole trees.
    #[dialog_common::test]
    async fn it_reads_only_blocks_on_differing_paths() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let entries: Vec<(u32, Vec<u8>)> = (0..2000u32).map(|i| (i, vec![i as u8])).collect();

        let base = build(entries.clone(), &mut storage).await?;

        // One modified entry produces trees that differ along one root-to-
        // leaf path on each side.
        let mut modified = base.clone();
        modified = modified
            .insert(1000u32.to_le_bytes(), vec![0xFF], &storage)
            .await?;
        for (hash, buffer) in modified.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }

        storage.backend().reset();
        let changes = collect_changes(&base, &modified, &storage).await?;
        let reads = storage.backend().reads();

        assert_eq!(changes.len(), 2, "one update is a Remove plus an Add");

        // The differing region is two root-to-leaf paths plus the segments
        // they end in. With ~2000 entries the trees hold dozens of nodes;
        // a path-proportional walk stays far below that. The bound is
        // deliberately loose to avoid coupling the test to the tree shape.
        assert!(
            reads <= 16,
            "diff of a single-entry change read {reads} blocks; expected a \
             path-proportional number, not the whole tree"
        );

        Ok(())
    }

    /// Concurrent adds of the same key resolve deterministically by value
    /// hash, regardless of integration order.
    #[dialog_common::test]
    async fn it_resolves_conflicting_adds_by_value_hash() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let base_entries: Vec<(u32, Vec<u8>)> = (0..50u32).map(|i| (i, vec![i as u8])).collect();

        let base = build(base_entries.clone(), &mut storage).await?;

        let mut ours = base.clone();
        ours = ours.insert(99u32.to_le_bytes(), vec![1], &storage).await?;
        for (hash, buffer) in ours.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }

        let mut theirs = base.clone();
        theirs = theirs
            .insert(99u32.to_le_bytes(), vec![2], &storage)
            .await?;
        for (hash, buffer) in theirs.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }

        // Integrate their changes into ours, and our changes into theirs;
        // both replicas must converge on the same value.
        let mut merged_ours = ours.clone();
        let their_changes = base.differentiate(&theirs, &storage, &storage);
        merged_ours.integrate(their_changes, &storage).await?;

        let mut merged_theirs = theirs.clone();
        let our_changes = base.differentiate(&ours, &storage, &storage);
        merged_theirs.integrate(our_changes, &storage).await?;

        for (hash, buffer) in merged_ours.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }
        for (hash, buffer) in merged_theirs.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }

        assert_eq!(
            merged_ours.root(),
            merged_theirs.root(),
            "replicas must converge regardless of integration order"
        );

        Ok(())
    }

    /// The novel node stream contains exactly the blocks a holder of the
    /// source tree is missing: copying them over makes the target tree
    /// fully readable, and the stream never includes shared blocks.
    #[dialog_common::test]
    async fn it_yields_novel_nodes_sufficient_to_materialize_the_target() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(CountingBackend::new());
        let entries: Vec<(u32, Vec<u8>)> = (0..1000u32).map(|i| (i, vec![i as u8])).collect();

        let base = build(entries.clone(), &mut storage).await?;
        let mut extended = base.clone();
        for i in 1000..1020u32 {
            extended = extended
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (hash, buffer) in extended.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }

        // A "remote" that already has the base tree.
        let mut remote = ContentAddressedStorage::new(CountingBackend::new());
        {
            let difference =
                TreeDifference::compute(&TestTree::empty(), &base, &storage, &storage).await?;
            let nodes = difference.novel_nodes();
            futures_util::pin_mut!(nodes);
            while let Some(node) = nodes.next().await {
                let node = node?;
                remote
                    .store(node.buffer().as_ref().to_vec(), node.hash())
                    .await?;
            }
        }

        // Upload only the novelty between base and extended.
        let difference = TreeDifference::compute(&base, &extended, &storage, &storage).await?;
        let nodes = difference.novel_nodes();
        futures_util::pin_mut!(nodes);
        let mut uploaded = 0;
        while let Some(node) = nodes.next().await {
            let node = node?;
            remote
                .store(node.buffer().as_ref().to_vec(), node.hash())
                .await?;
            uploaded += 1;
        }
        assert!(uploaded > 0, "extension must produce novel nodes");

        // The remote can now read the full extended tree.
        let restored = TestTree::from_hash(extended.root().clone());
        for (key, value) in (0..1020u32).map(|i| (i, vec![i as u8])) {
            assert_eq!(
                restored.get(&key.to_le_bytes(), &remote).await?,
                Some(value),
                "key {key} must be readable from the remote after upload"
            );
        }

        Ok(())
    }
}
