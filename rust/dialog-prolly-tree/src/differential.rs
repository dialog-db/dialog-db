use crate::{DialogProllyTreeError, Entry, KeyType, Node, Reference, Tree, ValueType};
use async_stream::{stream, try_stream};
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

/// A section is a list of references that differ between before and after.
#[derive(Debug, Clone)]
pub struct Section<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>(
    Vec<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>>,
)
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>;

impl<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>
    Section<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    pub fn new() -> Self {
        Section(vec![])
    }

    pub fn from(nodes: Vec<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>>) -> Self {
        Section(nodes)
    }
}

/// A section is a list of references that differ between before and after.
#[derive(Debug, Clone)]
pub struct SparseTree<'a, const F: u32, const H: usize, S: Settings<F, H>> {
    storage: &'a S::Storage,
    nodes: Vec<Node<F, H, S::Key, S::Value, S::Hash>>,
}

impl<'a, const F: u32, const H: usize, S: Settings<F, H>> SparseTree<'a, F, H, S> {
    pub fn from(
        tree: &'a Tree<F, H, S::Distribution, S::Key, S::Value, S::Hash, S::Storage>,
    ) -> Self {
        let storage = tree.storage();
        let nodes = tree.root().map(|root| vec![root.clone()]).unwrap_or(vec![]);

        SparseTree { storage, nodes }
    }

    pub fn upper_bound(&self) -> Option<&S::Key> {
        self.nodes.last().map(|node| node.reference().upper_bound())
    }

    /// Expands this node range into nodes of their children, result is still a
    /// same range except level down. Returns true if we were able to expand
    /// or false if we reached the segment nodes and there is nothing more to
    /// expand.
    pub async fn expand(&mut self) -> Result<bool, DialogProllyTreeError> {
        let nodes = &mut self.nodes;
        let mut expanded = false;
        for offset in 0..nodes.len() {
            let node = &nodes[offset];
            if node.is_branch() {
                let children = node.load_children(self.storage).await?;
                nodes.splice(offset..offset, children);
                expanded = true;
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
        let Self(left) = self;
        let Self(right) = other;

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

pub trait Settings<const BRANCH_FACTOR: u32, const HASH_SIZE: usize> {
    const BRANCH_FACTOR: u32;
    const HASH_SIZE: usize;
    type Distribution: crate::Distribution<BRANCH_FACTOR, HASH_SIZE, Self::Key, Self::Hash>;
    type Key: KeyType + 'static;
    type Value: ValueType;
    type Hash: HashType<HASH_SIZE>;
    type Storage: ContentAddressedStorage<HASH_SIZE, Hash = Self::Hash>;
}

pub struct Delta<'a, const F: u32, const H: usize, S: Settings<F, H>>(
    SparseTree<'a, F, H, S>,
    SparseTree<'a, F, H, S>,
);

impl<'a, const F: u32, const H: usize, S: Settings<F, H>> Delta<'a, F, H, S> {
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

    pub fn stream(&self) -> impl Differential<S::Key, S::Value> {
        let Delta(left, right) = self;
        let left_entries = left.stream();
        let right_entries = right.stream();
        // Here we need to diff them now
    }
}

/// Represents a difference between two trees as sections.
/// Sections can be descended (one level) or expanded (until segments).
pub struct Difference<
    'a,
    const BRANCH_FACTOR: u32,
    const HASH_SIZE: usize,
    Key,
    Value,
    Hash,
    Storage,
> where
    Key: KeyType + 'static,
    Value: ValueType + PartialEq + 'a,
    Hash: HashType<HASH_SIZE> + PartialEq + 'a,
    Storage: ContentAddressedStorage<HASH_SIZE, Hash = Hash>,
{
    /// Sections that differ in the before tree
    before_sections: Vec<Section<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>>,
    /// Sections that differ in the after tree
    after_sections: Vec<Section<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>>,
    storage: &'a Storage,
    _phantom: std::marker::PhantomData<(Value,)>,
}

impl<'a, const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash, Storage>
    Difference<'a, BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash, Storage>
where
    Key: KeyType + 'static,
    Value: ValueType + PartialEq + 'a,
    Hash: HashType<HASH_SIZE> + PartialEq + 'a,
    Storage: ContentAddressedStorage<HASH_SIZE, Hash = Hash>,
{
    /// Create a new Difference from two nodes.
    pub fn new(
        before: Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>,
        after: Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>,
        storage: &'a Storage,
    ) -> Result<Self, DialogProllyTreeError> {
        // Start with one section containing the root references
        let before_sections = if before.is_segment() {
            vec![]
        } else {
            vec![Section::from(&before)?]
        };

        let after_sections = if after.is_segment() {
            vec![]
        } else {
            vec![Section::from(&after)?]
        };

        Ok(Self {
            before_sections,
            after_sections,
            storage,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Descend one level - expand all sections by loading children and comparing.
    /// This prunes matching refs and creates narrower sections.
    pub async fn descend(&mut self) -> Result<(), DialogProllyTreeError> {
        let mut new_before_sections = Vec::new();
        let mut new_after_sections = Vec::new();

        // Process each pair of sections
        for section_idx in 0..self.before_sections.len().max(self.after_sections.len()) {
            let before_section = self.before_sections.get(section_idx);
            let after_section = self.after_sections.get(section_idx);

            match (before_section, after_section) {
                (Some(before_refs), Some(after_refs)) => {
                    // Load nodes concurrently
                    let before_futures: Vec<_> = before_refs
                        .iter()
                        .map(|r| {
                            Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                                r.hash().clone(),
                                self.storage,
                            )
                        })
                        .collect();

                    let after_futures: Vec<_> = after_refs
                        .iter()
                        .map(|r| {
                            Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                                r.hash().clone(),
                                self.storage,
                            )
                        })
                        .collect();

                    let mut before_nodes = Vec::new();
                    for future in before_futures {
                        before_nodes.push(future.await?);
                    }

                    let mut after_nodes = Vec::new();
                    for future in after_futures {
                        after_nodes.push(future.await?);
                    }

                    // Check if all are segments
                    let all_segments = before_nodes.iter().all(|n| n.is_segment())
                        && after_nodes.iter().all(|n| n.is_segment());

                    if all_segments {
                        // Keep as is - ready for entry diffing
                        new_before_sections.push(before_refs.clone());
                        new_after_sections.push(after_refs.clone());
                    } else {
                        // Collect child refs (or keep segment refs)
                        let mut before_child_refs = Vec::new();
                        for (idx, node) in before_nodes.into_iter().enumerate() {
                            if node.is_branch() {
                                before_child_refs.extend(node.references()?.iter().cloned());
                            } else {
                                // Keep segment ref
                                before_child_refs.push(before_refs[idx].clone());
                            }
                        }

                        let mut after_child_refs = Vec::new();
                        for (idx, node) in after_nodes.into_iter().enumerate() {
                            if node.is_branch() {
                                after_child_refs.extend(node.references()?.iter().cloned());
                            } else {
                                // Keep segment ref
                                after_child_refs.push(after_refs[idx].clone());
                            }
                        }

                        // Compare and prune
                        let sub_sections =
                            Self::compare_sections(&before_child_refs, &after_child_refs);
                        for (before_sub, after_sub) in sub_sections {
                            new_before_sections.push(before_sub);
                            new_after_sections.push(after_sub);
                        }
                    }
                }
                (Some(before_refs), None) => {
                    // Only before - keep as is
                    new_before_sections.push(before_refs.clone());
                    new_after_sections.push(Vec::new());
                }
                (None, Some(after_refs)) => {
                    // Only after - keep as is
                    new_before_sections.push(Vec::new());
                    new_after_sections.push(after_refs.clone());
                }
                (None, None) => unreachable!(),
            }
        }

        self.before_sections = new_before_sections;
        self.after_sections = new_after_sections;

        Ok(())
    }

    /// Check if all sections have reached segments (ready for entry diffing).
    pub async fn is_at_segments(&self) -> Result<bool, DialogProllyTreeError> {
        for section in &self.before_sections {
            for reference in section {
                let node = Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                    reference.hash().clone(),
                    self.storage,
                )
                .await?;
                if node.is_branch() {
                    return Ok(false);
                }
            }
        }

        for section in &self.after_sections {
            for reference in section {
                let node = Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                    reference.hash().clone(),
                    self.storage,
                )
                .await?;
                if node.is_branch() {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Expand sections by descending until all sections reach segments.
    pub async fn expand(&mut self) -> Result<(), DialogProllyTreeError> {
        while !self.is_at_segments().await? {
            self.descend().await?;
        }
        Ok(())
    }

    /// Convert into a stream that yields changes.
    /// Sections should be expanded to segments first via `expand()`.
    pub fn into_stream(self) -> impl Differential<Key, Value> + 'a {
        let storage = self.storage;
        let before_sections = self.before_sections;
        let after_sections = self.after_sections;

        stream! {
            // Process each pair of sections
            for section_idx in 0..before_sections.len().max(after_sections.len()) {
                let before_section = before_sections.get(section_idx);
                let after_section = after_sections.get(section_idx);

                match (before_section, after_section) {
                    (Some(before_refs), Some(after_refs)) => {
                        if before_refs.is_empty() {
                            // All additions
                            for reference in after_refs {
                                let node = match Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                                    reference.hash().clone(),
                                    storage,
                                ).await {
                                    Ok(n) => n,
                                    Err(e) => { yield Err(e); continue; }
                                };
                                let entries = match collect_all_entries::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(node, storage).await {
                                    Ok(e) => e,
                                    Err(e) => { yield Err(e); continue; }
                                };
                                for entry in entries {
                                    yield Ok(Change::Add(entry));
                                }
                            }
                            continue;
                        }

                        if after_refs.is_empty() {
                            // All removals
                            for reference in before_refs {
                                let node = match Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                                    reference.hash().clone(),
                                    storage,
                                ).await {
                                    Ok(n) => n,
                                    Err(e) => { yield Err(e); continue; }
                                };
                                let entries = match collect_all_entries::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(node, storage).await {
                                    Ok(e) => e,
                                    Err(e) => { yield Err(e); continue; }
                                };
                                for entry in entries {
                                    yield Ok(Change::Remove(entry));
                                }
                            }
                            continue;
                        }

                        // Load segment nodes and collect entries
                        let mut before_entries = Vec::new();
                        for reference in before_refs {
                            let node = match Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                                reference.hash().clone(),
                                storage,
                            ).await {
                                Ok(n) => n,
                                Err(e) => { yield Err(e); continue; }
                            };
                            let entries = match collect_all_entries::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(node, storage).await {
                                Ok(e) => e,
                                Err(e) => { yield Err(e); continue; }
                            };
                            before_entries.extend(entries);
                        }

                        let mut after_entries = Vec::new();
                        for reference in after_refs {
                            let node = match Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                                reference.hash().clone(),
                                storage,
                            ).await {
                                Ok(n) => n,
                                Err(e) => { yield Err(e); continue; }
                            };
                            let entries = match collect_all_entries::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(node, storage).await {
                                Ok(e) => e,
                                Err(e) => { yield Err(e); continue; }
                            };
                            after_entries.extend(entries);
                        }

                        // Diff entries
                        let changes = diff_entries(before_entries, after_entries);
                        for change in changes {
                            yield Ok(change);
                        }
                    }
                    (Some(before_refs), None) => {
                        // All removals
                        for reference in before_refs {
                            let node = match Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                                reference.hash().clone(),
                                storage,
                            ).await {
                                Ok(n) => n,
                                Err(e) => { yield Err(e); continue; }
                            };
                            let entries = match collect_all_entries::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(node, storage).await {
                                Ok(e) => e,
                                Err(e) => { yield Err(e); continue; }
                            };
                            for entry in entries {
                                yield Ok(Change::Remove(entry));
                            }
                        }
                    }
                    (None, Some(after_refs)) => {
                        // All additions
                        for reference in after_refs {
                            let node = match Node::<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>::from_hash(
                                reference.hash().clone(),
                                storage,
                            ).await {
                                Ok(n) => n,
                                Err(e) => { yield Err(e); continue; }
                            };
                            let entries = match collect_all_entries::<BRANCH_FACTOR, HASH_SIZE, _, _, _, _>(node, storage).await {
                                Ok(e) => e,
                                Err(e) => { yield Err(e); continue; }
                            };
                            for entry in entries {
                                yield Ok(Change::Add(entry));
                            }
                        }
                    }
                    (None, None) => unreachable!(),
                }
            }
        }
    }

    /// Compare two lists of references and return sections that differ.
    /// Each section is a range that needs further comparison.
    fn compare_sections(
        before_refs: &[Reference<HASH_SIZE, Key, Hash>],
        after_refs: &[Reference<HASH_SIZE, Key, Hash>],
    ) -> Vec<(
        Vec<Reference<HASH_SIZE, Key, Hash>>,
        Vec<Reference<HASH_SIZE, Key, Hash>>,
    )> {
        let mut result = Vec::new();
        let mut before_idx = 0;
        let mut after_idx = 0;

        while before_idx < before_refs.len() || after_idx < after_refs.len() {
            let mut before_section = Vec::new();
            let mut after_section = Vec::new();

            loop {
                if before_idx >= before_refs.len() || after_idx >= after_refs.len() {
                    while before_idx < before_refs.len() {
                        before_section.push(before_refs[before_idx].clone());
                        before_idx += 1;
                    }
                    while after_idx < after_refs.len() {
                        after_section.push(after_refs[after_idx].clone());
                        after_idx += 1;
                    }
                    break;
                }

                let before_ref = &before_refs[before_idx];
                let after_ref = &after_refs[after_idx];

                if before_ref.upper_bound() == after_ref.upper_bound() {
                    if before_ref.hash() == after_ref.hash() {
                        // Same hash - prune (exclusive of these refs)
                        before_idx += 1;
                        after_idx += 1;
                        break;
                    } else {
                        // Different hashes - include in section
                        before_section.push(before_ref.clone());
                        after_section.push(after_ref.clone());
                        before_idx += 1;
                        after_idx += 1;
                        break;
                    }
                } else {
                    // Advance side with smaller bound
                    if before_ref.upper_bound() < after_ref.upper_bound() {
                        before_section.push(before_ref.clone());
                        before_idx += 1;
                    } else {
                        after_section.push(after_ref.clone());
                        after_idx += 1;
                    }
                }
            }

            if !before_section.is_empty() || !after_section.is_empty() {
                result.push((before_section, after_section));
            }
        }

        result
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
    stream! {
        // Early exit if identical
        if before.hash() == after.hash() {
            return;
        }

        eprintln!("differentiate: before.is_segment()={}, after.is_segment()={}",
                  before.is_segment(), after.is_segment());

        // Special case: if both are segments, diff entries directly
        if before.is_segment() && after.is_segment() {
            eprintln!("Both segments - diffing entries directly");
            let before_entries = match before.into_entries() {
                Ok(e) => Vec::from(e),
                Err(e) => { yield Err(e); return; }
            };
            let after_entries = match after.into_entries() {
                Ok(e) => Vec::from(e),
                Err(e) => { yield Err(e); return; }
            };

            eprintln!("before_entries.len()={}, after_entries.len()={}",
                      before_entries.len(), after_entries.len());

            for change in diff_entries(before_entries, after_entries) {
                eprintln!("Yielding change: {:?}", match &change {
                    Change::Add(_) => "Add",
                    Change::Remove(_) => "Remove",
                });
                yield Ok(change);
            }
            return;
        }

        // Create Difference and expand to segments
        let mut diff = match Difference::new(before, after, storage) {
            Ok(d) => d,
            Err(e) => { yield Err(e); return; }
        };

        eprintln!("After new: before_sections.len()={}, after_sections.len()={}",
                  diff.before_sections.len(), diff.after_sections.len());

        match diff.expand().await {
            Ok(()) => {},
            Err(e) => { yield Err(e); return; }
        }

        eprintln!("After expand: before_sections.len()={}, after_sections.len()={}",
                  diff.before_sections.len(), diff.after_sections.len());

        // Stream the changes
        let stream = diff.into_stream();
        for await change in stream {
            eprintln!("Yielding change from stream: {:?}", match &change {
                Ok(Change::Add(_)) => "Add",
                Ok(Change::Remove(_)) => "Remove",
                Err(_) => "Error",
            });
            yield change;
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
