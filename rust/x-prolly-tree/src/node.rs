use std::ops::{Bound, RangeBounds};

use async_stream::try_stream;
use base58::ToBase58;
use futures_core::Stream;
use nonempty::NonEmpty;
use x_storage::{ContentAddressedStorage, HashType};

use crate::{Block, Entry, KeyType, Rank, Reference, ValueType, XProllyTreeError};

/// Primary representation of tree nodes.
///
/// The common error type used by this crate Each [`Node`] stores its children
/// in a [`ContentAddressedStorage`] as key/value pairs. Branches store a
/// collection of children references as [`References`], and segments (leaf
/// nodes) store their key-value [`Entry`] inline.
#[derive(Clone, Debug)]
pub struct Node<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    block: Block<HASH_SIZE, Key, Value, Hash>,
    /// A [`Reference`] that points to this [`Node`]s own [`Block`]
    reference: Reference<HASH_SIZE, Key, Hash>,
}

impl<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>
    Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>
where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    /// Whether this node is a branch.
    pub fn is_branch(&self) -> bool {
        self.block.is_branch()
    }

    /// Whether this node is a segment.
    pub fn is_segment(&self) -> bool {
        self.block.is_segment()
    }

    /// Create a new branch [`Node`] given [`Reference`] children, storing
    /// the new [`Node`] in the provided [`ContentAddressedStorage`]
    pub async fn branch<Storage>(
        children: NonEmpty<Reference<HASH_SIZE, Key, Hash>>,
        storage: &mut Storage,
    ) -> Result<Self, XProllyTreeError>
    where
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        let block = Block::branch(children);
        let bound = block.upper_bound().clone();
        let hash = storage.write(&block).await.map_err(|error| error.into())?;
        let reference = Reference::new(bound, hash);

        Ok(Node { block, reference })
    }

    /// Create a new segment [`Node`] given [`Entry`] children, storing the new
    /// [`Node`] in the provided [`ContentAddressedStorage`]
    pub async fn segment<Storage>(
        children: NonEmpty<Entry<Key, Value>>,
        storage: &mut Storage,
    ) -> Result<Self, XProllyTreeError>
    where
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        let block = Block::segment(children);
        let bound = block.upper_bound().clone();
        let hash = storage.write(&block).await.map_err(|error| error.into())?;
        let reference = Reference::new(bound, hash);

        Ok(Node { block, reference })
    }

    /// Hydrates a [`Node`] from [`ContentAddressedStorage`] given a [`Reference`].
    pub async fn from_reference<Storage>(
        reference: Reference<HASH_SIZE, Key, Hash>,
        storage: &Storage,
    ) -> Result<Self, XProllyTreeError>
    where
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        let Some(block) = storage
            .read(reference.hash())
            .await
            .map_err(|error| error.into())?
        else {
            return Err(XProllyTreeError::MissingBlock(format!("{}", reference)));
        };

        Ok(Node { block, reference })
    }

    /// Hydrates a [`Node`] from [`ContentAddressedStorage`] given a [`HashType`].
    pub async fn from_hash<Storage>(hash: Hash, storage: &Storage) -> Result<Self, XProllyTreeError>
    where
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        let Some(block) = storage.read(&hash).await.map_err(|error| error.into())? else {
            return Err(XProllyTreeError::MissingBlock(format!(
                "#{}",
                hash.bytes().to_base58()
            )));
        };
        let reference = Reference::new(block.upper_bound().clone(), hash);

        Ok(Node { block, reference })
    }

    /// Returns a [`Reference`] for this node.
    pub fn reference(&self) -> &Reference<HASH_SIZE, Key, Hash> {
        &self.reference
    }

    /// Returns the [`Hash`] for this [`Node`] used to retrieve from
    /// [`ContentAddressedStorage`].
    pub fn hash(&self) -> &Hash {
        &self.reference.hash()
    }

    /// Return all [`Entry`]s from this [`Node`] into a [`Entry`] collection.
    ///
    /// The result is an error if this is not a segment [`Node`].
    pub fn into_entries(self) -> Result<NonEmpty<Entry<Key, Value>>, XProllyTreeError> {
        if !self.is_segment() {
            return Err(XProllyTreeError::IncorrectTreeAccess(
                "Cannot convert branch into entries".into(),
            ));
        }

        self.block.into_entries()
    }

    /// Recursively descends the hierarchy, returning an [`Entry`] matching
    /// `key` if found.
    pub async fn get_entry<Storage>(
        &self,
        key: &Key,
        storage: &Storage,
    ) -> Result<Option<Entry<Key, Value>>, XProllyTreeError>
    where
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        #[allow(unused_assignments)]
        let mut current_node_holder: Option<Self> = None;
        let mut current_node = self;

        loop {
            match current_node.is_branch() {
                true => {
                    let Some(node) = current_node.child_by_key(key, storage).await? else {
                        return Ok(None);
                    };
                    current_node_holder = Some(node);
                    current_node = current_node_holder.as_ref().unwrap();
                }
                false => return current_node.entry_by_key(key),
            }
        }
    }

    /// Inserts a new [`Entry`] into the hierarchy represented by this [`Node`]
    /// as its root. If successful, returns the new root [`Node`] representing
    /// the hierarchy.
    pub async fn insert<Distribution, Storage>(
        &self,
        new_entry: Entry<Key, Value>,
        storage: &mut Storage,
    ) -> Result<Self, XProllyTreeError>
    where
        Distribution: crate::Distribution<BRANCH_FACTOR, HASH_SIZE, Key, Hash>,
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        let key = new_entry.key.to_owned();
        let mut node = self.to_owned();
        let mut branch_stack = vec![];
        let all_entries: Option<NonEmpty<Entry<Key, Value>>>;

        loop {
            match node.is_branch() {
                true => {
                    let mut left = vec![];
                    let mut right = vec![];
                    let mut next = None;
                    for child_reference in node.block.into_references()? {
                        // If key may be contained within the child reference,
                        // or if it's the largest boundary use the last child.
                        if next.is_some() {
                            right.push(child_reference);
                        } else if &key <= child_reference.upper_bound() {
                            next = Some(Node::from_reference(child_reference, storage).await?);
                        } else {
                            left.push(child_reference);
                        }
                    }
                    // If key is greater than the greatest child, use the
                    // greatest child.
                    if next.is_none() {
                        let last = left.pop().ok_or(XProllyTreeError::UnexpectedTreeShape(
                            "No upper bound found".into(),
                        ))?;
                        next = Some(Node::from_reference(last, storage).await?);
                    }
                    branch_stack.push((NonEmpty::from_vec(left), NonEmpty::from_vec(right)));
                    node = next.ok_or(XProllyTreeError::UnexpectedTreeShape(
                        "Next node not found".into(),
                    ))?;
                }
                false => {
                    let mut entries = node.block.into_entries()?;
                    match entries.binary_search_by(|probe| probe.key.cmp(&key)) {
                        // Entry was found; update the value.
                        Ok(index) => {
                            let Some(previous_entry) = entries.get_mut(index) else {
                                return Err(XProllyTreeError::UnexpectedTreeShape(format!(
                                    "Entry at index {} not found",
                                    index,
                                )));
                            };
                            previous_entry.value = new_entry.value;
                        }
                        // Entry was not found; insert at the provided index.
                        Err(index) => {
                            entries.insert(index, new_entry);
                        }
                    };
                    all_entries = Some(entries);
                    break;
                }
            }
        }
        let mut nodes = {
            let Some(entries) = all_entries else {
                return Err(XProllyTreeError::UnexpectedTreeShape(
                    "No entries found".into(),
                ));
            };
            let entries = entries.map(|entry| {
                let rank = Distribution::rank(&entry.key);
                (entry, rank)
            });
            Node::join_with_rank(entries, 1, storage).await?
        };
        let mut min_rank = 2;
        loop {
            let references = {
                let references = nodes.map(|(node, rank)| (node.reference().clone(), rank));
                match branch_stack.pop() {
                    Some(siblings) => {
                        // TBD if we must recompute rank for siblings references
                        // when building up the tree. Attempt to try setting
                        // rank to `0` for references outside of the modified
                        // path.
                        let left = siblings.0.map(|left| {
                            left.map(|reference| {
                                let rank = Distribution::rank(reference.upper_bound());
                                (reference, rank)
                            })
                        });
                        let right = siblings.1.map(|right| {
                            right.map(|reference| {
                                let rank = Distribution::rank(reference.upper_bound());
                                (reference, rank)
                            })
                        });
                        match (left, right) {
                            (None, None) => references,
                            (Some(left), None) => concat_nonempty(vec![left, references])?,
                            (None, Some(right)) => concat_nonempty(vec![references, right])?,
                            (Some(left), Some(right)) => {
                                concat_nonempty(vec![left, references, right])?
                            }
                        }
                    }
                    None => references,
                }
            };

            nodes = Node::join_with_rank(references, min_rank, storage).await?;
            if branch_stack.is_empty() && nodes.len() == 1 {
                break;
            }
            min_rank += 1;
        }
        Ok(nodes.head.0)
    }

    /// Returns an async stream over entries with keys within the provided range.
    pub fn get_range<'a, R, Storage>(
        &'a self,
        range: R,
        storage: &'a Storage,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, XProllyTreeError>> + 'a
    where
        R: RangeBounds<Key> + 'a,
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        let get_child_index_by_key =
            async |node: &Self,
                   key: Option<&Key>,
                   storage: &Storage|
                   -> Result<Option<(Self, usize)>, XProllyTreeError> {
                match key {
                    Some(key) => {
                        for (index, reference) in node.block.references()?.iter().enumerate() {
                            if *key <= *reference.upper_bound() {
                                return Ok(Some((
                                    Node::from_reference(reference.to_owned(), storage).await?,
                                    index,
                                )));
                            }
                        }
                        Ok(None)
                    }
                    // If no key provided, this was an unbounded range request; take
                    // the left-most child.
                    None => Ok(Some((
                        Node::from_reference(node.block.references()?.first().to_owned(), storage)
                            .await?,
                        0,
                    ))),
                }
            };

        // Get the start key. Included/Excluded ranges are identical here, the
        // check if key is in range is below, and this will at most read one
        // unnecessary segment iff `Bound::Excluded(K)` and `K` is a boundary
        // node.
        let start_key = match range.start_bound() {
            Bound::Included(start) => Some(start.clone()),
            Bound::Excluded(start) => Some(start.clone()),
            Bound::Unbounded => None,
        };
        // An entry was found matching the key range.
        let mut matching = false;

        // Track ancestor nodes and the index of the most recently visited child
        let mut branch_stack = vec![TreeLocation {
            node: self.to_owned(),
            index: None,
        }];

        try_stream! {
            loop {
                let Some(current) = branch_stack.last_mut() else {
                    return;
                };
                match current.node.is_branch() {
                    true => {
                        if !matching {
                            let Some((next_node, next_index)) = get_child_index_by_key(&current.node, start_key.as_ref(), storage).await? else {
                                // The start key is larger than any key stored in this tree.
                                return;
                            };
                            current.index = Some(next_index);
                            branch_stack.push(TreeLocation { node: next_node, index: None });
                        } else {
                            let next_index = match current.index {
                                Some(visited_index) => visited_index + 1,
                                None => 0
                            };
                            match current.node.block.references()?.get(next_index) {
                                Some(reference) => {
                                    let next_node = Node::from_reference(reference.to_owned(), storage).await?;
                                    current.index = Some(next_index);
                                    branch_stack.push(TreeLocation { node: next_node, index: None });
                                }
                                None => {
                                    // Parent needs to check next sibling
                                    branch_stack.pop();
                                }
                            }
                        }
                    }
                    false => {
                        let current = branch_stack.pop().ok_or_else(|| XProllyTreeError::UnexpectedTreeShape("Encountered segment with no ancestors".into()))?;
                        for entry in current.node.into_entries()? {
                            let entry_key = &entry.key;
                            if range.contains(&entry_key) {
                                if !matching {
                                    matching = true;
                                }
                                yield entry;
                            } else if matching {
                                // We've surpassed the range; abort.
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    /// Returns the decoded child [`Node`] that may contain `key` within its
    /// descendants.
    ///
    /// The result is an error if this is a branch [`Node`].
    async fn child_by_key<Storage>(
        &self,
        key: &Key,
        storage: &Storage,
    ) -> Result<Option<Self>, XProllyTreeError>
    where
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        if !self.is_branch() {
            return Err(XProllyTreeError::IncorrectTreeAccess(
                "Cannot descend through segment".into(),
            ));
        }
        for reference in self.block.references()? {
            if *key <= *reference.upper_bound() {
                return Ok(Some(
                    Node::from_reference(reference.to_owned(), storage).await?,
                ));
            }
        }
        Ok(None)
    }

    /// Returns this segment's [`Entry`] matching the provided `key`.
    ///
    /// The result is an error if this is not a segment [`Node`].
    fn entry_by_key(&self, key: &Key) -> Result<Option<Entry<Key, Value>>, XProllyTreeError> {
        if !self.is_segment() {
            return Err(XProllyTreeError::IncorrectTreeAccess(
                "Cannot read entries from a branch".into(),
            ));
        }
        for entry in self.block.entries()? {
            if *key == entry.key {
                return Ok(Some(entry.to_owned()));
            }
        }
        Ok(None)
    }

    /// Joins a collection of sibling [`Adopter`]s into one or more parent
    /// [`Node`]s, where branching is determined by rank.
    pub(crate) async fn join_with_rank<Adopter, Storage>(
        nodes: NonEmpty<(Adopter, Rank)>,
        minimum_rank: Rank,
        storage: &mut Storage,
    ) -> Result<NonEmpty<(Self, Rank)>, XProllyTreeError>
    where
        Adopter: crate::Adopter<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>,
        Storage: ContentAddressedStorage<
                HASH_SIZE,
                Block = Block<HASH_SIZE, Key, Value, Hash>,
                Hash = Hash,
            >,
    {
        let mut output = vec![];
        let mut pending = vec![];
        for (node, rank) in nodes {
            pending.push(node);
            if rank > minimum_rank {
                let children = NonEmpty::from_vec(std::mem::replace(&mut pending, vec![])).ok_or(
                    XProllyTreeError::InvalidConstruction(
                        "Cannot adopt an empty child list".into(),
                    ),
                )?;
                let node = Adopter::adopt(children, storage).await?;
                output.push((node, rank));
            }
        }
        if let Some(pending) = NonEmpty::from_vec(pending) {
            let node = Adopter::adopt(pending, storage).await?;
            output.push((node, minimum_rank));
        }
        NonEmpty::from_vec(output).ok_or(XProllyTreeError::InvalidConstruction(
            "Empty node list".into(),
        ))
    }
}

struct TreeLocation<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    pub node: Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>,
    pub index: Option<usize>,
}

/// TODO: Improve. Possibly remove NonEmpty as it introduces some overhead
/// compared to index comparison with slices.
fn concat_nonempty<T>(list: Vec<NonEmpty<T>>) -> Result<NonEmpty<T>, XProllyTreeError> {
    Ok(NonEmpty::flatten(NonEmpty::from_vec(list).ok_or(
        XProllyTreeError::InvalidConstruction("Empty child list".into()),
    )?))
}
