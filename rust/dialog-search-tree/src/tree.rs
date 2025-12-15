use std::marker::PhantomData;

use dialog_common::{Blake3Hash, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use nonempty::NonEmpty;
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
    ArchivedNodeBody, Buffer, Cache, ContentAddressedStorage, Delta, DialogSearchTreeError, Entry,
    Key, Link, Node, NodeBody, Rank, Segment, Value, distribution, into_owned,
};

pub struct Tree<Key, Value>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
    Value: self::Value,
{
    key: PhantomData<Key>,
    value: PhantomData<Value>,

    root: Blake3Hash,
    node_cache: Cache<Blake3Hash, Buffer>,

    delta: Delta<Blake3Hash, Buffer>,
}

impl<Key, Value> Tree<Key, Value>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
    Key::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Key::Archived: Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
{
    pub fn root(&self) -> &Blake3Hash {
        &self.root
    }

    pub fn empty() -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            root: NULL_BLAKE3_HASH.clone(),
            node_cache: Cache::new(),
            delta: Delta::zero(),
        }
    }

    pub async fn get<Backend>(
        &self,
        key: &Key,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Option<Value>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        if let Some(result) = self.search(key, storage).await? {
            if let Some(entry) = result.leaf.body()?.find_entry(key)? {
                into_owned(&entry.value).map(|value| Some(value))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn insert<Backend>(
        &self,
        key: Key,
        value: Value,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        let (next_entries, search_result) = if let Some(search_result) =
            self.search(&key, storage).await?
        {
            // Subtract nodes in the search path from the delta; these nodes are
            // destined to be superseded due to hashes changing all the way up
            // the path.
            // delta.subtract(search_result.path.iter().map(|layer| &layer.host));

            let segment = search_result.leaf.as_segment()?;
            let mut entries = into_owned::<Segment<Key, Value>>(segment)?.entries;

            match entries.binary_search_by(|entry| entry.key.cmp(&key)) {
                Ok(index) => {
                    let Some(previous_entry) = entries.get_mut(index) else {
                        return Err(DialogSearchTreeError::Access(format!(
                            "Entry at index {} not found",
                            index,
                        )));
                    };
                    previous_entry.value = value;
                }
                Err(index) => {
                    entries.insert(index, Entry { key, value });
                }
            }

            (
                entries
                    .into_iter()
                    .map(|entry| {
                        let rank =
                            distribution::geometric::rank(&Blake3Hash::hash(entry.key.as_ref()));
                        (entry, rank)
                    })
                    .collect::<Vec<_>>(),
                Some(search_result),
            )
        } else {
            // Empty tree, create from scratch
            let rank = distribution::geometric::rank(&Blake3Hash::hash(key.as_ref()));
            (vec![(Entry { key, value }, rank)], None)
        };

        let Some(next_entries) = NonEmpty::from_vec(next_entries) else {
            return Err(DialogSearchTreeError::Operation(
                "Insertion resulted in empty set of entries".into(),
            ));
        };

        let (next_root, delta) = self.distribute(next_entries, search_result)?;

        Ok(self.advance(next_root, delta))
    }

    pub fn flush(&mut self) -> impl Iterator<Item = (Blake3Hash, Buffer)> {
        self.delta.flush()
    }

    fn advance(&self, root: Blake3Hash, delta: Delta<Blake3Hash, Buffer>) -> Self {
        Tree {
            key: PhantomData,
            value: PhantomData,
            root,
            node_cache: self.node_cache.clone(),
            delta,
        }
    }

    async fn get_node<Backend>(
        &self,
        hash: &Blake3Hash,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Node<Key, Value>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        self.node_cache
            .get_or_fetch(hash, async move |key| {
                println!("Looking up {}", key);
                if let Some(buffer) = self.delta.get(hash) {
                    println!("Found in delta!");
                    Ok(Some(buffer))
                } else {
                    println!("Not found in delta..");
                    storage
                        .retrieve(key)
                        .await
                        .map(|maybe_bytes| maybe_bytes.map(|bytes| Buffer::from(bytes)))
                }
            })
            .await?
            .ok_or_else(|| {
                DialogSearchTreeError::Node(format!("Blob not found in storage: {}", hash))
            })
            .and_then(|buffer| Ok(Node::new(buffer)))
    }

    async fn search<Backend>(
        &self,
        key: &Key,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Option<SearchResult<Key, Value>>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        SearchResult::derive(&self.root, key, async |hash| {
            self.get_node(hash, storage).await
        })
        .await
    }

    fn distribute<Child>(
        &self,
        children: NonEmpty<(Child, Rank)>,
        mut search_result: Option<SearchResult<Key, Value>>,
    ) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError>
    where
        NodeBody<Key, Value>: TryFrom<Vec<Child>, Error = DialogSearchTreeError>,
    {
        const MINIMUM_RANK: u32 = 2;

        let mut minimum_rank = MINIMUM_RANK;
        let mut delta = self.delta.branch();
        let mut nodes = Tree::collect(children, 1)?;

        let mut search_path = if let Some(search_result) = search_result {
            delta.subtract(search_result.leaf.hash());
            search_result.path
        } else {
            vec![]
        };

        loop {
            let links = {
                delta.add_all(
                    nodes
                        .iter()
                        .map(|(node, _)| (node.hash().clone(), node.buffer().clone())),
                );

                let ranked_links = nodes
                    .into_iter()
                    .map(|(node, rank)| node.to_link().map(|link| (link, rank)))
                    .collect::<Result<Vec<(Link<Key>, Rank)>, DialogSearchTreeError>>()
                    .and_then(|links| {
                        NonEmpty::from_vec(links)
                            .ok_or_else(|| DialogSearchTreeError::Node("Empty child list".into()))
                    })?;

                match search_path.pop() {
                    Some(layer) => {
                        delta.subtract(&layer.host);
                        // TBD if we must recompute rank for siblings references
                        // when building up the tree. Attempt to try setting
                        // rank to `0` for references outside of the modified
                        // path.
                        let ranked_left_siblings = layer.left_siblings.map(into_ranked_links);
                        let ranked_right_siblings = layer.right_siblings.map(into_ranked_links);

                        match (ranked_left_siblings, ranked_right_siblings) {
                            (None, None) => ranked_links,
                            (Some(ranked_left_siblings), None) => {
                                concat_nonempty(vec![ranked_left_siblings, ranked_links])?
                            }
                            (None, Some(ranked_right_siblings)) => {
                                concat_nonempty(vec![ranked_links, ranked_right_siblings])?
                            }
                            (Some(ranked_left_siblings), Some(ranked_right_siblings)) => {
                                concat_nonempty(vec![
                                    ranked_left_siblings,
                                    ranked_links,
                                    ranked_right_siblings,
                                ])?
                            }
                        }
                    }
                    None => ranked_links,
                }
            };

            nodes = Tree::collect::<Link<_>>(links, minimum_rank)?;

            if search_path.is_empty() && nodes.len() == 1 {
                break;
            }

            minimum_rank += 1;
        }

        delta.add_all(
            nodes
                .iter()
                .map(|(node, _)| (node.hash().clone(), node.buffer().clone())),
        );

        Ok((nodes.head.0.hash().to_owned(), delta))
    }

    fn collect<Child>(
        children: NonEmpty<(Child, Rank)>,
        minimum_rank: Rank,
    ) -> Result<NonEmpty<(Node<Key, Value>, Rank)>, DialogSearchTreeError>
    where
        NodeBody<Key, Value>: TryFrom<Vec<Child>, Error = DialogSearchTreeError>,
    {
        let mut output: Vec<(Node<Key, Value>, u32)> = vec![];
        let mut pending = vec![];

        for (child, rank) in children {
            pending.push(child);
            if rank > minimum_rank {
                if pending.len() == 0 {
                    return Err(DialogSearchTreeError::Node(
                        "Attempted to collect empty child list into index node".into(),
                    ));
                }
                let node = Node::new(Buffer::from(
                    NodeBody::try_from(std::mem::take(&mut pending))?.as_bytes()?,
                ));

                output.push((node, rank));
            }
        }

        if pending.len() > 0 {
            let node = Node::new(Buffer::from(NodeBody::try_from(pending)?.as_bytes()?));
            output.push((node, minimum_rank));
        }

        NonEmpty::from_vec(output).ok_or_else(|| {
            DialogSearchTreeError::Node("Node list was empty after collection".into())
        })
    }
}

fn into_ranked_links<Key>(links: NonEmpty<Link<Key>>) -> NonEmpty<(Link<Key>, Rank)> {
    links.map(|link| {
        let rank = distribution::geometric::rank(&link.node);
        (link, rank)
    })
}

/// TODO: Improve. Possibly remove NonEmpty as it introduces some overhead
/// compared to index comparison with slices.
fn concat_nonempty<T>(list: Vec<NonEmpty<T>>) -> Result<NonEmpty<T>, DialogSearchTreeError> {
    Ok(NonEmpty::flatten(NonEmpty::from_vec(list).ok_or(
        DialogSearchTreeError::Node("Empty child list".into()),
    )?))
}

struct TreeLayer<Key>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
{
    pub host: Blake3Hash,
    pub left_siblings: Option<NonEmpty<Link<Key>>>,
    pub right_siblings: Option<NonEmpty<Link<Key>>>,
}

pub type SearchPath<Key> = Vec<TreeLayer<Key>>;

struct SearchResult<Key, Value>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    pub leaf: Node<Key, Value>,
    pub path: SearchPath<Key>,
}

impl<Key, Value> SearchResult<Key, Value>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key>,
    Key::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Key::Archived: Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
        Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    async fn derive<GetNode>(
        root: &Blake3Hash,
        key: &Key,
        get_node: GetNode,
    ) -> Result<Option<SearchResult<Key, Value>>, DialogSearchTreeError>
    where
        GetNode: AsyncFn(&Blake3Hash) -> Result<Node<Key, Value>, DialogSearchTreeError>,
    {
        if root == NULL_BLAKE3_HASH {
            return Ok(None);
        }

        // Depth scales logarithmically with number of entries, so 32 is truly
        // overkill here
        const MAXIMUM_TREE_DEPTH: usize = 32;

        let mut next_node = root.clone();
        let mut path = vec![];

        loop {
            if path.len() > MAXIMUM_TREE_DEPTH {
                return Err(DialogSearchTreeError::Operation(format!(
                    "Tree depth exceded the soft maximum ({MAXIMUM_TREE_DEPTH})"
                )));
            }

            let node = get_node(&next_node).await?;

            match node.body()? {
                ArchivedNodeBody::Index(index) => {
                    let mut left = vec![];
                    let mut right = vec![];
                    let mut next_descendant = None;

                    for link in index.links.iter() {
                        if next_descendant.is_some() {
                            right.push(link);
                        } else if key <= &link.upper_bound {
                            next_descendant = Some(&link.node);
                        } else {
                            left.push(link);
                        }
                    }

                    if next_descendant.is_none() {
                        let last_candidate = left.pop().ok_or(DialogSearchTreeError::Operation(
                            "No upper bound found".into(),
                        ))?;

                        next_descendant = Some(&last_candidate.node);
                    }

                    path.push(TreeLayer {
                        host: next_node,
                        left_siblings: NonEmpty::from_vec(
                            left.into_iter()
                                .map(into_owned)
                                .collect::<Result<_, DialogSearchTreeError>>()?,
                        ),
                        right_siblings: NonEmpty::from_vec(
                            right
                                .into_iter()
                                .map(into_owned)
                                .collect::<Result<_, DialogSearchTreeError>>()?,
                        ),
                    });

                    next_node = next_descendant
                        .ok_or_else(|| {
                            DialogSearchTreeError::Operation("Next node not found".into())
                        })
                        .and_then(|hash| into_owned(hash))?;
                }
                ArchivedNodeBody::Segment(_) => {
                    return Ok(Some(Self { leaf: node, path }));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    use crate::{ContentAddressedStorage, Tree};

    #[tokio::test]
    async fn it_creates_a_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        for i in 0..4096u32 {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        println!("{}", tree.root());

        let buffers = tree.flush().collect::<Vec<_>>();

        println!("Delta size: {}", buffers.len());

        for (key, value) in buffers {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        for i in 4096..4100u32 {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        Ok(())
    }
}
