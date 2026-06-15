use std::collections::HashMap;
use std::marker::PhantomData;

use dialog_common::{Blake3Hash, ConditionalSend, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
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
    Accessor, ArchivedNodeBody, Buffer, Cache, ContentAddressedStorage, Delta,
    DialogSearchTreeError, Distribution, Entry, Geometric, Key, Link, Node, NodeBody, Rank,
    SymmetryWith, Value, into_owned,
};

use super::{NodeEdit, TransientBody};

/// Rank threshold for the leaf level. The first index level uses
/// `BOTTOM_RANK + 1` and each level up adds one, matching the constants the
/// sequential [`crate::TreeShaper`] uses so persist groups children at exactly
/// the same boundaries.
const BOTTOM_RANK: Rank = 1;

/// A batch of edits applied to a tree as a single mutable session, in the style
/// of a Clojure transient.
///
/// Open one with [`Tree::transient`](crate::Tree::transient), apply any number
/// of [`insert`](Self::insert) / [`delete`](Self::delete) operations, then
/// [`persist`](Self::persist) to seal the result into a durable root and delta.
/// Only nodes the batch touched are copied and re-hashed; untouched subtrees
/// are carried by hash.
pub struct Transient<Key, Value, Backend, D = Geometric>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
{
    /// The root of the edit, or `None` for an empty tree.
    root: Option<NodeEdit<Key, Value>>,
    accessor: Accessor<Backend>,
    /// The tree's existing pending changes, carried forward so persisting the
    /// batch does not drop nodes that were unflushed before it.
    delta: Delta<Blake3Hash, Buffer>,
    distribution: PhantomData<D>,
}

impl<Key, Value, Backend, D> Transient<Key, Value, Backend, D>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
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
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSend,
    D: Distribution,
{
    /// Opens a transient over the tree rooted at `root`.
    pub async fn open(
        root: Blake3Hash,
        delta: Delta<Blake3Hash, Buffer>,
        cache: Cache<Blake3Hash, Buffer>,
        storage: ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError> {
        let accessor = Accessor::new(delta.branch(), cache, storage);
        let root = if &root == NULL_BLAKE3_HASH {
            None
        } else {
            let node: Node<Key, Value> = accessor.get_node(&root).await?;
            Some(NodeEdit::load(&node, node_upper_bound(&node)?)?)
        };

        Ok(Self {
            root,
            accessor,
            delta,
            distribution: PhantomData,
        })
    }

    /// Inserts or updates a single entry.
    pub async fn insert(&mut self, key: Key, value: Value) -> Result<(), DialogSearchTreeError> {
        let entry = Entry { key, value };
        match self.root.take() {
            None => {
                self.root = Some(NodeEdit::Transient {
                    upper_bound: entry.key.clone(),
                    body: TransientBody::Segment(vec![entry]),
                });
            }
            Some(mut root) => {
                self.descend(&mut root, &entry.key.clone(), Op::Upsert(entry))
                    .await?;
                self.root = Some(root);
            }
        }
        Ok(())
    }

    /// Removes a single entry. A missing key is a no-op.
    pub async fn delete(&mut self, key: &Key) -> Result<(), DialogSearchTreeError> {
        if let Some(mut root) = self.root.take() {
            self.descend(&mut root, key, Op::Remove).await?;
            self.root = Some(root);
        }
        Ok(())
    }

    /// Descends from `edit` to the segment covering `key`, lifting each node it
    /// passes to transient form, then applies `op` to that segment.
    ///
    /// Done in two phases so no borrow has to span an `await`: first lift the
    /// nodes along the path and record the child index taken at each level
    /// (reborrowing from `edit` each step), then follow the recorded indices to
    /// the segment and apply the op. The path length is the tree height, so the
    /// reborrow walk is cheap.
    async fn descend(
        &self,
        edit: &mut NodeEdit<Key, Value>,
        key: &Key,
        op: Op<Key, Value>,
    ) -> Result<(), DialogSearchTreeError> {
        let mut path = Vec::new();
        loop {
            let node = self.follow(edit, &path).await?;
            match node {
                NodeEdit::Transient {
                    body: TransientBody::Index(children),
                    ..
                } => {
                    let at = child_for::<Key, Value>(children, key);
                    self.lift(&mut children[at]).await?;
                    path.push(at);
                }
                NodeEdit::Transient { .. } => break,
                NodeEdit::Persistent(_) => unreachable!("follow lifts before returning"),
            }
        }

        // Apply at the segment, then refresh upper bounds back up the path.
        let leaf = self.follow(edit, &path).await?;
        if let NodeEdit::Transient {
            body: TransientBody::Segment(entries),
            ..
        } = leaf
        {
            apply_to_segment(entries, key, op);
        }
        for depth in (0..=path.len()).rev() {
            let node = self.follow(edit, &path[..depth]).await?;
            refresh_upper_bound(node);
        }
        Ok(())
    }

    /// Walks `edit` down the given child indices, lifting the node at the end of
    /// the path to transient form, and returns a mutable reference to it. Each index
    /// in `path` was produced by a prior descent step, so every node along the
    /// way is already transient.
    async fn follow<'a>(
        &self,
        edit: &'a mut NodeEdit<Key, Value>,
        path: &[usize],
    ) -> Result<&'a mut NodeEdit<Key, Value>, DialogSearchTreeError> {
        let mut node = edit;
        for &at in path {
            match node {
                NodeEdit::Transient {
                    body: TransientBody::Index(children),
                    ..
                } => node = &mut children[at],
                _ => unreachable!("path only descends through transient index nodes"),
            }
        }
        self.lift(node).await?;
        Ok(node)
    }

    /// Ensures `edit` is transient, loading it from storage if still persistent.
    async fn lift(&self, edit: &mut NodeEdit<Key, Value>) -> Result<(), DialogSearchTreeError> {
        if let NodeEdit::Persistent(link) = edit {
            let node: Node<Key, Value> = self.accessor.get_node(&link.node).await?;
            *edit = NodeEdit::load(&node, link.upper_bound.clone())?;
        }
        Ok(())
    }

    /// Seals the batch into a durable tree: the new root hash plus a delta of
    /// every node that changed. Untouched persistent subtrees pass through by
    /// hash and are neither decoded nor re-hashed.
    ///
    /// A canonical tree is balanced by rank, so every leaf is at the same
    /// height. [`seal`] walks the transient frontier bottom-up, grouping each level
    /// at `BOTTOM_RANK + level`. The top level may yield several nodes, so we
    /// fold up one level at a time until a single root remains, exactly as the
    /// sequential builder does.
    ///
    /// The returned delta starts from the tree's carried-forward pending
    /// changes, so nodes that were unflushed before the batch are preserved
    /// alongside the nodes this batch sealed.
    pub fn persist(self) -> Result<(Blake3Hash, Delta<Blake3Hash, Buffer>), DialogSearchTreeError> {
        let mut delta = self.delta;
        let root = match self.root {
            None => NULL_BLAKE3_HASH.clone(),
            Some(root) => {
                let mut context = RankContext::<Key, D>::new();
                let Sealed { level, mut links } = seal(root, &mut context, &mut delta)?;

                match level {
                    // The root was an untouched persistent node: its single link is
                    // already the durable root, returned verbatim with no
                    // regrouping.
                    None => links
                        .into_iter()
                        .next()
                        .map(|link| link.node)
                        .unwrap_or_else(|| NULL_BLAKE3_HASH.clone()),
                    // The root was transient: `seal` returned its children ungrouped
                    // at height `level`. Group them into a parent at least once
                    // (so a lone surviving segment still gains the index the
                    // sequential builder always wraps it in), then keep folding
                    // up one level at a time until a single root remains.
                    // `collapse_root` then strips any single-child index chain
                    // the fold left over an index, while keeping the legitimate
                    // index over a lone segment.
                    Some(level) => {
                        let mut level = level + 1;
                        loop {
                            links = group_links::<Key, Value, D>(
                                links,
                                BOTTOM_RANK + level,
                                &mut context,
                                &mut delta,
                            )?;
                            if links.len() <= 1 {
                                break;
                            }
                            level += 1;
                        }
                        let root = links
                            .into_iter()
                            .next()
                            .map(|link| link.node)
                            .unwrap_or_else(|| NULL_BLAKE3_HASH.clone());
                        collapse_root::<Key, Value>(root, &mut delta)?
                    }
                }
            }
        };
        Ok((root, delta))
    }
}

/// Strips a non-canonical chain of single-child index nodes from the root.
///
/// A canonical tree never has an index root whose only child is another index;
/// the only legitimate single-child root is an index over a lone segment.
/// Sealing a frontier whose upper levels were hollowed out by deletes can leave
/// such wrappers, so descend through them, dropping each from the delta, until
/// the root is a multi-child index or sits directly over a segment. The nodes
/// were just written to `delta`, so they are read back from there without
/// touching storage.
fn collapse_root<Key, Value>(
    mut root: Blake3Hash,
    delta: &mut Delta<Blake3Hash, Buffer>,
) -> Result<Blake3Hash, DialogSearchTreeError>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    if &root == NULL_BLAKE3_HASH {
        return Ok(root);
    }

    loop {
        let Some(buffer) = delta.get(&root) else {
            break;
        };
        let node: Node<Key, Value> = Node::new(buffer);
        let child_hash = match node.body()? {
            ArchivedNodeBody::Index(index) if index.links.len() == 1 => {
                <&Blake3Hash>::from(&index.links[0].node).clone()
            }
            _ => break,
        };

        let Some(child_buffer) = delta.get(&child_hash) else {
            break;
        };
        let child: Node<Key, Value> = Node::new(child_buffer);
        match child.body()? {
            ArchivedNodeBody::Index(_) => {
                // The single-child wrapper is unreachable in the canonical tree.
                delta.remove(&root);
                root = child_hash;
            }
            ArchivedNodeBody::Segment(_) => break,
        }
    }

    Ok(root)
}

/// A pending change to a single key.
enum Op<Key, Value> {
    Upsert(Entry<Key, Value>),
    Remove,
}

/// Applies one op to a sorted segment. `key` is the operation's key (used by
/// `Remove`; for `Upsert` it equals the entry's key).
fn apply_to_segment<Key, Value>(entries: &mut Vec<Entry<Key, Value>>, key: &Key, op: Op<Key, Value>)
where
    Key: Ord,
{
    match op {
        Op::Upsert(entry) => match entries.binary_search_by(|e| e.key.cmp(&entry.key)) {
            Ok(at) => entries[at].value = entry.value,
            Err(at) => entries.insert(at, entry),
        },
        Op::Remove => {
            if let Ok(at) = entries.binary_search_by(|e| e.key.cmp(key)) {
                entries.remove(at);
            }
        }
    }
}

/// Index of the child whose subtree covers `key`: the first child whose upper
/// bound is `>= key`, or the last child when the key exceeds every bound.
fn child_for<Key, Value>(children: &[NodeEdit<Key, Value>], key: &Key) -> usize
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    children
        .partition_point(|child| child.upper_bound() < key)
        .min(children.len().saturating_sub(1))
}

/// Recomputes an transient node's cached upper bound from its current contents.
fn refresh_upper_bound<Key, Value>(edit: &mut NodeEdit<Key, Value>)
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    if let NodeEdit::Transient { upper_bound, body } = edit {
        let bound = match body {
            TransientBody::Segment(entries) => entries.last().map(|e| e.key.clone()),
            TransientBody::Index(children) => children.last().map(|c| c.upper_bound().clone()),
        };
        if let Some(bound) = bound {
            *upper_bound = bound;
        }
    }
}

/// The upper bound key of a (loaded) node.
fn node_upper_bound<Key, Value>(node: &Node<Key, Value>) -> Result<Key, DialogSearchTreeError>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    node.body()?
        .upper_bound()
        .and_then(into_owned)
        .map_err(|_| DialogSearchTreeError::Node("node had no upper bound".into()))
}

/// A per-persist rank cache, so each boundary key is hashed once. Mirrors the
/// cache in the sequential mutation context.
struct RankContext<Key, D> {
    cache: HashMap<Key, Rank>,
    distribution: PhantomData<D>,
}

impl<Key, D> RankContext<Key, D>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    D: Distribution,
{
    fn new() -> Self {
        Self {
            cache: HashMap::new(),
            distribution: PhantomData,
        }
    }

    fn rank(&mut self, key: &Key) -> Rank {
        *self
            .cache
            .entry(key.clone())
            .or_insert_with(|| D::rank(key.as_ref()))
    }
}

/// The sealed contribution of one node edit, expressed as the ordered run of
/// child links that node hands to its parent.
///
/// Grouping a run of children into nodes always happens one level up, at the
/// point a parent flushes the run, so this carries the run *ungrouped*. That
/// is what lets adjacent transient siblings whose separating boundary a delete
/// dissolved fuse back together: their child runs are concatenated and grouped
/// as one, exactly as a from-scratch build would group the surviving keys.
///
/// `level` is the height of the children in `links` (0 for leaf segments,
/// `child_height + 1` for index children), or `None` for a pass-through
/// persistent subtree, whose link is already a sealed canonical unit and whose
/// height a parent does not need to inspect.
struct Sealed<Key> {
    level: Option<Rank>,
    links: Vec<Link<Key>>,
}

/// Seals one node edit bottom-up, returning the ungrouped run of child links it
/// contributes to its parent and writing every new node to `delta`.
///
/// A persistent node passes its own link through untouched. An transient segment
/// turns into the run of its entries' eventual leaf nodes, but those leaves are
/// only materialized when a parent flushes the surrounding transient run, so an
/// transient segment defers to [`merge_children`] via its parent. An transient index
/// merges its children into a single canonical child-link run (fusing adjacent
/// transient runs around any persistent boundaries) and reports that run.
///
/// The grouping that does happen here uses the sequential builder's `collect`
/// rule, so the result is the canonical tree.
fn seal<Key, Value, D>(
    edit: NodeEdit<Key, Value>,
    context: &mut RankContext<Key, D>,
    delta: &mut Delta<Blake3Hash, Buffer>,
) -> Result<Sealed<Key>, DialogSearchTreeError>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
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
    D: Distribution,
{
    match edit {
        NodeEdit::Persistent(link) => Ok(Sealed {
            level: None,
            links: vec![link],
        }),
        NodeEdit::Transient { body, .. } => match body {
            TransientBody::Segment(entries) => Ok(Sealed {
                level: Some(0),
                links: group_entries(entries, BOTTOM_RANK, context, delta)?,
            }),
            TransientBody::Index(children) => {
                let (level, links) = merge_children::<Key, Value, D>(children, context, delta)?;
                Ok(Sealed {
                    level: Some(level),
                    links,
                })
            }
        },
    }
}

/// Reduces an transient index's children to the canonical, ungrouped run of child
/// links it contributes to its parent.
///
/// A persistent child is a sealed canonical unit whose upper bound was a rank
/// boundary in the durable tree, so it never fuses with a neighbor: it both
/// terminates the transient run to its left and starts a fresh run to its right.
/// Each maximal run of adjacent transient children is therefore fused as one: at
/// the leaf level their entries concatenate and group at [`BOTTOM_RANK`]; above
/// it their child links concatenate and group at `BOTTOM_RANK + child_level`.
/// Concatenating the per-run results in child order yields the index's child
/// links, which the parent (or [`Transient::persist`]) groups into nodes.
fn merge_children<Key, Value, D>(
    children: Vec<NodeEdit<Key, Value>>,
    context: &mut RankContext<Key, D>,
    delta: &mut Delta<Blake3Hash, Buffer>,
) -> Result<(Rank, Vec<Link<Key>>), DialogSearchTreeError>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
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
    D: Distribution,
{
    // The child level, learned from the first transient child. A touched index
    // always has at least one transient child, so this is always set; it defaults
    // to the leaf level defensively.
    let mut child_level: Option<Rank> = None;
    // Output child links in order: persistent links pass through inline; each
    // transient run is grouped and spliced in when the run ends.
    let mut links: Vec<Link<Key>> = Vec::new();
    // The unflushed contents of the current transient run, at `child_level`.
    let mut pending = TransientRun::Empty;

    for child in children {
        match child {
            NodeEdit::Persistent(link) => {
                let run = std::mem::replace(&mut pending, TransientRun::Empty);
                links.extend(run.group(context, delta)?);
                links.push(link);
            }
            NodeEdit::Transient { body, .. } => match body {
                TransientBody::Segment(entries) => {
                    // Segment children are the leaf level (height 0).
                    child_level = Some(0);
                    pending.push_entries(entries);
                }
                TransientBody::Index(grandchildren) => {
                    let (grandchild_level, child_links) =
                        merge_children::<Key, Value, D>(grandchildren, context, delta)?;
                    child_level = Some(child_level.map_or(grandchild_level + 1, |level| {
                        level.max(grandchild_level + 1)
                    }));
                    pending.push_links(grandchild_level, child_links);
                }
            },
        }
    }

    let run = std::mem::replace(&mut pending, TransientRun::Empty);
    links.extend(run.group(context, delta)?);

    Ok((child_level.unwrap_or(0), links))
}

/// The unflushed contents of a run of adjacent transient children, kept ungrouped
/// so the whole run groups at once when a persistent boundary or the end of the
/// child list closes it.
enum TransientRun<Key, Value> {
    /// No transient children have accumulated yet.
    Empty,
    /// Leaf-level entries from a run of transient segments.
    Entries(Vec<Entry<Key, Value>>),
    /// Child links from a run of transient indices, with the height of those links.
    Links(Rank, Vec<Link<Key>>),
}

impl<Key, Value> TransientRun<Key, Value> {
    /// Appends an transient segment's entries to the run.
    fn push_entries(&mut self, entries: Vec<Entry<Key, Value>>) {
        match self {
            TransientRun::Empty => *self = TransientRun::Entries(entries),
            TransientRun::Entries(existing) => existing.extend(entries),
            TransientRun::Links(..) => {
                unreachable!("a node's children are all the same height")
            }
        }
    }

    /// Appends an transient index's child links (at height `level`) to the run.
    fn push_links(&mut self, level: Rank, child_links: Vec<Link<Key>>) {
        match self {
            TransientRun::Empty => *self = TransientRun::Links(level, child_links),
            TransientRun::Links(_, existing) => existing.extend(child_links),
            TransientRun::Entries(_) => {
                unreachable!("a node's children are all the same height")
            }
        }
    }

    /// Groups the run into nodes with the canonical cut rule and returns their
    /// links. Leaf entries group at [`BOTTOM_RANK`]; a run of child links at
    /// height `level` groups one level higher. An empty run yields no links.
    fn group<D>(
        self,
        context: &mut RankContext<Key, D>,
        delta: &mut Delta<Blake3Hash, Buffer>,
    ) -> Result<Vec<Link<Key>>, DialogSearchTreeError>
    where
        Key: self::Key + Clone,
        Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
        Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
        Key: for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
        Value: self::Value,
        Value: for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
        D: Distribution,
    {
        match self {
            TransientRun::Empty => Ok(Vec::new()),
            TransientRun::Entries(entries) => {
                group_entries::<Key, Value, D>(entries, BOTTOM_RANK, context, delta)
            }
            TransientRun::Links(level, child_links) => {
                group_links::<Key, Value, D>(child_links, BOTTOM_RANK + level + 1, context, delta)
            }
        }
    }
}

/// Groups a sorted entry list into leaf segments at `threshold`, writing each
/// to `delta` and returning their links. Cuts after every entry whose rank
/// exceeds `threshold`, with the trailing run forming a final segment.
fn group_entries<Key, Value, D>(
    entries: Vec<Entry<Key, Value>>,
    threshold: Rank,
    context: &mut RankContext<Key, D>,
    delta: &mut Delta<Blake3Hash, Buffer>,
) -> Result<Vec<Link<Key>>, DialogSearchTreeError>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    let mut links = Vec::new();
    let mut pending: Vec<Entry<Key, Value>> = Vec::new();
    for entry in entries {
        let rank = context.rank(&entry.key);
        pending.push(entry);
        if rank > threshold {
            links.push(seal_segment(std::mem::take(&mut pending), delta)?);
        }
    }
    if !pending.is_empty() {
        links.push(seal_segment(pending, delta)?);
    }
    Ok(links)
}

/// Groups a sorted link list into index nodes at `threshold`. Same cut rule as
/// [`group_entries`].
fn group_links<Key, Value, D>(
    children: Vec<Link<Key>>,
    threshold: Rank,
    context: &mut RankContext<Key, D>,
    delta: &mut Delta<Blake3Hash, Buffer>,
) -> Result<Vec<Link<Key>>, DialogSearchTreeError>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    let mut links = Vec::new();
    let mut pending: Vec<Link<Key>> = Vec::new();
    for child in children {
        let rank = context.rank(&child.upper_bound);
        pending.push(child);
        if rank > threshold {
            links.push(seal_index::<Key, Value>(
                std::mem::take(&mut pending),
                delta,
            )?);
        }
    }
    if !pending.is_empty() {
        links.push(seal_index::<Key, Value>(pending, delta)?);
    }
    Ok(links)
}

/// Serializes a leaf segment, writes it to `delta`, and returns its link.
fn seal_segment<Key, Value>(
    entries: Vec<Entry<Key, Value>>,
    delta: &mut Delta<Blake3Hash, Buffer>,
) -> Result<Link<Key>, DialogSearchTreeError>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
{
    let upper_bound = entries
        .last()
        .map(|e| e.key.clone())
        .ok_or_else(|| DialogSearchTreeError::Node("empty segment".into()))?;
    let body: NodeBody<Key, Value> = entries.try_into()?;
    seal_body(body, upper_bound, delta)
}

/// Serializes an index node, writes it to `delta`, and returns its link.
fn seal_index<Key, Value>(
    children: Vec<Link<Key>>,
    delta: &mut Delta<Blake3Hash, Buffer>,
) -> Result<Link<Key>, DialogSearchTreeError>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
{
    let upper_bound = children
        .last()
        .map(|c| c.upper_bound.clone())
        .ok_or_else(|| DialogSearchTreeError::Node("empty index".into()))?;
    let body: NodeBody<Key, Value> = children.try_into()?;
    seal_body(body, upper_bound, delta)
}

/// Serializes a node body, stores its buffer in `delta` keyed by hash, and
/// returns the link (hash + upper bound) referring to it.
fn seal_body<Key, Value>(
    body: NodeBody<Key, Value>,
    upper_bound: Key,
    delta: &mut Delta<Blake3Hash, Buffer>,
) -> Result<Link<Key>, DialogSearchTreeError>
where
    Key: self::Key + Clone,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
{
    let buffer = Buffer::from(body.as_bytes()?);
    let hash = buffer.blake3_hash().clone();
    delta.add(hash.clone(), buffer);
    Ok(Link {
        upper_bound,
        node: hash,
    })
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use dialog_storage::MemoryStorageBackend;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use crate::{ContentAddressedStorage, Delta, Tree};

    type TestTree = Tree<[u8; 4], Vec<u8>>;
    type TestStorage = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Inserts `entries` one at a time, then flushes the resulting tree to
    /// storage and returns it.
    async fn build_sequentially(
        entries: &[(u32, Vec<u8>)],
        storage: &mut TestStorage,
    ) -> Result<TestTree> {
        let mut tree = TestTree::empty();
        for (key, value) in entries {
            tree = tree
                .insert(key.to_le_bytes(), value.clone(), storage)
                .await?;
        }
        flush(&mut tree, storage).await?;
        Ok(tree)
    }

    /// Flushes a tree's pending nodes into storage.
    async fn flush(tree: &mut TestTree, storage: &mut TestStorage) -> Result<()> {
        for buffer in tree.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(())
    }

    /// A deterministic set of distinct `(key, value)` pairs.
    fn random_entries(count: usize, seed: u64) -> Vec<(u32, Vec<u8>)> {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut seen = std::collections::HashSet::new();
        let mut entries = Vec::with_capacity(count);
        while entries.len() < count {
            let key: u32 = rng.r#gen();
            if seen.insert(key) {
                entries.push((key, key.to_le_bytes().to_vec()));
            }
        }
        entries
    }

    #[dialog_common::test]
    async fn it_matches_sequential_inserts() -> Result<()> {
        let entries = random_entries(500, 0xA11CE);

        // Tree A: sequential inserts.
        let mut storage_a = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let tree_a = build_sequentially(&entries, &mut storage_a).await?;
        let root_a = tree_a.root().clone();

        // Tree B: one transient batch over an empty tree.
        let mut storage_b = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let base = TestTree::empty();
        let mut transient = base.transient(&storage_b).await?;
        for (key, value) in &entries {
            transient.insert(key.to_le_bytes(), value.clone()).await?;
        }
        let (root_b, mut delta) = transient.persist()?;
        for (_, buffer) in delta.flush() {
            storage_b
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        assert_eq!(
            root_a, root_b,
            "transient batch insert must produce the same root as sequential inserts"
        );
        Ok(())
    }

    #[dialog_common::test]
    async fn it_matches_sequential_with_deletes() -> Result<()> {
        let base_entries = random_entries(300, 0xBA5E);
        let new_entries = random_entries(50, 0x4EE);
        let to_delete: Vec<u32> = base_entries.iter().take(100).map(|(k, _)| *k).collect();

        // Sequential path: build base, then delete 100 and insert 50.
        let mut storage_seq = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree_seq = build_sequentially(&base_entries, &mut storage_seq).await?;
        for key in &to_delete {
            tree_seq = tree_seq.delete(&key.to_le_bytes(), &storage_seq).await?;
        }
        flush(&mut tree_seq, &mut storage_seq).await?;
        for (key, value) in &new_entries {
            tree_seq = tree_seq
                .insert(key.to_le_bytes(), value.clone(), &storage_seq)
                .await?;
        }
        flush(&mut tree_seq, &mut storage_seq).await?;
        let root_seq = tree_seq.root().clone();

        // Transient path: build the same base, then batch the same edits.
        let mut storage_batch = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let base = build_sequentially(&base_entries, &mut storage_batch).await?;
        let mut transient = base.transient(&storage_batch).await?;
        for key in &to_delete {
            transient.delete(&key.to_le_bytes()).await?;
        }
        for (key, value) in &new_entries {
            transient.insert(key.to_le_bytes(), value.clone()).await?;
        }
        let (root_batch, mut delta) = transient.persist()?;
        for (_, buffer) in delta.flush() {
            storage_batch
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        assert_eq!(
            root_seq, root_batch,
            "transient batch of deletes and inserts must match sequential edits"
        );
        Ok(())
    }

    /// Deleting most of a tree forces the structural cases the sequential path
    /// handles specially: segments emptying, index nodes losing all but one
    /// child, and the root collapsing. The batch must still land on the same
    /// canonical root, including when it shrinks back to a single segment and
    /// when it is emptied entirely.
    #[dialog_common::test]
    async fn it_matches_sequential_when_deletes_collapse_the_tree() -> Result<()> {
        let base_entries = random_entries(400, 0xC011A95E);
        // Keep only five keys; delete the rest.
        let survivors: Vec<u32> = base_entries.iter().take(5).map(|(k, _)| *k).collect();
        let to_delete: Vec<u32> = base_entries.iter().skip(5).map(|(k, _)| *k).collect();

        let mut storage_seq = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree_seq = build_sequentially(&base_entries, &mut storage_seq).await?;
        for key in &to_delete {
            tree_seq = tree_seq.delete(&key.to_le_bytes(), &storage_seq).await?;
        }
        flush(&mut tree_seq, &mut storage_seq).await?;
        let root_seq = tree_seq.root().clone();

        let mut storage_batch = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let base = build_sequentially(&base_entries, &mut storage_batch).await?;
        let mut transient = base.transient(&storage_batch).await?;
        for key in &to_delete {
            transient.delete(&key.to_le_bytes()).await?;
        }
        let (root_batch, mut delta) = transient.persist()?;
        for (_, buffer) in delta.flush() {
            storage_batch
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        assert_eq!(
            root_seq, root_batch,
            "collapsing deletes must reach the same canonical root"
        );

        // Now empty the tree on both sides and confirm they agree (null root).
        for key in &survivors {
            tree_seq = tree_seq.delete(&key.to_le_bytes(), &storage_seq).await?;
        }

        let batch_tree = base.committed(root_batch, Delta::zero());
        let mut empty_transient = batch_tree.transient(&storage_batch).await?;
        for key in &survivors {
            empty_transient.delete(&key.to_le_bytes()).await?;
        }
        let (empty_root, _) = empty_transient.persist()?;

        assert_eq!(
            tree_seq.root().clone(),
            empty_root,
            "emptying the tree must reach the null root on both paths"
        );
        Ok(())
    }

    /// Property: across many seeds and batch shapes, a transient batch of mixed
    /// inserts and deletes over a random base reaches the exact same canonical
    /// root as applying the same operations one at a time. This is the broad
    /// guard for the persist grouping (segment re-fusion across deleted
    /// boundaries, level derivation, root collapse).
    #[dialog_common::test]
    async fn it_matches_sequential_for_random_mixed_batches() -> Result<()> {
        for seed in 0..40u64 {
            let mut rng = StdRng::seed_from_u64(seed.wrapping_mul(0x9E37_79B9));
            let base_size = (rng.r#gen::<usize>() % 400) + 1;
            let base = random_entries(base_size, seed ^ 0xBA5E);

            // A batch: delete a random subset of existing keys, insert a random
            // set of fresh ones.
            let delete_count = rng.r#gen::<usize>() % (base_size + 1);
            let deletes: Vec<u32> = base.iter().take(delete_count).map(|(k, _)| *k).collect();
            let inserts = random_entries(rng.r#gen::<usize>() % 200, seed ^ 0x1452_E27F);

            // Sequential reference.
            let mut storage_seq = ContentAddressedStorage::new(MemoryStorageBackend::default());
            let mut tree_seq = build_sequentially(&base, &mut storage_seq).await?;
            for key in &deletes {
                tree_seq = tree_seq.delete(&key.to_le_bytes(), &storage_seq).await?;
            }
            for (key, value) in &inserts {
                tree_seq = tree_seq
                    .insert(key.to_le_bytes(), value.clone(), &storage_seq)
                    .await?;
            }
            flush(&mut tree_seq, &mut storage_seq).await?;

            // Transient batch.
            let mut storage_batch = ContentAddressedStorage::new(MemoryStorageBackend::default());
            let base_tree = build_sequentially(&base, &mut storage_batch).await?;
            let mut transient = base_tree.transient(&storage_batch).await?;
            for key in &deletes {
                transient.delete(&key.to_le_bytes()).await?;
            }
            for (key, value) in &inserts {
                transient.insert(key.to_le_bytes(), value.clone()).await?;
            }
            let (root_batch, _) = transient.persist()?;

            assert_eq!(
                tree_seq.root().clone(),
                root_batch,
                "seed {seed}: base={base_size} del={delete_count} ins={} diverged",
                inserts.len(),
            );
        }
        Ok(())
    }
}
