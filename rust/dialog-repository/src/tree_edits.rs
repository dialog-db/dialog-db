//! Ordered edits to a keyed result tree. A node carries its own value;
//! its descendants have identities and are edited independently.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use thiserror::Error;

use crate::Delta;

pub mod wire;

/// A node's identity within a projection, independent of its parent,
/// position and value. Two occurrences of the same entity need distinct IDs.
pub type TreeId = Arc<str>;

/// A child position. `None` for the parent denotes the forest's roots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreePosition {
    /// The parent of the child.
    pub parent: Option<TreeId>,
    /// The existing sibling to insert before; `None` appends.
    pub before: Option<TreeId>,
}

/// An operation on the tree the receiver already holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeEdit<V> {
    /// Insert one node with no children.
    Insert {
        /// The new node's ID.
        id: TreeId,
        /// Where to insert it.
        at: TreePosition,
        /// The node's own value.
        value: V,
    },
    /// Change one node's value, keeping its children.
    Set {
        /// The node to edit.
        id: TreeId,
        /// Its new value.
        value: V,
    },
    /// Relocate an existing node and its subtree.
    Move {
        /// The node to relocate.
        id: TreeId,
        /// Its destination, relative to an existing sibling.
        to: TreePosition,
    },
    /// Remove a node and its subtree.
    Remove {
        /// The root of the subtree to remove.
        id: TreeId,
    },
}

/// One result row's contribution to the projection. Repeated identical
/// contributions count as one node until the last is retracted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectedNode<V> {
    /// The node's stable identity.
    pub id: TreeId,
    /// Its parent, or `None` for a root.
    pub parent: Option<TreeId>,
    /// Its bytewise sort key among siblings. Ties sort by ID.
    pub order: Vec<u8>,
    /// Its local value. Children are separate contributions.
    pub value: V,
}

/// A contribution or edit would make the tree invalid.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{reason}: {id}")]
pub struct TreeEditError {
    /// The node involved.
    pub id: TreeId,
    /// What made the edit invalid.
    pub reason: &'static str,
}

fn invalid(id: &TreeId, reason: &'static str) -> TreeEditError {
    TreeEditError {
        id: id.clone(),
        reason,
    }
}

type Siblings = BTreeSet<(Vec<u8>, TreeId)>;

/// The producer's index from result contributions to edits. It indexes
/// siblings by sort key so edits find an anchor without scanning the
/// collection. It keeps no serialized or assembled subtree.
pub struct TreeProjection<V> {
    nodes: HashMap<TreeId, (ProjectedNode<V>, i64)>,
    children: HashMap<Option<TreeId>, Siblings>,
    visible: Option<HashSet<TreeId>>,
}

impl<V> Default for TreeProjection<V> {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            children: HashMap::new(),
            visible: None,
        }
    }
}

impl<V: Clone + Eq> TreeProjection<V> {
    /// Keep contributions with an unresolved parent dormant. If the
    /// parent arrives, publish its reachable descendants with it.
    /// Enable before applying any changes.
    pub fn reachable() -> Self {
        Self {
            visible: Some(HashSet::new()),
            ..Self::default()
        }
    }

    /// Apply a batch of changed contributions and return the edits from
    /// the previously published tree. An invalid batch changes nothing.
    pub fn apply(
        &mut self,
        delta: Delta<ProjectedNode<V>>,
    ) -> Result<Vec<TreeEdit<V>>, TreeEditError> {
        self.apply_changes(
            delta
                .retracted
                .into_iter()
                .map(|row| (row, -1))
                .chain(delta.asserted.into_iter().map(|row| (row, 1))),
        )
    }

    pub(crate) fn apply_changes(
        &mut self,
        changes: impl IntoIterator<Item = (ProjectedNode<V>, i64)>,
    ) -> Result<Vec<TreeEdit<V>>, TreeEditError> {
        // Consolidate across feeds before inspecting final identities:
        // one feed may still hold the old node while another has the new.
        let mut counts = BTreeMap::<TreeId, Vec<(ProjectedNode<V>, i64)>>::new();
        for (row, diff) in changes {
            let versions = counts
                .entry(row.id.clone())
                .or_insert_with(|| self.nodes.get(&row.id).cloned().into_iter().collect());
            if let Some((_, count)) = versions.iter_mut().find(|(value, _)| *value == row) {
                *count += diff;
            } else {
                versions.push((row, diff));
            }
        }
        let mut next = BTreeMap::new();
        for (id, versions) in counts {
            let mut held = None;
            for (row, count) in versions {
                if count < 0 {
                    return Err(invalid(&id, "retraction of a missing contribution"));
                }
                if count > 0 && held.replace((row, count)).is_some() {
                    return Err(invalid(&id, "conflicting contributions for one ID"));
                }
            }
            next.insert(id, held);
        }

        // Validate against the final batch while the old indexes are
        // still intact. Walk parents only for new or reparented nodes.
        self.validate(&next)?;

        let mut gone = Vec::new();
        let mut positions = BTreeSet::new();
        let mut previous = BTreeMap::new();
        for (id, after) in next {
            let before = self.nodes.remove(&id).map(|(row, _)| row);
            let new_row = after.as_ref().map(|(row, _)| row);
            let same_position = matches!((&before, new_row), (Some(old), Some(new))
                if old.parent == new.parent && old.order == new.order);
            if !same_position {
                if let Some(old) = &before
                    && let Some(siblings) = self.children.get_mut(&old.parent)
                {
                    siblings.remove(&(old.order.clone(), id.clone()));
                    if siblings.is_empty() {
                        self.children.remove(&old.parent);
                    }
                }
                if let Some(new) = new_row {
                    self.children
                        .entry(new.parent.clone())
                        .or_default()
                        .insert((new.order.clone(), id.clone()));
                    positions.insert(id.clone());
                }
            }
            if let Some(new) = after {
                self.nodes.insert(id.clone(), new);
            } else if let Some(old) = &before {
                gone.push((id.clone(), old.parent.clone()));
            }
            previous.insert(id, before);
        }
        if let Some(visible) = &mut self.visible {
            let mut dirty: Vec<_> = previous.keys().cloned().collect();
            let mut checked = HashSet::new();
            gone.clear();
            while let Some(id) = dirty.pop() {
                if !checked.insert(id.clone()) {
                    continue;
                }
                let before = visible.contains(&id);
                let mut cursor = Some(&id);
                let mut after = true;
                while let Some(node) = cursor {
                    let Some((row, _)) = self.nodes.get(node) else {
                        after = false;
                        break;
                    };
                    cursor = row.parent.as_ref();
                }
                if !after {
                    positions.remove(&id);
                }
                if before == after {
                    continue;
                }
                if let Some(children) = self.children.get(&Some(id.clone())) {
                    dirty.extend(children.iter().map(|(_, child)| child.clone()));
                }
                if after {
                    visible.insert(id.clone());
                    positions.insert(id.clone());
                    previous.insert(id, None);
                } else {
                    visible.remove(&id);
                    let parent = previous
                        .get(&id)
                        .and_then(|old| old.as_ref())
                        .or_else(|| self.nodes.get(&id).map(|(row, _)| row))
                        .and_then(|row| row.parent.clone());
                    gone.push((id, parent));
                }
            }
            previous.retain(|id, _| visible.contains(id));
        }

        // Each relocation depends on its new parent and its right-hand
        // anchor. Following those dependencies orders parents before
        // children and siblings from right to left, including on seed.
        let mut at: BTreeMap<_, _> = positions
            .iter()
            .map(|id| {
                let (row, _) = &self.nodes[id];
                let before = self.children[&row.parent]
                    .range((Excluded((row.order.clone(), id.clone())), Unbounded))
                    .next()
                    .map(|(_, id)| id.clone());
                let mut ancestor = row.parent.as_ref();
                while let Some(parent) = ancestor {
                    if positions.contains(parent) {
                        break;
                    }
                    ancestor = self.nodes[parent].0.parent.as_ref();
                }
                (
                    id.clone(),
                    (
                        TreePosition {
                            parent: row.parent.clone(),
                            before,
                        },
                        ancestor.cloned(),
                    ),
                )
            })
            .collect();
        let mut edits = Vec::new();
        while let Some(id) = at.keys().next().cloned() {
            let mut work = vec![(id, false)];
            while let Some((id, ready)) = work.pop() {
                if ready {
                    if let Some((position, _)) = at.remove(&id) {
                        edits.push(if previous[&id].is_some() {
                            TreeEdit::Move { id, to: position }
                        } else {
                            TreeEdit::Insert {
                                value: self.nodes[&id].0.value.clone(),
                                id,
                                at: position,
                            }
                        });
                    }
                } else if let Some((position, ancestor)) = at.get(&id) {
                    work.push((id, true));
                    work.extend(
                        position
                            .before
                            .iter()
                            .chain(ancestor)
                            .filter(|id| at.contains_key(*id))
                            .map(|id| (id.clone(), false)),
                    );
                }
            }
        }
        for (id, before) in previous {
            if let Some(old) = before
                && let Some((new, _)) = self.nodes.get(&id)
                && new.value != old.value
            {
                edits.push(TreeEdit::Set {
                    id,
                    value: new.value.clone(),
                });
            }
        }
        // Surviving children have already moved out of removed parents.
        // Only the roots of deleted subtrees need an operation.
        for (id, parent) in gone {
            if parent.as_ref().is_none_or(|parent| match &self.visible {
                Some(visible) => visible.contains(parent),
                None => self.nodes.contains_key(parent),
            }) {
                edits.push(TreeEdit::Remove { id });
            }
        }
        Ok(edits)
    }

    fn validate(
        &self,
        next: &BTreeMap<TreeId, Option<(ProjectedNode<V>, i64)>>,
    ) -> Result<(), TreeEditError> {
        let final_node = |id: &TreeId| match next.get(id) {
            Some(row) => row.as_ref(),
            None => self.nodes.get(id),
        };
        // Cache depths already checked in this batch. A seed with many
        // leaves under one parent checks each ancestor at most once.
        let mut checked = BTreeSet::new();
        for (id, after) in next {
            if let Some((row, _)) = after {
                if self
                    .nodes
                    .get(id)
                    .is_some_and(|(old, _)| old.parent == row.parent)
                {
                    continue;
                }
                let mut path = BTreeSet::new();
                let mut cursor = Some(id);
                while let Some(node) = cursor {
                    if checked.contains(node) {
                        break;
                    }
                    if !path.insert(node.clone()) {
                        return Err(invalid(id, "parent cycle"));
                    }
                    let Some((parent, _)) = final_node(node) else {
                        if self.visible.is_some() {
                            break;
                        }
                        return Err(invalid(id, "missing parent"));
                    };
                    cursor = parent.parent.as_ref();
                }
                checked.extend(path);
            } else if self.visible.is_none()
                && let Some(children) = self.children.get(&Some(id.clone()))
            {
                for (_, child) in children {
                    if final_node(child).is_some_and(|(row, _)| row.parent.as_ref() == Some(id)) {
                        return Err(invalid(child, "removed parent still has a child"));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct Ends {
    first: Option<TreeId>,
    last: Option<TreeId>,
}

struct Node<V> {
    value: V,
    parent: Option<TreeId>,
    prev: Option<TreeId>,
    next: Option<TreeId>,
    children: Ends,
}

/// A DOM-independent receiver of tree edits. Sibling links allow a
/// move or insertion by anchor without walking or shifting siblings.
pub struct EditedTree<V> {
    nodes: HashMap<TreeId, Node<V>>,
    roots: Ends,
}

impl<V> Default for EditedTree<V> {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            roots: Ends::default(),
        }
    }
}

impl<V> EditedTree<V> {
    /// The node's local value.
    pub fn value(&self, id: &str) -> Option<&V> {
        self.nodes.get(id).map(|node| &node.value)
    }

    /// Children in order, or roots when `parent` is `None`.
    pub fn children<'a>(&'a self, parent: Option<&str>) -> impl Iterator<Item = &'a str> + 'a {
        let first = match parent {
            Some(id) => self
                .nodes
                .get(id)
                .and_then(|node| node.children.first.as_ref()),
            None => self.roots.first.as_ref(),
        };
        std::iter::successors(first, |id| self.nodes[*id].next.as_ref()).map(AsRef::as_ref)
    }

    /// Apply one edit. A rejected edit leaves the tree unchanged.
    /// A producer's batch is applied in its given order.
    pub fn apply(&mut self, edit: TreeEdit<V>) -> Result<(), TreeEditError> {
        match edit {
            TreeEdit::Insert { id, at, value } => {
                if self.nodes.contains_key(&id) {
                    return Err(invalid(&id, "duplicate node"));
                }
                self.check_position(&id, &at)?;
                self.nodes.insert(
                    id.clone(),
                    Node {
                        value,
                        parent: None,
                        prev: None,
                        next: None,
                        children: Ends::default(),
                    },
                );
                self.attach(&id, at);
            }
            TreeEdit::Set { id, value } => {
                self.nodes
                    .get_mut(&id)
                    .ok_or_else(|| invalid(&id, "missing node"))?
                    .value = value;
            }
            TreeEdit::Move { id, to } => {
                if !self.nodes.contains_key(&id) {
                    return Err(invalid(&id, "missing node"));
                }
                self.check_position(&id, &to)?;
                self.detach(&id);
                self.attach(&id, to);
            }
            TreeEdit::Remove { id } => {
                if !self.nodes.contains_key(&id) {
                    return Err(invalid(&id, "missing node"));
                }
                self.detach(&id);
                let mut pending = vec![id];
                while let Some(id) = pending.pop() {
                    let Some(node) = self.nodes.remove(&id) else {
                        continue;
                    };
                    let mut child = node.children.first;
                    while let Some(id) = child {
                        child = self.nodes[&id].next.clone();
                        pending.push(id);
                    }
                }
            }
        }
        Ok(())
    }

    fn check_position(&self, id: &TreeId, at: &TreePosition) -> Result<(), TreeEditError> {
        if let Some(before) = &at.before
            && (before == id
                || !self
                    .nodes
                    .get(before)
                    .is_some_and(|node| node.parent == at.parent))
        {
            return Err(invalid(id, "invalid sibling anchor"));
        }
        let mut cursor = at.parent.as_ref();
        while let Some(parent) = cursor {
            if parent == id {
                return Err(invalid(id, "parent cycle"));
            }
            cursor = self
                .nodes
                .get(parent)
                .ok_or_else(|| invalid(id, "missing parent"))?
                .parent
                .as_ref();
        }
        Ok(())
    }

    fn ends_mut(&mut self, parent: Option<&TreeId>) -> &mut Ends {
        match parent {
            Some(id) => &mut self.nodes.get_mut(id).expect("validated parent").children,
            None => &mut self.roots,
        }
    }

    fn link(
        &mut self,
        parent: Option<&TreeId>,
        prev: Option<&TreeId>,
        next: Option<&TreeId>,
        first: Option<TreeId>,
        last: Option<TreeId>,
    ) {
        if let Some(prev) = prev {
            self.nodes.get_mut(prev).expect("linked sibling").next = first;
        } else {
            self.ends_mut(parent).first = first;
        }
        if let Some(next) = next {
            self.nodes.get_mut(next).expect("linked sibling").prev = last;
        } else {
            self.ends_mut(parent).last = last;
        }
    }

    fn detach(&mut self, id: &TreeId) {
        let node = &self.nodes[id];
        let (parent, prev, next) = (node.parent.clone(), node.prev.clone(), node.next.clone());
        self.link(
            parent.as_ref(),
            prev.as_ref(),
            next.as_ref(),
            next.clone(),
            prev.clone(),
        );
    }

    fn attach(&mut self, id: &TreeId, at: TreePosition) {
        let prev = match &at.before {
            Some(before) => self.nodes[before].prev.clone(),
            None => self.ends_mut(at.parent.as_ref()).last.clone(),
        };
        self.link(
            at.parent.as_ref(),
            prev.as_ref(),
            at.before.as_ref(),
            Some(id.clone()),
            Some(id.clone()),
        );
        let node = self.nodes.get_mut(id).expect("held node");
        (node.parent, node.prev, node.next) = (at.parent, prev, at.before);
    }
}
