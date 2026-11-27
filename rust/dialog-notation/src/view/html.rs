//! A producer and mount adapter for HTML over generic tree edits.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use dialog_corpus::Value;
use dialog_repository::{EditedTree, TreeEdit, TreeId, TreePosition};

use super::plan::Plan;
use super::{Address, Content, Given, Patch, Posted, Shown, Slot};
use crate::{Error, data};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod browser;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use browser::BrowserHost;

/// The local payload of an HTML node. Children are edited separately.
#[derive(Clone, Debug, PartialEq)]
pub enum HtmlNode {
    /// A text node.
    Text(String),
    /// An element with its attributes and command listeners.
    Element {
        /// HTML tag name.
        tag: String,
        /// HTML attributes.
        attributes: Vec<(String, String)>,
        /// Notation command bindings on the element.
        events: Vec<Posted>,
    },
}

fn invalid() -> Error {
    Error::Refused("invalid HTML edit payload".into())
}

fn list(value: Value) -> Result<Vec<Value>, Error> {
    data::items(&value)?.ok_or_else(invalid)
}

fn text(value: Value) -> Result<String, Error> {
    String::try_from(value).map_err(|_| invalid())
}

fn array<const N: usize>(value: Value) -> Result<[Value; N], Error> {
    list(value)?.try_into().map_err(|_| invalid())
}

impl HtmlNode {
    fn encode(&self) -> Result<Value, Error> {
        match self {
            Self::Text(text) => Ok(data::text(text)),
            Self::Element {
                tag,
                attributes,
                events,
            } => {
                let attributes = data::list(
                    attributes
                        .iter()
                        .map(|(key, value)| data::list([data::text(key), data::text(value)]))
                        .collect::<Result<Vec<_>, _>>()?,
                )?;
                let events = data::list(
                    events
                        .iter()
                        .map(|event| {
                            let fields = data::list(
                                event
                                    .fields
                                    .iter()
                                    .map(|(key, given)| {
                                        let (kind, value) = match given {
                                            Given::Value(value) => ("value", value.clone()),
                                            Given::Written(text) => ("written", data::text(text)),
                                            Given::Path(path) => {
                                                ("path", data::list(path.iter().map(data::text))?)
                                            }
                                        };
                                        data::list([data::text(key), data::text(kind), value])
                                    })
                                    .collect::<Result<Vec<_>, Error>>()?,
                            )?;
                            data::list([
                                data::text(&event.kind),
                                data::text(&event.command),
                                fields,
                            ])
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                )?;
                data::list([data::text(tag), attributes, events])
            }
        }
    }

    /// Decode the local payload sent by a watched HTML view.
    pub fn decode(value: Value) -> Result<Self, Error> {
        if let Value::String(text) = value {
            return Ok(Self::Text(text));
        }
        let [tag, attributes, events] = array(value)?;
        let attributes = list(attributes)?
            .into_iter()
            .map(|pair| {
                let [key, value] = array(pair)?;
                Ok((text(key)?, text(value)?))
            })
            .collect::<Result<_, Error>>()?;
        let events = list(events)?
            .into_iter()
            .map(|event| {
                let [kind, command, fields] = array(event)?;
                let fields = list(fields)?
                    .into_iter()
                    .map(|field| {
                        let [key, kind, value] = array(field)?;
                        let given = match text(kind)?.as_str() {
                            "value" => Given::Value(value),
                            "written" => Given::Written(text(value)?),
                            "path" => Given::Path(
                                list(value)?
                                    .into_iter()
                                    .map(text)
                                    .collect::<Result<_, _>>()?,
                            ),
                            _ => return Err(invalid()),
                        };
                        Ok((text(key)?, given))
                    })
                    .collect::<Result<_, Error>>()?;
                Ok(Posted {
                    kind: text(kind)?,
                    command: text(command)?,
                    fields,
                })
            })
            .collect::<Result<_, Error>>()?;
        Ok(Self::Element {
            tag: text(tag)?,
            attributes,
            events,
        })
    }
}

type Children = BTreeMap<usize, Vec<TreeId>>;
type MoveKey = (Vec<usize>, Vec<Value>);

fn child_address(parent: &Address, slot: usize, shown: &Shown) -> Address {
    let mut address = parent.clone();
    address.push((slot, shown.key.clone()));
    address
}

struct Held {
    at: Option<Address>,
    parent: Option<TreeId>,
    slot: usize,
    value: HtmlNode,
    children: Children,
}

/// Converts incremental view patches into DOM-independent edit frames.
/// IDs remain stable for the lifetime of each mounted instance.
#[derive(Default)]
pub struct HtmlEdits {
    next: u64,
    addresses: HashMap<Address, TreeId>,
    nodes: HashMap<TreeId, Held>,
    roots: Children,
    move_paths: BTreeSet<Vec<usize>>,
    occurrences: HashMap<MoveKey, usize>,
}

impl HtmlEdits {
    pub(crate) fn with_plan(plan: &Plan) -> Self {
        Self {
            move_paths: plan.move_paths.clone(),
            ..Self::default()
        }
    }

    /// Translate a batch emitted by a notation view initialized against
    /// an empty tree. A failed conversion requires a new watch and mount.
    pub fn edits(&mut self, patches: &[Patch]) -> Result<Vec<TreeEdit<Value>>, Error> {
        let moves = self.moves(patches)?;
        let moving: HashSet<&TreeId> = moves.values().collect();
        let mut edits = Vec::new();
        // The plan gives insertions their final sibling ranks. Detach
        // departures first so none of them can be an insertion anchor.
        for patch in patches {
            if let Patch::Remove { at } = patch {
                let id = self.address(at)?;
                self.detach(&id)?;
                if !moving.contains(&id) {
                    self.remove(id, &mut edits);
                }
            }
        }
        for patch in patches {
            match patch {
                Patch::Insert {
                    parent,
                    slot,
                    index,
                    instance,
                } => {
                    let id = if parent.is_empty() {
                        None
                    } else {
                        Some(self.address(parent)?)
                    };
                    let address = child_address(parent, *slot, instance);
                    if let Some(existing) = moves.get(&address) {
                        let to = self.place(existing, id, *slot, *index)?;
                        let held = self.nodes.get_mut(existing).ok_or_else(invalid)?;
                        held.parent = to.parent.clone();
                        held.slot = *slot;
                        edits.push(TreeEdit::Move {
                            id: existing.clone(),
                            to,
                        });
                        self.reconcile(existing, address, instance, &mut edits)?;
                    } else {
                        self.shown(id, address, *index, instance, &mut edits)?;
                    }
                }
                Patch::Remove { .. } => {}
                Patch::SetText { at, slot, text } => {
                    let parent = self.address(at)?;
                    let id = self.nodes[&parent]
                        .children
                        .get(slot)
                        .and_then(|slot| slot.first())
                        .cloned()
                        .ok_or_else(invalid)?;
                    self.nodes.get_mut(&id).ok_or_else(invalid)?.value =
                        HtmlNode::Text(text.clone());
                    edits.push(TreeEdit::Set {
                        id,
                        value: data::text(text),
                    });
                }
                Patch::SetAttribute { at, name, value } => {
                    self.change(at, &mut edits, |attributes, _| {
                        let (_, old) = attributes
                            .iter_mut()
                            .find(|(key, _)| key == name)
                            .ok_or_else(invalid)?;
                        old.clone_from(value);
                        Ok(())
                    })?;
                }
                Patch::SetEvent { at, index, event } => {
                    self.change(at, &mut edits, |_, events| {
                        events
                            .get_mut(*index)
                            .ok_or_else(invalid)?
                            .clone_from(event);
                        Ok(())
                    })?;
                }
            }
        }
        Ok(edits)
    }

    fn move_key(&self, address: &Address) -> Option<MoveKey> {
        if self.move_paths.is_empty() {
            return None;
        }
        let (_, key) = address.last()?;
        let path = address.iter().skip(1).map(|(slot, _)| *slot).collect();
        self.move_paths.contains(&path).then(|| (path, key.clone()))
    }

    fn moves(&self, patches: &[Patch]) -> Result<HashMap<Address, TreeId>, Error> {
        let mut departures = HashMap::new();
        for patch in patches {
            if let Patch::Remove { at } = patch
                && let Some(key) = self.move_key(at)
                && self.occurrences.get(&key) == Some(&1)
            {
                departures.insert(key, self.address(at)?);
            }
        }
        if departures.is_empty() {
            return Ok(HashMap::new());
        }

        // Descendants of an inserted ancestor also count toward
        // uniqueness, even though only direct arrivals can move.
        let mut arrivals = HashMap::new();
        for patch in patches {
            if let Patch::Insert {
                parent,
                slot,
                instance,
                ..
            } = patch
            {
                let mut path: Vec<usize> = parent.iter().skip(1).map(|(slot, _)| *slot).collect();
                if !parent.is_empty() {
                    path.push(*slot);
                }
                self.count_arrivals(&mut path, instance, &departures, &mut arrivals);
            }
        }
        let mut moves = HashMap::new();
        for patch in patches {
            if let Patch::Insert {
                parent,
                slot,
                instance,
                ..
            } = patch
            {
                let address = child_address(parent, *slot, instance);
                if let Some(key) = self.move_key(&address)
                    && arrivals.get(&key) == Some(&1)
                    && let Some(id) = departures.remove(&key)
                {
                    moves.insert(address, id);
                }
            }
        }
        Ok(moves)
    }

    fn count_arrivals(
        &self,
        path: &mut Vec<usize>,
        shown: &Shown,
        departures: &HashMap<MoveKey, TreeId>,
        arrivals: &mut HashMap<MoveKey, usize>,
    ) {
        if self.move_paths.contains(path) {
            let key = (path.clone(), shown.key.clone());
            if departures.contains_key(&key) {
                *arrivals.entry(key).or_default() += 1;
            }
        }
        let Content::Element { slots, .. } = &shown.content;
        for (slot, child) in slots.iter().enumerate() {
            if let Slot::Instances(children) = child {
                path.push(slot);
                for child in children {
                    self.count_arrivals(path, child, departures, arrivals);
                }
                path.pop();
            }
        }
    }

    fn address(&self, address: &Address) -> Result<TreeId, Error> {
        self.addresses.get(address).cloned().ok_or_else(invalid)
    }

    fn children(&mut self, parent: Option<&TreeId>) -> Result<&mut Children, Error> {
        match parent {
            Some(parent) => self
                .nodes
                .get_mut(parent)
                .map(|node| &mut node.children)
                .ok_or_else(invalid),
            None => Ok(&mut self.roots),
        }
    }

    fn place(
        &mut self,
        id: &TreeId,
        parent: Option<TreeId>,
        slot: usize,
        index: usize,
    ) -> Result<TreePosition, Error> {
        let children = self.children(parent.as_ref())?;
        let before = children
            .get(&slot)
            .and_then(|children| children.get(index))
            .or_else(|| {
                children
                    .range((std::ops::Bound::Excluded(slot), std::ops::Bound::Unbounded))
                    .find_map(|(_, children)| children.first())
            })
            .cloned();
        let siblings = children.entry(slot).or_default();
        if index > siblings.len() {
            return Err(invalid());
        }
        siblings.insert(index, id.clone());
        Ok(TreePosition { parent, before })
    }

    fn detach(&mut self, id: &TreeId) -> Result<(), Error> {
        let node = self.nodes.get(id).ok_or_else(invalid)?;
        let (parent, slot) = (node.parent.clone(), node.slot);
        let siblings = self
            .children(parent.as_ref())?
            .get_mut(&slot)
            .ok_or_else(invalid)?;
        siblings.retain(|child| child != id);
        Ok(())
    }

    fn remove(&mut self, id: TreeId, edits: &mut Vec<TreeEdit<Value>>) {
        let mut gone = vec![id.clone()];
        while let Some(id) = gone.pop() {
            if let Some(child) = self.nodes.remove(&id) {
                if let Some(address) = child.at {
                    self.addresses.remove(&address);
                    if let Some(key) = self.move_key(&address)
                        && let Entry::Occupied(mut entry) = self.occurrences.entry(key)
                    {
                        *entry.get_mut() -= 1;
                        if *entry.get() == 0 {
                            entry.remove();
                        }
                    }
                }
                gone.extend(child.children.into_values().flatten());
            }
        }
        edits.push(TreeEdit::Remove { id });
    }

    fn insert(
        &mut self,
        parent: Option<TreeId>,
        slot: usize,
        index: usize,
        address: Option<Address>,
        value: HtmlNode,
        edits: &mut Vec<TreeEdit<Value>>,
    ) -> Result<TreeId, Error> {
        let payload = value.encode()?;
        let id: TreeId = Arc::from(self.next.to_string());
        self.next += 1;
        let at = self.place(&id, parent.clone(), slot, index)?;
        if let Some(address) = &address {
            self.addresses.insert(address.clone(), id.clone());
            if let Some(key) = self.move_key(address) {
                *self.occurrences.entry(key).or_default() += 1;
            }
        }
        self.nodes.insert(
            id.clone(),
            Held {
                at: address,
                parent: parent.clone(),
                slot,
                value,
                children: Children::new(),
            },
        );
        edits.push(TreeEdit::Insert {
            id: id.clone(),
            at,
            value: payload,
        });
        Ok(id)
    }

    fn shown(
        &mut self,
        parent: Option<TreeId>,
        address: Address,
        index: usize,
        shown: &Shown,
        edits: &mut Vec<TreeEdit<Value>>,
    ) -> Result<(), Error> {
        let Content::Element {
            tag,
            attributes,
            events,
            slots,
        } = &shown.content;
        let slot = address.last().ok_or_else(invalid)?.0;
        let value = HtmlNode::Element {
            tag: tag.clone(),
            attributes: attributes.clone(),
            events: events.clone(),
        };
        let id = self.insert(parent, slot, index, Some(address.clone()), value, edits)?;
        for (slot, child) in slots.iter().enumerate() {
            match child {
                Slot::Text(text) => {
                    self.insert(
                        Some(id.clone()),
                        slot,
                        0,
                        None,
                        HtmlNode::Text(text.clone()),
                        edits,
                    )?;
                }
                Slot::Instances(children) => {
                    for (index, child) in children.iter().enumerate() {
                        let at = child_address(&address, slot, child);
                        self.shown(Some(id.clone()), at, index, child, edits)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn reconcile(
        &mut self,
        id: &TreeId,
        address: Address,
        shown: &Shown,
        edits: &mut Vec<TreeEdit<Value>>,
    ) -> Result<(), Error> {
        let Content::Element {
            tag,
            attributes,
            events,
            slots,
        } = &shown.content;
        let held = self.nodes.get_mut(id).ok_or_else(invalid)?;
        let old = held.at.replace(address.clone()).ok_or_else(invalid)?;
        self.addresses.remove(&old);
        self.addresses.insert(address.clone(), id.clone());
        self.set(
            id,
            HtmlNode::Element {
                tag: tag.clone(),
                attributes: attributes.clone(),
                events: events.clone(),
            },
            edits,
        )?;
        for (slot, child) in slots.iter().enumerate() {
            let old = self.nodes[id]
                .children
                .get(&slot)
                .cloned()
                .unwrap_or_default();
            match child {
                Slot::Text(text) => {
                    let text_id = old.first().ok_or_else(invalid)?;
                    self.set(text_id, HtmlNode::Text(text.clone()), edits)?;
                }
                Slot::Instances(children) => {
                    let remaining: HashSet<_> = children.iter().map(|child| &child.key).collect();
                    let mut kept = HashMap::new();
                    for id in old {
                        let (_, key) = self.nodes[&id]
                            .at
                            .as_ref()
                            .and_then(|address| address.last())
                            .ok_or_else(invalid)?;
                        if remaining.contains(key) {
                            kept.insert(key.clone(), id);
                        } else {
                            self.detach(&id)?;
                            self.remove(id, edits);
                        }
                    }
                    // Instances in a slot are in key order, so surviving
                    // children are already in the right relative order.
                    for (index, child) in children.iter().enumerate() {
                        let at = child_address(&address, slot, child);
                        if let Some(existing) = kept.get(&child.key) {
                            self.reconcile(existing, at, child, edits)?;
                        } else {
                            self.shown(Some(id.clone()), at, index, child, edits)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn set(
        &mut self,
        id: &TreeId,
        value: HtmlNode,
        edits: &mut Vec<TreeEdit<Value>>,
    ) -> Result<(), Error> {
        let current = &mut self.nodes.get_mut(id).ok_or_else(invalid)?.value;
        if *current != value {
            edits.push(TreeEdit::Set {
                id: id.clone(),
                value: value.encode()?,
            });
            *current = value;
        }
        Ok(())
    }

    fn change(
        &mut self,
        at: &Address,
        edits: &mut Vec<TreeEdit<Value>>,
        update: impl FnOnce(&mut Vec<(String, String)>, &mut Vec<Posted>) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let id = self.address(at)?;
        let held = &mut self.nodes.get_mut(&id).ok_or_else(invalid)?.value;
        let HtmlNode::Element {
            attributes, events, ..
        } = held
        else {
            return Err(invalid());
        };
        update(attributes, events)?;
        edits.push(TreeEdit::Set {
            id,
            value: held.encode()?,
        });
        Ok(())
    }
}

/// The operations a mounted HTML receiver requires from its host.
/// Implementations apply content as text and attributes, never markup.
pub trait HtmlHost {
    /// A handle to the existing mounted object.
    type Node: Clone;
    /// Create a detached node.
    fn create(&mut self, value: HtmlNode) -> Result<Self::Node, String>;
    /// Change local content without replacing the node or its children.
    fn set(&mut self, node: &Self::Node, value: HtmlNode) -> Result<(), String>;
    /// Insert or move a handle. A missing parent denotes the mount root.
    fn place(
        &mut self,
        node: &Self::Node,
        parent: Option<&Self::Node>,
        before: Option<&Self::Node>,
    ) -> Result<(), String>;
    /// Detach an entire subtree.
    fn remove(&mut self, node: &Self::Node) -> Result<(), String>;
}

/// Applies edits directly to a mounted HTML host. On an application
/// error discard the mount and seed a new one.
pub struct HtmlMount<H: HtmlHost> {
    host: H,
    mounted: EditedTree<H::Node>,
}

impl<H: HtmlHost> HtmlMount<H> {
    /// Start with an empty root.
    pub fn new(host: H) -> Self {
        Self {
            host,
            mounted: EditedTree::default(),
        }
    }

    /// The host after the applied edits.
    pub fn host(&self) -> &H {
        &self.host
    }

    fn node(&self, id: &str) -> Result<H::Node, String> {
        self.mounted
            .value(id)
            .cloned()
            .ok_or_else(|| format!("missing HTML node {id}"))
    }

    /// Apply one ordered batch from a trusted notation producer.
    pub fn apply(&mut self, edits: Vec<TreeEdit<Value>>) -> Result<(), String> {
        for edit in edits {
            let (node, at) = match edit {
                TreeEdit::Insert { id, at, value } => {
                    let node = self
                        .host
                        .create(HtmlNode::decode(value).map_err(|error| error.to_string())?)?;
                    self.mounted
                        .apply(TreeEdit::Insert {
                            id,
                            at: at.clone(),
                            value: node.clone(),
                        })
                        .map_err(|error| error.to_string())?;
                    (node, at)
                }
                TreeEdit::Move { id, to } => {
                    let node = self.node(&id)?;
                    self.mounted
                        .apply(TreeEdit::Move { id, to: to.clone() })
                        .map_err(|error| error.to_string())?;
                    (node, to)
                }
                TreeEdit::Remove { id } => {
                    let node = self.node(&id)?;
                    self.mounted
                        .apply(TreeEdit::Remove { id })
                        .map_err(|error| error.to_string())?;
                    self.host.remove(&node)?;
                    continue;
                }
                TreeEdit::Set { id, value } => {
                    let node = self.node(&id)?;
                    self.host.set(
                        &node,
                        HtmlNode::decode(value).map_err(|error| error.to_string())?,
                    )?;
                    continue;
                }
            };
            let parent = at.parent.as_deref().map(|id| self.node(id)).transpose()?;
            let before = at.before.as_deref().map(|id| self.node(id)).transpose()?;
            self.host.place(&node, parent.as_ref(), before.as_ref())?;
        }
        Ok(())
    }
}
