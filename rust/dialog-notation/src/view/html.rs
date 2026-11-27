//! A producer and mount adapter for HTML over generic tree edits.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use dialog_corpus::Value;
use dialog_repository::{EditedTree, TreeEdit, TreeId, TreePosition};

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
}

impl HtmlEdits {
    /// Translate a batch emitted by a notation view initialized against
    /// an empty tree. A failed conversion requires a new watch and mount.
    pub fn edits(&mut self, patches: &[Patch]) -> Result<Vec<TreeEdit<Value>>, Error> {
        let mut edits = Vec::new();
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
                    self.shown(id, parent, *slot, *index, instance, &mut edits)?;
                }
                Patch::Remove { at } => {
                    let id = self.address(at)?;
                    let node = &self.nodes[&id];
                    let parent = node.parent.clone();
                    let slot = node.slot;
                    let children = self
                        .children(parent.as_ref())?
                        .get_mut(&slot)
                        .ok_or_else(invalid)?;
                    children.retain(|child| child != &id);
                    let mut gone = vec![id.clone()];
                    while let Some(child) = gone.pop() {
                        if let Some(child) = self.nodes.remove(&child) {
                            if let Some(address) = child.at {
                                self.addresses.remove(&address);
                            }
                            gone.extend(child.children.into_values().flatten());
                        }
                    }
                    edits.push(TreeEdit::Remove { id });
                }
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
        if let Some(address) = &address {
            self.addresses.insert(address.clone(), id.clone());
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
            at: TreePosition { parent, before },
            value: payload,
        });
        Ok(id)
    }

    fn shown(
        &mut self,
        parent: Option<TreeId>,
        address: &Address,
        slot: usize,
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
        let mut address = address.clone();
        address.push((slot, shown.key.clone()));
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
                        self.shown(Some(id.clone()), &address, slot, index, child, edits)?;
                    }
                }
            }
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
