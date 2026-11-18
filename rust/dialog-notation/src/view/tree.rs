//! A view's instances, maintained from the rows of its layers.
//!
//! An instance of an element is one distinct value of its key under one
//! instance of its parent, and it is counted by the rows of its layer
//! behind it: it arrives with its first row and leaves with its last. A
//! row that arrives or leaves touches the instances it is behind and no
//! other; [`Maintainer::apply`] says what changed as [`Patch`]es. No
//! value is folded: the tree is the instances, each made of the row
//! behind it and its parent's.
//!
//! The tree is data, [`Rendered`]: a tag, attributes, events, and the
//! slots of what the element holds, a text or the instances of a child.
//! It is not markup and not a document object model.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use dialog_corpus::Value;

use crate::view::plan::Plan;
use crate::view::{Attribute, Input, Node, Piece};

/// A row of a layer: each variable and what it was bound to.
pub type Row = BTreeMap<String, Value>;

/// Where an instance is: from the root down, the slot of its parent it
/// stands in and its key. The root's slot is 0.
pub type Address = Vec<(usize, Vec<Value>)>;

/// What a field of an event's command is given.
#[derive(Clone, Debug, PartialEq)]
pub enum Given {
    /// A value of the row.
    Value(Value),
    /// A value written in the binding.
    Written(String),
    /// A path into the live event.
    Path(Vec<String>),
}

/// An event bound on an instance, its terms filled from the row.
#[derive(Clone, Debug, PartialEq)]
pub struct Posted {
    /// The event's type.
    pub kind: String,
    /// The command it posts.
    pub command: String,
    /// Each field of the command and what it is given.
    pub fields: Vec<(String, Given)>,
}

/// A nested view, where it is mounted: which view, of which entity.
#[derive(Clone, Debug, PartialEq)]
pub struct Mount {
    /// The view's name.
    pub view: String,
    /// The concept whose view it is, when the reference names one.
    pub model: Option<String>,
    /// The entity it renders, when the reference names one.
    pub of: Option<Value>,
    /// The reference's other entries, their parts joined; a call's
    /// labels, each term as the notation writes it.
    pub entries: Vec<(String, String)>,
    /// What the variables a call holds are bound to here: what its
    /// narrowing reads with.
    pub given: Vec<(String, Value)>,
}

/// What an instance is.
#[derive(Clone, Debug, PartialEq)]
pub enum Content {
    /// An element.
    Element {
        /// Its tag.
        tag: String,
        /// Its attributes, in the order written, each as text.
        attributes: Vec<(String, String)>,
        /// Its events, in the order written.
        events: Vec<Posted>,
        /// What it holds: a slot for each child the template writes.
        slots: Vec<Slot>,
    },
    /// A nested view, mounted here.
    View(Mount),
}

/// What a child of the template is in an instance.
#[derive(Clone, Debug, PartialEq)]
pub enum Slot {
    /// A text, a variable's value or a value written.
    Text(String),
    /// The instances of an element or a nested view, in the order of
    /// their keys.
    Instances(Vec<Shown>),
}

/// An instance: its key and what it is.
#[derive(Clone, Debug, PartialEq)]
pub struct Shown {
    /// What tells it apart from its siblings.
    pub key: Vec<Value>,
    /// What it is.
    pub content: Content,
}

/// A view's tree: the instances of its root.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Rendered {
    /// The root's instances, in the order of their keys.
    pub roots: Vec<Shown>,
}

/// A change to the tree.
#[derive(Clone, Debug, PartialEq)]
pub enum Patch {
    /// An instance arrives, with what it holds.
    Insert {
        /// Its parent; empty for a root.
        parent: Address,
        /// The parent's slot it stands in.
        slot: usize,
        /// Where among the slot's instances.
        index: usize,
        /// The instance.
        instance: Shown,
    },
    /// An instance leaves, with what it holds.
    Remove {
        /// The instance.
        at: Address,
    },
    /// A text of an instance changes.
    SetText {
        /// The instance.
        at: Address,
        /// Its slot.
        slot: usize,
        /// The text now.
        text: String,
    },
    /// An attribute of an instance changes.
    SetAttribute {
        /// The instance.
        at: Address,
        /// The attribute.
        name: String,
        /// Its value now.
        value: String,
    },
    /// What an event of an instance posts changes.
    SetEvent {
        /// The instance.
        at: Address,
        /// Which of its events.
        index: usize,
        /// The event now.
        event: Posted,
    },
    /// A nested view's reference changes.
    SetView {
        /// The instance.
        at: Address,
        /// The reference now.
        mount: Mount,
    },
}

/// The rows of one layer that arrived and that left.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Change {
    /// The layer, by its index in [`Plan::layers`].
    pub layer: usize,
    /// Rows that arrived.
    pub arrived: Vec<Row>,
    /// Rows that left.
    pub left: Vec<Row>,
}

/// A value with an order over every value: the order values have, and
/// their printed form where they have none.
#[derive(Clone, Debug)]
struct Datum(Value);

impl Ord for Datum {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or_else(|| format!("{:?}", self.0).cmp(&format!("{:?}", other.0)))
    }
}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Datum {}

type Key = Vec<Datum>;
type Chain = Vec<Key>;
type Kept = Vec<(String, Datum)>;
/// For each instance a change touched: whether it was in the tree, and
/// the row it showed.
type Before = BTreeMap<(usize, Chain), (bool, Option<Kept>)>;

/// A value as text: text as it is, an entity by its URI, a number as
/// written.
pub fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Entity(entity) => entity.to_string(),
        Value::Boolean(truth) => truth.to_string(),
        Value::UnsignedInt(number) => number.to_string(),
        Value::SignedInt(number) => number.to_string(),
        Value::Float(number) => number.to_string(),
        Value::Symbol(symbol) => symbol.to_string(),
        Value::Bytes(_) | Value::Record(_) => format!("{value:?}"),
    }
}

/// What an instance shows of itself, its children's instances apart.
#[derive(Clone, Debug, PartialEq)]
enum Own {
    Element {
        attributes: Vec<(String, String)>,
        events: Vec<Posted>,
        texts: Vec<Option<String>>,
    },
    View(Mount),
}

/// The instances of a view, kept from the rows of its layers.
#[derive(Clone, Debug)]
pub struct Maintainer {
    plan: Plan,
    entity: Option<Datum>,
    /// For each place, each instance by its chain of keys from the root,
    /// and the rows behind it, each counted.
    live: Vec<BTreeMap<Chain, BTreeMap<Kept, usize>>>,
    /// For each place, the places from the root down to it.
    lineage: Vec<Vec<usize>>,
    /// For each place, the variables it and every place below it show.
    shows: Vec<BTreeSet<String>>,
}

impl Maintainer {
    /// An empty tree of a view, rendered for `entity`, or for every
    /// instance of its concept where none is given.
    pub fn new(plan: Plan, entity: Option<Value>) -> Maintainer {
        let count = plan.elements.len();
        let lineage = (0..count)
            .map(|at| {
                let mut line = vec![at];
                let mut up = plan.elements[at].parent;
                while let Some(parent) = up {
                    line.push(parent);
                    up = plan.elements[parent].parent;
                }
                line.reverse();
                line
            })
            .collect();
        let mut shows: Vec<BTreeSet<String>> = plan
            .elements
            .iter()
            .map(|place| place.holds.iter().cloned().collect())
            .collect();
        for at in (0..count).rev() {
            if let Some(parent) = plan.elements[at].parent {
                let below = shows[at].clone();
                shows[parent].extend(below);
            }
        }
        Maintainer {
            live: vec![BTreeMap::new(); count],
            entity: entity.map(Datum),
            plan,
            lineage,
            shows,
        }
    }

    /// The plan it maintains.
    pub fn plan(&self) -> &Plan {
        &self.plan
    }

    /// The chain of keys a row places an instance of `element` at, if
    /// the row binds every key on the way.
    fn project(&self, element: usize, row: &Row) -> Option<Chain> {
        self.lineage[element]
            .iter()
            .map(|at| {
                self.plan.elements[*at]
                    .key
                    .iter()
                    .map(|name| row.get(name).cloned().map(Datum))
                    .collect::<Option<Key>>()
            })
            .collect()
    }

    fn kept(&self, layer: usize, row: &Row) -> Kept {
        self.plan.layers[layer]
            .row
            .iter()
            .filter_map(|name| Some((name.clone(), Datum(row.get(name)?.clone()))))
            .collect()
    }

    /// Whether an instance is in the tree: it has a row, and so does
    /// every instance above it, the root being of the entity rendered.
    fn mounted(&self, element: usize, chain: &Chain) -> bool {
        if let (Some(entity), Some(root)) = (&self.entity, chain.first())
            && root.as_slice() != std::slice::from_ref(entity)
        {
            return false;
        }
        self.lineage[element]
            .iter()
            .enumerate()
            .all(|(depth, at)| self.live[*at].contains_key(&chain[..=depth]))
    }

    /// The row an instance shows: the first of those behind it.
    fn shown(&self, element: usize, chain: &Chain) -> Option<Kept> {
        self.live[element]
            .get(chain)
            .and_then(|rows| rows.keys().next().cloned())
    }

    /// Every variable an instance can show, from its row and those of
    /// the instances above it, the nearer first; `before` gives the row
    /// an instance showed before a change, where it did.
    fn environment(
        &self,
        element: usize,
        chain: &Chain,
        before: Option<&Before>,
    ) -> BTreeMap<String, Value> {
        let mut values = BTreeMap::new();
        for (depth, at) in self.lineage[element].iter().enumerate() {
            let prefix: Chain = chain[..=depth].to_vec();
            let row = match before.and_then(|before| before.get(&(*at, prefix.clone()))) {
                Some((_, row)) => row.clone(),
                None => self.shown(*at, &prefix),
            };
            for (name, value) in row.unwrap_or_default() {
                values.insert(name, value.0);
            }
        }
        values
    }

    fn address(&self, element: usize, chain: &Chain) -> Address {
        self.lineage[element]
            .iter()
            .enumerate()
            .map(|(depth, at)| {
                (
                    self.plan.elements[*at].path.last().copied().unwrap_or(0),
                    chain[depth].iter().map(|datum| datum.0.clone()).collect(),
                )
            })
            .collect()
    }

    /// What an instance shows of itself, given what it can show.
    fn own(&self, element: usize, values: &BTreeMap<String, Value>) -> Own {
        let value_text = |name: &str| values.get(name).map(text).unwrap_or_default();
        let parts = |pieces: &[Piece]| {
            pieces
                .iter()
                .map(|piece| match piece {
                    Piece::Text(text) => text.clone(),
                    Piece::Value(value) => value.to_string(),
                    Piece::Variable(name) => value_text(name),
                })
                .collect::<String>()
        };
        match self.plan.node(element) {
            Node::Element(node) => Own::Element {
                attributes: node
                    .attributes
                    .iter()
                    .map(|(name, attribute)| {
                        let value = match attribute {
                            Attribute::Text(text) => text.clone(),
                            Attribute::Value(value) => value.to_string(),
                            Attribute::Variable(name) => value_text(name),
                            Attribute::Parts(pieces) => parts(pieces),
                            Attribute::Present => String::new(),
                        };
                        (name.clone(), value)
                    })
                    .collect(),
                events: node
                    .events
                    .iter()
                    .map(|event| Posted {
                        kind: event.kind.clone(),
                        command: event.command.clone(),
                        fields: event
                            .fields
                            .iter()
                            .map(|(label, input)| {
                                let given = match input {
                                    Input::Variable(name) => match values.get(name) {
                                        Some(value) => Given::Value(value.clone()),
                                        None => Given::Written(String::new()),
                                    },
                                    Input::Value(value) => Given::Written(value.to_string()),
                                    Input::Path(steps) => Given::Path(steps.clone()),
                                };
                                (label.clone(), given)
                            })
                            .collect(),
                    })
                    .collect(),
                texts: node
                    .children
                    .iter()
                    .map(|child| match child {
                        Node::Text(text) => Some(text.clone()),
                        Node::Value(value) => Some(value.to_string()),
                        Node::Variable(name) => Some(value_text(name)),
                        Node::Element(_) | Node::View(_) | Node::Call(_) => None,
                    })
                    .collect(),
            },
            Node::View(nested) => Own::View(Mount {
                view: nested.view.clone(),
                model: nested.model.clone(),
                of: nested
                    .of
                    .as_ref()
                    .and_then(|name| values.get(name).cloned()),
                entries: nested
                    .entries
                    .iter()
                    .map(|(name, pieces)| (name.clone(), parts(pieces)))
                    .collect(),
                given: Vec::new(),
            }),
            Node::Call(call) => {
                let bound = |term: &crate::syntax::Syntax| match term {
                    crate::syntax::Syntax::Variable(name) => values.get(name).cloned(),
                    crate::syntax::Syntax::Constant(value) => Some(value.clone()),
                    _ => None,
                };
                let given = self.plan.elements[element]
                    .holds
                    .iter()
                    .filter_map(|name| Some((name.clone(), values.get(name)?.clone())))
                    .collect();
                Own::View(Mount {
                    view: call.view.clone(),
                    model: Some(call.concept.clone()),
                    of: call.of.as_ref().and_then(bound),
                    entries: call
                        .labels
                        .iter()
                        .map(|(label, term)| {
                            let shown = match bound(term) {
                                Some(value) => crate::syntax::Syntax::Constant(value).to_string(),
                                None => term.to_string(),
                            };
                            (label.clone(), shown)
                        })
                        .collect(),
                    given,
                })
            }
            Node::Text(_) | Node::Value(_) | Node::Variable(_) => Own::Element {
                attributes: Vec::new(),
                events: Vec::new(),
                texts: Vec::new(),
            },
        }
    }

    /// The place that stands in a slot of an element, if one does.
    fn child(&self, element: usize, slot: usize) -> Option<usize> {
        (0..self.plan.elements.len()).find(|at| {
            self.plan.elements[*at].parent == Some(element)
                && self.plan.elements[*at].path.last() == Some(&slot)
        })
    }

    /// The instances of `element` under the instance `parent` of its
    /// parent, in the order of their keys.
    fn under(&self, element: usize, parent: &Chain) -> Vec<Chain> {
        self.live[element]
            .range(parent.clone()..)
            .take_while(|(chain, _)| chain.starts_with(parent))
            .filter(|(chain, _)| chain.len() == parent.len() + 1)
            .map(|(chain, _)| chain.clone())
            .collect()
    }

    /// An instance and every instance under it, as the tree has them.
    fn render(&self, element: usize, chain: &Chain) -> Shown {
        let values = self.environment(element, chain, None);
        let content = match self.own(element, &values) {
            Own::View(mount) => Content::View(mount),
            Own::Element {
                attributes,
                events,
                texts,
            } => {
                let tag = match self.plan.node(element) {
                    Node::Element(node) => node.tag.clone(),
                    _ => String::new(),
                };
                let slots = texts
                    .into_iter()
                    .enumerate()
                    .map(|(slot, text)| match text {
                        Some(text) => Slot::Text(text),
                        None => Slot::Instances(match self.child(element, slot) {
                            Some(child) => self
                                .under(child, chain)
                                .iter()
                                .map(|below| self.render(child, below))
                                .collect(),
                            None => Vec::new(),
                        }),
                    })
                    .collect();
                Content::Element {
                    tag,
                    attributes,
                    events,
                    slots,
                }
            }
        };
        Shown {
            key: chain
                .last()
                .map(|key| key.iter().map(|datum| datum.0.clone()).collect())
                .unwrap_or_default(),
            content,
        }
    }

    /// The tree as it stands.
    pub fn tree(&self) -> Rendered {
        Rendered {
            roots: self.live[0]
                .keys()
                .filter(|chain| self.mounted(0, chain))
                .map(|chain| self.render(0, chain))
                .collect(),
        }
    }

    /// Take rows that arrived and rows that left, of any layers, as one
    /// change, and say what it changed in the tree. Only the instances
    /// the rows are behind are looked at, and where the values one shows
    /// changed, the instances under it that show them.
    pub fn apply(&mut self, changes: &[Change]) -> Vec<Patch> {
        let mut moves: Vec<(usize, Chain, Kept, bool)> = Vec::new();
        let mut touched: BTreeSet<(usize, Chain)> = BTreeSet::new();
        for change in changes {
            let region = self.plan.region(change.layer);
            let rows = change
                .left
                .iter()
                .map(|row| (row, false))
                .chain(change.arrived.iter().map(|row| (row, true)));
            for (row, arrived) in rows {
                let kept = self.kept(change.layer, row);
                for element in &region {
                    if let Some(chain) = self.project(*element, row) {
                        touched.insert((*element, chain.clone()));
                        moves.push((*element, chain, kept.clone(), arrived));
                    }
                }
            }
        }
        let before: Before = touched
            .iter()
            .map(|(element, chain)| {
                (
                    (*element, chain.clone()),
                    (self.mounted(*element, chain), self.shown(*element, chain)),
                )
            })
            .collect();
        for (element, chain, kept, arrived) in moves {
            let rows = self.live[element].entry(chain.clone()).or_default();
            if arrived {
                *rows.entry(kept).or_insert(0) += 1;
            } else if let Some(count) = rows.get_mut(&kept) {
                *count -= 1;
                if *count == 0 {
                    rows.remove(&kept);
                }
            }
            if rows.is_empty() {
                self.live[element].remove(&chain);
            }
        }
        let mut patches = Vec::new();
        let mut moved: Vec<(usize, Chain)> = Vec::new();
        for (element, chain) in &touched {
            let within = moved.iter().any(|(at, prefix)| {
                at != element && self.lineage[*element].contains(at) && chain.starts_with(prefix)
            });
            if within {
                continue;
            }
            let (was, _) = before[&(*element, chain.clone())];
            let now = self.mounted(*element, chain);
            match (was, now) {
                (false, true) => {
                    let parent = chain[..chain.len() - 1].to_vec();
                    let index = match self.plan.elements[*element].parent {
                        Some(_) => self
                            .under(*element, &parent)
                            .iter()
                            .position(|sibling| sibling == chain)
                            .unwrap_or(0),
                        None => self.live[0]
                            .keys()
                            .filter(|root| self.mounted(0, root))
                            .position(|root| root == chain)
                            .unwrap_or(0),
                    };
                    let parent = match self.plan.elements[*element].parent {
                        Some(up) => self.address(up, &parent),
                        None => Vec::new(),
                    };
                    patches.push(Patch::Insert {
                        parent,
                        slot: self.plan.elements[*element]
                            .path
                            .last()
                            .copied()
                            .unwrap_or(0),
                        index,
                        instance: self.render(*element, chain),
                    });
                    moved.push((*element, chain.clone()));
                }
                (true, false) => {
                    patches.push(Patch::Remove {
                        at: self.address(*element, chain),
                    });
                    moved.push((*element, chain.clone()));
                }
                (true, true) => self.compare(*element, chain, &before, &touched, &mut patches),
                (false, false) => {}
            }
        }
        patches
    }

    /// Say what an instance in the tree before and after a change shows
    /// differently, and go down to the instances under it that show what
    /// changed.
    fn compare(
        &self,
        element: usize,
        chain: &Chain,
        before: &Before,
        touched: &BTreeSet<(usize, Chain)>,
        patches: &mut Vec<Patch>,
    ) {
        let then = self.environment(element, chain, Some(before));
        let now = self.environment(element, chain, None);
        let at = self.address(element, chain);
        match (self.own(element, &then), self.own(element, &now)) {
            (
                Own::Element {
                    attributes: old_attributes,
                    events: old_events,
                    texts: old_texts,
                },
                Own::Element {
                    attributes,
                    events,
                    texts,
                },
            ) => {
                for (old, new) in old_attributes.iter().zip(&attributes) {
                    if old != new {
                        patches.push(Patch::SetAttribute {
                            at: at.clone(),
                            name: new.0.clone(),
                            value: new.1.clone(),
                        });
                    }
                }
                for (index, (old, new)) in old_events.iter().zip(&events).enumerate() {
                    if old != new {
                        patches.push(Patch::SetEvent {
                            at: at.clone(),
                            index,
                            event: new.clone(),
                        });
                    }
                }
                for (slot, (old, new)) in old_texts.iter().zip(&texts).enumerate() {
                    if let (Some(old), Some(new)) = (old, new)
                        && old != new
                    {
                        patches.push(Patch::SetText {
                            at: at.clone(),
                            slot,
                            text: new.clone(),
                        });
                    }
                }
            }
            (Own::View(old), Own::View(new)) if old != new => {
                patches.push(Patch::SetView { at, mount: new });
            }
            _ => {}
        }
        let changed: BTreeSet<String> = then
            .keys()
            .chain(now.keys())
            .filter(|name| then.get(*name) != now.get(*name))
            .cloned()
            .collect();
        if changed.is_empty() {
            return;
        }
        for child in (0..self.plan.elements.len())
            .filter(|child| self.plan.elements[*child].parent == Some(element))
        {
            if self.shows[child].is_disjoint(&changed) {
                continue;
            }
            for below in self.under(child, chain) {
                if !touched.contains(&(child, below.clone())) && self.mounted(child, &below) {
                    self.compare(child, &below, before, touched, patches);
                }
            }
        }
    }
}

/// The tree of a view rendered afresh from every row of every layer, the
/// rows of layer `n` at `rows[n]`.
pub fn render(plan: &Plan, entity: Option<&Value>, rows: &[Vec<Row>]) -> Rendered {
    let mut maintainer = Maintainer::new(plan.clone(), entity.cloned());
    let changes: Vec<Change> = rows
        .iter()
        .enumerate()
        .map(|(layer, rows)| Change {
            layer,
            arrived: rows.clone(),
            left: Vec::new(),
        })
        .collect();
    maintainer.apply(&changes);
    maintainer.tree()
}

impl Rendered {
    /// Make a patch's change, as a renderer would: what the patches of a
    /// maintainer do to its tree before a change gives its tree after.
    pub fn apply(&mut self, patch: &Patch) -> Result<(), String> {
        fn find<'a>(list: &'a mut [Shown], key: &[Value]) -> Result<&'a mut Shown, String> {
            list.iter_mut()
                .find(|shown| shown.key == key)
                .ok_or_else(|| format!("no instance {key:?}"))
        }
        fn slot(shown: &mut Shown, at: usize) -> Result<&mut Slot, String> {
            match &mut shown.content {
                Content::Element { slots, .. } => {
                    slots.get_mut(at).ok_or_else(|| format!("no slot {at}"))
                }
                Content::View(_) => Err("a nested view has no slots".into()),
            }
        }
        fn instances(shown: &mut Shown, at: usize) -> Result<&mut Vec<Shown>, String> {
            match slot(shown, at)? {
                Slot::Instances(list) => Ok(list),
                Slot::Text(_) => Err(format!("slot {at} is a text")),
            }
        }
        fn reach<'a>(tree: &'a mut Rendered, address: &Address) -> Result<&'a mut Shown, String> {
            let ((_, root), rest) = address
                .split_first()
                .ok_or_else(|| "an empty address".to_string())?;
            let mut shown = find(&mut tree.roots, root)?;
            for (at, key) in rest {
                shown = find(instances(shown, *at)?, key)?;
            }
            Ok(shown)
        }
        match patch {
            Patch::Insert {
                parent,
                slot: at,
                index,
                instance,
            } => {
                let list = if parent.is_empty() {
                    &mut self.roots
                } else {
                    instances(reach(self, parent)?, *at)?
                };
                if *index > list.len() {
                    return Err(format!("index {index} past {}", list.len()));
                }
                list.insert(*index, instance.clone());
            }
            Patch::Remove { at } => {
                let (last, up) = at
                    .split_last()
                    .ok_or_else(|| "an empty address".to_string())?;
                let list = if up.is_empty() {
                    &mut self.roots
                } else {
                    instances(reach(self, &up.to_vec())?, last.0)?
                };
                let before = list.len();
                list.retain(|shown| shown.key != last.1);
                if list.len() == before {
                    return Err(format!("no instance {:?} to remove", last.1));
                }
            }
            Patch::SetText {
                at,
                slot: index,
                text,
            } => {
                *slot(reach(self, at)?, *index)? = Slot::Text(text.clone());
            }
            Patch::SetAttribute { at, name, value } => match &mut reach(self, at)?.content {
                Content::Element { attributes, .. } => {
                    let attribute = attributes
                        .iter_mut()
                        .find(|(written, _)| written == name)
                        .ok_or_else(|| format!("no attribute {name}"))?;
                    attribute.1 = value.clone();
                }
                Content::View(_) => return Err("a nested view has no attributes".into()),
            },
            Patch::SetEvent { at, index, event } => match &mut reach(self, at)?.content {
                Content::Element { events, .. } => {
                    *events
                        .get_mut(*index)
                        .ok_or_else(|| format!("no event {index}"))? = event.clone();
                }
                Content::View(_) => return Err("a nested view has no events".into()),
            },
            Patch::SetView { at, mount } => {
                reach(self, at)?.content = Content::View(mount.clone());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::view::plan::tests::{TODOS, view};

    fn entity(uri: &str) -> Value {
        crate::syntax::entity(uri).unwrap()
    }

    fn text_value(text: &str) -> Value {
        Value::String(text.into())
    }

    fn list(name: &str) -> Row {
        Row::from([
            ("l".into(), entity("list:home")),
            ("name".into(), text_value(name)),
        ])
    }

    fn todo(t: &str, title: &str, state: &str) -> Row {
        Row::from([
            ("l".into(), entity("list:home")),
            ("t".into(), entity(t)),
            ("title".into(), text_value(title)),
            ("s".into(), text_value(state)),
        ])
    }

    /// A card maintained by changes, checked after each against the card
    /// rendered afresh and against the card before with the patches made.
    struct Card {
        plan: Plan,
        maintainer: Maintainer,
        rows: Vec<Vec<Row>>,
        tree: Rendered,
    }

    impl Card {
        fn new(plan: Plan) -> Card {
            Card {
                rows: vec![Vec::new(); plan.layers.len()],
                maintainer: Maintainer::new(plan.clone(), Some(entity("list:home"))),
                plan,
                tree: Rendered::default(),
            }
        }

        fn change(&mut self, layer: usize, arrived: Vec<Row>, left: Vec<Row>) -> Vec<Patch> {
            for row in &left {
                let at = self.rows[layer]
                    .iter()
                    .position(|kept| kept == row)
                    .unwrap();
                self.rows[layer].remove(at);
            }
            self.rows[layer].extend(arrived.iter().cloned());
            let patches = self.maintainer.apply(&[Change {
                layer,
                arrived,
                left,
            }]);
            for patch in &patches {
                self.tree.apply(patch).unwrap();
            }
            let maintained = self.maintainer.tree();
            assert_eq!(self.tree, maintained, "the patches make the tree");
            assert_eq!(
                maintained,
                render(&self.plan, Some(&entity("list:home")), &self.rows),
                "the tree maintained is the tree rendered afresh"
            );
            patches
        }
    }

    fn card() -> Address {
        vec![(0, vec![entity("list:home")])]
    }

    fn items() -> Address {
        let mut at = card();
        at.push((1, Vec::new()));
        at
    }

    fn item(t: &str) -> Address {
        let mut at = items();
        at.push((0, vec![entity(t)]));
        at
    }

    fn li(t: &str, title: &str, state: &str) -> Shown {
        Shown {
            key: vec![entity(t)],
            content: Content::Element {
                tag: "li".into(),
                attributes: vec![("class".into(), state.into())],
                events: Vec::new(),
                slots: vec![Slot::Text(title.into())],
            },
        }
    }

    /// Each change is a patch of the instance it is behind and nothing
    /// else: an item added is one insert, a list renamed one text, an
    /// item retitled one text, completed one attribute, removed one
    /// remove; the card stays when the last item goes.
    #[test]
    fn it_patches_the_smallest_part() {
        let mut card_of = Card::new(Plan::of(&view(TODOS, "card")).unwrap());
        let home = Row::from([("l".into(), entity("list:home"))]);
        assert_eq!(
            card_of.change(0, vec![home], Vec::new()),
            [Patch::Insert {
                parent: Vec::new(),
                slot: 0,
                index: 0,
                instance: Shown {
                    key: vec![entity("list:home")],
                    content: Content::Element {
                        tag: "article".into(),
                        attributes: Vec::new(),
                        events: Vec::new(),
                        slots: vec![
                            Slot::Instances(Vec::new()),
                            Slot::Instances(vec![Shown {
                                key: Vec::new(),
                                content: Content::Element {
                                    tag: "ul".into(),
                                    attributes: Vec::new(),
                                    events: Vec::new(),
                                    slots: vec![Slot::Instances(Vec::new())],
                                },
                            }]),
                        ],
                    },
                },
            }],
            "a card with no name yet has no heading"
        );
        let patches = card_of.change(1, vec![list("Home")], Vec::new());
        assert!(matches!(
            patches.as_slice(),
            [Patch::Insert { slot: 0, .. }]
        ));

        assert_eq!(
            card_of.change(2, vec![todo("todo:1", "Buy milk", "open")], Vec::new()),
            [Patch::Insert {
                parent: items(),
                slot: 0,
                index: 0,
                instance: li("todo:1", "Buy milk", "open"),
            }]
        );
        assert_eq!(
            card_of.change(2, vec![todo("todo:2", "Buy milk", "open")], Vec::new()),
            [Patch::Insert {
                parent: items(),
                slot: 0,
                index: 1,
                instance: li("todo:2", "Buy milk", "open"),
            }],
            "two to-dos of one title are two items"
        );
        let mut heading = card();
        heading.push((0, Vec::new()));
        assert_eq!(
            card_of.change(1, vec![list("Chores")], vec![list("Home")]),
            [Patch::SetText {
                at: heading,
                slot: 0,
                text: "Chores".into(),
            }]
        );
        assert_eq!(
            card_of.change(
                2,
                vec![todo("todo:1", "Buy oat milk", "open")],
                vec![todo("todo:1", "Buy milk", "open")]
            ),
            [Patch::SetText {
                at: item("todo:1"),
                slot: 0,
                text: "Buy oat milk".into(),
            }]
        );
        assert_eq!(
            card_of.change(
                2,
                vec![todo("todo:1", "Buy oat milk", "done")],
                vec![todo("todo:1", "Buy oat milk", "open")]
            ),
            [Patch::SetAttribute {
                at: item("todo:1"),
                name: "class".into(),
                value: "done".into(),
            }]
        );
        assert_eq!(
            card_of.change(2, Vec::new(), vec![todo("todo:2", "Buy milk", "open")]),
            [Patch::Remove { at: item("todo:2") }]
        );
        // Another list's to-do is behind no instance of this card.
        let mut elsewhere = todo("todo:9", "Elsewhere", "open");
        elsewhere.insert("l".into(), entity("list:work"));
        assert_eq!(card_of.change(2, vec![elsewhere], Vec::new()), []);
        assert_eq!(
            card_of.change(2, Vec::new(), vec![todo("todo:1", "Buy oat milk", "done")]),
            [Patch::Remove { at: item("todo:1") }]
        );
        let Content::Element { slots, .. } = &card_of.tree.roots[0].content else {
            panic!("an article");
        };
        assert_eq!(slots.len(), 2, "the card stays with its heading and list");
    }

    /// A row that arrives under an instance not yet in the tree waits:
    /// the item is inserted with the card, inside the card's insert.
    #[test]
    fn it_mounts_what_waited_with_its_parent() {
        let mut card_of = Card::new(Plan::of(&view(TODOS, "card")).unwrap());
        assert_eq!(
            card_of.change(2, vec![todo("todo:1", "Buy milk", "open")], Vec::new()),
            []
        );
        let patches = card_of.change(
            0,
            vec![Row::from([("l".into(), entity("list:home"))])],
            Vec::new(),
        );
        let [Patch::Insert { instance, .. }] = patches.as_slice() else {
            panic!("{patches:?}");
        };
        let Content::Element { slots, .. } = &instance.content else {
            panic!("an article");
        };
        let Slot::Instances(lists) = &slots[1] else {
            panic!("the list");
        };
        let Content::Element { slots, .. } = &lists[0].content else {
            panic!("a ul");
        };
        assert_eq!(
            slots[0],
            Slot::Instances(vec![li("todo:1", "Buy milk", "open")])
        );
    }

    /// One query over every condition has no row for a list with no
    /// to-dos, and so no card.
    #[test]
    fn it_renders_nothing_for_an_empty_list_from_one_query() {
        let plan = Plan::single(&view(TODOS, "card")).unwrap();
        let mut row = list("Home");
        row.insert("l".into(), entity("list:home"));
        assert_eq!(
            render(&plan, Some(&entity("list:home")), &[Vec::new()]),
            Rendered::default()
        );
        let layered = Plan::of(&view(TODOS, "card")).unwrap();
        let tree = render(
            &layered,
            Some(&entity("list:home")),
            &[
                vec![Row::from([("l".into(), entity("list:home"))])],
                vec![row],
                Vec::new(),
            ],
        );
        assert_eq!(tree.roots.len(), 1, "the layered card is there, empty");
    }
}
