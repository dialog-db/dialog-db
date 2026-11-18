//! A view's layers: which of its conditions each element of its template
//! takes, and what tells the element's instances apart.
//!
//! A template is a tree of elements. Each element that holds variables
//! takes the conditions that bind them, given what the elements above it
//! bound: its layer. An element with a layer of its own has its
//! instances from that layer's rows; one without has them from the
//! nearest layer above it. No condition is folded: each layer is a query
//! whose row is the keys of the elements above it and the variables of
//! its own that the elements below it hold.
//!
//! The rule a condition is placed by, as
//! [design-view-maintenance.md](../../../../design/macros/design-view-maintenance.md)
//! has it, read top down:
//!
//! - the entity rendered is bound above the root, by whoever renders the
//!   view, and the root takes the premises that make it an instance of
//!   the concept ([`View::instance`]);
//! - an element takes a condition that binds a variable it holds and no
//!   element above it bound;
//! - an element takes a condition that binds a variable held below it in
//!   two of its children or more: it is their common ancestor. The
//!   specification says "the highest element that needs it", which two
//!   siblings tie for; this is the managing agent's reading;
//! - an element takes a condition that shares a variable with those it
//!   took, where no element above bound that variable, unless the
//!   condition binds a variable held below, which is left to the element
//!   below that holds it;
//! - an element takes a condition whose every variable is bound at it, a
//!   filter where its last variable is bound;
//! - a condition no element takes belongs to the root.
//!
//! What surprised the rule is kept beside the plan, a [`Case`] each.

use std::collections::BTreeSet;

use dialog_corpus::Value;
use dialog_query::ConceptConclusion;

use crate::Error;
use crate::compile::{Query, Step, compile};
use crate::kernel::{self, Keyed, Premise, Term};
use crate::syntax::{Parameters, Syntax};
use crate::view::{Attribute, Input, Nested, Node, Piece, View};

/// What stands at a place in the template.
#[derive(Clone, Debug, PartialEq)]
pub enum Placed {
    /// An element, by its tag.
    Element(String),
    /// A view nested by reference.
    View(Nested),
}

/// A place in the template that has instances: an element or a nested
/// view.
#[derive(Clone, Debug, PartialEq)]
pub struct Place {
    /// Where it is: the index of each child taken from the template's
    /// root down to it. The root's is empty.
    pub path: Vec<usize>,
    /// The place it stands in, by its index in [`Plan::elements`].
    pub parent: Option<usize>,
    /// What stands there.
    pub placed: Placed,
    /// The variables it holds directly: in its attributes, its text
    /// children, its events' terms, or a nested reference's entries.
    pub holds: Vec<String>,
    /// The variables bound at it: by the entity given, the layers above
    /// it, and its own.
    pub bound: Vec<String>,
    /// What tells its instances apart under one instance of its parent.
    pub key: Vec<String>,
    /// The layer its instances come from, by its index in
    /// [`Plan::layers`].
    pub layer: usize,
}

/// Why a condition is in a layer.
#[derive(Clone, Debug, PartialEq)]
pub enum Why {
    /// It makes the entity rendered an instance of the concept.
    Instance,
    /// It binds this variable, which the element holds.
    Binds(String),
    /// It binds this variable, which two children of the element or more
    /// hold below it.
    Shared(String),
    /// It shares this variable, bound by no element above, with a
    /// condition the element took.
    Pulled(String),
    /// Every variable it holds is bound at the element.
    Filter,
    /// No element needs what it binds: it belongs to the root.
    Unneeded,
    /// Every condition is at the root: the reading that is one query.
    Single,
}

/// A condition of a layer, as a kernel premise, and why it is there.
#[derive(Clone, Debug, PartialEq)]
pub struct Condition {
    /// The premise. `(unless …)` stands as it does in a rule.
    pub premise: Syntax,
    /// Why the layer holds it.
    pub why: Why,
}

/// The conditions an element takes, and the row its query gives.
#[derive(Clone, Debug, PartialEq)]
pub struct Layer {
    /// The element that takes them, by its index in [`Plan::elements`].
    pub element: usize,
    /// Its conditions, in the order the view writes them, those of the
    /// instance first.
    pub conditions: Vec<Condition>,
    /// The variables a row of its query binds: the keys of the elements
    /// above it, then its own variables that it or an element below it
    /// holds or is told apart by.
    pub row: Vec<String>,
}

/// Where the rule gave an answer that needs saying.
#[derive(Clone, Debug, PartialEq)]
pub enum Case {
    /// Several children hold what a condition binds, so it is placed at
    /// their common ancestor.
    Shared {
        /// The condition.
        condition: Syntax,
        /// Where it is placed.
        at: usize,
        /// The children below which it is needed.
        below: Vec<usize>,
    },
    /// No element needs what a condition binds, so it is at the root:
    /// the view is there only while it holds.
    Unneeded {
        /// The condition.
        condition: Syntax,
    },
    /// An element holds a variable no layer at it or above it binds.
    Unbound {
        /// The element.
        element: usize,
        /// The variable.
        variable: String,
        /// Whether an event of the element is what holds it.
        event: bool,
    },
    /// A layer's conditions do not bind a key of an element above it, so
    /// its rows are not tied to one instance of that element.
    Unjoined {
        /// The layer.
        layer: usize,
        /// The key it does not bind.
        variable: String,
    },
}

/// A view laid out in layers: what a back end that maintains it walks.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    /// The concept the view renders.
    pub concept: String,
    /// The view's name.
    pub name: String,
    /// The variable that is the entity rendered.
    pub entity: String,
    /// The template, as the view describes it.
    pub template: Node,
    /// The template's elements and nested views, the root first, each
    /// after its parent, in the order written.
    pub elements: Vec<Place>,
    /// The layers. The root's is first.
    pub layers: Vec<Layer>,
    /// What surprised the rule.
    pub cases: Vec<Case>,
}

/// A premise, with what it holds and what it binds.
struct Info {
    premise: Syntax,
    vars: BTreeSet<String>,
    binds: BTreeSet<String>,
}

fn info(premise: &Syntax) -> Info {
    let mut vars = Vec::new();
    premise.variables(&mut vars);
    let vars: BTreeSet<String> = vars.into_iter().collect();
    // A negation binds nothing. A formula binds what it gives, `is:`; a
    // check gives nothing. A read binds every variable it holds.
    let binds = if premise.applies() == Some("unless") {
        BTreeSet::new()
    } else if premise.is_formula() {
        let mut given = Vec::new();
        if let Some(is) = premise
            .operand()
            .and_then(|operand| operand.terms.get("is"))
        {
            is.variables(&mut given);
        }
        given.into_iter().collect()
    } else {
        vars.clone()
    };
    Info {
        premise: premise.clone(),
        vars,
        binds,
    }
}

fn add(name: &str, into: &mut Vec<String>) {
    if !into.iter().any(|known| known == name) {
        into.push(name.to_string());
    }
}

fn pieces(parts: &[Piece], into: &mut Vec<String>) {
    for piece in parts {
        if let Piece::Variable(name) = piece {
            add(name, into);
        }
    }
}

/// The variables a nested reference holds.
fn nested_holds(nested: &Nested) -> Vec<String> {
    let mut holds = Vec::new();
    if let Some(of) = &nested.of {
        add(of, &mut holds);
    }
    for (_, parts) in &nested.entries {
        pieces(parts, &mut holds);
    }
    holds
}

/// The variables an element holds directly, and those of them its
/// events hold.
fn element_holds(element: &crate::view::Element) -> (Vec<String>, Vec<String>) {
    let mut holds = Vec::new();
    for (_, attribute) in &element.attributes {
        match attribute {
            Attribute::Variable(name) => add(name, &mut holds),
            Attribute::Parts(parts) => pieces(parts, &mut holds),
            _ => {}
        }
    }
    let mut events = Vec::new();
    for event in &element.events {
        for (_, input) in &event.fields {
            if let Input::Variable(name) = input {
                add(name, &mut holds);
                add(name, &mut events);
            }
        }
    }
    for child in &element.children {
        if let Node::Variable(name) = child {
            add(name, &mut holds);
        }
    }
    (holds, events)
}

/// The places of a template, in pre-order, and the variables each one's
/// events hold.
fn places(
    node: &Node,
    path: Vec<usize>,
    parent: Option<usize>,
    into: &mut Vec<Place>,
    events: &mut Vec<Vec<String>>,
) {
    match node {
        Node::Element(element) => {
            let (holds, held) = element_holds(element);
            let at = into.len();
            into.push(Place {
                path: path.clone(),
                parent,
                placed: Placed::Element(element.tag.clone()),
                holds,
                bound: Vec::new(),
                key: Vec::new(),
                layer: 0,
            });
            events.push(held);
            for (index, child) in element.children.iter().enumerate() {
                let mut below = path.clone();
                below.push(index);
                places(child, below, Some(at), into, events);
            }
        }
        Node::View(nested) => {
            into.push(Place {
                path,
                parent,
                placed: Placed::View(nested.clone()),
                holds: nested_holds(nested),
                bound: Vec::new(),
                key: Vec::new(),
                layer: 0,
            });
            events.push(Vec::new());
        }
        Node::Text(_) | Node::Value(_) | Node::Variable(_) => {}
    }
}

/// Whether `ancestor` is `element` or stands above it.
fn above(elements: &[Place], ancestor: usize, element: usize) -> bool {
    let mut at = Some(element);
    while let Some(index) = at {
        if index == ancestor {
            return true;
        }
        at = elements[index].parent;
    }
    false
}

impl Plan {
    /// Lay a view out in layers by the rule above.
    pub fn of(view: &View) -> Result<Plan, Error> {
        let (mut elements, events) = Plan::places(view)?;
        let count = elements.len();
        let children: Vec<Vec<usize>> = (0..count)
            .map(|at| {
                (0..count)
                    .filter(|child| elements[*child].parent == Some(at))
                    .collect()
            })
            .collect();
        // What is held strictly below each element, and in its subtree.
        let mut below: Vec<BTreeSet<String>> = vec![BTreeSet::new(); count];
        for at in (0..count).rev() {
            if let Some(parent) = elements[at].parent {
                let mut held: BTreeSet<String> = below[at].clone();
                held.extend(elements[at].holds.iter().cloned());
                below[parent].extend(held);
            }
        }
        let subtree = |at: usize| -> BTreeSet<String> {
            let mut held = below[at].clone();
            held.extend(elements[at].holds.iter().cloned());
            held
        };

        let infos: Vec<Info> = view.conditions.iter().map(info).collect();
        let mut free: Vec<usize> = (0..infos.len()).collect();
        let mut cases = Vec::new();
        let mut layers: Vec<Layer> = Vec::new();
        let mut layer_of = vec![0usize; count];
        let mut bound_at: Vec<BTreeSet<String>> = vec![BTreeSet::new(); count];

        for at in 0..count {
            let above_here: BTreeSet<String> = match elements[at].parent {
                Some(parent) => bound_at[parent].clone(),
                None => BTreeSet::from([view.entity.clone()]),
            };
            let mut taken: Vec<Condition> = Vec::new();
            let mut bound = above_here.clone();
            if at == 0 {
                for premise in &view.instance {
                    info(premise).vars.into_iter().for_each(|name| {
                        bound.insert(name);
                    });
                    taken.push(Condition {
                        premise: premise.clone(),
                        why: Why::Instance,
                    });
                }
            }
            let needed: BTreeSet<String> = elements[at]
                .holds
                .iter()
                .filter(|name| !above_here.contains(*name))
                .cloned()
                .collect();
            let take = |index: usize,
                        why: Why,
                        free: &mut Vec<usize>,
                        taken: &mut Vec<Condition>,
                        bound: &mut BTreeSet<String>| {
                free.retain(|other| *other != index);
                bound.extend(infos[index].vars.iter().cloned());
                taken.push(Condition {
                    premise: infos[index].premise.clone(),
                    why,
                });
            };
            // What binds a variable this element holds.
            for index in free.clone() {
                if let Some(name) = infos[index].binds.intersection(&needed).next().cloned() {
                    take(index, Why::Binds(name), &mut free, &mut taken, &mut bound);
                }
            }
            // What binds a variable two children or more hold below.
            for index in free.clone() {
                let fresh: BTreeSet<String> = infos[index]
                    .binds
                    .difference(&above_here)
                    .cloned()
                    .collect();
                let sharing: Vec<usize> = children[at]
                    .iter()
                    .copied()
                    .filter(|child| !fresh.is_disjoint(&subtree(*child)))
                    .collect();
                if sharing.len() >= 2 {
                    let name = fresh
                        .iter()
                        .find(|name| sharing.iter().all(|child| subtree(*child).contains(*name)))
                        .or_else(|| fresh.iter().next())
                        .cloned()
                        .unwrap_or_default();
                    cases.push(Case::Shared {
                        condition: infos[index].premise.clone(),
                        at,
                        below: sharing,
                    });
                    take(index, Why::Shared(name), &mut free, &mut taken, &mut bound);
                }
            }
            // What shares a variable bound here first with what was taken,
            // unless it binds one held below.
            loop {
                let local: BTreeSet<String> = bound.difference(&above_here).cloned().collect();
                let pulled = free.iter().copied().find_map(|index| {
                    if infos[index].vars.is_subset(&bound) {
                        return None;
                    }
                    let shared = infos[index].vars.intersection(&local).next()?.clone();
                    let fresh: BTreeSet<String> =
                        infos[index].binds.difference(&bound).cloned().collect();
                    fresh.is_disjoint(&below[at]).then_some((index, shared))
                });
                match pulled {
                    Some((index, shared)) => take(
                        index,
                        Why::Pulled(shared),
                        &mut free,
                        &mut taken,
                        &mut bound,
                    ),
                    None => break,
                }
            }
            // What is all bound here: a filter.
            for index in free.clone() {
                if infos[index].vars.is_subset(&bound) {
                    take(index, Why::Filter, &mut free, &mut taken, &mut bound);
                }
            }
            if at == 0 || !taken.is_empty() {
                layer_of[at] = layers.len();
                layers.push(Layer {
                    element: at,
                    conditions: taken,
                    row: Vec::new(),
                });
            } else if let Some(parent) = elements[at].parent {
                layer_of[at] = layer_of[parent];
            }
            bound_at[at] = bound;
        }
        // What no element took is the root's.
        for index in free {
            cases.push(Case::Unneeded {
                condition: infos[index].premise.clone(),
            });
            layers[0].conditions.push(Condition {
                premise: infos[index].premise.clone(),
                why: Why::Unneeded,
            });
        }
        for (at, element) in elements.iter_mut().enumerate() {
            element.layer = layer_of[at];
            element.bound = bound_at[at].iter().cloned().collect();
        }
        let mut plan = Plan {
            concept: view.concept.clone(),
            name: view.name.clone(),
            entity: view.entity.clone(),
            template: view.template.clone(),
            elements,
            layers,
            cases,
        };
        plan.finish(&events);
        Ok(plan)
    }

    /// Every condition at the root, one query over them all: the reading
    /// in which a view is one query whose rows fill the template.
    pub fn single(view: &View) -> Result<Plan, Error> {
        let (mut elements, events) = Plan::places(view)?;
        let mut conditions: Vec<Condition> = view
            .instance
            .iter()
            .map(|premise| Condition {
                premise: premise.clone(),
                why: Why::Instance,
            })
            .collect();
        conditions.extend(view.conditions.iter().map(|premise| Condition {
            premise: premise.clone(),
            why: Why::Single,
        }));
        let mut bound = BTreeSet::from([view.entity.clone()]);
        for condition in &conditions {
            bound.extend(info(&condition.premise).vars);
        }
        for element in &mut elements {
            element.bound = bound.iter().cloned().collect();
        }
        let mut plan = Plan {
            concept: view.concept.clone(),
            name: view.name.clone(),
            entity: view.entity.clone(),
            template: view.template.clone(),
            elements,
            layers: vec![Layer {
                element: 0,
                conditions,
                row: Vec::new(),
            }],
            cases: Vec::new(),
        };
        plan.finish(&events);
        Ok(plan)
    }

    fn places(view: &View) -> Result<(Vec<Place>, Vec<Vec<String>>), Error> {
        let mut elements = Vec::new();
        let mut events = Vec::new();
        places(&view.template, Vec::new(), None, &mut elements, &mut events);
        if elements.is_empty() {
            return Err(Error::Refused(format!(
                "the view {} :{} renders no element: its template is {:?}",
                view.concept, view.name, view.template
            )));
        }
        Ok((elements, events))
    }

    /// Keys, rows, and the cases of an unbound variable and an unjoined
    /// layer, once the conditions are placed.
    fn finish(&mut self, events: &[Vec<String>]) {
        for (at, held) in events.iter().enumerate() {
            let unbound: Vec<String> = self.elements[at]
                .holds
                .iter()
                .filter(|name| !self.elements[at].bound.contains(*name))
                .cloned()
                .collect();
            for variable in unbound {
                let event = held.contains(&variable);
                self.cases.push(Case::Unbound {
                    element: at,
                    variable,
                    event,
                });
            }
        }
        for at in 0..self.elements.len() {
            let key = self.key(at);
            self.elements[at].key = key;
        }
        for index in 0..self.layers.len() {
            let owner = self.layers[index].element;
            let mut row = Vec::new();
            let mut at = self.elements[owner].parent;
            let mut ancestors = Vec::new();
            while let Some(parent) = at {
                ancestors.push(parent);
                at = self.elements[parent].parent;
            }
            for ancestor in ancestors.iter().rev() {
                for name in &self.elements[*ancestor].key {
                    add(name, &mut row);
                }
            }
            let mut own = BTreeSet::new();
            for condition in &self.layers[index].conditions {
                own.extend(info(&condition.premise).binds);
            }
            for name in &row {
                if !own.contains(name) {
                    self.cases.push(Case::Unjoined {
                        layer: index,
                        variable: name.clone(),
                    });
                }
            }
            // Its own variables that it or an element below holds or is
            // told apart by, in the order the elements are.
            for element in 0..self.elements.len() {
                if !above(&self.elements, owner, element) {
                    continue;
                }
                let place = &self.elements[element];
                for name in place.key.iter().chain(&place.holds) {
                    if own.contains(name) {
                        add(name, &mut row);
                    }
                }
            }
            self.layers[index].row = row;
        }
    }

    /// The reads and formulas of the layers on the way from the root to
    /// an element, its own included.
    fn premises(&self, element: usize) -> Vec<Premise> {
        self.layers
            .iter()
            .filter(|layer| above(&self.elements, layer.element, element))
            .flat_map(|layer| &layer.conditions)
            .filter(|condition| condition.premise.applies() != Some("unless"))
            .filter_map(|condition| kernel::premise(&condition.premise).ok())
            .collect()
    }

    /// What tells an element's instances apart: the root by the entity;
    /// any other by the variables it holds that the keys above it do not
    /// already fix, each said by the entity it is a field of where that
    /// field has one value, a collection's position first.
    fn key(&self, element: usize) -> Vec<String> {
        if element == 0 {
            return vec![self.entity.clone()];
        }
        let premises = self.premises(element);
        let mut known: BTreeSet<String> = BTreeSet::from([self.entity.clone()]);
        let mut at = self.elements[element].parent;
        while let Some(parent) = at {
            known.extend(self.elements[parent].key.iter().cloned());
            at = self.elements[parent].parent;
        }
        let mut key: Vec<String> = Vec::new();
        let mut positions: Vec<String> = Vec::new();
        for name in self.elements[element].holds.clone() {
            let mut visiting = BTreeSet::new();
            ensure(
                &name,
                &premises,
                &known,
                &mut key,
                &mut positions,
                &mut visiting,
            );
        }
        let (mut first, rest): (Vec<String>, Vec<String>) =
            key.into_iter().partition(|name| positions.contains(name));
        first.extend(rest);
        first
    }

    /// The query of a layer: its conditions, the entity rendered put in
    /// where it is given, and a row of [`Layer::row`]. Read a row with
    /// [`Plan::row`].
    pub fn query(&self, layer: usize, entity: Option<&Value>) -> Result<Query, Error> {
        let at = self
            .layers
            .get(layer)
            .ok_or_else(|| Error::Refused(format!("the view has no layer {layer}")))?;
        let given = entity.map(|value| Syntax::Constant(value.clone()));
        let premises: Vec<Syntax> = at
            .conditions
            .iter()
            .map(|condition| match &given {
                Some(value) => substitute(&condition.premise, &self.entity, value),
                None => condition.premise.clone(),
            })
            .collect();
        let row: Vec<&String> = at
            .row
            .iter()
            .filter(|name| given.is_none() || **name != self.entity)
            .collect();
        // The head's entity: the entity rendered, or the first a read of
        // the layer is of.
        let subject = given.clone().or_else(|| {
            premises
                .iter()
                .find_map(|premise| match kernel::premise(premise) {
                    Ok(Premise::Read(read)) => match read.entity {
                        Term::Variable(name) if name == self.entity => {
                            Some(Syntax::Variable(name.clone()))
                        }
                        _ => None,
                    },
                    _ => None,
                })
        });
        let subject = subject
            .or_else(|| {
                premises
                    .iter()
                    .find_map(|premise| match kernel::premise(premise) {
                        Ok(Premise::Read(read)) => match read.entity {
                            Term::Variable(name) => Some(Syntax::Variable(name)),
                            _ => None,
                        },
                        _ => None,
                    })
            })
            .ok_or_else(|| {
                Error::Refused(format!(
                    "layer {layer} of the view {} :{} reads no entity",
                    self.concept, self.name
                ))
            })?;
        let head = Syntax::Application(
            Box::new(Syntax::Reference("view.layer".into())),
            Parameters {
                terms: if row.is_empty() {
                    // A read names a field: a row of the entity alone
                    // says it under its own variable's name.
                    std::collections::BTreeMap::from([(self.entity.clone(), subject.clone())])
                } else {
                    row.iter()
                        .map(|name| ((*name).clone(), Syntax::Variable((*name).clone())))
                        .collect()
                },
                items: vec![subject],
            },
        );
        let mut items = vec![Syntax::Application(
            Box::new(Syntax::Reference("report".into())),
            Parameters {
                items: vec![head],
                ..Parameters::default()
            },
        )];
        items.extend(premises);
        let rule = Syntax::Application(
            Box::new(Syntax::Reference("rule".into())),
            Parameters {
                items,
                ..Parameters::default()
            },
        );
        match compile(&rule)?.as_slice() {
            [Step::Query(query)] => Ok((**query).clone()),
            other => Err(Error::Refused(format!(
                "layer {layer} compiles to {} steps, not one query",
                other.len()
            ))),
        }
    }

    /// A match of a layer's query as a row: each variable of
    /// [`Layer::row`] and what it was bound to, the entity rendered too
    /// where it was given.
    pub fn row(
        &self,
        query: &Query,
        conclusion: &ConceptConclusion,
        entity: Option<&Value>,
    ) -> Result<std::collections::BTreeMap<String, Value>, Error> {
        let mut row = query.values(conclusion)?;
        if let Some(entity) = entity {
            row.insert(self.entity.clone(), entity.clone());
        }
        Ok(row)
    }

    /// The part of the template a place stands for.
    pub fn node(&self, element: usize) -> &Node {
        let mut node = &self.template;
        for index in &self.elements[element].path {
            if let Node::Element(parent) = node {
                node = &parent.children[*index];
            }
        }
        node
    }

    /// The places whose instances come from a layer.
    pub fn region(&self, layer: usize) -> Vec<usize> {
        (0..self.elements.len())
            .filter(|at| self.elements[*at].layer == layer)
            .collect()
    }
}

/// Add to the key what fixes `name`, given what is known.
fn ensure(
    name: &str,
    premises: &[Premise],
    known: &BTreeSet<String>,
    key: &mut Vec<String>,
    positions: &mut Vec<String>,
    visiting: &mut BTreeSet<String>,
) {
    let mut fixed: BTreeSet<String> = known.clone();
    fixed.extend(key.iter().cloned());
    if closure(&fixed, premises).contains(name) || !visiting.insert(name.to_string()) {
        return;
    }
    // A field of one value fixes its variable once its entity, and its
    // key where it is an entry of a collection, are fixed.
    let field = premises.iter().find_map(|premise| match premise {
        Premise::Read(read) => read.fields.iter().find_map(|field| {
            let one = field.pick.as_deref() == Some("last");
            match &field.term {
                Term::Variable(held) if held == name && one => {
                    Some((read.entity.clone(), field.key.clone()))
                }
                _ => None,
            }
        }),
        Premise::Formula(_) => None,
    });
    match field {
        Some((entity, entry)) => {
            if let Some((Term::Variable(position), keyed)) = entry {
                if keyed == Keyed::Position && !positions.contains(&position) {
                    positions.push(position.clone());
                }
                ensure(&position, premises, known, key, positions, visiting);
            }
            if let Term::Variable(entity) = entity {
                ensure(&entity, premises, known, key, positions, visiting);
            }
            let mut fixed: BTreeSet<String> = known.clone();
            fixed.extend(key.iter().cloned());
            if !closure(&fixed, premises).contains(name) {
                add(name, key);
            }
        }
        None => {
            // An entry of a collection under a variable key: the key is a
            // key of its own, and a position is said first.
            for premise in premises {
                if let Premise::Read(read) = premise {
                    for field in &read.fields {
                        if let Some((Term::Variable(held), Keyed::Position)) = &field.key
                            && held == name
                            && !positions.contains(held)
                        {
                            positions.push(held.clone());
                        }
                    }
                }
            }
            add(name, key);
        }
    }
}

/// What the variables in `fixed` fix: a field of one value of a fixed
/// entity, at a fixed key where it is an entry, and what a formula gives
/// of fixed inputs.
fn closure(fixed: &BTreeSet<String>, premises: &[Premise]) -> BTreeSet<String> {
    let is_fixed = |term: &Term, fixed: &BTreeSet<String>| match term {
        Term::Variable(name) => fixed.contains(name),
        Term::Blank => false,
        _ => true,
    };
    let mut fixed = fixed.clone();
    loop {
        let before = fixed.len();
        for premise in premises {
            match premise {
                Premise::Read(read) => {
                    if !is_fixed(&read.entity, &fixed) {
                        continue;
                    }
                    for field in &read.fields {
                        let entry = field
                            .key
                            .as_ref()
                            .is_none_or(|(key, _)| is_fixed(key, &fixed));
                        if field.pick.as_deref() == Some("last")
                            && entry
                            && let Term::Variable(name) = &field.term
                        {
                            fixed.insert(name.clone());
                        }
                    }
                }
                Premise::Formula(formula) => {
                    let inputs = formula
                        .subject
                        .iter()
                        .chain(
                            formula
                                .labelled
                                .iter()
                                .filter(|(label, _)| label != "is")
                                .map(|(_, term)| term),
                        )
                        .all(|term| is_fixed(term, &fixed));
                    if inputs && let Some(Term::Variable(name)) = formula.at("is") {
                        fixed.insert(name.clone());
                    }
                }
            }
        }
        if fixed.len() == before {
            return fixed;
        }
    }
}

/// A tree with a variable put in by a value.
fn substitute(tree: &Syntax, name: &str, by: &Syntax) -> Syntax {
    match tree {
        Syntax::Variable(variable) if variable == name => by.clone(),
        Syntax::Application(operator, operand) => Syntax::Application(
            Box::new(substitute(operator, name, by)),
            Parameters {
                items: operand
                    .items
                    .iter()
                    .map(|item| substitute(item, name, by))
                    .collect(),
                terms: operand
                    .terms
                    .iter()
                    .map(|(label, term)| (label.clone(), substitute(term, name, by)))
                    .collect(),
            },
        ),
        Syntax::Map(entries) => Syntax::Map(
            entries
                .iter()
                .map(|(key, value)| (substitute(key, name, by), substitute(value, name, by)))
                .collect(),
        ),
        Syntax::Tuple(items) => Syntax::Tuple(
            items
                .iter()
                .map(|item| substitute(item, name, by))
                .collect(),
        ),
        other => other.clone(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{Analyzer, parse};

    /// A list, its to-dos, and the card that shows them.
    pub(crate) const TODOS: &str = r#"
        (concept (list ?l name: ?name)
          (xyz.list/name of: ?l is: ?name as: :text cardinality: :one))
        (concept (todo ?t list: ?l title: ?title state: ?s)
          (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
          (xyz.todo/title of: ?t is: ?title as: :text cardinality: :one)
          (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))
        (view list :card
          [:article
            [:h2 ?name]
            [:ul [:li {:class ?s} ?title]]]
          (list ?l name: ?name)
          (todo ?t list: ?l title: ?title state: ?s))"#;

    /// The view named `name` of a document, as analysis describes it.
    pub(crate) fn view(document: &str, name: &str) -> View {
        let analysis = Analyzer::new().unwrap().analyze(&parse(document).unwrap());
        assert!(
            analysis.diagnostics.iter().all(|d| d.refusal.is_none()),
            "{:?}",
            analysis.diagnostics
        );
        analysis
            .analyzed
            .iter()
            .filter(|tree| tree.applies() == Some("view"))
            .map(|tree| View::describe(tree).unwrap())
            .find(|view| view.name == name)
            .expect("the view")
    }

    fn whys(plan: &Plan, layer: usize) -> Vec<Why> {
        plan.layers[layer]
            .conditions
            .iter()
            .map(|condition| condition.why.clone())
            .collect()
    }

    /// The card's heading takes the list's name, its item the to-do, and
    /// the root the list being a list. An item is told apart by its
    /// to-do, not by the title it shows, and the heading by nothing: one
    /// a card.
    #[test]
    fn it_gives_each_element_the_conditions_that_bind_what_it_holds() {
        let plan = Plan::of(&view(TODOS, "card")).unwrap();
        let tags: Vec<String> = plan
            .elements
            .iter()
            .map(|place| match &place.placed {
                Placed::Element(tag) => tag.clone(),
                Placed::View(nested) => nested.view.clone(),
            })
            .collect();
        assert_eq!(tags, ["article", "h2", "ul", "li"]);
        let layered: Vec<usize> = plan.layers.iter().map(|layer| layer.element).collect();
        assert_eq!(layered, [0, 1, 3]);
        assert_eq!(whys(&plan, 0), [Why::Instance]);
        assert_eq!(whys(&plan, 1), [Why::Binds("name".into())]);
        assert_eq!(whys(&plan, 2), [Why::Binds("s".into())]);
        assert!(
            plan.layers[1].conditions[0]
                .premise
                .to_string()
                .starts_with("(xyz.list ?l ")
        );
        assert!(
            plan.layers[2].conditions[0]
                .premise
                .to_string()
                .starts_with("(xyz.todo ?t ")
        );
        let keys: Vec<Vec<String>> = plan
            .elements
            .iter()
            .map(|place| place.key.clone())
            .collect();
        assert_eq!(
            keys,
            [vec!["l".to_string()], vec![], vec![], vec!["t".into()]]
        );
        let rows: Vec<Vec<String>> = plan.layers.iter().map(|layer| layer.row.clone()).collect();
        assert_eq!(
            rows,
            [
                vec!["l".to_string()],
                vec!["l".into(), "name".into()],
                vec!["l".into(), "t".into(), "s".into(), "title".into()],
            ]
        );
        assert_eq!(
            plan.elements[2].layer, 0,
            "the list holds nothing: the root's"
        );
        assert!(plan.cases.is_empty(), "{:?}", plan.cases);
    }

    /// One query over every condition: one layer, at the root.
    #[test]
    fn it_puts_every_condition_at_the_root_when_asked() {
        let plan = Plan::single(&view(TODOS, "card")).unwrap();
        assert_eq!(plan.layers.len(), 1);
        assert_eq!(whys(&plan, 0), [Why::Instance, Why::Single, Why::Single]);
        assert!(plan.elements.iter().all(|place| place.layer == 0));
        assert_eq!(plan.elements[3].key, ["t"]);
    }

    /// Where the rule surprises: two siblings that hold one variable
    /// share the condition at their parent; a join on a variable no
    /// element holds is pulled into the element whose condition it ties;
    /// a condition no element needs is the root's, so the card is there
    /// only while it holds; a negation is a filter where its variables
    /// are bound.
    #[test]
    fn it_says_where_the_rule_surprises() {
        let shared = format!(
            "{TODOS}
             (view list :twice [:div [:p ?name] [:p ?name]] (list ?l name: ?name))"
        );
        let plan = Plan::of(&view(&shared, "twice")).unwrap();
        assert_eq!(whys(&plan, 0), [Why::Instance, Why::Shared("name".into())]);
        assert!(matches!(
            plan.cases.as_slice(),
            [Case::Shared { at: 0, below, .. }] if below == &[1, 2]
        ));

        let joined = format!(
            "{TODOS}
             (view list :joined [:ul [:li ?title]]
               (list ?l)
               (todo ?t title: ?title)
               (todo ?t list: ?l))"
        );
        let plan = Plan::of(&view(&joined, "joined")).unwrap();
        assert_eq!(plan.layers.len(), 2);
        assert_eq!(plan.layers[1].element, 1);
        assert_eq!(
            whys(&plan, 1),
            [Why::Binds("title".into()), Why::Filter],
            "the join to the list is a filter where ?t is bound"
        );

        let pulled = format!(
            "{TODOS}
             (concept (assigned ?t to: ?p)
               (xyz.todo/assignee of: ?t is: ?p as: :entity cardinality: :one))
             (concept (person ?p name: ?n)
               (xyz.person/name of: ?p is: ?n as: :text cardinality: :one))
             (view list :assigned [:ul [:li ?title]]
               (list ?l)
               (todo ?t list: ?l title: ?title)
               (assigned ?t to: ?p)
               (person ?p name: ?n))"
        );
        let plan = Plan::of(&view(&pulled, "assigned")).unwrap();
        assert_eq!(
            whys(&plan, 1),
            [
                Why::Binds("title".into()),
                Why::Pulled("t".into()),
                Why::Pulled("p".into())
            ]
        );
        assert_eq!(plan.elements[1].key, ["t"]);

        let unneeded = format!(
            "{TODOS}
             (view list :busy [:h2 ?name] (list ?l name: ?name) (todo ?t list: ?l))"
        );
        let plan = Plan::of(&view(&unneeded, "busy")).unwrap();
        assert_eq!(
            whys(&plan, 0),
            [Why::Instance, Why::Binds("name".into()), Why::Unneeded]
        );
        assert!(matches!(plan.cases.as_slice(), [Case::Unneeded { .. }]));

        let filtered = format!(
            "{TODOS}
             (view list :open [:ul [:li ?title]]
               (list ?l)
               (todo ?t list: ?l title: ?title)
               (unless (todo ?t state: :done)))"
        );
        let plan = Plan::of(&view(&filtered, "open")).unwrap();
        assert_eq!(whys(&plan, 1), [Why::Binds("title".into()), Why::Filter]);
    }
}
