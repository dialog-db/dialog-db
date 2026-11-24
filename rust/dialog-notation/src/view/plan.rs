//! A query whose answer is markup, laid out in layers: which conditions
//! each element of its template takes, and what tells the element's
//! instances apart.
//!
//! The query's required conditions are the root's layer, and its
//! instances are told apart by the query's entity, or stand once for a
//! query of no entity. A query it collects into its markup,
//! `(todo/chip ?t is: (collect ?chips))` put where `?chips` stands, is
//! that query's template put in its place, its variables named apart, and
//! its required conditions a layer of their own under the element it
//! stands in. Its instances are told apart by its head: the terms of the
//! head that the call leaves free, an entry's key first, so they stand in
//! the order of the keys. A read collected and counted, `(count ?open)`,
//! is a layer under the element that shows the count, whose instances are
//! counted and never shown. A query no document defines, named under a
//! declared concept, stands as its source: the concept applied to the
//! entity with its fields.
//!
//! A layer whose conditions do not bind a key of an element above it
//! takes the conditions of that element's layer too, so that each of its
//! rows is under one instance: the whole of a read collected with `_` for
//! the entity is collected under every instance.
//!
//! What tells one answer from another is the head: the to-do renamed is
//! the same `li`, its text set. That is a hypothesis, recorded in
//! design-queries.md.

use std::collections::{BTreeMap, BTreeSet};

use dialog_corpus::Value;
use dialog_query::ConceptConclusion;

use crate::Error;
use crate::compile::{Query, Step, compile};
use crate::select::{COLLECT, COUNT, Folded, ROW, SELECTED, Selection};
use crate::syntax::{Parameters, Syntax};
use crate::view::{Attribute, Element, Input, Node, Piece};

/// What stands at a place in the template.
#[derive(Clone, Debug, PartialEq)]
pub enum Placed {
    /// An element, by its tag.
    Element(String),
    /// The instances of a read that are counted, never shown: the count
    /// is this variable, shown by the place's parent.
    Count(String),
}

/// A place in the template that has instances.
#[derive(Clone, Debug, PartialEq)]
pub struct Place {
    /// Where it is: the index of each child taken from the template's
    /// root down to it. The root's is empty. A counted place has a last
    /// index past its parent's children, and so no slot.
    pub path: Vec<usize>,
    /// The place it stands in, by its index in [`Plan::elements`].
    pub parent: Option<usize>,
    /// What stands there.
    pub placed: Placed,
    /// The variables it holds directly: in its attributes, its text
    /// children, its events' terms.
    pub holds: Vec<String>,
    /// The variables bound at it: by the layers above it and its own.
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
    /// It is a required condition of the query whose markup the element
    /// is.
    Required,
    /// It is the read a query collects and counts.
    Counted,
    /// It reads a query no document defines, as its source.
    Source,
    /// It is a condition of a layer above, taken so that a row binds the
    /// key of an element above.
    Joined,
}

/// A condition of a layer, as a kernel premise, and why it is there.
#[derive(Clone, Debug, PartialEq)]
pub struct Condition {
    /// The premise. `(unless …)` stands as it does in a rule.
    pub premise: Syntax,
    /// Why the layer holds it.
    pub why: Why,
}

/// The conditions a place takes, and the row its query gives.
#[derive(Clone, Debug, PartialEq)]
pub struct Layer {
    /// The place that takes them, by its index in [`Plan::elements`].
    pub element: usize,
    /// Its conditions.
    pub conditions: Vec<Condition>,
    /// The variables a row of its query binds: the keys of the elements
    /// above it, then its own variables that it or an element below it
    /// holds or is told apart by.
    pub row: Vec<String>,
}

/// A count a place shows: the instances of a counted place under it.
#[derive(Clone, Debug, PartialEq)]
pub struct Count {
    /// The variable the count is.
    pub variable: String,
    /// The variable of the whole it counts, a read collected.
    pub whole: String,
    /// The place that shows it.
    pub host: usize,
    /// The counted place.
    pub place: usize,
}

/// A query no document defines, shown as its source: the concept it is
/// named under applied to the entity with its fields, as the notation
/// writes it.
#[derive(Clone, Debug, PartialEq)]
pub struct Source {
    /// The place that shows it, a `view-source` element.
    pub place: usize,
    /// The variable its text is held under.
    pub text: String,
    /// The concept.
    pub concept: String,
    /// The entity, a variable or a value.
    pub entity: Syntax,
    /// Each field and its term.
    pub fields: Vec<(String, Syntax)>,
}

/// A query whose answer is markup, laid out in layers: what a back end
/// that maintains it walks.
#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    /// The query, by its name.
    pub name: String,
    /// The variable of the entity it answers of; empty for a query of no
    /// entity, which stands once.
    pub entity: String,
    /// The template, every query collected into it put in its place.
    pub template: Node,
    /// The places, the root first, each after its parent; counted
    /// places last.
    pub elements: Vec<Place>,
    /// The layers. The root's is first.
    pub layers: Vec<Layer>,
    /// The counts shown.
    pub counts: Vec<Count>,
    /// The queries not found, shown as their source.
    pub sources: Vec<Source>,
    /// The entity the question names, where it names one by its value:
    /// `(select (todo/row todo:1 is: ?row))`.
    pub given: Option<Value>,
    /// The conditions of the query asked and the reads it counts in its
    /// own markup, as a selection is compiled from them: what
    /// [`Plan::folds`] lowers to the folds Dialog-DB keeps the counts by.
    /// `None` where it counts nothing so.
    pub folding: Option<Syntax>,
}

/// A count a view shows that Dialog-DB keeps, by a fold: each folded row
/// is an instance of the count's host, named by the terms it groups by,
/// and how many the count is there.
#[derive(Clone, Debug)]
pub struct FoldedCount {
    /// The count, by its index in [`Plan::counts`].
    pub count: usize,
    /// The fold, with the rules its queries read.
    pub folded: Folded,
    /// The terms it groups by, the keys of the host's instances.
    pub shared: Vec<String>,
    /// The field of a folded row that holds the count.
    pub field: String,
}

/// The tag a query not found, shown as its source, is wrapped in.
pub const SOURCE_TAG: &str = "view-source";

/// The relation a layer's rows are reported in.
const LAYER: &str = "dialog.notation.view.layer";

/// The field a layer's row is reported under, as one record.
const ROW_FIELD: &str = "row";

/// A premise, with what it holds and what it binds.
struct Info {
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
    Info { vars, binds }
}

fn add(name: &str, into: &mut Vec<String>) {
    if !into.iter().any(|known| known == name) {
        into.push(name.to_string());
    }
}

/// The variables an element holds directly.
fn element_holds(element: &Element) -> Vec<String> {
    let mut holds = Vec::new();
    for (_, attribute) in &element.attributes {
        match attribute {
            Attribute::Variable(name) => add(name, &mut holds),
            Attribute::Parts(parts) => {
                for piece in parts {
                    if let Piece::Variable(name) = piece {
                        add(name, &mut holds);
                    }
                }
            }
            _ => {}
        }
    }
    for event in &element.events {
        for (_, input) in &event.fields {
            if let Input::Variable(name) = input {
                add(name, &mut holds);
            }
        }
    }
    for child in &element.children {
        if let Node::Variable(name) = child {
            add(name, &mut holds);
        }
    }
    holds
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

fn malformed(tree: &Syntax) -> Error {
    Error::Refused(format!("{tree} is not a query as analysis gives one"))
}

fn items(tree: &Syntax) -> &[Syntax] {
    tree.operand().map_or(&[], |operand| &operand.items)
}

/// The premises of a query's conditions, from the kernel rules analysis
/// reported them by: the rule's premises, but for the entity it names
/// its row by.
fn premises(rules: &[Syntax]) -> Result<Vec<Syntax>, Error> {
    let [rule] = rules else {
        return Err(Error::Unsupported(format!(
            "conditions that read through {} definitions: a view of several is not decided",
            rules.len()
        )));
    };
    Ok(items(rule)
        .iter()
        .skip(1)
        .filter(|premise| {
            !(premise.is_formula()
                && premise.operand().is_some_and(|operand| {
                    operand.items.first() == Some(&Syntax::Reference(ROW.into()))
                }))
        })
        .cloned()
        .collect())
}

/// Where the whole a query collects comes from.
#[derive(Clone, Debug)]
enum From {
    /// A read: its premises.
    Rows(Vec<Syntax>),
    /// A query called.
    Call(Syntax),
    /// A query no document defines.
    Missing {
        concept: String,
        entity: Syntax,
        fields: Vec<(String, Syntax)>,
        premises: Vec<Syntax>,
    },
}

/// A query, as its analyzed tree says it.
#[derive(Clone, Debug)]
struct Defined {
    head: Syntax,
    required: Vec<Syntax>,
    calls: Vec<Syntax>,
    collects: Vec<(String, Syntax, From)>,
    shapes: Vec<(String, Syntax)>,
    view: Option<(String, Syntax)>,
}

impl Defined {
    fn read(tree: &Syntax) -> Result<(String, Defined), Error> {
        let [head, parts @ ..] = items(tree) else {
            return Err(malformed(tree));
        };
        let name = head.applies().ok_or_else(|| malformed(head))?.to_string();
        let mut defined = Defined {
            head: head.clone(),
            required: Vec::new(),
            calls: Vec::new(),
            collects: Vec::new(),
            shapes: Vec::new(),
            view: None,
        };
        for part in parts {
            match (part.applies(), items(part)) {
                (Some("required"), [_, rules @ ..]) => defined.required = premises(rules)?,
                (Some("call"), [call]) => defined.calls.push(call.clone()),
                (Some(COLLECT), [Syntax::Variable(target), item, source]) => {
                    let from = match (source.applies(), items(source)) {
                        (Some("rows"), [_, rules @ ..]) => From::Rows(premises(rules)?),
                        (Some("call"), [call]) => From::Call(call.clone()),
                        (
                            Some("missing"),
                            [
                                Syntax::Reference(concept),
                                entity,
                                Syntax::Map(fields),
                                _,
                                rules @ ..,
                            ],
                        ) => From::Missing {
                            concept: concept.clone(),
                            entity: entity.clone(),
                            fields: fields
                                .iter()
                                .filter_map(|(label, term)| match label {
                                    Syntax::Reference(label) => Some((label.clone(), term.clone())),
                                    _ => None,
                                })
                                .collect(),
                            premises: premises(rules)?,
                        },
                        _ => return Err(malformed(source)),
                    };
                    defined.collects.push((target.clone(), item.clone(), from));
                }
                (Some("is"), [Syntax::Variable(target), shape]) => {
                    defined.shapes.push((target.clone(), shape.clone()));
                }
                (Some("view"), [Syntax::Variable(target), template]) => {
                    defined.view = Some((target.clone(), template.clone()));
                }
                _ => return Err(malformed(part)),
            }
        }
        Ok((name, defined))
    }
}

/// Whether an analyzed `(dialog.notation.select …)` asks a query whose answer is
/// markup.
pub fn is_markup(tree: &Syntax) -> bool {
    let [call, defined @ ..] = items(tree) else {
        return false;
    };
    defined.iter().any(|query| {
        items(query).first().and_then(Syntax::applies) == call.applies()
            && items(query)
                .iter()
                .any(|part| part.applies() == Some("view"))
    })
}

/// A tree with each variable a substitution names put in, and every
/// other renamed with `prefix`.
fn renamed(tree: &Syntax, prefix: &str, given: &BTreeMap<String, Syntax>) -> Syntax {
    match tree {
        Syntax::Variable(name) => match given.get(name) {
            Some(term) => term.clone(),
            None if prefix.is_empty() => tree.clone(),
            None => Syntax::Variable(format!("{prefix}{name}")),
        },
        Syntax::Application(operator, operand) => Syntax::Application(
            Box::new(renamed(operator, prefix, given)),
            Parameters {
                items: operand
                    .items
                    .iter()
                    .map(|item| renamed(item, prefix, given))
                    .collect(),
                terms: operand
                    .terms
                    .iter()
                    .map(|(label, term)| (label.clone(), renamed(term, prefix, given)))
                    .collect(),
            },
        ),
        Syntax::Map(entries) => Syntax::Map(
            entries
                .iter()
                .map(|(key, value)| (renamed(key, prefix, given), renamed(value, prefix, given)))
                .collect(),
        ),
        Syntax::Tuple(items) => Syntax::Tuple(
            items
                .iter()
                .map(|item| renamed(item, prefix, given))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// The name of a variable, or how any other tree is written.
fn named(tree: &Syntax) -> String {
    match tree {
        Syntax::Variable(name) => name.clone(),
        other => other.to_string(),
    }
}

/// A layer as it is built: where its place is, its conditions and key.
struct Spec {
    path: Vec<usize>,
    conditions: Vec<Condition>,
    key: Vec<String>,
}

/// A count as it is built.
struct Counted {
    host: Vec<usize>,
    variable: String,
    whole: String,
    conditions: Vec<Syntax>,
    key: Vec<String>,
}

/// A query's template, each query it collects into it put in its place.
struct Builder<'a> {
    defined: &'a BTreeMap<String, Defined>,
    specs: Vec<Spec>,
    counted: Vec<Counted>,
    sources: Vec<(Vec<usize>, Source)>,
    fresh: usize,
}

impl Builder<'_> {
    /// The template of the query `name`, its variables named apart by
    /// `prefix` but for those `given`, standing at `path`: its layer, and
    /// those of what it collects, are noted.
    fn instantiate(
        &mut self,
        name: &str,
        prefix: &str,
        given: &BTreeMap<String, Syntax>,
        path: Vec<usize>,
        above: &BTreeSet<String>,
        root: bool,
    ) -> Result<Node, Error> {
        let defined = self
            .defined
            .get(name)
            .ok_or_else(|| Error::Unknown(name.to_string()))?;
        let Some((_, template)) = &defined.view else {
            return Err(Error::Refused(format!(
                "the query {name} is collected into markup, and its answer is not markup: it \
                 says no `(view ?x template)`"
            )));
        };
        if !defined.calls.is_empty() {
            return Err(Error::Unsupported(format!(
                "the query {name} calls another query that every answer needs: a view of one is \
                 not built yet; collect it, or read what it reads"
            )));
        }
        let rename = |tree: &Syntax| renamed(tree, prefix, given);
        let head = rename(&defined.head);
        let conditions: Vec<Syntax> = defined.required.iter().map(rename).collect();
        // What tells its instances apart: the terms of its head the call
        // leaves free, an entry's key first.
        let mut key = Vec::new();
        let operand = head.operand().cloned().unwrap_or_default();
        if let Some(Syntax::Map(pair)) = operand.terms.get("is")
            && let [(Syntax::Variable(entry), _)] = pair.as_slice()
        {
            add(entry, &mut key);
        }
        for term in operand.items.iter().chain(
            operand
                .terms
                .iter()
                .filter(|(label, _)| label.as_str() != "is")
                .map(|(_, term)| term),
        ) {
            if let Syntax::Variable(variable) = term
                && !above.contains(variable)
            {
                add(variable, &mut key);
            }
        }
        if root {
            key = match operand.items.first() {
                Some(Syntax::Variable(entity)) => vec![entity.clone()],
                _ => Vec::new(),
            };
        }
        let mut bound = above.clone();
        for condition in &conditions {
            bound.extend(info(condition).vars);
        }
        self.specs.push(Spec {
            path: path.clone(),
            conditions: conditions
                .iter()
                .map(|premise| Condition {
                    premise: premise.clone(),
                    why: Why::Required,
                })
                .collect(),
            key,
        });
        // What it collects, by the variable it is put under.
        let mut collected: BTreeMap<String, (Syntax, From)> = BTreeMap::new();
        for (target, item, from) in &defined.collects {
            let from = match from {
                From::Rows(premises) => From::Rows(premises.iter().map(rename).collect()),
                From::Call(call) => From::Call(rename(call)),
                From::Missing {
                    concept,
                    entity,
                    fields,
                    premises,
                } => From::Missing {
                    concept: concept.clone(),
                    entity: rename(entity),
                    fields: fields
                        .iter()
                        .map(|(label, term)| (label.clone(), rename(term)))
                        .collect(),
                    premises: premises.iter().map(rename).collect(),
                },
            };
            collected.insert(
                named(&rename(&Syntax::Variable(target.clone()))),
                (rename(item), from),
            );
        }
        let mut counts: BTreeMap<String, String> = BTreeMap::new();
        for (target, shape) in &defined.shapes {
            let shape = rename(shape);
            match (shape.applies(), items(&shape)) {
                (Some(COUNT), [Syntax::Variable(whole)]) => {
                    counts.insert(
                        named(&rename(&Syntax::Variable(target.clone()))),
                        whole.clone(),
                    );
                }
                _ => {
                    return Err(Error::Unsupported(format!(
                        "(is ?{target} {shape}) in a query whose answer is markup: a value it \
                         shows is bound by a condition, or is `(count ?x)` of what it collects"
                    )));
                }
            }
        }
        let node = crate::view::template(&rename(template))?;
        self.walk(node, path, &bound, &collected, &counts)
    }

    /// A node of a template, each query collected into it put in its
    /// place.
    fn walk(
        &mut self,
        node: Node,
        path: Vec<usize>,
        bound: &BTreeSet<String>,
        collected: &BTreeMap<String, (Syntax, From)>,
        counts: &BTreeMap<String, String>,
    ) -> Result<Node, Error> {
        let Node::Element(mut element) = node else {
            return Ok(node);
        };
        // A count it shows is counted under it.
        for held in element_holds(&element) {
            let Some(whole) = counts.get(&held) else {
                continue;
            };
            let Some((item, From::Rows(premises))) = collected.get(whole) else {
                return Err(Error::Unsupported(format!(
                    "(count ?{whole}) in markup counts a read collected; ?{whole} is not one"
                )));
            };
            let mut key = Vec::new();
            item.variables(&mut key);
            self.counted.push(Counted {
                host: path.clone(),
                variable: held.clone(),
                whole: whole.clone(),
                conditions: premises.clone(),
                key,
            });
        }
        let mut children = Vec::new();
        for (index, child) in std::mem::take(&mut element.children)
            .into_iter()
            .enumerate()
        {
            let mut below = path.clone();
            below.push(index);
            let child = match child {
                Node::Variable(name) => match collected.get(&name) {
                    Some((item, From::Call(call))) => self.called(call, item, below, bound)?,
                    Some((_, From::Missing { .. })) => self.missing(&name, collected, below)?,
                    Some((_, From::Rows(_))) => {
                        return Err(Error::Unsupported(format!(
                            "?{name} in markup is the whole of a read: markup holds the whole \
                             of a query whose answer is markup, or its count"
                        )));
                    }
                    None => Node::Variable(name),
                },
                other => self.walk(other, below, bound, collected, counts)?,
            };
            children.push(child);
        }
        element.children = children;
        Ok(Node::Element(element))
    }

    /// The template of a query called and collected, put in its place.
    fn called(
        &mut self,
        call: &Syntax,
        item: &Syntax,
        path: Vec<usize>,
        bound: &BTreeSet<String>,
    ) -> Result<Node, Error> {
        let name = call.applies().ok_or_else(|| malformed(call))?.to_string();
        let defined = self
            .defined
            .get(&name)
            .ok_or_else(|| Error::Unknown(name.clone()))?;
        let (Some(head), Some(wanted)) = (defined.head.operand(), call.operand()) else {
            return Err(malformed(call));
        };
        // The call's terms put in for the head's, but what it collects.
        let mut given = BTreeMap::new();
        let mut item_variables = Vec::new();
        item.variables(&mut item_variables);
        let mut pair = |term: &Syntax, own: &Syntax| {
            if let Syntax::Variable(own) = own
                && matches!(term, Syntax::Variable(_) | Syntax::Constant(_))
                && !matches!(term, Syntax::Variable(name) if item_variables.contains(name))
            {
                given.insert(own.clone(), term.clone());
            }
        };
        for (term, own) in wanted.items.iter().zip(&head.items) {
            pair(term, own);
        }
        for (label, term) in &wanted.terms {
            if let Some(own) = head.terms.get(label) {
                pair(term, own);
            }
        }
        self.fresh += 1;
        let prefix = format!("[q{}]", self.fresh);
        self.instantiate(&name, &prefix, &given, path, bound, false)
    }

    /// A query no document defines, shown as its source.
    fn missing(
        &mut self,
        name: &str,
        collected: &BTreeMap<String, (Syntax, From)>,
        path: Vec<usize>,
    ) -> Result<Node, Error> {
        let Some((
            _,
            From::Missing {
                concept,
                entity,
                fields,
                premises,
            },
        )) = collected.get(name)
        else {
            return Err(Error::Unknown(name.to_string()));
        };
        self.fresh += 1;
        let prefix = format!("[q{}]", self.fresh);
        // Only the source's own variables are named apart: those of the
        // query it stands in are bound above.
        let own: BTreeSet<String> = premises
            .iter()
            .flat_map(|premise| {
                let mut names = Vec::new();
                premise.variables(&mut names);
                names
            })
            .filter(|variable| variable.starts_with("[source-") || variable == crate::view::CALLED)
            .collect();
        let given: BTreeMap<String, Syntax> = premises
            .iter()
            .flat_map(|premise| {
                let mut names = Vec::new();
                premise.variables(&mut names);
                names
            })
            .filter(|variable| !own.contains(variable))
            .map(|variable| (variable.clone(), Syntax::Variable(variable)))
            .collect();
        let named = |tree: &Syntax| renamed(tree, &prefix, &given);
        let entity = named(entity);
        let text = format!("{prefix}view-source");
        let key = match &entity {
            Syntax::Variable(variable) => vec![variable.clone()],
            _ => Vec::new(),
        };
        self.specs.push(Spec {
            path: path.clone(),
            conditions: premises
                .iter()
                .map(|premise| Condition {
                    premise: named(premise),
                    why: Why::Source,
                })
                .collect(),
            key,
        });
        self.sources.push((
            path,
            Source {
                place: 0,
                text: text.clone(),
                concept: concept.clone(),
                entity,
                fields: fields
                    .iter()
                    .map(|(label, term)| (label.clone(), named(term)))
                    .collect(),
            },
        ));
        Ok(Node::Element(Element {
            tag: SOURCE_TAG.into(),
            attributes: Vec::new(),
            events: Vec::new(),
            children: vec![Node::Variable(text)],
        }))
    }
}

/// The places of a template, in pre-order.
fn places(node: &Node, path: Vec<usize>, parent: Option<usize>, into: &mut Vec<Place>) {
    if let Node::Element(element) = node {
        let at = into.len();
        into.push(Place {
            path: path.clone(),
            parent,
            placed: Placed::Element(element.tag.clone()),
            holds: element_holds(element),
            bound: Vec::new(),
            key: Vec::new(),
            layer: 0,
        });
        for (index, child) in element.children.iter().enumerate() {
            let mut below = path.clone();
            below.push(index);
            places(child, below, Some(at), into);
        }
    }
}

impl Plan {
    /// The plan of an analyzed `(dialog.notation.select CALL QUERY…)` whose query
    /// answers with markup.
    pub fn of(tree: &Syntax) -> Result<Plan, Error> {
        let [call, queries @ ..] = items(tree) else {
            return Err(malformed(tree));
        };
        let name = call.applies().ok_or_else(|| malformed(call))?.to_string();
        let defined: BTreeMap<String, Defined> = queries
            .iter()
            .map(Defined::read)
            .collect::<Result<_, _>>()?;
        let entity = defined
            .get(&name)
            .and_then(|query| query.head.operand())
            .and_then(|operand| operand.items.first())
            .map(|entity| match entity {
                Syntax::Variable(variable) => variable.clone(),
                _ => String::new(),
            })
            .unwrap_or_default();
        let mut builder = Builder {
            defined: &defined,
            specs: Vec::new(),
            counted: Vec::new(),
            sources: Vec::new(),
            fresh: 0,
        };
        let template = builder.instantiate(
            &name,
            "",
            &BTreeMap::new(),
            Vec::new(),
            &BTreeSet::new(),
            true,
        )?;
        let mut elements = Vec::new();
        places(&template, Vec::new(), None, &mut elements);
        // Each place a layer of its own takes, the others their parent's.
        let mut layers: Vec<Layer> = Vec::new();
        for at in 0..elements.len() {
            // A query collected that needs no condition of its own, one of
            // no entity, stands once under its parent: no layer of its own.
            match builder.specs.iter().position(|spec| {
                spec.path == elements[at].path && (at == 0 || !spec.conditions.is_empty())
            }) {
                Some(spec) => {
                    elements[at].key = builder.specs[spec].key.clone();
                    elements[at].layer = layers.len();
                    layers.push(Layer {
                        element: at,
                        conditions: std::mem::take(&mut builder.specs[spec].conditions),
                        row: Vec::new(),
                    });
                }
                None => {
                    let parent = elements[at].parent.unwrap_or(0);
                    elements[at].layer = elements[parent].layer;
                }
            }
        }
        let mut counts = Vec::new();
        for (index, counted) in builder.counted.into_iter().enumerate() {
            let Some(host) = elements.iter().position(|place| place.path == counted.host) else {
                continue;
            };
            let mut path = counted.host.clone();
            path.push(usize::MAX - index);
            let place = elements.len();
            elements.push(Place {
                path,
                parent: Some(host),
                placed: Placed::Count(counted.variable.clone()),
                holds: Vec::new(),
                bound: Vec::new(),
                key: counted.key,
                layer: layers.len(),
            });
            layers.push(Layer {
                element: place,
                conditions: counted
                    .conditions
                    .into_iter()
                    .map(|premise| Condition {
                        premise,
                        why: Why::Counted,
                    })
                    .collect(),
                row: Vec::new(),
            });
            counts.push(Count {
                variable: counted.variable,
                whole: counted.whole,
                host,
                place,
            });
        }
        let sources = builder
            .sources
            .into_iter()
            .filter_map(|(path, mut source)| {
                source.place = elements.iter().position(|place| place.path == path)?;
                Some(source)
            })
            .collect();
        let given = match call.operand().and_then(|operand| operand.items.first()) {
            Some(Syntax::Constant(value)) => Some(value.clone()),
            _ => None,
        };
        let folding = folding(call, &name, queries, &defined, &elements, &counts);
        let mut plan = Plan {
            folding,
            given,
            name,
            entity,
            template,
            elements,
            layers,
            counts,
            sources,
        };
        plan.join();
        plan.rows();
        Ok(plan)
    }

    /// The counts of [`Plan::counts`] Dialog-DB keeps, each by the fold
    /// the lowering of a selection gives the read it counts: a read the
    /// query asked counts in its own markup, one query, grouped by the
    /// entity the query answers of. A count of a query collected into the
    /// markup, a count of an entry, and a read the lowering leaves in
    /// memory are kept from the rows of their layer instead.
    pub fn folds(&self) -> Vec<FoldedCount> {
        let Some(tree) = &self.folding else {
            return Vec::new();
        };
        let Ok(selection) = Selection::compile(tree) else {
            return Vec::new();
        };
        let key: BTreeSet<&String> = self.elements[0].key.iter().collect();
        let parts = selection.folded(&self.name);
        let mut folds = Vec::new();
        for (count, shown) in self.counts.iter().enumerate() {
            let Some(part) = parts.iter().find(|part| part.target == shown.whole) else {
                continue;
            };
            if part.entry || part.shared.iter().collect::<BTreeSet<_>>() != key {
                continue;
            }
            // A count needs the count alone: the whole beside it would be
            // built again over the group on every change and not read.
            let whole = &selection.folds[part.fold];
            let reduce = whole.fold.reduce();
            let counting = dialog_query::reduce::Reduce::new(
                reduce.groups.clone(),
                reduce
                    .entries
                    .iter()
                    .filter(|entry| entry.field == part.count)
                    .cloned()
                    .collect(),
            );
            folds.push(FoldedCount {
                count,
                folded: Folded {
                    rules: whole.rules.clone(),
                    fold: dialog_query::Fold::new(
                        whole.fold.groups().clone(),
                        whole.fold.inputs().clone(),
                        counting,
                    ),
                },
                shared: part.shared.clone(),
                field: part.count.clone(),
            });
        }
        folds
    }

    /// A layer that does not bind a key of an element above it takes the
    /// conditions of that element's layer, so each of its rows is under
    /// one instance. Then what each place has bound.
    fn join(&mut self) {
        for index in 1..self.layers.len() {
            let owner = self.layers[index].element;
            let mut own = BTreeSet::new();
            for condition in &self.layers[index].conditions {
                own.extend(info(&condition.premise).binds);
            }
            let mut ancestors = Vec::new();
            let mut at = self.elements[owner].parent;
            while let Some(parent) = at {
                ancestors.push(parent);
                at = self.elements[parent].parent;
            }
            let mut taken: Vec<usize> = Vec::new();
            for ancestor in ancestors.iter().rev() {
                let unbound = self.elements[*ancestor]
                    .key
                    .iter()
                    .any(|name| !own.contains(name));
                let layer = self.elements[*ancestor].layer;
                if unbound && !taken.contains(&layer) {
                    taken.push(layer);
                }
            }
            for layer in taken {
                let joined: Vec<Condition> = self.layers[layer]
                    .conditions
                    .iter()
                    .map(|condition| Condition {
                        premise: condition.premise.clone(),
                        why: Why::Joined,
                    })
                    .collect();
                for condition in joined {
                    if !self.layers[index]
                        .conditions
                        .iter()
                        .any(|known| known.premise == condition.premise)
                    {
                        self.layers[index].conditions.push(condition);
                    }
                }
            }
        }
        for at in 0..self.elements.len() {
            let mut bound = BTreeSet::new();
            for layer in &self.layers {
                if above(&self.elements, layer.element, at) {
                    for condition in &layer.conditions {
                        bound.extend(info(&condition.premise).vars);
                    }
                }
            }
            self.elements[at].bound = bound.into_iter().collect();
        }
    }

    /// Each layer's row, once the keys are known.
    fn rows(&mut self) {
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
            // Its own variables that it or a place below shows or is told
            // apart by, in the order the places are.
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
            for source in &self.sources {
                if self.elements[source.place].layer == index {
                    for (_, term) in &source.fields {
                        let mut names = Vec::new();
                        term.variables(&mut names);
                        for name in names {
                            if own.contains(&name) {
                                add(&name, &mut row);
                            }
                        }
                    }
                }
            }
            self.layers[index].row = row;
        }
    }

    /// The place an address names, by the slot of each parent from the
    /// root down; the root's slot is not looked at.
    pub fn place_at(&self, slots: &[usize]) -> Option<usize> {
        let mut at = 0;
        for slot in slots.iter().skip(1) {
            at = (0..self.elements.len()).find(|child| {
                self.elements[*child].parent == Some(at)
                    && self.elements[*child].path.last() == Some(slot)
            })?;
        }
        Some(at)
    }

    /// The query of a layer: its conditions, the entity rendered put in
    /// where it is given, and a row of [`Layer::row`]. Read a row with
    /// [`Plan::row`]. A row is reported under an entity made of all its
    /// values, so a row's values stay together.
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
                Some(value) if !self.entity.is_empty() => {
                    substitute(&condition.premise, &self.entity, value)
                }
                _ => condition.premise.clone(),
            })
            .collect();
        let row = self.columns(layer, entity.is_some());
        let mut items = Vec::new();
        if row.is_empty() {
            // A row binds nothing but the entity given: it says it.
            let subject = given.clone().ok_or_else(|| {
                Error::Refused(format!(
                    "layer {layer} of the query {} binds nothing it shows",
                    self.name
                ))
            })?;
            items.push(report(Syntax::Application(
                Box::new(Syntax::Reference(LAYER.into())),
                Parameters {
                    items: vec![subject.clone()],
                    terms: BTreeMap::from([("c0".to_string(), subject)]),
                },
            )));
            items.extend(premises);
        } else {
            // A row is reported under the entity it is about, so a change
            // to that entity's facts is maintained for that entity alone,
            // and its values as one record, so they stay together: a
            // record is one value, and two rows of one entity are two
            // values, never crossed.
            let record = Syntax::Map(
                row.iter()
                    .enumerate()
                    .map(|(at, name)| {
                        (
                            Syntax::Reference(format!("c{at}")),
                            Syntax::Variable(name.clone()),
                        )
                    })
                    .collect(),
            );
            let subject = about(&premises).or(given.clone()).ok_or_else(|| {
                Error::Refused(format!(
                    "layer {layer} of the query {} reads no entity",
                    self.name
                ))
            })?;
            items.push(report(Syntax::Application(
                Box::new(Syntax::Reference(LAYER.into())),
                Parameters {
                    items: vec![subject],
                    terms: BTreeMap::from([(ROW_FIELD.to_string(), record)]),
                },
            )));
            items.extend(premises);
        }
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

    /// The variables a layer's query reports, the entity left out where
    /// it is given.
    fn columns(&self, layer: usize, given: bool) -> Vec<String> {
        self.layers[layer]
            .row
            .iter()
            .filter(|name| !given || **name != self.entity)
            .cloned()
            .collect()
    }

    /// A match of layer `layer`'s query as a row: each variable of
    /// [`Layer::row`] and what it was bound to, the entity rendered too
    /// where it was given.
    pub fn row(
        &self,
        layer: usize,
        query: &Query,
        conclusion: &ConceptConclusion,
        entity: Option<&Value>,
    ) -> Result<BTreeMap<String, Value>, Error> {
        let values = query.values(conclusion)?;
        let names = self.columns(layer, entity.is_some());
        let record = values
            .get(ROW_FIELD)
            .map(crate::data::entries)
            .transpose()?
            .flatten();
        let mut row = BTreeMap::new();
        for (at, name) in names.iter().enumerate() {
            let label = format!("c{at}");
            let value = match &record {
                Some(record) => crate::data::field(record, &label).cloned(),
                None => values.get(&label).cloned(),
            };
            if let Some(value) = value {
                row.insert(name.clone(), value);
            }
        }
        if let Some(entity) = entity
            && !self.entity.is_empty()
        {
            row.insert(self.entity.clone(), entity.clone());
        }
        Ok(row)
    }

    /// The part of the template a place stands for. A counted place
    /// stands for nothing shown.
    pub fn node(&self, element: usize) -> &Node {
        const NOTHING: &Node = &Node::Text(String::new());
        let mut node = &self.template;
        for index in &self.elements[element].path {
            match node {
                Node::Element(parent) => match parent.children.get(*index) {
                    Some(child) => node = child,
                    None => return NOTHING,
                },
                _ => return NOTHING,
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

/// The query asked, as a selection is compiled from it, with no more than
/// what its own markup counts: its conditions every answer needs, and each
/// read collected that a count of [`Plan::counts`] counts. `None` where
/// there is none.
fn folding(
    call: &Syntax,
    name: &str,
    queries: &[Syntax],
    defined: &BTreeMap<String, Defined>,
    elements: &[Place],
    counts: &[Count],
) -> Option<Syntax> {
    let own = defined.get(name)?;
    let wholes: BTreeSet<&String> = counts
        .iter()
        .filter(|count| elements[count.host].layer == 0)
        .filter(|count| {
            own.collects
                .iter()
                .any(|(target, _, from)| target == &count.whole && matches!(from, From::Rows(_)))
        })
        .map(|count| &count.whole)
        .collect();
    if wholes.is_empty() {
        return None;
    }
    let query = queries
        .iter()
        .find(|query| items(query).first().and_then(Syntax::applies) == Some(name))?;
    let [head, parts @ ..] = items(query) else {
        return None;
    };
    let mut kept = vec![head.clone()];
    for part in parts {
        let keep = match (part.applies(), items(part)) {
            (Some("required"), _) => true,
            (Some(COLLECT), [Syntax::Variable(target), _, source]) => {
                wholes.contains(target) && source.applies() == Some("rows")
            }
            _ => false,
        };
        if keep {
            kept.push(part.clone());
        }
    }
    let apply = |operator: &str, items: Vec<Syntax>| {
        Syntax::Application(
            Box::new(Syntax::Reference(operator.into())),
            Parameters {
                items,
                ..Parameters::default()
            },
        )
    };
    Some(apply(SELECTED, vec![call.clone(), apply("query", kept)]))
}

/// The entity a layer's reads are about: the variable most of its reads
/// are of, the first of those that tie.
fn about(premises: &[Syntax]) -> Option<Syntax> {
    let mut counted: Vec<(String, usize)> = Vec::new();
    for premise in premises {
        if !premise.is_read() {
            continue;
        }
        if let Some(Syntax::Variable(entity)) = items(premise).first() {
            match counted.iter_mut().find(|(name, _)| name == entity) {
                Some((_, count)) => *count += 1,
                None => counted.push((entity.clone(), 1)),
            }
        }
    }
    let most = counted.iter().map(|(_, count)| *count).max()?;
    counted
        .into_iter()
        .find(|(_, count)| *count == most)
        .map(|(name, _)| Syntax::Variable(name))
}

/// `(report head)`.
fn report(head: Syntax) -> Syntax {
    Syntax::Application(
        Box::new(Syntax::Reference("report".into())),
        Parameters {
            items: vec![head],
            ..Parameters::default()
        },
    )
}

/// A tree with a variable put in by a value.
fn substitute(tree: &Syntax, name: &str, by: &Syntax) -> Syntax {
    renamed(tree, "", &BTreeMap::from([(name.to_string(), by.clone())]))
}
