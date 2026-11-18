//! A view, described: what a back end walks to render it.
//!
//! `(view concept :name template condition…)` is proposed in the
//! design: the concept a view renders, its name, a template in Hiccup,
//! and the conditions that bind the template's variables, with events
//! inline as `:on/<type>`. The walker expands it to a kernel form that
//! describes it and nothing more:
//!
//! ```clj
//! (view (xyz.todo ?t title: (:last (:text ?title)))
//!   concept: todo name: :row entity: ?t template: [:li ?title])
//! ```
//!
//! [`View::describe`] reads that form into data. Nothing in it is a
//! Dialog-DB rule: the conditions are kernel premises, and the
//! template is elements, attributes, text, variables, events and
//! nested views. A back end lowers it to whatever it renders.
//!
//! [`plan`] lays it out in layers: each element of the template that
//! holds variables takes the conditions that bind them, given what the
//! elements above it bound, and each layer is a query of its own.
//! [`tree`] keeps the instances of the elements from the rows of those
//! queries as they come and go, and says each change as patches. No
//! value is folded. The Dialog-DB back end,
//! [`compile`](crate::compile()), gives a view as its plan.

use dialog_corpus::Value;

use crate::Error;
use crate::syntax::Syntax;

pub mod plan;
pub mod tree;

/// A view: the concept it renders, its name, the variable that is the
/// entity rendered, its template and its conditions.
#[derive(Clone, Debug, PartialEq)]
pub struct View {
    /// The concept, by its name.
    pub concept: String,
    /// The view's name: `row` for `:row`.
    pub name: String,
    /// The variable the conditions bind to the entity rendered. Empty for
    /// a view with no condition, which renders no entity and stands once.
    pub entity: String,
    /// What it renders.
    pub template: Node,
    /// The conditions, as kernel premises. `(unless …)` stands among
    /// them as it does in a rule.
    pub conditions: Vec<Syntax>,
    /// What makes the entity rendered an instance of the concept: the
    /// concept applied to it alone, as kernel premises. Empty where the
    /// kernel form says none.
    pub instance: Vec<Syntax>,
}

/// A part of a template.
#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    /// `[:li {…} …]`.
    Element(Element),
    /// Text, as written.
    Text(String),
    /// A value other than text: a number.
    Value(Syntax),
    /// What a condition bound this variable to.
    Variable(String),
    /// Another view, of an entity: `{:view :row :of ?t}`. A reference,
    /// never markup: the renderer resolves it.
    View(Nested),
    /// Another view, called: `(view todo :item list: ?l state: :open)`,
    /// mounted once a match of that view's conditions narrowed by the
    /// call's labels and entity.
    Call(Box<Call>),
}

/// A view called in a template, `(view concept :name entity label: term …)`.
#[derive(Clone, Debug, PartialEq)]
pub struct Call {
    /// The concept whose view it is.
    pub concept: String,
    /// The view's name.
    pub view: String,
    /// The entity it is called for, written first unlabeled or as `of:`:
    /// a variable or a value. None: every match.
    pub of: Option<Syntax>,
    /// The labels after it, fields of the concept, as written.
    pub labels: Vec<(String, Syntax)>,
    /// Whether the documents read hold the view. One that is not found is
    /// shown as its source.
    pub found: bool,
    /// The concept read the labels and the entity make, as kernel
    /// premises, its entity the variable [`CALLED`] where none is
    /// given: what narrows the called view's matches.
    pub narrowing: Vec<Syntax>,
    /// The concept's fields, by label, in the order of the alphabet.
    pub fields: Vec<String>,
    /// The concept read with every field a variable, `?source-<label>`,
    /// its entity [`CALLED`], as kernel premises: what shows a view not
    /// found as its source.
    pub source: Vec<Syntax>,
}

/// The variable a call's premises name the called entity by.
pub const CALLED: &str = "view-of";

/// An element: its tag, its attributes, the events it binds and what it
/// holds.
#[derive(Clone, Debug, PartialEq)]
pub struct Element {
    /// The tag: `li` for `:li`.
    pub tag: String,
    /// The attributes, in the order written, events apart.
    pub attributes: Vec<(String, Attribute)>,
    /// The events the element binds, `:on/<type>`, in the order written.
    pub events: Vec<Event>,
    /// What it holds, in order.
    pub children: Vec<Node>,
}

/// An attribute's value.
#[derive(Clone, Debug, PartialEq)]
pub enum Attribute {
    /// Text, as written.
    Text(String),
    /// A value other than text: a number.
    Value(Syntax),
    /// What a condition bound this variable to.
    Variable(String),
    /// Its parts joined, in order, as text children are:
    /// `:href ["/notebook/" ?n]`.
    Parts(Vec<Piece>),
    /// Present, with no value: `:draft true`.
    Present,
}

/// A part of a value written as a list.
#[derive(Clone, Debug, PartialEq)]
pub enum Piece {
    /// Text, as written.
    Text(String),
    /// A value other than text: a number.
    Value(Syntax),
    /// What a condition bound this variable to.
    Variable(String),
}

/// An event an element binds: its DOM type, the command it posts, and
/// what each field of the command is given.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    /// The event's type: `change` for `:on/change`.
    pub kind: String,
    /// The command it posts, by its name.
    pub command: String,
    /// Each field of the command and what fills it.
    pub fields: Vec<(String, Input)>,
}

/// What fills a field of the command an event posts.
#[derive(Clone, Debug, PartialEq)]
pub enum Input {
    /// What a condition bound this variable to.
    Variable(String),
    /// A value written in the binding.
    Value(Syntax),
    /// A path into the live event, `.currentTarget.checked`: its steps.
    Path(Vec<String>),
}

/// A view nested in a template, by reference.
#[derive(Clone, Debug, PartialEq)]
pub struct Nested {
    /// The view's name.
    pub view: String,
    /// The variable whose entity it renders, or none: the view of every
    /// instance.
    pub of: Option<String>,
    /// The concept whose view it is, when `:model` names it.
    pub model: Option<String>,
    /// Every other entry of the reference, by its key, with its parts.
    /// The notation gives them no meaning: they are the back end's.
    pub entries: Vec<(String, Vec<Piece>)>,
}

fn keyword(node: &Syntax) -> Option<&str> {
    node.keyword()
}

fn refused(why: String) -> Error {
    Error::Refused(why)
}

impl View {
    /// Read the kernel form a `view` expands to.
    pub fn describe(tree: &Syntax) -> Result<View, Error> {
        let (Some("view"), Some(operand)) = (tree.applies(), tree.operand()) else {
            return Err(refused(format!("{tree} is not a view")));
        };
        let at = |label: &str| {
            operand
                .terms
                .get(label)
                .ok_or_else(|| refused(format!("{tree} says no `{label}:`")))
        };
        let concept = match at("concept")? {
            Syntax::Reference(name) => name.clone(),
            other => return Err(refused(format!("{other} is not a concept's name"))),
        };
        let name = keyword(at("name")?)
            .ok_or_else(|| refused(format!("{tree}: a view's name is a keyword, `:row`")))?
            .to_string();
        let entity = match at("entity")? {
            Syntax::Variable(name) => name.clone(),
            Syntax::Blank => String::new(),
            other => return Err(refused(format!("{other} is not the variable of an entity"))),
        };
        let instance = match operand.terms.get("instance") {
            Some(Syntax::Tuple(premises)) => premises.clone(),
            Some(other) => {
                return Err(refused(format!(
                    "{other} is not the premises that make an entity an instance"
                )));
            }
            None => Vec::new(),
        };
        let mut template = node(at("template")?)?;
        // What analysis said of each call, in the order the template
        // holds them.
        if let Some(Syntax::Tuple(said)) = operand.terms.get("calls") {
            let mut said = said.iter();
            fill(&mut template, &mut said)?;
        }
        Ok(View {
            concept,
            name,
            entity,
            template,
            conditions: operand.items.clone(),
            instance,
        })
    }

    /// Every variable the template holds, events included, each once.
    pub fn variables(&self) -> Vec<&str> {
        fn add<'a>(name: &'a str, into: &mut Vec<&'a str>) {
            if !into.contains(&name) {
                into.push(name);
            }
        }
        fn parts<'a>(pieces: &'a [Piece], into: &mut Vec<&'a str>) {
            for piece in pieces {
                if let Piece::Variable(name) = piece {
                    add(name, into);
                }
            }
        }
        fn walk<'a>(node: &'a Node, into: &mut Vec<&'a str>) {
            match node {
                Node::Variable(name) => add(name, into),
                Node::View(nested) => {
                    if let Some(of) = &nested.of {
                        add(of, into);
                    }
                    for (_, pieces) in &nested.entries {
                        parts(pieces, into);
                    }
                }
                Node::Element(element) => {
                    for (_, attribute) in &element.attributes {
                        match attribute {
                            Attribute::Variable(name) => add(name, into),
                            Attribute::Parts(pieces) => parts(pieces, into),
                            _ => {}
                        }
                    }
                    for event in &element.events {
                        for (_, input) in &event.fields {
                            if let Input::Variable(name) = input {
                                add(name, into);
                            }
                        }
                    }
                    for child in &element.children {
                        walk(child, into);
                    }
                }
                Node::Call(call) => {
                    fn inside<'a>(tree: &'a Syntax, into: &mut Vec<&'a str>) {
                        match tree {
                            Syntax::Variable(name) => add(name, into),
                            Syntax::Application(operator, operand) => {
                                inside(operator, into);
                                operand.items.iter().for_each(|item| inside(item, into));
                                operand.terms.values().for_each(|term| inside(term, into));
                            }
                            Syntax::Map(entries) => entries.iter().for_each(|(key, value)| {
                                inside(key, into);
                                inside(value, into);
                            }),
                            Syntax::Tuple(items) => {
                                items.iter().for_each(|item| inside(item, into))
                            }
                            _ => {}
                        }
                    }
                    if let Some(of) = &call.of {
                        inside(of, into);
                    }
                    for (_, term) in &call.labels {
                        inside(term, into);
                    }
                }
                Node::Text(_) | Node::Value(_) => {}
            }
        }
        let mut names = Vec::new();
        walk(&self.template, &mut names);
        names
    }

    /// Every view the template calls, in the order written.
    pub fn calls(&self) -> Vec<&Call> {
        fn walk<'a>(node: &'a Node, into: &mut Vec<&'a Call>) {
            match node {
                Node::Call(call) => into.push(call),
                Node::Element(element) => {
                    element.children.iter().for_each(|child| walk(child, into));
                }
                _ => {}
            }
        }
        let mut calls = Vec::new();
        walk(&self.template, &mut calls);
        calls
    }

    /// Every event the template binds, in the order written.
    pub fn events(&self) -> Vec<&Event> {
        fn walk<'a>(node: &'a Node, into: &mut Vec<&'a Event>) {
            if let Node::Element(element) = node {
                into.extend(&element.events);
                element.children.iter().for_each(|child| walk(child, into));
            }
        }
        let mut events = Vec::new();
        walk(&self.template, &mut events);
        events
    }

    /// Every view nested in the template, in the order written.
    pub fn nested(&self) -> Vec<&Nested> {
        fn walk<'a>(node: &'a Node, into: &mut Vec<&'a Nested>) {
            match node {
                Node::View(nested) => into.push(nested),
                Node::Element(element) => {
                    element.children.iter().for_each(|child| walk(child, into));
                }
                _ => {}
            }
        }
        let mut nested = Vec::new();
        walk(&self.template, &mut nested);
        nested
    }
}

/// A part of a template.
fn node(tree: &Syntax) -> Result<Node, Error> {
    Ok(match tree {
        Syntax::Tuple(items) => {
            let Some((head, rest)) = items.split_first() else {
                return Err(refused(
                    "`[]` in a template: an element is `[:tag …]`".into(),
                ));
            };
            let tag = keyword(head)
                .ok_or_else(|| {
                    refused(format!(
                        "{tree} in a template: an element starts with its tag, `[:li …]`"
                    ))
                })?
                .to_string();
            // A map right after the tag is the element's attributes, as in
            // Hiccup, unless it names a view: the design nests one there,
            // `[:ul {:view :row :of ?t}]`.
            let names_view = |entries: &[(Syntax, Syntax)]| {
                entries
                    .iter()
                    .any(|(key, _)| matches!(key, Syntax::Reference(name) if name == "view"))
            };
            let (map, children) = match rest.split_first() {
                Some((Syntax::Map(entries), children)) if !names_view(entries) => {
                    (entries.as_slice(), children)
                }
                _ => (&[][..], rest),
            };
            let mut element = Element {
                tag,
                attributes: Vec::new(),
                events: Vec::new(),
                children: children.iter().map(node).collect::<Result<_, _>>()?,
            };
            for (key, value) in map {
                let Syntax::Reference(name) = key else {
                    return Err(refused(format!(
                        "{key} as an attribute of [:{} …]: an attribute's name is a keyword",
                        element.tag
                    )));
                };
                match name.strip_prefix("on/") {
                    Some(kind) => element.events.push(event(kind, value)?),
                    None => element
                        .attributes
                        .push((name.clone(), attribute(name, value)?)),
                }
            }
            Node::Element(element)
        }
        Syntax::Map(entries) => Node::View(nested(tree, entries)?),
        Syntax::Application(..) if tree.applies() == Some("view") => {
            Node::Call(Box::new(call(tree)?))
        }
        Syntax::Variable(name) => Node::Variable(name.clone()),
        Syntax::Constant(Value::String(text)) => Node::Text(text.clone()),
        Syntax::Constant(Value::Boolean(_)) => {
            return Err(Error::Unsupported(format!(
                "{tree} in a template: the design has no boolean term"
            )));
        }
        Syntax::Constant(_) => Node::Value(tree.clone()),
        other => {
            return Err(refused(format!(
                "{other} in a template: a template holds elements, text, variables and views"
            )));
        }
    })
}

/// An attribute's value.
///
/// A list is its parts joined, as text children are, `:href ["/n/" ?n]`,
/// and `true` is the attribute present with no value, `:draft true`:
/// both the managing agent's readings. `false` is refused: an attribute
/// that is not there is left out.
fn attribute(name: &str, value: &Syntax) -> Result<Attribute, Error> {
    Ok(match value {
        Syntax::Constant(Value::String(text)) => Attribute::Text(text.clone()),
        Syntax::Variable(variable) => Attribute::Variable(variable.clone()),
        Syntax::Constant(Value::Boolean(true)) => Attribute::Present,
        Syntax::Constant(Value::Boolean(false)) => {
            return Err(refused(format!(
                ":{name} false: an attribute that is not there is left out, not written `false`"
            )));
        }
        Syntax::Tuple(items) => Attribute::Parts(pieces(name, items)?),
        Syntax::Constant(_) => Attribute::Value(value.clone()),
        other => {
            return Err(refused(format!(
                ":{name} {other}: an attribute's value is text, a value, a variable, a list of \
                 those or `true`"
            )));
        }
    })
}

/// The parts of a value written as a list: text, values and variables.
fn pieces(name: &str, items: &[Syntax]) -> Result<Vec<Piece>, Error> {
    items
        .iter()
        .map(|item| match item {
            Syntax::Constant(Value::String(text)) => Ok(Piece::Text(text.clone())),
            Syntax::Variable(variable) => Ok(Piece::Variable(variable.clone())),
            Syntax::Constant(Value::Boolean(_)) => Err(refused(format!(
                ":{name}: {item} is no part of a value; its parts are text, values and variables"
            ))),
            Syntax::Constant(_) => Ok(Piece::Value(item.clone())),
            other => Err(refused(format!(
                ":{name}: {other} is no part of a value; its parts are text, values and variables"
            ))),
        })
        .collect()
}

/// `:on/<kind> (command field: term …)`.
fn event(kind: &str, value: &Syntax) -> Result<Event, Error> {
    let (Some(command), Some(operand)) = (value.applies(), value.operand()) else {
        return Err(refused(format!(
            ":on/{kind} {value}: an event binds the command it posts, `(todo/toggle todo: ?t)`"
        )));
    };
    if !operand.items.is_empty() {
        return Err(refused(format!(
            ":on/{kind} {value}: the command an event posts takes its fields by label"
        )));
    }
    let fields = operand
        .terms
        .iter()
        .map(|(label, term)| {
            let input = match term {
                Syntax::Variable(name) => Input::Variable(name.clone()),
                Syntax::Reference(path) if path.starts_with('.') => Input::Path(
                    path.split('.')
                        .filter(|step| !step.is_empty())
                        .map(str::to_string)
                        .collect(),
                ),
                Syntax::Constant(_) => Input::Value(term.clone()),
                other => {
                    return Err(refused(format!(
                        ":on/{kind}: {label}: {other} is no variable, value or path into the event"
                    )));
                }
            };
            Ok((label.clone(), input))
        })
        .collect::<Result<_, Error>>()?;
    Ok(Event {
        kind: kind.to_string(),
        command: command.to_string(),
        fields,
    })
}

/// `(view concept :name entity label: term …)` in a template: the entity
/// first unlabeled, or as `of:`, and every other label a field of the
/// concept. What analysis says of it is filled in by [`fill`].
fn call(tree: &Syntax) -> Result<Call, Error> {
    let shape = || {
        refused(format!(
            "{tree}: a view called in a template is `(view concept :name entity label: term …)`"
        ))
    };
    let operand = tree.operand().ok_or_else(shape)?;
    let (concept, view, entity) = match operand.items.as_slice() {
        [Syntax::Reference(concept), name, rest @ ..] if rest.len() <= 1 => {
            let view = keyword(name).ok_or_else(shape)?.to_string();
            (concept.clone(), view, rest.first().cloned())
        }
        _ => return Err(shape()),
    };
    let of = match (entity, operand.terms.get("of")) {
        (Some(_), Some(_)) => {
            return Err(refused(format!(
                "{tree}: the entity is written once, first or as `of:`"
            )));
        }
        (Some(entity), None) => Some(entity),
        (None, Some(entity)) => Some(entity.clone()),
        (None, None) => None,
    };
    if let Some(of) = &of
        && !matches!(of, Syntax::Variable(_) | Syntax::Constant(_))
    {
        return Err(refused(format!(
            "{tree}: the entity a view is called for is a variable or a value, not {of}"
        )));
    }
    Ok(Call {
        concept,
        view,
        of,
        labels: operand
            .terms
            .iter()
            .filter(|(label, _)| label.as_str() != "of")
            .map(|(label, term)| (label.clone(), term.clone()))
            .collect(),
        found: false,
        narrowing: Vec::new(),
        fields: Vec::new(),
        source: Vec::new(),
    })
}

/// What analysis said of each call, `[found narrowing fields source]`,
/// put into the calls of a template in the order it holds them.
fn fill<'a>(node: &mut Node, said: &mut impl Iterator<Item = &'a Syntax>) -> Result<(), Error> {
    match node {
        Node::Call(call) => {
            let Some(Syntax::Tuple(parts)) = said.next() else {
                return Err(refused(format!(
                    "the call of {} :{} has nothing said of it",
                    call.concept, call.view
                )));
            };
            let premises = |part: Option<&Syntax>| match part {
                Some(Syntax::Tuple(items)) => items.clone(),
                _ => Vec::new(),
            };
            call.found = matches!(parts.first(), Some(Syntax::Constant(Value::Boolean(true))));
            call.narrowing = premises(parts.get(1));
            call.fields = premises(parts.get(2))
                .iter()
                .filter_map(|field| match field {
                    Syntax::Reference(label) => Some(label.clone()),
                    _ => None,
                })
                .collect();
            call.source = premises(parts.get(3));
        }
        Node::Element(element) => {
            for child in &mut element.children {
                fill(child, said)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// `{:view :row :of ?t}`. `:of` may be left out, the view of every
/// instance; `:model` names the concept whose view it is; any other
/// entry is kept with its parts, the notation giving it no meaning. Each
/// the managing agent's reading.
fn nested(tree: &Syntax, entries: &[(Syntax, Syntax)]) -> Result<Nested, Error> {
    let mut nested = Nested {
        view: String::new(),
        of: None,
        model: None,
        entries: Vec::new(),
    };
    for (key, value) in entries {
        let Syntax::Reference(name) = key else {
            return Err(refused(format!(
                "{tree}: a nested view is `{{:view :row :of ?t}}`"
            )));
        };
        match (name.as_str(), value) {
            ("view", value) => {
                nested.view = keyword(value)
                    .ok_or_else(|| refused(format!("{tree}: `:view` names a view, `:row`")))?
                    .to_string();
            }
            ("of", Syntax::Variable(variable)) => nested.of = Some(variable.clone()),
            ("of", other) => {
                return Err(refused(format!(
                    "{tree}: `:of` is the variable of the entity rendered, not {other}"
                )));
            }
            ("model", Syntax::Reference(concept)) => nested.model = Some(concept.clone()),
            ("model", other) => {
                return Err(refused(format!(
                    "{tree}: `:model` names a concept, not {other}"
                )));
            }
            (_, Syntax::Tuple(items)) => nested.entries.push((name.clone(), pieces(name, items)?)),
            (_, value) => nested
                .entries
                .push((name.clone(), pieces(name, std::slice::from_ref(value))?)),
        }
    }
    if nested.view.is_empty() {
        return Err(refused(format!(
            "{tree}: a nested view names its view, `{{:view :row :of ?t}}`"
        )));
    }
    Ok(nested)
}
