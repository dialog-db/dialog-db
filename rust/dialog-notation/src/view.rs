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
//! nested views. A back end lowers it to whatever it renders. The
//! Dialog-DB back end, [`compile`](crate::compile()), refuses it: as
//! designed a view is a rule that folds into its value, which is the
//! question of #830.

use dialog_corpus::Value;

use crate::Error;
use crate::syntax::Syntax;

pub mod plan;

/// A view: the concept it renders, its name, the variable that is the
/// entity rendered, its template and its conditions.
#[derive(Clone, Debug, PartialEq)]
pub struct View {
    /// The concept, by its name.
    pub concept: String,
    /// The view's name: `row` for `:row`.
    pub name: String,
    /// The variable the conditions bind to the entity rendered.
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
}

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
        Ok(View {
            concept,
            name,
            entity,
            template: node(at("template")?)?,
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
                Node::Text(_) | Node::Value(_) => {}
            }
        }
        let mut names = Vec::new();
        walk(&self.template, &mut names);
        names
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
