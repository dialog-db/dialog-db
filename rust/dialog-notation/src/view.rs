//! Markup: what a query whose answer is a view says, and how a back end
//! keeps it.
//!
//! A query answers with markup when its body says `(view ?x template)`:
//!
//! ```clj
//! (query (todo/row ?t list: ?l is: ?row)
//!   (todo ?t list: ?l title: ?title)
//!   (todo/chip ?t is: (distinct ?chips))
//!   (view ?row [:li ?title ?chips]))
//! ```
//!
//! The template is Hiccup: an element is `[:tag {attributes} child…]`,
//! with events inline as `:on/<type>`, each the command it posts. A
//! variable is a value the query binds, or the markup of a query it
//! collects, which stands there as children, in the order of their keys.
//! [`template`] reads one into a [`Node`].
//!
//! [`plan`] lays a query and every query it collects out in layers, each
//! a query of its own, and [`tree`] keeps their instances from the rows
//! of those queries as they come and go, and says each change as
//! patches. No value is folded. The Dialog-DB back end,
//! [`compile`](crate::compile()), gives a query whose answer is markup as
//! its plan.

use dialog_corpus::Value;

use crate::Error;
use crate::syntax::Syntax;

pub mod plan;
pub mod tree;

/// The variable a view not found, shown as its source, names the entity
/// it reads by where the call names none.
pub const CALLED: &str = "view-of";

/// A part of a template.
#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    /// `[:li {…} …]`.
    Element(Element),
    /// Text, as written.
    Text(String),
    /// A value other than text: a number.
    Value(Syntax),
    /// What a condition bound this variable to, or the markup of a query
    /// collected.
    Variable(String),
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

fn keyword(node: &Syntax) -> Option<&str> {
    node.keyword()
}

fn refused(why: String) -> Error {
    Error::Refused(why)
}

/// The node a template is: `[:li {:class ?s} ?title]`.
pub fn template(tree: &Syntax) -> Result<Node, Error> {
    match node(tree)? {
        element @ Node::Element(_) => Ok(element),
        other => Err(refused(format!(
            "{tree} as a template: a view's markup is an element, `[:li …]`, not {other:?}"
        ))),
    }
}

/// Every variable a template holds, events included, each once, in the
/// order written.
pub fn variables(node: &Node) -> Vec<String> {
    fn add(name: &str, into: &mut Vec<String>) {
        if !into.iter().any(|known| known == name) {
            into.push(name.to_string());
        }
    }
    fn walk(node: &Node, into: &mut Vec<String>) {
        match node {
            Node::Variable(name) => add(name, into),
            Node::Element(element) => {
                for (_, attribute) in &element.attributes {
                    match attribute {
                        Attribute::Variable(name) => add(name, into),
                        Attribute::Parts(pieces) => {
                            for piece in pieces {
                                if let Piece::Variable(name) = piece {
                                    add(name, into);
                                }
                            }
                        }
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
    walk(node, &mut names);
    names
}

/// Every event a template binds, in the order written.
pub fn events(node: &Node) -> Vec<&Event> {
    fn walk<'a>(node: &'a Node, into: &mut Vec<&'a Event>) {
        if let Node::Element(element) = node {
            into.extend(&element.events);
            element.children.iter().for_each(|child| walk(child, into));
        }
    }
    let mut found = Vec::new();
    walk(node, &mut found);
    found
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
            // Hiccup.
            let (map, children) = match rest.split_first() {
                Some((Syntax::Map(entries), children)) => (entries.as_slice(), children),
                _ => (&[][..], rest),
            };
            // A map that names a view is how a view was nested by
            // reference, `[:ul {:view :row :of ?t}]`.
            if map
                .iter()
                .any(|(key, _)| matches!(key, Syntax::Reference(name) if name == "view"))
            {
                return Err(refused(format!(
                    "{tree}: a view nested by reference is a query collected, `(todo/row _ is: \
                     (distinct ?rows))`, its variable put where it stands"
                )));
            }
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
        Syntax::Map(_) => {
            return Err(refused(format!(
                "{tree} in a template: a view nested by reference is a query collected, \
                 `(todo/row _ is: (distinct ?rows))`, its variable put where it stands"
            )));
        }
        Syntax::Application(..) if tree.applies() == Some("view") => {
            return Err(refused(format!(
                "{tree} in a template: a view is a query whose answer is markup, and one in a \
                 template is that query collected, `(todo/chip ?t is: (distinct ?chips))`, its \
                 variable put where it stands"
            )));
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
                "{other} in a template: a template holds elements, text and variables"
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
