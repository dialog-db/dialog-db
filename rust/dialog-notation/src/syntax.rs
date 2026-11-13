//! The syntax tree: what the notation says, apart from how it was
//! written.
//!
//! There are three things, and this is the middle one:
//!
//! ```no_run
//! # fn main() -> Result<(), dialog_notation::Error> {
//! use dialog_notation::{Syntax, read};
//!
//! // The notation as parsed: a head, a part with no label, a labelled part.
//! let form = &read("(todo ?t state: :open)")?[0];
//!
//! // The syntax tree: an application of an operator to parameters.
//! let syntax = Syntax::try_from(form)?;
//!
//! // The value the tree is kept as: maps that say their type.
//! //
//! // { type: :application
//! //   operator: { type: :reference name: /todo }
//! //   operand: { type: :parameters
//! //              items: [{ type: :variable name: "t" }]
//! //              terms: { state: { type: :constant value: :open } } } }
//! let kept = syntax.value()?;
//! assert_eq!(Syntax::try_from(&kept)?, syntax);
//! # Ok(())
//! # }
//! ```
//!
//! A [`Form`] keeps what was written: the order of labelled parts, a
//! number's digits, which bracket. A [`Syntax`] keeps what was said. The
//! reader can change and the tree does not.
//!
//! Nothing here knows what a name refers to. An operator is a node like
//! any other, most often a [`Syntax::Reference`]; `todo` is a name until
//! it is resolved.
//!
//! The parts of an application written with no label stay in the order
//! written, as [`Parameters::items`], so `A` and `A B`
//! have one shape. `of:` is the label of the part with no label: where
//! no part is written without one, the part labelled `of:` is the
//! first item, and `(todo ?t state: :open)` and
//! `(todo state: :open of: ?t)` are one tree.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use base58::ToBase58;
use dialog_corpus::{CompositeMap, Entity, Value};
use merkle_reference::Reference;

use crate::Error;
use crate::data;
use crate::form::{Form, Kind, Mark, Part};
use crate::written::{Note, Path, Segment, Slot, Span, Written};

/// A node of the syntax tree.
#[derive(Clone, Debug, PartialEq)]
pub enum Syntax {
    /// An operator applied to parameters: `(todo ?t state: :open)`.
    Application(Box<Syntax>, Parameters),
    /// `?name`.
    Variable(String),
    /// `_`.
    Blank,
    /// A value: `"text"`, `42`, `true`, `:open`, `todo:1`.
    Constant(Value),
    /// A name, yet to be resolved: `todo`, `xyz.todo`, `xyz.todo/text`,
    /// `+`.
    Reference(String),
    /// A struct, `{key value}`, its keys nodes like its values, in the
    /// order written, which its name does not depend on. With no
    /// variable in it, it is a value.
    Map(Vec<(Syntax, Syntax)>),
    /// A tuple, `[a b]`.
    Tuple(Vec<Syntax>),
    /// `p ...`, in a pattern and in a template: any number of `p`.
    Repeat(Box<Syntax>),
}

/// What an operator is applied to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Parameters {
    /// The parts written with no label, in the order written.
    pub items: Vec<Syntax>,
    /// The labelled parts. The order they were written in is not kept.
    pub terms: BTreeMap<String, Syntax>,
    /// `...?name`, in a pattern and in a template: the labelled parts
    /// not named.
    pub rest: Option<String>,
}

fn refused(what: &str, error: impl Display) -> Error {
    Error::Refused(format!("{what}: {error}"))
}

/// A number as written, as the value it is: a natural where it can be,
/// then an integer, then a float.
pub(crate) fn number(text: &str) -> Result<Value, Error> {
    if let Ok(natural) = text.parse::<u64>() {
        Ok(Value::UnsignedInt(natural.into()))
    } else if let Ok(integer) = text.parse::<i64>() {
        Ok(Value::SignedInt(integer.into()))
    } else {
        text.parse::<f64>()
            .ok()
            .filter(|float| float.is_finite())
            .map(Value::Float)
            .ok_or_else(|| Error::Refused(format!("{text} is not a number")))
    }
}

pub(crate) fn entity(uri: &str) -> Result<Value, Error> {
    Entity::from_str(uri)
        .map(Value::Entity)
        .map_err(|error| refused("not an entity", error))
}

/// The nodes a run of forms is: a form followed by `...` is that form,
/// repeated.
fn run<'a>(
    forms: impl IntoIterator<Item = (&'a Form, Option<&'a Mark>)>,
    path: &mut Path,
    written: &mut Written,
) -> Result<Vec<Syntax>, Error> {
    let mut nodes: Vec<Syntax> = Vec::new();
    for (form, mark) in forms {
        match form {
            Form::Ellipsis => match nodes.pop() {
                Some(Syntax::Repeat(_)) | None => {
                    if let Some(mark) = mark {
                        written.refused.get_or_insert(mark.span);
                    }
                    return Err(Error::Refused("a `...` that repeats nothing".into()));
                }
                Some(repeated) => {
                    // The node the `...` follows is now what a repeat
                    // repeats, and the repeat is where it was.
                    path.push(Segment::Item(nodes.len()));
                    if let Some(mark) = mark {
                        let start = written
                            .span(path)
                            .map_or(mark.span.start, |span| span.start);
                        written.repeat(path);
                        written.notes.insert(
                            path.clone(),
                            Note {
                                span: Span {
                                    start,
                                    end: mark.span.end,
                                },
                                closing: mark.comments.clone(),
                                after: mark.after.clone(),
                                ..Note::default()
                            },
                        );
                    }
                    path.pop();
                    nodes.push(Syntax::Repeat(Box::new(repeated)));
                }
            },
            form => {
                path.push(Segment::Item(nodes.len()));
                let node = tree(form, mark, false, path, written);
                path.pop();
                nodes.push(node?);
            }
        }
    }
    Ok(nodes)
}

/// The node a form is. Where the form has a mark, what the node leaves
/// out of it is noted in `written` under `path`. `key` says the form is
/// the key of a struct's entry, which is spelled as a label where it
/// can be.
pub(crate) fn tree(
    form: &Form,
    mark: Option<&Mark>,
    key: bool,
    path: &mut Path,
    written: &mut Written,
) -> Result<Syntax, Error> {
    let mut note = mark.map(|mark| Note {
        span: mark.span,
        comments: mark.comments.clone(),
        closing: mark.closing.clone(),
        after: mark.after.clone(),
        ..Note::default()
    });
    let inside = |at: usize| mark.and_then(|mark| mark.parts.get(at));
    let node = match made(form, mark, &inside, path, written, note.as_mut()) {
        Ok(node) => node,
        Err(error) => {
            // What is refused is the innermost form that is: the first
            // to say so.
            if let Some(mark) = mark {
                written.refused.get_or_insert(mark.span);
            }
            return Err(error);
        }
    };
    if let (Some(mark), Some(mut note)) = (mark, note) {
        // An atom's token is kept where a printer would spell it
        // otherwise.
        if !mark.raw.is_empty() && mark.raw != node.spelled(key) {
            note.spelling = Some(mark.raw.clone());
        }
        written.notes.insert(path.clone(), note);
    }
    Ok(node)
}

/// The node `form` is, its parts noted in `written`, and the order they
/// were written in noted in `note`.
fn made<'a>(
    form: &'a Form,
    mark: Option<&'a Mark>,
    inside: &dyn Fn(usize) -> Option<&'a Mark>,
    path: &mut Path,
    written: &mut Written,
    mut note: Option<&mut Note>,
) -> Result<Syntax, Error> {
    Ok(match form {
        Form::Text(text) => Syntax::Constant(Value::String(text.clone())),
        Form::Number(text) => Syntax::Constant(number(text)?),
        Form::Keyword(name) => Syntax::Constant(entity(&format!("{name}:"))?),
        Form::Symbol(name) if name == "true" || name == "false" => {
            Syntax::Constant(Value::Boolean(name == "true"))
        }
        Form::Symbol(name) if form.kind() == Some(Kind::Entity) => Syntax::Constant(entity(name)?),
        Form::Symbol(name) => Syntax::Reference(name.clone()),
        Form::Variable(name) => Syntax::Variable(name.clone()),
        Form::Blank => Syntax::Blank,
        Form::Ellipsis => {
            return Err(Error::Refused("a `...` that repeats nothing".into()));
        }
        Form::Rest(name) => {
            return Err(Error::Refused(format!(
                "`...?{name}` stands for labelled parts, and is not among the parts of an application"
            )));
        }
        Form::Vector(items) => Syntax::Tuple(run(
            items
                .iter()
                .enumerate()
                .map(|(at, item)| (item, inside(at))),
            path,
            written,
        )?),
        Form::Map(entries) => {
            let mut nodes = Vec::new();
            for (at, (name, value)) in entries.iter().enumerate() {
                path.push(Segment::Key(at));
                let name = tree(name, inside(2 * at), true, path, written);
                path.pop();
                path.push(Segment::Value(at));
                let value = tree(value, inside(2 * at + 1), false, path, written);
                path.pop();
                nodes.push((name?, value?));
            }
            Syntax::Map(nodes)
        }
        Form::Application(head, parts) => {
            let mut operand = Parameters::default();
            let mut bare: Vec<(&Form, Option<&Mark>)> = Vec::new();
            let mut order = Vec::new();
            // `of:` is the part with no label, written with one.
            let primary = form.of().is_some();
            for (at, part) in parts.iter().enumerate() {
                let part_mark = inside(at + 1);
                let label_mark = mark
                    .and_then(|mark| mark.labels.get(at))
                    .and_then(Option::as_ref);
                match part {
                    Part::Bare(Form::Rest(name)) => {
                        if operand.rest.replace(name.clone()).is_some() {
                            return Err(Error::Refused(format!(
                                "{form} takes the labelled parts not named twice"
                            )));
                        }
                        if let (Some(rest), Some(note)) = (part_mark, note.as_mut()) {
                            order.push(Slot::Rest {
                                at: rest.span.start,
                            });
                            note.closing
                                .extend(rest.comments.iter().chain(&rest.after).cloned());
                            note.closing.sort_by_key(|comment| comment.at);
                        }
                    }
                    Part::Bare(part) => {
                        if !matches!(part, Form::Ellipsis) {
                            order.push(Slot::Item {
                                at: bare
                                    .iter()
                                    .filter(|(form, _)| **form != Form::Ellipsis)
                                    .count(),
                                of: None,
                            });
                        }
                        bare.push((part, part_mark));
                    }
                    Part::Labelled(label, part) => {
                        let item = primary && label == "of";
                        path.push(if item {
                            Segment::Item(0)
                        } else {
                            Segment::Term(label.clone())
                        });
                        let term = tree(part, part_mark, false, path, written);
                        // A comment above a label is about the part
                        // under it.
                        if let (Some((at, comments)), Some(noted)) =
                            (label_mark, written.notes.get_mut(path.as_slice()))
                        {
                            noted.comments.splice(0..0, comments.iter().cloned());
                            order.push(if item {
                                Slot::Item {
                                    at: 0,
                                    of: Some(*at),
                                }
                            } else {
                                Slot::Term {
                                    label: label.clone(),
                                    at: *at,
                                }
                            });
                        }
                        path.pop();
                        let term = term?;
                        if item {
                            operand.items.push(term);
                        } else if operand.terms.insert(label.clone(), term).is_some() {
                            return Err(Error::Refused(format!("{form} says `{label}:` twice")));
                        }
                    }
                }
            }
            if !bare.is_empty() {
                operand.items = run(bare, path, written)?;
            }
            path.push(Segment::Operator);
            let operator = tree(head, inside(0), false, path, written);
            path.pop();
            if let Some(note) = note.as_mut() {
                note.order = order;
            }
            Syntax::Application(Box::new(operator?), operand)
        }
    })
}

impl TryFrom<&Form> for Syntax {
    type Error = Error;

    fn try_from(form: &Form) -> Result<Self, Error> {
        tree(form, None, false, &mut Path::new(), &mut Written::default())
    }
}

/// A node of `kind` with `fields`.
fn node<'a>(
    kind: &str,
    fields: impl IntoIterator<Item = (&'a str, Value)>,
) -> Result<Value, Error> {
    data::record(
        fields
            .into_iter()
            .chain([("type", data::entity(&format!("{kind}:"))?)]),
    )
}

fn list(nodes: &[Syntax]) -> Result<Value, Error> {
    Ok(data::list(
        nodes
            .iter()
            .map(Syntax::value)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

impl Parameters {
    fn value(&self) -> Result<Value, Error> {
        let terms = data::record(
            self.terms
                .iter()
                .map(|(label, term)| Ok((label.as_str(), term.value()?)))
                .collect::<Result<Vec<_>, Error>>()?,
        )?;
        let rest = self
            .rest
            .as_ref()
            .map(|name| node("rest", [("name", data::text(name))]))
            .transpose()?;
        node(
            "parameters",
            [("items", list(&self.items)?), ("terms", terms)]
                .into_iter()
                .chain(rest.map(|rest| ("rest", rest))),
        )
    }
}

impl Syntax {
    /// The value the tree is kept as: a composite, every node a map
    /// that says its `type`.
    ///
    /// | node | fields |
    /// |---|---|
    /// | `:application` | `operator`, a node; `operand`, a `:parameters` |
    /// | `:parameters` | `items`, a list of nodes; `terms`, a map from label to node; `rest`, a `:rest`, where one is written |
    /// | `:variable` | `name`, text |
    /// | `:blank` | |
    /// | `:constant` | `value` |
    /// | `:reference` | `name`, a symbol |
    /// | `:map` | `entries`, a map from node to node |
    /// | `:tuple` | `items`, a list of nodes |
    /// | `:repeat` | `of`, a node |
    /// | `:rest` | `name`, text |
    pub fn value(&self) -> Result<Value, Error> {
        match self {
            Syntax::Application(operator, operand) => node(
                "application",
                [
                    ("operator", operator.value()?),
                    ("operand", operand.value()?),
                ],
            ),
            Syntax::Variable(name) => node("variable", [("name", data::text(name))]),
            Syntax::Blank => node("blank", []),
            Syntax::Constant(value) => node("constant", [("value", value.clone())]),
            Syntax::Reference(name) => node("reference", [("name", data::symbol(name)?)]),
            Syntax::Map(entries) => node(
                "map",
                [(
                    "entries",
                    data::map(
                        entries
                            .iter()
                            .map(|(key, value)| Ok((key.value()?, value.value()?)))
                            .collect::<Result<Vec<_>, Error>>()?,
                    )?,
                )],
            ),
            Syntax::Tuple(items) => node("tuple", [("items", list(items)?)]),
            Syntax::Repeat(repeated) => node("repeat", [("of", repeated.value()?)]),
        }
    }

    /// The name this applies, where it is the application of a name.
    pub fn applies(&self) -> Option<&str> {
        match self {
            Syntax::Application(operator, _) => match &**operator {
                Syntax::Reference(name) => Some(name),
                _ => None,
            },
            _ => None,
        }
    }

    /// What this application's operator is applied to.
    pub fn operand(&self) -> Option<&Parameters> {
        match self {
            Syntax::Application(_, operand) => Some(operand),
            _ => None,
        }
    }

    /// The keyword this is, without its `:`. A keyword is the entity
    /// `name:`.
    pub fn keyword(&self) -> Option<&str> {
        match self {
            Syntax::Constant(Value::Entity(entity)) => entity
                .as_str()
                .strip_suffix(':')
                .filter(|name| !name.is_empty() && !name.contains(':')),
            _ => None,
        }
    }

    /// Whether this applies a relation: its operator is a domain.
    pub fn is_read(&self) -> bool {
        self.applies()
            .is_some_and(|name| Kind::of(name) == Kind::Domain)
    }

    /// Whether this applies a formula: its operator is a keyword or an
    /// operator.
    pub fn is_formula(&self) -> bool {
        match self {
            Syntax::Application(operator, _) => {
                operator.keyword().is_some()
                    || matches!(&**operator, Syntax::Reference(name) if Kind::of(name) == Kind::Operator)
            }
            _ => false,
        }
    }

    /// The names of the variables in the tree, in the order they occur:
    /// the parts with no label first, then the labelled parts by label.
    pub fn variables(&self, into: &mut Vec<String>) {
        match self {
            Syntax::Variable(name) => into.push(name.clone()),
            Syntax::Application(operator, operand) => {
                operator.variables(into);
                operand.items.iter().for_each(|item| item.variables(into));
                operand.terms.values().for_each(|term| term.variables(into));
            }
            Syntax::Map(entries) => entries.iter().for_each(|(key, value)| {
                key.variables(into);
                value.variables(into);
            }),
            Syntax::Tuple(items) => items.iter().for_each(|item| item.variables(into)),
            Syntax::Repeat(repeated) => repeated.variables(into),
            Syntax::Blank | Syntax::Constant(_) | Syntax::Reference(_) => {}
        }
    }

    /// What names the tree: the reference of the value it is kept as.
    /// Two forms that say one thing have one reference, whatever order
    /// their labels were written in, and no layout has a part in it.
    pub fn reference(&self) -> Result<Reference, Error> {
        Ok(self.value()?.reference())
    }

    /// The entity that names the tree: `form:` and its reference.
    pub fn entity(&self) -> Result<Entity, Error> {
        Entity::from_str(&format!(
            "form:{}",
            self.reference()?.as_bytes().to_base58()
        ))
        .map_err(|error| refused("not an entity", error))
    }

    /// The tree as bytes: the ones the `record:` value it is kept as
    /// holds.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        match self.value()? {
            Value::Record(bytes) => Ok(bytes),
            other => Err(Error::Read(format!("a node that is no record: {other:?}"))),
        }
    }

    /// The tree `bytes` hold.
    pub fn decode(bytes: &[u8]) -> Result<Syntax, Error> {
        Syntax::try_from(&Value::Record(bytes.to_vec()))
    }

    /// The way from this node to the first node inside it that is
    /// `part`, or to itself.
    pub fn locate(&self, part: &Syntax) -> Option<Path> {
        fn find(node: &Syntax, part: &Syntax, path: &mut Path) -> bool {
            if node == part {
                return true;
            }
            let inside = |segment: Segment, node: &Syntax, path: &mut Path| {
                path.push(segment);
                let found = find(node, part, path);
                if !found {
                    path.pop();
                }
                found
            };
            match node {
                Syntax::Application(operator, operand) => {
                    inside(Segment::Operator, operator, path)
                        || operand
                            .items
                            .iter()
                            .enumerate()
                            .any(|(at, item)| inside(Segment::Item(at), item, path))
                        || operand
                            .terms
                            .iter()
                            .any(|(label, term)| inside(Segment::Term(label.clone()), term, path))
                }
                Syntax::Tuple(items) => items
                    .iter()
                    .enumerate()
                    .any(|(at, item)| inside(Segment::Item(at), item, path)),
                Syntax::Map(entries) => entries.iter().enumerate().any(|(at, (name, value))| {
                    inside(Segment::Key(at), name, path) || inside(Segment::Value(at), value, path)
                }),
                Syntax::Repeat(repeated) => inside(Segment::Repeated, repeated, path),
                _ => false,
            }
        }
        let mut path = Path::new();
        find(self, part, &mut path).then_some(path)
    }

    /// How a printer spells this node when nothing says how it was
    /// written. The key of a struct's entry is spelled as a label where
    /// it can be.
    pub fn spelled(&self, key: bool) -> String {
        match (key, self.form()) {
            (true, form) if form.label().is_some() => {
                format!("{}:", form.label().unwrap_or_default())
            }
            (_, form) => form.to_string(),
        }
    }

    /// The notation that says this. The parts with no label come first,
    /// then the labelled parts by label.
    pub fn form(&self) -> Form {
        match self {
            Syntax::Application(operator, operand) => {
                let mut parts = Vec::new();
                for item in &operand.items {
                    item.write(&mut parts);
                }
                let mut parts: Vec<Part> = parts.into_iter().map(Part::Bare).collect();
                parts.extend(
                    operand
                        .terms
                        .iter()
                        .map(|(label, term)| Part::Labelled(label.clone(), term.form())),
                );
                parts.extend(
                    operand
                        .rest
                        .iter()
                        .map(|name| Part::Bare(Form::Rest(name.clone()))),
                );
                Form::Application(Box::new(operator.form()), parts)
            }
            Syntax::Variable(name) => Form::Variable(name.clone()),
            Syntax::Blank => Form::Blank,
            Syntax::Constant(value) => match value {
                Value::String(text) => Form::Text(text.clone()),
                Value::Boolean(truth) => Form::Symbol(truth.to_string()),
                Value::UnsignedInt(natural) => Form::Number(natural.to_string()),
                Value::SignedInt(integer) => Form::Number(integer.to_string()),
                Value::Float(float) => Form::Number(format!("{float:?}")),
                Value::Entity(entity) => match entity.as_str().strip_suffix(':') {
                    Some(name) if !name.is_empty() && !name.contains(':') => {
                        Form::Keyword(name.to_string())
                    }
                    _ => Form::Symbol(entity.as_str().to_string()),
                },
                Value::Symbol(_) => Form::Symbol(data::spelled(value).unwrap_or_default()),
                // The notation writes neither.
                Value::Bytes(_) | Value::Record(_) => Form::Blank,
            },
            Syntax::Reference(name) => Form::Symbol(name.clone()),
            Syntax::Map(entries) => Form::Map(
                entries
                    .iter()
                    .map(|(key, value)| (key.form(), value.form()))
                    .collect(),
            ),
            Syntax::Tuple(items) => {
                let mut forms = Vec::new();
                for item in items {
                    item.write(&mut forms);
                }
                Form::Vector(forms)
            }
            Syntax::Repeat(repeated) => repeated.form(),
        }
    }

    /// This node among the members of a run: a repeat is what it
    /// repeats and then `...`.
    fn write(&self, forms: &mut Vec<Form>) {
        forms.push(self.form());
        if matches!(self, Syntax::Repeat(_)) {
            forms.push(Form::Ellipsis);
        }
    }
}

impl Display for Syntax {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut forms = Vec::new();
        self.write(&mut forms);
        for (at, form) in forms.iter().enumerate() {
            if at > 0 {
                write!(f, " ")?;
            }
            write!(f, "{form}")?;
        }
        Ok(())
    }
}

fn unread(what: &str) -> Error {
    Error::Read(format!("a node with no {what}"))
}

/// The node `value` is: a map that says its type.
fn fields(value: &Value) -> Result<CompositeMap, Error> {
    data::entries(value)?.ok_or_else(|| unread("type"))
}

fn nodes(node: &CompositeMap, field: &str) -> Result<Vec<Syntax>, Error> {
    data::field(node, field)
        .map(data::items)
        .transpose()?
        .flatten()
        .ok_or_else(|| unread(field))?
        .iter()
        .map(Syntax::try_from)
        .collect()
}

fn text(node: &CompositeMap, field: &str) -> Result<String, Error> {
    match data::field(node, field) {
        Some(Value::String(text)) => Ok(text.clone()),
        _ => Err(unread(field)),
    }
}

/// The type a node says it is: the name of the entity under `type`.
fn kind(node: &CompositeMap) -> Result<&str, Error> {
    match data::field(node, "type") {
        Some(Value::Entity(entity)) => entity
            .as_str()
            .strip_suffix(':')
            .ok_or_else(|| unread("type")),
        _ => Err(unread("type")),
    }
}

impl TryFrom<&Value> for Parameters {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Error> {
        let node = fields(value)?;
        if kind(&node)? != "parameters" {
            return Err(unread("parameters"));
        }
        let terms = data::field(&node, "terms")
            .map(data::entries)
            .transpose()?
            .flatten()
            .ok_or_else(|| unread("terms"))?
            .iter()
            .map(|(label, term)| {
                Ok((
                    data::spelled(label).ok_or_else(|| unread("label"))?,
                    Syntax::try_from(term)?,
                ))
            })
            .collect::<Result<_, Error>>()?;
        let rest = data::field(&node, "rest")
            .map(|rest| {
                let rest = fields(rest)?;
                match kind(&rest)? {
                    "rest" => text(&rest, "name"),
                    _ => Err(unread("rest")),
                }
            })
            .transpose()?;
        Ok(Parameters {
            items: nodes(&node, "items")?,
            terms,
            rest,
        })
    }
}

impl TryFrom<&Value> for Syntax {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Error> {
        let node = fields(value)?;
        let part = |field: &str| data::field(&node, field).ok_or_else(|| unread(field));
        Ok(match kind(&node)? {
            "application" => Syntax::Application(
                Box::new(Syntax::try_from(part("operator")?)?),
                Parameters::try_from(part("operand")?)?,
            ),
            "variable" => Syntax::Variable(text(&node, "name")?),
            "blank" => Syntax::Blank,
            "constant" => match part("value")? {
                Value::Bytes(_) | Value::Record(_) => {
                    return Err(Error::Unsupported(
                        "a constant that is bytes or a record: the notation writes neither".into(),
                    ));
                }
                value => Syntax::Constant(value.clone()),
            },
            "reference" => {
                Syntax::Reference(data::spelled(part("name")?).ok_or_else(|| unread("name"))?)
            }
            "map" => Syntax::Map(
                data::entries(part("entries")?)?
                    .ok_or_else(|| unread("entries"))?
                    .iter()
                    .map(|(key, value)| Ok((Syntax::try_from(key)?, Syntax::try_from(value)?)))
                    .collect::<Result<_, Error>>()?,
            ),
            "tuple" => Syntax::Tuple(nodes(&node, "items")?),
            "repeat" => Syntax::Repeat(Box::new(Syntax::try_from(part("of")?)?)),
            other => {
                return Err(Error::Read(format!(
                    "a node of no type this reads: {other}"
                )));
            }
        })
    }
}

/// What a name is given under: the name, what kind of thing is named,
/// and the parameters it is applied with.
///
/// A concept's entity is made of its attributes and does not say what
/// its fields are called, so two names over the same attributes share
/// it. The parameter names are therefore kept here, with the name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Signature {
    /// The name.
    pub name: String,
    /// What is named: `concept` for a concept.
    pub subject: String,
    /// The labels it is applied with.
    pub parameters: BTreeSet<String>,
}

impl Signature {
    /// The signature an application is written with: the name applied
    /// and the labels written. `None` where the operator is not a name.
    pub fn of(application: &Syntax, subject: &str) -> Option<Self> {
        match application {
            Syntax::Application(operator, operand) => match &**operator {
                Syntax::Reference(name) => Some(Signature {
                    name: name.clone(),
                    subject: subject.to_string(),
                    parameters: operand.terms.keys().cloned().collect(),
                }),
                _ => None,
            },
            _ => None,
        }
    }

    /// Whether an application written with `written` may be an
    /// application of this: the same name and kind, and no label this
    /// does not have. A read may leave labels out.
    pub fn takes(&self, written: &Signature) -> bool {
        self.name == written.name
            && self.subject == written.subject
            && written.parameters.is_subset(&self.parameters)
    }

    /// The signature as a value: a map.
    pub fn value(&self) -> Result<Value, Error> {
        data::record([
            ("name", data::text(&self.name)),
            ("subject", data::entity(&format!("{}:", self.subject))?),
            (
                "parameters",
                data::list(self.parameters.iter().map(data::text)),
            ),
        ])
    }

    /// The entity a definition is kept under: `name:` and the reference
    /// of the signature. A later definition under the same signature
    /// is kept under the same entity, and so replaces the earlier.
    pub fn entity(&self) -> Result<Entity, Error> {
        Entity::from_str(&format!(
            "name:{}",
            self.value()?.reference().as_bytes().to_base58()
        ))
        .map_err(|error| refused("not an entity", error))
    }
}

impl Form {
    /// What names what the form says: the reference of its
    /// [`Syntax`].
    pub fn reference(&self) -> Result<Reference, Error> {
        Syntax::try_from(self)?.reference()
    }

    /// The entity that names what the form says.
    pub fn entity(&self) -> Result<Entity, Error> {
        Syntax::try_from(self)?.entity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::read;

    fn one(text: &str) -> Syntax {
        Syntax::try_from(&read(text).unwrap()[0]).unwrap()
    }

    fn refusal(text: &str) -> Error {
        Syntax::try_from(&read(text).unwrap()[0]).unwrap_err()
    }

    fn keyword(name: &str) -> Value {
        data::entity(&format!("{name}:")).unwrap()
    }

    /// The maintainer's example, node for node, but for where the part
    /// with no label is kept: among the items, where several such parts
    /// have a place, and not under `of` among the terms.
    #[test]
    fn it_makes_the_tree_of_an_application() -> Result<(), Error> {
        let expected = data::record([
            ("type", keyword("application")),
            (
                "operator",
                data::record([
                    ("type", keyword("reference")),
                    ("name", data::symbol("todo")?),
                ])?,
            ),
            (
                "operand",
                data::record([
                    ("type", keyword("parameters")),
                    (
                        "items",
                        data::list([data::record([
                            ("type", keyword("variable")),
                            ("name", data::text("t")),
                        ])?]),
                    ),
                    (
                        "terms",
                        data::record([(
                            "state",
                            data::record([
                                ("type", keyword("constant")),
                                ("value", keyword("open")),
                            ])?,
                        )])?,
                    ),
                ])?,
            ),
        ])?;
        assert_eq!(one("(todo ?t state: :open)").value()?, expected);
        Ok(())
    }

    /// Every form the prelude and the examples are written in has a
    /// tree, the tree is data, and the data reads back as the tree.
    #[test]
    fn it_keeps_every_form_of_the_prelude_and_the_examples() -> Result<(), Error> {
        let mut count = 0;
        for text in [
            include_str!("prelude.clj"),
            include_str!("../examples/todo.clj"),
            include_str!("../examples/done.clj"),
        ] {
            for form in read(text)? {
                let syntax = Syntax::try_from(&form)?;
                let bytes = syntax.encode()?;
                let back = Syntax::decode(&bytes)?;
                assert_eq!(back.encode()?, bytes, "{form}");
                assert_eq!(back.reference()?, syntax.reference()?, "{form}");
                // What it prints as says the same thing.
                let printed = read(&back.to_string())?.remove(0);
                assert_eq!(printed.reference()?, syntax.reference()?, "{form}");
                count += 1;
            }
        }
        assert!(count > 20);
        Ok(())
    }

    /// The tree is of what a form says, not of how it was written.
    #[test]
    fn it_names_a_form_by_what_it_says() -> Result<(), Error> {
        let reference = |text: &str| one(text).reference();
        let form = reference("(xyz.todo ?t list: ?l text: ?x)")?;
        assert_eq!(form, reference("(xyz.todo ?t text: ?x list: ?l)")?);
        assert_eq!(form, reference("(xyz.todo text: ?x ?t list: ?l)")?);
        assert_eq!(form, reference("(xyz.todo text: ?x of: ?t list: ?l)")?);
        assert_ne!(form, reference("(xyz.todo ?t list: ?l text: ?y)")?);
        assert_eq!(reference("{a: 1 b: 2}")?, reference("{b: 2 a: 1}")?);
        assert_eq!(reference("(f 1.50)")?, reference("(f 1.5)")?);
        // A parenthesis applies and a bracket does not.
        assert_ne!(reference("(f a b)")?, reference("[f a b]")?);
        assert!(one("(f)").entity()?.as_str().starts_with("form:"));
        Ok(())
    }

    /// One call written two ways is one tree. One part with no label or
    /// several, the shape is one.
    #[test]
    fn it_reads_of_as_the_part_with_no_label() -> Result<(), Error> {
        let bare = one("(todo ?t state: :open)");
        let labelled = one("(todo state: :open of: ?t)");
        assert_eq!(bare, labelled);
        assert_eq!(bare.reference()?, labelled.reference()?);
        assert_eq!(labelled.to_string(), "(todo ?t state: :open)");
        // Beside a part with no label, `of:` is a label like any other.
        let Syntax::Application(_, operand) = one("(fam.parent ?a of: ?b)") else {
            panic!("an application");
        };
        assert_eq!(operand.items, vec![Syntax::Variable("a".into())]);
        assert_eq!(operand.terms.get("of"), Some(&Syntax::Variable("b".into())));

        for text in ["(a ?x)", "(a ?x) (b ?x)"] {
            let Syntax::Application(_, operand) = one(text) else {
                panic!("an application");
            };
            assert!(operand.terms.is_empty());
            assert!(!operand.items.is_empty());
        }
        Ok(())
    }

    /// A struct is a value like 5: as the one part with no label it is
    /// an item, and what it holds are its entries and not the terms of
    /// what it is passed to.
    #[test]
    fn it_passes_a_struct_as_a_part() -> Result<(), Error> {
        let Syntax::Application(_, operand) = one("(expand {note: ?n})") else {
            panic!("an application");
        };
        assert!(operand.terms.is_empty());
        assert_eq!(
            operand.items,
            vec![Syntax::Map(vec![(
                Syntax::Reference("note".into()),
                Syntax::Variable("n".into())
            )])]
        );
        assert_ne!(
            one("(expand {note: ?n})").reference()?,
            one("(expand note: ?n)").reference()?
        );
        Ok(())
    }

    #[test]
    fn it_reads_what_a_pattern_adds() -> Result<(), Error> {
        let Syntax::Application(operator, operand) =
            one("(?name ?this ?more ... ...?fields label: _)")
        else {
            panic!("an application");
        };
        assert_eq!(*operator, Syntax::Variable("name".into()));
        assert_eq!(
            operand.items,
            vec![
                Syntax::Variable("this".into()),
                Syntax::Repeat(Box::new(Syntax::Variable("more".into())))
            ]
        );
        assert_eq!(operand.rest.as_deref(), Some("fields"));
        assert_eq!(operand.terms.get("label"), Some(&Syntax::Blank));
        assert_eq!(
            one("(?name ?this ?more ... ...?fields label: _)").to_string(),
            "(?name ?this ?more ... label: _ ...?fields)"
        );
        Ok(())
    }

    #[test]
    fn it_tells_a_value_from_a_name() {
        let Syntax::Tuple(items) =
            one(r#"["text" 42 -3 1.5 true :open todo:1 todo xyz.todo/text +]"#)
        else {
            panic!("a tuple");
        };
        let constants = items
            .iter()
            .filter(|item| matches!(item, Syntax::Constant(_)))
            .count();
        assert_eq!(constants, 7);
        assert_eq!(
            items[7..],
            [
                Syntax::Reference("todo".into()),
                Syntax::Reference("xyz.todo/text".into()),
                Syntax::Reference("+".into())
            ]
        );
    }

    #[test]
    fn it_refuses_a_label_twice_and_a_repeat_of_nothing() {
        assert_eq!(
            refusal("(f a: 1 a: 2)"),
            Error::Refused("(f a: 1 a: 2) says `a:` twice".into())
        );
        assert_eq!(
            refusal("(f ...)"),
            Error::Refused("a `...` that repeats nothing".into())
        );
        assert_eq!(
            refusal("(f ?a ... ...)"),
            Error::Refused("a `...` that repeats nothing".into())
        );
        assert!(matches!(refusal("[...?rest]"), Error::Refused(_)));
        assert!(matches!(refusal("(f ...?a ...?b)"), Error::Refused(_)));
    }

    #[test]
    fn it_refuses_a_key_twice_where_the_tree_becomes_data() {
        assert!(matches!(one("{a: 1 a: 2}").value(), Err(Error::Refused(_))));
    }

    /// Two definitions over the same attributes, with other names for
    /// the fields: what each calls its fields is in its signature, and
    /// each is kept under an entity of its own.
    #[test]
    fn it_keeps_the_parameter_names_with_the_name() -> Result<(), Error> {
        let signature = |text: &str| Signature::of(&one(text), "concept").unwrap();
        let todo = signature("(todo ?t title: ?x author: ?a)");
        let task = signature("(todo ?t name: ?x issuer: ?a)");
        assert_eq!(
            todo.parameters.iter().collect::<Vec<_>>(),
            vec!["author", "title"]
        );
        assert_ne!(todo.entity()?, task.entity()?);
        // The order the labels were written in does not make another.
        assert_eq!(
            todo.entity()?,
            signature("(todo author: ?a of: ?t title: ?x)").entity()?
        );
        // Nor does what is named being of another kind share an entity.
        assert_ne!(
            todo.entity()?,
            Signature::of(&one("(todo ?t title: ?x author: ?a)"), "command")
                .unwrap()
                .entity()?
        );

        // A read may leave a label out; no application may add one.
        assert!(todo.takes(&signature("(todo ?t title: ?x)")));
        assert!(todo.takes(&signature("(todo ?t)")));
        assert!(!todo.takes(&signature("(todo ?t name: ?x)")));
        assert!(!todo.takes(&signature("(task ?t title: ?x)")));
        assert_eq!(Signature::of(&one("(?name ?t)"), "concept"), None);
        Ok(())
    }

    /// A name does not move with how a tree is kept: these are the names
    /// the trees had under the layout the notation had of its own,
    /// before it kept them as composite values.
    #[test]
    fn it_names_a_tree_apart_from_how_it_is_kept() -> Result<(), Error> {
        for (text, name) in [
            (
                "(todo ?t state: :open)",
                "form:6zdaZQpk7kebcAVHhZ7D2aHfm8qH2YH8DSpMFj3N8wzU",
            ),
            (
                r#"{a: 1 b: [2.0 "x" -3 ?y ...]}"#,
                "form:ANpSivMCf1y68JwcKpfx6hPmhpm5EkLJWcT67DduHD6k",
            ),
            (
                "(rule (xyz.todo ?t done: true) (xyz.todo ?t state: :done) (unless (f 1.5 x/y +)))",
                "form:DUVe6jwPbwVjGQu4D26HERqULgLWyxA8H5DN88bXZ9Px",
            ),
        ] {
            assert_eq!(one(text).entity()?.as_str(), name, "{text}");
        }
        assert_eq!(
            Signature::of(&one("(todo ?t title: ?x author: ?a)"), "concept")
                .unwrap()
                .entity()?
                .as_str(),
            "name:BDRfo9w4GFhULUXvQ921yhcqpwvQfKUgYTDZJM9gsx8q"
        );
        Ok(())
    }

    /// A tree is kept as a composite value, so it is read back from the
    /// value a query gives, and one number is kept one way: `2.0` reads
    /// back as `2`.
    #[test]
    fn it_keeps_a_tree_as_a_composite_value() -> Result<(), Error> {
        let tree = one(r#"(f ?x 2.0 {a: [1 "b"]} label: _ ...?rest)"#);
        let kept = tree.value()?;
        assert!(matches!(
            kept.composite(),
            Ok(Some(dialog_corpus::Composite::Map(_)))
        ));
        let back = Syntax::try_from(&kept)?;
        assert_eq!(
            back.to_string(),
            r#"(f ?x 2 {a: [1 "b"]} label: _ ...?rest)"#
        );
        assert_eq!(back.value()?, kept);
        assert_eq!(back.reference()?, tree.reference()?);
        Ok(())
    }

    #[test]
    fn it_refuses_data_that_is_no_node() -> Result<(), Error> {
        assert_eq!(
            Syntax::try_from(&data::text("todo")),
            Err(Error::Read("a node with no type".into()))
        );
        assert_eq!(
            Syntax::try_from(&data::list([keyword("verb")])),
            Err(Error::Read("a node with no type".into()))
        );
        assert_eq!(
            Syntax::try_from(&data::record([("type", keyword("verb"))])?),
            Err(Error::Read("a node of no type this reads: verb".into()))
        );
        assert_eq!(
            Syntax::try_from(&data::record([("type", keyword("variable"))])?),
            Err(Error::Read("a node with no name".into()))
        );
        Ok(())
    }
}
