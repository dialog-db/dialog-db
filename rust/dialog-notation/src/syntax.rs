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
//! A tree read from what an author wrote makes a value and reads back
//! from it, and prints as text that reads back as it. A tree the
//! analyzer gives does not: it names what the notation names itself,
//! `dialog.n.select`, `dialog.n.query.row` and a derived attribute as
//! `dialog.n.c/list/count-open`, which no document may write, so reading
//! it back from text or a value refuses it as `Error::Reserved`. Such a
//! tree is kept as it is, or its document is analyzed again.
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
use dialog_corpus::{Composite, Entity, Value};
use merkle_reference::Reference;

use crate::Error;
use crate::data;
use crate::form::{Form, Kind, Mark, Part};
use crate::written::{Note, Path, Segment, Slot, Written};

/// A name under the notation's namespace, `namespaced!("report.q")`,
/// or the namespace itself, `namespaced!()`: every family is spelled
/// from the one namespace here.
macro_rules! namespaced {
    () => {
        "dialog.n"
    };
    ($rest:literal) => {
        concat!("dialog.n", ".", $rest)
    };
}
pub(crate) use namespaced;

/// Everything the notation names for itself is under this namespace,
/// which a document cannot name: the relations it derives, reports and
/// lays out views in. The engine keeps `dialog.` for its own and refuses
/// a document's writes under it; this part is the notation's. Names are
/// compared with it by whole segment, never by a plain string prefix:
/// the engine has `dialog.node.*`, which shares the letters and no
/// segment.
pub const NAMESPACE: &str = namespaced!();

/// The prefix of every derived attribute's domain: `(list/count-open ?l
/// ?n)` derives and reads `dialog.n.c.<hash>/is`, the hash [`derived`]
/// makes of `list` and `count-open`. It is under `dialog.`, where the
/// engine refuses every write but a rule's, and `c` is no other family
/// of the notation's names. A concept's domain is concluded only.
pub const DERIVED: &str = namespaced!("c");

/// The bytes of a reference a derived attribute's domain takes: 128 bits.
const DERIVED_BYTES: usize = 16;

/// How many bytes a derived attribute's domain has: the prefix, a dot,
/// and 128 bits in base 32, 26 characters.
pub const DERIVED_LENGTH: usize = DERIVED.len() + 1 + (DERIVED_BYTES * 8).div_ceil(5);
/// The domain of the attribute `name` derived under the concept
/// `concept`, as each is written: the prefix and 128 bits of the
/// reference of the list of the two, in lowercase base 32. A list of two
/// texts is one value for one pair, so no other concept and attribute
/// make it, whatever either holds, dots included; and it is one length,
/// 37 bytes, so a label has 26 of the 64 the engine allows a relation.
/// A function of the two alone: the same in every process, and nothing
/// is kept.
pub fn derived(concept: &str, name: &str) -> Result<String, Error> {
    hashed(concept, name)
}

/// The prefix of a derived attribute as the analyzer names it in the
/// trees it gives, `dialog.n.c/list/count-open`: by what was written,
/// under a name no document can write.
pub const DERIVED_READ: &str = concat!(namespaced!("c"), "/");

/// A derived attribute as the analyzer names it: `dialog.n.c/` and the
/// concept and the field as written, `dialog.n.c/list/count-open`.
pub fn derived_read(concept: &str, field: &str) -> String {
    format!("{DERIVED_READ}{concept}/{field}")
}

/// The concept and the field a derived attribute's name as the analyzer
/// writes it holds, split at its last `/`: `dialog.n.c/nb/blocks/count`
/// is `nb/blocks` and `count`. None where the name is no such one: the
/// concept a declarable name, the field a label of one, neither empty.
pub fn derived_parts(name: &str) -> Option<(&str, &str)> {
    let (concept, field) = name.strip_prefix(DERIVED_READ)?.rsplit_once('/')?;
    let label =
        |part: &str| !part.is_empty() && !part.contains('/') && named(part) == Named::Declarable;
    (!concept.is_empty()
        && !concept.starts_with('/')
        && !concept.ends_with('/')
        && !concept.contains("//")
        && named(concept) == Named::Declarable
        && label(field))
    .then_some((concept, field))
}

/// Each derived attribute `tree` names as the analyzer writes them, by
/// the domain Dialog-DB knows it by: what a refusal or an answer about
/// what was compiled of `tree` is spelled by. A table of the tree's own,
/// so nothing is kept between trees.
pub fn derived_names(tree: &Syntax) -> BTreeMap<String, String> {
    let mut names = BTreeMap::new();
    let mut note = |name: &str| {
        let relation = match derived_parts(name) {
            Some(_) => name,
            None => match name.rsplit_once('/') {
                Some((relation, _)) if derived_parts(relation).is_some() => relation,
                _ => return,
            },
        };
        if let Some((concept, field)) = derived_parts(relation)
            && let Ok(domain) = derived(concept, field)
        {
            names.insert(domain, format!("{concept}/{field}"));
        }
    };
    let mut open = vec![tree];
    while let Some(part) = open.pop() {
        match part {
            Syntax::Reference(name) => note(name),
            Syntax::Application(operator, operand) => {
                operand.terms.keys().for_each(|label| note(label));
                open.extend(operand.terms.values());
                open.extend(operand.items.iter());
                open.push(operator);
            }
            Syntax::Map(entries) => entries.iter().for_each(|(key, value)| {
                open.push(key);
                open.push(value);
            }),
            Syntax::Tuple(items) => open.extend(items.iter()),
            _ => {}
        }
    }
    names
}

/// `text` as an author is told it, by [`told_with`] with no derived
/// attribute's domain known: what the analyzer names is spelled, and
/// variables are untagged.
pub fn told(text: &str) -> String {
    told_with(text, &BTreeMap::new())
}

/// `text` as an author is told it, in one pass: each derived attribute's
/// domain `names` holds written as the name it was made of,
/// `list/count-open` for its `/is` and `list/count status:` for a label;
/// a derived attribute as the analyzer names it, `dialog.n.c/list/label`,
/// as `list/label`; each variable the notation tagged as the author wrote
/// it, `?t[0]` and `?[q1]t` as `?t`, and one it made for none of theirs,
/// `?[2]`, as `_`. Every error an embedder is given passes through this.
pub fn told_with(text: &str, names: &BTreeMap<String, String>) -> String {
    let mut said = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        let derived = rest.find(DERIVED);
        let variable = rest.find('?');
        match (derived, variable) {
            (Some(at), variable) if variable.is_none_or(|variable| at < variable) => {
                said.push_str(&rest[..at]);
                let tail = &rest[at..];
                if let Some(written) = tail.strip_prefix(DERIVED_READ) {
                    let end = written
                        .find(|c: char| !(c.is_alphanumeric() || "-_/*+!<>=".contains(c)))
                        .unwrap_or(written.len());
                    said.push_str(&written[..end]);
                    rest = &written[end..];
                    continue;
                }
                let end = tail
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '.'))
                    .unwrap_or(tail.len());
                let domain = &tail[..end];
                match names.get(domain) {
                    Some(written) => {
                        let after = &tail[end..];
                        match after.strip_prefix('/') {
                            Some(after) => {
                                let label_end = after
                                    .find(|c: char| !(c.is_alphanumeric() || c == '-'))
                                    .unwrap_or(after.len());
                                match &after[..label_end] {
                                    "is" => said.push_str(written),
                                    label => {
                                        said.push_str(written);
                                        said.push(' ');
                                        said.push_str(label);
                                        said.push(':');
                                    }
                                }
                                rest = &after[label_end..];
                            }
                            None => {
                                said.push_str(written);
                                rest = after;
                            }
                        }
                    }
                    None => {
                        said.push_str(domain);
                        rest = &tail[end..];
                    }
                }
            }
            (_, Some(at)) => {
                said.push_str(&rest[..at]);
                let tail = &rest[at + 1..];
                let name_end = tail
                    .find(|c: char| !(c.is_alphanumeric() || "-_./*+!<>=".contains(c)))
                    .unwrap_or(tail.len());
                let (name, after) = tail.split_at(name_end);
                let tag = after
                    .strip_prefix('[')
                    .and_then(|inside| inside.find(']').map(|close| close + 2));
                match tag {
                    // Tagged in front, `?[q1]x`: the author's name follows
                    // the tag, or there is none, `?[2]`.
                    Some(length) if name.is_empty() => {
                        let after = &after[length..];
                        let end = after
                            .find(|c: char| !(c.is_alphanumeric() || "-_./*+!<>=".contains(c)))
                            .unwrap_or(after.len());
                        match &after[..end] {
                            "" => said.push('_'),
                            name => {
                                said.push('?');
                                said.push_str(name);
                            }
                        }
                        rest = &after[end..];
                    }
                    Some(length) => {
                        said.push('?');
                        said.push_str(name);
                        rest = &after[length..];
                    }
                    None => {
                        said.push('?');
                        said.push_str(name);
                        rest = after;
                    }
                }
            }
            _ => {
                said.push_str(rest);
                return said;
            }
        }
    }
}

/// The hash [`derived`] names the domain by.
fn hashed(concept: &str, name: &str) -> Result<String, Error> {
    let framed = data::list([data::text(concept), data::text(name)])?;
    let reference = framed.reference();
    let bytes = reference.as_bytes();
    let mut hash = String::with_capacity(DERIVED_LENGTH);
    hash.push_str(DERIVED);
    hash.push('.');
    // Base 32, lowercase, unpadded: five bits a character.
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut held: u32 = 0;
    let mut bits = 0;
    for byte in bytes.iter().take(DERIVED_BYTES) {
        held = (held << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            hash.push(char::from(ALPHABET[((held >> bits) & 31) as usize]));
        }
    }
    if bits > 0 {
        hash.push(char::from(ALPHABET[((held << (5 - bits)) & 31) as usize]));
    }
    Ok(hash)
}

/// Whether a variable is one the notation made: a template's own, at a
/// site, `x[3]`, a column, a fold's, a view's. Its name holds a `[`,
/// which the reader never reads in a variable, so no document can name
/// it.
///
/// Each name the notation makes is one name for one input:
///
/// | name | made of | why two inputs never make one name |
/// |---|---|---|
/// | `x[3]`, a variable at a site | a variable and the site's number | the suffix is `[`, digits, `]`; the last `[` of the name is the suffix's, as digits hold none, so the name splits into one variable and one number |
/// | `[q1]x`, a variable of a view's layer | the layer's number and a variable | the prefix ends at the first `]`, as digits hold none |
/// | `[collect-0]`, `[collect-0-key]`, `[collect-0-value]` | a number | no label or name of a document is in them |
/// | `[source]label`, `[source-key]` | a label, or nothing | a field's has `]` after `[source` and the key's has `-`, and `[source]` before a label is a fixed prefix |
/// | `[view-of]`, `[query-row]`, `[groups]`, `[inputs]` | nothing | constants, each spelled apart from the others and from the prefixes above |
/// | `dialog.n.report.q12` | a site's number | digits only |
/// | `c0`, `c1`, `row`, fields of a report | a column's number | the fields of the notation's own relations, never a document's |
/// | `dialog.n.c.<hash>` | a concept's name and an attribute's, as written | 128 bits of the reference of the list of the two: one value for one pair, so a collision is a hash's, not a spelling's |
/// | `<label>/key`, an entry's key in a rule's head | a field's label | a head with a field named so beside the entry is refused |
pub fn unwritten(name: &str) -> bool {
    name.contains('[')
}

/// What a name names, told from its spelling alone: the one place the
/// notation tells a stored name from a declared one and either from its
/// own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Named {
    /// Under the notation's namespace, `dialog.n` and `dialog.n.*`, or
    /// an attribute of such a domain: what only the notation writes.
    Reserved,
    /// Only operator characters: `+`, `<=`.
    Operator,
    /// A URI: `list:home`.
    Entity,
    /// A domain, where facts are stored: a name with a `.` and no `/`,
    /// `xyz.todo`.
    Domain,
    /// An attribute of a domain: a name with a `/` and a `.`,
    /// `xyz.todo/text`.
    Attribute,
    /// A name a document may declare, a concept's, a query's, a command's
    /// or a derived attribute's: no `.`, `todo`, `notebook/blocks`.
    Declarable,
    /// A derived attribute as the analyzer names it in the trees it
    /// gives, `dialog.n.c/list/count-open`, its parts well formed
    /// ([`derived_parts`]). A document never writes one.
    Derived,
}

/// What `name` names. See [`Named`].
pub fn named(name: &str) -> Named {
    if derived_parts(name).is_some() {
        return Named::Derived;
    }
    let relation = name.split_once('/').map_or(name, |(relation, _)| relation);
    if relation
        .strip_prefix(NAMESPACE)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
    {
        return Named::Reserved;
    }
    match Kind::of(name) {
        Kind::Operator => Named::Operator,
        Kind::Entity => Named::Entity,
        _ if !name.contains('.') => Named::Declarable,
        _ if name.contains('/') => Named::Attribute,
        _ => Named::Domain,
    }
}

/// Whether `name` may be declared: the name of a concept, a query or a
/// command, `todo` or `notebook/blocks`. One with a `.` anywhere is a
/// domain, `xyz.todo`, or an attribute in one, `xyz.todo/text`, read
/// directly; that is how the two are told apart (the maintainer,
/// 2026-11-24). Every place that decides whether a name is declared or
/// stored asks this.
pub fn declarable(name: &str) -> bool {
    !name.contains('.') && matches!(Kind::of(name), Kind::Name | Kind::Attribute)
}

/// The refusal of `form`, which declares `name` as `what` though it
/// cannot be declared.
pub fn undeclarable(form: &dyn std::fmt::Display, what: &str, name: &str) -> Error {
    Error::Refused(format!(
        "{form} declares a {what} named `{name}`, which is a stored name: a name with a `.` is a \
         domain or an attribute in one, read directly, and the name of a concept, a query or a \
         command has none, `todo` or `notebook/blocks`; that is how the two are told apart"
    ))
}

/// The variable the notation names `name` with `tag`: `x[3]`.
pub fn tagged(name: &str, tag: &str) -> String {
    format!("{name}[{tag}]")
}

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
}

/// What an operator is applied to.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Parameters {
    /// The parts written with no label, in the order written.
    pub items: Vec<Syntax>,
    /// The labelled parts. The order they were written in is not kept.
    pub terms: BTreeMap<String, Syntax>,
}

fn refused(what: &str, error: impl Display) -> Error {
    Error::Refused(format!("{what}: {error}"))
}

/// A number as written, as the value it is: a natural where it can be,
/// then an integer, then a float.
pub(crate) fn number(text: &str) -> Result<Value, Error> {
    // A whole number is a natural where it is not below zero, `-0`
    // among them, and an integer below it; one past 64 bits is held by
    // neither, and is refused rather than read as a float.
    let whole = !text.contains(['.', 'e', 'E']);
    if let Ok(natural) = text.parse::<u64>() {
        Ok(Value::UnsignedInt(natural.into()))
    } else if let Ok(integer) = text.parse::<i64>() {
        Ok(match u64::try_from(integer) {
            Ok(natural) => Value::UnsignedInt(natural.into()),
            Err(_) => Value::SignedInt(integer.into()),
        })
    } else if whole {
        Err(Error::Refused(format!(
            "{text} is a whole number past what 64 bits hold"
        )))
    } else {
        text.parse::<f64>()
            .ok()
            .filter(|float| float.is_finite())
            .map(Value::Float)
            .ok_or_else(|| Error::Refused(format!("{text} is not a number a float holds")))
    }
}

pub(crate) fn entity(uri: &str) -> Result<Value, Error> {
    Entity::from_str(uri)
        .map(Value::Entity)
        .map_err(|error| refused("not an entity", error))
}

/// The nodes a run of forms is, each in its place: the first is the
/// item `from`.
fn run<'a>(
    forms: impl IntoIterator<Item = (&'a Form, Option<&'a Mark>)>,
    path: &mut Path,
    written: &mut Written,
    from: usize,
) -> Result<Vec<Syntax>, Error> {
    let mut nodes: Vec<Syntax> = Vec::new();
    for (form, mark) in forms {
        path.push(Segment::Item(from + nodes.len()));
        let node = tree(form, mark, false, path, written);
        path.pop();
        nodes.push(node?);
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
        } else if mark.implied {
            // What the reader put there is written as nothing.
            note.spelling = Some(String::new());
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
        Form::Vector(items) => Syntax::Tuple(run(
            items
                .iter()
                .enumerate()
                .map(|(at, item)| (item, inside(at))),
            path,
            written,
            0,
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
            // Whether `of:` has been written: a second is a label twice.
            let mut of_said = false;
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
                    Part::Bare(part) => {
                        // Where `of:` is written out it is the first
                        // item, and a part with no label beside it,
                        // `...`, comes after it.
                        order.push(Slot::Item {
                            at: bare.len() + usize::from(primary),
                            of: None,
                        });
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
                            if of_said {
                                return Err(Error::Refused(format!("{form} says `of:` twice")));
                            }
                            of_said = true;
                            operand.items.push(term);
                        } else if operand.terms.insert(label.clone(), term).is_some() {
                            return Err(Error::Refused(format!("{form} says `{label}:` twice")));
                        }
                    }
                }
            }
            if !bare.is_empty() {
                let from = operand.items.len();
                operand.items.extend(run(bare, path, written, from)?);
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

    /// The tree `form` says. One a caller built that names a variable the
    /// notation makes, or a name under its namespace, is refused as
    /// `Reserved`, as no text read can say it.
    fn try_from(form: &Form) -> Result<Self, Error> {
        let tree = tree(form, None, false, &mut Path::new(), &mut Written::default())?;
        match crate::expand::reserved(&tree) {
            Some((_, name)) => Err(Error::Reserved(name)),
            None => Ok(tree),
        }
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
    data::list(
        nodes
            .iter()
            .map(Syntax::value)
            .collect::<Result<Vec<_>, _>>()?,
    )
}

impl Parameters {
    fn value(&self) -> Result<Value, Error> {
        let terms = data::record(
            self.terms
                .iter()
                .map(|(label, term)| Ok((label.as_str(), term.value()?)))
                .collect::<Result<Vec<_>, Error>>()?,
        )?;
        node(
            "parameters",
            [("items", list(&self.items)?), ("terms", terms)],
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
    /// | `:parameters` | `items`, a list of nodes; `terms`, a map from label to node |
    /// | `:variable` | `name`, text |
    /// | `:blank` | |
    /// | `:constant` | `value` |
    /// | `:reference` | `name`, a symbol |
    /// | `:map` | `entries`, a map from node to node |
    /// | `:tuple` | `items`, a list of nodes |
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
                    // A key is a node, and a map's key is text, so the
                    // entries are pairs. They are kept in the order of
                    // their keys and not as written, since a struct
                    // says the same in any order, and a key twice is
                    // refused here as it would be in a map. A set's
                    // keys are all the empty label: it is kept in the
                    // order of its members, and a member twice is
                    // refused.
                    {
                        let set = crate::select::is_set(self);
                        let mut pairs = entries
                            .iter()
                            .map(|(key, value)| Ok((key.value()?, value.value()?)))
                            .collect::<Result<Vec<_>, Error>>()?;
                        pairs.sort_by(|(a, x), (b, y)| {
                            a.to_bytes()
                                .cmp(&b.to_bytes())
                                .then_with(|| x.to_bytes().cmp(&y.to_bytes()))
                        });
                        let same = |pair: &[(Value, Value)]| {
                            pair[0].0 == pair[1].0 && (!set || pair[0].1 == pair[1].1)
                        };
                        if let Some(pair) = pairs.windows(2).find(|pair| same(pair)) {
                            let (what, twice) = if set {
                                ("member", from_value(&pair[0].1)?)
                            } else {
                                ("key", from_value(&pair[0].0)?)
                            };
                            return Err(Error::Refused(format!(
                                "{self} says the {what} {twice} twice"
                            )));
                        }
                        data::list(
                            pairs
                                .into_iter()
                                .map(|(key, value)| data::list([key, value]))
                                .collect::<Result<Vec<_>, Error>>()?,
                        )?
                    },
                )],
            ),
            Syntax::Tuple(items) => node("tuple", [("items", list(items)?)]),
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

    /// What this applies its operator to, to change.
    pub fn operand_mut(&mut self) -> Option<&mut Parameters> {
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
            .is_some_and(|name| Kind::of(name) == Kind::Domain || named(name) == Named::Derived)
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
            Syntax::Blank | Syntax::Constant(_) | Syntax::Reference(_) => {}
        }
    }

    /// How deep the tree nests and how many parts it has, itself among
    /// them: what an expansion is bounded by. A leaf is 0 deep. It walks
    /// the tree without recursing, so a tree of any depth is measured.
    pub(crate) fn extent(&self) -> (usize, usize) {
        let mut deepest = 0;
        let mut parts = 0;
        let mut open = vec![(self, 0)];
        while let Some((tree, depth)) = open.pop() {
            parts += 1;
            deepest = deepest.max(depth);
            match tree {
                Syntax::Application(operator, operand) => {
                    open.push((operator, depth + 1));
                    open.extend(operand.items.iter().map(|item| (item, depth + 1)));
                    open.extend(operand.terms.values().map(|term| (term, depth + 1)));
                }
                Syntax::Map(entries) => {
                    for (key, value) in entries {
                        open.push((key, depth + 1));
                        open.push((value, depth + 1));
                    }
                }
                Syntax::Tuple(items) => open.extend(items.iter().map(|item| (item, depth + 1))),
                Syntax::Variable(_)
                | Syntax::Blank
                | Syntax::Constant(_)
                | Syntax::Reference(_) => {}
            }
        }
        (deepest, parts)
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

    /// The tree as bytes: the canonical bytes of the composite it is
    /// kept as.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        match self.value()? {
            Value::Composite(composite) => Ok(composite.to_bytes().into_owned()),
            other => Err(Error::Read(format!(
                "a node that is no composite: {other:?}"
            ))),
        }
    }

    /// The tree `bytes` hold.
    pub fn decode(bytes: &[u8]) -> Result<Syntax, Error> {
        let composite =
            Composite::from_bytes(bytes).map_err(|error| Error::Read(error.to_string()))?;
        Syntax::try_from(&Value::from(composite))
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
                format!(":{}", form.label().unwrap_or_default())
            }
            (_, form) => form.to_string(),
        }
    }

    /// The notation that says this. The parts with no label come first,
    /// then the labelled parts by label.
    pub fn form(&self) -> Form {
        match self {
            Syntax::Application(operator, operand) => {
                let mut parts: Vec<Part> = operand
                    .items
                    .iter()
                    .map(|item| Part::Bare(item.form()))
                    .collect();
                parts.extend(
                    operand
                        .terms
                        .iter()
                        .map(|(label, term)| Part::Labelled(label.clone(), term.form())),
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
                // A map or a list is written as the tree it is answered
                // as, which reads back as that tree.
                Value::Composite(_) => crate::compile::written(value).form(),
                // Bytes have no written form: they are shown as `…`, which
                // reads as a name and so as no value, and `printed`
                // refuses them.
                Value::Bytes(_) | Value::Record(_) => Form::Symbol(UNWRITTEN.into()),
            },
            Syntax::Reference(name) => Form::Symbol(name.clone()),
            Syntax::Map(entries) => Form::Map(
                entries
                    .iter()
                    .map(|(key, value)| (key.form(), value.form()))
                    .collect(),
            ),
            Syntax::Tuple(items) => Form::Vector(items.iter().map(Syntax::form).collect()),
        }
    }
}

/// What bytes are shown as where a tree that holds them is shown: a
/// name, so that it reads back as no value.
const UNWRITTEN: &str = "…";

/// Whether `value` has no written form: bytes, or a map or a list that
/// holds them.
fn unwritable(value: &Value) -> bool {
    match value {
        Value::Bytes(_) | Value::Record(_) => true,
        Value::Composite(composite) if composite.is_map() => composite
            .entries()
            .any(|(key, value)| unwritable(&key) || unwritable(&value)),
        Value::Composite(composite) => composite.items().any(|item| unwritable(&item)),
        _ => false,
    }
}

impl Syntax {
    /// The text that says this and reads back as it, or `Unsupported`
    /// where it holds a value the notation has no written form for,
    /// bytes. A map or a list reads back as the tree it is answered as.
    pub fn printed(&self) -> Result<String, Error> {
        fn check(tree: &Syntax) -> Result<(), Error> {
            match tree {
                Syntax::Constant(value) if unwritable(value) => Err(Error::Unsupported(
                    "bytes have no written form, so a tree that holds them is not printed".into(),
                )),
                Syntax::Application(operator, operand) => {
                    check(operator)?;
                    operand.items.iter().try_for_each(check)?;
                    operand.terms.values().try_for_each(check)
                }
                Syntax::Map(entries) => entries
                    .iter()
                    .try_for_each(|(key, value)| check(key).and_then(|()| check(value))),
                Syntax::Tuple(items) => items.iter().try_for_each(check),
                _ => Ok(()),
            }
        }
        check(self)?;
        Ok(self.to_string())
    }
}

/// The tree as a person reads it, for messages and for what a view
/// shows. It cannot fail, so a value with no written form, bytes, is
/// shown as `…`. For text that is read back, use [`Syntax::printed`],
/// which refuses such a value.
impl Display for Syntax {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.form())
    }
}

fn unread(what: &str) -> Error {
    Error::Read(format!("a node with no {what}"))
}

/// The node `value` is: a map that says its type.
fn fields(value: &Value) -> Result<data::Entries, Error> {
    data::entries(value)?.ok_or_else(|| unread("type"))
}

fn nodes(node: &data::Entries, field: &str) -> Result<Vec<Syntax>, Error> {
    data::field(node, field)
        .map(data::items)
        .transpose()?
        .flatten()
        .ok_or_else(|| unread(field))?
        .iter()
        .map(Syntax::try_from)
        .collect()
}

fn text(node: &data::Entries, field: &str) -> Result<String, Error> {
    match data::field(node, field) {
        Some(Value::String(text)) => Ok(text.clone()),
        _ => Err(unread(field)),
    }
}

/// The type a node says it is: the name of the entity under `type`.
fn kind(node: &data::Entries) -> Result<&str, Error> {
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
            .map(|(label, term)| Ok((label.to_string(), from_value(term)?)))
            .collect::<Result<_, Error>>()?;
        Ok(Parameters {
            items: nodes(&node, "items")?,
            terms,
        })
    }
}

impl TryFrom<&Value> for Syntax {
    type Error = Error;

    /// The tree `value` keeps. One that names a variable the notation
    /// makes, or a name under its namespace, is refused as `Reserved`:
    /// no text can say it, and a document never holds it.
    fn try_from(value: &Value) -> Result<Self, Error> {
        let tree = from_value(value)?;
        match crate::expand::reserved(&tree) {
            Some((_, name)) => Err(Error::Reserved(name)),
            None => Ok(tree),
        }
    }
}

/// The tree `value` keeps, as it keeps it.
fn from_value(value: &Value) -> Result<Syntax, Error> {
    {
        let node = fields(value)?;
        let part = |field: &str| data::field(&node, field).ok_or_else(|| unread(field));
        Ok(match kind(&node)? {
            "application" => Syntax::Application(
                Box::new(from_value(part("operator")?)?),
                Parameters::try_from(part("operand")?)?,
            ),
            "variable" => Syntax::Variable(text(&node, "name")?),
            "blank" => Syntax::Blank,
            "constant" => match part("value")? {
                Value::Bytes(_) | Value::Record(_) | Value::Composite(_) => {
                    return Err(Error::Unsupported(
                        "a constant that is bytes, a record or a composite: the notation writes none of them"
                            .into(),
                    ));
                }
                value => Syntax::Constant(value.clone()),
            },
            "reference" => {
                Syntax::Reference(data::spelled(part("name")?).ok_or_else(|| unread("name"))?)
            }
            "map" => Syntax::Map(
                data::items(part("entries")?)?
                    .ok_or_else(|| unread("entries"))?
                    .iter()
                    .map(|entry| match data::items(entry)?.as_deref() {
                        Some([key, value]) => Ok((from_value(key)?, from_value(value)?)),
                        _ => Err(unread("entries")),
                    })
                    .collect::<Result<_, Error>>()?,
            ),
            "tuple" => Syntax::Tuple(nodes(&node, "items")?),
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
                data::list(self.parameters.iter().map(data::text))?,
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

    /// A variable the notation tagged in front, `?[q1]x`, is the
    /// author's `?x`, as one tagged behind, `?x[1]`, is; one tagged with
    /// no name of the author's after it, `?[2]`, is `_`. A name of any
    /// letters is kept whole.
    #[test]
    fn it_untags_a_variable_tagged_in_front_or_behind() {
        assert_eq!(told("?[q1]x and ?[source]label"), "?x and ?label");
        assert_eq!(told("?x[1], ?[2] and ?[q3]"), "?x, _ and _");
        assert_eq!(told("?[q1]été and ?名前[2]"), "?été and ?名前");
    }

    /// A text is told in one pass: each derived attribute named as it is
    /// written, under its label or as its value, and each tagged variable
    /// untagged, wherever they stand and however many there are.
    #[test]
    fn it_tells_a_text_in_the_authors_terms_in_one_pass() {
        let open = derived("list", "count-open").unwrap();
        let count = derived("list", "count").unwrap();
        // A tree as the analyzer gives it, which no reader reads.
        let read = |relation: &str| {
            Syntax::Application(
                Box::new(Syntax::Reference(relation.into())),
                Parameters::default(),
            )
        };
        let names = derived_names(&Syntax::Tuple(vec![
            read("dialog.n.c/list/count-open"),
            read("dialog.n.c/list/count"),
        ]));
        assert_eq!(
            told_with(
                &format!("{open}/is and {count}/status beside ?t[0], ?[2] and {open}"),
                &names
            ),
            "list/count-open and list/count status: beside ?t, _ and list/count-open"
        );
        // A hash no table holds is shown as it is; a derived attribute as
        // the analyzer names it is spelled with none.
        assert_eq!(told(&format!("{open}/is")), format!("{open}/is"));
        assert_eq!(told("(dialog.n.c/list/label ?l)"), "(list/label ?l)");
    }

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
                        ])?])?,
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

    /// Every form the prelude and the to-do documents are written in has a
    /// tree, the tree is data, and the data reads back as the tree.
    #[test]
    fn it_keeps_every_form_of_the_prelude_and_the_to_do_documents() -> Result<(), Error> {
        let mut count = 0;
        for text in [
            include_str!("prelude.clj"),
            include_str!("../tests/fixtures/todo.clj"),
            include_str!("../tests/fixtures/done.clj"),
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
        assert_eq!(reference("{:a 1 :b 2}")?, reference("{:b 2 :a 1}")?);
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
        let Syntax::Application(_, operand) = one("(expand {:note ?n})") else {
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
            one("(expand {:note ?n})").reference()?,
            one("(expand note: ?n)").reference()?
        );
        Ok(())
    }

    /// The tree gives `...` no meaning either: it is a name among the
    /// parts, and what it is followed by is the part after it. The
    /// analyzer reads the two as the parts not named.
    #[test]
    fn it_keeps_three_dots_as_a_name_among_the_parts() -> Result<(), Error> {
        let Syntax::Application(operator, operand) = one("(?name ?this label: _ ... ?fields)")
        else {
            panic!("an application");
        };
        assert_eq!(*operator, Syntax::Variable("name".into()));
        assert_eq!(
            operand.items,
            vec![
                Syntax::Variable("this".into()),
                Syntax::Reference("...".into()),
                Syntax::Variable("fields".into())
            ]
        );
        assert_eq!(operand.terms.get("label"), Some(&Syntax::Blank));
        assert_eq!(
            one("(?name ?this label: _ ... ?fields)").to_string(),
            "(?name ?this ... ?fields label: _)"
        );
        // With nothing after it, it is the last part.
        assert_eq!(
            one("(todo ?t list: ?l ...)").to_string(),
            "(todo ?t ... list: ?l)"
        );
        // In a struct it is a key, and what follows it the value.
        assert_eq!(
            one("{:author ?who ... ?others}"),
            Syntax::Map(vec![
                (
                    Syntax::Reference("author".into()),
                    Syntax::Variable("who".into())
                ),
                (
                    Syntax::Reference("...".into()),
                    Syntax::Variable("others".into())
                ),
            ])
        );
        let kept = one("{:author ?who ... ?others}");
        assert_eq!(
            Syntax::try_from(&kept.value()?)?.reference()?,
            kept.reference()?
        );
        Ok(())
    }

    /// `'x` is `(quote x)`: an application like any other in the tree.
    #[test]
    fn it_reads_a_quote_as_an_application_of_quote() {
        assert_eq!(one("'(1 2 3)"), one("(quote (1 2 3))"));
        assert_eq!(one("'(1 2 3)").applies(), Some("quote"));
        assert_eq!(one("{:tags '(1 (2 3))}").to_string(), "{:tags '(1 (2 3))}");
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

    /// A label twice is refused, `of:` as any other: it would otherwise
    /// be read as two parts with no label.
    #[test]
    fn it_refuses_a_label_twice() {
        assert_eq!(
            refusal("(f a: 1 a: 2)"),
            Error::Refused("(f a: 1 a: 2) says `a:` twice".into())
        );
        assert_eq!(
            refusal("(f of: 1 of: 2)"),
            Error::Refused("(f of: 1 of: 2) says `of:` twice".into())
        );
    }

    #[test]
    fn it_refuses_a_key_twice_where_the_tree_becomes_data() {
        assert!(matches!(one("{:a 1 :a 2}").value(), Err(Error::Refused(_))));
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

    /// A name does not move with how a tree's bytes are laid out. It
    /// is the name of the tree as data, so it moved when a record's key
    /// became text and a struct's entries pairs, and again when the tree
    /// came to be kept as Dialog-DB's composite value: these are the
    /// names since.
    #[test]
    fn it_names_a_tree_apart_from_how_it_is_kept() -> Result<(), Error> {
        for (text, name) in [
            (
                "(todo ?t state: :open)",
                "form:9Un5DfjUwfoT164kEXGCky9jesHX4ouitmuzYgWBgyWu",
            ),
            (
                "(rule (xyz.todo ?t done: true) (xyz.todo ?t state: :done) (unless (f 1.5 x/y +)))",
                "form:2q6BQJToSvhaNUnDLcRB3NoXvzd7aWLZkgCXkGdVruXn",
            ),
        ] {
            assert_eq!(one(text).entity()?.as_str(), name, "{text}");
        }
        assert_eq!(
            Signature::of(&one("(todo ?t title: ?x author: ?a)"), "concept")
                .unwrap()
                .entity()?
                .as_str(),
            "name:9swvZUPk8zcaUij6tHQUv8qUUbCzUFKUARy1v7UXMQd3"
        );
        Ok(())
    }

    /// A tree is kept as a composite value, so it is read back from the
    /// value a query gives, and one number is kept one way: `2.0` reads
    /// back as `2`.
    #[test]
    fn it_keeps_a_tree_as_a_composite_value() -> Result<(), Error> {
        let tree = one(r#"(f ?x 2.0 {:a [1 "b"]} label: _ ... ?rest)"#);
        let kept = tree.value()?;
        assert!(matches!(&kept, Value::Composite(composite) if composite.is_map()));
        let back = Syntax::try_from(&kept)?;
        assert_eq!(
            back.to_string(),
            r#"(f ?x 2 {:a [1 "b"]} ... ?rest label: _)"#
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
            Syntax::try_from(&data::list([keyword("verb")])?),
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

    /// A form made up from `next`, `depth` levels at most: every atom
    /// the reader takes, at its limits, and forms nested and empty.
    fn made(next: &mut impl FnMut() -> u64, depth: usize) -> String {
        const ATOMS: [&str; 25] = [
            "0",
            "-0",
            "18446744073709551615",
            "-9223372036854775808",
            "9223372036854775807",
            "0.0",
            "-0.0",
            "1.5",
            "100000000000000000000.0",
            "0.0000001",
            "1e20",
            "2.5E-3",
            "179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.0",
            r#""""#,
            r#""a \"quoted\" \\ text""#,
            "\"two\nlines\tand é\"",
            ":done",
            "todo:1",
            "xyz.todo/title",
            "?x",
            "?a.b",
            "_",
            "true",
            "false",
            "'quoted",
        ];
        let pick = next() % 10;
        if depth == 0 || pick < 5 {
            return ATOMS[(next() % ATOMS.len() as u64) as usize].to_string();
        }
        let count = (next() % 4) as usize;
        let parts: Vec<String> = (0..count).map(|_| made(next, depth - 1)).collect();
        match pick {
            5 | 6 => format!("[{}]", parts.join(" ")),
            7 => format!("(f {})", parts.join(" ")),
            8 => {
                let entries: Vec<String> = parts
                    .iter()
                    .enumerate()
                    .map(|(at, part)| format!(":k{at} {part}"))
                    .collect();
                format!("{{{}}}", entries.join(" "))
            }
            _ => {
                let terms: Vec<String> = parts
                    .iter()
                    .enumerate()
                    .map(|(at, part)| format!("l{at}: {part}"))
                    .collect();
                format!("(g ?e {})", terms.join(" "))
            }
        }
    }

    /// Read, print and read again is the trees first read, whichever
    /// printer prints them, for every form the reader takes: numbers at
    /// their limits and written with an exponent, `-0`, texts with what
    /// a text escapes, keywords, names, and forms nested and empty. A
    /// whole number too large for 64 bits is refused, not read as a
    /// float.
    #[test]
    fn it_reads_back_what_it_prints() -> Result<(), Error> {
        let mut read = 0;
        for seed in 1..=400u64 {
            let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
            let mut next = move || {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state
            };
            let text = made(&mut next, 4);
            // Only what the reader takes is to read back.
            let Ok((trees, written)) = crate::parse_written(&text) else {
                continue;
            };
            read += 1;
            let shown: Vec<String> = trees.iter().map(ToString::to_string).collect();
            assert_eq!(
                crate::parse(&shown.join(" "))?,
                trees,
                "{text} printed as {shown:?}"
            );
            let printed = crate::print(&trees, &written);
            assert_eq!(
                crate::parse(&printed)?,
                trees,
                "{text} printed as {printed}"
            );
        }
        assert!(read > 300, "{read} of 400 forms read");
        // A constant of every kind prints as text that reads back as the
        // tree a value of it is answered as; bytes have no written form,
        // and print as no other value.
        let map = data::record([
            ("title", data::text("Buy milk")),
            (
                "tags",
                data::list([data::text("errand"), data::text("home")])?,
            ),
        ])?;
        for value in [
            data::text("a \"quoted\" text"),
            Value::Boolean(true),
            Value::UnsignedInt(7u64.into()),
            Value::SignedInt((-7i64).into()),
            Value::Float(1.5),
            data::entity("todo:1")?,
            data::entity("open:")?,
            data::symbol("xyz.todo/text")?,
            map.clone(),
            data::list([map, data::list([])?, Value::UnsignedInt(1u64.into())])?,
        ] {
            let printed = Syntax::Constant(value.clone()).printed()?;
            assert_eq!(
                crate::parse(&printed)?,
                vec![crate::compile::written(&value)],
                "{printed}"
            );
        }
        for bytes in [
            Value::Bytes(vec![1, 2]),
            Value::Record(vec![3]),
            data::list([Value::Bytes(vec![4])])?,
        ] {
            assert!(
                matches!(
                    Syntax::Constant(bytes.clone()).printed(),
                    Err(Error::Unsupported(_))
                ),
                "{bytes:?}"
            );
            let printed = Syntax::Constant(bytes).to_string();
            assert!(
                !crate::parse(&printed).is_ok_and(|trees| trees == vec![Syntax::Blank]),
                "{printed}"
            );
        }
        for text in ["18446744073709551616", "-9223372036854775809", "1e400"] {
            assert!(
                matches!(crate::parse(text), Err(Error::Refused(why)) if why.contains(text)),
                "{text}: {:?}",
                crate::parse(text)
            );
        }
        Ok(())
    }

    /// A tree read back from a value, or made from forms a caller built,
    /// may name a variable the notation makes, or a name under its
    /// namespace, which no text can: both are refused as `Reserved`.
    #[test]
    fn it_refuses_a_tree_from_a_value_or_forms_that_names_what_the_notation_makes()
    -> Result<(), Error> {
        for made in ["z[1]", "[source]b"] {
            let tree = Syntax::Application(
                Box::new(Syntax::Reference("xyz.a".into())),
                Parameters {
                    items: vec![Syntax::Variable("t".into())],
                    terms: [("v".to_string(), Syntax::Variable(made.into()))].into(),
                },
            );
            let value = tree.value()?;
            assert!(
                matches!(Syntax::try_from(&value), Err(Error::Reserved(name)) if name == format!("?{made}")),
                "{made}"
            );
            let form = tree.form();
            assert!(
                matches!(Syntax::try_from(&form), Err(Error::Reserved(name)) if name == format!("?{made}")),
                "{made}"
            );
        }
        let tree = Syntax::Application(
            Box::new(Syntax::Reference("dialog.n.select".into())),
            Parameters::default(),
        );
        assert!(matches!(
            Syntax::try_from(&tree.value()?),
            Err(Error::Reserved(name)) if name == "dialog.n.select"
        ));
        assert!(matches!(
            Syntax::try_from(&tree.form()),
            Err(Error::Reserved(name)) if name == "dialog.n.select"
        ));
        Ok(())
    }
}
