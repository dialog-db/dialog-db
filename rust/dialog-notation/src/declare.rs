//! What a document declares, as data another back end walks.
//!
//! [`check`](crate::check()) gives, beside what a document compiles to,
//! its declarations in the order written: each concept and command,
//! with the comment above it, its lines as written and the comment
//! above each, and the Dialog-DB descriptor a read of it compiles to;
//! and each query, by its name, with whether its answer is markup.

use dialog_query::{AttributeDescriptor, ConceptDescriptor};

use crate::analyze::Analyzer;
use crate::compile::{Step, compile};
use crate::form::Kind;
use crate::kernel::{LAYERS, PICKS, TYPES};
use crate::syntax::{Parameters, Syntax};
use crate::written::{Comment, Segment, Written};

/// One thing a document declares.
#[derive(Clone, Debug, PartialEq)]
pub enum Declaration {
    /// A concept or a command.
    Concept(Concept),
    /// A query, and the comment above it.
    Query {
        /// What the comment right above the query says.
        description: Option<String>,
        /// Its name: `todo/row`.
        name: String,
        /// Whether its answer is markup: its body says `(view ?x …)`.
        markup: bool,
    },
}

/// A concept or a command, as declared.
#[derive(Clone, Debug, PartialEq)]
pub struct Concept {
    /// Its name: `todo`, `todo/add`.
    pub name: String,
    /// What the comment right above it says.
    pub description: Option<String>,
    /// Whether it is a command: its head names no entity and its lines
    /// are on the control layer.
    pub command: bool,
    /// The variable its head names the entity by, for a concept.
    pub entity: Option<String>,
    /// Its head's fields, by label, each with its variable.
    pub fields: Vec<(String, Syntax)>,
    /// Its lines, in the order written.
    pub lines: Vec<Line>,
    /// What a read of it compiles to: one descriptor a domain it reads.
    pub descriptors: Vec<ConceptDescriptor>,
}

/// One line of a concept: one attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct Line {
    /// The attribute by its full name, as written: `xyz.todo/title`, or
    /// `xyz.tonk.notebook/?k` for every entry of a collection.
    pub attribute: String,
    /// What it is of: `of:`.
    pub of: Syntax,
    /// What it is: `is:`.
    pub is: Syntax,
    /// `as:`, as written: a type's keyword, or a variant's cases.
    pub kind: Option<Syntax>,
    /// `cardinality:` as written, `one` or `many`, or none.
    pub cardinality: Option<String>,
    /// `in:`, the layer, as written.
    pub layer: Option<String>,
    /// The key, where the line is over a collection.
    pub key: Option<Key>,
    /// What the comment right above the line says.
    pub description: Option<String>,
    /// The label the concept's head gives the line's value, `title` for
    /// `(todo ?t title: ?x)` over `(xyz.todo/title of: ?t is: ?x)`: the
    /// name a field goes by. None where the value is no head field.
    pub label: Option<String>,
    /// The attribute descriptor the line compiles to, as Dialog-DB reads
    /// it.
    pub descriptor: Option<AttributeDescriptor>,
}

/// The key of a line over a collection.
#[derive(Clone, Debug, PartialEq)]
pub struct Key {
    /// The key: a variable, or the name of one entry.
    pub term: Syntax,
    /// What a key is, `by:`: `name` or `position`.
    pub by: String,
    /// The label the concept's head gives the key, where it is a head
    /// field. A key in an entry, `block: {?k ?b}`, has none: the entry
    /// is the line's label.
    pub label: Option<String>,
}

/// What the comments right above the node at `path` say.
pub(crate) fn said(written: &Written, path: &[Segment]) -> Option<String> {
    let said: Vec<&str> = written
        .comments(path)
        .into_iter()
        .map(Comment::said)
        .filter(|said| !said.is_empty())
        .collect();
    (!said.is_empty()).then(|| said.join("\n"))
}

fn keyword(node: Option<&Syntax>) -> Option<String> {
    node.and_then(Syntax::keyword).map(str::to_string)
}

/// The lines a body part says: one, for a line, or one a field, for a
/// read written with wrappers.
fn lines(part: &Syntax, description: Option<String>) -> Vec<Line> {
    let (Some(name), Some(operand)) = (part.applies(), part.operand()) else {
        return Vec::new();
    };
    if Kind::of(name) == Kind::Attribute {
        let (of, is) = match (operand.items.as_slice(), operand.terms.get("is")) {
            ([of], Some(is)) => (of.clone(), is.clone()),
            ([of, is], None) => (of.clone(), is.clone()),
            _ => (Syntax::Blank, Syntax::Blank),
        };
        let key = keyword(operand.terms.get("by")).map(|by| {
            let field = name
                .split_once('/')
                .map(|(_, field)| field)
                .unwrap_or_default();
            Key {
                term: match field.strip_prefix('?') {
                    Some(variable) => Syntax::Variable(variable.to_string()),
                    None => Syntax::Constant(dialog_corpus::Value::String(field.to_string())),
                },
                by,
                label: None,
            }
        });
        return vec![Line {
            attribute: name.to_string(),
            of,
            is,
            kind: operand.terms.get("as").cloned(),
            cardinality: keyword(operand.terms.get("cardinality")),
            layer: keyword(operand.terms.get("in")),
            key,
            description,
            label: None,
            descriptor: None,
        }];
    }
    // A read, `(xyz.todo ?t title: (:last (:text ?x)))`: a line a field,
    // its type, pick and layer taken off the wrappers.
    let of = operand.items.first().cloned().unwrap_or(Syntax::Blank);
    operand
        .terms
        .iter()
        .map(|(label, term)| {
            let mut line = Line {
                attribute: if label.contains('/') {
                    label.clone()
                } else {
                    format!("{name}/{label}")
                },
                of: of.clone(),
                is: term.clone(),
                kind: None,
                cardinality: None,
                layer: None,
                key: None,
                description: description.clone(),
                label: None,
                descriptor: None,
            };
            let mut node = term;
            while let Syntax::Application(operator, Parameters { items, terms }) = node {
                let (Some(word), [inner], true) =
                    (operator.keyword(), items.as_slice(), terms.is_empty())
                else {
                    break;
                };
                if TYPES.contains(&word) {
                    line.kind = Some((**operator).clone());
                } else if PICKS.contains(&word) {
                    line.cardinality = match word {
                        "last" => Some("one".into()),
                        "all" => Some("many".into()),
                        _ => None,
                    };
                } else if LAYERS.contains(&word) {
                    line.layer = Some(word.to_string());
                } else {
                    break;
                }
                node = inner;
            }
            line.is = node.clone();
            line
        })
        .collect()
}

/// The concept a `(concept head line…)` tree at `at` declares, with the
/// descriptors a read of it compiles to through `analyzer`.
pub(crate) fn concept(
    tree: &Syntax,
    at: usize,
    written: &Written,
    analyzer: &Analyzer,
) -> Option<Concept> {
    let [head, body @ ..] = tree.operand()?.items.as_slice() else {
        return None;
    };
    let name = head.applies()?.to_string();
    let head_operand = head.operand()?;
    let entity = match head_operand.items.first() {
        Some(Syntax::Variable(variable)) => Some(variable.clone()),
        _ => None,
    };
    let mut lines: Vec<Line> = body
        .iter()
        .enumerate()
        .flat_map(|(index, part)| {
            let mut lines = lines(
                part,
                said(written, &[Segment::Item(at), Segment::Item(index + 1)]),
            );
            // The descriptor each line compiles to: the kernel read the
            // part is, a field a line.
            let read = match crate::expand::line(part) {
                Ok(Some(read)) => Some(read),
                Ok(None) if part.is_read() => Some(part.clone()),
                _ => None,
            };
            if let Some(read) = read.and_then(|read| crate::kernel::read(&read).ok())
                && read.fields.len() == lines.len()
            {
                for (line, field) in lines.iter_mut().zip(&read.fields) {
                    line.descriptor = crate::compile::descriptor(field).ok();
                }
            }
            lines
        })
        .collect();
    // The label the head gives each line's value and key.
    // A collection's field is one entry, `block: {?k ?b}`: its label is
    // the value's, and the key has none of its own.
    let labelled = |term: &Syntax| {
        head_operand.terms.iter().find_map(|(label, field)| {
            let value = match field {
                Syntax::Map(pair) => match pair.as_slice() {
                    [(_, value)] => value,
                    _ => field,
                },
                _ => field,
            };
            (value == term && matches!(term, Syntax::Variable(_))).then(|| label.clone())
        })
    };
    for line in &mut lines {
        line.label = labelled(&line.is);
        if let Some(key) = &mut line.key {
            key.label = head_operand.terms.iter().find_map(|(label, field)| {
                (field == &key.term && matches!(key.term, Syntax::Variable(_)))
                    .then(|| label.clone())
            });
        }
    }
    let command = entity.is_none()
        && !lines.is_empty()
        && lines
            .iter()
            .all(|line| line.layer.as_deref() == Some("control"));
    // What a read of it compiles to: the descriptors Dialog-DB reads it
    // through.
    let query = Syntax::Application(
        Box::new(Syntax::Reference("select".into())),
        Parameters {
            items: vec![head.clone()],
            ..Parameters::default()
        },
    );
    let analysis = analyzer.clone().analyze(&[query]);
    let descriptors = analysis
        .analyzed
        .iter()
        .filter_map(|rule| compile(rule).ok())
        .flatten()
        .filter_map(|step| match step {
            Step::Query(query) => Some(query.select.predicate),
            _ => None,
        })
        .collect();
    Some(Concept {
        name,
        description: said(written, &[Segment::Item(at)]),
        command,
        entity,
        fields: head_operand
            .terms
            .iter()
            .map(|(label, term)| (label.clone(), term.clone()))
            .collect(),
        lines,
        descriptors,
    })
}

/// The query a tree defines, with the comment above it.
pub(crate) fn query(tree: &Syntax, origin: usize, written: &Written) -> Option<Declaration> {
    let operand = tree.operand()?;
    let (head, body) = operand.items.split_first()?;
    Some(Declaration::Query {
        description: said(written, &[Segment::Item(origin)]),
        name: head.applies()?.to_string(),
        markup: body
            .iter()
            .any(|condition| condition.applies() == Some("view")),
    })
}
