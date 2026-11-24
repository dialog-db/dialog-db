//! What a document declares, as data another back end walks.
//!
//! [`check`](crate::check()) gives, beside what a document compiles to,
//! its declarations in the order written: each concept and command,
//! with the comment above it, its lines as written and the comment
//! above each, and the Dialog-DB descriptor a read of it compiles to;
//! and each query, by its name, with whether its answer is markup.

use dialog_query::{AttributeDescriptor, ConceptDescriptor};

use crate::Error;
use crate::analyze::Analyzer;
use crate::compile::{Step, compile};
use crate::form::Kind;
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
    /// The attribute's cardinality, `one` or `many`, as Dialog-DB reads
    /// it: as written, by `cardinality:` or `(:one …)`, or by a pick, or
    /// `many` where none is written, since such a line reads every
    /// claim. None only where the line does not compile.
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
fn lines_of(part: &Syntax, description: Option<String>) -> Result<Vec<Line>, Error> {
    let (Some(name), Some(operand)) = (part.applies(), part.operand()) else {
        return Ok(Vec::new());
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
        return Ok(vec![Line {
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
        }]);
    }
    // A read, `(xyz.todo ?t title: (:last (:text ?x)))`: a line a field,
    // what it is written under taken off it as the kernel reads it.
    let of = operand.items.first().cloned().unwrap_or(Syntax::Blank);
    operand
        .terms
        .iter()
        .map(|(label, term)| {
            let wrapped = crate::kernel::wrapped(term)?;
            let cardinality = wrapped.cardinality.or(match wrapped.pick.as_deref() {
                Some("last") => Some("one".into()),
                // `all` reads every claim, and an elector chooses one of
                // several: the attribute holds many.
                Some(_) => Some("many".into()),
                None => None,
            });
            let attribute = if label.contains('/') {
                label.clone()
            } else {
                format!("{name}/{label}")
            };
            Ok(Line {
                attribute,
                of: of.clone(),
                is: wrapped.inner.clone(),
                kind: wrapped.cases.or(wrapped.kind.map(|(_, written)| written)),
                cardinality,
                layer: wrapped.layer,
                key: wrapped.key.map(|(at, by, _)| Key {
                    term: at.clone(),
                    by,
                    label: None,
                }),
                description: description.clone(),
                label: None,
                descriptor: None,
            })
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
) -> Result<Option<Concept>, Error> {
    let Some([head, body @ ..]) = tree.operand().map(|operand| operand.items.as_slice()) else {
        return Ok(None);
    };
    let (Some(name), Some(head_operand)) = (head.applies(), head.operand()) else {
        return Ok(None);
    };
    let name = name.to_string();
    let entity = match head_operand.items.first() {
        Some(Syntax::Variable(variable)) => Some(variable.clone()),
        _ => None,
    };
    let mut lines: Vec<Line> = Vec::new();
    for (index, part) in body.iter().enumerate() {
        let mut these = lines_of(
            part,
            said(written, &[Segment::Item(at), Segment::Item(index + 1)]),
        )?;
        // The descriptor each line compiles to: the kernel read the part
        // is, a field a line. Its cardinality is the line's, as Dialog-DB
        // reads it.
        let read = match crate::expand::line(part)? {
            Some(read) => Some(read),
            None if part.is_read() => Some(part.clone()),
            None => None,
        };
        if let Some(read) = read {
            let read = crate::kernel::read(&read)?;
            if read.fields.len() == these.len() {
                for (line, field) in these.iter_mut().zip(&read.fields) {
                    let descriptor = crate::compile::descriptor(field)?;
                    line.cardinality = Some(
                        match descriptor.cardinality() {
                            dialog_query::Cardinality::One => "one",
                            dialog_query::Cardinality::Many => "many",
                        }
                        .into(),
                    );
                    line.descriptor = Some(descriptor);
                }
            }
        }
        lines.extend(these);
    }
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
            // A field an elector chooses is labelled as the value it
            // chooses from.
            let value = crate::expand::elected(value).map_or(value, |(_, inner)| inner);
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
    // The head is asked with what an elector in it chooses from: the
    // elector is the concept's pick, and a read is of the value.
    let mut asked = head.clone();
    if let Syntax::Application(_, operand) = &mut asked {
        for term in operand.terms.values_mut() {
            if let Some((_, inner)) = crate::expand::elected(term) {
                *term = inner.clone();
            }
        }
    }
    let query = Syntax::Application(
        Box::new(Syntax::Reference("select".into())),
        Parameters {
            items: vec![asked],
            ..Parameters::default()
        },
    );
    let analysis = analyzer.trial(&[query]);
    if let Some(error) = analysis
        .refusals()
        .find_map(|refused| refused.refusal.clone())
    {
        return Err(error);
    }
    let mut descriptors = Vec::new();
    for rule in &analysis.analyzed {
        for step in compile(rule)? {
            if let Step::Query(query) = step {
                descriptors.push(query.select.predicate);
            }
        }
    }
    Ok(Some(Concept {
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
    }))
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
