//! Expansion: rewriting a tree until every part of it is primitive.
//!
//! The primitive forms are small: `rule`, whose first part is its head and whose
//! other parts are its premises, `(unless …)` among them for what must
//! not hold; the application of a relation and of a formula; `assert`,
//! `retract` and `report` as a rule's head. A head with no word is
//! derived. `assert` of claims on the control layer, a command's, issues
//! them for one tick. A rule has one head. Every other form is given its meaning by a rule
//! about syntax:
//!
//! ```clj
//! (rule (syntax (rule (report ?head) ?head))
//!   (expand (select ?head)))
//! ```
//!
//! `(expand pattern)` holds for the tree being expanded when it has the
//! pattern's shape, and `(syntax template)` is what the tree becomes.
//! Neither is walked into: what is inside them is syntax.
//!
//! A template answers `(all a b)` when the tree stands for several, as
//! a concept does for the reads of its body. Among the premises they
//! stand one after another. In the head's place they are a rule each,
//! with the premises again. `(all …)` is not written in a rule: it is
//! refused there.
//!
//! The [`Expander`] is the walker. It rewrites a top-level tree until
//! it is a `rule`, then every head and premise until each applies a
//! relation or a formula, asking the rules about syntax at each step.
//! In this proof of concept those rules are kept and evaluated here,
//! not stored in a replica and not run by the query engine, which
//! cannot yet take a tree apart.

use std::collections::{BTreeMap, BTreeSet};

use dialog_corpus::Value;

use crate::Error;
use crate::diagnostic::Diagnostic;
use crate::form::Kind;
use crate::parse::parse;
use crate::pattern::{Binding, Bindings, REST, fill, matches};
use crate::primitive::QUOTE;
use crate::primitive::{
    AMONG, CARDINALITIES, CONTROL, ELECTORS, Head, IN, Keyed, PICKS, RETIRED, Rule, TYPES,
};
use crate::syntax::{self, Parameters, Signature, Syntax};
use crate::written::{Path, Segment};

mod query;

/// The core forms, written in the primitive forms.
pub const PRELUDE: &str = include_str!("prelude.clj");

/// Where a form stands when it is expanded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Position {
    /// A rule's head, or the top level of a document.
    Head,
    /// A premise, `(unless …)` or not.
    Premise,
}

/// What a rule about syntax concludes.
#[derive(Clone, Debug)]
enum Answer {
    /// `(syntax template)`: the form becomes the template, filled.
    Syntax(Syntax),
    /// `(warning "…")`: the form stands, and this is said about it.
    Warning(String),
}

/// A rule about syntax: `(expand pattern)` among its premises, and
/// `(syntax template)` or `(warning "…")` as its head.
#[derive(Clone, Debug)]
struct Definition {
    answer: Answer,
    pattern: Syntax,
    position: Option<Position>,
    when: Vec<Syntax>,
    unless: Vec<Syntax>,
    /// What the rule defines, where it defines a concept: a name
    /// applied with labels, standing for reads and nothing else.
    signature: Option<Signature>,
    /// The command whose entity this definition reads, where it is the
    /// one a command's declaration adds for `of:` in a premise.
    entity_of: Option<String>,
}

/// One form that several definitions take, and which of them stands
/// for it in the rule being expanded.
#[derive(Clone, Debug)]
struct Choice {
    form: Syntax,
    taken: usize,
    of: usize,
}

/// The formula that makes a position between two others.
const POSITION: &str = "dialog/position";

/// What a primitive rule says of itself when it is a write at the top level
/// with premises: it runs once and is not installed.
pub const ONCE: &str = "once";

/// The words that make a rule's head a write or an answer.
const HEAD_WORDS: [&str; 3] = ["assert", "retract", "report"];

/// How many times one form may be rewritten on its way to the primitive forms,
/// counted along the path from the top-level form. The prelude and every
/// test of the crate rewrite a form at most twice; one rewritten 32
/// times is one whose expansion does not end. It is below what a form
/// that grows a level each step reaches before it is too deep to be
/// named: a level of syntax is up to three of a composite value, which
/// nests 128 deep.
pub(crate) const STEPS: usize = 32;

/// How deep a tree may nest, given to the expander or made by it: as
/// deep as a composite value nests, and a level of syntax is at least
/// one of a composite, so nothing past it can be named or kept, and the
/// stages that walk a tree are never given one deeper.
pub(crate) const DEEPEST: usize = 2 * crate::form::DEPTH;

/// What one form may expand to is at most sixteen times the parts of
/// the top-level form it is in, and 1024 more: a template that writes a
/// variable twice doubles a form each step, and is refused past this
/// long before the steps run out.
const GROWTH: usize = 16;
const SPARE: usize = 1024;

/// The label no field may have: what every query and rule calls the
/// entity.
pub const THIS: &str = "this";

/// Whether a document may not name `name`, a relation or an attribute
/// of one: it is under the notation's own namespace.
fn reserved_name(name: &str) -> bool {
    matches!(
        syntax::named(name),
        syntax::Named::Reserved | syntax::Named::Derived
    )
}

/// The first part of `tree` as written that names what the notation
/// names itself, with the name, outermost first. What is quoted is not
/// a form and names nothing.
pub(crate) fn reserved(tree: &Syntax) -> Option<(&Syntax, String)> {
    let mut open = vec![tree];
    while let Some(part) = open.pop() {
        match part {
            Syntax::Reference(name) if reserved_name(name) => return Some((part, name.clone())),
            // A variable the notation makes holds a `[`, which the reader
            // never reads: only a caller that builds trees can name one.
            Syntax::Variable(name) if syntax::unwritten(name) => {
                return Some((part, format!("?{name}")));
            }
            Syntax::Application(..) if part.applies() == Some(QUOTE) => {}
            // `this` is what every query and rule calls the entity, so a
            // field of that name would be the entity. (A concept pinned
            // to an entity, which the design writes `this:`, is not
            // built.)
            Syntax::Application(_, operand) if operand.terms.contains_key(THIS) => {
                return Some((part, format!("{THIS}:")));
            }
            Syntax::Application(operator, operand) => {
                // A label is a name like any other: one under the
                // namespace would conclude or read what the notation keeps.
                if let Some(label) = operand.terms.keys().find(|label| reserved_name(label)) {
                    return Some((part, format!("{label}:")));
                }
                open.extend(operand.terms.values().rev());
                open.extend(operand.items.iter().rev());
                open.push(operator);
            }
            Syntax::Map(entries) => {
                for (key, value) in entries.iter().rev() {
                    open.push(value);
                    open.push(key);
                }
            }
            Syntax::Tuple(items) => open.extend(items.iter().rev()),
            _ => {}
        }
    }
    None
}

/// `pending` with the concepts of the document put in the order they
/// read each other: one whose body reads a concept another form of the
/// document declares comes after that form, so that what it reads is a
/// concept when it is installed, whatever order they are written in.
/// What reads nothing of the document keeps its place, a rule about
/// syntax before any concept; concepts that read each other round keep
/// the order written.
fn concepts_in_order(pending: &mut Vec<usize>, trees: &[Syntax]) {
    let declares = |at: usize| {
        let tree = &trees[at];
        (tree.applies() == Some("concept"))
            .then(|| items(tree).first().and_then(Syntax::applies))
            .flatten()
            .map(str::to_string)
    };
    let concepts: Vec<usize> = pending
        .iter()
        .copied()
        .filter(|at| declares(*at).is_some())
        .collect();
    let reads = |at: usize| -> Vec<String> {
        items(&trees[at])
            .iter()
            .skip(1)
            .flat_map(|member| {
                let mut names = Vec::new();
                let mut open = vec![member];
                while let Some(part) = open.pop() {
                    if let Some(name) = part.applies() {
                        names.push(name.to_string());
                    }
                    open.extend(items(part));
                }
                names
            })
            .collect()
    };
    let mut placed: Vec<usize> = Vec::new();
    let mut left = concepts.clone();
    while !left.is_empty() {
        let ready = left.iter().position(|at| {
            reads(*at).iter().all(|name| {
                left.iter()
                    .all(|other| other == at || declares(*other).as_deref() != Some(name.as_str()))
            })
        });
        // Concepts that read each other round: the first written goes.
        placed.push(left.remove(ready.unwrap_or(0)));
    }
    let first = pending.iter().position(|at| concepts.contains(at));
    pending.retain(|at| !concepts.contains(at));
    let at = first.map_or(pending.len(), |first| first.min(pending.len()));
    pending.splice(at..at, placed);
}

/// The first application in `tree` as written that applies, or labels
/// a part with, what is no name, [`syntax::Named::Malformed`], and the
/// name. A reference that stands as a value, a symbol `/todo` or a set's
/// empty key, names nothing applied. What is quoted is not a form.
fn malformed(tree: &Syntax) -> Option<(&Syntax, String)> {
    let bad = |name: &str| syntax::named(name) == syntax::Named::Malformed;
    let mut open = vec![tree];
    while let Some(part) = open.pop() {
        match part {
            Syntax::Application(..) if part.applies() == Some(QUOTE) => {}
            Syntax::Application(operator, operand) => {
                if let Syntax::Reference(name) = &**operator
                    && bad(name)
                {
                    return Some((part, name.clone()));
                }
                // The empty label, `:`, is the label of what has none.
                if let Some(label) = operand
                    .terms
                    .keys()
                    .find(|label| !label.is_empty() && bad(label))
                {
                    return Some((part, label.clone()));
                }
                open.extend(operand.terms.values().rev());
                open.extend(operand.items.iter().rev());
            }
            Syntax::Map(entries) => entries.iter().rev().for_each(|(_, value)| {
                open.push(value);
            }),
            Syntax::Tuple(items) => open.extend(items.iter().rev()),
            _ => {}
        }
    }
    None
}

/// The first `(collect ?x)` in `tree` as written: the marker of what a
/// read gathers before it was `distinct`. Only its shape, one part and
/// no label: `collect` as a name is refused as one of the notation's own
/// words. What is quoted is not a form.
fn collected_as_before(tree: &Syntax) -> Option<&Syntax> {
    let mut open = vec![tree];
    while let Some(part) = open.pop() {
        match part {
            Syntax::Application(..) if part.applies() == Some(QUOTE) => {}
            Syntax::Application(operator, operand) => {
                if part.applies() == Some(crate::select::COLLECT)
                    && operand.items.len() == 1
                    && operand.terms.is_empty()
                {
                    return Some(part);
                }
                open.push(operator);
                open.extend(operand.terms.values().rev());
                open.extend(operand.items.iter().rev());
            }
            Syntax::Map(entries) => entries.iter().rev().for_each(|(key, value)| {
                open.push(value);
                open.push(key);
            }),
            Syntax::Tuple(items) => open.extend(items.iter().rev()),
            _ => {}
        }
    }
    None
}

/// The first read of a domain in `tree` as written with a label that
/// makes an attribute longer than Dialog-DB keeps, the label and the
/// attribute: `(xyz.t ?x label: …)` reads `xyz.t/label`. What is quoted
/// is not a form.
fn overlong(tree: &Syntax) -> Option<(&Syntax, String, String)> {
    let mut open = vec![tree];
    while let Some(part) = open.pop() {
        match part {
            Syntax::Application(..) if part.applies() == Some(QUOTE) => {}
            Syntax::Application(operator, operand) => {
                if let Syntax::Reference(domain) = &**operator
                    && syntax::named(domain) == syntax::Named::Domain
                    && let Some(label) = operand.terms.keys().find(|label| {
                        !label.is_empty()
                            && domain.len() + 1 + label.len() > syntax::RELATION_LENGTH
                    })
                {
                    return Some((part, label.clone(), format!("{domain}/{label}")));
                }
                open.extend(operand.terms.values().rev());
                open.extend(operand.items.iter().rev());
            }
            Syntax::Map(entries) => entries.iter().rev().for_each(|(_, value)| {
                open.push(value);
            }),
            Syntax::Tuple(items) => open.extend(items.iter().rev()),
            _ => {}
        }
    }
    None
}

/// The first application in `tree` as written with a label that holds a
/// `/`, and the label. Only the notation writes one, where it joins the
/// reads of one entity under `unless` into one read: a document that
/// writes one names another domain's attribute where its own field
/// stands. What is quoted is not a form.
fn slashed(tree: &Syntax) -> Option<(&Syntax, String)> {
    let mut open = vec![tree];
    while let Some(part) = open.pop() {
        match part {
            Syntax::Application(..) if part.applies() == Some(QUOTE) => {}
            Syntax::Application(operator, operand) => {
                if let Some(label) = operand.terms.keys().find(|label| label.contains('/')) {
                    return Some((part, label.clone()));
                }
                open.extend(operand.terms.values().rev());
                open.extend(operand.items.iter().rev());
                open.push(operator);
            }
            Syntax::Map(entries) => entries.iter().rev().for_each(|(key, value)| {
                open.push(value);
                open.push(key);
            }),
            Syntax::Tuple(items) => open.extend(items.iter().rev()),
            _ => {}
        }
    }
    None
}

/// What the two writes were called while a mark told what is kept from
/// what holds one round. Unrefused, either would be read as a name that
/// nothing gives a meaning, and said to be unknown.
const FORMER_WORDS: [(&str, &str); 2] = [("assert!", "assert"), ("retract!", "retract")];

/// Refuse a write under the name it had.
fn former(tree: &Syntax) -> Result<(), Error> {
    match FORMER_WORDS
        .iter()
        .find(|(former, _)| tree.applies() == Some(former))
    {
        Some((former, word)) => Err(Error::Refused(format!(
            "`{former}` is written `{word}`: no form carries `!`"
        ))),
        None => Ok(()),
    }
}

/// The words whose parts are syntax, never walked into.
const DIRECTIVES: [&str; 3] = ["expand", "syntax", "warning"];

/// `name` applied to `items`, with no labelled part.
fn apply(name: &str, items: Vec<Syntax>) -> Syntax {
    Syntax::Application(
        Box::new(Syntax::Reference(name.to_string())),
        Parameters {
            items,
            ..Parameters::default()
        },
    )
}

/// A formula of one part, as a type and a pick are: `(:text ?x)`.
fn wrap(word: &str, inner: Syntax) -> Result<Syntax, Error> {
    Ok(Syntax::Application(
        Box::new(Syntax::Constant(syntax::entity(&format!("{word}:"))?)),
        Parameters {
            items: vec![inner],
            ..Parameters::default()
        },
    ))
}

/// A rule about syntax whose pattern chooses a field by an elector,
/// `(account ?a status: (max ?s))`, as the rule that applies the field as
/// written and reads its line under the elector: the pattern takes
/// `status: ?s`, and the line whose value is `?s` reads `is: (max ?s)`.
/// Any other rule is as it was.
fn electing(rule: Syntax) -> Syntax {
    let Syntax::Application(operator, mut operand) = rule else {
        return rule;
    };
    let mut chosen: BTreeMap<String, String> = BTreeMap::new();
    for premise in operand.items.iter_mut().skip(1) {
        if premise.applies() != Some("expand") {
            continue;
        }
        let Some(Syntax::Application(_, pattern)) = premise
            .operand_mut()
            .and_then(|expand| expand.items.first_mut())
        else {
            continue;
        };
        for term in pattern.terms.values_mut() {
            if let Some((word, Syntax::Variable(name))) = elected(term) {
                chosen.insert(name.clone(), word.to_string());
                *term = Syntax::Variable(name.clone());
            }
        }
    }
    if !chosen.is_empty()
        && let Some(head) = operand.items.first_mut()
        && head.applies() == Some("syntax")
        && let Some(template) = head
            .operand_mut()
            .and_then(|syntax| syntax.items.first_mut())
    {
        elect(template, &chosen);
    }
    Syntax::Application(operator, operand)
}

/// Every line in `template` whose value is a variable `chosen` names,
/// read under that variable's elector.
fn elect(template: &mut Syntax, chosen: &BTreeMap<String, String>) {
    let Syntax::Application(operator, operand) = template else {
        return;
    };
    match &**operator {
        Syntax::Reference(word) if word == "all" => {
            for member in &mut operand.items {
                elect(member, chosen);
            }
        }
        Syntax::Reference(name) if syntax::named(name) == syntax::Named::Attribute => {
            let value = match operand.terms.get_mut("is") {
                Some(value) => Some(value),
                None => operand.items.get_mut(1),
            };
            if let Some(value) = value
                && let Syntax::Variable(name) = value
                && let Some(word) = chosen.get(name.as_str())
            {
                *value = apply(word, vec![value.clone()]);
            }
        }
        _ => {}
    }
}

/// Refuse `top`, which is no elector: over a listed type it chose the
/// case listed first. A listing is written least first, so the winner is
/// listed last: `top` over a list is `max` over the list reversed, or
/// `min` over the same list. It is looked for where an elector stands,
/// a term's value, `status: (top ?s)`, and as a pick, `(:top ?s)`, so a
/// name `top` applied elsewhere is what it is.
fn retired(tree: &Syntax) -> Result<(), Error> {
    let refuse = |node: &Syntax| {
        Err(Error::Refused(format!(
            "{node} is under `top`, which is no elector: `top` over a list is `max` over the \
             list reversed, written least first, or `min` over the same list"
        )))
    };
    match tree {
        Syntax::Application(operator, operand) => {
            if operator.keyword() == Some(RETIRED) {
                return refuse(tree);
            }
            for term in operand.terms.values() {
                if term.applies() == Some(RETIRED)
                    && term
                        .operand()
                        .is_some_and(|inner| inner.items.len() == 1 && inner.terms.is_empty())
                {
                    return refuse(term);
                }
            }
            retired(operator)?;
            operand.items.iter().try_for_each(retired)?;
            operand.terms.values().try_for_each(retired)
        }
        Syntax::Map(entries) => entries
            .iter()
            .try_for_each(|(key, value)| retired(key).and_then(|()| retired(value))),
        Syntax::Tuple(items) => items.iter().try_for_each(retired),
        _ => Ok(()),
    }
}

/// The elector a term is written under and the term it chooses from:
/// `(max ?s)` is `max` over `?s`.
pub(crate) fn elected(term: &Syntax) -> Option<(&str, &Syntax)> {
    match (term.applies(), term.operand()) {
        (Some(word), Some(operand)) if ELECTORS.contains(&word) && operand.terms.is_empty() => {
            match operand.items.as_slice() {
                [inner] => Some((word, inner)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// The read a line is. A line is one attribute, by its full name, and
/// is what a document writes; the read below it is primitive, which
/// the walker writes for it and no document writes:
///
/// ```clj
/// (xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)
/// (xyz.todo ?i title: (:one (:text ?t)))
/// ```
///
/// `of:` is the entity and `is:` the value; unlabeled, the two are the
/// entity and then the value, `(xyz.todo/title ?i ?t)`. `as:` is the type the value
/// is read under. `cardinality:` says how many claims the attribute
/// holds for one entity: `:one` reads the newest, `:many` every one.
/// Either may be left out; with no cardinality the read takes every
/// claim. An elector on the value, `is: (max ?s)`, chooses one claim of
/// many, as one in the concept's head does. `in:`
/// is the layer: a command's, `:control`, any other a stack declares,
/// `:session`, or a variable, `?layer`, which a read binds to the layer
/// each row is in and a write must have bound.
///
/// `None` when `tree` applies no attribute.
pub(crate) fn line(tree: &Syntax) -> Result<Option<Syntax>, Error> {
    let Syntax::Application(operator, operand) = tree else {
        return Ok(None);
    };
    let Some((domain, field)) = (match &**operator {
        // Only a stored attribute, its domain dotted: a name of words
        // joined by `/` with no `.` is a concept's or a derived
        // attribute's, never a relation named by its first word.
        Syntax::Reference(name) if syntax::named(name) == syntax::Named::Attribute => {
            name.split_once('/')
        }
        _ => None,
    }) else {
        return Ok(None);
    };
    let mut terms = operand.terms.clone();
    // A line over a collection leaves the key a variable in the name,
    // and says what a key is.
    let by = terms.remove("by");
    let (field, key) = match (field.strip_prefix('?'), by) {
        (Some(key), Some(by)) => match by.keyword().and_then(Keyed::of) {
            Some(_) => ("entry", Some((Syntax::Variable(key.to_string()), by))),
            None => {
                return Err(Error::Refused(format!(
                    "{by} is not what a key is: `by: :name` or `by: :position`"
                )));
            }
        },
        (Some(_), None) => {
            return Err(Error::Refused(format!(
                "{tree} is over a collection, and says what a key is: `by: :name` or \
                 `by: :position`"
            )));
        }
        // One entry, its key written in the name: `by:` is part of the
        // attribute's identity, so it is said here too.
        (None, Some(by)) => match by.keyword().and_then(Keyed::of) {
            Some(_) => (
                "entry",
                Some((
                    Syntax::Constant(dialog_corpus::Value::String(field.to_string())),
                    by,
                )),
            ),
            None => {
                return Err(Error::Refused(format!(
                    "{by} is not what a key is: `by: :name` or `by: :position`"
                )));
            }
        },
        (None, None) => (field, None),
    };
    let (entity, mut value) = match (operand.items.as_slice(), terms.remove("is")) {
        ([entity], Some(value)) => (entity, value),
        // Unlabeled, an attribute's two terms are the entity and then
        // the value, as Datomic writes `[?e :attr ?v]`.
        ([entity, value], None) => (entity, value.clone()),
        ([_], None) => {
            return Err(Error::Refused(format!(
                "{tree} says no value: a line is an attribute, its entity and its value, \
                 unlabeled or as `of:` and `is:`"
            )));
        }
        _ => {
            return Err(Error::Refused(format!(
                "{tree} needs its entity, under `of:` or with no label, and nothing else without a \
                 label: it is read as an attribute, since no concept is declared under its name"
            )));
        }
    };
    // An elector written on the value, `is: (max ?s)`, chooses one of
    // the line's claims, as one in the concept's head does.
    let mut elector = None;
    if let Some((word, inner)) = elected(&value) {
        elector = Some(word.to_string());
        value = inner.clone();
    }
    let mut cases = None;
    if let Some(kind) = terms.remove("as") {
        let word = match &kind {
            // A variant, `[:open :done]`: each of its cases is an entity,
            // listed least first.
            Syntax::Tuple(listed)
                if !listed.is_empty() && listed.iter().all(|case| case.keyword().is_some()) =>
            {
                cases = Some(kind.clone());
                "entity"
            }
            kind => kind
                .keyword()
                .filter(|word| TYPES.contains(word))
                .ok_or_else(|| {
                    Error::Refused(format!(
                        "{kind} is not a type: `as:` takes one of {}, or the cases of a variant, `[:open :done]`",
                        TYPES.map(|word| format!(":{word}")).join(" ")
                    ))
                })?,
        };
        value = wrap(word, value)?;
    }
    if let Some(cases) = cases {
        value = Syntax::Application(
            Box::new(Syntax::Constant(syntax::entity(&format!("{AMONG}:"))?)),
            Parameters {
                items: vec![value],
                terms: BTreeMap::from([("cases".into(), cases)]),
            },
        );
    }
    if let Some(cardinality) = terms.remove("cardinality") {
        let word = match cardinality.keyword() {
            Some(word @ ("one" | "many")) => word,
            _ => {
                return Err(Error::Refused(format!(
                    "{cardinality} is not a cardinality: `:one` or `:many`"
                )));
            }
        };
        value = wrap(word, value)?;
    }
    if let Some(elector) = elector {
        value = wrap(&elector, value)?;
    }
    if let Some(layer) = terms.remove("in") {
        crate::primitive::layer(&layer)?;
        value = match layer.keyword() {
            Some(CONTROL) => wrap(CONTROL, value)?,
            _ => Syntax::Application(
                Box::new(Syntax::Constant(syntax::entity(&format!("{IN}:"))?)),
                Parameters {
                    items: vec![value],
                    terms: BTreeMap::from([("at".into(), layer)]),
                },
            ),
        };
    }
    if let Some((key, by)) = key {
        value = Syntax::Application(
            Box::new(Syntax::Constant(syntax::entity("entry:")?)),
            Parameters {
                items: vec![value],
                terms: BTreeMap::from([("key".into(), key), ("by".into(), by)]),
            },
        );
    }
    if let Some(label) = terms.keys().next() {
        return Err(Error::Refused(format!(
            "{tree} says `{label}:`, which a line does not take: `of:`, `is:`, `as:` and `cardinality:`"
        )));
    }
    Ok(Some(Syntax::Application(
        Box::new(Syntax::Reference(domain.to_string())),
        Parameters {
            items: vec![entity.clone()],
            terms: BTreeMap::from([(field.to_string(), value)]),
        },
    )))
}

/// The read a derived attribute is: one named under a declared concept,
/// `list/count-open`, in that concept's own domain. It is a relation of
/// its own, whose domain is a hash of the concept's name and the
/// attribute's as written ([`syntax::derived`]), so it is one length and
/// one name for one concept and attribute whatever they spell. The build
/// has no identity of a concept to hash at this point: a concept is a
/// rule about syntax until its descriptor is compiled, and a hash of
/// that would rename every derived relation whenever a line of the
/// concept changed.
///
/// ```clj
/// (list/open ?l ?n)
/// (dialog.n.c.<hash of list, open> ?l is: ?n)
/// ```
///
/// Its two terms unlabeled are the entity and the value, under `is:`:
/// sugar for `of:` and `is:`, `(list/open of: ?l is: ?n)`, which reads
/// the same. With labels, it is the fields they name,
/// `(list/count ?l status: ?s count: ?n)`. A label has what the 64 bytes
/// of a relation leave beside the domain.
fn derived(
    tree: &Syntax,
    concept: &str,
    name: &str,
    domain: &str,
    reads: bool,
) -> Result<Syntax, Error> {
    let operand = tree.operand().cloned().unwrap_or_default();
    let (entity, terms) = match (operand.items.as_slice(), operand.terms.is_empty()) {
        ([entity, value], true) => (
            entity.clone(),
            BTreeMap::from([("is".into(), value.clone())]),
        ),
        ([entity], false) if !operand.terms.contains_key("of") => (entity.clone(), operand.terms),
        // `of:` is the entity written by its label.
        ([], false) if operand.terms.contains_key("of") => {
            let mut terms = operand.terms;
            let entity = terms.remove("of").unwrap_or(Syntax::Blank);
            (entity, terms)
        }
        _ => {
            return Err(Error::Refused(format!(
                "{tree} needs its entity and a value, or its entity, unlabeled or under `of:`, \
                 and labelled fields: it is read as an attribute in the domain of the concept \
                 `{concept}`, since no concept is declared under its name"
            )));
        }
    };
    // `as:` is the type of the value under `is:`, as on a line: the
    // primitive read wraps the value in it. A derived attribute's
    // cardinality is what its rules conclude, and no read or head says it.
    let mut terms = terms;
    if terms.contains_key("cardinality") {
        return Err(Error::Refused(format!(
            "{tree} says `cardinality:` of `{concept}/{name}`, a derived attribute, whose \
             cardinality is what its rules conclude: leave it out"
        )));
    }
    // `as:` a type, or a listed type's cases, is the type of the value
    // under `is:`; a map, of each field it names, `as: {:status :text}`.
    let types = || {
        crate::primitive::TYPES
            .iter()
            .map(|word| format!(":{word}"))
            .collect::<Vec<_>>()
            .join(" ")
    };
    let typed = |kind: &Syntax| -> Result<Syntax, Error> {
        if let Syntax::Tuple(listed) = kind
            && !listed.is_empty()
            && listed.iter().all(|case| case.keyword().is_some())
        {
            return Ok(kind.clone());
        }
        kind.keyword()
            .filter(|word| crate::primitive::TYPES.contains(word))
            .map(|_| kind.clone())
            .ok_or_else(|| {
                Error::Refused(format!(
                    "{tree} gives `as:` {kind}, and a type is one of {}, the cases of a listed \
                     type, `[:open :done]`, or a map of a label to its type, `{{:status :text}}`",
                    types()
                ))
            })
    };
    let mut typing: BTreeMap<String, Syntax> = BTreeMap::new();
    match terms.remove("as") {
        None => {}
        Some(Syntax::Map(entries)) => {
            for (label, kind) in &entries {
                // A map's key `:status` is read as the name `status`.
                let named = match label {
                    Syntax::Reference(name) if !name.contains(['/', '.']) => Some(name.as_str()),
                    other => other.keyword(),
                };
                let Some(label) = named else {
                    return Err(Error::Refused(format!(
                        "{tree} gives `as:` a map whose key {label} is no label: a key is a \
                         field the read names, `{{:status :text}}`"
                    )));
                };
                typing.insert(label.to_string(), typed(kind)?);
            }
        }
        Some(kind) => {
            let kind = typed(&kind)?;
            if !terms.contains_key("is") {
                return Err(Error::Refused(format!(
                    "{tree} gives `as:` and no `is:`: `as:` is the type of the value under \
                     `is:`; type a labelled field by its label, `as: {{:status {kind}}}`"
                )));
            }
            typing.insert("is".into(), kind);
        }
    }
    if let Some(label) = typing.keys().find(|label| !terms.contains_key(*label)) {
        return Err(Error::Refused(format!(
            "{tree} types `{label}:` under `as:`, which it does not name"
        )));
    }
    for (label, term) in terms.iter_mut() {
        // An elector on a term of a read, `is: (max ?n)`, chooses one of
        // the claims, as on a line, over the value as it is typed. In a
        // head that derives it is a fold.
        let (elector, mut value) = match elected(term).filter(|_| reads) {
            Some((word, inner)) => (Some(word.to_string()), inner.clone()),
            None => (None, term.clone()),
        };
        match typing.remove(label.as_str()) {
            Some(Syntax::Tuple(cases)) => {
                value = Syntax::Application(
                    Box::new(Syntax::Constant(syntax::entity(&format!("{AMONG}:"))?)),
                    Parameters {
                        items: vec![wrap("entity", value)?],
                        terms: BTreeMap::from([("cases".into(), Syntax::Tuple(cases))]),
                    },
                );
            }
            Some(kind) => {
                if let Some(word) = kind.keyword() {
                    value = wrap(word, value)?;
                }
            }
            None => {}
        }
        if let Some(word) = elector {
            value = wrap(&word, value)?;
        }
        *term = value;
    }
    let room = 64 - syntax::DERIVED_LENGTH - 1;
    if let Some(label) = terms.keys().find(|label| label.len() > room) {
        return Err(Error::Refused(format!(
            "{tree} derives `{concept}/{name}` with the label `{label}:`, of {} bytes: a derived \
             attribute's label has {room}",
            label.len()
        )));
    }
    Ok(Syntax::Application(
        Box::new(Syntax::Reference(domain.to_string())),
        Parameters {
            items: vec![entity],
            terms,
        },
    ))
}

/// A template that stands for reads, with every line in it as the read
/// it is, and the lines of one entity under one domain as one read of
/// several fields. Any other template is left as it is.
///
/// ```clj
/// (xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)
/// (xyz.todo/state of: ?i is: ?s as: :entity cardinality: :one)
///
/// (xyz.todo ?i state: (:one (:entity ?s)) title: (:one (:text ?t)))
/// ```
fn lines(template: Syntax) -> Result<Syntax, Error> {
    let mut any = false;
    // A body that holds what is neither a line nor a read, a read of
    // another concept, is left as it is; its lines still say their
    // cardinality.
    let mut other = false;
    let mut reads: Vec<Syntax> = Vec::new();
    for part in members(template.clone()) {
        let read = match line(&part)? {
            // A line of a concept or a command says its cardinality: a
            // field is one claim or many, and nothing chooses for it.
            Some(_)
                if !part
                    .operand()
                    .is_some_and(|operand| operand.terms.contains_key("cardinality")) =>
            {
                return Err(Error::Refused(format!(
                    "{part} says no cardinality: a line of a concept or a command writes \
                     `cardinality: :one` or `cardinality: :many`"
                )));
            }
            Some(read) => {
                any = true;
                read
            }
            None if part.is_read() => part,
            None => {
                other = true;
                continue;
            }
        };
        // Two blanks are two entities, so a read of a blank stays apart.
        let fits = |earlier: &Syntax| {
            earlier.applies() == read.applies()
                && items(earlier) == items(&read)
                && items(&read) != [Syntax::Blank]
                && match (earlier.operand(), read.operand()) {
                    (Some(earlier), Some(read)) => read
                        .terms
                        .keys()
                        .all(|label| !earlier.terms.contains_key(label)),
                    _ => false,
                }
        };
        match (
            reads.iter_mut().find(|earlier| fits(earlier)),
            read.operand(),
        ) {
            (Some(Syntax::Application(_, earlier)), Some(read)) => {
                earlier.terms.extend(read.terms.clone());
            }
            _ => reads.push(read),
        }
    }
    Ok(match (any, reads.len()) {
        _ if other => template,
        (false, _) => template,
        (true, 1) => reads.remove(0),
        (true, _) => apply("all", reads),
    })
}

/// Whether `part` is written as a line of a concept or a command, not a
/// read: by a word only a line says, `as:`, `in:`, `cardinality:` or
/// `by:`. (`of:` is read as the part with no label, so it says nothing a
/// read does not.)
fn is_line(part: &Syntax) -> bool {
    part.operand().is_some_and(|operand| {
        ["as", "in", "cardinality", "by"]
            .iter()
            .any(|word| operand.terms.contains_key(*word))
    })
}

/// `rule` with every variable an expansion named for itself, `?c.<site>`,
/// written `_` in the premises where it occurs once in the whole rule: it
/// joins nothing, and `_` is how one that joins nothing is written, so a
/// rule stores alike whoever wrote it (the managing agent's reading). A
/// variable the author wrote is never changed. Heads are left as they are:
/// a field a head leaves out is told apart by its variable. A query is
/// stored nowhere, and its report reads the entity its premise binds, so
/// it is left as it is too.
fn blanked(rule: Syntax) -> Syntax {
    let mut clauses = Clauses::of(&rule);
    if clauses
        .head
        .as_ref()
        .and_then(Syntax::applies)
        .is_some_and(|word| matches!(word, "syntax" | "warning" | "report"))
    {
        return rule;
    }
    let mut all = Vec::new();
    rule.variables(&mut all);
    let once: BTreeSet<String> = all
        .iter()
        .filter(|name| {
            syntax::unwritten(name) && all.iter().filter(|other| other == name).count() == 1
        })
        .cloned()
        .collect();
    if once.is_empty() {
        return rule;
    }
    fn walk(tree: &Syntax, once: &BTreeSet<String>) -> Syntax {
        match tree {
            Syntax::Variable(name) if once.contains(name) => Syntax::Blank,
            Syntax::Application(operator, operand) => Syntax::Application(
                Box::new(walk(operator, once)),
                Parameters {
                    items: operand.items.iter().map(|item| walk(item, once)).collect(),
                    terms: operand
                        .terms
                        .iter()
                        .map(|(label, term)| (label.clone(), walk(term, once)))
                        .collect(),
                },
            ),
            Syntax::Tuple(items) => {
                Syntax::Tuple(items.iter().map(|item| walk(item, once)).collect())
            }
            Syntax::Map(entries) => Syntax::Map(
                entries
                    .iter()
                    .map(|(key, value)| (walk(key, once), walk(value, once)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }
    clauses.when = clauses
        .when
        .iter()
        .map(|premise| walk(premise, &once))
        .collect();
    clauses.unless = clauses
        .unless
        .iter()
        .map(|premise| walk(premise, &once))
        .collect();
    clauses.into_rule()
}

/// Whether `tree` is a value wrapped as the primitive forms read it: in a type,
/// a pick, a cardinality or a layer, `(:integer ?n)`, or in a listed
/// type's cases, `(:among ?s cases: [:open :done])`.
fn wrapper(tree: &Syntax) -> bool {
    match tree {
        Syntax::Application(operator, operand) => match operator.keyword() {
            Some(AMONG) => {
                operand.items.len() == 1
                    && operand.terms.len() == 1
                    && operand.terms.contains_key("cases")
            }
            Some(IN) => {
                operand.items.len() == 1
                    && operand.terms.len() == 1
                    && operand.terms.contains_key("at")
            }
            Some(word) => {
                (TYPES.contains(&word)
                    || PICKS.contains(&word)
                    || CARDINALITIES.contains(&word)
                    || word == CONTROL)
                    && operand.items.len() == 1
                    && operand.terms.is_empty()
            }
            None => false,
        },
        _ => false,
    }
}

/// A value with the wrappers [`wrapper`] tells taken off it.
struct Peeled<'a> {
    kind: Option<&'a str>,
    pick: Option<&'a str>,
    cardinality: Option<&'a str>,
    /// The layer as a line writes it after `in:`: `:control`, `?l`.
    layer: Option<String>,
    cases: Option<&'a Syntax>,
    inner: &'a Syntax,
}

impl<'a> Peeled<'a> {
    fn of(mut node: &'a Syntax) -> Self {
        let mut peeled = Peeled {
            kind: None,
            pick: None,
            cardinality: None,
            layer: None,
            cases: None,
            inner: node,
        };
        while wrapper(node) {
            let Syntax::Application(operator, operand) = node else {
                break;
            };
            let (Some(word), [inner]) = (operator.keyword(), operand.items.as_slice()) else {
                break;
            };
            if word == AMONG {
                peeled.cases = peeled.cases.or(operand.terms.get("cases"));
                node = inner;
                continue;
            }
            if word == IN {
                peeled.layer = peeled
                    .layer
                    .or_else(|| operand.terms.get("at").map(ToString::to_string));
                node = inner;
                continue;
            }
            let slot = if TYPES.contains(&word) {
                &mut peeled.kind
            } else if PICKS.contains(&word) {
                &mut peeled.pick
            } else if CARDINALITIES.contains(&word) {
                &mut peeled.cardinality
            } else {
                peeled.layer.get_or_insert_with(|| format!(":{word}"));
                node = inner;
                continue;
            };
            slot.get_or_insert(word);
            node = inner;
        }
        peeled.inner = node;
        peeled
    }

    /// The cardinality said, or the one a pick of every claim or the
    /// newest says.
    fn cardinality(&self) -> Option<&'a str> {
        self.cardinality.or(match self.pick {
            Some("last") => Some("one"),
            Some("all") => Some("many"),
            _ => None,
        })
    }

    /// The value, under the elector it is written under.
    fn value(&self) -> String {
        match self.pick {
            Some(elector @ ("max" | "min")) => format!("({elector} {})", self.inner),
            _ => self.inner.to_string(),
        }
    }

    /// The line of `attribute` of `entity` that says this value.
    fn line(&self, attribute: &str, entity: &str) -> String {
        let mut line = format!("({attribute} of: {entity} is: {}", self.value());
        match (self.cases, self.kind) {
            (Some(cases), _) => line.push_str(&format!(" as: {cases}")),
            (None, Some(kind)) => line.push_str(&format!(" as: :{kind}")),
            (None, None) => {}
        }
        if let Some(cardinality) = self.cardinality() {
            line.push_str(&format!(" cardinality: :{cardinality}"));
        }
        if let Some(layer) = &self.layer {
            line.push_str(&format!(" in: {layer}"));
        }
        line.push(')');
        line
    }
}

/// The parts with no label of an application, or none.
fn items(tree: &Syntax) -> &[Syntax] {
    tree.operand().map_or(&[], |operand| &operand.items)
}

/// Whether a term is read on the control layer: `(:control …)`.
fn controlled(term: &Syntax) -> bool {
    matches!(term, Syntax::Application(operator, _) if operator.keyword() == Some(CONTROL))
}

/// Whether a read is of claims on the control layer: every labelled
/// part of it `(:control …)`.
fn on_control(tree: &Syntax) -> bool {
    tree.is_read()
        && tree.operand().is_some_and(|operand| {
            !operand.terms.is_empty() && operand.terms.values().all(controlled)
        })
}

/// The parts of a primitive rule.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Clauses {
    /// What the rule says about itself: its labelled parts.
    pub said: BTreeMap<String, Syntax>,
    /// Its head: the first part.
    pub head: Option<Syntax>,
    /// Its premises: what must hold.
    pub when: Vec<Syntax>,
    /// What `(unless …)` holds: what must not.
    pub unless: Vec<Syntax>,
}

impl Clauses {
    /// The parts of a `(rule …)` tree: its first part is its head, every
    /// part after it is a premise, and what `(unless …)` holds must not
    /// hold.
    pub fn of(rule: &Syntax) -> Self {
        let mut clauses = Clauses::default();
        let Some(operand) = rule.operand() else {
            return clauses;
        };
        clauses.said = operand.terms.clone();
        let Some((head, premises)) = operand.items.split_first() else {
            return clauses;
        };
        clauses.head = Some(head.clone());
        for premise in premises {
            if premise.applies() == Some("unless") {
                clauses.unless.extend(items(premise).iter().cloned());
            } else {
                clauses.when.push(premise.clone());
            }
        }
        clauses
    }

    /// These parts as a `(rule …)` tree.
    pub fn into_rule(self) -> Syntax {
        let mut parts: Vec<Syntax> = self.head.into_iter().collect();
        parts.extend(self.when);
        if !self.unless.is_empty() {
            parts.push(apply("unless", self.unless));
        }
        Syntax::Application(
            Box::new(Syntax::Reference("rule".into())),
            Parameters {
                items: parts,
                terms: self.said,
            },
        )
    }
}

/// The trees a tree stands for: itself, or what `(all …)` holds.
fn members(tree: Syntax) -> Vec<Syntax> {
    if tree.applies() == Some("all") {
        items(&tree).iter().cloned().flat_map(members).collect()
    } else {
        vec![tree]
    }
}

/// `tree` without a trailing `...` or `... _`, and whether it had one.
fn open(tree: &Syntax) -> (Syntax, bool) {
    let dots = |part: &Syntax| matches!(part, Syntax::Reference(name) if name == REST);
    let Syntax::Application(operator, operand) = tree else {
        return (tree.clone(), false);
    };
    let kept = match operand.items.as_slice() {
        [items @ .., last] if dots(last) => items,
        [items @ .., last, Syntax::Blank] if dots(last) => items,
        _ => return (tree.clone(), false),
    };
    (
        Syntax::Application(
            operator.clone(),
            Parameters {
                items: kept.to_vec(),
                terms: operand.terms.clone(),
            },
        ),
        true,
    )
}

/// How many parts of `said` the rule's `pattern` leaves to a `...`: at
/// the top, the parts it does not name, whether its `...` names them or
/// not; below, what each repeat took. A rule that names more leaves
/// fewer.
fn left(pattern: &Syntax, said: &Syntax, bindings: &Bindings) -> usize {
    let mut rest: Vec<String> = Vec::new();
    let mut top = 0;
    if let (Some(pattern), Some(said)) = (pattern.operand(), said.operand())
        && let Some(at) = pattern
            .items
            .iter()
            .position(|part| matches!(part, Syntax::Reference(name) if name == REST))
    {
        if let Some(Syntax::Variable(name)) = pattern.items.get(at + 1) {
            rest.push(name.clone());
            rest.push(crate::pattern::row_key(name));
        }
        top = said.items.len().saturating_sub(at)
            + said
                .terms
                .keys()
                .filter(|label| !pattern.terms.contains_key(*label))
                .count();
    }
    let below: usize = bindings
        .iter()
        .filter(|(name, _)| !rest.contains(name))
        .map(|(_, binding)| match binding {
            Binding::Many(taken) => taken.len(),
            Binding::Row(taken) => taken.len(),
            Binding::One(_) => 0,
        })
        .sum();
    top + below
}

/// Whether `tree` is written as a form may be, and otherwise the part
/// that is not and why. The reader gives labels and `...` no meaning,
/// so what they may not say is said here.
///
/// A form with a label written is one record, and its one part with no
/// label is the record's `of:`. So beside labels a second part with no
/// label is refused, and so is `of:` beside a part with none. A view
/// called in a template names its concept and its view first, and those
/// two are not counted. `...` and
/// what follows it are the parts not named, and no part of their own.
fn valid(tree: &Syntax) -> Result<(), Box<(Syntax, Error)>> {
    let refuse = |why: String| Err(Box::new((tree.clone(), Error::Refused(why))));
    match tree {
        Syntax::Reference(name) if name.starts_with(REST) && name != REST => refuse(format!(
            "`{name}`: `...` stands apart from what follows it, `... {}`",
            &name[REST.len()..]
        )),
        // What is quoted is not a form that says anything.
        Syntax::Application(..) if tree.applies() == Some(QUOTE) => Ok(()),
        Syntax::Application(operator, operand) => {
            // What is inside first, so the part that is refused is the
            // innermost that is.
            valid(operator)?;
            operand
                .items
                .iter()
                .chain(operand.terms.values())
                .try_for_each(valid)?;
            // A view called in a template names its concept and its view
            // first, `(view todo :item list: ?l)`: after those two, a part
            // with no label is the entity it is called for, as `of:` is.
            let called = usize::from(tree.applies() == Some("view")) * 2;
            let named = operand
                .items
                .iter()
                .take_while(|item| !matches!(item, Syntax::Reference(name) if name == REST))
                .count()
                .saturating_sub(called);
            if !operand.terms.is_empty() && named > 1 {
                return refuse(format!(
                    "{tree} has {named} parts with no label beside its labelled parts: the \
                     labelled parts are one record, and its one part with no label is its `of:`"
                ));
            }
            if named > 0 && operand.terms.contains_key("of") {
                return refuse(format!(
                    "{tree} says `of:` twice: the part with no label is its `of:`"
                ));
            }
            Ok(())
        }
        Syntax::Tuple(items) => items.iter().try_for_each(valid),
        Syntax::Map(entries) => entries.iter().try_for_each(|(key, value)| {
            valid(key)?;
            valid(value)
        }),
        _ => Ok(()),
    }
}

/// `tree` with the variables an expansion at `site` named for itself
/// renamed by where each first occurs, so two answers that differ only
/// in what a definition called its own variables are equal.
fn unnamed(tree: &Syntax, site: &str) -> Syntax {
    fn walk(tree: &Syntax, suffix: &str, seen: &mut Vec<String>) -> Syntax {
        match tree {
            Syntax::Variable(name) if name.ends_with(suffix) => {
                let at = seen
                    .iter()
                    .position(|other| other == name)
                    .unwrap_or_else(|| {
                        seen.push(name.clone());
                        seen.len() - 1
                    });
                Syntax::Variable(format!("{at}{suffix}"))
            }
            Syntax::Tuple(items) => {
                Syntax::Tuple(items.iter().map(|item| walk(item, suffix, seen)).collect())
            }
            Syntax::Map(entries) => Syntax::Map(
                entries
                    .iter()
                    .map(|(key, value)| (walk(key, suffix, seen), walk(value, suffix, seen)))
                    .collect(),
            ),
            Syntax::Application(operator, operand) => {
                let operator = walk(operator, suffix, seen);
                let items = operand
                    .items
                    .iter()
                    .map(|item| walk(item, suffix, seen))
                    .collect();
                let terms = operand
                    .terms
                    .iter()
                    .map(|(label, term)| (label.clone(), walk(term, suffix, seen)))
                    .collect();
                Syntax::Application(Box::new(operator), Parameters { items, terms })
            }
            other => other.clone(),
        }
    }
    walk(tree, &format!("[{site}]"), &mut Vec::new())
}

/// Several reads of one entity as one read, so that their absence is
/// the absence of all of them together. Dialog negates one proposition,
/// and a read of one entity may span domains.
fn together(mut trees: Vec<Syntax>, written: &Syntax) -> Result<Syntax, Error> {
    if trees.len() == 1 {
        return Ok(trees.remove(0));
    }
    let spans = || {
        Error::Refused(format!(
            "the absence of {written} is not one read: dialog negates one proposition, and this spans entities"
        ))
    };
    let entity = items(&trees[0]).first().cloned();
    if !trees
        .iter()
        .all(|tree| tree.is_read() && items(tree).first() == entity.as_ref())
    {
        return Err(spans());
    }
    let domain = trees[0].applies().unwrap_or_default().to_string();
    let mut operand = Parameters {
        items: entity.into_iter().collect(),
        ..Parameters::default()
    };
    for tree in &trees {
        let other = tree.applies().unwrap_or_default();
        for (label, term) in tree
            .operand()
            .into_iter()
            .flat_map(|operand| &operand.terms)
        {
            let whole = if label.contains('/') || other == domain {
                label.clone()
            } else {
                format!("{other}/{label}")
            };
            if let Some(said) = operand.terms.insert(whole.clone(), term.clone())
                && said != *term
            {
                return Err(Error::Refused(format!(
                    "the absence of {written} says `{whole}:` twice"
                )));
            }
        }
    }
    Ok(Syntax::Application(
        Box::new(Syntax::Reference(domain)),
        operand,
    ))
}

/// The walker, with the rules about syntax it has been given.
#[derive(Clone, Debug, Default)]
pub struct Expander {
    syntax: Vec<Definition>,
    /// The names some rule about syntax is for.
    names: BTreeSet<String>,
    /// The names of the concepts declared. A name declared wins over
    /// reading it as an attribute: `todo/add`.
    concepts: BTreeSet<String>,
    /// The commands declared, each by its signature, with the domain
    /// its claims are in. Two concepts of one name and other labels are
    /// two signatures, so neither displaces the other.
    commands: BTreeMap<Signature, String>,
    /// How many rules about syntax have been kept, ever: what tells a
    /// pass over a document that it declared something.
    declared: usize,
    /// The queries defined, each as it was written, by name.
    queries: BTreeMap<String, Syntax>,
    /// The concepts that hold a collection, each by its signature, with
    /// the label of the field that is one entry of it, `block: {?k ?b}`.
    entries: BTreeMap<Signature, String>,
    /// The domains some installed rule reads.
    reads: BTreeSet<String>,
    /// Every form the rules were asked about since the last report,
    /// and which tree of the document it was asked for.
    asked: Vec<(Syntax, Position, usize)>,
    /// The trees of the document being expanded, as given.
    document: Vec<Syntax>,
    /// What the document being expanded defines, each with the tree
    /// that defines it: a concept by its signature and by its name, a
    /// query by its name.
    defining: BTreeMap<String, usize>,
    /// The pairs of the document's trees that define one name, the one
    /// met first and the other: what the document means would depend on
    /// the order they are written in.
    pub(crate) twice: Vec<(usize, usize, Error)>,
    /// Trees of the document left out, each refused as it says.
    pub(crate) skipping: BTreeMap<usize, Error>,
    /// The names each tree of the document read as an attribute's, by
    /// the tree: one the document declares as a concept after that tree
    /// was expanded was read as another relation than the one meant.
    misread: Vec<(usize, String)>,
    /// Each name read or concluded as a derived attribute, `todo/tag`,
    /// with each use: the document, by its number, the form in it, by
    /// its place, and the form as written. A later document that declares
    /// the name a concept would leave the rules installed reading another
    /// relation than its own text does. A use is kept only while its form
    /// is: one refused is forgotten, as it installed nothing.
    derived_uses: BTreeMap<String, Vec<(usize, usize, String)>>,
    /// Each derived attribute a rule concludes, `list/label`, with each
    /// conclusion: the document, the form in it, and the labels its head
    /// writes. What a read of a label no conclusion writes is warned of
    /// by. A conclusion is kept only while its form is.
    derived_concluded: BTreeMap<String, Vec<(usize, usize, BTreeSet<String>)>>,
    /// The documents checked so far, this one among them.
    documents: usize,
    /// The sites of the tree being expanded, each by its reference,
    /// numbered in the order they are met: what a site's own variables
    /// are named apart by. Begun again for each tree, so the numbers
    /// are a function of that tree alone.
    sites: BTreeMap<String, usize>,
    /// Which of them is being expanded.
    tree: usize,
    /// For each primitive rule of the last document, which tree it came of.
    origin: Vec<usize>,
    /// Each tree of the last document a primitive rule of which is the same
    /// as one an earlier tree came to, with that tree: the rule is kept
    /// once, at the earlier tree.
    repeats: Vec<(usize, usize)>,
    /// The parts of the tree being expanded that are being worked on,
    /// outermost first: what a refusal is about.
    blame: Vec<Syntax>,
    /// The forms several definitions take in the rule being expanded.
    choices: Vec<Choice>,
    /// Whether the head being expanded reports, and writes nothing.
    reporting: bool,
    /// Whether the premise being expanded is one that must not hold.
    negating: bool,
    /// The forms rewritten on the way to the one being expanded, from
    /// the top-level form, each with the pattern of the definition that
    /// rewrote it: a form met again on it is a cycle.
    path: Vec<(Syntax, String)>,
    /// The pattern of the definition the last form asked about was
    /// rewritten by.
    answered: String,
    /// How many parts one form of the top-level tree being expanded may
    /// expand to.
    largest: usize,
    /// Whether the prelude has been read: what is installed after it is
    /// a document's.
    prelude_read: bool,
}

impl Expander {
    /// Why nothing says what `tree` is. Where it applies, in a derived
    /// attribute's shape, `(list/open ?l ?n)` or `(list/open ?l is: ?n)`,
    /// a concept declared under a name whose part before its last `/` is
    /// a concept too, it is written as that concept's derived attribute,
    /// and a name is a concept or a derived attribute of one, never both:
    /// the refusal names the use and the declaration.
    fn unfitting(&self, tree: &Syntax) -> Error {
        // A derived attribute's own shapes: its entity and its value,
        // unlabeled or the value under `is:`.
        let derived = tree.operand().is_some_and(|operand| {
            match (operand.items.len(), operand.terms.len()) {
                (2, 0) => true,
                (1, 1) => operand.terms.contains_key("is"),
                _ => false,
            }
        });
        let shared = tree.applies().filter(|_| derived).and_then(|name| {
            let (concept, _) = name.rsplit_once('/')?;
            (self.concepts.contains(name) && self.concepts.contains(concept))
                .then_some((name, concept))
        });
        let Some((name, concept)) = shared else {
            return Error::Unknown(tree.to_string());
        };
        let declared = self
            .defining
            .get(&format!("concept {name}"))
            .and_then(|at| self.document.get(*at))
            .map_or_else(
                || format!("an earlier document declares `{name}` a concept"),
                |form| format!("{form} declares `{name}` a concept"),
            );
        Error::Refused(format!(
            "{tree} is written as a derived attribute of the concept `{concept}`, and {declared}: \
             a name is a concept or a derived attribute of one, never both, so name the derived \
             attribute another way, or read the concept by its fields"
        ))
    }

    /// The concepts declared under `name` that a use written with
    /// `labels` fits: each has every label written, and, unless the use
    /// ends with `...`, no other. Two concepts of one
    /// name and other labels are told apart by what a use writes, never
    /// by the order they were declared in.
    fn fitting(&self, name: &str, labels: &BTreeSet<String>, dotted: bool) -> Vec<&Signature> {
        let written = Signature {
            name: name.to_string(),
            subject: "concept".into(),
            parameters: labels.clone(),
        };
        let mut found: Vec<&Signature> = self
            .syntax
            .iter()
            .filter_map(|rule| rule.signature.as_ref())
            .filter(|signature| signature.takes(&written))
            .filter(|signature| dotted || signature.parameters == written.parameters)
            .collect();
        found.sort();
        found.dedup();
        found
    }

    /// The concepts under `name` a use written with `labels` fits, each
    /// as `said` to name them in a refusal.
    fn ambiguous(said: String, fitting: &[&Signature]) -> Error {
        Error::Ambiguous(
            said,
            fitting
                .iter()
                .map(|signature| {
                    format!(
                        "({} {})",
                        signature.name,
                        signature
                            .parameters
                            .iter()
                            .map(|label| format!("{label}:"))
                            .collect::<Vec<_>>()
                            .join(" ")
                    )
                })
                .collect::<Vec<_>>()
                .join(" | "),
        )
    }

    /// A walker that knows the core forms.
    pub fn new() -> Result<Self, Error> {
        let mut expander = Expander::default();
        expander.submit(PRELUDE)?;
        expander.prelude_read = true;
        Ok(expander)
    }

    /// Parse `text` and expand every tree of it. A rule about syntax is
    /// kept here; every other comes back as primitive rules, in the order
    /// written, with what the rules said about the document. A tree
    /// whose head stands for several comes back as a rule each.
    pub fn submit(&mut self, text: &str) -> Result<(Vec<Syntax>, Vec<Diagnostic>), Error> {
        self.expand(parse(text)?)
    }

    /// Expand every tree of `trees`, as [`submit`](Self::submit) does
    /// the trees a text parses to. The first refusal is the error.
    pub fn expand(&mut self, trees: Vec<Syntax>) -> Result<(Vec<Syntax>, Vec<Diagnostic>), Error> {
        let (primitive, diagnostics) = self.check(trees);
        match diagnostics
            .iter()
            .find_map(|diagnostic| diagnostic.refusal.clone())
        {
            Some(error) => Err(error),
            None => Ok((primitive, diagnostics)),
        }
    }

    /// Expand every tree of `trees` and say everything there is to say
    /// about them. A tree that is refused gives no rule and a
    /// diagnostic that holds the refusal, and the trees after it are
    /// expanded all the same. The refusals come first, in the order of
    /// their trees, and then what the rules said without refusing.
    pub fn check(&mut self, trees: Vec<Syntax>) -> (Vec<Syntax>, Vec<Diagnostic>) {
        self.documents += 1;
        self.asked.clear();
        self.origin.clear();
        self.repeats.clear();
        self.defining.clear();
        self.twice.clear();
        self.misread.clear();
        self.document = trees.clone();
        // The order of a document's forms does not matter. What declares
        // a name is expanded before anything that may use it: a concept
        // or a rule about syntax first, then a query, which reads
        // concepts, then the rest. A query expanded before a concept it
        // reads would read the concept's name as an attribute's, and
        // nothing would expand it again. The use of a macro may declare
        // a concept, and is expanded among the rest in the order
        // written: a query below one is expanded after it, with the rest,
        // so that it reads what the use declares above it.
        let core = |tree: &Syntax| {
            matches!(
                tree.applies(),
                Some("concept" | "rule" | "query" | "select" | "assert" | "retract")
            )
        };
        let first_use = trees.iter().position(|tree| !core(tree));
        let rank = |at: usize| {
            let tree = &trees[at];
            match tree.applies() {
                Some("concept") => 0,
                Some("rule")
                    if items(tree)
                        .first()
                        .and_then(Syntax::applies)
                        .is_some_and(|word| matches!(word, "syntax" | "warning")) =>
                {
                    0
                }
                Some("query") if first_use.is_none_or(|first| at < first) => 1,
                _ => 2,
            }
        };
        let skipping = std::mem::take(&mut self.skipping);
        let mut pending: Vec<usize> = (0..trees.len())
            .filter(|at| !skipping.contains_key(at))
            .collect();
        pending.sort_by_key(|at| rank(*at));
        concepts_in_order(&mut pending, &trees);
        let mut done: BTreeMap<usize, Vec<Syntax>> = BTreeMap::new();
        let mut refused: BTreeMap<usize, Diagnostic> = skipping
            .into_iter()
            .map(|(at, error)| {
                let refusal =
                    Diagnostic::refused(trees.get(at).cloned(), vec![Segment::Item(at)], error);
                (at, refusal)
            })
            .collect();
        // A form a macro expands to may declare a name too. Whatever
        // failed before it is expanded again once it has, until nothing
        // more is declared. A name two such expansions need of each
        // other is refused as whatever the first one met.
        for _ in 0..=trees.len() {
            let declared = self.declared;
            let mut failed = Vec::new();
            for at in pending {
                let tree = &trees[at];
                self.tree = at;
                self.misread.retain(|(read, _)| *read != at);
                self.blame.clear();
                self.sites.clear();
                let mut rules = Vec::new();
                let expanded = self
                    .written(tree)
                    .and_then(|()| self.each(tree, &mut rules))
                    .and_then(|()| {
                        // What no rule may write, by the test `compile`
                        // makes of a tree however it was built.
                        match rules.iter().find_map(crate::compile::write_refusal) {
                            Some(refused) => self.about(tree, |_| Err(refused)),
                            None => Ok(()),
                        }
                    });
                match expanded {
                    Ok(()) => {
                        refused.remove(&at);
                        done.insert(at, rules);
                    }
                    Err(error) => {
                        // The refusal is about the innermost part being
                        // worked on that is a part of the tree as written.
                        let (form, inside) = self
                            .blame
                            .iter()
                            .rev()
                            .find_map(|part| Some((part.clone(), tree.locate(part)?)))
                            .unwrap_or((tree.clone(), Path::new()));
                        let mut path = vec![Segment::Item(at)];
                        path.extend(inside);
                        refused.insert(at, Diagnostic::refused(Some(form), path, error));
                        failed.push(at);
                    }
                }
            }
            pending = failed;
            if pending.is_empty() || self.declared == declared {
                break;
            }
        }
        // A tree that read as an attribute's a name the document declared
        // as a concept after it read another relation than the concept:
        // it is refused, saying to declare the concept above it. The tree
        // that declares the name is not.
        let misread: Vec<(usize, String)> = self
            .misread
            .iter()
            .filter(|(at, name)| {
                self.concepts.contains(name)
                    && self.defining.get(&format!("concept {name}")) != Some(at)
            })
            .cloned()
            .collect();
        for (at, name) in misread {
            if done.remove(&at).is_some() {
                let error = Error::Refused(format!(
                    "{} reads `{name}`, or a name under it, as an attribute's, and the document \
                     declares `{name}` as a concept after it: declare the concept above what \
                     reads it",
                    trees.get(at).map(ToString::to_string).unwrap_or_default()
                ));
                refused.insert(
                    at,
                    Diagnostic::refused(trees.get(at).cloned(), vec![Segment::Item(at)], error),
                );
            }
        }
        // A rule, a concept or a query written again installs nothing
        // new, and is kept once. A write written again writes again: a
        // document is a sequence, so each is kept in its place.
        let mut primitives = Vec::new();
        for (at, rules) in done {
            for rule in rules {
                match primitives.iter().position(|kept| *kept == rule) {
                    Some(first) if !writes(&rule) => {
                        let earlier = self.origin[first];
                        if earlier != at && !self.repeats.contains(&(at, earlier)) {
                            self.repeats.push((at, earlier));
                        }
                    }
                    _ => {
                        primitives.push(rule);
                        self.origin.push(at);
                    }
                }
            }
        }
        let reads = self.conclusions(&primitives);
        let forgotten: BTreeSet<usize> = refused.keys().copied().collect();
        self.forget_uses(&forgotten);
        let mut diagnostics: Vec<Diagnostic> = refused.into_values().collect();
        diagnostics.extend(self.diagnose());
        diagnostics.extend(self.unconcluded(reads, &trees));
        (primitives, diagnostics)
    }

    /// Forget the derived uses the forms `refused` of the document being
    /// checked made: they installed nothing.
    pub(crate) fn forget_uses(&mut self, refused: &BTreeSet<usize>) {
        let document = self.documents;
        self.derived_uses.retain(|_, uses| {
            uses.retain(|(at, tree, _)| *at != document || !refused.contains(tree));
            !uses.is_empty()
        });
        self.derived_concluded.retain(|_, concluded| {
            concluded.retain(|(at, tree, _)| *at != document || !refused.contains(tree));
            !concluded.is_empty()
        });
    }

    /// Record what each rule of `primitive` concludes of a derived
    /// attribute, and give each read of one its premises make: the form
    /// it came of, the derived attribute, and the labels it reads.
    fn conclusions(&mut self, primitive: &[Syntax]) -> Vec<(usize, String, BTreeSet<String>)> {
        let derived = |tree: &Syntax| -> Option<(String, BTreeSet<String>)> {
            let name = tree.applies()?.strip_prefix(syntax::DERIVED_READ)?;
            let labels = tree
                .operand()
                .map(|operand| operand.terms.keys().cloned().collect())
                .unwrap_or_default();
            Some((name.to_string(), labels))
        };
        let mut reads = Vec::new();
        for (rule, at) in primitive.iter().zip(&self.origin) {
            let Some(operand) = rule.operand() else {
                continue;
            };
            let Some((head, premises)) = operand.items.split_first() else {
                continue;
            };
            if let Some((name, labels)) = derived(head) {
                self.derived_concluded
                    .entry(name)
                    .or_default()
                    .push((self.documents, *at, labels));
            }
            for premise in premises {
                let parts: Vec<&Syntax> = if premise.applies() == Some("unless") {
                    premise
                        .operand()
                        .map(|operand| operand.items.iter().collect())
                        .unwrap_or_default()
                } else {
                    vec![premise]
                };
                for part in parts {
                    if let Some((name, labels)) = derived(part) {
                        reads.push((*at, name, labels));
                    }
                }
            }
        }
        reads
    }

    /// A warning of each of `reads` that names a label no conclusion of
    /// its derived attribute this expander knows writes, when it knows
    /// one: a rule it does not know, one on the branch, may conclude it,
    /// so it is never refused, and with none known nothing is said.
    fn unconcluded(
        &self,
        reads: Vec<(usize, String, BTreeSet<String>)>,
        trees: &[Syntax],
    ) -> Vec<Diagnostic> {
        let mut said: Vec<Diagnostic> = Vec::new();
        for (at, name, labels) in reads {
            let Some(concluded) = self.derived_concluded.get(&name) else {
                continue;
            };
            let known: BTreeSet<&String> =
                concluded.iter().flat_map(|(_, _, labels)| labels).collect();
            let unknown: Vec<String> = labels
                .iter()
                .filter(|label| !known.contains(label))
                .map(|label| format!("`{label}:`"))
                .collect();
            if unknown.is_empty() {
                continue;
            }
            let concludes: Vec<String> = known.iter().map(|label| format!("`{label}:`")).collect();
            let diagnostic = Diagnostic {
                form: trees.get(at).cloned(),
                message: format!(
                    "a read of `{name}` names {}, which no rule known here concludes of it: they \
                     conclude {}. A rule on the branch may; if not, the read answers nothing",
                    unknown.join(", "),
                    concludes.join(", ")
                ),
                path: vec![Segment::Item(at)],
                span: None,
                refusal: None,
            };
            if !said.contains(&diagnostic) {
                said.push(diagnostic);
            }
        }
        said
    }

    /// Say the tree being expanded defines `key`: refused, naming both,
    /// where another tree of the document defines it already.
    pub(crate) fn defines(&mut self, key: String, name: &str) -> Result<(), Error> {
        match self.defining.get(&key) {
            Some(&at) if at != self.tree => {
                let form = |at: usize| {
                    self.document
                        .get(at)
                        .map(ToString::to_string)
                        .unwrap_or_default()
                };
                let error = Error::Twice(name.to_string(), form(at), form(self.tree));
                self.twice.push((at, self.tree, error.clone()));
                Err(error)
            }
            Some(_) => Ok(()),
            None => {
                self.defining.insert(key, self.tree);
                Ok(())
            }
        }
    }

    /// Refuse a tree of a document that names what the notation names
    /// itself, about the part that does.
    fn written(&mut self, tree: &Syntax) -> Result<(), Error> {
        if let Some((part, name)) = reserved(tree) {
            let part = part.clone();
            return self.about(&part, |_| Err(Error::Reserved(name)));
        }
        if let Some(part) = collected_as_before(tree) {
            let part = part.clone();
            return self.about(&part, |_| {
                Err(Error::Refused(format!(
                    "{part}: `collect` is written `distinct`, `(distinct ?x)`: what a read \
                     gathers is the set of the distinct values the field takes, Datomic's word"
                )))
            });
        }
        if let Some((part, name)) = malformed(tree) {
            let part = part.clone();
            let why = syntax::unkept(&name).unwrap_or_else(|| {
                "a name is a word, or words with a single `/` between each two, `todo`, \
                 `notebook/blocks`, `nb/blocks/count`, and an attribute a domain, a `/` and a \
                 word, `xyz.todo/text`"
                    .to_string()
            });
            let message = format!("{part} names `{name}`, which is no name: {why}");
            return self.about(&part, |_| Err(Error::Refused(message)));
        }
        if let Some((part, label, attribute)) = overlong(tree) {
            let part = part.clone();
            let message = format!(
                "{part} has the label `{label}:`, and `{label}:` makes the attribute \
                 `{attribute}`, which is {} bytes, and an attribute is at most {}",
                attribute.len(),
                syntax::RELATION_LENGTH
            );
            return self.about(&part, |_| Err(Error::Refused(message)));
        }
        if let Some((part, label)) = slashed(tree) {
            let part = part.clone();
            // A field of the notation's record of descriptions is the
            // notation's to write, as `compile` says of it.
            if label
                .split_once('/')
                .is_some_and(|(domain, _)| domain == crate::compile::DESCRIBED)
            {
                return self.about(&part, |_| Err(Error::Reserved(label)));
            }
            let (domain, field) = label.rsplit_once('/').unwrap_or((&label, ""));
            let message = format!(
                "{part} has the label `{label}:`, which names an attribute of another domain: a \
                 label is a field of the read's own domain, so read `{domain}` by itself, \
                 `({domain} … {field}: …)`"
            );
            return self.about(&part, |_| Err(Error::Refused(message)));
        }
        // `top` is refused as what it was, where it stands.
        if retired(tree).is_ok()
            && let Some((part, message)) = self.wrapper(tree)
        {
            return self.about(&part, |_| Err(Error::Refused(message)));
        }
        Ok(())
    }

    /// The first value `tree` writes wrapped as the primitive forms read it, in a
    /// type, a pick, a cardinality or a layer, `(:one (:integer 3))`, and
    /// what to write in its place. The walker writes these for a line,
    /// and a document writes none: the read it stands in, and why.
    fn wrapper(&self, tree: &Syntax) -> Option<(Syntax, String)> {
        match tree {
            Syntax::Application(operator, operand) => {
                if wrapper(tree) {
                    let message = format!(
                        "{tree} is a value as the primitive forms read it, and a document writes no \
                         wrapper: say its type, cardinality and layer on a line, `as:`, \
                         `cardinality:` and `in:`, and an elector on its value, `is: (max ?x)`"
                    );
                    return Some((tree.clone(), message));
                }
                if let Syntax::Reference(name) = &**operator {
                    let unlabeled = operand.items.get(1).map(|value| ("is", value));
                    let labelled = operand
                        .terms
                        .iter()
                        .map(|(label, value)| (label.as_str(), value));
                    for (label, value) in unlabeled.into_iter().chain(labelled) {
                        if wrapper(value) {
                            return Some((tree.clone(), self.unwrapped(tree, name, label, value)));
                        }
                    }
                }
                std::iter::once(&**operator)
                    .chain(&operand.items)
                    .chain(operand.terms.values())
                    .find_map(|part| self.wrapper(part))
            }
            Syntax::Tuple(items) => items.iter().find_map(|item| self.wrapper(item)),
            Syntax::Map(entries) => entries
                .iter()
                .find_map(|(key, value)| self.wrapper(key).or_else(|| self.wrapper(value))),
            _ => None,
        }
    }

    /// What `read`, applying `name`, writes in place of the wrapped
    /// `value` of its field `label`.
    fn unwrapped(&self, read: &Syntax, name: &str, label: &str, value: &Syntax) -> String {
        let peeled = Peeled::of(value);
        let refused = format!("{read} writes {value}, and a document writes no wrapper");
        let operand = read.operand();
        let entity = || {
            operand
                .and_then(|operand| operand.terms.get("of").or(operand.items.first()))
                .map_or("_".to_string(), ToString::to_string)
        };
        let attribute = match syntax::named(name) {
            syntax::Named::Domain => Some(format!("{name}/{label}")),
            syntax::Named::Attribute => Some(name.to_string()),
            _ => None,
        };
        if let Some(attribute) = attribute {
            return format!(
                "{refused}: write the field as a line, `{}`",
                peeled.line(&attribute, &entity())
            );
        }
        if self.concepts.contains(name) {
            return format!(
                "{refused}: a concept's lines say its fields' types, cardinalities and layers, \
                 so read the field as it is, `{label}: {}`",
                peeled.inner
            );
        }
        if peeled.cardinality().is_some() {
            return format!(
                "{refused}: a derived attribute's cardinality is what its rules conclude, so \
                 leave it out, `{label}: {}`",
                peeled.value()
            );
        }
        if let Some(layer) = &peeled.layer {
            return format!(
                "{refused}: `{name}` is a derived attribute, concluded and read on the default \
                 layer only, not in: {layer}"
            );
        }
        let kind = match (peeled.cases, peeled.kind) {
            (Some(cases), _) => Some(cases.to_string()),
            (None, Some(kind)) => Some(format!(":{kind}")),
            (None, None) => None,
        };
        let typed = match (kind, label) {
            (None, _) => String::new(),
            (Some(kind), "is") => format!(" as: {kind}"),
            (Some(kind), label) => format!(" as: {{:{label} {kind}}}"),
        };
        format!(
            "{refused}: write `{label}: {}{typed}`, `as:` typing the value under `is:` or, \
             as a map, each field it names",
            peeled.value()
        )
    }
    fn each(&mut self, tree: &Syntax, primitive: &mut Vec<Syntax>) -> Result<(), Error> {
        // Where several definitions take a form that is read, the rule
        // is expanded once a definition: what is read is what any of
        // them reads. Within one rule a form that recurs is the same
        // definition's each time.
        let (depth, parts) = tree.extent();
        if depth > DEEPEST {
            return self.about(tree, |_| {
                Err(Error::Refused(format!(
                    "a form nested {depth} deep: the expander takes {DEEPEST}"
                )))
            });
        }
        self.largest = SPARE.saturating_add(parts.saturating_mul(GROWTH));
        if let Err(refused) = valid(tree) {
            let (part, error) = *refused;
            return self.about(&part, |_| Err(error));
        }
        if let Some(rules) = self.query_forms(tree)? {
            primitive.extend(rules);
            return Ok(());
        }
        let mut choices = Vec::new();
        loop {
            self.choices = choices;
            let rules = self.top(tree.clone())?;
            choices = std::mem::take(&mut self.choices);
            for rule in rules {
                if let Some(rule) = self.install(rule)?
                    && !primitive.contains(&rule)
                {
                    primitive.push(rule);
                }
            }
            while let Some(last) = choices.last_mut() {
                if last.taken + 1 < last.of {
                    last.taken += 1;
                    break;
                }
                choices.pop();
            }
            if choices.is_empty() {
                return Ok(());
            }
        }
    }

    /// Do `work` with `part` as what a refusal in it is about.
    fn about<T>(
        &mut self,
        part: &Syntax,
        work: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let depth = self.blame.len();
        self.blame.push(part.clone());
        let done = work(self);
        if done.is_ok() {
            self.blame.truncate(depth);
        }
        done
    }

    /// For each primitive rule the last document expanded to, which of its
    /// trees the rule came of.
    pub fn origin(&self) -> &[usize] {
        &self.origin
    }

    /// Each tree the last document expanded to a rule an earlier tree
    /// came to as well, with that earlier tree.
    pub fn repeats(&self) -> &[(usize, usize)] {
        &self.repeats
    }

    /// Keep a rule about syntax, and note what any other rule reads.
    fn install(&mut self, rule: Syntax) -> Result<Option<Syntax>, Error> {
        let rule = electing(rule);
        let clauses = Clauses::of(&rule);
        let answer = match clauses.head.as_ref() {
            Some(head) if head.applies() == Some("syntax") => match items(head) {
                [template] => Some(Answer::Syntax(template.clone())),
                _ => return Err(Error::Refused(format!("{head} takes one template"))),
            },
            Some(head) if head.applies() == Some("warning") => match items(head) {
                [Syntax::Constant(dialog_corpus::Value::String(message))] => {
                    Some(Answer::Warning(message.clone()))
                }
                _ => return Err(Error::Refused(format!("{head} takes one text"))),
            },
            _ => None,
        };
        // A line of a concept or a command, however it is written, is a
        // stored attribute: one that names a derived attribute of a
        // declared concept, which rules conclude, is refused, naming it.
        if let Some(Answer::Syntax(template)) = &answer
            && let Some((line, name, concept)) =
                members(template.clone()).into_iter().find_map(|member| {
                    let name = member.applies()?.to_string();
                    let (concept, _) = name.rsplit_once('/')?;
                    let concept = concept.to_string();
                    (is_line(&member)
                        && syntax::declarable(&name)
                        && self.concepts.contains(&concept)
                        && !self.concepts.contains(&name))
                    .then_some((member, name, concept))
                })
        {
            return Err(Error::Refused(format!(
                "{line} names `{name}`, a derived attribute of the concept `{concept}`, which \
                 rules conclude: a line of a concept or a command is a stored attribute, its \
                 domain dotted, `{}`",
                syntax::stored(&name)
            )));
        }
        // A concept written as lines is kept as the reads they are.
        let answer = match answer {
            Some(Answer::Syntax(template)) => Some(Answer::Syntax(lines(template)?)),
            other => other,
        };
        let Some(answer) = answer else {
            for premise in &clauses.when {
                if let (true, Some(domain)) = (premise.is_read(), premise.applies()) {
                    self.reads.insert(domain.to_string());
                }
            }
            return Ok(Some(rule));
        };

        let mut when = clauses.when;
        let at = when
            .iter()
            .position(|premise| premise.applies() == Some("expand"))
            .ok_or_else(|| {
                Error::Refused(format!(
                    "a rule about syntax needs an (expand …) premise: {rule}"
                ))
            })?;
        let expand = when.remove(at);
        let pattern = match items(&expand) {
            [pattern] => pattern.clone(),
            _ => return Err(Error::Refused(format!("{expand} takes one pattern"))),
        };
        let position = match expand.operand().and_then(|operand| operand.terms.get("in")) {
            None => None,
            Some(word) => match word.keyword() {
                Some("head") => Some(Position::Head),
                Some("premise") => Some(Position::Premise),
                _ => return Err(Error::Refused(format!("{word} is not a position"))),
            },
        };

        if let Answer::Syntax(template) = &answer {
            Self::check_concept(&pattern, template)?;
        }
        if let Some(name) = pattern.applies() {
            self.names.insert(name.to_string());
        }
        // A definition under a signature replaces the one before it
        // under the same: the last write wins. Under another signature
        // it stands beside it.
        let shaped = match &answer {
            Answer::Syntax(template) => {
                position.is_none()
                    && when.is_empty()
                    && clauses.unless.is_empty()
                    && members(template.clone()).iter().all(|member| {
                        // A read of a relation, or of a declared concept,
                        // which stands for reads of relations.
                        member.is_read()
                            || member
                                .applies()
                                .is_some_and(|name| self.concepts.contains(name))
                    })
            }
            _ => false,
        };
        // A document's macro, whatever its template, does not take a word
        // of the notation's own, the prelude's own are its, nor a name
        // spelled as no stored name is. A concept or a command is said as
        // one.
        if let Some(name) = pattern.applies()
            && self.prelude_read
            && (syntax::FORMS.contains(&name)
                || syntax::named(name) == syntax::Named::Declarable
                    && syntax::unspelled(name).is_some())
        {
            let what = match (shaped, items(&pattern).is_empty()) {
                (true, true) => "command",
                (true, false) => "concept",
                (false, _) => "macro",
            };
            return Err(syntax::undeclarable(&pattern, what, name));
        }
        // A concept's or a command's name is a name: one applied as an
        // entity, `(a:b ?x …)`, has no signature, and is refused rather
        // than kept as a rule about syntax no read reaches.
        if shaped
            && let Syntax::Application(operator, _) = &pattern
            && let Syntax::Constant(_) = &**operator
        {
            let what = match items(&pattern).is_empty() {
                true => "command",
                false => "concept",
            };
            return Err(syntax::undeclarable(&pattern, what, &operator.to_string()));
        }
        let signature = shaped.then(|| Signature::of(&pattern, "concept")).flatten();
        if let Some(declared) = &signature {
            // A name with a `.` is stored, read directly, and a
            // concept's or a command's name has none; one of the
            // notation's own words would shadow it.
            if !syntax::declares(&declared.name) {
                let what = match items(&pattern).is_empty() {
                    true => "command",
                    false => "concept",
                };
                return Err(syntax::undeclarable(&pattern, what, &declared.name));
            }
            // A field it declares is a declared name too.
            if let Some(refused) = declared
                .parameters
                .iter()
                .find_map(|label| syntax::unspelled_field(&pattern, label))
            {
                return Err(refused);
            }
            // A name rules of an earlier document read or concluded as
            // a derived attribute stays one: those rules are installed,
            // and would read another relation than the concept.
            if let Some((_, _, form)) = self.derived_uses.get(&declared.name).and_then(|uses| {
                uses.iter()
                    .find(|(document, ..)| *document != self.documents)
            }) {
                return Err(Error::Refused(format!(
                    "{pattern} declares `{}` a concept, and an earlier document read or \
                     concluded it as a derived attribute of `{}` in {form}: the rules installed \
                     keep that relation, so the name stays a derived attribute; declare the \
                     concept under another name",
                    declared.name,
                    declared
                        .name
                        .rsplit_once('/')
                        .map_or("", |(concept, _)| concept),
                )));
            }
            // One document says one thing of a name, in any order: a
            // concept a form defines and another form defines again, or
            // names as a query, is refused naming both.
            self.defines(
                format!("concept {} {:?}", declared.name, declared.parameters),
                &declared.name,
            )?;
            if self
                .defining
                .contains_key(&format!("query {}", declared.name))
            {
                self.defines(format!("query {}", declared.name), &declared.name)?;
            }
            self.defining
                .entry(format!("concept {}", declared.name))
                .or_insert(self.tree);
            // The field that is one entry of a collection, `block: {?k ?b}`.
            let entry = pattern.operand().and_then(|operand| {
                operand.terms.iter().find_map(|(label, term)| match term {
                    Syntax::Map(pair) if matches!(pair.as_slice(), [(Syntax::Variable(_), _)]) => {
                        Some(label.clone())
                    }
                    _ => None,
                })
            });
            match entry {
                Some(label) => self.entries.insert(declared.clone(), label),
                None => self.entries.remove(declared),
            };
            self.syntax.retain(|rule| {
                rule.signature.as_ref() != Some(declared)
                    && rule.entity_of.as_ref() != Some(&declared.name)
            });
            self.concepts.insert(declared.name.clone());
            match &answer {
                Answer::Syntax(template) if items(&pattern).is_empty() && on_control(template) => {
                    self.commands.insert(
                        declared.clone(),
                        template.applies().unwrap_or_default().to_string(),
                    );
                }
                _ => {
                    self.commands.remove(declared);
                }
            }
        }
        self.declared += 1;
        // A premise that reads a command may name its entity, `of: ?c`,
        // and so bind the entity the command's content derived: the
        // command's own variable for its entity is what `of:` fills.
        // Only a premise: an entity is derived when a command is
        // asserted, and never given.
        let read = match (&signature, &answer) {
            (Some(declared), Answer::Syntax(template)) if self.commands.contains_key(declared) => {
                match (items(template).first(), &pattern) {
                    (Some(own @ Syntax::Variable(_)), Syntax::Application(operator, operand)) => {
                        Some(Definition {
                            answer: answer.clone(),
                            pattern: Syntax::Application(
                                operator.clone(),
                                Parameters {
                                    items: vec![own.clone()],
                                    terms: operand.terms.clone(),
                                },
                            ),
                            position: Some(Position::Premise),
                            when: when.clone(),
                            unless: clauses.unless.clone(),
                            signature: None,
                            entity_of: Some(declared.name.clone()),
                        })
                    }
                    _ => None,
                }
            }
            _ => None,
        };
        self.syntax.push(Definition {
            answer,
            pattern,
            position,
            when,
            unless: clauses.unless,
            signature,
            entity_of: None,
        });
        self.syntax.extend(read);
        Ok(None)
    }

    /// A concept maps facts to itself and back. So what it stands for is
    /// reads and nothing else, since a check has no inverse, and every
    /// variable in them is one of its parameters, since a write must
    /// know every value. The reads may be of several entities.
    ///
    /// A command is a concept whose head names no entity and whose lines
    /// are all on the control layer. Its entity is the one variable that
    /// is no parameter, and stands only as what its lines are of. What a
    /// concept that is one of these and not the other is, the design
    /// does not say, and it is refused as unsupported.
    fn check_concept(pattern: &Syntax, template: &Syntax) -> Result<(), Error> {
        let said = members(template.clone());
        if !said.iter().any(Syntax::is_read)
            || !said.iter().all(|tree| tree.is_read() || tree.is_formula())
        {
            return Ok(());
        }
        if let Some(check) = said.iter().find(|tree| tree.is_formula()) {
            return Err(Error::Refused(format!(
                "a concept maps both ways, and a check has no inverse: {check}"
            )));
        }
        // A concept holds one collection, and its key is a head field.
        let entries: Vec<&Syntax> = said
            .iter()
            .flat_map(|tree| tree.operand().into_iter().flat_map(|operand| operand.terms.values()))
            .filter(|term| matches!(term, Syntax::Application(operator, _) if operator.keyword() == Some("entry")))
            .collect();
        if entries.len() > 1 {
            return Err(Error::Refused(format!(
                "{pattern} holds {} collections, and a concept holds one: two would give a row \
                 for every pair of their entries",
                entries.len()
            )));
        }
        let mut heads = Vec::new();
        pattern.variables(&mut heads);
        if let Some(Syntax::Variable(key)) = entries
            .first()
            .and_then(|entry| entry.operand())
            .and_then(|operand| operand.terms.get("key"))
        {
            if !heads.contains(key) {
                return Err(Error::Refused(format!(
                    "the key of a collection is a field of the concept's head: ?{key}"
                )));
            }
            // An entry is one term, its key and its value: they were never
            // apart.
            if let Some((label, _)) = pattern.operand().and_then(|operand| {
                operand
                    .terms
                    .iter()
                    .find(|(_, term)| matches!(term, Syntax::Variable(name) if name == key))
            }) {
                return Err(Error::Refused(format!(
                    "{pattern} gives the key of its collection a field of its own, `{label}:`: a \
                     collection's field is one entry, its key and its value, `block: {{?{key} \
                     ?b}}`"
                )));
            }
        }
        let controls = said.iter().filter(|tree| on_control(tree)).count();
        let touched = said
            .iter()
            .filter(|tree| {
                tree.operand()
                    .is_some_and(|operand| operand.terms.values().any(controlled))
            })
            .count();
        let command = match (items(pattern).is_empty(), touched) {
            (false, 0) => false,
            (true, _) if controls == said.len() => true,
            (false, _) => {
                return Err(Error::Unsupported(format!(
                    "{pattern} names its entity and holds lines on the control layer: what such \
                     a concept is is not decided, a command's head names no entity"
                )));
            }
            (true, 0) => {
                return Err(Error::Unsupported(format!(
                    "{pattern} names no entity and its lines are not on the control layer: \
                     what such a concept is is not decided, a command's lines are `in: :control`"
                )));
            }
            (true, _) => {
                return Err(Error::Unsupported(format!(
                    "{pattern} names no entity and mixes lines on the control layer with \
                     stored ones: what such a concept is is not decided"
                )));
            }
        };
        let mut given = Vec::new();
        pattern.variables(&mut given);
        // What a line is of, apart from what it says of it.
        let mut valued = Vec::new();
        for tree in &said {
            if let Some(operand) = tree.operand() {
                operand
                    .terms
                    .values()
                    .for_each(|term| term.variables(&mut valued));
            }
        }
        let mut entities = Vec::new();
        said.iter()
            .flat_map(|tree| items(tree).first())
            .for_each(|entity| entity.variables(&mut entities));
        let own: BTreeSet<&String> = entities
            .iter()
            .chain(&valued)
            .filter(|name| !given.contains(name))
            .collect();
        if let Some(own) = own.iter().find(|name| !command || valued.contains(name)) {
            return Err(Error::Refused(format!(
                "a concept maps both ways, so every variable in it is a parameter: ?{own}"
            )));
        }
        if command && own.len() > 1 {
            return Err(Error::Unsupported(format!(
                "{pattern} holds lines about more than one entity it does not name: what such a \
                 command is is not decided"
            )));
        }
        if command && said.len() > 1 {
            return Err(Error::Unsupported(format!(
                "{pattern} holds lines of more than one domain: this build derives a command's \
                 entity from the claims of one domain"
            )));
        }
        Ok(())
    }

    /// The bindings under which a rule about syntax applies to `tree`.
    fn applies(&self, rule: &Definition, tree: &Syntax, position: Position) -> Option<Bindings> {
        if rule.position.is_some_and(|wanted| wanted != position) {
            return None;
        }
        let mut bindings = matches(tree, &rule.pattern, Bindings::new())?;
        for premise in &rule.when {
            bindings = self.holds(premise, bindings)?;
        }
        if rule
            .unless
            .iter()
            .any(|premise| self.holds(premise, bindings.clone()).is_some())
        {
            return None;
        }
        Some(bindings)
    }

    /// Whether a premise of a rule about syntax holds, and what it binds.
    /// Such a premise applies one of a few formulas over names, asks
    /// which domain a declared command is in, `(:command ?name is: ?domain)`,
    /// or reads what this walker knows: the names syntax is defined for,
    /// `(dialog.name _ is: ?name)`, and the domains installed rules
    /// read, `(dialog.rule _ on: ?domain)`.
    fn holds(&self, premise: &Syntax, bindings: Bindings) -> Option<Bindings> {
        let value = |term: &Syntax| match term {
            Syntax::Variable(name) => match bindings.get(name) {
                Some(Binding::One(tree)) => Some(tree.clone()),
                _ => None,
            },
            other => Some(other.clone()),
        };
        let name = |term: &Syntax| match value(term)? {
            Syntax::Reference(name) => Some(name),
            _ => None,
        };
        let Syntax::Application(operator, operand) = premise else {
            return None;
        };
        let subject = operand.items.first();
        // `is:` bound to `domain`, or holding it already.
        let is = |domain: String| {
            let Syntax::Variable(is) = operand.terms.get("is")? else {
                return None;
            };
            let domain = Binding::One(Syntax::Reference(domain));
            let mut bindings = bindings.clone();
            match bindings.get(is) {
                Some(bound) if *bound == domain => {}
                Some(_) => return None,
                None => {
                    bindings.insert(is.clone(), domain);
                }
            }
            Some(bindings)
        };
        match (operator.keyword(), &**operator) {
            // A command declared, and the domain its claims are in.
            // A name that is a command under one domain: two commands of
            // one name in two domains say no one domain, and are no
            // command here.
            (Some("command"), _) => {
                let name = name(subject?)?;
                let domains: BTreeSet<&String> = self
                    .commands
                    .iter()
                    .filter(|(signature, _)| signature.name == name)
                    .map(|(_, domain)| domain)
                    .collect();
                match domains.len() {
                    1 => is(domains.into_iter().next()?.clone()),
                    _ => None,
                }
            }
            (Some("domain"), _) => (syntax::named(&name(subject?)?) == syntax::Named::Domain)
                .then_some(bindings.clone()),
            (Some("name"), _) => {
                (Kind::of(&name(subject?)?) == Kind::Name).then_some(bindings.clone())
            }
            (None, Syntax::Reference(relation)) if relation == "dialog.name" => self
                .names
                .contains(&name(operand.terms.get("is")?)?)
                .then_some(bindings.clone()),
            (None, Syntax::Reference(relation)) if relation == "dialog.rule" => self
                .reads
                .contains(&name(operand.terms.get("on")?)?)
                .then_some(bindings.clone()),
            _ => None,
        }
    }

    /// What the rules say `tree` expands to in `position`. No answer is
    /// refused. Of several:
    ///
    /// - Answers that differ only in the names of their own variables
    ///   are one answer.
    /// - A write takes the definition whose signature is the labels
    ///   written, since a write says every field. None or several such
    ///   is refused.
    /// - A read takes each in turn, the rule expanded once for each.
    /// - What must not hold is refused: its absence under any of the
    ///   definitions would be a rule of several negations, not several
    ///   rules.
    fn ask(&mut self, tree: &Syntax, position: Position) -> Result<Syntax, Error> {
        // What the template's own variables are named apart by: the
        // tree being expanded and where it stands, by reference, so the
        // names do not move with how either is spelled, and numbered in
        // the order the tree's sites are met, so two sites are two
        // numbers.
        let key = apply(
            "site",
            vec![
                tree.clone(),
                Syntax::Reference(
                    match position {
                        Position::Head => "head",
                        Position::Premise => "premise",
                    }
                    .into(),
                ),
            ],
        )
        .reference()?
        .to_string();
        let next = self.sites.len();
        let site = self.sites.entry(key).or_insert(next).to_string();
        // Of the rules that apply, the ones that name the most: a
        // pattern that leaves a part to its `...` gives way to one that
        // names it. `(?concept ... ?fields)` and
        // `(?concept ?subject ... ?fields)` both match a call with a
        // part that has no label, and the second is the one meant.
        // A part a read leaves out matches anything, so a trailing
        // `...` adds nothing to one and is taken off.
        let (said, dotted) = open(tree);
        let mut found: Vec<(Syntax, Option<Signature>)> = Vec::new();
        let mut patterns: Vec<String> = Vec::new();
        let mut least = usize::MAX;
        for rule in &self.syntax {
            if let (Answer::Syntax(template), Some(bindings)) =
                (&rule.answer, self.applies(rule, &said, position))
            {
                let left = left(&rule.pattern, &said, &bindings);
                if left > least {
                    continue;
                }
                if left < least {
                    least = left;
                    found.clear();
                    patterns.clear();
                }
                let answer = fill(template, &bindings, &site)?;
                let same = unnamed(&answer, &site);
                if !found.iter().any(|(other, _)| unnamed(other, &site) == same) {
                    found.push((answer, rule.signature.clone()));
                    patterns.push(rule.pattern.to_string());
                }
            }
        }
        self.asked.push((tree.clone(), position, self.tree));
        // A read of a concept says every field, or ends with `...`: one
        // without it fits only a concept whose fields are those it says,
        // and one that leaves fields out untold is refused, naming them.
        // With `...` it fits every concept whose fields include its own,
        // each read in turn. A write is not a read, and is fitted below.
        let reads = position == Position::Premise || self.reporting;
        if reads && !dotted && found.iter().any(|(_, signature)| signature.is_some()) {
            let written = Signature::of(&said, "concept").map(|written| written.parameters);
            let exact = |signature: &Option<Signature>| {
                signature.as_ref().map(|signature| &signature.parameters) == written.as_ref()
            };
            if found.iter().any(|(_, signature)| exact(signature)) {
                found.retain(|(_, signature)| signature.is_none() || exact(signature));
            } else if let Some(written) = &written {
                let untold = found
                    .iter()
                    .filter_map(|(_, signature)| signature.as_ref())
                    .map(|signature| {
                        let left: Vec<String> = signature
                            .parameters
                            .difference(written)
                            .map(|label| format!("{label}:"))
                            .collect();
                        format!(
                            "{} of ({} {})",
                            left.join(" "),
                            signature.name,
                            signature
                                .parameters
                                .iter()
                                .map(|label| format!("{label}:"))
                                .collect::<Vec<_>>()
                                .join(" ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", and ");
                let shown = tree.to_string();
                let dots = match shown.strip_suffix(')') {
                    Some(open) => format!("{open} ...)"),
                    None => format!("{shown} ..."),
                };
                return Err(Error::Refused(format!(
                    "{shown} leaves out {untold}: a read that says some of a concept's fields \
                     ends with `...`, `{dots}`"
                )));
            }
        }
        let ambiguous = |found: &[(Syntax, Option<Signature>)]| {
            Error::Ambiguous(
                tree.to_string(),
                found
                    .iter()
                    .map(|(answer, _)| answer.to_string())
                    .collect::<Vec<_>>()
                    .join(" | "),
            )
        };
        let chosen = match found.len() {
            0 => Err(self.unfitting(tree)),
            1 => Ok(0),
            _ if self.negating => Err(ambiguous(&found)),
            _ if position == Position::Head && !self.reporting => {
                let written = Signature::of(&said, "concept");
                let mut exact = found.iter().enumerate().filter(|(_, (_, signature))| {
                    signature.is_some()
                        && signature.as_ref().map(|signature| &signature.parameters)
                            == written.as_ref().map(|written| &written.parameters)
                });
                match (exact.next(), exact.next()) {
                    (Some((at, _)), None) => Ok(at),
                    _ => Err(ambiguous(&found)),
                }
            }
            // Reads through several concepts are each read in turn; two
            // rules about syntax that name as much as each other are a
            // tie, at a premise as anywhere.
            _ if found.iter().any(|(_, signature)| signature.is_none()) => Err(ambiguous(&found)),
            of => {
                let taken = match self.choices.iter().find(|choice| choice.form == *tree) {
                    Some(choice) => choice.taken,
                    None => {
                        self.choices.push(Choice {
                            form: tree.clone(),
                            taken: 0,
                            of,
                        });
                        0
                    }
                };
                (taken < of)
                    .then_some(taken)
                    .ok_or_else(|| ambiguous(&found))
            }
        }?;
        self.answered = patterns.swap_remove(chosen);
        Ok(found.swap_remove(chosen).0)
    }

    /// `asked` rewritten once, as [`ask`](Self::ask) does, on the path
    /// of the forms rewritten before it. A form met again on the path is
    /// a cycle, and a path longer than [`STEPS`], or an answer deeper
    /// than [`DEEPEST`] or larger than the tree allows, is an expansion
    /// that does not end: each is refused, naming the definitions on the
    /// way.
    fn rewrite(&mut self, asked: &Syntax, position: Position) -> Result<Syntax, Error> {
        let through = |path: &[(Syntax, String)]| {
            let mut named: Vec<String> = Vec::new();
            for (_, definition) in path {
                if !named.contains(definition) {
                    named.push(definition.clone());
                }
            }
            named
        };
        if let Some(at) = self.path.iter().position(|(form, _)| form == asked) {
            return Err(Error::Cycle(
                asked.to_string(),
                self.path[at..]
                    .iter()
                    .map(|(_, definition)| definition.clone())
                    .collect(),
            ));
        }
        if self.path.len() >= STEPS {
            return Err(Error::Endless(Box::new(crate::Endless {
                form: asked.to_string(),
                past: format!("{STEPS} steps"),
                through: through(&self.path),
            })));
        }
        let answer = self.ask(asked, position)?;
        let definition = std::mem::take(&mut self.answered);
        self.path.push((asked.clone(), definition));
        let (depth, parts) = answer.extent();
        let past = if depth > DEEPEST {
            Some(format!("{DEEPEST} deep"))
        } else if parts > self.largest {
            Some(format!("{} parts", self.largest))
        } else {
            None
        };
        match past {
            Some(past) => Err(Error::Endless(Box::new(crate::Endless {
                form: asked.to_string(),
                past,
                through: through(&self.path),
            }))),
            None => Ok(answer),
        }
    }

    /// What the rules say about the forms they were asked about, now that
    /// the whole document is in: a rule further down may be what reads a
    /// command issued further up.
    fn diagnose(&mut self) -> Vec<Diagnostic> {
        let mut diagnostics: Vec<Diagnostic> = Vec::new();
        for (form, position, tree) in std::mem::take(&mut self.asked) {
            for rule in &self.syntax {
                if let (Answer::Warning(message), Some(_)) =
                    (&rule.answer, self.applies(rule, &form, position))
                {
                    let mut path = vec![Segment::Item(tree)];
                    path.extend(
                        self.document
                            .get(tree)
                            .and_then(|written| written.locate(&form))
                            .unwrap_or_default(),
                    );
                    let diagnostic = Diagnostic {
                        form: Some(form.clone()),
                        message: message.clone(),
                        path,
                        span: None,
                        refusal: None,
                    };
                    if !diagnostics.contains(&diagnostic) {
                        diagnostics.push(diagnostic);
                    }
                }
            }
        }
        diagnostics
    }

    /// A head or a premise, rewritten until it applies a relation or a
    /// formula. `(all …)` comes back when it stands for several.
    fn application(&mut self, tree: Syntax, position: Position) -> Result<Syntax, Error> {
        // What this form is rewritten through is on its own path, not on
        // the path of the form beside it.
        let depth = self.path.len();
        let done = self.applied(tree, position);
        self.path.truncate(depth);
        done
    }

    /// [`application`](Self::application), on the path it is given.
    fn applied(&mut self, mut tree: Syntax, position: Position) -> Result<Syntax, Error> {
        loop {
            let word = tree.applies().map(str::to_string);
            if tree.is_read()
                || tree.is_formula()
                || word
                    .as_deref()
                    .is_some_and(|word| DIRECTIVES.contains(&word))
            {
                return Ok(tree);
            }
            // A query's answer is read by whoever asks and by a query,
            // never where rows are.
            if let Some(word) = word.as_deref()
                && self.queries.contains_key(word)
            {
                let call = tree.clone();
                return self.about(&call, |this| Err(this.read_where_rows_are(&call)));
            }
            if word.as_deref() == Some("all") {
                let each = items(&tree)
                    .iter()
                    .cloned()
                    .map(|member| self.application(member, position))
                    .collect::<Result<Vec<_>, _>>()?;
                return Ok(apply("all", each));
            }
            // `(dialog/position ?b after: ?lo before: ?hi is: ?at)`, the
            // formula a position key is made by, as the design writes it:
            // a name with a `/` that is no attribute.
            if word.as_deref() == Some(POSITION)
                && !self.concepts.contains(POSITION)
                && let Syntax::Application(_, operand) = &tree
            {
                return Ok(Syntax::Application(
                    Box::new(Syntax::Constant(syntax::entity("position:")?)),
                    operand.clone(),
                ));
            }
            // `(notation/read text: ?s at: ?n …)` and `notation/analyze`,
            // whose code is the notation's, kept by a keyword a `/` cannot
            // be in: `notation.read`.
            if let Some(name) = word
                .as_deref()
                .filter(|word| crate::supplied::declares(word))
                && !self.concepts.contains(name)
                && let Syntax::Application(_, operand) = &tree
            {
                return Ok(Syntax::Application(
                    Box::new(Syntax::Constant(syntax::entity(&format!(
                        "{}:",
                        crate::supplied::keyword(name)
                    ))?)),
                    operand.clone(),
                ));
            }
            // A concept declared under a name that holds a `/` is the
            // concept, not an attribute.
            let declared = word
                .as_deref()
                .is_some_and(|word| self.concepts.contains(word));
            // The name read as an attribute's, and the concept it would be
            // derived under, what comes before its last `/`: either
            // declared later would have made this read another relation.
            if !declared
                && let Some(word) = word
                    .as_deref()
                    .filter(|word| syntax::declarable(word) && word.contains('/'))
            {
                self.misread.push((self.tree, word.to_string()));
                if let Some((concept, _)) = word.rsplit_once('/')
                    && !self.concepts.contains(concept)
                {
                    self.misread.push((self.tree, concept.to_string()));
                }
            }
            // A derived attribute is named as a concept is, with no `.`:
            // `my/list.count` under the concept `my` would be a stored
            // name.
            if !declared
                && let Some(word) = word.as_deref().filter(|word| {
                    syntax::named(word) == syntax::Named::Attribute
                        && word
                            .rsplit_once('/')
                            .is_some_and(|(concept, _)| self.concepts.contains(concept))
                })
            {
                let word = word.to_string();
                return self.about(&tree, |_| {
                    Err(syntax::undeclarable(&tree, "derived attribute", &word))
                });
            }
            // An attribute named under a declared concept is one a rule
            // concludes about that concept's entities: `list/count-open`.
            // The concept is all before the last `/`, as a field's label
            // holds none: `nb/blocks/count` is `count` of `nb/blocks`.
            // The tree names it by what was written, and Dialog-DB's name
            // for it is made where it is compiled.
            if !declared
                && let Some((concept, name)) = word.as_deref().and_then(|word| {
                    (syntax::declarable(word) && word.contains('/'))
                        .then(|| word.rsplit_once('/'))
                        .flatten()
                })
                && self.concepts.contains(concept)
            {
                let domain = syntax::derived_read(concept, name);
                let form = self
                    .document
                    .get(self.tree)
                    .map(ToString::to_string)
                    .unwrap_or_default();
                let used = (self.documents, self.tree, form);
                let uses = self
                    .derived_uses
                    .entry(format!("{concept}/{name}"))
                    .or_default();
                if !uses.contains(&used) {
                    uses.push(used);
                }
                // A derived attribute's head declares its fields.
                if position == Position::Head
                    && let Some(refused) = tree.operand().and_then(|operand| {
                        operand
                            .terms
                            .keys()
                            .find_map(|label| syntax::unspelled_field(&tree, label))
                    })
                {
                    return self.about(&tree, |_| Err(refused));
                }
                // A derived attribute's name is spelled as a declared one.
                if let Some(why) = syntax::unspelled(name) {
                    let message = format!(
                        "{tree} names the derived attribute `{concept}/{name}`, which is spelled \
                         as no stored name is: {why}"
                    );
                    return self.about(&tree, |_| Err(Error::Refused(message)));
                }
                let reads = position == Position::Premise || self.reporting;
                return self.about(&tree, |_| derived(&tree, concept, name, &domain, reads));
            }
            if !declared && let Some(read) = self.about(&tree, |_| line(&tree))? {
                return Ok(read);
            }
            // A name of words joined by `/`, no `.`, that nothing defines
            // and whose first part is no concept is a concept or a
            // derived attribute not declared above what reads it.
            if !declared
                && let Some(word) = word.as_deref().filter(|word| {
                    syntax::declarable(word)
                        && !self.names.contains(*word)
                        && !self.queries.contains_key(*word)
                })
                && let Some((concept, _)) = word.rsplit_once('/')
                && !self.concepts.contains(concept)
            {
                let message = format!(
                    "`{word}` names no attribute of a domain, which holds a `.`, `{}`, and no \
                     concept `{word}`, nor a concept `{concept}` it would be derived under, is \
                     declared above what reads it",
                    syntax::stored(word)
                );
                return self.about(&tree, |_| Err(Error::Refused(message)));
            }
            if let (Position::Head, Some(word)) = (
                position,
                word.as_deref().filter(|word| HEAD_WORDS.contains(word)),
            ) {
                let inner = match items(&tree) {
                    [inner] => inner.clone(),
                    _ => return Err(Error::Refused(format!("{tree} takes one application"))),
                };
                let reporting = std::mem::replace(&mut self.reporting, word == "report");
                let stands = self.application(inner, Position::Head);
                self.reporting = reporting;
                let mut each: Vec<Syntax> = members(stands?)
                    .into_iter()
                    .map(|member| apply(word, vec![member]))
                    .collect();
                return Ok(if each.len() == 1 {
                    each.remove(0)
                } else {
                    apply("all", each)
                });
            }
            let asked = tree;
            tree = self.about(&asked, |this| {
                former(&asked)?;
                this.rewrite(&asked, position)
            })?;
        }
    }

    /// `(id ?t list: ?l text: ?x)` and `(fresh ?n)`, the formulas a new
    /// thing is made by, as the primitive premise they are, or `None` for
    /// any other premise.
    ///
    /// `id` binds `?t` to the entity `(:id domain list: ?l text: ?x)`
    /// names, the same on every device. The design leaves open whether
    /// it names a domain or a concept; the build takes the domain of
    /// the head `?t` is the entity of, so `id` names what `(:id …)`
    /// names there. A head of no domain, or of two, is unsupported.
    ///
    /// `fresh` is unsupported: Dialog-DB has no formula that mints.
    fn made(&self, premise: &Syntax, heads: &[Syntax]) -> Result<Option<Syntax>, Error> {
        let word = match premise.applies() {
            Some(word @ ("id" | "fresh")) if !self.concepts.contains(word) => word,
            _ => return Ok(None),
        };
        if word == "fresh" {
            return Err(Error::Unsupported(format!(
                "{premise}: Dialog-DB has no formula that makes a new entity in a rule. Its \
                 formulas are functions of their inputs, and an entity never equal to any \
                 other is not one"
            )));
        }
        let (Some(operand), [entity @ Syntax::Variable(_)]) = (premise.operand(), items(premise))
        else {
            return Err(Error::Refused(format!(
                "{premise} binds one variable, the entity: `(id ?t list: ?l text: ?x)`"
            )));
        };
        if operand.terms.is_empty() || operand.terms.contains_key("is") {
            return Err(Error::Refused(format!(
                "{premise} identifies its entity by labels and values, and says no `is:`: \
                 `(id ?t list: ?l text: ?x)`"
            )));
        }
        let mut domains: Vec<&str> = heads
            .iter()
            .filter_map(|head| match head.is_read() {
                true => Some(head),
                false => items(head).first(),
            })
            .filter(|read| read.is_read() && items(read).first() == Some(entity))
            .filter_map(Syntax::applies)
            .collect();
        domains.dedup();
        let domain = match domains.as_slice() {
            [domain] => domain.to_string(),
            [] => {
                return Err(Error::Unsupported(format!(
                    "{premise}: {entity} is the entity of no head, and the build takes the \
                     domain an `id` names from the head: whether `id` names a domain or a \
                     concept is not decided"
                )));
            }
            _ => {
                return Err(Error::Unsupported(format!(
                    "{premise}: {entity} is the entity of heads of {}, and whether `id` names a \
                     domain or a concept is not decided",
                    domains.join(" and ")
                )));
            }
        };
        let mut terms = operand.terms.clone();
        terms.insert("is".into(), entity.clone());
        Ok(Some(Syntax::Application(
            Box::new(Syntax::Constant(syntax::entity("id:")?)),
            Parameters {
                items: vec![Syntax::Reference(domain)],
                terms,
            },
        )))
    }

    /// A top-level tree, rewritten until it is primitive rules. A write at
    /// the top level is a rule with that head and whatever clauses
    /// follow it. A primitive rule has one head, so a head that stands for
    /// several, as a concept of several reads does, is a rule each, with
    /// the premises again.
    fn top(&mut self, mut tree: Syntax) -> Result<Vec<Syntax>, Error> {
        self.path.clear();
        retired(&tree)?;
        let written = tree.clone();
        while tree.applies() != Some("rule") {
            // A call stands in a rule, a query or a write. Alone at the
            // top level it says nothing to do.
            if tree.is_read()
                || tree.is_formula()
                || tree.applies() == Some("all")
                || tree
                    .applies()
                    .is_some_and(|word| Kind::of(word) == Kind::Attribute)
            {
                return self.about(&written, |_| {
                    Err(Error::Refused(format!(
                        "{written} is a call, and a call is not a top-level form: the top level \
                         takes (select …), (assert …), (retract …), (rule …), (concept …) and \
                         (query …)"
                    )))
                });
            }
            tree = match (
                tree.applies().filter(|word| HEAD_WORDS.contains(word)),
                tree.operand(),
            ) {
                (Some(word), Some(operand)) => {
                    let Some((first, rest)) = operand.items.split_first() else {
                        return Err(Error::Refused(format!("{tree} takes an application")));
                    };
                    let mut parts = vec![apply(word, vec![first.clone()])];
                    let mut terms = operand.terms.clone();
                    // A write at the top level reads as a rule does, its
                    // premises after its head. It runs once, in its
                    // document, and is never installed.
                    match (word, rest) {
                        (_, []) => {}
                        ("assert" | "retract", [when]) if when.applies() == Some("when") => {
                            let tree = tree.clone();
                            let written = Syntax::Application(
                                Box::new(Syntax::Reference(word.into())),
                                Parameters {
                                    items: std::iter::once(first.clone())
                                        .chain(items(when).iter().cloned())
                                        .collect(),
                                    terms: operand.terms.clone(),
                                },
                            );
                            return self.about(&tree, |_| {
                                Err(Error::Refused(format!(
                                    "{tree}: `when` is no longer written. A write's premises follow \
                                     its head, as a rule's do: {written}, as in `(assert \
                                     (xyz.todo/state of: ?t is: :done) (xyz.todo/text ?t \"Call \
                                     mom\"))`. It runs once; a write that stays is a `rule`"
                                )))
                            });
                        }
                        ("assert" | "retract", premises) => {
                            parts.extend(premises.iter().cloned());
                            terms.insert(ONCE.into(), Syntax::Constant(Value::Boolean(true)));
                        }
                        _ => parts.extend(rest.iter().cloned()),
                    }
                    Syntax::Application(
                        Box::new(Syntax::Reference("rule".into())),
                        Parameters {
                            items: parts,
                            terms,
                        },
                    )
                }
                _ => self.about(&tree, |this| {
                    former(&tree)?;
                    this.rewrite(&tree, Position::Head)
                })?,
            };
        }
        let written = Clauses::of(&tree);
        // `(all …)` is what a template answers when a tree stands for
        // several. Nobody writes it: several premises are written one
        // after another, and several heads are a rule each.
        let several = |part: &Syntax| part.applies() == Some("all");
        if let Some(head) = &written.head
            && (several(head)
                || (head
                    .applies()
                    .is_some_and(|word| HEAD_WORDS.contains(&word))
                    && items(head).iter().any(several)))
        {
            self.blame.push(head.clone());
            return Err(Error::Refused(format!(
                "{head} is several heads: a rule has one head, so write a rule for each"
            )));
        }
        if let Some(part) = written
            .when
            .iter()
            .chain(&written.unless)
            .find(|part| several(part))
        {
            self.blame.push(part.clone());
            return Err(Error::Refused(format!(
                "{part} is not a premise: premises are written one after another"
            )));
        }
        let heads = match written.head {
            Some(head) => {
                members(self.about(&head, |this| this.application(head.clone(), Position::Head))?)
            }
            None => Vec::new(),
        };
        let mut walked = Clauses {
            said: written.said,
            ..Clauses::default()
        };
        for premise in written.when {
            // Nothing but its place tells a head from a premise, so a
            // write after the first part is said to be misplaced, and
            // not asked about as a premise.
            if premise
                .applies()
                .is_some_and(|word| HEAD_WORDS.contains(&word))
            {
                self.blame.push(premise.clone());
                return Err(Error::Refused(format!(
                    "{premise} is a head, and stands among the premises: the first part of a rule is its head, and a rule has one, so write a rule for each"
                )));
            }
            if let Some(made) = self.about(&premise, |this| this.made(&premise, &heads))? {
                walked.when.push(made);
                continue;
            }
            walked.when.extend(members(self.about(&premise, |this| {
                this.application(premise.clone(), Position::Premise)
            })?));
        }
        for premise in written.unless {
            let joined = self.about(&premise, |this| {
                this.negating = true;
                let stands = this.application(premise.clone(), Position::Premise);
                this.negating = false;
                together(members(stands?), &premise)
            })?;
            walked.unless.push(joined);
        }
        let split = heads.len() > 1;
        let mut rules = Vec::new();
        for head in heads {
            let rule = Clauses {
                head: Some(head),
                ..walked.clone()
            }
            .into_rule();
            // A concept of several reads is reported a rule a read. A
            // read the query named no field of says nothing, so it gets
            // no rule.
            if split
                && let Ok(typed) = Rule::parse(&rule)
                && let Head::Report(read) = &typed.head
                && read.named(&typed.bound()).is_empty()
            {
                continue;
            }
            rules.push(blanked(rule));
        }
        if rules.is_empty() {
            return Err(Error::Refused(format!("{tree} reports no field")));
        }
        Ok(rules)
    }
}

/// Whether `rule`, a primitive tree, writes where it stands: a write at the
/// top level, with no premise, or with premises and run once.
fn writes(rule: &Syntax) -> bool {
    let Some(operand) = rule.operand() else {
        return false;
    };
    matches!(
        operand.items.first().and_then(Syntax::applies),
        Some("assert" | "retract")
    ) && (operand.items.len() == 1
        || matches!(
            operand.terms.get("once"),
            Some(Syntax::Constant(Value::Boolean(true)))
        ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TODO: &str = r#"
        (concept (todo ?t list: ?l text: ?x state: ?s)
          (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
          (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
          (xyz.todo/state of: ?t is: ?s cardinality: :one))
        (concept (todo/add todo: ?t list: ?l text: ?x)
          (xyz.todo.add/todo of: ?c is: ?t in: :control cardinality: :many)
          (xyz.todo.add/list of: ?c is: ?l in: :control cardinality: :many)
          (xyz.todo.add/text of: ?c is: ?x in: :control cardinality: :many))
        (rule (assert (todo ?t list: ?l text: ?x state: :open))
          (todo/add todo: ?t list: ?l text: ?x))
    "#;

    /// The primitive trees `text` expands to after `setup`: a tree the
    /// notation names its own variables in is not printed and read back,
    /// since no document can spell those names.
    fn primitives(setup: &str, text: &str) -> Result<Vec<Syntax>, Error> {
        let mut expander = Expander::new()?;
        expander.submit(setup)?;
        Ok(expander.submit(text)?.0)
    }

    fn expanded(setup: &str, text: &str) -> Result<Vec<String>, Error> {
        let mut expander = Expander::new()?;
        expander.submit(setup)?;
        let (primitive, _) = expander.submit(text)?;
        Ok(primitive.iter().map(Syntax::to_string).collect())
    }

    /// What a document expands to, or its refusal, given a few
    /// seconds: an expansion that never ends fails the test instead of
    /// hanging it.
    fn expanded_soon(setup: &str, text: &str) -> Result<Vec<String>, Error> {
        let (sent, received) = std::sync::mpsc::channel();
        let (setup, written) = (setup.to_string(), text.to_string());
        std::thread::spawn(move || {
            let _ = sent.send(expanded(&setup, &written));
        });
        received
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap_or_else(|_| Err(Error::Refused(format!("{text} did not finish expanding"))))
    }

    /// A definition that expands a form to itself again, directly or
    /// through others, is refused naming the form and the definitions on
    /// the way; one that grows the form each time, or names its own
    /// variable anew each time, is refused past the expander's bounds.
    /// A chain of definitions under the bound expands.
    #[test]
    fn it_refuses_an_expansion_that_never_ends() {
        let cycles = [
            (
                "(rule (syntax (loop ?x)) (expand (loop ?x)))",
                "(rule (xyz.out ?a) (loop ?a))",
                1,
            ),
            (
                "(rule (syntax (pong ?x)) (expand (ping ?x))) (rule (syntax (ping ?x)) (expand (pong ?x)))",
                "(rule (xyz.out ?a) (ping ?a))",
                2,
            ),
            (
                "(rule (syntax (all (echo ?x) (xyz.a ?x))) (expand (echo ?x)))",
                "(rule (xyz.out ?a) (echo ?a))",
                1,
            ),
            ("(rule (syntax (again)) (expand (again)))", "(again)", 1),
        ];
        for (setup, text, through) in cycles {
            match expanded_soon(setup, text) {
                Err(Error::Cycle(form, definitions)) => {
                    assert!(!form.is_empty(), "{text}");
                    assert_eq!(definitions.len(), through, "{text}: {definitions:?}");
                }
                other => panic!("{text}: {other:?}"),
            }
        }
        let endless = [
            (
                "(rule (syntax (spin ?y)) (expand (spin ?x)))",
                "(rule (xyz.out ?a) (spin ?a))",
                "steps",
            ),
            (
                "(rule (syntax (grow (w ?x))) (expand (grow ?x)))",
                "(rule (xyz.out ?a) (grow ?a))",
                "steps",
            ),
            (
                "(rule (syntax (twice [?x ?x])) (expand (twice ?x)))",
                "(rule (xyz.out ?a) (twice ?a))",
                "parts",
            ),
        ];
        for (setup, text, bound) in endless {
            match expanded_soon(setup, text) {
                Err(Error::Endless(endless)) => {
                    assert!(endless.past.contains(bound), "{text}: {endless:?}");
                    assert!(!endless.through.is_empty(), "{text}");
                }
                other => panic!("{text}: {other:?}"),
            }
        }
        // A tree given to the expander deeper than it takes, as no reader
        // made it, is refused before anything walks it. (A tree is
        // cloned and dropped by recursion, so one too deep for those is
        // the caller's to never build.)
        let mut deep = Syntax::Constant(Value::UnsignedInt(1));
        for _ in 0..DEEPEST * 4 {
            deep = Syntax::Tuple(vec![deep]);
        }
        let tree = apply("assert", vec![apply("xyz.note", vec![deep])]);
        let refused = std::thread::spawn(move || Expander::new()?.expand(vec![tree]))
            .join()
            .map_err(|_| Error::Refused("the expander overflowed".into()));
        assert!(
            matches!(&refused, Ok(Err(Error::Refused(why))) if why.contains("nested")),
            "{refused:?}"
        );
        // A chain as long as the bound allows, ending in a read.
        let chain: String = (1..STEPS)
            .map(|at| format!("(rule (syntax (step{} ?x)) (expand (step{at} ?x)))", at + 1))
            .chain([format!(
                "(rule (syntax (xyz.a ?x)) (expand (step{STEPS} ?x)))"
            )])
            .collect();
        assert_eq!(
            expanded_soon(&chain, "(rule (xyz.out ?a) (step1 ?a))").unwrap(),
            vec!["(rule (xyz.out ?a) (xyz.a ?a))"]
        );
    }

    /// The variables a template names for itself at two sites of one
    /// rule are two variables, whatever the sites are: the reviewer's
    /// two premises once named theirs alike and joined on them.
    #[test]
    fn it_names_the_own_variables_of_two_sites_apart() -> Result<(), Error> {
        let rules = primitives(
            "(concept (task ?t title: ?x) (xyz.todo ?t text: ?x) (xyz.todo ?t alt: ?x))",
            "(rule (xyz.out ?a97 other: ?a940) (task ?a97 ...) (task ?a940 ...))",
        )?;
        let [rule] = rules.as_slice() else {
            return Err(Error::Refused(format!("one rule: {rules:?}")));
        };
        let mut names = Vec::new();
        rule.variables(&mut names);
        let own: BTreeSet<String> = names
            .into_iter()
            .filter(|name| syntax::unwritten(name))
            .collect();
        assert_eq!(own.len(), 2, "{rule}");
        Ok(())
    }

    /// One form written twice in the same place of a rule is one site,
    /// and its own variables are one; the prelude says so.
    #[test]
    fn it_names_the_own_variables_of_one_form_written_twice_alike() -> Result<(), Error> {
        let rules = primitives(
            "(concept (task ?t title: ?x) (xyz.todo ?t text: ?x) (xyz.todo ?t alt: ?x))",
            "(rule (xyz.out ?a other: ?b) (task ?a ...) (task ?b ...) (task ?a ...))",
        )?;
        let [rule] = rules.as_slice() else {
            return Err(Error::Refused(format!("one rule: {rules:?}")));
        };
        let mut names = Vec::new();
        rule.variables(&mut names);
        let own: BTreeSet<String> = names
            .into_iter()
            .filter(|name| syntax::unwritten(name))
            .collect();
        assert_eq!(own.len(), 2, "{rule}");
        assert!(PRELUDE.contains("two sites of one rule never share it"));
        Ok(())
    }

    /// However many sites a rule has, each names its own variables
    /// apart from every other's: a site's own variables are as many as
    /// its sites, and the names come out the same each time the rule is
    /// expanded.
    #[test]
    fn it_names_the_own_variables_of_every_site_apart() -> Result<(), Error> {
        for count in [2, 10, 100, 1000] {
            let premises: String = (0..count).map(|at| format!(" (task ?e{at} ...)")).collect();
            let fields: String = (0..count).map(|at| format!(" f{at}: ?e{at}")).collect();
            let text = format!("(rule (xyz.out ?e0{fields}){premises})");
            // Two lines, so a site's own variable joins them and is not
            // a blank.
            let setup =
                "(concept (task ?t title: ?x) (xyz.todo ?t text: ?x) (xyz.todo ?t alt: ?x))";
            let rules = primitives(setup, &text)?;
            assert_eq!(rules, primitives(setup, &text)?, "the same each time");
            let [rule] = rules.as_slice() else {
                return Err(Error::Refused(format!("one rule: {rules:?}")));
            };
            let mut names = Vec::new();
            rule.variables(&mut names);
            let own: BTreeSet<String> = names
                .into_iter()
                .filter(|name| syntax::unwritten(name))
                .collect();
            assert_eq!(own.len(), count, "{count} sites");
        }
        Ok(())
    }

    /// A line over a collection leaves the key a variable in the
    /// attribute's name and says what a key is. It is the read of every
    /// entry, its key a field of the concept's head. A concept holds one
    /// collection.
    #[test]
    fn it_reads_a_line_over_a_collection() {
        assert_eq!(
            expanded(
                "(concept (notebook-entry ?b block: {?p ?x})
                   (xyz.tonk.notebook/?p of: ?b is: ?x as: :entity cardinality: :one by: :position))",
                "(select (notebook-entry notebook:1 block: {?p ?x}))"
            )
            .unwrap(),
            vec![
                "(rule (report (xyz.tonk.notebook notebook:1 entry: (:entry (:one (:entity ?x)) by: :position key: ?p))) \
                 (xyz.tonk.notebook notebook:1 entry: (:entry (:one (:entity ?x)) by: :position key: ?p)))"
            ]
        );
        for (text, why) in [
            (
                "(concept (m ?e source: {?n ?s}) (xyz.m/?n of: ?e is: ?s cardinality: :many))",
                "says what a key is",
            ),
            (
                "(concept (m ?e source: ?s) (xyz.m/source of: ?e is: ?s by: :colour cardinality: :many))",
                "is not what a key is",
            ),
            (
                "(concept (m ?e source: {?n ?s}) (xyz.m/?n of: ?e is: ?s by: :colour cardinality: :many))",
                "is not what a key is",
            ),
            (
                "(concept (m ?e source: ?s) (xyz.m/?n of: ?e is: ?s by: :name cardinality: :many))",
                "the key of a collection is a field of the concept's head: ?n",
            ),
            (
                "(concept (m ?e source: {?n ?s} block: {?p ?b})
                   (xyz.m/?n of: ?e is: ?s by: :name cardinality: :many)
                   (xyz.n/?p of: ?e is: ?b by: :position cardinality: :many))",
                "a concept holds one",
            ),
        ] {
            assert!(
                matches!(expanded("", text), Err(Error::Refused(said)) if said.contains(why)),
                "{text}: {:?}",
                expanded("", text)
            );
        }
    }

    /// A write at the top level reads as a rule does: its premises follow
    /// its head, and it is a rule that says it runs once, never installed.
    /// `(when …)` there is a former word, refused naming the form to write.
    #[test]
    fn it_reads_premises_after_a_top_level_write_as_a_rule_that_runs_once() {
        assert_eq!(
            expanded(
                "",
                r#"(assert (xyz.todo/state of: ?t is: :done) (xyz.todo/text ?t "Call mom"))"#
            )
            .unwrap(),
            vec![
                r#"(rule (assert (xyz.todo ?t state: :done)) (xyz.todo ?t text: "Call mom") once: true)"#
            ]
        );
        assert!(
            expanded(
                "",
                "(retract (xyz.todo/tag ?t ?g) (xyz.todo/tag ?t ?g) (unless (xyz.todo/state ?t :open)))"
            )
            .unwrap()[0]
                .contains("(unless (xyz.todo ?t state: :open)) once: true)")
        );
        for text in [
            r#"(assert (xyz.todo/state of: ?t is: :done) (when (xyz.todo/text ?t "Call mom")))"#,
            "(assert (xyz.todo/state of: ?t is: :done) (when))",
        ] {
            assert!(
                matches!(expanded("", text), Err(Error::Refused(why))
                    if why.contains("`when` is no longer written")
                        && why.contains("(assert (xyz.todo/state of: ?t is: :done) (xyz.todo/text ?t \"Call mom\"))")),
                "{text}: {:?}",
                expanded("", text)
            );
        }
    }

    /// `id` stands as a premise and binds its entity to what `(:id …)`
    /// names in the domain of the head it is the entity of. Whether it
    /// names a domain or a concept is not decided, so an entity of no
    /// head or of heads of two domains is unsupported, and so is
    /// `fresh`, which Dialog-DB has no formula for.
    #[test]
    fn it_reads_id_and_fresh_as_premises() {
        let made = expanded(
            TODO,
            "(rule (assert (todo ?t list: ?l text: ?x state: :open))
               (todo/add list: ?l text: ?x ...)
               (id ?t list: ?l text: ?x))",
        )
        .unwrap();
        assert_eq!(made.len(), 1);
        assert!(
            made[0].ends_with(" (:id xyz.todo is: ?t list: ?l text: ?x))"),
            "{made:?}"
        );
        for (text, why) in [
            (
                "(rule (assert (xyz.note ?n text: ?x)) (xyz.draft ?d text: ?x) (fresh ?n))",
                "no formula that makes a new entity",
            ),
            (
                "(rule (xyz.seen ?d n: ?x) (xyz.draft ?d text: ?x) (id ?t text: ?x))",
                "is the entity of no head",
            ),
            (
                "(concept (card ?c title: ?x owner: ?o)
                   (xyz.card ?c title: ?x) (xyz.owned ?c by: ?o))
                 (rule (assert (card ?c title: ?x owner: ?o))
                   (xyz.draft ?d title: ?x by: ?o)
                   (id ?c title: ?x))",
                "is the entity of heads of xyz.card and xyz.owned",
            ),
        ] {
            assert!(
                matches!(expanded("", text), Err(Error::Unsupported(why2)) if why2.contains(why)),
                "{text}: {:?}",
                expanded("", text)
            );
        }
        assert!(matches!(
            expanded("", "(rule (assert (xyz.note ?n text: ?x)) (xyz.draft ?d text: ?x) (id ?n))"),
            Err(Error::Refused(why)) if why.contains("by labels and values")
        ));
    }

    /// A variable the expansion named for itself and the rule uses once
    /// is written `_` among the premises, so the stored body holds a
    /// blank there. One the author wrote is kept, used once or not, and
    /// so is a command's entity read with `of:` and used again.
    #[test]
    fn it_writes_an_expansions_variable_used_once_as_a_blank() {
        let rule = expanded(
            TODO,
            "(rule (xyz.added ?t text: ?x) (todo/add todo: ?t text: ?x ...))",
        )
        .unwrap();
        assert_eq!(
            rule,
            vec![
                "(rule (xyz.added ?t text: ?x) (xyz.todo.add _ list: (:control (:many _)) text: (:control (:many ?x)) todo: (:control (:many ?t))))"
            ]
        );
        let compiled = crate::compile(&parse(&rule[0]).unwrap().remove(0));
        let Ok([crate::compile::Step::Deduce(stored)]) = compiled.as_deref() else {
            panic!("one rule");
        };
        let body = serde_json::to_string(&stored.descriptor()).unwrap();
        assert!(!body.contains("?c."), "{body}");
        assert!(body.contains(r#"{"?":{}}"#), "{body}");

        let read = expanded(
            TODO,
            "(rule (xyz.added ?c text: ?x) (todo/add of: ?c text: ?x))",
        )
        .unwrap();
        assert!(read[0].contains("(xyz.todo.add ?c "), "{read:?}");
        let own = expanded("", "(rule (xyz.seen ?t n: 1) (xyz.todo ?t text: ?once))").unwrap();
        assert!(own[0].contains("text: ?once"), "{own:?}");
    }

    /// The order of a document's forms does not matter: uses written
    /// above the concepts and commands they use expand as they do
    /// below them.
    #[test]
    fn it_expands_a_document_in_any_order() {
        let declarations = r#"
            (concept (todo ?t list: ?l text: ?x state: ?s)
              (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
          (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
          (xyz.todo/state of: ?t is: ?s cardinality: :one))
            (concept (todo/add todo: ?t list: ?l text: ?x)
              (xyz.todo.add/todo of: ?c is: ?t in: :control cardinality: :many)
              (xyz.todo.add/list of: ?c is: ?l in: :control cardinality: :many)
              (xyz.todo.add/text of: ?c is: ?x in: :control cardinality: :many))"#;
        let uses = r#"
            (assert (todo/add todo: todo:1 list: list:home text: "Buy milk"))
            (rule (assert (todo ?t list: ?l text: ?x state: :open))
              (todo/add todo: ?t list: ?l text: ?x))
            (rule (todo/count ?l ?t) (todo ?t list: ?l ...))
            (select (todo ?t text: ?x ...))"#;
        let expand = |text: &str| {
            let mut expander = Expander::new().unwrap();
            let (primitives, diagnostics) = expander.submit(text).unwrap();
            assert!(diagnostics.is_empty(), "{diagnostics:?}");
            let mut primitives: Vec<String> = primitives.iter().map(Syntax::to_string).collect();
            primitives.sort();
            primitives
        };
        let first = expand(&format!("{declarations}{uses}"));
        assert_eq!(first.len(), 4, "{first:?}");
        assert_eq!(first, expand(&format!("{uses}{declarations}")));
    }

    /// A macro's expansion may declare a name: what used it above is
    /// expanded again once it is declared.
    #[test]
    fn it_expands_again_what_a_later_expansion_declares_a_name_for() {
        let mut expander = Expander::new().unwrap();
        let (primitive, diagnostics) = expander
            .submit(
                "(select (memo ?t text: ?x))
                 (rule (syntax (concept (?name ?t text: ?x) (xyz.note/text of: ?t is: ?x cardinality: :many)))
                   (expand (note-kind ?name)))
                 (note-kind memo)",
            )
            .unwrap();
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert_eq!(
            primitive.iter().map(Syntax::to_string).collect::<Vec<_>>(),
            vec!["(rule (report (xyz.note ?t text: (:many ?x))) (xyz.note ?t text: (:many ?x)))"]
        );
        assert_eq!(expander.origin(), &[0]);
    }

    /// A read of a concept that says some of its fields ends with
    /// `...`: one that leaves fields out without it is refused, naming
    /// the fields left out and saying to write `...`. Every field said,
    /// it needs none, and a trailing `...` adds nothing.
    #[test]
    fn it_refuses_a_concept_read_that_leaves_fields_out_untold() {
        for (text, left) in [
            ("(select (todo ?t text: ?x))", "list: state:"),
            (
                "(rule (xyz.seen ?t text: ?x) (todo ?t text: ?x))",
                "list: state:",
            ),
            (
                "(query (todo/text ?t is: ?x) (todo ?t text: ?x state: :open))",
                "list:",
            ),
            (
                "(rule (xyz.lonely ?t text: ?x) (todo ?t list: ?l text: ?x state: ?s) \
                   (unless (todo ?t state: :done)))",
                "list: text:",
            ),
        ] {
            let refused = expanded(TODO, text);
            assert!(
                matches!(&refused, Err(Error::Refused(why))
                    if why.contains(left) && why.contains("`...`")),
                "{text}: {refused:?}"
            );
        }
        let open = expanded(TODO, "(select (todo ?t text: ?x ...))").unwrap();
        assert_eq!(open.len(), 1);
        assert!(open[0].contains("text: (:one (:text ?x))"), "{open:?}");
        // `...` alone is `... _`. The two say one thing, and differ in
        // the names the expansion gives its own variables, which are
        // made of the form as written.
        assert_eq!(
            expanded(TODO, "(select (todo ?t text: ?x ... _))")
                .unwrap()
                .len(),
            1
        );
        // Every field said, it needs no `...`, and `...` adds nothing.
        assert!(expanded(TODO, "(select (todo ?t list: ?l text: ?x state: ?s))").is_ok());
        assert!(expanded(TODO, "(select (todo ?t list: ?l text: ?x state: ?s ...))").is_ok());
        // What it leaves out may not be bound: a concept's fields are
        // facts, and no record of them is kept. No definition takes a
        // read with its rest bound, so nothing says what it is.
        let bound = expanded(TODO, "(select (todo ?t text: ?x ... ?others))");
        assert!(
            matches!(&bound, Err(Error::Unknown(form)) if form == "(todo ?t ... ?others text: ?x)"),
            "{bound:?}"
        );
        // A relation is read by the fields it says: it is no concept.
        assert!(expanded(TODO, "(select (xyz.todo ?t text: ?x))").is_ok());
        assert!(expanded(TODO, "(select (xyz.todo ?t text: ?x ...))").is_ok());
        // A write is not a read: an assert says every field, and a
        // retract every field or its entity alone, as before.
        assert!(
            expanded(
                TODO,
                "(assert (todo todo:1 list: list:1 text: \"a\" state: :open))"
            )
            .is_ok()
        );
        assert!(expanded(TODO, "(retract (todo todo:1))").is_ok());
    }

    /// Of two concepts of one name, a read without `...` fits the one
    /// whose fields are those it says, and one with `...` fits every one
    /// whose fields include them, each read in turn: a read is a
    /// selector, and selects all it fits. A read that says fields only
    /// one has fits that one with or without `...`.
    #[test]
    fn it_fits_a_read_to_concepts_by_its_fields_and_its_dots() {
        let both = "(concept (item/tag ?t tag: ?g)
               (xyz.item/tag of: ?t is: ?g as: :text cardinality: :many))
             (concept (item/tag ?t tag: ?g source: ?s)
               (xyz.item/label of: ?t is: ?g as: :text cardinality: :many)
               (xyz.item/source of: ?t is: ?s as: :text cardinality: :one))";
        let exact = expanded(both, "(rule (xyz.seen ?t tag: ?g) (item/tag ?t tag: ?g))").unwrap();
        assert_eq!(exact.len(), 1, "{exact:?}");
        assert!(
            exact[0].contains("(xyz.item ?t tag:") && !exact[0].contains("label:"),
            "{exact:?}"
        );
        let every = expanded(
            both,
            "(rule (xyz.seen ?t tag: ?g) (item/tag ?t tag: ?g ...))",
        )
        .unwrap();
        assert_eq!(every.len(), 2, "{every:?}");
        let labelled = expanded(
            both,
            "(rule (xyz.seen ?t tag: ?g) (item/tag ?t tag: ?g source: ?s))",
        )
        .unwrap();
        assert_eq!(labelled.len(), 1, "{labelled:?}");
        assert!(labelled[0].contains("label:"), "{labelled:?}");
        let sourced = expanded(
            both,
            "(rule (xyz.seen ?t source: ?s) (item/tag ?t source: ?s))",
        );
        assert!(
            matches!(&sourced, Err(Error::Refused(why)) if why.contains("tag:") && why.contains("`...`")),
            "{sourced:?}"
        );
    }

    /// `(unless …)` once, last, or several times among the premises, is
    /// one rule.
    #[test]
    fn it_reads_unless_once_or_several_times_as_one_rule() {
        let beside = expanded(
            TODO,
            "(rule (xyz.lonely ?t text: ?x)
               (todo ?t text: ?x ...)
               (unless (xyz.tag ?t name: _) (xyz.star ?t by: _)))",
        )
        .unwrap();
        let inside = expanded(
            TODO,
            "(rule (xyz.lonely ?t text: ?x)
               (todo ?t text: ?x ...)
               (unless (xyz.tag ?t name: _))
               (unless (xyz.star ?t by: _)))",
        )
        .unwrap();
        assert_eq!(beside, inside);
        assert!(beside[0].contains("(unless (xyz.tag ?t name: _) (xyz.star ?t by: _))"));
    }

    /// Nothing but its place tells a head from a premise, so a write
    /// after the first part is refused. A rule has one head: `(all …)`
    /// in its place is refused too, and both refusals say to write a
    /// rule for each.
    #[test]
    fn it_refuses_a_second_head_however_it_is_written() {
        let among = expanded(
            "",
            "(rule (assert (xyz.todo ?t list: ?to))
                   (retract (xyz.todo ?t list: ?from))
               (xyz.move ?t to: ?to from: ?from))",
        );
        assert!(
            matches!(&among, Err(Error::Refused(why))
                if why.contains("stands among the premises") && why.contains("a rule for each")),
            "{among:?}"
        );
        for written in [
            "(rule (all (assert (xyz.todo ?t list: ?to))
                        (retract (xyz.todo ?t list: ?from)))
               (xyz.move ?t to: ?to from: ?from))",
            "(rule (assert (all (xyz.todo ?t list: ?to) (xyz.seen ?t at: ?to)))
               (xyz.move ?t to: ?to from: ?from))",
            "(assert (all (xyz.todo ?t list: ?to) (xyz.seen ?t at: ?to))
               (xyz.move ?t to: ?to from: ?from))",
        ] {
            let several = expanded("", written);
            assert!(
                matches!(&several, Err(Error::Refused(why))
                    if why.contains("is several heads") && why.contains("a rule for each")),
                "{several:?}"
            );
        }
        let premise = expanded(
            "",
            "(rule (xyz.done ?t at: ?to)
               (all (xyz.todo ?t list: ?to) (xyz.seen ?t at: ?to)))",
        );
        assert!(
            matches!(&premise, Err(Error::Refused(why)) if why.contains("is not a premise")),
            "{premise:?}"
        );
        // The two rules that say it.
        assert_eq!(
            expanded(
                "",
                "(rule (assert (xyz.todo ?t list: ?to))
                   (xyz.move ?t to: ?to from: ?from))
                 (rule (retract (xyz.todo ?t list: ?from))
                   (xyz.move ?t to: ?to from: ?from))",
            )
            .unwrap(),
            vec![
                "(rule (assert (xyz.todo ?t list: ?to)) (xyz.move ?t from: ?from to: ?to))",
                "(rule (retract (xyz.todo ?t list: ?from)) (xyz.move ?t from: ?from to: ?to))",
            ]
        );
    }

    /// A concept of several reads stands for them all. Written, it is a
    /// rule a read, each with the premises again, since a primitive rule
    /// has one head.
    #[test]
    fn it_writes_a_concept_of_several_reads_as_a_rule_each() {
        let primitives = expanded(
            "(concept (card ?c title: ?x owner: ?o)
               (xyz.card/title of: ?c is: ?x as: :text cardinality: :one)
               (xyz.owned/by of: ?c is: ?o as: :entity cardinality: :one))",
            "(rule (assert (card ?c title: ?x owner: ?o))
               (xyz.draft ?c title: ?x by: ?o))",
        )
        .unwrap();
        assert_eq!(
            primitives,
            vec![
                "(rule (assert (xyz.card ?c title: (:one (:text ?x)))) (xyz.draft ?c by: ?o title: ?x))",
                "(rule (assert (xyz.owned ?c by: (:one (:entity ?o)))) (xyz.draft ?c by: ?o title: ?x))",
            ]
        );
    }

    /// Reported, a read the query named no field of gets no rule.
    #[test]
    fn it_reports_only_the_reads_a_query_names_a_field_of() {
        let primitives = expanded(
            "(concept (card ?c title: ?x owner: ?o)
               (xyz.card/title of: ?c is: ?x as: :text cardinality: :one)
               (xyz.owned/by of: ?c is: ?o as: :entity cardinality: :one))",
            "(select (card ?c title: ?x ...))",
        )
        .unwrap();
        assert_eq!(primitives.len(), 1, "{primitives:?}");
        assert!(primitives[0].starts_with("(rule (report (xyz.card ?c title: (:one (:text ?x))))"));
    }

    /// A pattern is any form, not an application only: here a form in
    /// brackets at the top level.
    #[test]
    fn it_expands_a_form_that_is_not_an_application() {
        let primitives = expanded(
            r#"(rule (syntax (assert (xyz.note/text of: ?n is: ?x as: :text)))
                 (expand [note ?n ?x]))"#,
            r#"[note note:1 "hi"]"#,
        )
        .unwrap();
        assert_eq!(
            primitives,
            vec![r#"(rule (assert (xyz.note note:1 text: (:text "hi"))))"#]
        );
    }

    #[test]
    fn it_expands_a_concept_where_it_is_read_and_where_it_is_written() {
        let mut expander = Expander::new().unwrap();
        let (primitive, diagnostics) = expander.submit(TODO).unwrap();
        let [rule] = primitive.as_slice() else {
            panic!("one rule: {primitive:?}");
        };
        let rule = rule.to_string();
        // The command's entity, which the author did not write and the
        // rule uses once, is a blank.
        assert_eq!(
            rule,
            "(rule (assert (xyz.todo ?t list: (:one (:entity ?l)) state: (:one :open) text: (:one (:text ?x)))) \
             (xyz.todo.add _ list: (:control (:many ?l)) text: (:control (:many ?x)) todo: (:control (:many ?t))))"
        );
        assert!(diagnostics.is_empty());
    }

    /// A field the query leaves out is still read, under a variable of
    /// the expansion's own: one where the concept stands as the head,
    /// another where it stands as the premise.
    #[test]
    fn it_expands_a_query_to_a_rule_that_reports() {
        let primitives = primitives(TODO, "(select (todo ?t text: ?x ...))").unwrap();
        let rule = Clauses::of(&primitives[0]);
        let at = |tree: &Syntax, label: &str| {
            tree.operand()
                .and_then(|operand| operand.terms.get(label))
                .cloned()
        };
        let own = |tree: &Syntax| match at(tree, "list") {
            Some(list) => list.to_string(),
            None => panic!("{tree} reads the list"),
        };
        let reported = rule.head.as_ref().unwrap();
        assert_eq!(reported.applies(), Some("report"));
        let head = &items(reported)[0];
        assert_eq!(at(head, "text"), at(&rule.when[0], "text"));
        assert!(own(head).starts_with("(:one (:entity ?l["));
        assert!(own(&rule.when[0]).starts_with("(:one (:entity ?l["));
        assert_ne!(own(head), own(&rule.when[0]));
    }

    /// A command is issued by asserting it: at the top level, a rule
    /// with that head and no premise. Its head names no entity, so the
    /// primitive rule holds one of the expansion's own, which the primitive forms
    /// derives from what the command says. One declared that no rule
    /// reads is issued all the same, and said so.
    #[test]
    fn it_issues_a_command_and_warns_when_nothing_reads_it() {
        let mut expander = Expander::new().unwrap();
        expander.submit(TODO).unwrap();
        let (primitive, diagnostics) = expander
            .submit(r#"(assert (todo/add todo: todo:1 list: list:home text: "Buy milk"))"#)
            .unwrap();
        assert_eq!(primitive.len(), 1);
        assert!(
            primitive[0]
                .to_string()
                .starts_with("(rule (assert (xyz.todo.add ?c["),
            "{primitive:?}"
        );
        assert!(diagnostics.is_empty());

        let (_, diagnostics) = expander
            .submit(
                r#"(concept (todo/snooze todo: ?t)
                     (xyz.todo.snooze/todo of: ?c is: ?t in: :control cardinality: :many))
                   (assert (todo/snooze todo: todo:1))"#,
            )
            .unwrap();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].message, "no rule reads this command");
        assert_eq!(
            diagnostics[0]
                .form
                .as_ref()
                .map(Syntax::to_string)
                .as_deref(),
            Some("(todo/snooze todo: todo:1)")
        );

        // A command is declared, so one misspelled is no command: a name
        // with a `/` that no concept is declared under is an attribute.
        assert!(matches!(
            expander.submit(r#"(assert (todo/ad todo: todo:1 list: list:home text: "x"))"#),
            Err(Error::Refused(why)) if why.contains("no concept is declared under its name")
        ));
    }

    #[test]
    fn it_does_not_warn_about_a_command_a_later_rule_reads() {
        let mut expander = Expander::new().unwrap();
        let (_, diagnostics) = expander
            .submit(
                "(concept (counter/increment counter: ?n)
                   (xyz.counter.increment/counter of: ?c is: ?n in: :control cardinality: :many))
                 (assert (counter/increment counter: counter:a))
                 (rule (assert (xyz.counter ?c count: (+ ?n : 1)))
                   (counter/increment counter: ?c) (xyz.counter ?c count: ?n))",
            )
            .unwrap();
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
    }

    /// A command is a concept, so a premise reads it as one, by any of
    /// its fields, and a rule with it as its head and no word deduces
    /// it, holding while the premises do.
    #[test]
    fn it_reads_and_deduces_a_command_as_a_concept() {
        let premise = expanded(
            TODO,
            "(rule (xyz.added ?t text: ?x) (todo/add todo: ?t text: ?x ...))",
        )
        .unwrap();
        assert_eq!(premise.len(), 1);
        assert_eq!(
            premise[0],
            "(rule (xyz.added ?t text: ?x) (xyz.todo.add _ list: (:control (:many _)) text: (:control (:many ?x)) todo: (:control (:many ?t))))"
        );
        let deduced = expanded(
            TODO,
            r#"(rule (todo/add todo: ?t list: list:home text: ?x) (xyz.draft ?t text: ?x))"#,
        )
        .unwrap();
        assert!(
            deduced[0].starts_with("(rule (xyz.todo.add ?c["),
            "{deduced:?}"
        );
        let deduced = primitives(
            TODO,
            r#"(rule (todo/add todo: ?t list: list:home text: ?x) (xyz.draft ?t text: ?x))"#,
        )
        .unwrap();
        let primitives = Rule::parse(&deduced[0]).unwrap();
        assert!(matches!(&primitives.head, Head::Derive(read)
            if matches!(&read.entity, crate::primitive::Term::Formula(id) if id.name == "id")));
    }

    /// A call alone at the top level says nothing to do, a command's
    /// included: the top level takes a question, a write, a rule and a
    /// concept.
    #[test]
    fn it_refuses_a_call_at_the_top_level() {
        for text in [
            r#"(todo/add todo: todo:1 list: list:home text: "x")"#,
            r#"(todo todo:1 list: list:home text: "x" state: :open)"#,
            r#"(xyz.todo todo:1 text: "x")"#,
            r#"(xyz.todo/text of: todo:1 is: "x")"#,
        ] {
            assert!(
                matches!(expanded(TODO, text), Err(Error::Refused(why))
                    if why.contains("a call is not a top-level form")),
                "{text}"
            );
        }
    }

    /// A command's head names no entity, and its lines are on the
    /// control layer. A concept that is one and not the other, and one
    /// whose lines are about several entities it does not name, are
    /// what the design does not decide, and are unsupported. A head that
    /// names no entity over lines on another layer is no command.
    #[test]
    fn it_refuses_what_is_not_decided_of_a_command() {
        for (text, why) in [
            (
                "(concept (todo/x text: ?x) (xyz.todo/text of: ?c is: ?x cardinality: :many))",
                "its lines are not on the control layer",
            ),
            (
                "(concept (todo/x text: ?x n: ?n)
                   (xyz.todo.x/text of: ?c is: ?x in: :control cardinality: :many)
                   (xyz.todo.x/n of: ?c is: ?n cardinality: :many))",
                "mixes lines on the control layer with stored ones",
            ),
            (
                "(concept (todo/x ?t text: ?x) (xyz.todo.x/text of: ?t is: ?x in: :control cardinality: :many))",
                "names its entity and holds lines on the control layer",
            ),
            (
                "(concept (todo/x text: ?x n: ?n)
                   (xyz.todo.x/text of: ?c is: ?x in: :control cardinality: :many)
                   (xyz.todo.x/n of: ?d is: ?n in: :control cardinality: :many))",
                "about more than one entity",
            ),
            (
                "(concept (todo/x text: ?x n: ?n)
                   (xyz.todo.x/text of: ?c is: ?x in: :control cardinality: :many)
                   (xyz.todo.y/n of: ?c is: ?n in: :control cardinality: :many))",
                "more than one domain",
            ),
            (
                "(concept (todo/x text: ?x) (xyz.todo.x/text of: ?c is: ?x in: :draft cardinality: :many))",
                "its lines are not on the control layer",
            ),
        ] {
            assert!(
                matches!(expanded("", text), Err(Error::Unsupported(said)) if said.contains(why)),
                "{text}: {:?}",
                expanded("", text)
            );
        }
        // A variable of a command's that is no parameter and stands as
        // a value is refused as any concept's is.
        assert!(matches!(
            expanded(
                "",
                "(concept (todo/x text: ?x) (xyz.todo.x/text of: ?c is: ?y in: :control cardinality: :many))"
            ),
            Err(Error::Refused(why)) if why.contains("every variable in it is a parameter")
        ));
    }

    /// A line's `in:` is the layer its value is on: `:control` a
    /// command's, any other keyword a layer a stack declares, a variable
    /// the layer a read binds.
    #[test]
    fn it_reads_a_line_on_any_layer() {
        for (layer, wrapped) in [
            (":control", "(:control ?t)"),
            (":session", "(:in ?t at: :session)"),
            ("?l", "(:in ?t at: ?l)"),
        ] {
            assert_eq!(
                expanded(
                    "",
                    &format!("(select (xyz.todo/title of: ?i is: ?t in: {layer}))")
                )
                .unwrap(),
                vec![format!(
                    "(rule (report (xyz.todo ?i title: {wrapped})) (xyz.todo ?i title: {wrapped}))"
                )],
                "{layer}"
            );
        }
    }

    #[test]
    fn it_refuses_what_nothing_gives_a_meaning_and_what_two_rules_do() {
        assert!(matches!(
            expanded(TODO, "(select (todo ?t ...) (frobnicate ?t))"),
            Err(Error::Unknown(_))
        ));
        // A write that says the fields of neither definition.
        assert!(matches!(
            expanded(
                TODO,
                "(concept (todo ?t title: ?x) (xyz.note ?t title: ?x)) (assert (todo todo:1))"
            ),
            Err(Error::Ambiguous(..))
        ));
    }

    /// Two definitions under one name and other labels stand side by
    /// side, and the labels an application writes say which it is.
    #[test]
    fn it_resolves_a_name_by_the_labels_written() {
        let note = "(concept (todo ?t title: ?x) (xyz.note ?t title: ?x))";
        assert_eq!(
            expanded(TODO, &format!("{note} (select (todo ?t title: ?x))")).unwrap(),
            vec!["(rule (report (xyz.note ?t title: ?x)) (xyz.note ?t title: ?x))"]
        );
        assert_eq!(
            expanded(TODO, &format!("{note} (select (todo ?t text: ?x ...))")).unwrap(),
            expanded(TODO, "(select (todo ?t text: ?x ...))").unwrap()
        );
    }

    /// A definition under the signature of an earlier one replaces it:
    /// the last write wins.
    #[test]
    fn it_replaces_a_definition_by_a_later_one_of_the_same_signature() {
        let task = "(concept (todo ?t list: ?l text: ?x state: ?s)
                      (abc.task ?t list: ?l text: ?x state: ?s))";
        // The order the labels are written in is not another signature.
        let reordered = "(concept (todo ?t state: ?s text: ?x list: ?l)
                           (abc.task ?t list: ?l text: ?x state: ?s))";
        for later in [task, reordered] {
            let rules =
                expanded(TODO, &format!("{later} (select (todo ?t text: ?x ...))")).unwrap();
            assert_eq!(rules.len(), 1, "{rules:?}");
            assert!(rules[0].contains(")) (abc.task ?t "), "{rules:?}");
            assert!(!rules[0].contains("xyz.todo"), "{rules:?}");
        }
    }

    /// Of the rules about syntax that apply, the one that names the most
    /// of what is written is taken, and a part left to `...` is left
    /// whether `...` names it or not: `...` and `... ?r` rank the same. A
    /// tie of rules about syntax that are no concepts is refused at a
    /// premise, as it is elsewhere; concepts whose labels fit are each
    /// read, as the design has it.
    #[test]
    fn it_takes_the_rule_that_names_the_most_however_the_rest_is_written() {
        let any = "(rule (syntax (xyz.any ?e)) (expand (thing ?e ...)))";
        let named = "(rule (syntax (xyz.named ?e name: ?n)) (expand (thing ?e name: ?n)))";
        let asked = "(select (xyz.x ?e v: ?n) (thing ?e name: ?n))";
        assert_eq!(
            expanded(&format!("{any} {named}"), asked).unwrap(),
            vec!["(rule (report (xyz.x ?e v: ?n)) (xyz.named ?e name: ?n))"]
        );
        let rest = "(rule (syntax (xyz.rest ?e)) (expand (thing ?e ... ?r)))";
        assert_eq!(
            expanded(&format!("{rest} {named}"), asked).unwrap(),
            expanded(&format!("{any} {named}"), asked)
                .unwrap()
                .iter()
                .map(|rule| rule.replace("xyz.any", "xyz.rest"))
                .collect::<Vec<_>>(),
        );
        // `(thing ?e ...)` and `(thing ?e ... ?r)` leave the same parts,
        // so they are one signature, defined twice.
        assert!(matches!(
            expanded(&format!("{any} {rest}"), asked),
            Err(Error::Twice(..))
        ));
        // Two rules about syntax that are no concepts, and leave the
        // same parts, are a tie, refused at a premise as elsewhere.
        let first = "(rule (syntax (== ?e : 1)) (expand (thing ?e ...)))";
        let second = "(rule (syntax (== ?e : 2)) (expand (thing ?e ... ?r)))";
        assert!(
            matches!(
                expanded(&format!("{first} {second}"), asked),
                Err(Error::Ambiguous(..))
            ),
            "{:?}",
            expanded(&format!("{first} {second}"), asked)
        );
    }

    /// Where the labels written fit several definitions, a read is of
    /// every one of them: the rule is expanded once a definition, and a
    /// form that recurs in it is the same definition's each time.
    #[test]
    fn it_reads_through_every_definition_the_labels_fit() {
        let both = "(concept (todo ?t title: ?x done: ?d) (xyz.todo ?t title: ?x done: ?d))
                    (concept (todo ?t title: ?x pinned: ?p) (xyz.note ?t title: ?x pinned: ?p))";
        let rules = expanded(both, "(select (todo ?t title: ?x ...))").unwrap();
        assert_eq!(rules.len(), 2, "{rules:?}");
        for (rule, domain) in rules.iter().zip(["xyz.todo", "xyz.note"]) {
            // The labels come in their own order, so the field the
            // query left out stands before or after the one it named.
            assert!(
                rule.starts_with(&format!("(rule (report ({domain} ?t "))
                    && rule.contains(" title: ?x"),
                "{rule}"
            );
            assert!(rule.contains(&format!(")) ({domain} ?t ")), "{rule}");
        }
        // A write says every field, so the labels written are one
        // definition's signature.
        assert_eq!(
            expanded(both, r#"(assert (todo todo:1 title: "x" pinned: true))"#).unwrap(),
            vec![r#"(rule (assert (xyz.note todo:1 pinned: true title: "x")))"#]
        );
        // A write that fits both and is neither is refused, and so is
        // the absence of what several definitions take.
        assert!(matches!(
            expanded(both, r#"(assert (todo todo:1 title: "x"))"#),
            Err(Error::Ambiguous(..))
        ));
        assert!(matches!(
            expanded(
                both,
                "(select (xyz.tag ?t name: ?n) (xyz.tag ?t name: ?n) (unless (todo ?t title: _ ...)))"
            ),
            Err(Error::Ambiguous(..))
        ));
    }

    /// A line is one attribute by its full name, and is the read of
    /// that one field: `as:` the type it is read under, `cardinality:`
    /// the newest claim or every claim.
    #[test]
    fn it_reads_a_line_as_the_read_of_one_attribute() {
        let read = |line: &str| {
            let rules = expanded("", &format!("(select {line})")).unwrap();
            assert_eq!(rules.len(), 1, "{rules:?}");
            rules[0]
                .strip_prefix("(rule (report ")
                .and_then(|rest| rest.split_once(") ("))
                .map(|(head, _)| head.to_string())
                .unwrap_or_else(|| rules[0].clone())
        };
        assert_eq!(
            read("(xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)"),
            "(xyz.todo ?i title: (:one (:text ?t)))"
        );
        assert_eq!(
            read("(xyz.list/task of: ?l is: ?i as: :entity cardinality: :many)"),
            "(xyz.list ?l task: (:many (:entity ?i)))"
        );
        // The entity with no label, as any primary may be written.
        assert_eq!(
            read("(xyz.todo/title ?i is: ?t as: :text cardinality: :one)"),
            "(xyz.todo ?i title: (:one (:text ?t)))"
        );
        // Unlabeled, the entity and then the value.
        assert_eq!(read("(xyz.todo/title ?i ?t)"), "(xyz.todo ?i title: ?t)");
        assert_eq!(
            read(r#"(xyz.todo/title ?i "Buy milk")"#),
            r#"(xyz.todo ?i title: "Buy milk")"#
        );
        // A part left out says nothing.
        assert_eq!(
            read("(xyz.todo/title of: ?i is: ?t)"),
            "(xyz.todo ?i title: ?t)"
        );
        assert_eq!(
            read("(xyz.todo/title of: ?i is: ?t cardinality: :many)"),
            "(xyz.todo ?i title: (:many ?t))"
        );
        // The cases of a variant are entities.
        assert_eq!(
            read("(xyz.todo/state of: ?i is: ?s as: [:open :done] cardinality: :one)"),
            "(xyz.todo ?i state: (:one (:among (:entity ?s) cases: [:open :done])))"
        );
        // A line stands where a read does: in a write, and in a rule.
        assert_eq!(
            expanded(
                "",
                "(assert (xyz.todo/state of: todo:1 is: :done as: :entity cardinality: :one))"
            )
            .unwrap(),
            vec!["(rule (assert (xyz.todo todo:1 state: (:one (:entity :done)))))"]
        );
        assert_eq!(
            expanded(
                "",
                "(rule (xyz.done ?i title: ?t)
                   (xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)
                   (xyz.todo/state of: ?i is: :done as: :entity cardinality: :one))"
            )
            .unwrap(),
            vec![
                "(rule (xyz.done ?i title: ?t) (xyz.todo ?i title: (:one (:text ?t))) (xyz.todo ?i state: (:one (:entity :done))))"
            ]
        );
    }

    /// What a line does not say as a line is refused, each with what
    /// it is: a layer is a keyword or a variable.
    #[test]
    fn it_refuses_a_line_that_is_not_one() {
        let refused = |line: &str, why: &str| {
            let said = expanded("", &format!("(select {line})"));
            assert!(
                matches!(&said, Err(Error::Refused(said)) if said.contains(why)),
                "{line}: {said:?}"
            );
        };
        refused("(xyz.todo/title of: ?i)", "says no value");
        refused("(xyz.todo/title ?i ?t ?u)", "needs its entity");
        // Beside a type, the two terms are labelled.
        refused(
            "(xyz.todo/title ?i ?t as: :text)",
            "has 2 parts with no label",
        );
        refused("(xyz.todo/title is: ?t)", "needs its entity");
        // Two parts with no label are refused where the text is read.
        refused("(xyz.todo/title ?i ?j is: ?t)", "has 2 parts with no label");
        refused("(xyz.todo/title of: ?i is: ?t as: :words)", "is not a type");
        refused("(xyz.todo/title of: ?i is: ?t as: [])", "is not a type");
        refused(
            "(xyz.todo/title of: ?i is: ?t as: [:open 2])",
            "is not a type",
        );
        refused(
            "(xyz.todo/title of: ?i is: ?t cardinality: :few)",
            "is not a cardinality",
        );
        refused(
            "(xyz.todo/title of: ?i is: ?t pick: :last)",
            "says `pick:`, which a line does not take",
        );
        refused(
            r#"(xyz.todo/title of: ?i is: ?t in: "draft")"#,
            "is not a layer",
        );
    }

    /// A concept is its lines. Kept, the lines of one entity under one
    /// domain are one read of several fields, so a concept written as
    /// lines is read, written and reported as one written as that read.
    #[test]
    fn it_keeps_a_concept_of_lines_as_the_reads_they_are() {
        let lines = "(concept (todo ?i list: ?l title: ?t state: ?s)
                       (xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)
                       (xyz.todo/state of: ?i is: ?s as: [:open :done] cardinality: :one)
                       (xyz.list/task of: ?l is: ?i as: :entity cardinality: :many))";
        for text in [
            "(select (todo ?i list: ?l title: ?t state: ?s))",
            "(select (todo ?i title: ?t ...))",
            r#"(assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#,
            "(rule (xyz.open ?i title: ?t) (todo ?i title: ?t state: :open ...))",
        ] {
            // Each line is the primitive read of its field, the lines of
            // one entity one read.
            let by_lines = expanded(lines, text).unwrap();
            assert!(!by_lines.is_empty());
            assert!(
                by_lines.iter().any(|rule| rule.contains("(xyz.todo ")
                    && rule.contains("state: (:one (:among (:entity ")
                    && rule.contains("title: (:one (:text ")),
                "{text}: {by_lines:?}"
            );
        }
        assert_eq!(
            expanded(
                lines,
                r#"(assert (todo todo:1 list: list:home title: "Buy milk" state: :open))"#
            )
            .unwrap(),
            vec![
                r#"(rule (assert (xyz.todo todo:1 state: (:one (:among (:entity :open) cases: [:open :done])) title: (:one (:text "Buy milk")))))"#,
                "(rule (assert (xyz.list list:home task: (:many (:entity todo:1)))))",
            ]
        );

        assert_eq!(
            expanded(
                lines,
                "(rule (xyz.open ?i title: ?t) (todo ?i title: ?t state: :open ...))"
            )
            .unwrap(),
            vec![
                "(rule (xyz.open ?i title: ?t) (xyz.todo ?i state: (:one (:among (:entity :open) cases: [:open :done])) title: (:one (:text ?t))) (xyz.list _ task: (:many (:entity ?i))))"
            ]
        );

        // Read from the value's side, a line gives the reverse relation.
        assert_eq!(
            expanded(
                "(concept (list-item ?l todo: ?t)
                   (xyz.list/task of: ?l is: ?t cardinality: :many))",
                "(select (list-item list:home todo: ?t))"
            )
            .unwrap(),
            vec![
                "(rule (report (xyz.list list:home task: (:many ?t))) (xyz.list list:home task: (:many ?t)))"
            ]
        );
    }

    /// An attribute's two terms unlabeled are the same as `of:` and
    /// `is:`, in a premise, a head, a line of a concept and a write.
    #[test]
    fn it_reads_an_attribute_of_two_unlabeled_terms_as_of_and_is() {
        for (unlabeled, labelled) in [
            (
                "(select (xyz.todo/title ?t ?x))",
                "(select (xyz.todo/title of: ?t is: ?x))",
            ),
            (
                "(rule (xyz.done/title ?t ?x) (xyz.todo/title ?t ?x) (xyz.todo/state ?t :done))",
                "(rule (xyz.done/title of: ?t is: ?x) (xyz.todo/title of: ?t is: ?x) (xyz.todo/state of: ?t is: :done))",
            ),
            (
                r#"(assert (xyz.todo/title todo:1 "Buy milk"))"#,
                r#"(assert (xyz.todo/title of: todo:1 is: "Buy milk"))"#,
            ),
        ] {
            let read = expanded("", unlabeled).unwrap();
            assert!(!read.is_empty());
            assert_eq!(read, expanded("", labelled).unwrap(), "{unlabeled}");
        }
        // A line of a concept says its cardinality, a labelled part, so
        // there it is written with `of:` and `is:`: the two unlabeled
        // terms are refused, saying what to write.
        let unlabeled = expanded(
            "",
            "(concept (titled ?t title: ?x) (xyz.todo/title ?t ?x)) (select (titled ?t title: ?x))",
        );
        assert!(
            matches!(&unlabeled, Err(Error::Refused(why)) if why.contains("cardinality: :one")),
            "{unlabeled:?}"
        );
        assert!(
            expanded(
                "",
                "(concept (titled ?t title: ?x) (xyz.todo/title of: ?t is: ?x cardinality: :many)) \
                 (select (titled ?t title: ?x))"
            )
            .is_ok()
        );
    }

    /// Every variable of a concept's body is one of its parameters,
    /// written as lines too, and a line that is not one refuses the
    /// concept where it is defined.
    #[test]
    fn it_refuses_a_concept_whose_lines_hold_a_variable_its_head_does_not() {
        assert!(matches!(
            expanded(
                "(concept (todo ?i title: ?t)
                   (xyz.todo/title of: ?i is: ?t as: :text cardinality: :one)
                   (xyz.list/task of: ?l is: ?i as: :entity cardinality: :many))",
                "(select (todo ?i title: ?t ...))"
            ),
            Err(Error::Refused(why)) if why.contains("every variable in it is a parameter: ?l")
        ));
        assert!(matches!(
            expanded(
                "(concept (todo ?i title: ?t)
                   (xyz.todo/title of: ?i is: ?t cardinality: :few))",
                "(select (todo ?i title: ?t ...))"
            ),
            Err(Error::Refused(why)) if why.contains("is not a cardinality")
        ));
    }

    /// An attribute named under a declared concept is in that concept's
    /// domain: a relation of its own, concluded and read under the one
    /// name. Unlabeled it is the entity and a value under `is:`; with
    /// labels, the fields they name. A concept's own name, `todo/add`
    /// declared, is the concept and not an attribute of `todo`.
    #[test]
    fn it_reads_a_derived_attribute_in_its_concepts_domain() {
        let lists = "(concept (list ?l name: ?n)
                       (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))
                     (concept (todo ?t list: ?l state: ?s)
                       (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one)
                       (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :one))";
        // Named by what was written; Dialog-DB's name for it is made
        // where it is compiled.
        let open = syntax::derived_read("list", "count-open");
        let count = syntax::derived_read("list", "count");
        let concluded = expanded(
            lists,
            "(rule (list/count-open ?l (count ?t))
               (list ?l ...)
               (todo ?t list: ?l state: :open ...))",
        )
        .unwrap();
        assert_eq!(concluded.len(), 1);
        assert!(
            concluded[0].starts_with(&format!(
                "(rule ({open} ?l is: (count ?t)) (xyz.list ?l name: "
            )),
            "{concluded:?}"
        );
        assert_eq!(
            expanded(lists, "(select (list/count-open list:home ?n))").unwrap(),
            vec![format!(
                "(rule (report ({open} list:home is: ?n)) ({open} list:home is: ?n))"
            )]
        );
        // With labels, it concludes the fields they name.
        let labelled = expanded(
            lists,
            "(rule (list/count ?l status: ?s count: (count ?t))
               (todo ?t list: ?l state: ?s ...))",
        )
        .unwrap();
        assert!(
            labelled[0].starts_with(&format!("(rule ({count} ?l count: (count ?t) status: ?s) ")),
            "{labelled:?}"
        );
        // Under no declared concept, the same spelling is an attribute.
        assert_eq!(
            expanded(lists, "(select (xyz.shelf/count-open shelf:a ?n))").unwrap(),
            vec![
                "(rule (report (xyz.shelf shelf:a count-open: ?n)) (xyz.shelf shelf:a count-open: ?n))"
            ]
        );
        // A declared name wins over an attribute of the concept before
        // its `/`.
        let command = expanded(
            &format!(
                "{lists}
                 (concept (list/archive list: ?l)
                   (xyz.list.archive/list of: ?c is: ?l in: :control cardinality: :many))"
            ),
            "(select (list/archive list: ?l))",
        )
        .unwrap();
        assert!(
            command[0].starts_with("(rule (report (xyz.list.archive "),
            "{command:?}"
        );
        assert!(matches!(
            expanded(lists, "(select (list/count-open list:home))"),
            Err(Error::Refused(why)) if why.contains("needs its entity and a value")
        ));
    }

    /// A write was `assert!` and `retract!` while a mark told what is
    /// kept. Either is refused with the word to write, not said to be
    /// unknown.
    #[test]
    fn it_refuses_a_write_under_the_name_it_had() {
        for (former, word) in [("assert!", "assert"), ("retract!", "retract")] {
            for text in [
                format!(r#"({former} (todo todo:1 list: list:home text: "x" state: :open))"#),
                format!("(rule ({former} (xyz.todo ?t state: :done)) (todo/add todo: ?t))"),
            ] {
                assert!(
                    matches!(
                        expanded(TODO, &text),
                        Err(Error::Refused(why)) if why == format!("`{former}` is written `{word}`: no form carries `!`")
                    ),
                    "{text}"
                );
            }
        }
    }

    /// Two definitions of one concept, which call its field by other
    /// names. An application that writes no label is the same read
    /// through either, so nothing is chosen and the rule is one.
    #[test]
    fn it_reads_one_concept_once_however_many_names_its_fields_have() {
        let both = "(concept (todo ?t title: ?a) (xyz.todo ?t text: ?a))
                    (concept (todo ?t name: ?b) (xyz.todo ?t text: ?b))";
        let rules = expanded(
            both,
            "(select (xyz.tag ?t name: ?n) (xyz.tag ?t name: ?n) (todo ?t ...))",
        )
        .unwrap();
        assert_eq!(rules.len(), 1, "{rules:?}");
        // Each name for the field reads the same attribute.
        assert_eq!(
            expanded(both, "(select (todo ?t title: ?x ...))").unwrap(),
            expanded(both, "(select (todo ?t name: ?x))").unwrap()
        );
    }

    #[test]
    fn it_refuses_a_concept_that_cannot_map_both_ways() {
        assert!(matches!(
            expanded(
                "",
                "(concept (cheap ?i price: ?n) (shop.item ?i price: ?n) (< ?n : 100))"
            ),
            Err(Error::Refused(_))
        ));
        assert!(matches!(
            expanded("", "(concept (titled ?t) (xyz.todo ?t text: ?x))"),
            Err(Error::Refused(_))
        ));
    }

    #[test]
    fn it_reads_a_concept_from_the_other_side() {
        assert_eq!(
            expanded(
                "(concept (list-item ?l todo: ?t) (xyz.todo ?t list: ?l))",
                "(select (list-item list:home todo: ?t))"
            )
            .unwrap(),
            vec!["(rule (report (xyz.todo ?t list: list:home)) (xyz.todo ?t list: list:home))"]
        );
    }

    #[test]
    fn it_joins_the_reads_of_one_entity_under_unless() {
        let primitives = expanded(
            "(concept (author ?p name: ?n email: ?e) (io.person ?p name: ?n) (io.contact ?p email: ?e))",
            "(select (io.person ?p name: ?n) (io.person ?p name: ?n) (unless (author ?p ...)))",
        )
        .unwrap();
        assert!(primitives[0].contains("(unless (io.person ?p io.contact/email: ?e["));
        assert!(primitives[0].contains(" name: ?n["));
    }

    /// A concept, a command or a query is declared under a name, and a
    /// name with `:` in it is an entity, which names no concept: each is
    /// refused naming what was written, never installed as a rule about
    /// syntax no read can reach.
    #[test]
    fn it_refuses_declaring_an_entity_as_a_name() {
        for (text, what, name) in [
            (
                "(concept (a:b ?x name: ?n) (xyz.t/name of: ?x is: ?n cardinality: :one))",
                "concept",
                "a:b",
            ),
            (
                "(concept (a:b/c ?x name: ?n) (xyz.t/name of: ?x is: ?n cardinality: :one))",
                "concept",
                "a:b/c",
            ),
            (
                "(concept (a:b name: ?n) \
                   (xyz.t/name of: ?c is: ?n in: :control cardinality: :many))",
                "command",
                "a:b",
            ),
            (
                "(query (a:b ?x is: ?n) (xyz.t/name of: ?x is: ?n cardinality: :one))",
                "query",
                "a:b",
            ),
        ] {
            let refused = expanded("", text).unwrap_err().to_string();
            assert!(
                refused.contains(&format!("declares a {what} named `{name}`"))
                    && refused.contains("is an entity"),
                "{text}: {refused}"
            );
        }
    }

    /// A concept, a command or a query declared under one of the
    /// notation's own words would shadow it: each is refused naming the
    /// word, and nothing is installed under it.
    #[test]
    fn it_refuses_declaring_a_word_of_the_notation() {
        for word in syntax::FORMS {
            for (text, what) in [
                (
                    format!(
                        "(concept ({word} ?x name: ?n) \
                           (xyz.t/name of: ?x is: ?n cardinality: :one))"
                    ),
                    "concept",
                ),
                (
                    format!(
                        "(concept ({word} name: ?n) \
                           (xyz.t/name of: ?c is: ?n in: :control cardinality: :many))"
                    ),
                    "command",
                ),
                (
                    format!(
                        "(query ({word} ?x is: ?n) \
                           (xyz.t/name of: ?x is: ?n cardinality: :one))"
                    ),
                    "query",
                ),
            ] {
                let refused = expanded("", &text).unwrap_err().to_string();
                // `collect` in a concept is refused before its name is
                // asked about, saying where it stands.
                let collected = word == "distinct"
                    && refused.contains("distinct stands only in a query")
                    || word == "collect" && refused.contains("`collect` is written `distinct`");
                assert!(
                    collected
                        || refused.contains(&format!("declares a {what} named `{word}`"))
                            && refused.contains("one of the notation's own words"),
                    "{text}: {refused}"
                );
            }
        }
        // A read of the word after its refusal is the notation's again.
        let mut expander = Expander::new().unwrap();
        assert!(
            expander
                .submit(
                    "(concept (select ?x name: ?n) (xyz.t/name of: ?x is: ?n cardinality: :one))"
                )
                .is_err()
        );
        let (primitive, _) = expander.submit("(select (xyz.t ?x name: ?n))").unwrap();
        assert_eq!(primitive.len(), 1);
        assert!(
            primitive[0]
                .to_string()
                .starts_with("(rule (report (xyz.t ?x")
        );
    }

    /// A declared name follows the rule a stored name's part does (gaps
    /// row 47, decided): each part between `/` starts with a lowercase
    /// letter, holds lowercase letters, digits and `-` only, and does not
    /// end with `-`. One that does not is refused, naming the part and
    /// the character, as a concept, a query, a command, a macro and a
    /// derived attribute's name alike: a zero-width space, an accent, a
    /// `#`, a `?`, an uppercase letter, a `_`, a leading digit.
    #[test]
    fn it_refuses_a_declared_name_with_a_character_a_stored_one_has_not() {
        let line = "(xyz.t/name of: ?x is: ?n cardinality: :one)";
        for (name, says) in [
            ("t\u{200b}x", "`t\u{200b}x` holds `\u{200b}`"),
            ("caf\u{e9}", "`caf\u{e9}` holds `\u{e9}`"),
            ("cafe\u{301}", "`cafe\u{301}` holds `\u{301}`"),
            ("a#b", "`a#b` holds `#`"),
            ("a?b", "`a?b` holds `?`"),
            ("Todo", "`Todo` starts with `T`"),
            ("to_do", "`to_do` holds `_`"),
            ("9lives", "`9lives` starts with `9`"),
            ("todo-", "`todo-` ends with `-`"),
            ("nb/Blocks", "`Blocks` starts with `B`"),
        ] {
            for text in [
                format!("(concept ({name} ?x name: ?n) {line})"),
                format!(
                    "(concept ({name} name: ?n) (xyz.t.go/name of: ?c is: ?n in: :control cardinality: :many))"
                ),
                format!("(query ({name} ?x is: ?n) (xyz.t ?x name: ?n))"),
                format!("(rule (syntax (+ ?x 1)) (expand ({name} ?x)))"),
            ] {
                let refused = expanded("", &text).unwrap_err().to_string();
                assert!(
                    refused.contains(says) && refused.contains("a lowercase letter"),
                    "{text}: {refused}"
                );
            }
        }
        // A derived attribute's field, under a declared concept.
        let concept = format!("(concept (t ?x name: ?n) {line})");
        for (field, says) in [
            ("Count", "`Count` starts with `C`"),
            ("open_n", "`open_n` holds `_`"),
        ] {
            let text = format!("(rule (t/{field} ?x ?n) (t ?x name: ?n))");
            let refused = expanded(&concept, &text).unwrap_err().to_string();
            assert!(refused.contains(says), "{text}: {refused}");
        }
        // What a stored name's part may be, a declared one may.
        for name in [
            "count-open",
            "notebook/blocks",
            "dialog/position",
            "nb/blocks/count",
            "a9-b",
        ] {
            assert_eq!(syntax::unspelled(name), None, "{name}");
        }
    }

    #[test]
    fn it_refuses_a_name_dialog_db_cannot_keep_in_the_authors_terms() {
        let line = "(xyz.t/name of: ?x is: ?n cardinality: :one)";
        let long = "a".repeat(64);
        let field = "a".repeat(59);
        let domain = format!("xyz.{}", "a".repeat(61));
        for (text, says) in [
            (
                format!("(concept ({long} ?x name: ?n) {line})"),
                format!("`{long}` is 64 bytes, and a name is at most 63"),
            ),
            (
                format!("(concept (a/{long} ?x name: ?n) {line})"),
                format!(
                    "`a/{long}` is 66 bytes, and a name is at most 63 bytes, or 64 with a `/` in it"
                ),
            ),
            (
                format!("(select ({long} ?x name: ?n))"),
                format!("`{long}` is 64 bytes, and a name is at most 63"),
            ),
            (
                format!("(select ({domain} ?x name: ?n))"),
                format!("`{domain}` is 65 bytes, and a domain is at most 64"),
            ),
            (
                format!("(select (xyz.t/{field} ?x ?n))"),
                format!("`xyz.t/{field}` is 65 bytes, and an attribute is at most 64"),
            ),
            (
                format!("(select (xyz.t ?x {field}: ?n))"),
                format!(
                    "`{field}:` makes the attribute `xyz.t/{field}`, which is 65 bytes, and an \
                     attribute is at most 64"
                ),
            ),
            (
                format!("(concept (a\u{0}b ?x name: ?n) {line})"),
                "`a\u{0}b` holds a NUL".to_string(),
            ),
            (
                "(select (xyz.t ?x a\u{0}b: ?n))".to_string(),
                "`a\u{0}b` holds a NUL".to_string(),
            ),
            (
                "(select (.a ?x name: ?n))".to_string(),
                "`.a` has a domain with an empty part between its dots".to_string(),
            ),
            (
                "(select (a. ?x name: ?n))".to_string(),
                "`a.` has a domain with an empty part between its dots".to_string(),
            ),
            (
                "(select (x..y/name ?x ?n))".to_string(),
                "`x..y/name` has a domain with an empty part between its dots".to_string(),
            ),
        ] {
            let refused = expanded("", &text).unwrap_err().to_string();
            assert!(
                refused.contains(&says) && !refused.contains("Relation"),
                "{text}: {refused}"
            );
            // The check says the same, and nothing of a relation.
            let checked = crate::check::check(&mut crate::analyze::Analyzer::new().unwrap(), &text);
            assert!(
                checked
                    .diagnostics
                    .iter()
                    .any(|said| said.message.contains(&says))
                    && checked
                        .diagnostics
                        .iter()
                        .all(|said| !said.message.contains("Relation")),
                "{text}: {:?}",
                checked.diagnostics
            );
        }
        // At the limit each is a name, and its read compiles.
        let name = "a".repeat(63);
        let field = "a".repeat(58);
        // A domain read by a field is at most what leaves the field room.
        let domain = format!("xyz.{}", "a".repeat(54));
        let slashed = format!("a/{}", "a".repeat(62));
        for text in [
            format!("(concept ({name} ?x name: ?n) {line}) (select ({name} ?x name: ?n))"),
            format!("(concept ({slashed} ?x name: ?n) {line}) (select ({slashed} ?x name: ?n))"),
            format!("(select ({domain} ?x name: ?n))"),
            format!("(select (xyz.t/{field} ?x ?n))"),
            format!("(select (xyz.t ?x {field}: ?n))"),
        ] {
            let checked = crate::check::check(&mut crate::analyze::Analyzer::new().unwrap(), &text);
            assert!(
                checked.refusals().next().is_none(),
                "{text}: {:?}",
                checked.diagnostics
            );
        }
    }

    /// `syntax::FORMS` is written by hand, so it is checked against what
    /// the notation defines: every word the sources apply as a form,
    /// `applies() == Some("…")` and its kin, every word of the primitive forms'
    /// tables and every macro the prelude defines is in it, and every
    /// word in it is one of those. A match on a keyword, `:command` or
    /// `:float`, is not on what is applied, and is not read. It reads the
    /// crate's sources outside their tests, embedded when it is built, so
    /// it runs wherever its binary does, an archive or wasm, and fails
    /// when a module is declared that it does not embed.
    #[test]
    fn it_lists_every_word_the_notation_defines_in_forms() {
        use std::collections::BTreeSet;
        let files: [(&str, &str); 24] = [
            ("analyze.rs", include_str!("analyze.rs")),
            ("apply.rs", include_str!("apply.rs")),
            ("check.rs", include_str!("check.rs")),
            ("compile.rs", include_str!("compile.rs")),
            ("data.rs", include_str!("data.rs")),
            ("declare.rs", include_str!("declare.rs")),
            ("diagnostic.rs", include_str!("diagnostic.rs")),
            ("expand.rs", include_str!("expand.rs")),
            ("expand/query.rs", include_str!("expand/query.rs")),
            ("form.rs", include_str!("form.rs")),
            ("primitive.rs", include_str!("primitive.rs")),
            ("lib.rs", include_str!("lib.rs")),
            ("live.rs", include_str!("live.rs")),
            ("parse.rs", include_str!("parse.rs")),
            ("pattern.rs", include_str!("pattern.rs")),
            ("print.rs", include_str!("print.rs")),
            ("select.rs", include_str!("select.rs")),
            ("supplied.rs", include_str!("supplied.rs")),
            ("syntax.rs", include_str!("syntax.rs")),
            ("view.rs", include_str!("view.rs")),
            ("view/markup.rs", include_str!("view/markup.rs")),
            ("view/plan.rs", include_str!("view/plan.rs")),
            ("view/tree.rs", include_str!("view/tree.rs")),
            ("written.rs", include_str!("written.rs")),
        ];
        // Every module the crate declares, from `lib.rs` down, is one of
        // them: a `mod name;` in `lib.rs` is `name.rs`, and in `a.rs` is
        // `a/name.rs`.
        let held: BTreeSet<&str> = files.iter().map(|(path, _)| *path).collect();
        let mut declared = BTreeSet::new();
        let mut open = vec!["lib.rs".to_string()];
        while let Some(path) = open.pop() {
            let text = files
                .iter()
                .find(|(held, _)| *held == path)
                .map(|(_, text)| *text)
                .unwrap_or_else(|| panic!("`{path}` is declared and not embedded here"));
            let under = match path.as_str() {
                "lib.rs" => String::new(),
                other => format!("{}/", other.trim_end_matches(".rs")),
            };
            for line in text.lines() {
                let line = line.trim();
                let name = ["mod ", "pub mod ", "pub(crate) mod "]
                    .iter()
                    .find_map(|start| line.strip_prefix(start))
                    .and_then(|rest| rest.strip_suffix(';'));
                if let Some(name) = name {
                    let module = format!("{under}{name}.rs");
                    if declared.insert(module.clone()) {
                        open.push(module);
                    }
                }
            }
        }
        declared.insert("lib.rs".to_string());
        assert_eq!(
            declared.iter().map(String::as_str).collect::<BTreeSet<_>>(),
            held,
            "the modules the crate declares and the files embedded here"
        );
        let mut sources = String::new();
        for (_, text) in files {
            // A file's tests are not what it defines.
            sources.push_str(text.split("#[cfg(test)]\nmod tests").next().unwrap_or(""));
            sources.push('\n');
        }
        // Each quoted word in `Some("…")` and `Some("…" | "…")` on a line
        // that asks what is applied, and in the arms of a `match` on it:
        // the lines below the match, indented deeper.
        let mut defined: BTreeSet<String> = BTreeSet::new();
        let mut within: Option<usize> = None;
        for line in sources.lines() {
            let indent = line.len() - line.trim_start().len();
            let line = line.trim();
            if within.is_some_and(|depth| !line.is_empty() && indent <= depth) {
                within = None;
            }
            if line.starts_with("match") && line.contains("applies()") {
                within = Some(indent);
            }
            if line.starts_with("//") || !(line.contains("applies()") || within.is_some()) {
                continue;
            }
            for part in line.split("Some(").skip(1) {
                let inner = part.split(')').next().unwrap_or("");
                for word in inner.split('|') {
                    if let Some(word) = word
                        .trim()
                        .strip_prefix('"')
                        .and_then(|w| w.strip_suffix('"'))
                    {
                        defined.insert(word.to_string());
                    }
                }
            }
        }
        defined.extend(HEAD_WORDS.iter().map(|word| word.to_string()));
        defined.extend(DIRECTIVES.iter().map(|word| word.to_string()));
        defined.extend(crate::primitive::FOLDS.iter().map(|word| word.to_string()));
        defined.extend(
            [
                ONCE,
                THIS,
                POSITION,
                crate::supplied::READ,
                crate::supplied::ANALYZE,
                crate::supplied::PIPELINE,
                crate::primitive::QUOTE,
                crate::select::COLLECT,
                crate::select::DISTINCT,
                "site",
            ]
            .map(String::from),
        );
        defined.extend(Expander::new().unwrap().names.iter().cloned());
        defined.extend(crate::supplied::STAGES.map(String::from));
        // The key a site is named apart by is the notation's own and never
        // read: no document's use of `site` meets it.
        defined.remove("site");
        let forms: BTreeSet<String> = syntax::FORMS.iter().map(|word| word.to_string()).collect();
        assert_eq!(
            defined.difference(&forms).collect::<Vec<_>>(),
            Vec::<&String>::new(),
            "defined, and not in FORMS"
        );
        assert_eq!(
            forms.difference(&defined).collect::<Vec<_>>(),
            Vec::<&String>::new(),
            "in FORMS, and defined nowhere"
        );
    }

    /// A rule about syntax a document writes whose template is a read
    /// is a concept under the signature its pattern is written with, so
    /// a concept of the same name and other labels stands beside it, in
    /// either order and across documents, and a use's labels say which
    /// it is. Under the same labels the later replaces the earlier.
    #[test]
    fn it_keeps_a_macro_and_a_concept_of_one_name_beside_each_other() {
        let made = "(rule (syntax (xyz.t ?x name: ?n)) (expand (m ?x ?n)))";
        let concept = "(concept (m ?x name: ?n) (xyz.t/name of: ?x is: ?n cardinality: :one))";
        let by_concept = "(rule (report (xyz.t ?x name: (:one ?n))) (xyz.t ?x name: (:one ?n)))";
        let by_macro = "(rule (report (xyz.t ?x name: ?n)) (xyz.t ?x name: ?n))";
        for (setup, text, answer) in [
            (
                "",
                format!("{made} {concept} (select (m ?x name: ?n))"),
                by_concept,
            ),
            ("", format!("{concept} {made} (select (m ?x ?n))"), by_macro),
            (
                made,
                format!("{concept} (select (m ?x name: ?n))"),
                by_concept,
            ),
            (made, format!("{concept} (select (m ?x ?n))"), by_macro),
            (concept, format!("{made} (select (m ?x ?n))"), by_macro),
            (
                concept,
                format!("{made} (select (m ?x name: ?n))"),
                by_concept,
            ),
        ] {
            assert_eq!(
                expanded(setup, &text).unwrap(),
                vec![answer],
                "{setup} | {text}"
            );
        }
        // A macro under a word of the notation is refused as a concept
        // under it is.
        let refused = expanded(
            "",
            "(rule (syntax (xyz.t ?x name: ?n)) (expand (select ?x ?n)))",
        )
        .unwrap_err()
        .to_string();
        assert!(
            refused.contains("one of the notation's own words"),
            "{refused}"
        );
    }

    /// A name of words joined by `/` with no `.` is a concept's or a
    /// derived attribute's, never a stored attribute: under no concept
    /// of its name, nor one of its first part, it is refused naming
    /// what is missing, wherever it stands, and never read as a relation
    /// named by its first word, `(foo ?x x: ?n)`, which for a word of
    /// the notation would be its form, `(rule ?x x: ?n)`. A dotted one
    /// is a stored attribute, its two terms the entity and the value.
    #[test]
    fn it_refuses_a_slashed_name_no_concept_declares_wherever_it_stands() {
        let concept = "(concept (t ?x name: ?n) (xyz.t/name of: ?x is: ?n cardinality: :one))";
        for word in std::iter::once("foo").chain(syntax::FORMS) {
            for text in [
                format!("{concept} (rule ({word}/x ?x ?n) (t ?x name: ?n))"),
                format!("{concept} (select ({word}/x ?x ?n))"),
                format!("{concept} (rule (t ?x name: ?n) ({word}/x ?x ?n))"),
                format!("{concept} (select (t ?x name: ?n) (unless ({word}/x ?x ?n)))"),
            ] {
                let says = format!(
                    "`{word}/x` names no attribute of a domain, which holds a `.`, `{}`, and no \
                     concept `{word}/x`, nor a concept `{word}` it would be derived under, is \
                     declared above what reads it",
                    syntax::stored(&format!("{word}/x"))
                );
                let refused = expanded("", &text).unwrap_err().to_string();
                assert!(refused.contains(&says), "{text}: {refused}");
                let checked =
                    crate::check::check(&mut crate::analyze::Analyzer::new().unwrap(), &text);
                assert!(
                    checked
                        .diagnostics
                        .iter()
                        .any(|said| said.message.contains(&says)),
                    "{text}: {:?}",
                    checked.diagnostics
                );
            }
        }
        assert_eq!(
            expanded("", "(select (xyz.t/name ?x ?n))").unwrap(),
            vec!["(rule (report (xyz.t ?x name: ?n)) (xyz.t ?x name: ?n))"]
        );
    }

    /// A name in a value's place is kept as a tree's names are, so one
    /// too long is refused as one applied is, in the author's terms; in
    /// a write a bare name is no value at all, and is refused saying so
    /// where it is compiled.
    #[test]
    fn it_refuses_a_name_too_long_in_a_values_place() {
        let long = "a".repeat(64);
        let says = format!("`{long}` is 64 bytes, and a name is at most 63 bytes");
        for text in [
            format!("(select (xyz.t ?x state: {long}))"),
            format!(
                "(concept (t ?x name: ?n) (xyz.t/name of: ?x is: ?n cardinality: :one)) \
                 (select (t ?x name: {long}))"
            ),
        ] {
            let refused = expanded("", &text).unwrap_err().to_string();
            assert!(refused.contains(&says), "{text}: {refused}");
        }
        let checked = crate::check::check(
            &mut crate::analyze::Analyzer::new().unwrap(),
            &format!("(assert (xyz.t x:1 state: {long}))"),
        );
        assert!(
            checked
                .refusals()
                .any(|refused| refused.message.contains("is a name, not a value")),
            "{:?}",
            checked.diagnostics
        );
    }

    /// A document's macro under one of the notation's own words is
    /// refused at its declaration whatever its template, a read or not,
    /// and the word keeps its meaning after.
    #[test]
    fn it_refuses_a_macro_named_by_a_word_of_the_notation_whatever_its_template() {
        for word in syntax::FORMS {
            let text = format!("(rule (syntax (+ ?x 1)) (expand ({word} ?x)))");
            let refused = expanded("", &text).unwrap_err().to_string();
            // `collect` in a rule is refused before its name is asked
            // about, saying where it stands.
            let collected = word == "distinct"
                && refused.contains("distinct stands only in a query")
                || word == "collect" && refused.contains("`collect` is written `distinct`");
            assert!(
                collected
                    || refused.contains(&format!("declares a macro named `{word}`"))
                        && refused.contains("one of the notation's own words"),
                "{text}: {refused}"
            );
        }
        let mut expander = Expander::new().unwrap();
        assert!(
            expander
                .submit("(rule (syntax (+ ?x 1)) (expand (select ?x)))")
                .is_err()
        );
        let (primitive, _) = expander.submit("(select (xyz.t ?x name: ?n))").unwrap();
        assert!(
            primitive[0]
                .to_string()
                .starts_with("(rule (report (xyz.t ?x")
        );
    }
}
