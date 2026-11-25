//! The kernel, typed: what an expanded rule is made of.
//!
//! Analysis leaves a `(rule …)` tree whose every head and premise
//! applies a relation or a formula. This module reads that tree into
//! the shapes Dialog-DB's own descriptors have, and refuses what is not
//! kernel.

use std::collections::BTreeSet;

use dialog_corpus::Value;

use crate::Error;
use crate::pattern::REST;
use crate::syntax::{self, Syntax};

/// The types a term may be read under: `(:text ?x)`.
pub const TYPES: [&str; 8] = [
    "text", "natural", "integer", "float", "boolean", "entity", "bytes", "symbol",
];

/// The picks a term may be read under: `(:last ?x)`. `last` and `all`
/// say what a cardinality does, one and many; `max` and `min` are a
/// concept field's electors, which choose one of the claims a field of
/// cardinality many holds. Over a listed type they rank by the listing,
/// written least first.
pub const PICKS: [&str; 4] = ["last", "all", "max", "min"];

/// The electors a concept's head may choose a field's claim by:
/// `(max ?s)`.
pub const ELECTORS: [&str; 3] = ["last", "max", "min"];

/// The elector the notation no longer takes: `top` over a list is `max`
/// over the list reversed, or `min` over the same list. A document that writes it is refused.
pub const RETIRED: &str = "top";

/// The cardinalities a term may be read under: `(:one ?x)`, `(:many ?x)`.
pub const CARDINALITIES: [&str; 2] = ["one", "many"];

/// The wrapper that lists the cases of a variant, least first:
/// `(:among ?x cases: [:open :done])`.
pub const AMONG: &str = "among";

/// The layers a term may be read on: `(:control ?x)`. A claim on the
/// control layer holds for the tick it is asserted in and is gone after
/// it, which is a command's lifetime. Dialog-DB has no other layer yet.
pub const LAYERS: [&str; 1] = ["control"];

/// The layer a command's claims are on.
pub const CONTROL: &str = "control";

/// The folds a term may be: `(count ?t)`. Each makes one value of the
/// many a variable is bound to, a group at a time.
pub const FOLDS: [&str; 6] = ["count", "count-distinct", "sum", "min", "max", "avg"];

/// A term: what stands where a value does.
#[derive(Clone, Debug, PartialEq)]
pub enum Term {
    /// `?name`.
    Variable(String),
    /// `_`.
    Blank,
    /// `"text"`.
    Text(String),
    /// A number as written.
    Number(String),
    /// An entity: a URI, or a keyword, which is the entity `name:`.
    Entity(String),
    /// A name that stands for itself, as the domain does in
    /// `(:id xyz.todo …)`.
    Symbol(String),
    /// An application of a formula, standing for its `is:` value.
    Formula(Box<Formula>),
    /// A struct, `{key value}`, or a list, `[a b]` or `'(a b)`. With no
    /// variable in it, it is a value; with one, a pattern.
    Struct(Syntax),
    /// A fold, `(count ?t)`: one value made of every value its part is
    /// bound to. It stands in a field of a head that derives, and the
    /// rows are grouped by the entity and the head's other fields.
    Fold {
        /// Which fold: one of [`FOLDS`].
        apply: String,
        /// What is folded.
        of: Box<Term>,
    },
}

/// What a collection's key is: a name, which makes it a dictionary, or
/// a position, which makes it a sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Keyed {
    /// `by: :name`.
    Name,
    /// `by: :position`.
    Position,
}

impl Keyed {
    /// The key as a line says it: `name` or `position`.
    pub fn of(word: &str) -> Option<Keyed> {
        match word {
            "name" => Some(Keyed::Name),
            "position" => Some(Keyed::Position),
            _ => None,
        }
    }
}

/// One field of a read: an attribute, the term its value stands under,
/// and the type and pick it is read under.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    /// The label as written, which may name another domain's field.
    pub label: String,
    /// The attribute: `domain/name`.
    pub attribute: String,
    /// The value.
    pub term: Term,
    /// The type, when the term was written under one.
    pub kind: Option<String>,
    /// The pick, when the term was written under one.
    pub pick: Option<String>,
    /// The cardinality, `one` or `many`, when the term was written
    /// under one.
    pub cardinality: Option<String>,
    /// The cases of a variant the value is one of, least first, each the
    /// name of the entity it is: empty unless the term was written under
    /// `(:among …)`.
    pub listed: Vec<String>,
    /// The layer, when the term was written on one.
    pub layer: Option<String>,
    /// Where the field is every entry of a collection: the term its key
    /// stands under, and what a key is. Its attribute is then the
    /// collection's domain and `[symbol]` or `[position]`.
    pub key: Option<(Term, Keyed)>,
}

/// An application of a relation: one entity and the fields named.
#[derive(Clone, Debug, PartialEq)]
pub struct Read {
    /// The domain applied: for a derived attribute, the one Dialog-DB
    /// knows it by, `dialog.n.c.<hash>`.
    pub domain: String,
    /// For a derived attribute, the name it was written by,
    /// `list/count-open`.
    pub written: Option<String>,
    /// The entity.
    pub entity: Term,
    /// The fields, in the order written.
    pub fields: Vec<Field>,
}

/// An application of a formula.
#[derive(Clone, Debug, PartialEq)]
pub struct Formula {
    /// Its name: a keyword's, or an operator.
    pub name: String,
    /// The part with no label.
    pub subject: Option<Term>,
    /// The labelled parts. The label `:` is the empty text.
    pub labelled: Vec<(String, Term)>,
}

impl Formula {
    /// The term under `label`.
    pub fn at(&self, label: &str) -> Option<&Term> {
        self.labelled
            .iter()
            .find_map(|(name, term)| (name == label).then_some(term))
    }
}

/// A premise of a rule.
#[derive(Clone, Debug, PartialEq)]
pub enum Premise {
    /// A read of stored or derived facts.
    Read(Read),
    /// A computation or a check.
    Formula(Formula),
}

/// A head of a rule.
#[derive(Clone, Debug, PartialEq)]
pub enum Head {
    /// No word: the relation holds while the premises do.
    Derive(Read),
    /// `assert`: the facts stay until retracted.
    Assert(Read),
    /// `retract`: the facts are taken back.
    Retract(Read),
    /// `assert` of claims on the control layer, which is issuing a
    /// command: the facts hold for the next tick and for that tick only.
    /// How long a fact lasts belongs to its layer, not to the word.
    Hold(Read),
    /// `report`: the row is part of the answer.
    Report(Read),
}

impl Read {
    /// The fields the author named. A field an expansion added, under a
    /// variable of its own that no premise binds, is one the author
    /// left out, which a report does not show.
    pub fn named(&self, bound: &BTreeSet<String>) -> Vec<&Field> {
        self.fields
            .iter()
            .filter(|field| match &field.term {
                Term::Variable(name) => !crate::syntax::unwritten(name) || bound.contains(name),
                _ => true,
            })
            .collect()
    }
}

/// A kernel rule: one head, and what must and must not hold for it.
#[derive(Clone, Debug, PartialEq)]
pub struct Rule {
    /// What the rule says it is for.
    pub description: Option<String>,
    /// Whether it runs once, in its document, and is never installed:
    /// a write at the top level with a `when`.
    pub once: bool,
    /// Its head.
    pub head: Head,
    /// What must hold.
    pub when: Vec<Premise>,
    /// What must not.
    pub unless: Vec<Premise>,
}

/// What the reader makes of `'x`: `(quote x)`. Where a value stands, a
/// quoted `( )` is a list.
pub const QUOTE: &str = "quote";

pub(crate) fn term(node: &Syntax) -> Result<Term, Error> {
    Ok(match node {
        Syntax::Variable(name) => Term::Variable(name.clone()),
        Syntax::Blank => Term::Blank,
        Syntax::Constant(Value::String(text)) => Term::Text(text.clone()),
        Syntax::Constant(Value::UnsignedInt(natural)) => Term::Number(natural.to_string()),
        Syntax::Constant(Value::SignedInt(integer)) => Term::Number(integer.to_string()),
        Syntax::Constant(Value::Float(float)) => Term::Number(format!("{float:?}")),
        Syntax::Constant(Value::Boolean(truth)) => Term::Symbol(truth.to_string()),
        Syntax::Constant(Value::Entity(entity)) => Term::Entity(entity.as_str().to_string()),
        Syntax::Constant(Value::Symbol(_)) => Term::Symbol(node.to_string()),
        Syntax::Reference(name) => Term::Symbol(name.clone()),
        application if application.is_formula() => Term::Formula(Box::new(formula(application)?)),
        Syntax::Map(_) | Syntax::Tuple(_) => Term::Struct(node.clone()),
        // `'(1 2 3)`: quoted, a `( )` is the list of what it holds, the
        // same list `[1 2 3]` is.
        quoted if quoted.applies() == Some(QUOTE) => Term::Struct(node.clone()),
        Syntax::Application(_, operand)
            if node.applies().is_some_and(|word| FOLDS.contains(&word)) =>
        {
            let (apply, [of], true) = (
                node.applies().unwrap_or_default(),
                operand.items.as_slice(),
                operand.terms.is_empty(),
            ) else {
                return Err(Error::Refused(format!(
                    "{node} folds one thing: `(count ?t)`"
                )));
            };
            Term::Fold {
                apply: apply.to_string(),
                of: Box::new(term(of)?),
            }
        }
        Syntax::Application(..) => {
            return Err(Error::Refused(format!(
                "{node} is not a term: it applies no formula, and a list where a value stands \
                 is `[1 2 3]`, or quoted, `'(1 2 3)`"
            )));
        }
        other => return Err(Error::Refused(format!("{other} is not a term"))),
    })
}

fn formula(node: &Syntax) -> Result<Formula, Error> {
    let (Syntax::Application(operator, operand), true) = (node, node.is_formula()) else {
        return Err(Error::Refused(format!("{node} applies no formula")));
    };
    let name = match (operator.keyword(), &**operator) {
        (Some(name), _) => name.to_string(),
        (None, Syntax::Reference(name)) => name.clone(),
        _ => return Err(Error::Refused(format!("{node} applies no formula"))),
    };
    let subject = match operand.items.as_slice() {
        [] => None,
        [subject] => Some(term(subject)?),
        _ => {
            return Err(Error::Refused(format!(
                "{node} has more than one part without a label"
            )));
        }
    };
    let labelled = operand
        .terms
        .iter()
        .map(|(label, part)| Ok((label.clone(), term(part)?)))
        .collect::<Result<_, Error>>()?;
    Ok(Formula {
        name,
        subject,
        labelled,
    })
}

/// A field's term, with the type, pick, cardinality and layer it was
/// written under taken off: `(:control (:one (:text ?x)))` is `?x`,
/// text, cardinality one, on the control layer.
/// What a field's value is written under, taken off it: its type, its
/// pick, its cardinality, a variant's cases, its layer, and the key
/// where it is every entry of a collection; and the value under them.
pub(crate) struct Wrapped<'a> {
    /// The type, by name, and as written.
    pub kind: Option<(String, Syntax)>,
    pub pick: Option<String>,
    pub cardinality: Option<String>,
    /// The cases of a variant, by name, and as written.
    pub listed: Vec<String>,
    pub cases: Option<Syntax>,
    pub layer: Option<String>,
    /// The key's term and what a key is, by name and as read.
    pub key: Option<(&'a Syntax, String, Keyed)>,
    /// The value under every wrapper.
    pub inner: &'a Syntax,
}

/// The wrappers of a field's value taken off it, as the kernel reads
/// them: `(:entry …)` outermost, then any of a type, a pick, a
/// cardinality, `(:among …)` and a layer, each once at most.
pub(crate) fn wrapped(mut node: &Syntax) -> Result<Wrapped<'_>, Error> {
    let mut wrapped = Wrapped {
        kind: None,
        pick: None,
        cardinality: None,
        listed: Vec::new(),
        cases: None,
        layer: None,
        key: None,
        inner: node,
    };
    // `(:entry ?x key: ?p by: :position)`: every entry of a collection.
    if let Syntax::Application(operator, operand) = node
        && operator.keyword() == Some("entry")
    {
        let by = operand.terms.get("by").and_then(Syntax::keyword);
        let (Some(at), Some(by), Some(keyed), [inner]) = (
            operand.terms.get("key"),
            by,
            by.and_then(Keyed::of),
            operand.items.as_slice(),
        ) else {
            return Err(Error::Refused(format!(
                "{node} is an entry: `(:entry value key: term by: :name)`"
            )));
        };
        wrapped.key = Some((at, by.to_string(), keyed));
        node = inner;
    }
    while let Syntax::Application(operator, operand) = node {
        if let (Some(AMONG), [inner], Some(cases @ Syntax::Tuple(listed)), 1) = (
            operator.keyword(),
            operand.items.as_slice(),
            operand.terms.get("cases"),
            operand.terms.len(),
        ) {
            if wrapped.cases.is_some() {
                return Err(Error::Refused(format!(
                    "{node} is written under two variants: a value has one"
                )));
            }
            wrapped.listed = listed
                .iter()
                .map(|case| {
                    case.keyword().map(str::to_string).ok_or_else(|| {
                        Error::Refused(format!("{case} is not a case: a case is a keyword"))
                    })
                })
                .collect::<Result<_, Error>>()?;
            wrapped.cases = Some(cases.clone());
            node = inner;
            continue;
        }
        let (Some(word), [inner], true) = (
            operator.keyword(),
            operand.items.as_slice(),
            operand.terms.is_empty(),
        ) else {
            break;
        };
        // Each kind of wrapper once: an inner one would say otherwise
        // what the outer one said.
        let twice = |held: bool, what: &str| match held {
            true => Err(Error::Refused(format!(
                "{node} is written under two {what}s: a value has one"
            ))),
            false => Ok(()),
        };
        if TYPES.contains(&word) {
            twice(wrapped.kind.is_some(), "type")?;
            wrapped.kind = Some((word.to_string(), (**operator).clone()));
        } else if PICKS.contains(&word) {
            twice(wrapped.pick.is_some(), "pick")?;
            wrapped.pick = Some(word.to_string());
        } else if CARDINALITIES.contains(&word) {
            twice(wrapped.cardinality.is_some(), "cardinalitie")?;
            wrapped.cardinality = Some(word.to_string());
        } else if LAYERS.contains(&word) {
            twice(wrapped.layer.is_some(), "layer")?;
            wrapped.layer = Some(word.to_string());
        } else {
            break;
        }
        node = inner;
    }
    wrapped.inner = node;
    Ok(wrapped)
}

fn field(domain: &str, label: &str, node: &Syntax) -> Result<Field, Error> {
    let wrapped = wrapped(node)?;
    Ok(Field {
        label: label.to_string(),
        // A label with a `/` names another relation's field, as the
        // notation joins reads of one entity: that relation's domain, a
        // derived attribute's as Dialog-DB knows it.
        attribute: match label.rsplit_once('/') {
            Some((relation, name)) => match syntax::derived_parts(relation) {
                Some((concept, field)) => format!("{}/{name}", syntax::derived(concept, field)?),
                None => label.to_string(),
            },
            None => format!("{domain}/{label}"),
        },
        term: term(wrapped.inner)?,
        kind: wrapped.kind.map(|(kind, _)| kind),
        pick: wrapped.pick,
        cardinality: wrapped.cardinality,
        listed: wrapped.listed,
        layer: wrapped.layer,
        key: wrapped
            .key
            .map(|(at, _, keyed)| Ok::<_, Error>((term(at)?, keyed)))
            .transpose()?,
    })
}

pub(crate) fn read(node: &Syntax) -> Result<Read, Error> {
    let (Some(domain), Some(operand), true) = (node.applies(), node.operand(), node.is_read())
    else {
        // A line of no domain, `memo/note` with no concept `memo/note`
        // declared, comes here as `(memo ?t note: …)`: it is said as it
        // was written.
        if let (Some(domain), Some(operand)) = (node.applies(), node.operand())
            && syntax::named(domain) == syntax::Named::Declarable
            && !domain.contains('/')
            && let Some(label) = operand.terms.keys().next()
        {
            return Err(Error::Refused(format!(
                "`{domain}/{label}` names no attribute of a domain, which holds a `.`, \
                 `xyz.{domain}/{label}`, and no concept or derived attribute `{domain}/{label}` \
                 is declared above what reads it"
            )));
        }
        return Err(Error::Refused(format!("{node} applies no relation")));
    };
    // A trailing `...` says the other fields are left out on purpose. A
    // relation has no list of its fields, so it reads the same with it
    // and without.
    let dots = |part: &Syntax| matches!(part, Syntax::Reference(name) if name == REST);
    let items = match operand.items.as_slice() {
        [items @ .., last] if dots(last) => items,
        [items @ .., last, Syntax::Blank] if dots(last) => items,
        items => items,
    };
    let entity = match items {
        [entity] => term(entity)?,
        _ => {
            return Err(Error::Refused(format!(
                "{node} needs its entity, and nothing else without a label"
            )));
        }
    };
    // A derived attribute is named by what was written, and Dialog-DB
    // knows it by the hash of that, made here.
    let (domain, written) = match syntax::derived_parts(domain) {
        Some((concept, field)) => (
            syntax::derived(concept, field)?,
            Some(format!("{concept}/{field}")),
        ),
        None => (domain.to_string(), None),
    };
    let domain = domain.as_str();
    let fields = operand
        .terms
        .iter()
        .map(|(label, part)| field(domain, label, part))
        .collect::<Result<Vec<_>, _>>()?;
    if fields.iter().any(|field| field.label == "of") {
        return Err(Error::Refused(format!(
            "{node} says `of:` twice: the part with no label is `of:`"
        )));
    }
    if fields.is_empty() {
        return Err(Error::Refused(format!("{node} names no field")));
    }
    // A derived attribute is what rules conclude, on the default layer:
    // a command is a concept of its own.
    if let Some(written) = &written
        && let Some(layer) = fields.iter().find_map(|field| field.layer.as_deref())
    {
        return Err(Error::Refused(format!(
            "{node} has `{written}`, a derived attribute, on the {layer} layer: a derived \
             attribute is what rules conclude, on the default layer, and is read there; a \
             command is a concept of its own, its lines on the {layer} layer, \
             `(concept ({written}-go …) (xyz.… in: :{layer} …))`"
        )));
    }
    Ok(Read {
        domain: domain.to_string(),
        written,
        entity,
        fields,
    })
}

pub(crate) fn premise(node: &Syntax) -> Result<Premise, Error> {
    if node.is_read() {
        Ok(Premise::Read(read(node)?))
    } else if node.is_formula() {
        Ok(Premise::Formula(formula(node)?))
    } else {
        Err(Error::Refused(format!("{node} is not a kernel premise")))
    }
}

/// Whether a read's claims are a command's: every field on the control
/// layer. Some on it and some not is a write the design does not say
/// the meaning of.
fn control(node: &Syntax, read: &Read) -> Result<bool, Error> {
    let on = read
        .fields
        .iter()
        .filter(|field| field.layer.as_deref() == Some(CONTROL))
        .count();
    match on {
        0 => Ok(false),
        all if all == read.fields.len() => Ok(true),
        _ => Err(Error::Unsupported(format!(
            "{node} holds claims on the control layer beside stored ones: what such a \
             command is, and what it keeps, is not decided"
        ))),
    }
}

fn head(node: &Syntax) -> Result<Head, Error> {
    if node.is_read() {
        // A command concluded with no word is a deduction: it holds in
        // this tick while the premises do, for other rules to read.
        let read = read(node)?;
        control(node, &read)?;
        return Ok(Head::Derive(read));
    }
    if node.applies() == Some("all") {
        return Err(Error::Refused(format!(
            "{node} is several heads, and a kernel rule has one"
        )));
    }
    let inner = match node.operand().map(|operand| operand.items.as_slice()) {
        Some([inner]) => read(inner)?,
        _ => return Err(Error::Refused(format!("{node} is not a kernel head"))),
    };
    if matches!(node.applies(), Some("assert" | "retract")) && inner.written.is_some() {
        return Err(Error::Refused(format!(
            "{node} writes an attribute in a concept's domain, which rules conclude and nothing \
             writes: derive it with a rule, `(rule (list/count-open ?l ?n) …)`"
        )));
    }
    let command = control(node, &inner)?;
    match (node.applies(), command) {
        (Some("assert"), true) => Ok(Head::Hold(inner)),
        (Some("assert"), false) => Ok(Head::Assert(inner)),
        (Some("retract"), true) => Err(Error::Unsupported(format!(
            "{node} retracts a command: what that means is not decided, since a command \
             holds for one tick and nothing keeps it"
        ))),
        (Some("retract"), false) => Ok(Head::Retract(inner)),
        (Some("report"), _) => Ok(Head::Report(inner)),
        _ => Err(Error::Refused(format!("{node} is not a kernel head"))),
    }
}

/// The entity a command's claims derive when they are concluded of an
/// entity nothing names: `(:id domain label: term …)` over the claims
/// about to be written. Identical commands in one tick are one.
fn derived(read: &Read) -> Term {
    Term::Formula(Box::new(Formula {
        name: "id".into(),
        subject: Some(Term::Symbol(read.domain.clone())),
        labelled: read
            .fields
            .iter()
            .map(|field| (field.label.clone(), field.term.clone()))
            .collect(),
    }))
}

impl Rule {
    /// Read an analyzed tree: a `(rule …)` whose every head and premise
    /// is kernel.
    pub fn parse(node: &Syntax) -> Result<Self, Error> {
        let (Some("rule"), Some(operand)) = (node.applies(), node.operand()) else {
            return Err(Error::Refused(format!("{node} is not a rule")));
        };
        let description = match operand.terms.get("description") {
            Some(Syntax::Constant(Value::String(text))) => Some(text.clone()),
            _ => None,
        };
        // The first part is the head. Every part after it is a premise,
        // and what `(unless …)` holds must not hold.
        let members = |clause: &Syntax| clause.operand().map(|operand| operand.items.clone());
        let Some((first, premises)) = operand.items.split_first() else {
            return Err(Error::Refused(format!("{node} has no head")));
        };
        let once = matches!(
            operand.terms.get("once"),
            Some(Syntax::Constant(Value::Boolean(true)))
        );
        let mut rule = Rule {
            description,
            once,
            head: head(first)?,
            when: Vec::new(),
            unless: Vec::new(),
        };
        for part in premises {
            if part.applies() == Some("unless") {
                for negated in members(part).unwrap_or_default() {
                    rule.unless.push(premise(&negated)?);
                }
            } else {
                rule.when.push(premise(part)?);
            }
        }
        // A command's head names no entity. Issued or deduced, its
        // entity is derived from what it says.
        let bound = rule.bound();
        if let Head::Hold(read) | Head::Derive(read) = &mut rule.head
            && read.fields.iter().all(|field| field.layer.is_some())
            && match &read.entity {
                Term::Variable(name) => !bound.contains(name),
                Term::Blank => true,
                _ => false,
            }
        {
            read.entity = derived(read);
        }
        Ok(rule)
    }

    /// Every variable the premises may bind.
    pub fn bound(&self) -> BTreeSet<String> {
        fn term(part: &Term, names: &mut BTreeSet<String>) {
            match part {
                Term::Variable(name) => {
                    names.insert(name.clone());
                }
                Term::Formula(inner) => formula(inner, names),
                Term::Struct(form) => {
                    let mut inside = Vec::new();
                    form.variables(&mut inside);
                    names.extend(inside);
                }
                Term::Fold { of, .. } => term(of, names),
                _ => {}
            }
        }
        fn formula(formula: &Formula, names: &mut BTreeSet<String>) {
            formula
                .subject
                .iter()
                .for_each(|subject| term(subject, names));
            formula
                .labelled
                .iter()
                .for_each(|(_, part)| term(part, names));
        }
        let mut names = BTreeSet::new();
        for premise in &self.when {
            match premise {
                Premise::Read(read) => {
                    term(&read.entity, &mut names);
                    for field in &read.fields {
                        term(&field.term, &mut names);
                        if let Some((key, _)) = &field.key {
                            term(key, &mut names);
                        }
                    }
                }
                Premise::Formula(inner) => formula(inner, &mut names),
            }
        }
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse as forms;

    #[test]
    fn it_reads_a_rule_into_its_head_and_premises() {
        let form = forms(
            "(rule (assert (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
               (xyz.counter.increment _ counter: (:control ?c))
               (xyz.counter ?c count: (:last (:natural ?n))))",
        )
        .unwrap()
        .remove(0);
        let rule = Rule::parse(&form).unwrap();
        let Head::Assert(head) = &rule.head else {
            panic!("the head asserts");
        };
        assert_eq!(head.fields[0].attribute, "xyz.counter/count");
        assert_eq!(head.fields[0].kind.as_deref(), Some("natural"));
        assert_eq!(head.fields[0].pick.as_deref(), Some("last"));
        assert!(matches!(&head.fields[0].term, Term::Formula(formula) if formula.name == "+"));
        assert_eq!(rule.when.len(), 2);
    }

    #[test]
    fn it_reads_a_keyword_as_an_entity_and_a_field_of_another_domain_whole() {
        let form = forms("(rule (report (io.person ?p state: :open io.contact/email: ?e)))")
            .unwrap()
            .remove(0);
        let rule = Rule::parse(&form).unwrap();
        let Head::Report(head) = &rule.head else {
            panic!("the head reports");
        };
        // The fields come by label, since a tree keeps no order of them.
        assert_eq!(head.fields[0].attribute, "io.contact/email");
        assert_eq!(head.fields[1].term, Term::Entity("open:".into()));
    }

    #[test]
    fn it_refuses_what_is_not_kernel() {
        let form = forms("(rule (assert (todo ?t)))").unwrap().remove(0);
        assert!(matches!(Rule::parse(&form), Err(Error::Refused(_))));
    }

    /// How long a fact lasts belongs to its layer: `assert` of claims
    /// on the control layer issues a command, and the same head with no
    /// word deduces one. Either way an entity nothing names is derived
    /// from what the command says. A domain's name says nothing of it.
    #[test]
    fn it_issues_what_an_assert_on_the_control_layer_says() {
        let head = |text: &str| Rule::parse(&forms(text).unwrap().remove(0)).map(|rule| rule.head);
        let derived = |read: &Read| {
            matches!(&read.entity, Term::Formula(formula)
                if formula.name == "id"
                    && formula.subject == Some(Term::Symbol(read.domain.clone()))
                    && formula.labelled == vec![("n".to_string(), Term::Variable("n".into()))])
        };
        assert!(matches!(
            head("(rule (assert (xyz.go ?c n: (:control ?n))) (xyz.c ?d n: ?n))"),
            Ok(Head::Hold(read)) if read.domain == "xyz.go" && derived(&read)
        ));
        assert!(matches!(
            head("(rule (xyz.go _ n: (:control ?n)) (xyz.c ?d n: ?n))"),
            Ok(Head::Derive(read)) if derived(&read)
        ));
        // An entity a premise binds is the one concluded.
        assert!(matches!(
            head("(rule (xyz.go ?d n: (:control ?n)) (xyz.c ?d n: ?n))"),
            Ok(Head::Derive(read)) if read.entity == Term::Variable("d".into())
        ));
        assert!(matches!(
            head("(rule (assert (xyz.go ?c n: ?n)) (xyz.c ?c n: ?n))"),
            Ok(Head::Assert(read)) if read.entity == Term::Variable("c".into())
        ));
        assert!(matches!(
            head("(rule (command.go.x ?c n: ?n) (xyz.c ?c n: ?n))"),
            Ok(Head::Derive(_))
        ));
    }

    /// What retracting a command means, and what a write of control
    /// claims beside stored ones is, are not decided: both are said to
    /// be unsupported.
    #[test]
    fn it_refuses_what_is_not_decided_of_a_command() {
        let head = |text: &str| Rule::parse(&forms(text).unwrap().remove(0)).map(|rule| rule.head);
        assert!(matches!(
            head("(rule (retract (xyz.go ?c n: (:control ?n))) (xyz.c ?c n: ?n))"),
            Err(Error::Unsupported(why)) if why.contains("retracts a command")
        ));
        for text in [
            "(rule (assert (xyz.go ?c n: (:control ?n) m: ?n)) (xyz.c ?c n: ?n))",
            "(rule (xyz.go ?c n: (:control ?n) m: ?n) (xyz.c ?c n: ?n))",
        ] {
            assert!(
                matches!(head(text), Err(Error::Unsupported(why)) if why.contains("beside stored ones")),
                "{text}"
            );
        }
    }

    /// A fold is a term: one value made of every value its part is
    /// bound to.
    #[test]
    fn it_reads_a_fold_where_a_term_stands() {
        let form = forms(
            "(rule (dialog.n.c.listcountopen ?l is: (count ?t))
               (xyz.todo ?t list: ?l))",
        )
        .unwrap()
        .remove(0);
        let rule = Rule::parse(&form).unwrap();
        let Head::Derive(head) = &rule.head else {
            panic!("the head derives");
        };
        assert_eq!(
            head.fields[0].term,
            Term::Fold {
                apply: "count".into(),
                of: Box::new(Term::Variable("t".into()))
            }
        );
        for unread in ["(count)", "(count ?t ?u)", "(count ?t by: ?l)"] {
            let form = forms(&format!(
                "(rule (dialog.n.c.listcountopen ?l is: {unread}) (xyz.todo ?t list: ?l))"
            ))
            .unwrap()
            .remove(0);
            assert!(
                matches!(Rule::parse(&form), Err(Error::Refused(why)) if why.contains("folds one thing")),
                "{unread}"
            );
        }
    }

    /// A concept's domain is concluded only: a rule derives what is in
    /// it, and a write of it, asserted or retracted, is refused.
    #[test]
    fn it_refuses_a_write_in_a_concepts_domain() {
        let head = |text: &str| Rule::parse(&forms(text).unwrap().remove(0)).map(|rule| rule.head);
        for text in [
            "(rule (assert (dialog.n.c/list/count-open list:home is: 3)))",
            "(rule (assert (dialog.n.c/list/count-open ?l is: ?n)) (xyz.n ?l n: ?n))",
            "(rule (retract (dialog.n.c/list/count-open ?l is: ?n)) (xyz.n ?l n: ?n))",
        ] {
            assert!(
                matches!(head(text), Err(Error::Refused(why)) if why.contains("rules conclude and nothing writes")),
                "{text}"
            );
        }
        assert!(matches!(
            head("(rule (dialog.n.c.listcountopen ?l is: ?n) (xyz.n ?l n: ?n))"),
            Ok(Head::Derive(_))
        ));
        // A domain that only starts with the word is no concept's.
        assert!(matches!(
            head("(rule (assert (concepts.x ?l is: ?n)) (xyz.n ?l n: ?n))"),
            Ok(Head::Assert(_))
        ));
    }

    /// A kernel rule has one head: no head and several are both refused.
    #[test]
    fn it_refuses_a_rule_of_no_head_and_of_several() {
        let none = forms("(rule)").unwrap().remove(0);
        assert!(
            matches!(Rule::parse(&none), Err(Error::Refused(why)) if why.contains("has no head"))
        );
        let several = forms(
            "(rule (all (assert (xyz.a ?a n: ?n)) (retract (xyz.b ?a n: ?n))) (xyz.c ?a n: ?n))",
        )
        .unwrap()
        .remove(0);
        assert!(
            matches!(Rule::parse(&several), Err(Error::Refused(why)) if why.contains("has one"))
        );
    }

    /// A value written under two wrappers of one kind is refused, where
    /// the inner one said otherwise what the outer one said.
    #[test]
    fn it_refuses_a_value_under_two_wrappers_of_one_kind() {
        for text in [
            "(xyz.n ?e v: (:text (:integer ?x)))",
            "(xyz.n ?e v: (:one (:many ?x)))",
            "(xyz.n ?e v: (:last (:all ?x)))",
            "(xyz.n ?e v: (:control (:control ?x)))",
        ] {
            let tree = crate::parse(text).unwrap().remove(0);
            assert!(
                matches!(read(&tree), Err(Error::Refused(why)) if why.contains("written under two")),
                "{text}"
            );
        }
        let tree = crate::parse("(xyz.n ?e v: (:one (:text ?x)))")
            .unwrap()
            .remove(0);
        assert!(read(&tree).is_ok());
    }
}
