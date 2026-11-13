//! The kernel, typed: what an expanded rule is made of.
//!
//! Analysis leaves a `(rule …)` tree whose every head and premise
//! applies a relation or a formula. This module reads that tree into
//! the shapes Dialog-DB's own descriptors have, and refuses what is not
//! kernel.

use std::collections::BTreeSet;

use dialog_corpus::Value;

use crate::Error;
use crate::syntax::Syntax;

/// The types a term may be read under: `(:text ?x)`.
pub const TYPES: [&str; 8] = [
    "text", "natural", "integer", "float", "boolean", "entity", "bytes", "symbol",
];

/// The picks a term may be read under: `(:last ?x)`.
pub const PICKS: [&str; 4] = ["last", "all", "max", "min"];

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
    /// A struct, `{key value}`, or a tuple, `[a b]`. With no variable in
    /// it, it is a value; with one, a pattern.
    Struct(Syntax),
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
}

/// An application of a relation: one entity and the fields named.
#[derive(Clone, Debug, PartialEq)]
pub struct Read {
    /// The domain applied.
    pub domain: String,
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
    /// `assert!`: the facts stay until retracted.
    Assert(Read),
    /// `retract!`: the facts are taken back.
    Retract(Read),
    /// `assert`: the facts hold for one round.
    Hold(Read),
    /// `report`: the row is part of the answer.
    Report(Read),
}

impl Head {
    /// The relation the head is about, whatever it does with it.
    pub fn read(&self) -> &Read {
        match self {
            Head::Derive(read)
            | Head::Assert(read)
            | Head::Retract(read)
            | Head::Hold(read)
            | Head::Report(read) => read,
        }
    }
}

impl Read {
    /// The fields the author named. A field an expansion added, under a
    /// variable of its own that no premise binds, is one the author
    /// left out, which a report does not show.
    pub fn named(&self, bound: &BTreeSet<String>) -> Vec<&Field> {
        self.fields
            .iter()
            .filter(|field| match &field.term {
                Term::Variable(name) => !name.contains('.') || bound.contains(name),
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

fn term(node: &Syntax) -> Result<Term, Error> {
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
        // `'(1 2 3)`: quoted, a `( )` is the list of what it holds.
        quoted if quoted.applies() == Some(QUOTE) => Term::Struct(node.clone()),
        Syntax::Application(..) => {
            return Err(Error::Refused(format!(
                "{node} is not a term: it applies no formula, and a list where a value stands \
                 is quoted, `'(1 2 3)`"
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

/// A field's term, with the type and pick it was written under taken
/// off: `(:last (:text ?x))` is `?x`, text, last.
fn field(domain: &str, label: &str, mut node: &Syntax) -> Result<Field, Error> {
    let mut kind = None;
    let mut pick = None;
    while let Syntax::Application(operator, operand) = node {
        let (Some(word), [inner], true) = (
            operator.keyword(),
            operand.items.as_slice(),
            operand.terms.is_empty(),
        ) else {
            break;
        };
        if TYPES.contains(&word) {
            kind = Some(word.to_string());
        } else if PICKS.contains(&word) {
            pick = Some(word.to_string());
        } else {
            break;
        }
        node = inner;
    }
    Ok(Field {
        label: label.to_string(),
        attribute: if label.contains('/') {
            label.to_string()
        } else {
            format!("{domain}/{label}")
        },
        term: term(node)?,
        kind,
        pick,
    })
}

fn read(node: &Syntax) -> Result<Read, Error> {
    let (Some(domain), Some(operand), true) = (node.applies(), node.operand(), node.is_read())
    else {
        return Err(Error::Refused(format!("{node} applies no relation")));
    };
    let entity = match operand.items.as_slice() {
        [entity] => term(entity)?,
        _ => {
            return Err(Error::Refused(format!(
                "{node} needs its entity, and nothing else without a label"
            )));
        }
    };
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
    Ok(Read {
        domain: domain.to_string(),
        entity,
        fields,
    })
}

fn premise(node: &Syntax) -> Result<Premise, Error> {
    if node.is_read() {
        Ok(Premise::Read(read(node)?))
    } else if node.is_formula() {
        Ok(Premise::Formula(formula(node)?))
    } else {
        Err(Error::Refused(format!("{node} is not a kernel premise")))
    }
}

fn head(node: &Syntax) -> Result<Head, Error> {
    if node.is_read() {
        return Ok(Head::Derive(read(node)?));
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
    match node.applies() {
        Some("assert!") => Ok(Head::Assert(inner)),
        Some("retract!") => Ok(Head::Retract(inner)),
        Some("assert") => Ok(Head::Hold(inner)),
        Some("report") => Ok(Head::Report(inner)),
        _ => Err(Error::Refused(format!("{node} is not a kernel head"))),
    }
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
        let mut rule = Rule {
            description,
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
        Ok(rule)
    }

    /// Every variable the premises may bind.
    pub fn bound(&self) -> BTreeSet<String> {
        fn term(term: &Term, names: &mut BTreeSet<String>) {
            match term {
                Term::Variable(name) => {
                    names.insert(name.clone());
                }
                Term::Formula(inner) => formula(inner, names),
                Term::Struct(form) => {
                    let mut inside = Vec::new();
                    form.variables(&mut inside);
                    names.extend(inside);
                }
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
                    read.fields
                        .iter()
                        .for_each(|field| term(&field.term, &mut names));
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
            "(rule (assert! (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
               (command.increment.xyz.counter _ about: ?c)
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
        let form = forms("(rule (assert! (todo ?t)))").unwrap().remove(0);
        assert!(matches!(Rule::parse(&form), Err(Error::Refused(_))));
    }

    /// A kernel rule has one head: no head and several are both refused.
    #[test]
    fn it_refuses_a_rule_of_no_head_and_of_several() {
        let none = forms("(rule)").unwrap().remove(0);
        assert!(
            matches!(Rule::parse(&none), Err(Error::Refused(why)) if why.contains("has no head"))
        );
        let several = forms(
            "(rule (all (assert! (xyz.a ?a n: ?n)) (retract! (xyz.b ?a n: ?n))) (xyz.c ?a n: ?n))",
        )
        .unwrap()
        .remove(0);
        assert!(
            matches!(Rule::parse(&several), Err(Error::Refused(why)) if why.contains("has one"))
        );
    }
}
