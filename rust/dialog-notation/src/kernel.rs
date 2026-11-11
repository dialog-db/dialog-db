//! The kernel, typed: what an expanded rule is made of.
//!
//! Expansion leaves a `(rule …)` form whose every head and premise
//! applies a relation or a formula. This module reads that form into
//! the shapes Dialog-DB's own descriptors have, and refuses what is not
//! kernel.

use crate::Error;
use crate::form::{Form, Kind, Part};

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

/// A kernel rule.
#[derive(Clone, Debug, PartialEq)]
pub struct Rule {
    /// What the rule says it is for.
    pub description: Option<String>,
    /// Its heads.
    pub heads: Vec<Head>,
    /// What must hold.
    pub when: Vec<Premise>,
    /// What must not.
    pub unless: Vec<Premise>,
}

fn term(form: &Form) -> Result<Term, Error> {
    Ok(match form {
        Form::Variable(name) => Term::Variable(name.clone()),
        Form::Blank => Term::Blank,
        Form::Text(text) => Term::Text(text.clone()),
        Form::Number(number) => Term::Number(number.clone()),
        Form::Keyword(name) => Term::Entity(format!("{name}:")),
        Form::Symbol(name) if form.kind() == Some(Kind::Entity) => Term::Entity(name.clone()),
        Form::Symbol(name) => Term::Symbol(name.clone()),
        application if application.is_formula() => Term::Formula(Box::new(formula(application)?)),
        Form::Map(_) => {
            return Err(Error::Unsupported(format!(
                "{form} where a term stands: a struct is a value and, with a variable in it, a \
                 pattern, and Dialog-DB has no record values yet"
            )));
        }
        other => return Err(Error::Refused(format!("{other} is not a term"))),
    })
}

fn formula(form: &Form) -> Result<Formula, Error> {
    let name = match form.as_application() {
        Some((Form::Keyword(name) | Form::Symbol(name), _)) => name.clone(),
        _ => return Err(Error::Refused(format!("{form} applies no formula"))),
    };
    let subject = match form.bare().as_slice() {
        [] => None,
        [subject] => Some(term(subject)?),
        _ => {
            return Err(Error::Refused(format!(
                "{form} has more than one part without a label"
            )));
        }
    };
    let labelled = form
        .labelled()
        .into_iter()
        .map(|(label, part)| Ok((label.to_string(), term(part)?)))
        .collect::<Result<_, Error>>()?;
    Ok(Formula {
        name,
        subject,
        labelled,
    })
}

/// A field's term, with the type and pick it was written under taken
/// off: `(:last (:text ?x))` is `?x`, text, last.
fn field(domain: &str, label: &str, mut form: &Form) -> Result<Field, Error> {
    let mut kind = None;
    let mut pick = None;
    while let Some((Form::Keyword(word), [Part::Bare(inner)])) = form.as_application() {
        if TYPES.contains(&word.as_str()) {
            kind = Some(word.clone());
        } else if PICKS.contains(&word.as_str()) {
            pick = Some(word.clone());
        } else {
            break;
        }
        form = inner;
    }
    Ok(Field {
        label: label.to_string(),
        attribute: if label.contains('/') {
            label.to_string()
        } else {
            format!("{domain}/{label}")
        },
        term: term(form)?,
        kind,
        pick,
    })
}

fn read(form: &Form) -> Result<Read, Error> {
    let domain = form
        .head()
        .filter(|_| form.is_read())
        .ok_or_else(|| Error::Refused(format!("{form} applies no relation")))?
        .to_string();
    let entity = match form.bare().as_slice() {
        [entity] => term(entity)?,
        _ => {
            return Err(Error::Refused(format!(
                "{form} needs its entity, and nothing else without a label"
            )));
        }
    };
    let fields = form
        .labelled()
        .into_iter()
        .map(|(label, part)| field(&domain, label, part))
        .collect::<Result<Vec<_>, _>>()?;
    if fields.iter().any(|field| field.label == "of") {
        return Err(Error::Refused(format!(
            "{form} says `of:` twice: the part with no label is `of:`"
        )));
    }
    if fields.is_empty() {
        return Err(Error::Refused(format!("{form} names no field")));
    }
    Ok(Read {
        domain,
        entity,
        fields,
    })
}

fn premise(form: &Form) -> Result<Premise, Error> {
    if form.is_read() {
        Ok(Premise::Read(read(form)?))
    } else if form.is_formula() {
        Ok(Premise::Formula(formula(form)?))
    } else {
        Err(Error::Refused(format!("{form} is not a kernel premise")))
    }
}

fn head(form: &Form) -> Result<Head, Error> {
    if form.is_read() {
        return Ok(Head::Derive(read(form)?));
    }
    let inner = match form.bare().as_slice() {
        [inner] => read(inner)?,
        _ => return Err(Error::Refused(format!("{form} is not a kernel head"))),
    };
    match form.head() {
        Some("assert!") => Ok(Head::Assert(inner)),
        Some("retract!") => Ok(Head::Retract(inner)),
        Some("assert") => Ok(Head::Hold(inner)),
        Some("report") => Ok(Head::Report(inner)),
        _ => Err(Error::Refused(format!("{form} is not a kernel head"))),
    }
}

impl Rule {
    /// Read an expanded `(rule …)` form.
    pub fn parse(form: &Form) -> Result<Self, Error> {
        if !form.is("rule") {
            return Err(Error::Refused(format!("{form} is not a rule")));
        }
        let clauses = crate::expand::Clauses::of(form);
        let description = clauses.said.iter().find_map(|(label, said)| match said {
            Form::Text(text) if label == "description" => Some(text.clone()),
            _ => None,
        });
        Ok(Rule {
            description,
            heads: clauses.heads.iter().map(head).collect::<Result<_, _>>()?,
            when: clauses.when.iter().map(premise).collect::<Result<_, _>>()?,
            unless: clauses
                .unless
                .iter()
                .map(premise)
                .collect::<Result<_, _>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::read as forms;

    #[test]
    fn it_reads_a_rule_into_heads_and_premises() {
        let form = forms(
            "(rule (assert! (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
               (when (command.increment.xyz.counter _ about: ?c)
                     (xyz.counter ?c count: (:last (:natural ?n)))))",
        )
        .unwrap()
        .remove(0);
        let rule = Rule::parse(&form).unwrap();
        let Head::Assert(head) = &rule.heads[0] else {
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
        let Head::Report(head) = &rule.heads[0] else {
            panic!("the head reports");
        };
        assert_eq!(head.fields[0].term, Term::Entity("open:".into()));
        assert_eq!(head.fields[1].attribute, "io.contact/email");
    }

    #[test]
    fn it_refuses_what_is_not_kernel() {
        let form = forms("(rule (assert! (todo ?t)))").unwrap().remove(0);
        assert!(Rule::parse(&form).is_err());
    }
}
