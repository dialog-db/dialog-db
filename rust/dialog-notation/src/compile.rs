//! Compiling a kernel rule to what Dialog-DB runs.
//!
//! Nothing here evaluates anything. A kernel rule becomes the
//! descriptors Dialog-DB already has, and Dialog-DB plans and runs
//! them:
//!
//! | kernel | Dialog-DB |
//! |---|---|
//! | `(assert! A)`, `(retract! A)` with no premise | a [`ConceptStatement`], asserted or retracted |
//! | `(assert A)` with no premise | a [`ConceptStatement`], dispatched: it holds one round |
//! | a bare head with premises | a [`DeductiveRule`] |
//! | `(assert! A)`, `(retract! A)` with premises | an [`InductiveRule`] |
//! | `(report A)` with one read as its premise | a [`ConceptQuery`] |
//! | `(report A)` with any other premises | a [`DeductiveRule`] deriving the report, and a [`ConceptQuery`] of it |
//!
//! A rule of several heads becomes one Dialog-DB rule a head, since a
//! Dialog-DB rule has one. What Dialog-DB has no counterpart for comes
//! back as [`Error::Unsupported`], never as something close.

use std::collections::{BTreeMap, BTreeSet};

use dialog_corpus::{Entity, Value};
use dialog_query::concept::descriptor::ConceptStatement;
use dialog_query::{
    Any, AttributeDescriptor, ConceptConclusion, ConceptQuery, DeductiveRule, InductiveRule,
    Term as Variable,
};
use serde_json::{Map, Value as Json, json};

use crate::Error;
use crate::form::{Form, Part};
use crate::kernel::{Field, Formula, Head, Premise, Read, Rule, TYPES, Term};

/// One thing to do with a branch.
#[derive(Debug, Clone)]
pub enum Step {
    /// Facts to assert.
    Assert(ConceptStatement),
    /// Facts to retract.
    Retract(ConceptStatement),
    /// A command to issue: facts that hold for one round.
    Issue(ConceptStatement),
    /// A rule to install, deriving a relation.
    Deduce(Box<DeductiveRule>),
    /// A rule to install, writing when its premises come to hold.
    Induce(Box<InductiveRule>),
    /// A query to run.
    Query(Box<Query>),
}

/// Where a part of a reported row comes from.
#[derive(Debug, Clone)]
enum Slot {
    /// The row says this, whatever matched.
    Is(Form),
    /// The row says what this variable was bound to.
    Bound(String),
}

/// A query: what to select, and how a match reads as a row.
#[derive(Debug, Clone)]
pub struct Query {
    /// The rule deriving the report, when the premises are more than
    /// one read. It is laid over the branch for this query alone.
    pub rule: Option<DeductiveRule>,
    /// What to select.
    pub select: ConceptQuery,
    domain: String,
    entity: Slot,
    fields: Vec<(String, Slot)>,
}

impl Query {
    /// A match, as the head the query reports with its variables
    /// replaced by what they were bound to.
    pub fn row(&self, conclusion: &ConceptConclusion) -> Result<Form, Error> {
        let fill = |slot: &Slot| match slot {
            Slot::Is(form) => Ok(form.clone()),
            Slot::Bound(name) => conclusion
                .source()
                .lookup(&Variable::<Any>::var(name.as_str()))
                .and_then(|binding| binding.content())
                .map(|value| written(&value))
                .map_err(|error| Error::Refused(format!("a match binds no {name}: {error}"))),
        };
        let mut parts = vec![Part::Bare(fill(&self.entity)?)];
        for (label, slot) in &self.fields {
            parts.push(Part::Labelled(label.clone(), fill(slot)?));
        }
        Ok(Form::application(Form::symbol(&self.domain), parts))
    }
}

/// A value, as it would be written.
fn written(value: &Value) -> Form {
    match value {
        Value::Entity(entity) => {
            let uri = entity.to_string();
            match uri.strip_suffix(':') {
                Some(name) if !name.is_empty() && !name.contains(':') => {
                    Form::Keyword(name.to_string())
                }
                _ => Form::Symbol(uri),
            }
        }
        Value::String(text) => Form::Text(text.clone()),
        Value::UnsignedInt(number) => Form::Number(number.to_string()),
        Value::SignedInt(number) => Form::Number(number.to_string()),
        Value::Float(number) => Form::Number(number.to_string()),
        Value::Boolean(truth) => Form::Symbol(truth.to_string()),
        Value::Symbol(symbol) => Form::Symbol(symbol.to_string()),
        Value::Bytes(bytes) | Value::Record(bytes) => Form::Vector(
            bytes
                .iter()
                .map(|byte| Form::Number(byte.to_string()))
                .collect(),
        ),
    }
}

/// Compile an expanded `(rule …)` form.
pub fn compile(form: &Form) -> Result<Vec<Step>, Error> {
    let rule = Rule::parse(form)?;
    if rule.heads.is_empty() {
        return Err(Error::Refused(format!("{form} has no head")));
    }
    // A concept of several reads stands as several heads. Where the
    // author named fields of only some, the others say nothing.
    let bound = bound(&rule);
    let heads: Vec<&Head> = rule
        .heads
        .iter()
        .filter(|head| !kept(head.read(), &bound).is_empty())
        .collect();
    if heads.is_empty() {
        return Err(Error::Refused(format!("{form} concludes no field")));
    }
    if rule.when.is_empty() {
        if !rule.unless.is_empty() {
            return Err(Error::Refused(format!(
                "{form} says what must not hold and nothing that must"
            )));
        }
        return heads.into_iter().map(ground).collect();
    }
    heads
        .into_iter()
        .enumerate()
        .map(|(at, head)| match head {
            Head::Derive(read) => deduce(&rule, read),
            Head::Assert(read) => induce(&rule, read, "assert!"),
            Head::Retract(read) => induce(&rule, read, "retract!"),
            Head::Report(read) => report(&rule, read, &format!("{form} {at}")),
            Head::Hold(read) => Err(Error::Unsupported(format!(
                "a rule that issues a command: the row of {} needs an entity made from \
                 its fields, and Dialog-DB has no formula that makes one",
                read.domain
            ))),
        })
        .collect()
}

fn refused(what: &str, error: impl std::fmt::Display) -> Error {
    Error::Refused(format!("Dialog-DB refuses {what}: {error}"))
}

/// A number as Dialog-DB reads it, under the type it was written under.
fn number(text: &str, kind: Option<&str>) -> Result<Json, Error> {
    let not = |kind: &str| Error::Refused(format!("{text} is not {kind}"));
    match kind {
        Some("natural") => text
            .parse::<u64>()
            .map(Json::from)
            .map_err(|_| not("natural")),
        Some("integer") => text
            .parse::<i64>()
            .map(|number| json!({ "integer:": number }))
            .map_err(|_| not("integer")),
        Some("float") => text
            .parse::<f64>()
            .map(Json::from)
            .map_err(|_| not("float")),
        Some(other) => Err(not(other)),
        None if text.contains('.') => text
            .parse::<f64>()
            .map(Json::from)
            .map_err(|_| not("a number")),
        None if text.starts_with('-') => text
            .parse::<i64>()
            .map(Json::from)
            .map_err(|_| not("a number")),
        None => text
            .parse::<u64>()
            .map(Json::from)
            .map_err(|_| not("a number")),
    }
}

/// The entity `(:id domain label: term …)` names when every part of it
/// is a constant: the hash of what it says.
fn id(formula: &Formula) -> Result<Option<String>, Error> {
    let mut said = Vec::new();
    for term in formula.subject.iter() {
        match ground(term) {
            Some(text) => said.push(text),
            None => return Ok(None),
        }
    }
    let mut labelled = BTreeSet::new();
    for (label, term) in &formula.labelled {
        match ground(term) {
            Some(text) => labelled.insert(format!("{label}: {text}")),
            None => return Ok(None),
        };
    }
    said.extend(labelled);
    return Ok(Some(format!(
        "id:{}",
        blake3::hash(said.join(" ").as_bytes()).to_hex()
    )));

    fn ground(term: &Term) -> Option<String> {
        match term {
            Term::Text(text) => Some(Form::Text(text.clone()).to_string()),
            Term::Number(text) | Term::Entity(text) | Term::Symbol(text) => Some(text.clone()),
            Term::Variable(_) | Term::Blank | Term::Formula(_) => None,
        }
    }
}

/// A constant as Dialog-DB reads it. `None` for what is not a constant.
fn constant(term: &Term, kind: Option<&str>) -> Result<Option<Json>, Error> {
    Ok(Some(match term {
        Term::Text(text) => json!({ "text:": text }),
        Term::Number(text) => number(text, kind)?,
        Term::Entity(uri) => json!({ "entity:": uri }),
        Term::Symbol(name) if name == "true" || name == "false" => json!(name == "true"),
        Term::Symbol(name) if name.contains('/') => json!({ "symbol:": name }),
        Term::Symbol(name) => {
            return Err(Error::Refused(format!(
                "{name} is a name, not a value: text is quoted, an entity has a `:`"
            )));
        }
        Term::Formula(formula) if formula.name == "id" => match id(formula)? {
            Some(uri) => json!({ "entity:": uri }),
            None => return Ok(None),
        },
        Term::Variable(_) | Term::Blank | Term::Formula(_) => return Ok(None),
    }))
}

/// The attribute a field reads or writes. With no pick written the
/// field holds every value: it is a relation, and a relation is a set.
fn attribute(field: &Field) -> Json {
    let mut attribute = Map::new();
    attribute.insert("the".into(), json!(field.attribute));
    if let Some(kind) = &field.kind {
        attribute.insert("as".into(), json!(format!("{kind}:")));
    }
    let pick = field.pick.as_deref().unwrap_or("all");
    attribute.insert("pick".into(), json!(format!("{pick}:")));
    Json::Object(attribute)
}

/// Facts with nothing left to bind: what a head with no premise says.
fn statement(read: &Read) -> Result<ConceptStatement, Error> {
    let entity = match constant(&read.entity, None)? {
        Some(Json::Object(tagged)) if tagged.contains_key("entity:") => tagged["entity:"]
            .as_str()
            .unwrap_or_default()
            .parse::<Entity>()
            .map_err(|error| refused("the entity", error))?,
        _ => {
            return Err(Error::Refused(format!(
                "a write with no premise needs its entity: {}",
                read.domain
            )));
        }
    };
    let mut with = Vec::new();
    for field in kept(read, &BTreeSet::new()) {
        let value = constant(&field.term, field.kind.as_deref())?.ok_or_else(|| {
            Error::Refused(format!(
                "a write with no premise needs a value for {}",
                field.attribute
            ))
        })?;
        let value: Value =
            serde_json::from_value(value).map_err(|error| refused("the value", error))?;
        let descriptor: AttributeDescriptor = serde_json::from_value(attribute(field))
            .map_err(|error| refused("the attribute", error))?;
        with.push(
            descriptor
                .resolve(value)
                .map_err(|error| refused("the value", error))?,
        );
    }
    if with.is_empty() {
        return Err(Error::Refused(format!("{} writes no field", read.domain)));
    }
    Ok(ConceptStatement { this: entity, with })
}

fn ground(head: &Head) -> Result<Step, Error> {
    match head {
        Head::Assert(read) => statement(read).map(Step::Assert),
        Head::Retract(read) => statement(read).map(Step::Retract),
        Head::Hold(read) => statement(read).map(Step::Issue),
        Head::Derive(read) => Err(Error::Unsupported(format!(
            "{} derived from no premise: Dialog-DB derives from at least one",
            read.domain
        ))),
        Head::Report(read) => Err(Error::Refused(format!(
            "a report of {} needs a premise",
            read.domain
        ))),
    }
}

/// The fields of a head that say something. A field an expansion added
/// and no premise binds is one the author left out: a write through a
/// concept writes the fields named, and a report shows them.
fn kept<'a>(read: &'a Read, bound: &BTreeSet<String>) -> Vec<&'a Field> {
    read.fields
        .iter()
        .filter(|field| match &field.term {
            Term::Variable(name) => !name.contains('.') || bound.contains(name),
            _ => true,
        })
        .collect()
}

/// Every variable the premises of a rule may bind.
fn bound(rule: &Rule) -> BTreeSet<String> {
    fn term(term: &Term, names: &mut BTreeSet<String>) {
        match term {
            Term::Variable(name) => {
                names.insert(name.clone());
            }
            Term::Formula(inner) => formula(inner, names),
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
    for premise in &rule.when {
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

fn variable(name: &str) -> Json {
    json!({ "?": { "name": name } })
}

/// One Dialog-DB rule being put together: the names its variables go
/// by there, and its premises.
#[derive(Default)]
struct Scope {
    /// The name each variable goes by in Dialog-DB. A head's fields are
    /// its rule's variables there, so a variable a head field holds goes
    /// by the field's name, and every other by its own behind a `?`,
    /// which no field name has.
    names: BTreeMap<String, String>,
    when: Vec<Json>,
    unless: Vec<Json>,
    fresh: usize,
    /// Whether what is being compiled must not hold. A formula inside
    /// it cannot be lifted out: lifted, it would have to hold.
    negated: bool,
    /// The variables what must hold binds.
    bound: BTreeSet<String>,
    /// The variables only what must not hold names, seen so far.
    absent: BTreeSet<String>,
}

impl Scope {
    fn name(&mut self, name: &str) -> String {
        self.names
            .entry(name.to_string())
            .or_insert_with(|| format!("?{name}"))
            .clone()
    }

    fn fresh(&mut self) -> String {
        self.fresh += 1;
        format!("?.{}", self.fresh)
    }

    fn body(&mut self, rule: &Rule) -> Result<(), Error> {
        self.bound = bound(rule);
        for premise in &rule.when {
            let compiled = self.premise(premise)?;
            self.when.push(compiled);
        }
        self.negated = true;
        for premise in &rule.unless {
            let compiled = self.premise(premise)?;
            self.unless.push(compiled);
        }
        self.negated = false;
        Ok(())
    }

    fn premise(&mut self, premise: &Premise) -> Result<Json, Error> {
        match premise {
            Premise::Read(read) => self.read(read),
            Premise::Formula(formula) => self.formula(formula, None),
        }
    }

    /// A term. An application of a formula is lifted into a premise of
    /// its own, and the term is the variable its value is bound to.
    fn term(&mut self, term: &Term, kind: Option<&str>) -> Result<Json, Error> {
        if let Some(constant) = constant(term, kind)? {
            return Ok(constant);
        }
        match term {
            // A variable only what must not hold names is anything at
            // all there. Dialog-DB wants it blank, which cannot say that
            // two of them are the same.
            Term::Variable(name) if self.negated && !self.bound.contains(name) => {
                if self.absent.insert(name.clone()) {
                    Ok(json!({ "?": {} }))
                } else {
                    Err(Error::Unsupported(format!(
                        "?{name} twice inside what must not hold, and nowhere else: Dialog-DB \
                         negates one proposition, whose own variables are blank"
                    )))
                }
            }
            Term::Variable(name) => Ok(variable(&self.name(name))),
            Term::Blank => Ok(json!({ "?": {} })),
            Term::Formula(formula) if formula.name == "id" => Err(Error::Unsupported(format!(
                "an entity made from what a rule binds, (:id {} …): Dialog-DB has no formula \
                 that makes one",
                formula
                    .subject
                    .as_ref()
                    .and_then(|term| match term {
                        Term::Symbol(name) => Some(name.as_str()),
                        _ => None,
                    })
                    .unwrap_or("…")
            ))),
            Term::Formula(formula) => {
                if self.negated {
                    return Err(Error::Unsupported(format!(
                        "({} …) inside what must not hold: Dialog-DB negates one proposition",
                        formula.name
                    )));
                }
                if TYPES.contains(&formula.name.as_str()) {
                    let subject = formula.subject.as_ref().ok_or_else(|| {
                        Error::Refused(format!("(:{}) types nothing", formula.name))
                    })?;
                    let subject = self.term(subject, Some(&formula.name))?;
                    self.when.push(json!({
                        "assert": "type",
                        "where": { "of": subject, "is": format!("{}:", formula.name) }
                    }));
                    return Ok(subject);
                }
                let result = variable(&self.fresh());
                let lifted = self.formula(formula, Some(result.clone()))?;
                self.when.push(lifted);
                Ok(result)
            }
            _ => Err(Error::Refused("not a term".into())),
        }
    }

    fn read(&mut self, read: &Read) -> Result<Json, Error> {
        let mut with = Map::new();
        let mut terms = Map::new();
        terms.insert("this".into(), self.term(&read.entity, None)?);
        for field in &read.fields {
            let term = self.term(&field.term, field.kind.as_deref())?;
            if with.insert(field.label.clone(), attribute(field)).is_some() {
                return Err(Error::Refused(format!(
                    "{} names {} twice",
                    read.domain, field.label
                )));
            }
            terms.insert(field.label.clone(), term);
        }
        Ok(json!({ "assert": { "with": with }, "where": terms }))
    }

    /// A formula as a premise. `result` is the variable its value goes
    /// to when it stands where a term does; otherwise its `is:` part
    /// says.
    fn formula(&mut self, formula: &Formula, result: Option<Json>) -> Result<Json, Error> {
        let name = formula.name.as_str();
        let missing = |what: &str| Error::Refused(format!("({name} …) has no {what}"));
        let subject = || formula.subject.as_ref().ok_or_else(|| missing("subject"));
        let computed = |scope: &mut Self, selector: &str, first: &str, second: &str| {
            let of = scope.term(subject()?, None)?;
            let with = scope.term(formula.at("").ok_or_else(|| missing("`:` part"))?, None)?;
            let is = match (&result, formula.at("is")) {
                (Some(_), Some(_)) => {
                    return Err(Error::Refused(format!(
                        "({name} …) stands for its value and says `is:` as well"
                    )));
                }
                (Some(result), None) => result.clone(),
                (None, Some(is)) => scope.term(is, None)?,
                (None, None) => return Err(missing("`is:` part")),
            };
            Ok(json!({ "assert": selector, "where": { first: of, second: with, "is": is } }))
        };
        match name {
            "+" => computed(self, "math/sum", "of", "with"),
            "-" => computed(self, "math/difference", "of", "subtract"),
            "*" => computed(self, "math/product", "of", "times"),
            "/" => computed(self, "math/quotient", "of", "by"),
            "<" | "<=" | ">" | ">=" if result.is_some() => Err(Error::Refused(format!(
                "({name} …) is a check, and a check has no value"
            ))),
            "<" | "<=" | ">" | ">=" => {
                let of = self.term(subject()?, None)?;
                let with = self.term(formula.at("").ok_or_else(|| missing("`:` part"))?, None)?;
                Ok(json!({ "assert": name, "where": { "of": of, "with": with } }))
            }
            "value" if result.is_none() => {
                let this = self.term(subject()?, None)?;
                let is = self.term(formula.at("is").ok_or_else(|| missing("`is:` part"))?, None)?;
                Ok(json!({ "assert": "==", "where": { "this": this, "is": is } }))
            }
            kind if TYPES.contains(&kind) && result.is_none() => {
                let of = self.term(subject()?, Some(kind))?;
                Ok(json!({ "assert": "type", "where": { "of": of, "is": format!("{kind}:") } }))
            }
            _ => Err(Error::Unsupported(format!(
                "the formula {name}: this proof of concept knows + - * / < <= > >= :value \
                 and the types"
            ))),
        }
    }

    /// A head, as the concept a Dialog-DB rule concludes. Its fields are
    /// that rule's variables, so each is tied to what the head says of
    /// it: a variable already going by the field's name needs nothing, a
    /// formula is lifted with the field as its value, and anything else
    /// is an equality.
    fn head(&mut self, read: &Read, fields: &[&Field]) -> Result<Json, Error> {
        if fields.is_empty() {
            return Err(Error::Refused(format!(
                "{} concludes no field",
                read.domain
            )));
        }
        let tie = |this: &str, is: Json| json!({ "assert": "==", "where": { "this": variable(this), "is": is } });
        match &read.entity {
            Term::Variable(name) if self.name(name) == "this" => {}
            Term::Blank => {
                return Err(Error::Refused(format!(
                    "{} is concluded of no entity",
                    read.domain
                )));
            }
            other => {
                let entity = self.term(other, None)?;
                self.when.push(tie("this", entity));
            }
        }
        let mut with = Map::new();
        for field in fields {
            if with.insert(field.label.clone(), attribute(field)).is_some() {
                return Err(Error::Refused(format!(
                    "{} names {} twice",
                    read.domain, field.label
                )));
            }
            match &field.term {
                Term::Variable(name) if self.name(name) == field.label => {}
                Term::Blank => {
                    return Err(Error::Refused(format!(
                        "{} is concluded with no value",
                        field.attribute
                    )));
                }
                Term::Formula(formula)
                    if formula.name != "id" && !TYPES.contains(&formula.name.as_str()) =>
                {
                    let lifted = self.formula(formula, Some(variable(&field.label)))?;
                    self.when.push(lifted);
                }
                other => {
                    let value = self.term(other, field.kind.as_deref())?;
                    self.when.push(tie(&field.label, value));
                }
            }
        }
        Ok(json!({ "with": with }))
    }

    /// Give the variables a head holds the names its fields have.
    fn adopt(&mut self, read: &Read, fields: &[&Field]) {
        if let Term::Variable(name) = &read.entity {
            self.names.insert(name.clone(), "this".into());
        }
        for field in fields {
            if let Term::Variable(name) = &field.term {
                self.names
                    .entry(name.clone())
                    .or_insert_with(|| field.label.clone());
            }
        }
    }

    fn rule(self, head: &str, concept: Json, description: Option<&str>) -> Json {
        let mut rule = Map::new();
        if let Some(description) = description {
            rule.insert("description".into(), json!(description));
        }
        rule.insert(head.into(), concept);
        rule.insert("when".into(), Json::Array(self.when));
        if !self.unless.is_empty() {
            rule.insert("unless".into(), Json::Array(self.unless));
        }
        Json::Object(rule)
    }
}

/// A Dialog-DB rule concluding `read` when the premises of `rule` hold.
fn concluding(rule: &Rule, read: &Read, head: &str) -> Result<Json, Error> {
    let bound = bound(rule);
    let fields = kept(read, &bound);
    let mut scope = Scope::default();
    scope.adopt(read, &fields);
    scope.body(rule)?;
    let concept = scope.head(read, &fields)?;
    Ok(scope.rule(head, concept, rule.description.as_deref()))
}

fn deduce(rule: &Rule, read: &Read) -> Result<Step, Error> {
    serde_json::from_value(concluding(rule, read, "deduce")?)
        .map(|rule| Step::Deduce(Box::new(rule)))
        .map_err(|error| refused("the rule", error))
}

fn induce(rule: &Rule, read: &Read, head: &str) -> Result<Step, Error> {
    serde_json::from_value(concluding(rule, read, head)?)
        .map(|rule| Step::Induce(Box::new(rule)))
        .map_err(|error| refused("the rule", error))
}

/// A report. When the premises are one read, the read is what is
/// selected, and a row is the head filled from the match. Otherwise a
/// rule derives a relation of the report's own, a column for each
/// variable the head holds, and that relation is what is selected.
fn report(rule: &Rule, read: &Read, site: &str) -> Result<Step, Error> {
    let bound = bound(rule);
    let fields = kept(read, &bound);
    for term in fields.iter().map(|field| &field.term).chain([&read.entity]) {
        if let Term::Variable(name) = term
            && !bound.contains(name)
        {
            return Err(Error::Refused(format!(
                "the report holds ?{name}, which no premise binds"
            )));
        }
    }
    let shown = |term: &Term| match term {
        Term::Text(text) => Some(Form::Text(text.clone())),
        Term::Number(text) => Some(Form::Number(text.clone())),
        Term::Entity(uri) => Some(match uri.strip_suffix(':') {
            Some(name) if !name.is_empty() && !name.contains(':') => Form::Keyword(name.into()),
            _ => Form::Symbol(uri.clone()),
        }),
        Term::Symbol(name) => Some(Form::Symbol(name.clone())),
        Term::Variable(_) | Term::Blank | Term::Formula(_) => None,
    };

    if let ([Premise::Read(only)], []) = (rule.when.as_slice(), rule.unless.as_slice()) {
        let mut scope = Scope::default();
        let select = scope.read(only)?;
        if scope.when.is_empty() {
            let slot = |term: &Term, scope: &mut Scope| match (term, shown(term)) {
                (_, Some(form)) => Ok(Slot::Is(form)),
                (Term::Variable(name), _) => Ok(Slot::Bound(scope.name(name))),
                _ => Err(Error::Unsupported(
                    "a report of a computed value from one read".into(),
                )),
            };
            let direct = (|| {
                let entity = slot(&read.entity, &mut scope)?;
                let fields = fields
                    .iter()
                    .map(|field| Ok((field.label.clone(), slot(&field.term, &mut scope)?)))
                    .collect::<Result<Vec<_>, Error>>()?;
                Ok::<_, Error>((entity, fields))
            })();
            if let Ok((entity, fields)) = direct {
                return Ok(Step::Query(Box::new(Query {
                    rule: None,
                    select: serde_json::from_value(select)
                        .map_err(|error| refused("the query", error))?,
                    domain: read.domain.clone(),
                    entity,
                    fields,
                })));
            }
        }
    }

    let relation = format!("report.q{}", &blake3::hash(site.as_bytes()).to_hex()[..12]);
    let mut scope = Scope::default();
    let entity = match (&read.entity, shown(&read.entity)) {
        (Term::Variable(name), _) => {
            scope.names.insert(name.clone(), "this".into());
            Slot::Bound("this".into())
        }
        (_, Some(form)) => Slot::Is(form),
        _ => {
            return Err(Error::Refused(format!(
                "{} is reported of no entity",
                read.domain
            )));
        }
    };
    // A column for each field that holds a variable or a computation.
    let mut columns: Vec<Field> = Vec::new();
    let mut slots = Vec::new();
    for field in &fields {
        let slot = match shown(&field.term) {
            Some(form) => Slot::Is(form),
            None => {
                let column = format!("c{}", columns.len());
                if let Term::Variable(name) = &field.term {
                    scope
                        .names
                        .entry(name.clone())
                        .or_insert_with(|| column.clone());
                }
                columns.push(Field {
                    label: column.clone(),
                    attribute: format!("{relation}/{column}"),
                    term: field.term.clone(),
                    kind: None,
                    pick: None,
                });
                Slot::Bound(column)
            }
        };
        slots.push((field.label.clone(), slot));
    }
    if columns.is_empty() {
        columns.push(Field {
            label: "holds".into(),
            attribute: format!("{relation}/holds"),
            term: Term::Symbol("true".into()),
            kind: None,
            pick: None,
        });
    }
    scope.body(rule)?;
    let head = Read {
        domain: relation,
        entity: read.entity.clone(),
        fields: columns,
    };
    let concept = scope.head(&head, &head.fields.iter().collect::<Vec<_>>())?;
    let mut terms = Map::new();
    terms.insert("this".into(), variable("this"));
    for column in &head.fields {
        terms.insert(column.label.clone(), variable(&column.label));
    }
    let select = json!({ "assert": concept.clone(), "where": terms });
    Ok(Step::Query(Box::new(Query {
        rule: Some(
            serde_json::from_value(scope.rule("deduce", concept, None))
                .map_err(|error| refused("the rule behind the query", error))?,
        ),
        select: serde_json::from_value(select).map_err(|error| refused("the query", error))?,
        domain: read.domain.clone(),
        entity,
        fields: slots,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::read as forms;

    fn steps(text: &str) -> Result<Vec<Step>, Error> {
        compile(&forms(text)?.remove(0))
    }

    #[test]
    fn it_compiles_a_write_with_no_premise_to_a_statement() {
        let steps =
            steps(r#"(rule (assert! (xyz.todo todo:1 text: (:last (:text "Buy milk")) n: 3)))"#)
                .unwrap();
        let [Step::Assert(statement)] = steps.as_slice() else {
            panic!("one assertion");
        };
        assert_eq!(statement.this.to_string(), "todo:1");
        assert_eq!(statement.with[0].is, Value::String("Buy milk".into()));
        assert_eq!(statement.with[0].pick, dialog_query::Pick::Last);
        assert_eq!(statement.with[1].is, Value::UnsignedInt(3));
        assert_eq!(statement.with[1].pick, dialog_query::Pick::All);
    }

    #[test]
    fn it_compiles_each_head_to_a_rule_of_its_kind() {
        let steps = steps(
            "(rule (assert! (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
                   (retract! (xyz.counter ?c stale: ?n))
                   (xyz.seen ?c count: ?n)
               (when (xyz.counter ?c count: (:last (:natural ?n)))))",
        )
        .unwrap();
        assert!(matches!(
            steps.as_slice(),
            [Step::Induce(_), Step::Induce(_), Step::Deduce(_)]
        ));
    }

    #[test]
    fn it_says_what_dialog_has_no_counterpart_for() {
        assert!(matches!(
            steps(
                "(rule (assert (command.go.x (:id command.go.x of: ?c) of: ?c)) (when (xyz.counter ?c count: ?n)))"
            ),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            steps(
                "(rule (report (xyz.a ?a n: ?n)) (when (xyz.a ?a n: ?n)) (unless (xyz.b ?a n: (+ ?n : 1))))"
            ),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            steps("(rule (report (xyz.a ?a n: ?n)) (when (xyz.a ?a n: ?n) (:like ?n : \"x\")))"),
            Err(Error::Unsupported(_))
        ));
    }

    #[test]
    fn it_refuses_a_report_of_what_no_premise_binds() {
        assert!(matches!(
            steps("(rule (report (xyz.a ?a n: ?m)) (when (xyz.a ?a n: ?n)))"),
            Err(Error::Refused(_))
        ));
    }
}
