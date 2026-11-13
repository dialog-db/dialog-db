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
//! | `(assert A)` with premises | an [`InductiveRule`] over a concept marked to hold one round |
//! | `(:id domain label: term …)` | the entity `entity/derive` makes of the domain, the labels and each value in turn |
//! | `(report A)` with one read as its premise | a [`ConceptQuery`] |
//! | `(report A)` with any other premises | a [`DeductiveRule`] deriving the report, and a [`ConceptQuery`] of it |
//! | `description: "…"` on a rule Dialog-DB keeps | a [`ConceptStatement`] asserting `syntax.meta/description` of the rule's entity |
//!
//! A kernel rule has one head, as a Dialog-DB rule has. What Dialog-DB
//! has no counterpart for comes back as [`Error::Unsupported`], never as
//! something close.

use std::collections::{BTreeMap, BTreeSet};

use dialog_corpus::{Composite, Entity, Relation, Value};
use dialog_query::concept::descriptor::ConceptStatement;
use dialog_query::formula::entity::derive;
use dialog_query::{
    Any, AttributeDescriptor, ConceptConclusion, ConceptQuery, DeductiveRule,
    DeductiveRuleDescriptor, InductiveRule, InductiveRuleDescriptor, ListPattern, MapPattern,
    Pattern, Premise as DialogPremise, Proposition, Term as Variable,
};
use serde_json::{Map, Value as Json, json};

use crate::Error;
use crate::data;
use crate::kernel::{Field, Formula, Head, Premise, Read, Rule, TYPES, Term};
use crate::syntax::{self, Parameters, Syntax};

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
    /// A concept whose facts hold one round: what a rule issues as a
    /// command. The branch is told so beside the rule that issues it.
    Transient(Entity),
    /// A query to run.
    Query(Box<Query>),
}

/// Where a part of a reported row comes from.
#[derive(Debug, Clone)]
enum Slot {
    /// The row says this, whatever matched.
    Is(Syntax),
    /// The row says what this variable was bound to.
    Bound(String),
    /// The row says the value this pattern is, with what its variables
    /// were bound to.
    Built(Pattern),
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
    pub fn row(&self, conclusion: &ConceptConclusion) -> Result<Syntax, Error> {
        let fill = |slot: &Slot| match slot {
            Slot::Is(form) => Ok(form.clone()),
            Slot::Bound(name) => conclusion
                .source()
                .lookup(&Variable::<Any>::var(name.as_str()))
                .and_then(|binding| binding.content())
                .map(|value| written(&value))
                .map_err(|error| Error::Refused(format!("a match binds no {name}: {error}"))),
            Slot::Built(pattern) => pattern
                .build(conclusion.source())
                .map(|value| written(&value))
                .ok_or_else(|| Error::Refused("a match leaves a part of a struct unbound".into())),
        };
        let mut operand = Parameters {
            items: vec![fill(&self.entity)?],
            ..Parameters::default()
        };
        for (label, slot) in &self.fields {
            operand.terms.insert(label.clone(), fill(slot)?);
        }
        Ok(Syntax::Application(
            Box::new(Syntax::Reference(self.domain.clone())),
            operand,
        ))
    }
}

/// A value, as the tree that says it.
fn written(value: &Value) -> Syntax {
    match value {
        Value::Record(_) if matches!(value.composite(), Ok(Some(_))) => {
            match value.composite() {
                Ok(Some(Composite::Map(map))) => Syntax::Map(
                    map.iter()
                        .map(|(key, value)| {
                            // A symbol of no domain is the label a key
                            // was written as.
                            let key = match data::spelled(key) {
                                Some(label) => Syntax::Reference(label),
                                None => written(key),
                            };
                            (key, written(value))
                        })
                        .collect(),
                    None,
                ),
                Ok(Some(Composite::Tuple(items))) => {
                    Syntax::Tuple(items.iter().map(written).collect())
                }
                // A list is written `( )`: its first item where an
                // operator stands, and the others after it.
                Ok(Some(Composite::List(items))) => match items.split_first() {
                    Some((first, others)) => Syntax::Application(
                        Box::new(written(first)),
                        Parameters {
                            items: others.iter().map(written).collect(),
                            ..Parameters::default()
                        },
                    ),
                    // The list of nothing has no tree: `( )` has a
                    // first part. It is printed as it would be written
                    // and does not read back.
                    None => Syntax::Reference("()".into()),
                },
                _ => Syntax::Blank,
            }
        }
        Value::Bytes(bytes) | Value::Record(bytes) => Syntax::Tuple(
            bytes
                .iter()
                .map(|byte| Syntax::Constant(Value::UnsignedInt(u64::from(*byte).into())))
                .collect(),
        ),
        // A symbol is written as the name that says it, `/todo` where
        // it has no domain.
        Value::Symbol(symbol) => Syntax::Reference(symbol.to_string()),
        other => Syntax::Constant(other.clone()),
    }
}

/// The domain a description is kept in, as a fact about what it
/// describes. Dialog-DB reserves `dialog.` for what it writes itself, so
/// `dialog.meta/description` cannot be written from here.
pub const DESCRIBED: &str = "syntax.meta";

/// Compile an analyzed tree: the third phase, from a rule of the
/// kernel to what Dialog-DB runs.
pub fn compile(form: &Syntax) -> Result<Vec<Step>, Error> {
    let rule = Rule::parse(form)?;
    if rule.when.is_empty() {
        if !rule.unless.is_empty() {
            return Err(Error::Refused(format!(
                "{form} says what must not hold and nothing that must"
            )));
        }
        return Ok(vec![ground(&rule.head)?]);
    }
    let mut steps = match &rule.head {
        Head::Derive(read) => vec![deduce(&rule, read)?],
        Head::Assert(read) => vec![Step::Induce(Box::new(induce(&rule, read, "assert!")?))],
        Head::Retract(read) => vec![Step::Induce(Box::new(induce(&rule, read, "retract!")?))],
        Head::Hold(read) => {
            let issuing = induce(&rule, read, "assert!")?;
            vec![
                Step::Transient(issuing.conclusion().this()),
                Step::Induce(Box::new(issuing)),
            ]
        }
        Head::Report(read) => {
            if read.named(&rule.bound()).is_empty() {
                return Err(Error::Refused(format!("{form} reports no field")));
            }
            // The relation a report is derived into is named by its
            // rule.
            let site = form.reference()?.to_string();
            vec![report(&rule, read, &site)?]
        }
    };
    // What a rule says it is for is a fact about the rule, kept beside
    // it: Dialog-DB leaves a description out of what a rule is and out
    // of what it stores of one. A rule Dialog-DB cannot keep has no
    // entity to say it of.
    let kept = steps.iter().find_map(|step| match step {
        Step::Deduce(rule) => rule.try_this(),
        Step::Induce(rule) => rule.try_this(),
        _ => None,
    });
    if let (Some(description), Some(entity)) = (&rule.description, kept) {
        steps.push(Step::Assert(statement(&Read {
            domain: DESCRIBED.into(),
            entity: Term::Entity(entity.as_str().to_string()),
            fields: vec![Field {
                label: "description".into(),
                attribute: format!("{DESCRIBED}/description"),
                term: Term::Text(description.clone()),
                kind: Some("text".into()),
                pick: Some("last".into()),
            }],
        })?));
    }
    Ok(steps)
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

/// What `(:id domain label: term …)` is derived from before any of its
/// values, and the values in the order they are taken: the domain and
/// the labels make the seed, and the labels are sorted, so the entity
/// does not depend on the order the parts were written in.
fn seed(formula: &Formula) -> Result<(Entity, Vec<&Term>), Error> {
    let Some(Term::Symbol(domain)) = &formula.subject else {
        return Err(Error::Refused(
            "(:id …) needs the domain its entity is for".into(),
        ));
    };
    let mut parts: Vec<(&str, &Term)> = formula
        .labelled
        .iter()
        .map(|(label, term)| (label.as_str(), term))
        .collect();
    parts.sort_by_key(|(label, _)| *label);
    if parts.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(Error::Refused(format!(
            "(:id {domain} …) names a label twice"
        )));
    }
    let labels: Vec<&str> = parts.iter().map(|(label, _)| *label).collect();
    let seed = derive(
        &Value::String(domain.clone()),
        &Value::String(labels.join(" ")),
    );
    Ok((seed, parts.into_iter().map(|(_, term)| term).collect()))
}

/// The entity `(:id domain label: term …)` names when every part of it
/// is a constant: what `entity/derive` makes of the seed and each value
/// in turn, worked out here. `None` when a rule has to bind a part.
fn id(formula: &Formula) -> Result<Option<Entity>, Error> {
    let (mut entity, terms) = seed(formula)?;
    for term in terms {
        let Some(value) = constant(term, None)? else {
            return Ok(None);
        };
        let value: Value =
            serde_json::from_value(value).map_err(|error| refused("the value", error))?;
        entity = derive(&Value::Entity(entity), &value);
    }
    Ok(Some(entity))
}

/// The pattern a struct, a tuple or a list is. A key written as a label is a
/// symbol of no domain; any other key is the value it is written as.
/// `name` says what a variable goes by where the pattern stands, and
/// what a blank, given no name, is there.
fn shape(
    form: &Syntax,
    name: &mut dyn FnMut(Option<&str>) -> Variable<Any>,
) -> Result<Pattern, Error> {
    fn key(node: &Syntax) -> Result<Value, Error> {
        match node {
            Syntax::Constant(value) => Ok(value.clone()),
            Syntax::Reference(label) => data::symbol(label),
            other => Err(Error::Refused(format!(
                "{other} is not a key: a key is a value"
            ))),
        }
    }
    fn walk(
        node: &Syntax,
        name: &mut dyn FnMut(Option<&str>) -> Variable<Any>,
    ) -> Result<Pattern, Error> {
        Ok(match node {
            Syntax::Variable(variable) => name(Some(variable)).into(),
            Syntax::Blank => name(None).into(),
            Syntax::Constant(value) => value.clone().into(),
            Syntax::Map(entries, rest) => {
                let named = MapPattern::new(
                    entries
                        .iter()
                        .map(|(at, value)| Ok((key(at)?, walk(value, name)?)))
                        .collect::<Result<Vec<_>, Error>>()?,
                )
                .map_err(|error| refused("the struct", error))?;
                // `...?rest` is the entries not named, as a struct.
                match rest {
                    Some(rest) => named.with_rest(name(Some(rest))),
                    None => named,
                }
                .into()
            }
            Syntax::Tuple(items) => Pattern::tuple(
                items
                    .iter()
                    .map(|item| walk(item, name))
                    .collect::<Result<Vec<_>, Error>>()?,
            ),
            // A name with a `/` in it is the symbol it says, as it is
            // where a field's value is one: `xyz.todo/text`, and `/todo`
            // for a symbol of no domain, which is how one is printed.
            Syntax::Reference(symbol) if symbol.contains('/') => symbol
                .parse::<Relation>()
                .map(Value::Symbol)
                .map_err(|error| refused("the symbol", error))?
                .into(),
            Syntax::Reference(symbol) => {
                return Err(Error::Refused(format!(
                    "{symbol} is a name, not a value: text is quoted, an entity has a `:`"
                )));
            }
            // `( )` is a list, and `...?run` after its items is the
            // ones after them, as a list. Headed by a keyword or an
            // operator it is a formula, which nothing here applies.
            Syntax::Application(..) if node.is_formula() => {
                return Err(Error::Unsupported(format!(
                    "{node} where a value stands: it applies a formula, and a list that \
                     starts with a keyword or an operator has no spelling yet"
                )));
            }
            Syntax::Application(first, operand) => {
                if let Some(label) = operand.terms.keys().next() {
                    return Err(Error::Refused(format!(
                        "{node} is a list, and a list has no part labelled `{label}:`"
                    )));
                }
                let items = ListPattern::new(
                    std::iter::once(&**first)
                        .chain(&operand.items)
                        .map(|item| walk(item, name))
                        .collect::<Result<Vec<_>, Error>>()?,
                );
                match &operand.rest {
                    Some(run) => items.with_run(name(Some(run))),
                    None => items,
                }
                .into()
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "{other} inside a struct: only values, variables, structs, tuples and lists are"
                )));
            }
        })
    }
    walk(form, name)
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
            Some(entity) => json!({ "entity:": entity.to_string() }),
            None => return Ok(None),
        },
        Term::Variable(_) | Term::Blank | Term::Formula(_) | Term::Struct(_) => return Ok(None),
    }))
}

/// The value a term is, where it is one: a constant, or a struct with
/// no variable in it.
fn value(term: &Term, kind: Option<&str>) -> Result<Option<Value>, Error> {
    if let Term::Struct(form) = term {
        return Ok(shape(form, &mut |name| match name {
            Some(name) => Variable::var(name.to_string()),
            None => Variable::blank(),
        })?
        .as_value());
    }
    constant(term, kind)?
        .map(|constant| {
            serde_json::from_value(constant).map_err(|error| refused("the value", error))
        })
        .transpose()
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
    whole(read, &BTreeSet::new())?;
    for field in &read.fields {
        let value = value(&field.term, field.kind.as_deref())?.ok_or_else(|| {
            Error::Refused(format!(
                "a write with no premise needs a value for {}",
                field.attribute
            ))
        })?;
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

/// A write says every field of what it writes. A field an expansion
/// added and no premise binds is one the author left out, and a concept
/// written in part is not the concept: a read through it would not find
/// what was written.
fn whole(read: &Read, bound: &BTreeSet<String>) -> Result<(), Error> {
    let named = read.named(bound);
    let left_out: Vec<&str> = read
        .fields
        .iter()
        .filter(|field| !named.contains(field))
        .map(|field| field.attribute.as_str())
        .collect();
    if left_out.is_empty() {
        Ok(())
    } else {
        Err(Error::Refused(format!(
            "a write says every field, and this one leaves out {}: write the relation \
             itself to write part of it",
            left_out.join(", ")
        )))
    }
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
    /// The patterns that stand where a term does, each under the
    /// variable written in its place. A pattern has no wire form, so it
    /// is put in once Dialog-DB has read the rest.
    shapes: Vec<(String, Syntax, Pattern)>,
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
        self.bound = rule.bound();
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
            // What must not hold binds nothing: Dialog-DB wants every
            // variable in it bound by what must hold, or blank.
            Term::Variable(name)
                if self.negated && !self.bound.contains(name) && !name.contains('.') =>
            {
                Err(Error::Refused(format!(
                    "?{name} is in `unless` and no premise binds it: write `_` for \
                     anything at all"
                )))
            }
            // A field the author left out of a concept is anything at
            // all, so it is blank, which cannot say that two of them are
            // the same.
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
            Term::Struct(form) => {
                // A blank is a variable of its own here, which binds
                // what stood there and joins nothing, so that a row can
                // say it.
                let pattern = shape(form, &mut |name| match name {
                    Some(name) => Variable::var(self.name(name)),
                    None => Variable::var(self.fresh()),
                })?;
                let place = self.fresh();
                self.shapes.push((place.clone(), form.clone(), pattern));
                Ok(variable(&place))
            }
            Term::Formula(formula) if formula.name == "id" => self.id(formula, None),
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

    /// `(:id domain label: term …)` where a rule binds a part of it: a
    /// chain of `entity/derive`, from the seed through each value. The
    /// last link binds `result`, or a variable of its own.
    fn id(&mut self, formula: &Formula, result: Option<&str>) -> Result<Json, Error> {
        if self.negated {
            return Err(Error::Unsupported(
                "(:id …) inside what must not hold: Dialog-DB negates one proposition".into(),
            ));
        }
        let (seed, terms) = seed(formula)?;
        let mut entity = json!({ "entity:": seed.to_string() });
        let last = terms.len().saturating_sub(1);
        for (at, term) in terms.into_iter().enumerate() {
            let with = self.term(term, None)?;
            let is = match result {
                Some(name) if at == last => variable(name),
                _ => variable(&self.fresh()),
            };
            self.when.push(json!({
                "assert": "entity/derive",
                "where": { "of": entity, "with": with, "is": is.clone() }
            }));
            entity = is;
        }
        Ok(entity)
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
            Term::Formula(formula)
                if formula.name == "id" && constant(&read.entity, None)?.is_none() =>
            {
                self.id(formula, Some("this"))?;
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

    /// The patterns, put where their variables were written in a query
    /// Dialog-DB has read.
    fn place(&self, query: &mut ConceptQuery) {
        let placed: Vec<(String, Pattern)> = query
            .terms
            .iter()
            .filter_map(|(field, term)| {
                let (_, _, pattern) = self
                    .shapes
                    .iter()
                    .find(|(place, _, _)| term.name() == Some(place.as_str()))?;
                Some((field.clone(), pattern.clone()))
            })
            .collect();
        for (field, pattern) in placed {
            query.terms.set(field, pattern);
        }
    }

    /// What ties each pattern to the variable written in its place: an
    /// equality, which builds the value where the pattern's variables
    /// are bound and takes it apart where the variable is. A rule is
    /// handed to Dialog-DB written out, and a pattern has no written
    /// form, so these join its premises once the rest is read.
    fn equalities(&self) -> Vec<Proposition> {
        self.shapes
            .iter()
            .filter_map(|(place, _, pattern)| {
                match Pattern::from(Variable::<Any>::var(place.clone())).is(pattern.clone()) {
                    DialogPremise::Assert(proposition) => Some(proposition),
                    DialogPremise::Unless(_) => None,
                }
            })
            .collect()
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
fn concluding(rule: &Rule, read: &Read, head: &str) -> Result<(Json, Vec<Proposition>), Error> {
    whole(read, &rule.bound())?;
    let fields: Vec<&Field> = read.fields.iter().collect();
    let mut scope = Scope::default();
    scope.adopt(read, &fields);
    scope.body(rule)?;
    let concept = scope.head(read, &fields)?;
    let patterns = scope.equalities();
    Ok((
        scope.rule(head, concept, rule.description.as_deref()),
        patterns,
    ))
}

/// A deductive rule as Dialog-DB reads it written out, with the
/// patterns it holds put among its premises.
fn deductive(written: Json, patterns: Vec<Proposition>) -> Result<DeductiveRule, Error> {
    let mut descriptor: DeductiveRuleDescriptor =
        serde_json::from_value(written).map_err(|error| refused("the rule", error))?;
    descriptor.when.extend(patterns);
    descriptor
        .compile()
        .map_err(|error| refused("the rule", error))
}

fn deduce(rule: &Rule, read: &Read) -> Result<Step, Error> {
    let (written, patterns) = concluding(rule, read, "deduce")?;
    deductive(written, patterns).map(|rule| Step::Deduce(Box::new(rule)))
}

fn induce(rule: &Rule, read: &Read, head: &str) -> Result<InductiveRule, Error> {
    let (written, patterns) = concluding(rule, read, head)?;
    let mut descriptor: InductiveRuleDescriptor =
        serde_json::from_value(written).map_err(|error| refused("the rule", error))?;
    descriptor.when.extend(patterns);
    descriptor
        .compile()
        .map_err(|error| refused("the rule", error))
}

/// A report. When the premises are one read, the read is what is
/// selected, and a row is the head filled from the match. Otherwise a
/// rule derives a relation of the report's own, a column for each
/// variable the head holds, and that relation is what is selected.
fn report(rule: &Rule, read: &Read, site: &str) -> Result<Step, Error> {
    let bound = rule.bound();
    let fields = read.named(&bound);
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
        Term::Text(text) => Some(Syntax::Constant(Value::String(text.clone()))),
        Term::Number(text) => syntax::number(text).ok().map(Syntax::Constant),
        Term::Entity(uri) => syntax::entity(uri).ok().map(Syntax::Constant),
        Term::Symbol(name) if name == "true" || name == "false" => {
            Some(Syntax::Constant(Value::Boolean(name == "true")))
        }
        Term::Symbol(name) => Some(Syntax::Reference(name.clone())),
        Term::Variable(_) | Term::Blank | Term::Formula(_) | Term::Struct(_) => None,
    };

    if let ([Premise::Read(only)], []) = (rule.when.as_slice(), rule.unless.as_slice()) {
        let mut scope = Scope::default();
        let select = scope.read(only)?;
        if scope.when.is_empty() {
            let slot = |term: &Term, scope: &mut Scope| match (term, shown(term)) {
                (_, Some(form)) => Ok(Slot::Is(form)),
                (Term::Variable(name), _) => Ok(Slot::Bound(scope.name(name))),
                (Term::Struct(form), _) => {
                    // The struct as the read holds it, where the head
                    // says the one the read takes apart.
                    let read = scope
                        .shapes
                        .iter()
                        .find(|(_, written, _)| written == form)
                        .map(|(_, _, pattern)| pattern.clone());
                    match read {
                        Some(pattern) => Ok(Slot::Built(pattern)),
                        None => Ok(Slot::Built(shape(form, &mut |name| match name {
                            Some(name) => Variable::var(scope.name(name)),
                            None => Variable::blank(),
                        })?)),
                    }
                }
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
                let mut select: ConceptQuery =
                    serde_json::from_value(select).map_err(|error| refused("the query", error))?;
                scope.place(&mut select);
                return Ok(Step::Query(Box::new(Query {
                    rule: None,
                    select,
                    domain: read.domain.clone(),
                    entity,
                    fields,
                })));
            }
        }
    }

    let relation = format!("report.q{}", &site[..12]);
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
    let patterns = scope.equalities();
    let mut terms = Map::new();
    terms.insert("this".into(), variable("this"));
    for column in &head.fields {
        terms.insert(column.label.clone(), variable(&column.label));
    }
    let select = json!({ "assert": concept.clone(), "where": terms });
    Ok(Step::Query(Box::new(Query {
        rule: Some(deductive(scope.rule("deduce", concept, None), patterns)?),
        select: serde_json::from_value(select).map_err(|error| refused("the query", error))?,
        domain: read.domain.clone(),
        entity,
        fields: slots,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    fn steps(text: &str) -> Result<Vec<Step>, Error> {
        compile(&parse(text)?.remove(0))
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
        // A tree keeps no order of labels, so the fields come by label.
        assert_eq!(statement.with[0].is, Value::UnsignedInt(3));
        assert_eq!(statement.with[0].pick, dialog_query::Pick::All);
        assert_eq!(statement.with[1].is, Value::String("Buy milk".into()));
        assert_eq!(statement.with[1].pick, dialog_query::Pick::Last);
    }

    #[test]
    fn it_compiles_a_head_to_a_rule_of_its_kind() {
        let premise = "(xyz.counter ?c count: (:last (:natural ?n)))";
        let asserted = steps(&format!(
            "(rule (assert! (xyz.counter ?c count: (:last (:natural (+ ?n : 1))))) {premise})"
        ))
        .unwrap();
        assert!(matches!(asserted.as_slice(), [Step::Induce(_)]));
        let retracted = steps(&format!(
            "(rule (retract! (xyz.counter ?c stale: ?n)) {premise})"
        ))
        .unwrap();
        assert!(matches!(retracted.as_slice(), [Step::Induce(_)]));
        let derived = steps(&format!("(rule (xyz.seen ?c count: ?n) {premise})")).unwrap();
        assert!(matches!(derived.as_slice(), [Step::Deduce(_)]));
    }

    /// A kernel rule has one head, so several are refused and nothing is
    /// compiled of them.
    #[test]
    fn it_refuses_a_rule_of_several_heads() {
        let refused = steps(
            "(rule (all (assert! (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
                        (retract! (xyz.counter ?c stale: ?n)))
               (xyz.counter ?c count: (:last (:natural ?n))))",
        );
        assert!(
            matches!(&refused, Err(Error::Refused(why)) if why.contains("a kernel rule has one")),
            "{:?}",
            refused.as_ref().map(Vec::len)
        );
    }

    #[test]
    fn it_says_what_dialog_has_no_counterpart_for() {
        assert!(matches!(
            steps(
                "(rule (report (xyz.a ?a n: ?n)) (xyz.a ?a n: ?n) (unless (xyz.b ?a n: (+ ?n : 1))))"
            ),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            steps("(rule (report (xyz.a ?a n: ?n)) (xyz.a ?a n: ?n) (:like ?n : \"x\"))"),
            Err(Error::Unsupported(_))
        ));
    }

    #[test]
    fn it_compiles_a_command_a_rule_issues_to_a_rule_over_a_transient_concept() {
        let steps = steps(
            "(rule (assert (command.go.x (:id command.go.x about: ?c) about: ?c))
               (xyz.counter ?c count: ?n))",
        )
        .unwrap();
        let [Step::Transient(concept), Step::Induce(rule)] = steps.as_slice() else {
            panic!("a transient concept and the rule that concludes it");
        };
        assert_eq!(*concept, rule.conclusion().this());
    }

    #[test]
    fn it_names_one_entity_whatever_order_the_parts_of_an_id_are_in() {
        let entity = |text: &str| match steps(text).unwrap().as_slice() {
            [Step::Assert(statement)] => statement.this.clone(),
            _ => panic!("one assertion"),
        };
        let one = entity(r#"(rule (assert! (xyz.n (:id xyz.n a: 1 b: "x") a: 1)))"#);
        assert!(one.as_str().starts_with("id:"));
        assert_eq!(
            one,
            entity(r#"(rule (assert! (xyz.n (:id xyz.n b: "x" a: 1) a: 1)))"#)
        );
        assert_ne!(
            one,
            entity(r#"(rule (assert! (xyz.n (:id xyz.n a: 1 b: "y") a: 1)))"#)
        );
        assert_ne!(
            one,
            entity(r#"(rule (assert! (xyz.m (:id xyz.m a: 1 b: "x") a: 1)))"#)
        );
    }

    #[test]
    fn it_refuses_a_write_that_leaves_a_field_out() {
        // `?l.1` is how a field an expansion added and nothing binds reads.
        assert!(matches!(
            steps("(rule (assert! (xyz.todo ?t list: ?l.1 state: :done)) (xyz.go ?t on: :yes))"),
            Err(Error::Refused(_))
        ));
        assert!(matches!(
            steps("(rule (assert! (xyz.todo todo:1 list: ?l.1 state: :done)))"),
            Err(Error::Refused(_))
        ));
    }

    #[test]
    fn it_refuses_a_variable_only_unless_names() {
        assert!(matches!(
            steps("(rule (report (xyz.a ?a n: ?n)) (xyz.a ?a n: ?n) (unless (xyz.b ?a n: ?m)))"),
            Err(Error::Refused(_))
        ));
        assert!(
            steps("(rule (report (xyz.a ?a n: ?n)) (xyz.a ?a n: ?n) (unless (xyz.b ?a n: _)))")
                .is_ok()
        );
    }

    #[test]
    fn it_refuses_a_report_of_what_no_premise_binds() {
        assert!(matches!(
            steps("(rule (report (xyz.a ?a n: ?m)) (xyz.a ?a n: ?n))"),
            Err(Error::Refused(_))
        ));
    }
}
