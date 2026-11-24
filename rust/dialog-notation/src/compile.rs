//! Compiling a kernel rule to what Dialog-DB runs.
//!
//! Nothing here evaluates anything. A kernel rule becomes the
//! descriptors Dialog-DB already has, and Dialog-DB plans and runs
//! them:
//!
//! | kernel | Dialog-DB |
//! |---|---|
//! | `(assert A)`, `(retract A)` with no premise | a [`ConceptStatement`], asserted or retracted |
//! | `(assert A)` of claims on the control layer, with no premise | a [`ConceptStatement`], dispatched: a command, which holds one round |
//! | a bare head with premises | a [`DeductiveRule`] |
//! | a bare head with premises, a field of it a fold, `(count ?t)` | nothing yet: Dialog-DB refuses a fold in a rule that derives |
//! | `(assert A)`, `(retract A)` with premises | an [`InductiveRule`] |
//! | `(assert A)` of claims on the control layer, with premises | an [`InductiveRule`] over a concept marked to hold one round |
//! | `(:id domain label: term …)` | the entity `entity/derive` makes of the domain, the labels and each value in turn |
//! | `(report A)` with one read as its premise | a [`ConceptQuery`] |
//! | `(report A)` with any other premises | a [`DeductiveRule`] deriving the report, and a [`ConceptQuery`] of it |
//! | `description: "…"` on a rule Dialog-DB keeps | a [`ConceptStatement`] asserting `syntax.meta/description` of the rule's entity |
//! | `(select.query …)` of a query whose answer is markup | a [`Plan`](crate::Plan): its layers, each a query of its own, that a back end maintains it by; nothing written |
//! | `(select.query …)`, a query asked | a [`Selection`](crate::select::Selection): the [`ConceptQuery`]s its rows come from, put together in memory; nothing written |
//!
//! A kernel rule has one head, as a Dialog-DB rule has. What Dialog-DB
//! has no counterpart for comes back as [`Error::Unsupported`], never as
//! something close.

use std::collections::{BTreeMap, BTreeSet};

use dialog_corpus::{Entity, Relation, Value};
use dialog_query::concept::descriptor::ConceptStatement;
use dialog_query::formula::entity::derive;
use dialog_query::{
    Any, AttributeDescriptor, ConceptConclusion, ConceptQuery, DeductiveRule,
    DeductiveRuleDescriptor, InductiveRule, InductiveRuleDescriptor, ListPattern, MapPattern,
    Pattern, Premise as DialogPremise, Proposition, Term as Variable,
};
use serde_json::{Map, Value as Json, json};

use crate::Error;
use crate::kernel::{Field, Formula, Head, Keyed, Premise, QUOTE, Read, Rule, TYPES, Term};
use crate::pattern::REST;
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
    /// A write to make once, of every match of its `when`: a write at
    /// the top level, never installed.
    Write(Box<Write>),
    /// A view: the plan a back end maintains it by, every layer's query
    /// compiled. Nothing of it is written to a branch.
    View(Box<crate::view::plan::Plan>),
    /// A query asked: the Dialog-DB queries its rows come from, and how
    /// they are put together into its answers. Nothing of it is written.
    Select(Box<crate::select::Selection>),
}

/// A write at the top level with a `when`: what its `when` matches, and
/// the write each match makes.
#[derive(Debug, Clone)]
pub struct Write {
    /// What the `when` matches, as a query whose row is every variable
    /// the write holds.
    pub query: Query,
    /// Whether the write retracts.
    pub retract: bool,
    head: Read,
}

impl Write {
    /// The facts one match writes: the head with each variable as the
    /// match bound it.
    pub fn statement(&self, conclusion: &ConceptConclusion) -> Result<ConceptStatement, Error> {
        let row = self.query.row(conclusion)?;
        let mut values: BTreeMap<String, Term> = BTreeMap::new();
        if let (Term::Variable(name), Some([entity, ..])) = (
            &self.head.entity,
            row.operand().map(|operand| operand.items.as_slice()),
        ) {
            values.insert(name.clone(), crate::kernel::term(entity)?);
        }
        for (label, value) in row.operand().into_iter().flat_map(|operand| &operand.terms) {
            values.insert(label.clone(), crate::kernel::term(value)?);
        }
        fn bound(term: &Term, values: &BTreeMap<String, Term>) -> Result<Term, Error> {
            Ok(match term {
                Term::Variable(name) => values.get(name).cloned().ok_or_else(|| {
                    Error::Refused(format!("a match of the `when` binds no ?{name}"))
                })?,
                Term::Formula(formula) => Term::Formula(Box::new(Formula {
                    name: formula.name.clone(),
                    subject: formula
                        .subject
                        .as_ref()
                        .map(|subject| bound(subject, values))
                        .transpose()?,
                    labelled: formula
                        .labelled
                        .iter()
                        .map(|(label, part)| Ok((label.clone(), bound(part, values)?)))
                        .collect::<Result<_, Error>>()?,
                })),
                other => other.clone(),
            })
        }
        let mut head = self.head.clone();
        head.entity = bound(&head.entity, &values)?;
        for field in &mut head.fields {
            field.term = bound(&field.term, &values)?;
            if let Some((key, _)) = &mut field.key {
                *key = bound(key, &values)?;
            }
        }
        statement(&head)
    }
}

/// Every variable a term holds, in a formula too.
fn variables(term: &Term, into: &mut Vec<String>) {
    match term {
        Term::Variable(name) if !into.contains(name) => into.push(name.clone()),
        Term::Formula(formula) => {
            formula
                .subject
                .iter()
                .for_each(|subject| variables(subject, into));
            formula
                .labelled
                .iter()
                .for_each(|(_, part)| variables(part, into));
        }
        Term::Struct(form) => {
            let mut inside = Vec::new();
            form.variables(&mut inside);
            for name in inside {
                if !into.contains(&name) {
                    into.push(name);
                }
            }
        }
        _ => {}
    }
}

/// A write at the top level with a `when`. It runs once: what the `when`
/// matches is a query, and each match makes the write.
fn once(rule: &Rule, form: &Syntax) -> Result<Step, Error> {
    let (read, retract) = match &rule.head {
        Head::Assert(read) => (read, false),
        Head::Retract(read) => (read, true),
        Head::Hold(_) => {
            return Err(Error::Unsupported(format!(
                "{form} issues a command with a `when`: whether a command at the top level keeps \
                 `when` is not decided"
            )));
        }
        _ => {
            return Err(Error::Refused(format!(
                "{form} is not a write, and only a write takes a `when`"
            )));
        }
    };
    let bound = rule.bound();
    let mut held = Vec::new();
    variables(&read.entity, &mut held);
    for field in &read.fields {
        variables(&field.term, &mut held);
        if let Some((key, _)) = &field.key {
            variables(key, &mut held);
        }
    }
    if let Some(name) = held.iter().find(|name| !bound.contains(*name)) {
        return Err(Error::Refused(format!(
            "every variable in a write with a `when` is bound by it, and ?{name} is not"
        )));
    }
    whole(read, &bound)?;
    let entity = match &read.entity {
        Term::Variable(_) => read.entity.clone(),
        _ => Term::Entity("once:write".into()),
    };
    let fields = held
        .iter()
        .filter(|name| entity != Term::Variable((*name).clone()))
        .map(|name| Field {
            label: name.clone(),
            attribute: format!("once.write/{name}"),
            term: Term::Variable(name.clone()),
            kind: None,
            pick: None,
            cardinality: None,
            listed: Vec::new(),
            layer: None,
            key: None,
        })
        .collect();
    let row = Read {
        domain: "once.write".into(),
        entity,
        fields,
    };
    let site = form.reference()?.to_string();
    let Step::Query(query) = report(rule, &row, &site)? else {
        return Err(Error::Refused(format!(
            "{form} matches nothing a query can say"
        )));
    };
    Ok(Step::Write(Box::new(Write {
        query: *query,
        retract,
        head: read.clone(),
    })))
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
    /// Each field's label, what its key is where it is an entry of a
    /// collection, and its value.
    fields: Vec<(String, Option<Slot>, Slot)>,
}

impl Query {
    /// A match, as each field the query reports, by its label, and the
    /// value it was bound to. A field that is an entry of a collection
    /// is said under its label, not its key.
    pub fn values(&self, conclusion: &ConceptConclusion) -> Result<BTreeMap<String, Value>, Error> {
        let value = |slot: &Slot| match slot {
            Slot::Is(Syntax::Constant(value)) => Ok(value.clone()),
            Slot::Is(other) => Err(Error::Refused(format!("{other} is not a value"))),
            Slot::Bound(name) => conclusion
                .source()
                .lookup(&Variable::<Any>::var(name.as_str()))
                .and_then(|binding| binding.content())
                .map_err(|error| Error::Refused(format!("a match binds no {name}: {error}"))),
            Slot::Built(pattern) => pattern
                .build(conclusion.source())
                .ok_or_else(|| Error::Refused("a match leaves a part of a struct unbound".into())),
        };
        self.fields
            .iter()
            .map(|(label, _, slot)| Ok((label.clone(), value(slot)?)))
            .collect()
    }

    /// The variable the select binds a field to, by the field's label,
    /// where the field is what a variable was bound to.
    pub fn bound(&self, label: &str) -> Option<&str> {
        self.fields.iter().find_map(|(field, _, slot)| match slot {
            Slot::Bound(name) if field == label => Some(name.as_str()),
            _ => None,
        })
    }

    /// The label of the field that is the entity a row is reported
    /// under, where one is.
    pub fn about(&self) -> Option<&str> {
        let Slot::Bound(entity) = &self.entity else {
            return None;
        };
        self.fields.iter().find_map(|(field, _, slot)| match slot {
            Slot::Bound(name) if name == entity => Some(field.as_str()),
            _ => None,
        })
    }

    /// What to select with the variable of each field `labels` names
    /// renamed to the name beside it, and every other variable
    /// `{prefix}` and its name, so that the rows of two queries join on
    /// those fields alone. `None` where a field named is not what a
    /// variable was bound to.
    pub fn renamed(&self, labels: &[(String, String)], prefix: &str) -> Option<ConceptQuery> {
        let mut names = BTreeMap::new();
        for (label, name) in labels {
            names.insert(self.bound(label)?.to_string(), name.clone());
        }
        fn rename(json: &mut Json, names: &BTreeMap<String, String>, prefix: &str) {
            match json {
                Json::Object(object) => {
                    if let Some(Json::Object(variable)) = object.get_mut("?")
                        && let Some(Json::String(name)) = variable.get_mut("name")
                    {
                        *name = names
                            .get(name.as_str())
                            .cloned()
                            .unwrap_or_else(|| format!("{prefix}{name}"));
                        return;
                    }
                    for value in object.values_mut() {
                        rename(value, names, prefix);
                    }
                }
                Json::Array(items) => {
                    for item in items {
                        rename(item, names, prefix);
                    }
                }
                _ => {}
            }
        }
        let mut select = serde_json::to_value(&self.select).ok()?;
        rename(&mut select, &names, prefix);
        serde_json::from_value(select).ok()
    }

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
        // An entry of a collection is said under its key, as the
        // attribute it is: `(xyz.tonk.notebook notebook:1 N1: block:a)`.
        for (label, key, slot) in &self.fields {
            let label = match key.as_ref().map(fill).transpose()? {
                Some(Syntax::Constant(Value::String(key))) => key,
                Some(other) => other.to_string(),
                None => label.clone(),
            };
            operand.terms.insert(label, fill(slot)?);
        }
        Ok(Syntax::Application(
            Box::new(Syntax::Reference(self.domain.clone())),
            operand,
        ))
    }
}

/// Whether a map's text key reads as a name.
fn label(key: &str) -> bool {
    key.starts_with(|first: char| first.is_ascii_lowercase())
        && key.chars().all(|part| {
            part.is_ascii_lowercase() || part.is_ascii_digit() || matches!(part, '-' | '.' | '/')
        })
}

/// A value, as the tree that says it. A list is said as `[a b]`: the data
/// model has one sequence, and `[ ]` is how it is written where a value
/// stands.
pub(crate) fn written(value: &Value) -> Syntax {
    match value {
        // A map has no order. It is said by its keys in the order of
        // their written form, which is not the order it is kept in.
        Value::Composite(composite) if composite.is_map() => {
            let mut entries: Vec<(Syntax, Syntax)> = composite
                .entries()
                .map(|(key, value)| {
                    let key = match key {
                        // A key that reads as a name is the label it was
                        // written as; any other text is text.
                        Value::String(key) if label(&key) => Syntax::Reference(key),
                        other => written(&other),
                    };
                    (key, written(&value))
                })
                .collect();
            entries.sort_by_key(|(key, _)| key.to_string());
            Syntax::Map(entries)
        }
        Value::Composite(composite) => {
            Syntax::Tuple(composite.items().map(|item| written(&item)).collect())
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

/// The namespace of the relations a report derives, each named by the
/// reference of its site.
pub const REPORTED: &str = "report.q";

/// Compile an analyzed tree: the third phase, from a rule of the
/// kernel to what Dialog-DB runs.
pub fn compile(form: &Syntax) -> Result<Vec<Step>, Error> {
    // A query whose answer is markup is laid out in layers, each a query
    // of its own, and maintained where it is rendered: nothing of it is
    // folded, and nothing of it is written. It compiles when every
    // layer's query does.
    if form.applies() == Some(crate::select::SELECTED) && crate::view::plan::is_markup(form) {
        let plan = crate::view::plan::Plan::of(form)?;
        for layer in 0..plan.layers.len() {
            if plan.layers[layer].conditions.is_empty() {
                continue;
            }
            plan.query(layer, None).map_err(|error| match error {
                Error::Refused(why) => {
                    Error::Refused(format!("the view {}, layer {layer}: {why}", plan.name))
                }
                Error::Unsupported(why) => {
                    Error::Unsupported(format!("the view {}, layer {layer}: {why}", plan.name))
                }
                other => other,
            })?;
        }
        return Ok(vec![Step::View(Box::new(plan))]);
    }
    if form.applies() == Some(crate::select::SELECTED) {
        return Ok(vec![Step::Select(Box::new(
            crate::select::Selection::compile(form)?,
        ))]);
    }
    let rule = retracted(Rule::parse(form)?, form)?;
    if rule.once {
        return Ok(vec![once(&rule, form)?]);
    }
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
    // of what it stores of one. A rule with no entity has none to say
    // it of.
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
                pick: None,
                cardinality: Some("one".into()),
                listed: Vec::new(),
                layer: None,
                key: None,
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

/// The pattern a struct or a list is, a list written `[a b]` or
/// `'(a b)`. A key is text: a label as it is written, or text itself.
/// `name` says what a variable goes by where the pattern stands, and
/// what a blank, given no name, is there.
///
/// `open` is how a struct that is read is meant: a struct with no
/// `...` is any record that holds what it says, so it takes the entries
/// it does not name under a blank. Where a struct is written it is the
/// record of what it says and no more. A list is its items either way.
fn shape(
    form: &Syntax,
    open: bool,
    name: &mut dyn FnMut(Option<&str>) -> Variable<Any>,
) -> Result<Pattern, Error> {
    fn key(node: &Syntax) -> Result<String, Error> {
        match node {
            Syntax::Constant(Value::String(text)) => Ok(text.clone()),
            Syntax::Reference(label) => Ok(label.clone()),
            other => Err(Error::Refused(format!(
                "{other} is not a key: a key is a label or text"
            ))),
        }
    }
    /// What takes the parts not named: the variable after `...`.
    fn rest(
        taken: &Syntax,
        name: &mut dyn FnMut(Option<&str>) -> Variable<Any>,
    ) -> Result<Variable<Any>, Error> {
        match taken {
            Syntax::Variable(rest) => Ok(name(Some(rest))),
            Syntax::Blank => Ok(name(None)),
            other => Err(Error::Unsupported(format!(
                "`... {other}` where a value stands: a variable takes the parts not named"
            ))),
        }
    }
    /// The list `items` are, and `... ?run` after them for the ones
    /// after them, as a list.
    fn sequence(
        node: &Syntax,
        mut items: Vec<&Syntax>,
        name: &mut dyn FnMut(Option<&str>) -> Variable<Any>,
        quoted: bool,
        open: bool,
    ) -> Result<Pattern, Error> {
        let dots = |part: &Syntax| matches!(part, Syntax::Reference(symbol) if symbol == REST);
        let run = match items.iter().position(|item| dots(item)) {
            Some(at) if at + 2 == items.len() => {
                let taken = rest(items[at + 1], name)?;
                items.truncate(at);
                Some(taken)
            }
            Some(at) if at + 1 == items.len() => {
                items.truncate(at);
                Some(name(None))
            }
            Some(_) => {
                return Err(Error::Refused(format!(
                    "{node}: `...` stands after the items of a list, with what takes \
                     the ones after them"
                )));
            }
            None => None,
        };
        let list = ListPattern::new(
            items
                .into_iter()
                .map(|item| walk(item, name, quoted, open))
                .collect::<Result<Vec<_>, Error>>()?,
        );
        Ok(match run {
            Some(run) => list.with_run(run),
            None => list,
        }
        .into())
    }
    fn walk(
        node: &Syntax,
        name: &mut dyn FnMut(Option<&str>) -> Variable<Any>,
        quoted: bool,
        open: bool,
    ) -> Result<Pattern, Error> {
        let dots = |part: &Syntax| matches!(part, Syntax::Reference(symbol) if symbol == REST);
        Ok(match node {
            Syntax::Variable(variable) => name(Some(variable)).into(),
            Syntax::Blank => name(None).into(),
            Syntax::Constant(value) => value.clone().into(),
            // A set, a map with no names, is kept in a record as a list:
            // its members in the order of their values, each once.
            Syntax::Map(entries) if crate::select::is_set(node) => {
                let mut members: Vec<&Syntax> = entries.iter().map(|(_, member)| member).collect();
                if members
                    .iter()
                    .all(|member| matches!(member, Syntax::Constant(_)))
                {
                    members.sort_by(|a, b| crate::select::order(a, b));
                    members.dedup();
                }
                ListPattern::new(
                    members
                        .into_iter()
                        .map(|member| walk(member, name, quoted, open))
                        .collect::<Result<Vec<_>, Error>>()?,
                )
                .into()
            }
            // The entry under `...` is the entries not named, as a
            // struct.
            Syntax::Map(entries) => {
                let named = MapPattern::new(
                    entries
                        .iter()
                        .filter(|(at, _)| !dots(at))
                        .map(|(at, value)| {
                            Ok((Value::String(key(at)?), walk(value, name, quoted, open)?))
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                )
                .map_err(|error| refused("the struct", error))?;
                match entries.iter().find(|(at, _)| dots(at)) {
                    Some((_, taken)) => named.with_rest(rest(taken, name)?),
                    None if open => named.with_rest(name(None)),
                    None => named,
                }
                .into()
            }
            // `[a b]` is the list of what it holds: the data model has no
            // tuple, so a tuple where a value stands is a list.
            Syntax::Tuple(items) => sequence(node, items.iter().collect(), name, quoted, open)?,
            // `'(1 2 3)`: what is quoted is not applied, and a `( )` in
            // it is the list of what it holds.
            Syntax::Application(_, operand) if !quoted && node.applies() == Some(QUOTE) => {
                match (operand.items.as_slice(), operand.terms.is_empty()) {
                    ([list @ Syntax::Application(..)], true) => walk(list, name, true, open)?,
                    _ => {
                        return Err(Error::Refused(format!(
                            "{node} where a value stands: what is quoted there is a list, \
                             `'(1 2 3)`"
                        )));
                    }
                }
            }
            // A list, and `... ?run` after its items for the ones after
            // them, as a list.
            Syntax::Application(first, operand) if quoted => {
                if let Some(label) = operand.terms.keys().next() {
                    return Err(Error::Refused(format!(
                        "{node} is a list, and a list has no part labelled `{label}:`"
                    )));
                }
                let items = std::iter::once(&**first).chain(&operand.items).collect();
                sequence(node, items, name, quoted, open)?
            }
            Syntax::Application(..) => {
                return Err(Error::Refused(format!(
                    "{node} where a value stands: a list is `[1 2 3]`, or quoted, `'(1 2 3)`"
                )));
            }
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
        })
    }
    walk(form, name, false, open)
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
        Term::Variable(_)
        | Term::Blank
        | Term::Formula(_)
        | Term::Struct(_)
        | Term::Fold { .. } => return Ok(None),
    }))
}

/// The value a term is, where it is one: a constant, or a struct with
/// no variable in it.
fn value(term: &Term, kind: Option<&str>) -> Result<Option<Value>, Error> {
    if let Term::Struct(form) = term {
        return Ok(shape(form, false, &mut |name| match name {
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

/// The domain of a collection a field is every entry of, and what its
/// key is, as Dialog-DB says it.
fn collection(field: &Field) -> Option<(&str, &'static str)> {
    let (_, keyed) = field.key.as_ref()?;
    let (domain, _) = field.attribute.rsplit_once('/')?;
    Some((
        domain,
        match keyed {
            Keyed::Name => "dictionary",
            Keyed::Position => "sequence",
        },
    ))
}

/// The descriptor of the attribute a field reads or writes, as Dialog-DB
/// reads it.
pub(crate) fn descriptor(field: &Field) -> Result<AttributeDescriptor, Error> {
    serde_json::from_value(attribute(field, Role::Attribute)?)
        .map_err(|error| refused("the attribute", error))
}

/// What a field's attribute stands as: a concept field, which may
/// choose one of its claims by an elector, or an attribute on its own,
/// which a write and a rule's conclusion go by and which chooses
/// nothing.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Field,
    Attribute,
}

/// The attribute a field reads or writes, with its cardinality: `one`,
/// whose read is the newest claim and whose write succeeds it, or
/// `many`, every claim. A field written under neither is many: it holds
/// every value, as a relation does. Read as a concept field it carries
/// the elector it was written under, `max`, `min` or `top`, which
/// chooses one of the claims a cardinality-many attribute holds; an
/// attribute on its own carries none. A variant's cases are listed, best
/// first, so `top` can rank by them. A field that is every entry of a
/// collection reads the collection.
fn attribute(field: &Field, role: Role) -> Result<Json, Error> {
    let mut attribute = Map::new();
    match collection(field) {
        Some((domain, keyed)) => {
            attribute.insert("the".into(), json!({ "domain": domain, "keyed": keyed }));
        }
        None => {
            attribute.insert("the".into(), json!(field.attribute));
        }
    }
    if !field.listed.is_empty() {
        let cases: Vec<String> = field.listed.iter().map(|case| format!("{case}:")).collect();
        attribute.insert("as".into(), json!(cases));
    } else if let Some(kind) = &field.kind {
        attribute.insert("as".into(), json!(format!("{kind}:")));
    }
    let written = field.cardinality.as_deref();
    let (cardinality, pick) = match (field.pick.as_deref(), written) {
        (None, written) => (written.unwrap_or("many"), None),
        (Some("last"), None | Some("one")) => ("one", None),
        (Some("last"), Some(written)) => (written, Some("last")),
        (Some("all"), None | Some("many")) => ("many", None),
        (Some(elector), None | Some("many")) => ("many", Some(elector)),
        (Some(pick), Some(written)) => {
            return Err(Error::Refused(format!(
                "{} is read under `{pick}` and cardinality {written}: `{pick}` chooses among \
                 the claims of cardinality many",
                field.attribute
            )));
        }
    };
    attribute.insert("cardinality".into(), json!(cardinality));
    if let (Role::Field, Some(pick)) = (role, pick) {
        attribute.insert("pick".into(), json!(pick));
    }
    Ok(Json::Object(attribute))
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
        let mut written = attribute(field, Role::Attribute)?;
        // One entry of a collection is written under its key: the
        // attribute `domain/key`, of the collection's key shape. A
        // relation spelled as text takes no position for its name, so
        // the attribute is put in once the value is resolved.
        let mut entry = None;
        if let (Some((domain, keyed)), Some((key, _))) = (collection(field), &field.key) {
            let Term::Text(key) = key else {
                return Err(Error::Refused(format!(
                    "a write with no premise needs the key of its entry of {domain}, as text"
                )));
            };
            let keyed = match keyed {
                "dictionary" => dialog_query::Keyed::Dictionary,
                _ => dialog_query::Keyed::Sequence,
            };
            let symbol = domain
                .parse()
                .map_err(|error| refused("the collection", error))?;
            entry = Some(
                dialog_query::The::collection(symbol, keyed)
                    .entry(key)
                    .map_err(|error| refused("the key", error))?,
            );
            written["the"] = json!(format!("{domain}/entry"));
        }
        let descriptor: AttributeDescriptor =
            serde_json::from_value(written).map_err(|error| refused("the attribute", error))?;
        let mut attribution = descriptor
            .resolve(value)
            .map_err(|error| refused("the value", error))?;
        if let Some(entry) = entry {
            attribution.the = entry;
        }
        with.push(attribution);
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
    let unnamed = |term: &Term| matches!(term, Term::Variable(name) if name.contains('.') && !bound.contains(name));
    let left_out: Vec<&str> = read
        .fields
        .iter()
        .filter(|field| {
            !named.contains(field) || field.key.as_ref().is_some_and(|(key, _)| unnamed(key))
        })
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
    /// variable written in its place. The rest is written as JSON and
    /// read by Dialog-DB, and a pattern is put in after: this was built
    /// before a pattern had a written form (format 4 of a rule body).
    shapes: Vec<(String, Syntax, Pattern)>,
    /// The records a head builds, each from what its premises bind, under
    /// the field it concludes.
    built: Vec<(String, Pattern)>,
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
        // `(!= ?a is: ?b)` is the absence of `(== ?a is: ?b)`: Dialog-DB
        // has equality as a constraint, and negates a proposition.
        let unequal = |premise: &Premise| match premise {
            Premise::Formula(formula) if formula.name == "!=" => Some(Formula {
                name: "==".into(),
                ..formula.clone()
            }),
            _ => None,
        };
        for premise in &rule.when {
            if unequal(premise).is_some() {
                continue;
            }
            let compiled = self.premise(premise)?;
            self.when.push(compiled);
        }
        self.negated = true;
        for premise in &rule.when {
            if let Some(equal) = unequal(premise) {
                let compiled = self.formula(&equal, None)?;
                self.unless.push(compiled);
            }
        }
        for premise in &rule.unless {
            if unequal(premise).is_some() {
                return Err(Error::Unsupported(
                    "`!=` inside what must not hold: it is the absence of `==` already, and \
                     Dialog-DB negates one proposition"
                        .into(),
                ));
            }
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
                let pattern = shape(form, true, &mut |name| match name {
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
            Term::Fold { apply, .. } => Err(Error::Refused(format!(
                "({apply} …) is a fold, and stands only in a field of a head that derives: what it makes is never written, and is no value a premise can ask for"
            ))),
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
            let mut term = self.term(&field.term, field.kind.as_deref())?;
            if let Some((key, _)) = &field.key {
                term = json!({ "the": self.term(key, None)?, "is": term });
            }
            if with
                .insert(field.label.clone(), attribute(field, Role::Field)?)
                .is_some()
            {
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
            // `(:id domain label: term … is: ?t)` as a premise: the chain
            // of `entity/derive` ends in what `is:` holds.
            "id" if result.is_none() => {
                let Some(Term::Variable(is)) = formula.at("is") else {
                    return Err(Error::Refused(
                        "(:id …) as a premise binds a variable under `is:`".into(),
                    ));
                };
                let is = self.name(is);
                let inner = Formula {
                    labelled: formula
                        .labelled
                        .iter()
                        .filter(|(label, _)| label != "is")
                        .cloned()
                        .collect(),
                    ..formula.clone()
                };
                if inner.labelled.is_empty() {
                    return Err(missing("label to identify by"));
                }
                self.id(&inner, Some(&is))?;
                self.when
                    .pop()
                    .ok_or_else(|| Error::Refused("(:id …) derived nothing".into()))
            }
            // `(dialog/position ?b after: ?lo before: ?hi is: ?at)`: the
            // position between two, `""` an open bound, biased by the
            // member so the same insert on two devices lands alike.
            "position" => {
                let member = self.term(subject()?, None)?;
                let mut cells = Map::new();
                cells.insert("member".into(), member);
                for cell in ["after", "before"] {
                    let term = formula
                        .at(cell)
                        .ok_or_else(|| missing(&format!("`{cell}:` part")))?;
                    cells.insert(cell.into(), self.term(term, Some("text"))?);
                }
                let is = match (&result, formula.at("is")) {
                    (Some(result), None) => result.clone(),
                    (None, Some(is)) => self.term(is, None)?,
                    _ => return Err(missing("`is:` part, or a place where its value stands")),
                };
                cells.insert("is".into(), is);
                Ok(json!({ "assert": "dialog/position", "where": cells }))
            }
            "value" | "==" if result.is_none() => {
                let this = self.term(subject()?, None)?;
                let is = self.term(formula.at("is").ok_or_else(|| missing("`is:` part"))?, None)?;
                Ok(json!({ "assert": "==", "where": { "this": this, "is": is } }))
            }
            kind if TYPES.contains(&kind) && result.is_none() => {
                let of = self.term(subject()?, Some(kind))?;
                Ok(json!({ "assert": "type", "where": { "of": of, "is": format!("{kind}:") } }))
            }
            _ => Err(Error::Unsupported(format!(
                "the formula {name}: this proof of concept knows + - * / < <= > >= == != \
                 :value, dialog/position and the types"
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
            if with
                .insert(field.label.clone(), attribute(field, Role::Attribute)?)
                .is_some()
            {
                return Err(Error::Refused(format!(
                    "{} names {} twice",
                    read.domain, field.label
                )));
            }
            // An entry's key is the head's key operand, `entry/key`.
            if let Some((key, _)) = &field.key {
                let value = self.term(key, None)?;
                self.when.push(tie(&format!("{}/key", field.label), value));
            }
            match &field.term {
                Term::Variable(name) if self.name(name) == field.label => {}
                Term::Blank => {
                    return Err(Error::Refused(format!(
                        "{} is concluded with no value",
                        field.attribute
                    )));
                }
                // A record the head concludes is built of what the
                // premises bind: the field is that record.
                Term::Struct(form) if read.domain.starts_with("report.") => {
                    let pattern = shape(form, false, &mut |name| match name {
                        Some(name) => Variable::var(self.name(name)),
                        None => Variable::blank(),
                    })?;
                    self.built.push((field.label.clone(), pattern));
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
        let built = self.built.iter().filter_map(|(field, pattern)| {
            match pattern.clone().is(Variable::<Any>::var(field.clone())) {
                DialogPremise::Assert(proposition) => Some(proposition),
                DialogPremise::Unless(_) => None,
            }
        });
        self.shapes
            .iter()
            .filter_map(|(place, _, pattern)| {
                match Pattern::from(Variable::<Any>::var(place.clone())).is(pattern.clone()) {
                    DialogPremise::Assert(proposition) => Some(proposition),
                    DialogPremise::Unless(_) => None,
                }
            })
            .chain(built)
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

/// A retract as the design decides it.
///
/// - Through a concept by its entity alone, `(retract (todo todo:1))`,
///   it "retracts every field of the concept on that entity": the rule
///   reads the fields on the entity and retracts what it read. At the
///   top level that read is a `when`, run once.
/// - Of one entry of a collection with the entity, the key and the value
///   written, it retracts that entry, as a retract of one attribute's
///   value retracts just that value (the managing agent's reading).
///   One that leaves out the key or the value is not decided.
///
/// A retract that names some fields of a concept and not others is a
/// write that leaves fields out, refused as one.
fn retracted(mut rule: Rule, form: &Syntax) -> Result<Rule, Error> {
    let Head::Retract(read) = &rule.head else {
        return Ok(rule);
    };
    let bound = rule.bound();
    let unnamed = |term: &Term| match term {
        Term::Variable(name) => name.contains('.') && !bound.contains(name),
        Term::Blank => true,
        _ => false,
    };
    if let Some(field) = read.fields.iter().find(|field| {
        field
            .key
            .as_ref()
            .is_some_and(|(key, _)| unnamed(key) || unnamed(&field.term))
    }) {
        return Err(Error::Unsupported(format!(
            "{form} retracts from {} and leaves out the key or the value of the entry: what \
             such a retract of a collection takes is not decided",
            field.attribute
        )));
    }
    let collection = read.fields.iter().any(|field| field.key.is_some());
    if !collection && !unnamed(&read.entity) && read.named(&bound).is_empty() {
        let every = read.clone();
        if rule.when.is_empty() && rule.unless.is_empty() {
            rule.once = true;
        }
        rule.when.push(Premise::Read(every));
    }
    Ok(rule)
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
    // Dialog-DB has the fold and refuses it here (`ReduceInOpenRule`):
    // a rule that derives is read by rules installed after it, and a
    // fold has no reading over a relation still being derived.
    if let Some(field) = read
        .fields
        .iter()
        .find(|field| matches!(field.term, Term::Fold { .. }))
    {
        return Err(Error::Unsupported(format!(
            "{} is a fold in a rule that derives: Dialog-DB admits a fold where nothing is still being derived, in a query, a subscription or a rule that writes, and refuses one in a rule that derives",
            field.attribute
        )));
    }
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
    // A query's row of a part read through several definitions names
    // the part: each definition is a rule of its own, and every one of
    // them derives the one relation the part is named by, so one select
    // reads them all.
    let mut read = read.clone();
    let mut site = site.to_string();
    let mut parted = false;
    if read.domain == crate::select::ROW
        && let Some(at) = read
            .fields
            .iter()
            .position(|field| field.label == crate::select::PART)
        && let Term::Text(part) = &read.fields[at].term
    {
        site = part.clone();
        read.fields.remove(at);
        parted = true;
    }
    let site = site.as_str();
    let read = &read;
    let bound = rule.bound();
    // A read of a command names no entity, so where it is reported each
    // side of the query has an entity of its expansion's own. The row
    // says the entity the premise of the same relation read.
    let mut read = read.clone();
    if let Term::Variable(name) = &read.entity
        && name.contains('.')
        && !bound.contains(name)
        && let Some(entity) = rule.when.iter().find_map(|premise| match premise {
            Premise::Read(premise) if premise.domain == read.domain => Some(premise.entity.clone()),
            _ => None,
        })
    {
        read.entity = entity;
    }
    let read = &read;
    let fields = read.named(&bound);
    let keys = fields
        .iter()
        .filter_map(|field| field.key.as_ref().map(|(key, _)| key));
    for term in fields
        .iter()
        .map(|field| &field.term)
        .chain(keys)
        .chain([&read.entity])
    {
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
        Term::Variable(_)
        | Term::Blank
        | Term::Formula(_)
        | Term::Struct(_)
        | Term::Fold { .. } => None,
    };

    // A query's row that is one record too is derived by a rule, so the
    // record is a value the select binds, which a fold can collect.
    let recorded = parted
        || read.domain == crate::select::ROW
            && fields
                .iter()
                .any(|field| matches!(field.term, Term::Struct(_)));
    if let ([Premise::Read(only)], []) = (rule.when.as_slice(), rule.unless.as_slice())
        && !recorded
    {
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
                        None => Ok(Slot::Built(shape(form, false, &mut |name| match name {
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
                    .map(|field| {
                        let key = match &field.key {
                            Some((key, _)) => Some(slot(key, &mut scope)?),
                            None => None,
                        };
                        Ok((field.label.clone(), key, slot(&field.term, &mut scope)?))
                    })
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

    // Twelve characters of the site's reference: a name is short, and
    // 58 to the twelfth sites leave two of them alike only by chance.
    let relation = format!("{REPORTED}{}", site.chars().take(12).collect::<String>());
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
        // An entry's key is a column of its own.
        let key = match &field.key {
            None => None,
            Some((key, _)) => Some(match shown(key) {
                Some(form) => Slot::Is(form),
                None => {
                    let column = format!("c{}", columns.len());
                    if let Term::Variable(name) = key {
                        scope
                            .names
                            .entry(name.clone())
                            .or_insert_with(|| column.clone());
                    }
                    columns.push(Field {
                        label: column.clone(),
                        attribute: format!("{relation}/{column}"),
                        term: key.clone(),
                        kind: None,
                        pick: None,
                        cardinality: None,
                        listed: Vec::new(),
                        layer: None,
                        key: None,
                    });
                    Slot::Bound(column)
                }
            }),
        };
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
                    cardinality: None,
                    listed: Vec::new(),
                    layer: None,
                    key: None,
                });
                Slot::Bound(column)
            }
        };
        // A field that is the entity reads as the entity, so the select
        // binds one variable to both.
        let slot = match (&field.term, &read.entity) {
            (Term::Variable(term), Term::Variable(entity)) if term == entity => {
                Slot::Bound("this".into())
            }
            _ => slot,
        };
        slots.push((field.label.clone(), key, slot));
    }
    if columns.is_empty() {
        columns.push(Field {
            label: "holds".into(),
            attribute: format!("{relation}/holds"),
            term: Term::Symbol("true".into()),
            kind: None,
            pick: None,
            cardinality: None,
            listed: Vec::new(),
            layer: None,
            key: None,
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
            steps(r#"(rule (assert (xyz.todo todo:1 text: (:last (:text "Buy milk")) n: 3)))"#)
                .unwrap();
        let [Step::Assert(statement)] = steps.as_slice() else {
            panic!("one assertion");
        };
        assert_eq!(statement.this.to_string(), "todo:1");
        // A tree keeps no order of labels, so the fields come by label.
        assert_eq!(statement.with[0].is, Value::UnsignedInt(3));
        assert_eq!(
            statement.with[0].cardinality,
            dialog_query::Cardinality::Many
        );
        assert_eq!(statement.with[1].is, Value::String("Buy milk".into()));
        assert_eq!(
            statement.with[1].cardinality,
            dialog_query::Cardinality::One
        );
    }

    #[test]
    fn it_compiles_a_head_to_a_rule_of_its_kind() {
        let premise = "(xyz.counter ?c count: (:last (:natural ?n)))";
        let asserted = steps(&format!(
            "(rule (assert (xyz.counter ?c count: (:last (:natural (+ ?n : 1))))) {premise})"
        ))
        .unwrap();
        assert!(matches!(asserted.as_slice(), [Step::Induce(_)]));
        let retracted = steps(&format!(
            "(rule (retract (xyz.counter ?c stale: ?n)) {premise})"
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
            "(rule (all (assert (xyz.counter ?c count: (:last (:natural (+ ?n : 1)))))
                        (retract (xyz.counter ?c stale: ?n)))
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

    /// An assert of claims on the control layer issues a command: a
    /// rule over a concept marked to hold one round. Its entity is
    /// derived from what it says.
    #[test]
    fn it_compiles_a_command_a_rule_issues_to_a_rule_over_a_transient_concept() {
        let steps = steps(
            "(rule (assert (xyz.go ?c counter: (:control ?d)))
               (xyz.counter ?d count: ?n))",
        )
        .unwrap();
        let [Step::Transient(concept), Step::Induce(rule)] = steps.as_slice() else {
            panic!("a transient concept and the rule that concludes it");
        };
        assert_eq!(*concept, rule.conclusion().this());

        // At the top level, a dispatched statement under the entity
        // its content derives, the same whatever the order it is said in.
        let issued = |text: &str| match compile(&parse(text).unwrap().remove(0)).unwrap().as_slice()
        {
            [Step::Issue(statement)] => statement.this.clone(),
            _ => panic!("one command issued"),
        };
        let one = issued(r#"(rule (assert (xyz.go ?c a: (:control 1) b: (:control "x"))))"#);
        assert!(one.as_str().starts_with("id:"));
        assert_eq!(
            one,
            issued(r#"(rule (assert (xyz.go ?d b: (:control "x") a: (:control 1))))"#)
        );
        assert_ne!(
            one,
            issued(r#"(rule (assert (xyz.go ?c a: (:control 2) b: (:control "x"))))"#)
        );
    }

    /// A fold is read, and nothing runs it yet. In a head that derives
    /// it is what Dialog-DB has and refuses there, which is said as
    /// unsupported. Anywhere else it is no term.
    #[test]
    fn it_says_a_fold_in_a_head_that_derives_is_not_supported() {
        assert!(matches!(
            steps(
                "(rule (concept.list.count-open ?l is: (count ?t))
                   (xyz.todo ?t list: ?l))"
            ),
            Err(Error::Unsupported(said)) if said.contains("concept.list.count-open/is is a fold")
        ));
        for written in [
            "(rule (assert (xyz.list ?l open: (count ?t))) (xyz.todo ?t list: ?l))",
            "(rule (xyz.busy ?l n: ?n) (xyz.todo ?t list: ?l) (xyz.cap ?l n: (count ?t)))",
        ] {
            assert!(
                matches!(
                    steps(written),
                    Err(Error::Refused(said)) if said.contains("stands only in a field of a head that derives")
                ),
                "{written}"
            );
        }
    }

    /// A retract of a collection that leaves out its entry's key or its
    /// value is not decided. One that writes the entity, the key and the
    /// value compiles, at the top level and in a rule.
    #[test]
    fn it_refuses_a_retract_from_a_collection_that_leaves_out_its_key_or_value() {
        for text in [
            "(rule (retract (xyz.n ?b entry: (:entry ?x.1 key: ?p by: :position))) (xyz.c ?b at: ?p))",
            "(rule (retract (xyz.n ?b entry: (:entry ?x key: ?p.1 by: :position))) (xyz.c ?b on: ?x))",
        ] {
            assert!(
                matches!(steps(text), Err(Error::Unsupported(why)) if why.contains("leaves out the key or the value")),
                "{text}"
            );
        }
        assert!(matches!(
            steps(r#"(rule (retract (xyz.n notebook:1 entry: (:entry block:a key: "N1" by: :position))))"#)
                .unwrap()
                .as_slice(),
            [Step::Retract(_)]
        ));
        assert!(matches!(
            steps("(rule (retract (xyz.n ?b entry: (:entry ?x key: ?p by: :position))) (xyz.c ?b at: ?p on: ?x))")
                .unwrap()
                .as_slice(),
            [Step::Induce(_)]
        ));
    }

    /// A retract through a concept by its entity alone reads every field
    /// of it and retracts what it read: in a rule, among the premises;
    /// at the top level, as a write that runs once.
    #[test]
    fn it_retracts_every_field_of_a_concept_named_by_its_entity() {
        assert!(matches!(
            steps("(rule (retract (xyz.block ?b source: ?s.1 notebook: ?n.1)) (xyz.remove ?c subject: ?b))")
                .unwrap()
                .as_slice(),
            [Step::Induce(_)]
        ));
        assert!(matches!(
            steps("(rule (retract (xyz.block block:1 source: ?s.1 notebook: ?n.1)))")
                .unwrap()
                .as_slice(),
            [Step::Write(_)]
        ));
        // Some fields named and not others is a write that leaves fields
        // out.
        assert!(matches!(
            steps(r#"(rule (retract (xyz.block block:1 source: "x" notebook: ?n.1)))"#),
            Err(Error::Refused(why)) if why.contains("a write says every field")
        ));
    }

    #[test]
    fn it_names_one_entity_whatever_order_the_parts_of_an_id_are_in() {
        let entity = |text: &str| match steps(text).unwrap().as_slice() {
            [Step::Assert(statement)] => statement.this.clone(),
            _ => panic!("one assertion"),
        };
        let one = entity(r#"(rule (assert (xyz.n (:id xyz.n a: 1 b: "x") a: 1)))"#);
        assert!(one.as_str().starts_with("id:"));
        assert_eq!(
            one,
            entity(r#"(rule (assert (xyz.n (:id xyz.n b: "x" a: 1) a: 1)))"#)
        );
        assert_ne!(
            one,
            entity(r#"(rule (assert (xyz.n (:id xyz.n a: 1 b: "y") a: 1)))"#)
        );
        assert_ne!(
            one,
            entity(r#"(rule (assert (xyz.m (:id xyz.m a: 1 b: "x") a: 1)))"#)
        );
    }

    #[test]
    fn it_refuses_a_write_that_leaves_a_field_out() {
        // `?l.1` is how a field an expansion added and nothing binds reads.
        assert!(matches!(
            steps("(rule (assert (xyz.todo ?t list: ?l.1 state: :done)) (xyz.go ?t on: :yes))"),
            Err(Error::Refused(_))
        ));
        assert!(matches!(
            steps("(rule (assert (xyz.todo todo:1 list: ?l.1 state: :done)))"),
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
