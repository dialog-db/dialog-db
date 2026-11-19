//! Queries, as the walker reads them: kept where they are defined, and
//! analyzed where they are asked.
//!
//! `(query HEAD condition…)` defines a query. Its head is a concept's,
//! and says under `is:` what it answers. Its body is conditions, and
//! `(is ?x shape)` for what a variable is. A condition is one of:
//!
//! - a read or a call of another query, which every answer needs;
//! - a read or a call with `(distinct ?x)` for one of its terms, which
//!   binds `?x` to the whole of that term, `{}` for none, and never
//!   keeps a row from being an answer.
//!
//! Nothing of a query is compiled where it is defined. `(select CALL)`
//! of one is analyzed into a `(select.query …)` tree that holds the
//! query and every query it reaches, each condition expanded to kernel
//! rules (see [`crate::select`]).
//!
//! A rule and a concept never read a query, and never collect: a rule's
//! conclusion is read by rules, so a fold there could feed itself.

use std::collections::{BTreeMap, BTreeSet};

use dialog_corpus::Entity;

use crate::Error;
use crate::definition::{self, Kind as DefinitionKind, Reference, SELF};
use crate::form::Kind;
use crate::select::{
    COLLECT, COUNT, DISTINCT, PART, ROW, ROW_FIELD, SELECTED, collected, unknown_function,
};
use crate::syntax::{Parameters, Syntax};

use super::{Expander, apply, items};

/// Where in a condition its `(distinct ?x)` stands.
#[derive(Clone, Debug)]
enum Place {
    /// The part with no label at this index: the entity.
    Item(usize),
    /// A labelled part.
    Term(String),
}

/// A condition that collects.
#[derive(Clone, Debug)]
struct Collecting {
    target: String,
    condition: Syntax,
    place: Place,
    /// Whether it calls a query.
    call: bool,
}

/// A query's body, sorted.
#[derive(Clone, Debug, Default)]
struct Body {
    /// Reads every answer needs, `(unless …)` among them.
    required: Vec<Syntax>,
    /// Calls of queries every answer needs.
    calls: Vec<Syntax>,
    collects: Vec<Collecting>,
    shapes: Vec<(String, Syntax)>,
    /// `(view ?x template)`: the answer is markup.
    view: Option<(String, Syntax)>,
}

/// The variable a query's conditions are reported under, as the entity
/// of their row.
const ROW_ENTITY: &str = "query-row";

/// Every `(distinct …)` in a tree, wherever it stands.
fn collects_in(tree: &Syntax) -> Vec<&Syntax> {
    applications_of(tree, DISTINCT)
}

/// Every application of `word` in a tree, wherever it stands.
fn applications_of<'a>(tree: &'a Syntax, word: &str) -> Vec<&'a Syntax> {
    let mut found = Vec::new();
    fn walk<'a>(tree: &'a Syntax, word: &str, found: &mut Vec<&'a Syntax>) {
        if tree.applies() == Some(word) {
            found.push(tree);
            return;
        }
        match tree {
            Syntax::Application(operator, operand) => {
                walk(operator, word, found);
                operand
                    .items
                    .iter()
                    .for_each(|item| walk(item, word, found));
                operand
                    .terms
                    .values()
                    .for_each(|term| walk(term, word, found));
            }
            Syntax::Map(entries) => entries.iter().for_each(|(key, value)| {
                walk(key, word, found);
                walk(value, word, found);
            }),
            Syntax::Tuple(items) => items.iter().for_each(|item| walk(item, word, found)),
            _ => {}
        }
    }
    walk(tree, word, &mut found);
    found
}

/// What `(collect ?x)` is refused with: the word is `distinct`.
fn collect_refused(found: &Syntax) -> Error {
    Error::Refused(format!(
        "{found}: the whole of a field is written `(distinct ?x)`, not `collect`: \
         `(todo/tag ?t tag: (distinct ?tags))` gives the set of its values, and of a keyed \
         field the set of its entries, which `{{... ?entries}}` pours into one map"
    ))
}

/// Every variable of `trees`, each once, in the order they first stand.
fn variables<'a>(trees: impl IntoIterator<Item = &'a Syntax>) -> Vec<String> {
    let mut all = Vec::new();
    for tree in trees {
        tree.variables(&mut all);
    }
    let mut seen = BTreeSet::new();
    all.retain(|name| seen.insert(name.clone()));
    all
}

/// `tree` with the term at `place` put in by `by`.
fn replaced(tree: &Syntax, place: &Place, by: Syntax) -> Syntax {
    let Syntax::Application(operator, operand) = tree else {
        return tree.clone();
    };
    let mut operand = operand.clone();
    match place {
        Place::Item(at) => operand.items[*at] = by,
        Place::Term(label) => {
            operand.terms.insert(label.clone(), by);
        }
    }
    Syntax::Application(operator.clone(), operand)
}

/// The term at `place`.
fn at<'a>(tree: &'a Syntax, place: &Place) -> Option<&'a Syntax> {
    let operand = tree.operand()?;
    match place {
        Place::Item(at) => operand.items.get(*at),
        Place::Term(label) => operand.terms.get(label),
    }
}

/// Whether a term is an entry: a key and a value, `{?k ?v}`, the key a
/// variable.
fn entry(term: &Syntax) -> bool {
    matches!(term, Syntax::Map(pair) if matches!(pair.as_slice(), [(Syntax::Variable(_), _)]))
}

/// Asking a query is written `(select …)`; what `(query …)` written the
/// old way, as a question, is refused with.
fn asks(tree: &Syntax) -> Error {
    Error::Refused(format!(
        "{tree} asks, and asking is written `(select …)`: `(query …)` defines a query, a head \
         that says under `is:` what it answers, `(query (todo/card ?t is: ?card) …)`, and the \
         conditions it answers by"
    ))
}

impl Expander {
    /// What the forms of the query layer are: a definition kept, a
    /// question of a query analyzed, and the refusals of `distinct`
    /// and of a query read where rows are. `None` for any other tree.
    pub(super) fn query_forms(&mut self, tree: &Syntax) -> Result<Option<Vec<Syntax>>, Error> {
        if matches!(
            tree.applies(),
            Some("query" | "select" | "rule" | "concept" | "assert" | "retract")
        ) && let Some(found) = applications_of(tree, COLLECT).first()
        {
            let found = (*found).clone();
            return self.about(&found, |_| Err(collect_refused(&found)));
        }
        match tree.applies() {
            Some("query") => {
                self.define(tree)?;
                Ok(Some(Vec::new()))
            }
            Some("select") => {
                if let Some(found) = collects_in(tree).first() {
                    let found = (*found).clone();
                    return self.about(&found, |_| {
                        Err(Error::Refused(format!(
                            "{found} stands in what is asked: the whole of a field is collected \
                             in a query's body, `(query (todo/card ?t is: ?card) … (todo/tag ?t \
                             tag: (distinct ?tags)) …)`, and the query is asked"
                        )))
                    });
                }
                match items(tree) {
                    [call] if call.applies().is_some_and(|name| self.is_query(name)) => {
                        let call = call.clone();
                        self.about(&call, |this| this.selected(&call))
                            .map(|selected| Some(vec![selected]))
                    }
                    parts
                        if parts
                            .iter()
                            .any(|part| part.applies().is_some_and(|name| self.is_query(name))) =>
                    {
                        Err(Error::Refused(format!(
                            "{tree} asks a query with something beside it: a query is asked \
                             alone, `(select (todo/card ?t is: ?card))`, and its conditions are \
                             in its body"
                        )))
                    }
                    _ => Ok(None),
                }
            }
            Some("view") => Err(Error::Refused(format!(
                "{tree}: a view is a query whose answer is markup, `(query (todo/row ?t is: \
                 ?row) (todo ?t title: ?title) (view ?row [:li ?title]))`, and a view in a \
                 template is a query collected, `(todo/chip ?t is: (distinct ?chips))`"
            ))),
            Some(word @ ("rule" | "concept" | "assert" | "retract")) => {
                let word = word.to_string();
                // A write's head that collects is a write of the whole.
                let head = items(tree).first();
                let written = match word.as_str() {
                    "assert" | "retract" => head,
                    _ => head.filter(|head| matches!(head.applies(), Some("assert" | "retract"))),
                };
                if let Some(found) = written.and_then(|head| collects_in(head).first().copied()) {
                    let found = found.clone();
                    return self.about(&found, |_| {
                        Err(Error::Refused(format!(
                            "{found} writes the whole of a field: a write goes through one \
                             entry, `(assert (notebook/blocks ?n block: {{\"N3\" block:x}}))`, \
                             and the whole is not written"
                        )))
                    });
                }
                if let Some(found) = collects_in(tree).first() {
                    let found = (*found).clone();
                    let why = if word == "concept" {
                        "a concept maps facts both ways, and the whole of a field is no fact"
                    } else {
                        "a rule's conclusion is read by rules, so a fold in one could feed itself"
                    };
                    return self.about(&found, |_| {
                        Err(Error::Refused(format!(
                            "{found} stands in a {word}, and distinct stands only in a query: \
                             {why}"
                        )))
                    });
                }
                if word == "concept"
                    && let Some(call) = items(tree)
                        .iter()
                        .skip(1)
                        .find(|line| line.applies().is_some_and(|name| self.is_query(name)))
                {
                    let call = call.clone();
                    return self.about(&call, |this| Err(this.read_where_rows_are(&call)));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// What a query applied in a rule or a concept is refused with.
    pub(super) fn read_where_rows_are(&self, call: &Syntax) -> Error {
        Error::Refused(format!(
            "{call} applies the query {}: a query's answer is read by whoever asks, `(select …)`, \
             and by a query, never by a rule or a concept, which give rows",
            call.applies().unwrap_or_default()
        ))
    }

    /// Keep a query's definition, once it is one that can be answered.
    fn define(&mut self, tree: &Syntax) -> Result<(), Error> {
        let [head, body @ ..] = items(tree) else {
            return Err(asks(tree));
        };
        if body.is_empty() {
            return Err(asks(tree));
        }
        let (Some(name), Some(operand)) = (head.applies(), head.operand()) else {
            return Err(asks(tree));
        };
        // A domain or a stored attribute is read, not defined.
        let stored = match Kind::of(name) {
            Kind::Domain | Kind::Entity | Kind::Operator => true,
            Kind::Attribute => name
                .split_once('/')
                .is_some_and(|(domain, _)| domain.contains('.')),
            Kind::Name => false,
        };
        if stored || !operand.terms.contains_key("is") {
            return Err(asks(tree));
        }
        let name = name.to_string();
        let _ = self.rows(&name);
        if self.concepts.contains(&name) {
            return Err(Error::Refused(format!(
                "{head}: {name} is a concept, and a query is named apart from what it reads"
            )));
        }
        if operand.items.len() > 1 {
            return Err(Error::Refused(format!(
                "{head} names more than one entity: a query's head is a concept's"
            )));
        }
        let sorted = self.body(&name, body)?;
        self.safe(head, &sorted)?;
        // It answers, as far as walking it can tell: every read in it
        // expands, and every query it calls is known. What is refused
        // is said of what was written.
        let outer = self.referenced.replace(Vec::new());
        let analyzed = self.analyze_query(&name, tree, false);
        let references = std::mem::replace(&mut self.referenced, outer).unwrap_or_default();
        analyzed?;
        // What is kept is the query analyzed: every reference resolved
        // and written out, every query it reaches beside it, each named
        // by its entity, and its own name `@`. Its entity is made of the
        // same, analyzed from its canonical form.
        let (own, mut reached) = self.analyze_query(&name, tree, false)?;
        let canonical = canonical_query(tree)?;
        // A callee is in it by its entity, which its calls name: what it
        // was kept as is no part of the caller's.
        let (identity, mut written) = self.analyze_query(&name, &canonical, true)?;
        reached.push(own);
        let kept = definition::Definition::new(
            DefinitionKind::Query,
            apply(REACHED, reached),
            &apply(REACHED, vec![identity.clone()]),
        )?;
        // What is written of it is its canonical form, each callee in it
        // as that callee is written.
        written.push(identity);
        self.canonical
            .insert(kept.entity.clone(), apply(REACHED, written));
        let key = definition::signature(DefinitionKind::Query, operand.terms.keys().cloned());
        self.record(kept, name, key, references)?;
        self.declared += 1;
        Ok(())
    }

    /// Whether `name` names a query some namespace binds.
    pub(super) fn is_query(&self, name: &str) -> bool {
        self.rows(name).is_ok_and(|rows| {
            rows.iter().any(|row| {
                self.definitions
                    .get(&row.definition)
                    .is_some_and(|kept| kept.kind == DefinitionKind::Query)
            })
        })
    }

    /// The query `name` names, applied as `call` is: its entity and what
    /// it was kept as, every query it reaches and then its own, last,
    /// named `@`. Resolved by the labels the call writes among the
    /// queries the space binds the name to; several the labels fit are
    /// refused, the candidates listed.
    pub(super) fn query_named(
        &self,
        name: &str,
        call: &Syntax,
    ) -> Result<Option<(Entity, Vec<Syntax>)>, Error> {
        let written: BTreeSet<String> = call
            .operand()
            .map(|operand| operand.terms.keys().cloned().collect())
            .unwrap_or_default();
        let rows: Vec<definition::Name> = self
            .rows(name)?
            .into_iter()
            .filter(|row| {
                self.definitions
                    .get(&row.definition)
                    .is_some_and(|kept| kept.kind == DefinitionKind::Query)
                    && definition::labels_of(&row.signature).is_some_and(|(_, labels)| {
                        written.iter().all(|label| labels.contains(label))
                    })
            })
            .collect();
        let mut entities: Vec<&Entity> = rows.iter().map(|row| &row.definition).collect();
        entities.sort();
        entities.dedup();
        match entities.as_slice() {
            [] => Ok(None),
            [entity] => {
                let kept = &self.definitions[*entity];
                Ok(Some(((*entity).clone(), items(&kept.form).to_vec())))
            }
            _ => {
                let mut candidates: Vec<(String, String)> = rows
                    .iter()
                    .map(|row| (row.signature.clone(), row.definition.to_string()))
                    .collect();
                candidates.sort();
                candidates.dedup();
                Err(Error::Several(Box::new(crate::Candidates {
                    name: name.to_string(),
                    candidates,
                })))
            }
        }
    }

    /// The query `name` that `tree` defines, analyzed: its own tree, its
    /// name `@` and each call naming its callee by entity, and the trees
    /// of every query it reaches, as each was kept.
    fn analyze_query(
        &mut self,
        name: &str,
        tree: &Syntax,
        canonical: bool,
    ) -> Result<(Syntax, Vec<Syntax>), Error> {
        let [head, conditions @ ..] = items(tree) else {
            return Err(asks(tree));
        };
        let head = head.clone();
        let body = self.body(name, conditions)?;
        let mut reached: Vec<Syntax> = Vec::new();
        // Each query called, by the name written: the name it is kept
        // under in what reaches it, its entity, and its head.
        let mut callees: BTreeMap<String, (String, Syntax)> = BTreeMap::new();
        for called in body.calls.iter().chain(
            body.collects
                .iter()
                .filter(|collect| collect.call)
                .map(|collect| &collect.condition),
        ) {
            let callee = called.applies().unwrap_or_default().to_string();
            if callees.contains_key(&callee) {
                continue;
            }
            let (entity, kept) = self
                .about(called, |this| this.query_named(&callee, called))?
                .ok_or_else(|| Error::Unknown(callee.clone()))?;
            let kept = match self.canonical_of(&entity).filter(|_| canonical) {
                Some(form) => items(&form).to_vec(),
                None => kept,
            };
            let repository = self
                .rows(&callee)?
                .into_iter()
                .find(|row| row.definition == entity)
                .map(|row| row.repository)
                .unwrap_or_else(|| self.spaces.repository().clone());
            if let Some(referenced) = self.referenced.as_mut() {
                let reference = Reference {
                    name: callee.clone(),
                    repository,
                    entity: entity.clone(),
                };
                if !referenced.contains(&reference) {
                    referenced.push(reference);
                }
            }
            let own = entity.to_string();
            let mut callee_head = Syntax::Blank;
            for (at, kept_tree) in kept.iter().enumerate() {
                let kept_tree = if at + 1 == kept.len() {
                    let renamed = renamed_head(kept_tree, &own);
                    callee_head = items(&renamed).first().cloned().unwrap_or(Syntax::Blank);
                    renamed
                } else {
                    kept_tree.clone()
                };
                let named = items(&kept_tree)
                    .first()
                    .and_then(Syntax::applies)
                    .map(str::to_string);
                if !reached.iter().any(|done| {
                    items(done)
                        .first()
                        .and_then(Syntax::applies)
                        .map(str::to_string)
                        == named
                }) {
                    reached.push(kept_tree);
                }
            }
            callees.insert(callee, (own, callee_head));
        }
        let called = |call: &Syntax| match call.applies().and_then(|name| callees.get(name)) {
            Some((own, _)) => definition::operated(call, own),
            None => call.clone(),
        };
        let mut parts = vec![definition::operated(&head, SELF)];
        if !body.required.is_empty() {
            let columns = variables(
                body.required
                    .iter()
                    .filter(|condition| condition.applies() != Some("unless")),
            );
            let rules = self.reported(&columns, &body.required)?;
            let mut required = vec![Syntax::Tuple(
                columns.iter().cloned().map(Syntax::Variable).collect(),
            )];
            required.extend(rules);
            parts.push(apply("required", required));
        }
        for call in &body.calls {
            parts.push(apply("call", vec![called(call)]));
        }
        for (index, collect) in body.collects.iter().enumerate() {
            if self.missing(collect, &body) {
                let source = self.about(&collect.condition, |this| this.source(collect))?;
                parts.push(apply(
                    DISTINCT,
                    vec![
                        Syntax::Variable(collect.target.clone()),
                        Syntax::Blank,
                        source,
                    ],
                ));
                continue;
            }
            let keyed = if collect.call {
                let callee = collect.condition.applies().unwrap_or_default();
                callees
                    .get(callee)
                    .and_then(|(_, head)| at(head, &collect.place))
                    .is_some_and(entry)
            } else {
                match (&collect.place, collect.condition.applies()) {
                    (Place::Term(label), Some(concept)) => self.entries.get(concept) == Some(label),
                    _ => false,
                }
            };
            let item = if keyed {
                Syntax::Map(vec![(
                    Syntax::Variable(format!("distinct-{index}-key")),
                    Syntax::Variable(format!("distinct-{index}-value")),
                )])
            } else {
                Syntax::Variable(format!("distinct-{index}"))
            };
            let condition = replaced(&collect.condition, &collect.place, item.clone());
            let source = if collect.call {
                apply("call", vec![called(&condition)])
            } else {
                let mut shared = Vec::new();
                replaced(&collect.condition, &collect.place, Syntax::Blank).variables(&mut shared);
                let mut columns = variables(std::iter::once(&Syntax::Tuple(
                    shared.into_iter().map(Syntax::Variable).collect(),
                )));
                columns.extend(variables([&item]));
                let rules = self.about(&collect.condition, |this| {
                    this.reported(&columns, std::slice::from_ref(&condition))
                })?;
                let mut rows = vec![Syntax::Tuple(
                    columns.iter().cloned().map(Syntax::Variable).collect(),
                )];
                rows.extend(rules);
                apply("rows", rows)
            };
            parts.push(apply(
                DISTINCT,
                vec![Syntax::Variable(collect.target.clone()), item, source],
            ));
        }
        for (target, shape) in &body.shapes {
            parts.push(apply(
                "is",
                vec![Syntax::Variable(target.clone()), shape.clone()],
            ));
        }
        if let Some((target, template)) = &body.view {
            parts.push(apply(
                "view",
                vec![Syntax::Variable(target.clone()), template.clone()],
            ));
        }
        Ok((apply("query", parts), reached))
    }

    /// A query's body sorted: what every answer needs, what collects,
    /// and what each variable `is`.
    fn body(&mut self, name: &str, conditions: &[Syntax]) -> Result<Body, Error> {
        let mut body = Body::default();
        for condition in conditions {
            self.about(condition, |this| this.sort(name, condition, &mut body))?;
        }
        Ok(body)
    }

    fn sort(&mut self, name: &str, condition: &Syntax, body: &mut Body) -> Result<(), Error> {
        match condition.applies() {
            Some("is") => {
                let (Some(operand), [Syntax::Variable(target), shape]) =
                    (condition.operand(), items(condition))
                else {
                    return Err(Error::Refused(format!(
                        "{condition}: `is` says what a variable is, `(is ?card {{:title ?title}})`"
                    )));
                };
                if !operand.terms.is_empty() {
                    return Err(Error::Refused(format!(
                        "{condition}: `is` says what a variable is, `(is ?card {{:title ?title}})`"
                    )));
                }
                functions(shape)?;
                body.shapes.push((target.clone(), shape.clone()));
                return Ok(());
            }
            Some("view") => {
                let (Some(operand), [Syntax::Variable(target), template]) =
                    (condition.operand(), items(condition))
                else {
                    return Err(Error::Refused(format!(
                        "{condition}: `view` says a variable is markup, `(view ?row [:li ?title])`"
                    )));
                };
                if !operand.terms.is_empty() {
                    return Err(Error::Refused(format!(
                        "{condition}: `view` says a variable is markup, `(view ?row [:li ?title])`"
                    )));
                }
                if body.view.is_some() {
                    return Err(Error::Refused(format!(
                        "{condition}: a query's answer is markup once; a part of it is a query of \
                         its own, collected"
                    )));
                }
                crate::view::template(template)?;
                body.view = Some((target.clone(), template.clone()));
                return Ok(());
            }
            Some("unless") => {
                if let Some(found) = collects_in(condition).first() {
                    return Err(Error::Refused(format!(
                        "{found} stands under `unless`, which binds nothing: the whole of a \
                         field is collected by a read of its own"
                    )));
                }
                body.required.push(condition.clone());
                return Ok(());
            }
            _ => {}
        }
        let Some(called) = condition.applies() else {
            return Err(Error::Refused(format!(
                "{condition} is not a condition: a query's body holds reads, calls, and `(is ?x \
                 shape)`"
            )));
        };
        let call = self.is_query(called) || called == name;
        if called == name {
            return Err(Error::Refused(format!(
                "{condition}: the query {name} calls itself, and a query that calls itself is \
                 not answered yet"
            )));
        }
        let operand = condition.operand().cloned().unwrap_or_default();
        let mut places: Vec<(Place, String)> = Vec::new();
        for (index, item) in operand.items.iter().enumerate() {
            if let Some(target) = collected(item) {
                places.push((Place::Item(index), target?.to_string()));
            }
        }
        for (label, term) in &operand.terms {
            if let Some(target) = collected(term) {
                places.push((Place::Term(label.clone()), target?.to_string()));
            }
        }
        let all = collects_in(condition);
        if all.len() > places.len() {
            return Err(Error::Refused(format!(
                "{} stands inside a term: distinct is a term of a read, `(todo/tag ?t tag: \
                 (distinct ?tags))`",
                all[0]
            )));
        }
        match places.as_slice() {
            [] if call => body.calls.push(condition.clone()),
            [] => body.required.push(condition.clone()),
            [(place, target)] => body.collects.push(Collecting {
                target: target.clone(),
                condition: condition.clone(),
                place: place.clone(),
                call,
            }),
            _ => {
                return Err(Error::Refused(format!(
                    "{condition} collects more than one of its terms: a read collects one, and \
                     the others are what it is grouped by"
                )));
            }
        }
        Ok(())
    }

    /// The safety of a query: what every answer is bound by.
    fn safe(&self, head: &Syntax, body: &Body) -> Result<(), Error> {
        let mut bound: Vec<String> = variables(
            body.required
                .iter()
                .filter(|condition| condition.applies() != Some("unless")),
        );
        bound.extend(variables(&body.calls));
        let entity = items(head).first();
        if entity.is_some() && body.required.is_empty() && body.calls.is_empty() {
            return Err(Error::Refused(format!(
                "{head} answers of an entity and its body only collects: nothing says which \
                 entities it answers for. A read that does not collect says that, or a query of \
                 no entity, `(query (table/directory is: ?d) …)`, answers once"
            )));
        }
        if let Some(Syntax::Variable(name)) = entity
            && !bound.contains(name)
        {
            return Err(Error::Refused(format!(
                "{head}: ?{name}, the entity it answers of, is bound by no condition that does \
                 not collect"
            )));
        }
        let mut given = bound.clone();
        for collect in &body.collects {
            let mut held = Vec::new();
            replaced(&collect.condition, &collect.place, Syntax::Blank).variables(&mut held);
            if let Some(free) = held.iter().find(|name| !bound.contains(name)) {
                return Err(Error::Refused(format!(
                    "{} collects, and a read that collects binds nothing and keeps no row from \
                     being an answer: so ?{free} in it is bound by a condition that does not \
                     collect, or is a constant or `_`",
                    collect.condition
                )));
            }
            if bound.contains(&collect.target) || given.contains(&collect.target) {
                return Err(Error::Refused(format!(
                    "{}: ?{} is the whole of a field, and is bound by nothing else",
                    collect.condition, collect.target
                )));
            }
            given.push(collect.target.clone());
        }
        for (target, shape) in &body.shapes {
            let mut held = Vec::new();
            shape.variables(&mut held);
            if let Some(free) = held.iter().find(|name| !given.contains(name)) {
                return Err(Error::Refused(format!(
                    "(is ?{target} {shape}): ?{free} is bound by nothing the query says"
                )));
            }
            given.push(target.clone());
        }
        if let Some((target, template)) = &body.view {
            let node = crate::view::template(template)?;
            if let Some(free) = crate::view::variables(&node)
                .into_iter()
                .find(|name| !given.iter().any(|bound| bound == name))
            {
                return Err(Error::Refused(format!(
                    "(view ?{target} …) holds ?{free}, which no condition of the query binds"
                )));
            }
            self.events(&node)?;
            given.push(target.clone());
        }
        let mut held = Vec::new();
        head.variables(&mut held);
        if let Some(free) = held.iter().find(|name| !given.contains(name)) {
            return Err(Error::Refused(format!(
                "{head}: ?{free} is bound by nothing the query says"
            )));
        }
        Ok(())
    }

    /// Every event of a template posts a declared command, with every
    /// field of it and no other.
    fn events(&self, node: &crate::view::Node) -> Result<(), Error> {
        for event in crate::view::events(node) {
            // The command is looked up, so a driver reads it.
            let _ = self.rows(&event.command);
            if !self.commands.contains_key(&event.command) {
                return Err(Error::Refused(format!(
                    ":on/{} posts {}, which is no command declared",
                    event.kind, event.command
                )));
            }
            let declared = self.syntax.iter().find_map(|rule| {
                rule.signature
                    .as_ref()
                    .filter(|signature| signature.name == event.command)
            });
            let given: BTreeSet<String> = event
                .fields
                .iter()
                .map(|(label, _)| label.clone())
                .collect();
            if let Some(declared) = declared
                && declared.parameters != given
            {
                return Err(Error::Refused(format!(
                    ":on/{} posts {} with {}: an event gives every field of its command and no \
                     other, {}",
                    event.kind,
                    event.command,
                    given.iter().cloned().collect::<Vec<_>>().join(" "),
                    declared
                        .parameters
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(" ")
                )));
            }
        }
        Ok(())
    }

    /// Whether a collecting read is of a query no document defines, named
    /// under a declared concept, whose whole the query's markup holds: a
    /// view not found, shown as its source.
    fn missing(&self, collect: &Collecting, body: &Body) -> bool {
        let Some(name) = collect.condition.applies() else {
            return false;
        };
        let held = body.view.as_ref().is_some_and(|(_, template)| {
            crate::view::template(template)
                .map(|node| crate::view::variables(&node).contains(&collect.target))
                .unwrap_or(false)
        });
        if let Some((concept, _)) = name.split_once('/') {
            let _ = self.rows(concept);
        }
        held && !collect.call
            && matches!(&collect.place, Place::Term(label) if label == "is")
            && !self.concepts.contains(name)
            && !self.is_query(name)
            && name
                .split_once('/')
                .is_some_and(|(concept, _)| self.concepts.contains(concept))
    }

    /// `(select CALL)` of a query: the query and every query it reaches,
    /// as each was kept when it was defined. Nothing is analyzed again,
    /// so a query defined again under a callee's name changes nothing a
    /// query kept before it answers.
    fn selected(&mut self, call: &Syntax) -> Result<Syntax, Error> {
        let name = call.applies().unwrap_or_default().to_string();
        let (_, mut kept) = self
            .query_named(&name, call)?
            .ok_or_else(|| Error::Unknown(name.clone()))?;
        let Some(own) = kept.pop() else {
            return Err(Error::Unknown(name));
        };
        let mut own = renamed_head(&own, &name);
        // What the call gives by value narrows the query it asks, so its
        // reads are of that and not of every answer: one card is not
        // every card. A query whose answer is markup is laid out for
        // whatever entity it is rendered for, and is not.
        if !items(&own)
            .iter()
            .skip(1)
            .any(|part| part.applies() == Some("view"))
        {
            let given = bound_by(call, items(&own).first());
            if !given.is_empty() {
                own = narrowed(&own, &given);
            }
        }
        let mut parts = vec![call.clone()];
        parts.extend(kept);
        parts.push(own);
        Ok(apply(SELECTED, parts))
    }

    /// What shows a query no document defines as its source: the
    /// concept it is named under, applied to the call's entity with every
    /// field, those the call names narrowed by what it gives:
    /// `(missing CONCEPT ENTITY {label term …} [column…] RULE…)`.
    fn source(&mut self, collect: &Collecting) -> Result<Syntax, Error> {
        let name = collect.condition.applies().unwrap_or_default().to_string();
        let concept = name
            .split_once('/')
            .map(|(concept, _)| concept.to_string())
            .unwrap_or_default();
        let operand = collect.condition.operand().cloned().unwrap_or_default();
        let entity = match operand.items.first() {
            Some(entity @ (Syntax::Variable(_) | Syntax::Constant(_))) => entity.clone(),
            _ => Syntax::Variable(crate::view::CALLED.into()),
        };
        let fields: Vec<String> = self
            .syntax
            .iter()
            .find_map(|rule| {
                rule.signature
                    .as_ref()
                    .filter(|signature| signature.name == concept)
            })
            .map(|signature| signature.parameters.iter().cloned().collect())
            .unwrap_or_default();
        let mut terms = BTreeMap::new();
        for label in &fields {
            let term = match operand.terms.get(label) {
                Some(term) if label != "is" => term.clone(),
                _ => {
                    let value = Syntax::Variable(format!("source-{label}"));
                    if self.entries.get(&concept) == Some(label) {
                        Syntax::Map(vec![(Syntax::Variable("source-key".into()), value)])
                    } else {
                        value
                    }
                }
            };
            terms.insert(label.clone(), term);
        }
        let read = Syntax::Application(
            Box::new(Syntax::Reference(concept.clone())),
            Parameters {
                items: vec![entity.clone()],
                terms: terms.clone(),
            },
        );
        let columns = variables([&read]);
        let rules = self.reported(&columns, std::slice::from_ref(&read))?;
        let mut parts = vec![
            Syntax::Reference(concept),
            entity,
            Syntax::Map(
                terms
                    .into_iter()
                    .map(|(label, term)| (Syntax::Reference(label), term))
                    .collect(),
            ),
            Syntax::Tuple(columns.into_iter().map(Syntax::Variable).collect()),
        ];
        parts.extend(rules);
        Ok(apply("missing", parts))
    }

    /// The kernel rules that report what `conditions` bind, a column a
    /// variable of `columns` in order, `c0` and on, under the entity the
    /// conditions are about; with more than one column beside it, as one
    /// record under [`ROW_FIELD`] too.
    fn reported(
        &mut self,
        columns: &[String],
        conditions: &[Syntax],
    ) -> Result<Vec<Syntax>, Error> {
        if columns.is_empty() {
            return Err(Error::Unsupported(format!(
                "{}: conditions that bind no variable are a check, and a query of one is not \
                 built yet",
                conditions
                    .iter()
                    .map(Syntax::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
            )));
        }
        let mut terms: BTreeMap<String, Syntax> = columns
            .iter()
            .enumerate()
            .map(|(at, name)| (format!("c{at}"), Syntax::Variable(name.clone())))
            .collect();
        // A row is reported under the entity its reads are about, so a
        // change to that entity's facts is maintained for that entity
        // alone. Each column is a field. Where more than one column is
        // not the entity, two rows of one entity could be read crossed,
        // a field of one beside a field of the other, so the row is
        // also one record, which is one value and keeps them together.
        let about = about(conditions)
            .filter(|about| matches!(about, Syntax::Variable(name) if columns.contains(name)));
        let rule = match about {
            Some(about) => {
                if columns.len() > 2 {
                    terms.insert(
                        ROW_FIELD.into(),
                        Syntax::Map(
                            columns
                                .iter()
                                .enumerate()
                                .map(|(at, name)| {
                                    (
                                        Syntax::Reference(format!("c{at}")),
                                        Syntax::Variable(name.clone()),
                                    )
                                })
                                .collect(),
                        ),
                    );
                }
                let mut parts = vec![apply(
                    "report",
                    vec![Syntax::Application(
                        Box::new(Syntax::Reference(ROW.into())),
                        Parameters {
                            items: vec![about],
                            terms,
                        },
                    )],
                )];
                parts.extend(conditions.iter().cloned());
                apply("rule", parts)
            }
            // Reads of no entity a variable names: the row is told apart
            // by an entity made of all its values.
            None => {
                let row = Syntax::Variable(ROW_ENTITY.into());
                let head = apply(
                    "report",
                    vec![Syntax::Application(
                        Box::new(Syntax::Reference(ROW.into())),
                        Parameters {
                            items: vec![row.clone()],
                            terms: terms.clone(),
                        },
                    )],
                );
                let mut parts = vec![head];
                parts.extend(conditions.iter().cloned());
                parts.push(Syntax::Application(
                    Box::new(Syntax::Reference("id".into())),
                    Parameters {
                        items: vec![row],
                        terms,
                    },
                ));
                apply("rule", parts)
            }
        };
        let mut rules = Vec::new();
        self.each(&rule, &mut rules)?;
        // Read through several definitions, each is a rule of its own;
        // they are told they report one part, so that they derive one
        // relation, and one query reads every row of the part.
        if rules.len() > 1 {
            let part = Syntax::Constant(crate::data::text(rule.reference()?.to_string()));
            rules = rules.iter().map(|each| parted(each, &part)).collect();
        }
        Ok(rules)
    }
}

/// A kernel rule that reports a query's row, with the row naming
/// `part`.
fn parted(rule: &Syntax, part: &Syntax) -> Syntax {
    let mut rule = rule.clone();
    if let Syntax::Application(_, operand) = &mut rule
        && let Some(Syntax::Application(_, report)) = operand.items.first_mut()
        && let Some(Syntax::Application(_, row)) = report.items.first_mut()
    {
        row.terms.insert(PART.into(), part.clone());
    }
    rule
}

/// The entity a query's conditions are about: the variable most of the
/// reads among them are of, the first of those that tie. A read is a
/// concept, an attribute or a domain applied to its entity first.
fn about(conditions: &[Syntax]) -> Option<Syntax> {
    let mut counted: Vec<(String, usize)> = Vec::new();
    for condition in conditions {
        if condition.is_formula() || condition.applies().is_none() {
            continue;
        }
        if let Some(Syntax::Variable(entity)) = items(condition).first() {
            match counted.iter_mut().find(|(name, _)| name == entity) {
                Some((_, count)) => *count += 1,
                None => counted.push((entity.clone(), 1)),
            }
        }
    }
    let most = counted.iter().map(|(_, count)| *count).max()?;
    counted
        .into_iter()
        .find(|(_, count)| *count == most)
        .map(|(name, _)| Syntax::Variable(name))
}

/// The head's variables a call gives a value for: its entity and each
/// field, where the call writes a constant.
fn bound_by(call: &Syntax, head: Option<&Syntax>) -> BTreeMap<String, Syntax> {
    let mut given = BTreeMap::new();
    let (Some(wanted), Some(own)) = (call.operand(), head.and_then(Syntax::operand)) else {
        return given;
    };
    let mut pair = |term: &Syntax, own: &Syntax| {
        if let (Syntax::Constant(_), Syntax::Variable(variable)) = (term, own) {
            given.insert(variable.clone(), term.clone());
        }
    };
    for (term, own) in wanted.items.iter().zip(&own.items) {
        pair(term, own);
    }
    for (label, term) in &wanted.terms {
        if let Some(own) = own.terms.get(label) {
            pair(term, own);
        }
    }
    given
}

/// Refuse a function of what was collected other than `count`.
fn functions(shape: &Syntax) -> Result<(), Error> {
    match shape {
        Syntax::Application(_, operand) => {
            if shape.applies() != Some(COUNT) {
                return Err(unknown_function(shape));
            }
            operand.items.iter().try_for_each(functions)
        }
        Syntax::Map(entries) => entries.iter().try_for_each(|(key, value)| {
            functions(key)?;
            functions(value)
        }),
        Syntax::Tuple(items) => items.iter().try_for_each(functions),
        _ => Ok(()),
    }
}

/// What a query is kept as: every query it reaches, then its own.
const REACHED: &str = "reached";

/// A kept query's tree with its head named `name` for `@`.
fn renamed_head(tree: &Syntax, name: &str) -> Syntax {
    let Syntax::Application(operator, operand) = tree else {
        return tree.clone();
    };
    let mut operand = operand.clone();
    if let Some(head) = operand.items.first_mut()
        && head.applies() == Some(SELF)
    {
        *head = definition::operated(head, name);
    }
    Syntax::Application(operator.clone(), operand)
}

/// A query's tree with each rule its rows are read by narrowed to what
/// a call gives: `(== ?v is: value)` among its premises, where `?v`
/// stands in it.
fn narrowed(query: &Syntax, given: &BTreeMap<String, Syntax>) -> Syntax {
    fn rule(tree: &Syntax, given: &BTreeMap<String, Syntax>) -> Syntax {
        let Syntax::Application(operator, operand) = tree else {
            return tree.clone();
        };
        let mut names = Vec::new();
        tree.variables(&mut names);
        let mut operand = operand.clone();
        for (variable, value) in given {
            if names.contains(variable) {
                let mut terms = BTreeMap::new();
                terms.insert("is".to_string(), value.clone());
                operand.items.push(Syntax::Application(
                    Box::new(Syntax::Reference("==".into())),
                    Parameters {
                        items: vec![Syntax::Variable(variable.clone())],
                        terms,
                    },
                ));
            }
        }
        Syntax::Application(operator.clone(), operand)
    }
    fn rows(part: &Syntax, given: &BTreeMap<String, Syntax>) -> Syntax {
        let Syntax::Application(operator, operand) = part else {
            return part.clone();
        };
        let mut operand = operand.clone();
        for item in operand.items.iter_mut().skip(1) {
            if item.applies() == Some("rule") {
                *item = rule(item, given);
            }
        }
        Syntax::Application(operator.clone(), operand)
    }
    let Syntax::Application(operator, operand) = query else {
        return query.clone();
    };
    let mut operand = operand.clone();
    for part in operand.items.iter_mut().skip(1) {
        match part.applies() {
            Some("required") => *part = rows(part, given),
            Some(DISTINCT) => {
                if let Syntax::Application(_, inner) = part
                    && let Some(source) = inner.items.get_mut(2)
                    && source.applies() == Some("rows")
                {
                    *source = rows(source, given);
                }
            }
            _ => {}
        }
    }
    Syntax::Application(operator.clone(), operand)
}

/// A query's definition in canonical form: its own name `@`, its
/// variables named by the engine's labelling, its conditions in the
/// order of their encoding and each `is` after what it is made of.
fn canonical_query(tree: &Syntax) -> Result<Syntax, Error> {
    let [head, conditions @ ..] = items(tree) else {
        return Err(asks(tree));
    };
    let head = definition::operated(head, SELF);
    let (fixed, set) = definition::canonical(std::slice::from_ref(&head), conditions)?;
    let mut ordered: Vec<Syntax> = set
        .iter()
        .filter(|part| !matches!(part.applies(), Some("is" | "view")))
        .cloned()
        .collect();
    let mut shapes: Vec<Syntax> = set
        .iter()
        .filter(|part| part.applies() == Some("is"))
        .cloned()
        .collect();
    let target = |shape: &Syntax| match items(shape).first() {
        Some(Syntax::Variable(name)) => Some(name.clone()),
        _ => None,
    };
    while !shapes.is_empty() {
        let waiting: Vec<String> = shapes.iter().filter_map(target).collect();
        let ready = shapes
            .iter()
            .position(|shape| {
                let own = target(shape);
                let mut needs = Vec::new();
                if let Some(made) = items(shape).get(1) {
                    made.variables(&mut needs);
                }
                needs
                    .iter()
                    .all(|name| Some(name) == own.as_ref() || !waiting.contains(name))
            })
            .unwrap_or(0);
        ordered.push(shapes.remove(ready));
    }
    ordered.extend(
        set.iter()
            .filter(|part| part.applies() == Some("view"))
            .cloned(),
    );
    let mut parts = fixed;
    parts.extend(ordered);
    Ok(Syntax::Application(
        Box::new(Syntax::Reference("query".into())),
        Parameters {
            items: parts,
            terms: tree
                .operand()
                .map(|operand| operand.terms.clone())
                .unwrap_or_default(),
        },
    ))
}
