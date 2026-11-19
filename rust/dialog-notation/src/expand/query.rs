//! Queries, as the walker reads them: kept where they are defined, and
//! analyzed where they are asked.
//!
//! `(query HEAD condition…)` defines a query. Its head is a concept's,
//! and says under `is:` what it answers. Its body is conditions, and
//! `(is ?x shape)` for what a variable is. A condition is one of:
//!
//! - a read or a call of another query, which every answer needs;
//! - a read or a call with `(collect ?x)` for one of its terms, which
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

use crate::Error;
use crate::form::Kind;
use crate::select::{COLLECT, COUNT, PART, ROW, ROW_FIELD, SELECTED, collected, unknown_function};
use crate::syntax::{Parameters, Syntax};

use super::{Expander, apply, items};

/// Where in a condition its `(collect ?x)` stands.
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

/// Every `(collect …)` in a tree, wherever it stands.
fn collects_in(tree: &Syntax) -> Vec<&Syntax> {
    let mut found = Vec::new();
    fn walk<'a>(tree: &'a Syntax, found: &mut Vec<&'a Syntax>) {
        if tree.applies() == Some(COLLECT) {
            found.push(tree);
            return;
        }
        match tree {
            Syntax::Application(operator, operand) => {
                walk(operator, found);
                operand.items.iter().for_each(|item| walk(item, found));
                operand.terms.values().for_each(|term| walk(term, found));
            }
            Syntax::Map(entries) => entries.iter().for_each(|(key, value)| {
                walk(key, found);
                walk(value, found);
            }),
            Syntax::Tuple(items) => items.iter().for_each(|item| walk(item, found)),
            _ => {}
        }
    }
    walk(tree, &mut found);
    found
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
    /// question of a query analyzed, and the refusals of `collect`
    /// and of a query read where rows are. `None` for any other tree.
    pub(super) fn query_forms(&mut self, tree: &Syntax) -> Result<Option<Vec<Syntax>>, Error> {
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
                             tag: (collect ?tags)) …)`, and the query is asked"
                        )))
                    });
                }
                match items(tree) {
                    [call]
                        if call
                            .applies()
                            .is_some_and(|name| self.queries.contains_key(name)) =>
                    {
                        let call = call.clone();
                        self.about(&call, |this| this.selected(&call))
                            .map(|selected| Some(vec![selected]))
                    }
                    parts
                        if parts.iter().any(|part| {
                            part.applies()
                                .is_some_and(|name| self.queries.contains_key(name))
                        }) =>
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
                 template is a query collected, `(todo/chip ?t is: (collect ?chips))`"
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
                            "{found} stands in a {word}, and collect stands only in a query: \
                             {why}"
                        )))
                    });
                }
                if word == "concept"
                    && let Some(call) = items(tree).iter().skip(1).find(|line| {
                        line.applies()
                            .is_some_and(|name| self.queries.contains_key(name))
                    })
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
        let body = self.body(&name, body)?;
        self.safe(head, &body)?;
        // It answers, as far as walking it can tell: every read in it
        // expands, and every query it calls is known.
        let before = self.queries.insert(name.clone(), tree.clone());
        let checked = self.analyzed(&name, &mut Vec::new());
        if let Err(error) = checked {
            match before {
                Some(before) => self.queries.insert(name, before),
                None => self.queries.remove(&name),
            };
            return Err(error);
        }
        self.declared += 1;
        Ok(())
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
        let call = self.queries.contains_key(called) || called == name;
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
                "{} stands inside a term: collect is a term of a read, `(todo/tag ?t tag: \
                 (collect ?tags))`",
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
        held && !collect.call
            && matches!(&collect.place, Place::Term(label) if label == "is")
            && !self.concepts.contains(name)
            && !self.queries.contains_key(name)
            && name
                .split_once('/')
                .is_some_and(|(concept, _)| self.concepts.contains(concept))
    }

    /// `(select CALL)` of a query: the query and every query it reaches,
    /// analyzed.
    fn selected(&mut self, call: &Syntax) -> Result<Syntax, Error> {
        let name = call.applies().unwrap_or_default().to_string();
        // What the call gives by value is put into the query it asks,
        // so its reads are of that and not of every answer: one card is
        // not every card. A query whose answer is markup is laid out for
        // whatever entity it is rendered for, and is not.
        let written = self.queries.get(&name).cloned();
        if let Some(tree) = &written
            && !items(tree)
                .iter()
                .skip(1)
                .any(|condition| condition.applies() == Some("view"))
        {
            let given = bound_by(call, items(tree).first());
            if !given.is_empty() {
                self.queries
                    .insert(name.clone(), crate::pattern::replaced(tree, &given));
            }
        }
        let mut reached = Vec::new();
        let analyzed = self.analyzed(&name, &mut reached);
        if let Some(tree) = written {
            self.queries.insert(name.clone(), tree);
        }
        analyzed?;
        let mut parts = vec![call.clone()];
        parts.extend(reached.into_iter().map(|(_, analyzed)| analyzed));
        Ok(apply(SELECTED, parts))
    }

    /// The analyzed tree of the query `name`, added to `reached` with the
    /// analyzed trees of the queries it calls, each once. A query met
    /// again on the way to itself is refused.
    fn analyzed(&mut self, name: &str, reached: &mut Vec<(String, Syntax)>) -> Result<(), Error> {
        self.reach(name, reached, &mut Vec::new())
    }

    fn reach(
        &mut self,
        name: &str,
        reached: &mut Vec<(String, Syntax)>,
        path: &mut Vec<String>,
    ) -> Result<(), Error> {
        if path.iter().any(|on| on == name) {
            path.push(name.to_string());
            return Err(Error::Refused(format!(
                "the query {name} calls itself, through {}: a query that calls itself is not \
                 answered yet",
                path.join(" and ")
            )));
        }
        if reached.iter().any(|(done, _)| done == name) {
            return Ok(());
        }
        let tree = self
            .queries
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Unknown(name.to_string()))?;
        let [head, conditions @ ..] = items(&tree) else {
            return Err(asks(&tree));
        };
        let head = head.clone();
        let body = self.body(name, conditions)?;
        path.push(name.to_string());
        for called in body.calls.iter().chain(
            body.collects
                .iter()
                .filter(|collect| collect.call)
                .map(|collect| &collect.condition),
        ) {
            let callee = called.applies().unwrap_or_default().to_string();
            self.reach(&callee, reached, path)?;
        }
        path.pop();
        let mut parts = vec![head.clone()];
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
            parts.push(apply("call", vec![call.clone()]));
        }
        for (index, collect) in body.collects.iter().enumerate() {
            if self.missing(collect, &body) {
                let source = self.about(&collect.condition, |this| this.source(collect))?;
                parts.push(apply(
                    COLLECT,
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
                let head = self
                    .queries
                    .get(callee)
                    .and_then(|tree| items(tree).first().cloned());
                head.as_ref()
                    .and_then(|head| at(head, &collect.place))
                    .is_some_and(entry)
            } else {
                match (&collect.place, collect.condition.applies()) {
                    (Place::Term(label), Some(concept)) => self.entries.get(concept) == Some(label),
                    _ => false,
                }
            };
            let item = if keyed {
                Syntax::Map(vec![(
                    Syntax::Variable(format!("collect-{index}-key")),
                    Syntax::Variable(format!("collect-{index}-value")),
                )])
            } else {
                Syntax::Variable(format!("collect-{index}"))
            };
            let condition = replaced(&collect.condition, &collect.place, item.clone());
            let source = if collect.call {
                apply("call", vec![condition])
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
                COLLECT,
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
        reached.push((name.to_string(), apply("query", parts)));
        Ok(())
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
