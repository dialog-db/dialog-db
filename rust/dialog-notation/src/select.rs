//! Asking a query: rows in, a tree out.
//!
//! A query is the second layer. Rules and concepts give rows, may
//! recurse and never collect. A query reads them, nests, and collects:
//!
//! ```clj
//! (query (todo/card ?t is: ?card)
//!   (todo ?t title: ?title)
//!   (todo/tag ?t tag: (collect ?tags))
//!   (is ?card {:title ?title :tags ?tags}))
//!
//! (select (todo/card ?t is: ?card))
//! ```
//!
//! Dialog-DB cannot state a fold in a query, so a query is evaluated
//! here, over rows Dialog-DB gives. What it reads is ordinary queries:
//! the conditions every answer needs as one, and each read that
//! collects as one more. A [`Selection`] holds them, and the queries
//! each query it reaches calls. Whoever runs it runs every one of
//! [`Selection::queries`] on a branch, and [`Selection::answer`] puts
//! the rows together in memory: a read that collects is grouped by the
//! terms it shares with the rest, and a call to a query takes that
//! query's rows where its fields agree with the call.
//!
//! The analyzed tree a selection is compiled from says all of it:
//!
//! ```clj
//! (select.query CALL
//!   (query HEAD
//!     (required [?t ?title] RULE…)       ; every answer needs these
//!     (call CALL)                         ; and a row of this query
//!     (collect ?tags ?g (rows [?t ?g] RULE…))       ; the whole of a read
//!     (collect ?chips ?c (call (todo/chip ?t is: ?c))) ; of a call
//!     (is ?card SHAPE)))
//! ```
//!
//! A `RULE` is a kernel rule that reports a column for each variable
//! the vector names, in its order, under `c0`, `c1` and on.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use dialog_corpus::Value;
use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
use dialog_query::rule::DeductiveRule;
use dialog_query::{ConceptQuery, Fold, FoldConclusion, Term};

use crate::Error;
use crate::compile::{Query, Step, compile, written};
use crate::syntax::{Parameters, Syntax};

/// What a query's analyzed tree is applied as.
pub const SELECTED: &str = "select.query";

/// The relation a query's conditions are reported in, a column a
/// variable.
pub const ROW: &str = "query.row";

/// The one function of a collection there is yet.
pub const COUNT: &str = "count";

/// What collects the whole of a field.
pub const COLLECT: &str = "collect";

/// A row: what each variable was bound to.
pub type Row = BTreeMap<String, Syntax>;

/// Whether a tree is a set: a map whose every key is the empty label.
pub fn is_set(tree: &Syntax) -> bool {
    match tree {
        Syntax::Map(entries) => {
            !entries.is_empty()
                && entries
                    .iter()
                    .all(|(key, _)| matches!(key, Syntax::Reference(name) if name.is_empty()))
        }
        _ => false,
    }
}

/// The order of two values: by value where both are constants that
/// compare, and by how they are written otherwise.
pub fn order(a: &Syntax, b: &Syntax) -> Ordering {
    if let (Syntax::Constant(a), Syntax::Constant(b)) = (a, b)
        && let Some(ordering) = a.partial_cmp(b)
    {
        return ordering;
    }
    a.to_string().cmp(&b.to_string())
}

/// Whether `tree` is `(collect ?x)`, and the variable if so.
pub fn collected(tree: &Syntax) -> Option<Result<&str, Error>> {
    if tree.applies() != Some(COLLECT) {
        return None;
    }
    Some(match tree.operand() {
        Some(Parameters { items, terms }) if terms.is_empty() => match items.as_slice() {
            [Syntax::Variable(name)] => Ok(name.as_str()),
            _ => Err(Error::Refused(format!(
                "{tree}: collect takes one variable, what the whole of the field is bound to"
            ))),
        },
        _ => Err(Error::Refused(format!(
            "{tree}: collect takes one variable, what the whole of the field is bound to"
        ))),
    })
}

/// Rows from Dialog-DB: the queries that give them, and the variable
/// each column is.
#[derive(Clone, Debug)]
struct Rows {
    columns: Vec<String>,
    /// Indices into [`Selection::queries`]; their rows are one union.
    runs: Vec<usize>,
}

/// Where the whole of a field comes from.
#[derive(Clone, Debug)]
enum Source {
    /// A read, run once and grouped by the terms it shares.
    Rows(Rows),
    /// A query, called with each row.
    Call(Syntax),
}

/// A read that collects.
#[derive(Clone, Debug)]
struct Collect {
    /// What the whole is bound to.
    target: String,
    /// One of what is collected: a variable, or an entry `{?k ?v}`.
    item: Syntax,
    source: Source,
    /// The fold Dialog-DB gives the whole by, an index into
    /// [`Selection::folds`], with the terms it groups by.
    folded: Option<(usize, Vec<String>)>,
}

/// The prefix of a variable a fold's groups read that no column is.
const GROUPS: &str = "fold.groups.";

/// The prefix of a variable a fold's inputs read that no column is.
const INPUTS: &str = "fold.inputs.";

/// The field a fold counts the whole of `target` in.
fn counted(target: &str) -> String {
    format!("count.{target}")
}

/// A read that collects, as Dialog-DB folds it: the conditions every
/// answer needs as the groups, the read as the inputs, each group
/// folded to the distinct values of the read and how many there are.
/// A group the read matches nothing of is folded over inputs all
/// absent: the empty set, and 0.
#[derive(Clone, Debug)]
pub struct Folded {
    /// The rules the two queries read, laid over the branch for the
    /// fold alone.
    pub rules: Vec<DeductiveRule>,
    /// The fold.
    pub fold: Fold<ConceptQuery, ConceptQuery>,
}

impl Folded {
    /// A folded row, as each term it groups by and each fold, by its
    /// name, and the value it was bound to.
    pub fn values(&self, row: &FoldConclusion) -> BTreeMap<String, Value> {
        row.values()
            .iter()
            .filter_map(|(name, value)| Some((name.clone(), value.clone()?)))
            .collect()
    }
}

/// A query, as its analyzed tree says it.
#[derive(Clone, Debug)]
struct Defined {
    head: Syntax,
    required: Option<Rows>,
    calls: Vec<Syntax>,
    collects: Vec<Collect>,
    shapes: Vec<(String, Syntax)>,
}

/// A query asked: the call, the queries it reaches, and the Dialog-DB
/// queries their rows come from.
#[derive(Clone, Debug)]
pub struct Selection {
    /// What is asked: a query's head applied.
    pub call: Syntax,
    /// Every Dialog-DB query the answer needs. Each is run once, and its
    /// rows read with [`Query::values`].
    pub queries: Vec<Query>,
    /// Every read that collects that Dialog-DB folds. Each is run once,
    /// and its rows read with [`Folded::values`].
    pub folds: Vec<Folded>,
    defined: BTreeMap<String, Defined>,
}

fn malformed(tree: &Syntax) -> Error {
    Error::Refused(format!("{tree} is not a query as analysis gives one"))
}

fn items(tree: &Syntax) -> &[Syntax] {
    tree.operand().map_or(&[], |operand| &operand.items)
}

/// The variables a vector names.
fn names(tree: &Syntax) -> Result<Vec<String>, Error> {
    match tree {
        Syntax::Tuple(items) => items
            .iter()
            .map(|item| match item {
                Syntax::Variable(name) => Ok(name.clone()),
                other => Err(malformed(other)),
            })
            .collect(),
        other => Err(malformed(other)),
    }
}

impl Selection {
    /// The selection an analyzed `(select.query …)` tree says.
    pub fn compile(tree: &Syntax) -> Result<Selection, Error> {
        let [call, defined @ ..] = items(tree) else {
            return Err(malformed(tree));
        };
        let mut selection = Selection {
            call: call.clone(),
            queries: Vec::new(),
            folds: Vec::new(),
            defined: BTreeMap::new(),
        };
        for query in defined {
            let [head, parts @ ..] = items(query) else {
                return Err(malformed(query));
            };
            let name = head.applies().ok_or_else(|| malformed(head))?.to_string();
            let mut defined = Defined {
                head: head.clone(),
                required: None,
                calls: Vec::new(),
                collects: Vec::new(),
                shapes: Vec::new(),
            };
            for part in parts {
                match (part.applies(), items(part)) {
                    (Some("required"), [columns, rules @ ..]) => {
                        defined.required = Some(selection.rows(columns, rules)?);
                    }
                    (Some("call"), [call]) => defined.calls.push(call.clone()),
                    (Some("collect"), [Syntax::Variable(target), item, source]) => {
                        let source = match (source.applies(), items(source)) {
                            (Some("rows"), [columns, rules @ ..]) => {
                                Source::Rows(selection.rows(columns, rules)?)
                            }
                            (Some("call"), [call]) => Source::Call(call.clone()),
                            _ => return Err(malformed(source)),
                        };
                        defined.collects.push(Collect {
                            target: target.clone(),
                            item: item.clone(),
                            source,
                            folded: None,
                        });
                    }
                    (Some("is"), [Syntax::Variable(target), shape]) => {
                        defined.shapes.push((target.clone(), shape.clone()));
                    }
                    (Some("view"), _) => {
                        return Err(Error::Unsupported(format!(
                            "the answer of {name} is markup, and a query whose answer is not \
                             markup collects it or calls it"
                        )));
                    }
                    _ => return Err(malformed(part)),
                }
            }
            let required = defined.required.clone();
            for collect in &mut defined.collects {
                collect.folded = required
                    .as_ref()
                    .and_then(|required| selection.fold(required, collect));
            }
            selection.defined.insert(name, defined);
        }
        Ok(selection)
    }

    /// The fold of a read that collects, where Dialog-DB can give it:
    /// the read is one query, what it collects one variable, and the
    /// terms it shares with the rest are bound by the conditions every
    /// answer needs, which are one query too. The index of the fold
    /// and the terms it groups by.
    fn fold(&mut self, required: &Rows, collect: &Collect) -> Option<(usize, Vec<String>)> {
        let Source::Rows(rows) = &collect.source else {
            return None;
        };
        let Syntax::Variable(member) = &collect.item else {
            return None;
        };
        let ([groups], [inputs]) = (required.runs.as_slice(), rows.runs.as_slice()) else {
            return None;
        };
        let shared: Vec<String> = rows
            .columns
            .iter()
            .filter(|column| *column != member)
            .cloned()
            .collect();
        if !shared.iter().all(|term| required.columns.contains(term)) {
            return None;
        }
        let (groups, inputs) = (&self.queries[*groups], &self.queries[*inputs]);
        let reduce = Reduce::new(
            shared.clone(),
            vec![
                ReduceEntry::new(
                    collect.target.clone(),
                    Aggregator::Distinct,
                    Term::var(member.as_str()),
                ),
                ReduceEntry::new(
                    counted(&collect.target),
                    Aggregator::CountDistinct,
                    Term::var(member.as_str()),
                ),
            ],
        );
        let fold = Fold::new(
            groups.renamed(&required.columns, GROUPS)?,
            inputs.renamed(&rows.columns, INPUTS)?,
            reduce,
        );
        let rules = groups.rule.iter().chain(&inputs.rule).cloned().collect();
        self.folds.push(Folded { rules, fold });
        Some((self.folds.len() - 1, shared))
    }

    /// The rules of a part compiled, each a query of its own.
    fn rows(&mut self, columns: &Syntax, rules: &[Syntax]) -> Result<Rows, Error> {
        let columns = names(columns)?;
        let mut runs = Vec::new();
        for rule in rules {
            match compile(rule)?.as_slice() {
                [Step::Query(query)] => {
                    runs.push(self.queries.len());
                    self.queries.push((**query).clone());
                }
                other => {
                    return Err(Error::Refused(format!(
                        "{rule} compiles to {} steps, not one query",
                        other.len()
                    )));
                }
            }
        }
        Ok(Rows { columns, runs })
    }

    /// The answers, given the rows of each of [`Selection::queries`] and
    /// of each of [`Selection::folds`] in its order: the call, filled
    /// once an answer.
    pub fn answer(
        &self,
        results: &[Vec<BTreeMap<String, Value>>],
        folded: &[Vec<BTreeMap<String, Value>>],
    ) -> Result<Vec<Syntax>, Error> {
        if results.len() != self.queries.len() || folded.len() != self.folds.len() {
            return Err(Error::Refused(format!(
                "a selection of {} queries and {} folds is given the rows of {} and {}",
                self.queries.len(),
                self.folds.len(),
                results.len(),
                folded.len()
            )));
        }
        let mut evaluation = Evaluation {
            selection: self,
            results,
            folded,
            folds: BTreeMap::new(),
            done: BTreeMap::new(),
            read: BTreeMap::new(),
            grouped: BTreeMap::new(),
            indexed: BTreeMap::new(),
        };
        let mut answers: Vec<Syntax> = evaluation
            .called(&self.call, &Row::new())?
            .iter()
            .map(|row| fill(&self.call, row))
            .collect();
        answers.sort_by(order);
        answers.dedup();
        Ok(answers)
    }
}

/// What tells rows apart, and groups them: how each value is written.
fn keyed<'a>(values: impl Iterator<Item = Option<&'a Syntax>>) -> String {
    values
        .map(|value| value.map(ToString::to_string).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

/// One answering: the rows each query gives, worked out once, and the
/// indexes they are looked up by.
struct Evaluation<'a> {
    selection: &'a Selection,
    results: &'a [Vec<BTreeMap<String, Value>>],
    /// The rows of each fold.
    folded: &'a [Vec<BTreeMap<String, Value>>],
    /// The rows of each fold by the terms it groups by.
    folds: BTreeMap<usize, HashMap<String, usize>>,
    done: BTreeMap<String, Vec<Row>>,
    /// The rows of each part's queries, by its first query.
    read: BTreeMap<usize, Vec<Row>>,
    /// The rows of a collecting read grouped by the terms it shares, by
    /// its first query.
    grouped: BTreeMap<usize, HashMap<String, Vec<usize>>>,
    /// The answers of a query by the value of its entity.
    indexed: BTreeMap<String, HashMap<String, Vec<usize>>>,
}

impl Evaluation<'_> {
    /// The rows of a part's queries, each column as its variable, each
    /// once; worked out once a part.
    fn rows(&mut self, rows: &Rows) -> Vec<Row> {
        let Some(first) = rows.runs.first() else {
            return Vec::new();
        };
        if let Some(read) = self.read.get(first) {
            return read.clone();
        }
        let mut all: Vec<Row> = Vec::new();
        let mut seen = HashSet::new();
        for run in &rows.runs {
            for values in &self.results[*run] {
                let row: Row = rows
                    .columns
                    .iter()
                    .enumerate()
                    .filter_map(|(at, name)| {
                        values
                            .get(&format!("c{at}"))
                            .map(|value| (name.clone(), written(value)))
                    })
                    .collect();
                if seen.insert(format!("{row:?}")) {
                    all.push(row);
                }
            }
        }
        self.read.insert(*first, all.clone());
        all
    }

    /// Every row of the query named `name`: what each variable of its
    /// head and body is bound to.
    fn query(&mut self, name: &str) -> Result<(), Error> {
        if self.done.contains_key(name) {
            return Ok(());
        }
        let defined = self
            .selection
            .defined
            .get(name)
            .ok_or_else(|| Error::Unknown(name.to_string()))?;
        let mut rows = match &defined.required {
            Some(required) => self.rows(required),
            None => vec![Row::new()],
        };
        for call in &defined.calls {
            let mut joined = Vec::new();
            for row in &rows {
                joined.extend(self.called(call, row)?);
            }
            rows = joined;
        }
        for collect in &defined.collects {
            for row in &mut rows {
                if let Some((fold, shared)) = &collect.folded {
                    let (whole, count) = self.fold(*fold, shared, &collect.target, row);
                    row.insert(collect.target.clone(), whole);
                    row.insert(counted(&collect.target), count);
                    continue;
                }
                let whole = self.whole(collect, row)?;
                row.insert(collect.target.clone(), whole);
            }
        }
        for (target, shape) in &defined.shapes {
            for row in &mut rows {
                let built = build(shape, row)?;
                row.insert(target.clone(), built);
            }
        }
        let mut kept: Vec<Row> = Vec::new();
        let mut seen = HashSet::new();
        for row in rows {
            if seen.insert(format!("{row:?}")) {
                kept.push(row);
            }
        }
        self.done.insert(name.to_string(), kept);
        Ok(())
    }

    /// The answers of the query `name` whose entity is `entity`, by an
    /// index made once.
    fn answers_of(&mut self, name: &str, head: &Syntax, entity: &Syntax) -> Vec<usize> {
        let answers = self.done.get(name).map(Vec::as_slice).unwrap_or_default();
        let index = self.indexed.entry(name.to_string()).or_insert_with(|| {
            let mut index: HashMap<String, Vec<usize>> = HashMap::new();
            let own = head.operand().and_then(|operand| operand.items.first());
            for (at, answer) in answers.iter().enumerate() {
                let value = own.map(|own| fill(own, answer));
                index
                    .entry(keyed(std::iter::once(value.as_ref())))
                    .or_default()
                    .push(at);
            }
            index
        });
        index
            .get(&keyed(std::iter::once(Some(entity))))
            .cloned()
            .unwrap_or_default()
    }

    /// The rows `row` extends to where `call` holds: one for each answer
    /// of the query it applies whose head agrees with the call's terms.
    /// Where the call's entity is bound, only the answers of that entity
    /// are looked at.
    fn called(&mut self, call: &Syntax, row: &Row) -> Result<Vec<Row>, Error> {
        let name = call.applies().ok_or_else(|| malformed(call))?;
        let head = self
            .selection
            .defined
            .get(name)
            .map(|defined| defined.head.clone())
            .ok_or_else(|| Error::Unknown(name.to_string()))?;
        self.query(name)?;
        let (Some(wanted), Some(given)) = (call.operand(), head.operand()) else {
            return Err(malformed(call));
        };
        let bound = wanted.items.first().and_then(|term| match term {
            Syntax::Variable(variable) => row.get(variable).cloned(),
            Syntax::Constant(_) => Some(term.clone()),
            _ => None,
        });
        let candidates: Vec<usize> = match (&bound, given.items.first()) {
            (Some(entity), Some(Syntax::Variable(_))) => self.answers_of(name, &head, entity),
            _ => (0..self.done.get(name).map(Vec::len).unwrap_or_default()).collect(),
        };
        let answers = self.done.get(name).map(Vec::as_slice).unwrap_or_default();
        let mut extended = Vec::new();
        'answers: for at in candidates {
            let answer = &answers[at];
            let mut joined = row.clone();
            for (at, term) in wanted.items.iter().enumerate() {
                let Some(value) = given.items.get(at) else {
                    return Err(Error::Refused(format!(
                        "{call} names an entity, and the query {name} answers of none"
                    )));
                };
                if !unify(term, &fill(value, answer), &mut joined) {
                    continue 'answers;
                }
            }
            for (label, term) in &wanted.terms {
                let Some(value) = given.terms.get(label) else {
                    return Err(Error::Refused(format!(
                        "{call} says `{label}:`, which the query {name} has no field of"
                    )));
                };
                if !unify(term, &fill(value, answer), &mut joined) {
                    continue 'answers;
                }
            }
            extended.push(joined);
        }
        Ok(extended)
    }

    /// The whole of `target` Dialog-DB folded for `row`, a set, and how
    /// many it holds: the folded row whose groups agree with `row`. A
    /// row no fold gives is of a group the conditions did not give, and
    /// holds nothing.
    fn fold(
        &mut self,
        fold: usize,
        shared: &[String],
        target: &str,
        row: &Row,
    ) -> (Syntax, Syntax) {
        let folded = &self.folded[fold];
        let index = self.folds.entry(fold).or_insert_with(|| {
            folded
                .iter()
                .enumerate()
                .map(|(at, found)| {
                    let values: Vec<Syntax> = shared
                        .iter()
                        .map(|term| found.get(term).map(written).unwrap_or(Syntax::Blank))
                        .collect();
                    (keyed(values.iter().map(Some)), at)
                })
                .collect()
        });
        let found = index
            .get(&keyed(shared.iter().map(|term| row.get(term))))
            .map(|at| &folded[*at]);
        let members = match found.and_then(|found| found.get(target)).map(written) {
            Some(Syntax::Tuple(items)) => items,
            _ => Vec::new(),
        };
        let count = found
            .and_then(|found| found.get(&counted(target)).cloned())
            .unwrap_or(Value::UnsignedInt(0));
        let entries = members
            .into_iter()
            .map(|member| (Syntax::Reference(String::new()), member))
            .collect();
        (Syntax::Map(gathered(entries)), Syntax::Constant(count))
    }

    /// The whole of what a collecting read gives for `row`: a map of
    /// key to value for entries, a set otherwise. Nothing is `{}`. A
    /// read's rows are grouped once by the terms it shares with the rest.
    fn whole(&mut self, collect: &Collect, row: &Row) -> Result<Syntax, Error> {
        let matched: Vec<Row> = match &collect.source {
            Source::Rows(rows) => {
                let mut wanted = Vec::new();
                collect.item.variables(&mut wanted);
                let shared: Vec<String> = rows
                    .columns
                    .iter()
                    .filter(|column| !wanted.contains(column))
                    .cloned()
                    .collect();
                let first = rows.runs.first().copied().unwrap_or_default();
                if !self.read.contains_key(&first) {
                    self.rows(rows);
                }
                let read = self.read.get(&first).map(Vec::as_slice).unwrap_or_default();
                let groups = self.grouped.entry(first).or_insert_with(|| {
                    let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
                    for (at, found) in read.iter().enumerate() {
                        groups
                            .entry(keyed(shared.iter().map(|column| found.get(column))))
                            .or_default()
                            .push(at);
                    }
                    groups
                });
                groups
                    .get(&keyed(shared.iter().map(|column| row.get(column))))
                    .map(|found| found.iter().map(|at| read[*at].clone()).collect())
                    .unwrap_or_default()
            }
            Source::Call(call) => self.called(call, row)?,
        };
        let mut entries: Vec<(Syntax, Syntax)> = Vec::new();
        for found in &matched {
            let entry = match &collect.item {
                Syntax::Map(pair) => match pair.as_slice() {
                    [(key, value)] => (fill(key, found), fill(value, found)),
                    _ => return Err(malformed(&collect.item)),
                },
                member => (Syntax::Reference(String::new()), fill(member, found)),
            };
            entries.push(entry);
        }
        Ok(Syntax::Map(gathered(entries)))
    }
}

/// Entries put in order, each once: a map by its keys, and a set, whose
/// keys are all the empty label, by its members. Of two entries under
/// one key the first is kept.
fn gathered(mut entries: Vec<(Syntax, Syntax)>) -> Vec<(Syntax, Syntax)> {
    entries.sort_by(|(a, x), (b, y)| order(a, b).then_with(|| order(x, y)));
    let set = entries
        .iter()
        .all(|(key, _)| matches!(key, Syntax::Reference(name) if name.is_empty()));
    entries.dedup_by(|(b, y), (a, x)| if set { y == x } else { a == b });
    entries
}

/// Whether `pattern` holds of `value`, binding in `row` the variables
/// it holds that are not bound yet.
fn unify(pattern: &Syntax, value: &Syntax, row: &mut Row) -> bool {
    match pattern {
        Syntax::Blank => true,
        Syntax::Variable(name) => match row.get(name) {
            Some(bound) => bound == value,
            None => {
                row.insert(name.clone(), value.clone());
                true
            }
        },
        Syntax::Map(patterns) => {
            let Syntax::Map(entries) = value else {
                return false;
            };
            if patterns.len() != entries.len() {
                return false;
            }
            patterns
                .iter()
                .zip(entries)
                .all(|((key, wanted), (given, value))| {
                    unify(key, given, row) && unify(wanted, value, row)
                })
        }
        Syntax::Tuple(patterns) => match value {
            Syntax::Tuple(items) if items.len() == patterns.len() => patterns
                .iter()
                .zip(items)
                .all(|(wanted, item)| unify(wanted, item, row)),
            _ => false,
        },
        literal => literal == value,
    }
}

/// A tree with each variable a row binds put in by what it is bound to.
fn fill(tree: &Syntax, row: &Row) -> Syntax {
    match tree {
        Syntax::Variable(name) => row.get(name).cloned().unwrap_or_else(|| tree.clone()),
        Syntax::Application(operator, operand) => Syntax::Application(
            Box::new(fill(operator, row)),
            Parameters {
                items: operand.items.iter().map(|item| fill(item, row)).collect(),
                terms: operand
                    .terms
                    .iter()
                    .map(|(label, term)| (label.clone(), fill(term, row)))
                    .collect(),
            },
        ),
        Syntax::Map(entries) => Syntax::Map(
            entries
                .iter()
                .map(|(key, value)| (fill(key, row), fill(value, row)))
                .collect(),
        ),
        Syntax::Tuple(items) => Syntax::Tuple(items.iter().map(|item| fill(item, row)).collect()),
        other => other.clone(),
    }
}

/// What `(is ?x shape)` binds `?x` to in `row`: a record, a function of
/// what was collected, or a term.
fn build(shape: &Syntax, row: &Row) -> Result<Syntax, Error> {
    Ok(match shape {
        Syntax::Variable(name) => row
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Refused(format!("?{name} is bound by nothing the query says")))?,
        Syntax::Map(entries) => Syntax::Map(
            entries
                .iter()
                .map(|(key, value)| Ok((build(key, row)?, build(value, row)?)))
                .collect::<Result<_, Error>>()?,
        ),
        Syntax::Tuple(items) => Syntax::Tuple(
            items
                .iter()
                .map(|item| build(item, row))
                .collect::<Result<_, Error>>()?,
        ),
        Syntax::Application(..) if shape.applies() == Some(COUNT) => match items(shape) {
            // The whole of a read Dialog-DB folded, which counted it.
            [Syntax::Variable(whole)] if row.contains_key(&counted(whole)) => {
                row[&counted(whole)].clone()
            }
            [whole] => match build(whole, row)? {
                Syntax::Map(entries) => {
                    Syntax::Constant(Value::UnsignedInt((entries.len() as u64).into()))
                }
                other => {
                    return Err(Error::Refused(format!(
                        "{shape}: {other} is not the whole of a field, and count counts one"
                    )));
                }
            },
            _ => {
                return Err(Error::Refused(format!(
                    "{shape}: count takes one collection"
                )));
            }
        },
        Syntax::Application(..) => return Err(unknown_function(shape)),
        other => other.clone(),
    })
}

/// What a function of a collection other than `count` is refused with.
pub fn unknown_function(shape: &Syntax) -> Error {
    Error::Unsupported(format!(
        "{shape}: the one function of what was collected there is yet is `(count ?x)`"
    ))
}
