//! A pattern: what a parameter holds.
//!
//! ```text
//! Pattern = Variable | Constant              the leaves: what a Term is
//!         | Map   { key: Pattern ... , rest }
//!         | List  [ Pattern ... , run ]
//! ```
//!
//! A pattern stands where a value does. Where the value is bound, the
//! pattern takes it apart and binds the variables inside it
//! ([`Pattern::unify`]). Where every variable inside it is bound, the
//! pattern is the value ([`Pattern::build`]). The value is a
//! [`Composite`] kept as a record.
//!
//! One pattern says one thing both ways, so that a rule means the same
//! whichever of its premises runs first:
//!
//! - A map pattern with no rest is the map of exactly its entries, and
//!   matches no map that holds another. With a rest it matches a map
//!   that holds its entries and binds the others to the rest, as a map.
//!   A blank rest takes the others and binds nothing.
//! - A list pattern with no run is the list of exactly its items. With
//!   a run it matches a list that starts with its items and binds the
//!   ones after them to the run, as a list.
//!
//! A pattern that holds a blank cannot be built, since a blank is never
//! bound. It can only take a value apart.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::Environment;
use crate::Parameters;
use crate::Premise;
use crate::concept::query::ConceptQuery;
use crate::constraint::{Constraint, Equality};
use crate::corpus::{Composite, CompositeError, CompositeMap, Value};
use crate::error::EvaluationError;
use crate::proposition::Proposition;
use crate::selection::{Match, Selection};
use crate::term::Term;
use crate::try_stream;
use crate::types::Any;

/// What a parameter holds: a variable, a constant, or a map or a list
/// of patterns. See the [module](self).
///
/// A pattern is no larger than a [`Term<Any>`], so what holds a
/// pattern where it held a term (an [`Equality`]'s two sides) is the
/// size it was, and is moved and cloned at the cost it was.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Pattern {
    /// A leaf: a variable or a constant, with the descriptor a term has.
    Term(Term<Any>),
    /// A map of patterns by key.
    Map(MapPattern),
    /// A list of patterns.
    List(ListPattern),
}

const _: () = assert!(size_of::<Pattern>() == size_of::<Term<Any>>());

/// A map pattern: a pattern for the value under each key it names, and
/// what the entries it did not name are bound to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MapPattern {
    /// The named entries, in the order a map of them is written in.
    entries: Vec<(Value, Pattern)>,
    /// What the entries not named are bound to, as a map. `None` when
    /// the map holds no other entry. Boxed, as a run is, so that a
    /// pattern is no larger than the term its leaf is.
    rest: Option<Box<Term<Any>>>,
}

/// A list pattern: a pattern for each of its first items, and what the
/// items after them are bound to.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListPattern {
    /// The first items, in order.
    items: Vec<Pattern>,
    /// What the items after them are bound to, as a list. `None` when
    /// the list holds no other item.
    run: Option<Box<Term<Any>>>,
}

impl MapPattern {
    /// The pattern of a map that holds exactly `entries`. Refuses a key
    /// given twice, as a map does.
    pub fn new(
        entries: impl IntoIterator<Item = (Value, Pattern)>,
    ) -> Result<Self, CompositeError> {
        let entries: Vec<(Value, Pattern)> = entries.into_iter().collect();
        // A map decides which keys are one and in what order they are
        // written, so the entries are put through one.
        let order = CompositeMap::new(
            entries
                .iter()
                .enumerate()
                .map(|(index, (key, _))| (key.clone(), Value::UnsignedInt(index as u128))),
        )?;
        let mut slots: Vec<Option<Pattern>> = entries
            .into_iter()
            .map(|(_, pattern)| Some(pattern))
            .collect();
        let entries = order
            .iter()
            .filter_map(|(key, index)| {
                let index = usize::try_from(index.clone()).ok()?;
                Some((key.clone(), slots.get_mut(index)?.take()?))
            })
            .collect();
        Ok(Self {
            entries,
            rest: None,
        })
    }

    /// This pattern with the entries it does not name bound to `rest`,
    /// as a map. A blank takes them and binds nothing.
    pub fn with_rest(mut self, rest: Term<Any>) -> Self {
        self.rest = Some(Box::new(rest));
        self
    }

    /// The named entries, in the order a map of them is written in.
    pub fn entries(&self) -> &[(Value, Pattern)] {
        &self.entries
    }

    /// What the entries not named are bound to.
    pub fn rest(&self) -> Option<&Term<Any>> {
        self.rest.as_deref()
    }
}

impl ListPattern {
    /// The pattern of a list that holds exactly `items`.
    pub fn new(items: impl IntoIterator<Item = Pattern>) -> Self {
        Self {
            items: items.into_iter().collect(),
            run: None,
        }
    }

    /// This pattern with the items after its own bound to `run`, as a
    /// list. A blank takes them and binds nothing.
    pub fn with_run(mut self, run: Term<Any>) -> Self {
        self.run = Some(Box::new(run));
        self
    }

    /// The first items, in order.
    pub fn items(&self) -> &[Pattern] {
        &self.items
    }

    /// What the items after the first are bound to.
    pub fn run(&self) -> Option<&Term<Any>> {
        self.run.as_deref()
    }
}

impl Pattern {
    /// The pattern of a map that holds exactly `entries`. Refuses a key
    /// given twice. Use [`MapPattern::with_rest`] to take the others.
    pub fn map<K: Into<Value>, P: Into<Pattern>>(
        entries: impl IntoIterator<Item = (K, P)>,
    ) -> Result<Self, CompositeError> {
        MapPattern::new(
            entries
                .into_iter()
                .map(|(key, pattern)| (key.into(), pattern.into())),
        )
        .map(Pattern::Map)
    }

    /// The pattern of a list that holds exactly `items`. Use
    /// [`ListPattern::with_run`] to take the ones after them.
    pub fn list<P: Into<Pattern>>(items: impl IntoIterator<Item = P>) -> Self {
        Pattern::List(ListPattern::new(items.into_iter().map(Into::into)))
    }

    /// The leaf this pattern is, when it is one.
    pub fn as_term(&self) -> Option<&Term<Any>> {
        match self {
            Pattern::Term(term) => Some(term),
            _ => None,
        }
    }

    /// The variable's name, when this pattern is a named variable.
    pub fn name(&self) -> Option<&str> {
        self.as_term().and_then(Term::name)
    }

    /// An equality between this pattern and `other`: where one side is
    /// bound the other is inferred from it, a map or a list by
    /// taking the value apart.
    ///
    /// ```no_run
    /// # use dialog_query::{Pattern, Term, types::Any};
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // ?note is the map of ?title under "title" and ?body under "body".
    /// let premise = Pattern::map([
    ///     ("title".to_string(), Term::<Any>::var("title")),
    ///     ("body".to_string(), Term::<Any>::var("body")),
    /// ])?
    /// .is(Term::<Any>::var("note"));
    /// # let _ = premise;
    /// # Ok(())
    /// # }
    /// ```
    pub fn is(self, other: impl Into<Pattern>) -> Premise {
        Premise::from(Constraint::Equality(Equality::between(self, other)))
    }

    /// Whether this pattern is a map or a list.
    pub fn is_composite(&self) -> bool {
        !matches!(self, Pattern::Term(_))
    }

    /// Every leaf of this pattern, a rest and a run among them, in the
    /// order they are written in.
    pub fn terms(&self) -> Vec<&Term<Any>> {
        let mut terms = Vec::new();
        self.collect(&mut terms);
        terms
    }

    fn collect<'a>(&'a self, terms: &mut Vec<&'a Term<Any>>) {
        match self {
            Pattern::Term(term) => terms.push(term),
            Pattern::Map(map) => {
                for (_, pattern) in &map.entries {
                    pattern.collect(terms);
                }
                terms.extend(map.rest.as_deref());
            }
            Pattern::List(list) => {
                for pattern in &list.items {
                    pattern.collect(terms);
                }
                terms.extend(list.run.as_deref());
            }
        }
    }

    /// The names of the variables inside this pattern, each once, in
    /// the order they are first written in.
    pub fn variables(&self) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        for term in self.terms() {
            if let Some(name) = term.name()
                && !names.contains(&name)
            {
                names.push(name);
            }
        }
        names
    }

    /// Whether this pattern holds a blank: a place that matches any
    /// value and binds nothing, so the pattern cannot be built.
    pub fn has_blank(&self) -> bool {
        match self {
            Pattern::Term(term) => term.is_blank(),
            _ => self.terms().into_iter().any(Term::is_blank),
        }
    }

    /// Whether this pattern holds no variable, and so is one value.
    pub fn is_constant(&self) -> bool {
        match self {
            Pattern::Term(term) => term.is_constant(),
            _ => self.terms().into_iter().all(Term::is_constant),
        }
    }

    /// Whether every variable inside this pattern is bound in `env`, so
    /// the pattern is a value there. A pattern that holds a blank never
    /// is.
    pub fn is_bound(&self, env: &Environment) -> bool {
        match self {
            Pattern::Term(term) => term.is_bound(env),
            _ => self.terms().into_iter().all(|term| term.is_bound(env)),
        }
    }

    /// Adds the names of the variables inside this pattern to `env`.
    pub fn bind(&self, env: &mut Environment) {
        match self {
            Pattern::Term(term) => term.bind(env),
            _ => {
                for term in self.terms() {
                    term.bind(env);
                }
            }
        }
    }

    /// This pattern with every leaf, rest and run replaced by what `f`
    /// gives for it.
    pub fn map_terms(&self, f: &impl Fn(&Term<Any>) -> Term<Any>) -> Pattern {
        match self {
            Pattern::Term(term) => Pattern::Term(f(term)),
            Pattern::Map(map) => Pattern::Map(MapPattern {
                entries: map
                    .entries
                    .iter()
                    .map(|(key, pattern)| (key.clone(), pattern.map_terms(f)))
                    .collect(),
                rest: map.rest.as_deref().map(|rest| Box::new(f(rest))),
            }),
            Pattern::List(list) => Pattern::List(ListPattern {
                items: list.items.iter().map(|item| item.map_terms(f)).collect(),
                run: list.run.as_deref().map(|run| Box::new(f(run))),
            }),
        }
    }

    /// The value this pattern is under `row`: `None` while a variable
    /// inside it has no value there, where it holds a blank, and where
    /// its parts make no value (a rest that is not a map, or one that
    /// holds a key the pattern names).
    pub fn build(&self, row: &Match) -> Option<Value> {
        let leaf = |term: &Term<Any>| match term {
            Term::Constant(value) => Some(value.clone()),
            Term::Variable {
                name: Some(name), ..
            } => row.value_of(name).cloned(),
            Term::Variable { name: None, .. } => None,
        };
        let composite = match self {
            Pattern::Term(term) => return leaf(term),
            Pattern::Map(map) => {
                let mut entries = Vec::with_capacity(map.entries.len());
                for (key, pattern) in &map.entries {
                    entries.push((key.clone(), pattern.build(row)?));
                }
                let named = CompositeMap::new(entries).ok()?;
                match map.rest.as_deref() {
                    None => Composite::Map(named),
                    Some(rest) => {
                        let Ok(Some(Composite::Map(others))) = leaf(rest)?.composite() else {
                            return None;
                        };
                        Composite::Map(
                            named
                                .with(
                                    others
                                        .iter()
                                        .map(|(key, value)| (key.clone(), value.clone())),
                                )
                                .ok()?,
                        )
                    }
                }
            }
            Pattern::List(list) => {
                let mut items = Vec::with_capacity(list.items.len());
                for pattern in &list.items {
                    items.push(pattern.build(row)?);
                }
                if let Some(run) = list.run.as_deref() {
                    let Ok(Some(Composite::List(others))) = leaf(run)?.composite() else {
                        return None;
                    };
                    items.extend(others.iter().cloned());
                }
                Composite::list(items)
            }
        };
        Some(Value::from(composite))
    }

    /// The value this pattern is, when it holds no variable.
    pub fn as_value(&self) -> Option<Value> {
        self.is_constant()
            .then(|| self.build(&Match::new()))
            .flatten()
    }

    /// Take `value` apart by this pattern, binding the variables inside
    /// it in `row`. `Ok(false)` when the value does not match: it is not
    /// of this shape, a constant differs, or a variable is already
    /// bound to something else. `row` may then hold some of the
    /// bindings, so match into a copy.
    ///
    /// A record under a version this release does not read is refused,
    /// by its number: it may well match, and this release cannot tell.
    pub fn unify(&self, value: &Value, row: &mut Match) -> Result<bool, EvaluationError> {
        let composite = match self {
            Pattern::Term(term) => return Ok(unify_leaf(term, value, row)),
            _ => match value.composite() {
                Ok(Some(composite)) => composite,
                Ok(None) => return Ok(false),
                Err(CompositeError::UnknownVersion { version }) => {
                    return Err(EvaluationError::RecordVersion { version });
                }
                // Bytes that are not a composite's are a record of some
                // other kind, which no pattern of a shape matches.
                Err(_) => return Ok(false),
            },
        };
        match (self, &composite) {
            (Pattern::Map(pattern), Composite::Map(map)) => {
                for (key, part) in &pattern.entries {
                    let Some(value) = map.get(key) else {
                        return Ok(false);
                    };
                    if !part.unify(value, row)? {
                        return Ok(false);
                    }
                }
                match pattern.rest.as_deref() {
                    None => Ok(map.len() == pattern.entries.len()),
                    Some(Term::Variable { name: None, .. }) => Ok(true),
                    Some(rest) => {
                        let others = map.without(pattern.entries.iter().map(|(key, _)| key));
                        Ok(unify_leaf(rest, &Value::from(Composite::Map(others)), row))
                    }
                }
            }
            (Pattern::List(pattern), Composite::List(items)) => {
                let named = pattern.items.len();
                if items.len() < named || (pattern.run.is_none() && items.len() != named) {
                    return Ok(false);
                }
                for (part, item) in pattern.items.iter().zip(items.iter()) {
                    if !part.unify(item, row)? {
                        return Ok(false);
                    }
                }
                match pattern.run.as_deref() {
                    None | Some(Term::Variable { name: None, .. }) => Ok(true),
                    Some(run) => Ok(unify_leaf(
                        run,
                        &Value::from(Composite::list(items[named..].iter().cloned())),
                        row,
                    )),
                }
            }
            _ => Ok(false),
        }
    }
}

/// Match a leaf against a value: a constant by the value it reads as,
/// as a scan matches one, and a variable by binding it, which a
/// variable bound to another value, or typed for another, refuses.
fn unify_leaf(term: &Term<Any>, value: &Value, row: &mut Match) -> bool {
    match term {
        Term::Constant(constant) => value == constant || value.reads_as(constant),
        Term::Variable { name: None, .. } => true,
        variable => row.bind(variable, value.clone()).is_ok(),
    }
}

/// A leaf is the term it holds.
impl PartialEq<Term<Any>> for Pattern {
    fn eq(&self, other: &Term<Any>) -> bool {
        self.as_term() == Some(other)
    }
}

impl From<Term<Any>> for Pattern {
    fn from(term: Term<Any>) -> Self {
        Pattern::Term(term)
    }
}

impl From<&Term<Any>> for Pattern {
    fn from(term: &Term<Any>) -> Self {
        Pattern::Term(term.clone())
    }
}

impl From<Value> for Pattern {
    fn from(value: Value) -> Self {
        Pattern::Term(Term::Constant(value))
    }
}

impl From<MapPattern> for Pattern {
    fn from(map: MapPattern) -> Self {
        Pattern::Map(map)
    }
}

impl From<ListPattern> for Pattern {
    fn from(list: ListPattern) -> Self {
        Pattern::List(list)
    }
}

/// How a pattern is written: `?name` for a variable, `_` for a blank, a
/// constant as its value, `{key: pattern, ...rest}` for a map and `[item,
/// ...run]` for a list. Two patterns
/// written alike are one pattern but for the types their variables
/// declare.
impl fmt::Display for Pattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn leaf(term: &Term<Any>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match term {
                Term::Constant(value) => write!(f, "{value:?}"),
                Term::Variable {
                    name: Some(name), ..
                } => write!(f, "?{name}"),
                Term::Variable { name: None, .. } => write!(f, "_"),
            }
        }
        fn rest(first: bool, term: Option<&Term<Any>>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let Some(term) = term else {
                return Ok(());
            };
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "...")?;
            leaf(term, f)
        }
        match self {
            Pattern::Term(term) => leaf(term, f),
            Pattern::Map(map) => {
                write!(f, "{{")?;
                for (index, (key, pattern)) in map.entries.iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{key:?}: {pattern}")?;
                }
                rest(map.entries.is_empty(), map.rest.as_deref(), f)?;
                write!(f, "}}")
            }
            Pattern::List(list) => {
                write!(f, "[")?;
                for (index, pattern) in list.items.iter().enumerate() {
                    if index > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{pattern}")?;
                }
                rest(list.items.is_empty(), list.run.as_deref(), f)?;
                write!(f, "]")
            }
        }
    }
}

/// A pattern as it is written where a parameter stands, in a stored rule
/// body and wherever a rule or a query is printed.
///
/// A leaf is written as a [`Term`] is: a variable under `?`, a constant
/// as its value. A map or a list is a map under the word for its
/// shape, with what takes the entries or items it does not name beside
/// it:
///
/// ```text
/// { "map": [[key, pattern], …], "rest": term }
/// { "list": [pattern, …], "run": term }
/// ```
///
/// A map's entries are pairs, in the order a map keeps its keys in, as
/// a key is any value and a dag-cbor map takes text for a key. The
/// words are not the name of a type, which ends in `:`, nor `?`, so the
/// two never read as a constant or a variable.
impl serde::Serialize for Pattern {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Pattern::Term(term) => term.serialize(serializer),
            Pattern::Map(map) => {
                let rest = map.rest();
                let mut written =
                    serializer.serialize_map(Some(1 + usize::from(rest.is_some())))?;
                written.serialize_entry(MAP, map.entries())?;
                if let Some(rest) = rest {
                    written.serialize_entry(REST, rest)?;
                }
                written.end()
            }
            Pattern::List(list) => {
                let run = list.run();
                let mut written = serializer.serialize_map(Some(1 + usize::from(run.is_some())))?;
                written.serialize_entry(LIST, list.items())?;
                if let Some(run) = run {
                    written.serialize_entry(RUN, run)?;
                }
                written.end()
            }
        }
    }
}

const MAP: &str = "map";
const REST: &str = "rest";
const LIST: &str = "list";
const RUN: &str = "run";

/// Reads a pattern as it is written: a map under `map` or `list` as
/// that shape, and anything else as a [`Term`] is read.
impl<'de> serde::Deserialize<'de> for Pattern {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        // Read as a term is, through a value that can be looked into
        // before it is known which of the two it is.
        let raw = serde_json::Value::deserialize(deserializer)?;
        Pattern::read(raw).map_err(D::Error::custom)
    }
}

impl Pattern {
    fn read(raw: serde_json::Value) -> Result<Pattern, String> {
        let refused = |error: serde_json::Error| error.to_string();
        let term = |raw: serde_json::Value| -> Result<Term<Any>, String> {
            serde_json::from_value(raw).map_err(refused)
        };
        let each = |raw: serde_json::Value| -> Result<Vec<Pattern>, String> {
            match raw {
                serde_json::Value::Array(items) => items.into_iter().map(Pattern::read).collect(),
                other => Err(format!("the items of a pattern are a list, not {other}")),
            }
        };
        let serde_json::Value::Object(mut shape) = raw else {
            return term(raw).map(Pattern::Term);
        };
        if let Some(entries) = shape.remove(MAP) {
            let serde_json::Value::Array(entries) = entries else {
                return Err(format!(
                    "the entries of a map pattern are pairs, not {entries}"
                ));
            };
            let entries = entries
                .into_iter()
                .map(|entry| match entry {
                    serde_json::Value::Array(pair) if pair.len() == 2 => {
                        let mut pair = pair.into_iter();
                        let key = pair.next().unwrap_or_default();
                        let part = pair.next().unwrap_or_default();
                        Ok((
                            serde_json::from_value::<Value>(key).map_err(refused)?,
                            Pattern::read(part)?,
                        ))
                    }
                    other => Err(format!("an entry of a map pattern is a pair, not {other}")),
                })
                .collect::<Result<Vec<_>, String>>()?;
            let map = MapPattern::new(entries).map_err(|error| error.to_string())?;
            return Ok(match shape.remove(REST) {
                Some(rest) => map.with_rest(term(rest)?),
                None => map,
            }
            .into());
        }
        if let Some(items) = shape.remove(LIST) {
            let list = ListPattern::new(each(items)?);
            return Ok(match shape.remove(RUN) {
                Some(run) => list.with_run(term(run)?),
                None => list,
            }
            .into());
        }
        term(serde_json::Value::Object(shape)).map(Pattern::Term)
    }
}

static WHOLE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A composite pattern as a premise is run with it: the variable that
/// stands for the whole value in the premise's own slot, and the
/// pattern that value is built from or taken apart by.
///
/// A premise reads a variable or a constant in each slot. So a premise
/// one of whose parameters is a map or a list runs with a
/// variable there, the whole, and the pattern is applied around it:
/// [`build`] binds the whole from the pattern where the pattern's
/// variables are bound before the premise runs, and `take_apart`
/// matches what the premise bound the whole to against the pattern
/// after it.
#[derive(Debug, Clone, PartialEq)]
pub struct Shape {
    /// The variable standing for the whole value.
    pub whole: Term<Any>,
    /// The pattern of that value.
    pub pattern: Arc<Pattern>,
}

impl Shape {
    /// The variable that stands for the whole of `pattern` wherever it
    /// is written. It is named by how the pattern is written, so two
    /// premises holding one pattern share it: with its variables bound
    /// it is one value in both.
    pub fn whole_of(pattern: &Pattern) -> Term<Any> {
        Term::var(format!("{pattern}"))
    }

    /// The shape of `pattern` for one run of a premise. A pattern that
    /// holds a blank is not one value, so each run of each premise
    /// holding it gets a whole of its own, where two premises would
    /// otherwise be joined on it.
    pub fn of(pattern: Arc<Pattern>) -> Self {
        Self::numbered(pattern, || WHOLE_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// The shape of `pattern`, its whole told apart by what `number`
    /// gives where the pattern holds a blank and so is not one value.
    pub(crate) fn numbered(pattern: Arc<Pattern>, number: impl FnOnce() -> u64) -> Self {
        let whole = if pattern.has_blank() {
            Term::var(format!("{pattern}#{}", number()))
        } else {
            Self::whole_of(&pattern)
        };
        Self { whole, pattern }
    }
}

/// Whether `proposition` holds a map or a list of terms. Only
/// a concept application and an equality can: every other premise is a
/// struct of terms. Asked of every premise a rule is spelled from, so
/// it builds no parameters to answer.
pub(crate) fn holds_shape(proposition: &Proposition) -> bool {
    match proposition {
        Proposition::Concept(query) => query.terms.has_shapes(),
        Proposition::Constraint(Constraint::Equality(equality)) => {
            equality.this.is_composite() || equality.is.is_composite()
        }
        _ => false,
    }
}

/// The premises of a conjunction with every map and list a
/// positive concept premise holds moved out of it: the premise is left
/// reading the variable that stands for the whole value, and an
/// equality between that variable and the pattern follows it.
///
/// ```text
///  note/payload { this: ?note, is: {title: ?title, ...?rest} }
///
///  note/payload { this: ?note, is: ?whole }
///  ?whole == {title: ?title, ...?rest}
/// ```
///
/// The two are planned like any premises. Where the pattern's variables
/// are bound first the equality builds the whole and the concept is
/// read by it. Where the concept runs first the equality takes what it
/// read apart and drops a row that does not match. Everything that
/// evaluates a rule's body then meets terms only, and one premise that
/// knows patterns.
///
/// A negated premise is left whole: it asks whether any fact matches,
/// which two premises cannot say, and its concept takes its own
/// parameters apart when it runs (see
/// [`ConceptQuery`](crate::ConceptQuery)).
pub(crate) fn lower(premises: Vec<Premise>) -> Vec<Premise> {
    if !premises.iter().any(|premise| {
        matches!(premise, Premise::Assert(Proposition::Concept(query)) if query.terms.has_shapes())
    }) {
        return premises;
    }
    let mut number = 0u64;
    let mut lowered = Vec::with_capacity(premises.len() + 1);
    for premise in premises {
        match premise {
            Premise::Assert(Proposition::Concept(query)) if query.terms.has_shapes() => {
                let (terms, shapes) = query.terms.lowered_by(|| {
                    number += 1;
                    number
                });
                lowered.push(Premise::Assert(Proposition::Concept(ConceptQuery {
                    predicate: query.predicate,
                    terms,
                })));
                for shape in shapes {
                    lowered.push(Premise::from(Constraint::Equality(Equality::between(
                        shape.whole,
                        shape.pattern.as_ref().clone(),
                    ))));
                }
            }
            other => lowered.push(other),
        }
    }
    lowered
}

/// Before a premise runs: bind each whole from its pattern, in every
/// row where the pattern is a value and the whole is not yet bound. A
/// row where it is not a value passes as it is, and the premise binds
/// the whole.
pub fn build<'a, M: Selection + 'a>(shapes: Arc<[Shape]>, selection: M) -> impl Selection + 'a {
    try_stream! {
        for await row in selection {
            let mut row: Match = row?;
            for shape in shapes.iter() {
                if row.contains(&shape.whole) {
                    continue;
                }
                if let Some(value) = shape.pattern.build(&row) {
                    row.bind(&shape.whole, value)?;
                }
            }
            yield row;
        }
    }
}

/// After a premise ran with `terms`: match what the whole of each map
/// and list they hold is bound to in `row` against its pattern,
/// binding the variables inside it. `None` for a row that does not
/// match, which is dropped. A whole bound to nothing (an optional field
/// with no value) matches no pattern. A whole the premise left unbound
/// leaves the pattern's variables unbound, as it would a variable in
/// its place. A row that is an error passes as it is.
pub(crate) fn take_apart(
    terms: &Parameters,
    row: Result<Match, EvaluationError>,
) -> Option<Result<Match, EvaluationError>> {
    let row = match row {
        Ok(row) => row,
        Err(error) => return Some(Err(error)),
    };
    let mut matched = row.clone();
    for (name, pattern) in terms.shapes() {
        let whole = terms.get(name).and_then(Term::name).unwrap_or_default();
        let Some(binding) = row.get(whole) else {
            continue;
        };
        let value = binding.as_value()?;
        match pattern.unify(value, &mut matched) {
            Ok(true) => {}
            Ok(false) => return None,
            Err(error) => return Some(Err(error)),
        }
    }
    Some(Ok(matched))
}

#[cfg(test)]
mod tests;
