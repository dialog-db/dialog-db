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
//! [`Composite`]: a map pattern stands for a composite map (a record
//! among them) and a list pattern for a composite list.
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
//!
//! # Keys
//!
//! A map pattern names each key as the composite holds it: text for a
//! text key, an attribute for a record's field, or any other scalar.
//! Text and an attribute are two keys, as they are in a composite: the
//! pattern `{"title": ?t}` does not read the field `note/title`, and
//! `{note/title: ?t}` reads no text key.
//!
//! A composite's key may hold several values, kept as a set. An entry
//! of a map pattern names one value, so it matches a key that holds
//! exactly one, and a map whose named key holds several matches no
//! pattern that names it: built, the pattern holds the one value it
//! names, so taken apart it must have read one, or a rule would hold or
//! not by which of its premises ran first. A key the pattern does not
//! name goes to the rest with every value it holds.
//!
//! # Reading in place
//!
//! A value is taken apart through the composite's view: a named key by
//! [`Composite::get`] and an item by [`Composite::item`], each decoding
//! only what it returns, and a nested composite read out as a view of
//! the same buffer. A pattern that binds one field three levels down
//! decodes that field and nothing beside it. Only a rest or a run that
//! binds a variable decodes what it takes, to build the map or the
//! list it is bound to.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::Environment;
use crate::Parameters;
use crate::Premise;
use crate::attribute::Relation as Attribute;
use crate::concept::query::ConceptQuery;
use crate::constraint::{Constraint, Equality};
use crate::corpus::{Composite, CompositeError, CorpusRelation, Value};
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
    /// The named entries, in the order a composite map of them is
    /// written in: by the key's bytes. A key is a scalar, as a map's
    /// is (see the [module](self#keys)).
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

/// What a map pattern's key is written from: text, an attribute (as
/// [`the!`](crate::the) writes one, or the corpus's), or
/// any scalar [`Value`]. A text key and an attribute are two keys, as
/// they are in a composite.
pub trait Key {
    /// The key as the composite holds it.
    fn into_key(self) -> Value;
}

impl Key for Value {
    fn into_key(self) -> Value {
        self
    }
}

impl Key for String {
    fn into_key(self) -> Value {
        Value::String(self)
    }
}

impl Key for &str {
    fn into_key(self) -> Value {
        Value::String(self.to_string())
    }
}

impl Key for CorpusRelation {
    fn into_key(self) -> Value {
        Value::Symbol(self)
    }
}

impl Key for Attribute {
    fn into_key(self) -> Value {
        Value::from(self)
    }
}

/// Why a map pattern cannot be made of the entries it was given.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum PatternError {
    /// A key was named twice. An entry names one value, and a key the
    /// pattern names is matched only where it holds exactly one (see
    /// the [module](self#keys)), so a second entry under it could
    /// match nothing.
    #[error("a map pattern names a key once, and {0:?} was named twice")]
    RepeatedKey(Value),
    /// A key is not one a composite can hold: a composite, or NaN.
    #[error("{0}")]
    Key(#[from] CompositeError),
}

impl MapPattern {
    /// The pattern of a map that holds exactly `entries`. Refuses a key
    /// given twice ([`PatternError::RepeatedKey`]) and a key no
    /// composite holds ([`PatternError::Key`]).
    pub fn new<K: Key>(
        entries: impl IntoIterator<Item = (K, Pattern)>,
    ) -> Result<Self, PatternError> {
        let entries: Vec<(Value, Pattern)> = entries
            .into_iter()
            .map(|(key, pattern)| (key.into_key(), pattern))
            .collect();
        // A composite decides which keys are one (the natural 3 and the
        // float 3.0) and in what order they are written, so the keys
        // are put through one, each holding where it was given.
        let order = Composite::map(
            entries
                .iter()
                .enumerate()
                .map(|(index, (key, _))| (key.clone(), Value::UnsignedInt(index as u128))),
        )?;
        let mut slots: Vec<Option<Pattern>> = entries
            .into_iter()
            .map(|(_, pattern)| Some(pattern))
            .collect();
        let mut named: Vec<(Value, Pattern)> = Vec::with_capacity(slots.len());
        for (key, index) in order.entries() {
            if named.last().is_some_and(|(last, _)| *last == key) {
                return Err(PatternError::RepeatedKey(key));
            }
            let pattern = usize::try_from(index)
                .ok()
                .and_then(|index| slots.get_mut(index)?.take());
            if let Some(pattern) = pattern {
                named.push((key, pattern));
            }
        }
        Ok(Self {
            entries: named,
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
    pub fn map<K: Key, P: Into<Pattern>>(
        entries: impl IntoIterator<Item = (K, P)>,
    ) -> Result<Self, PatternError> {
        MapPattern::new(
            entries
                .into_iter()
                .map(|(key, pattern)| (key, pattern.into())),
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
    /// holds a key the pattern names; a run that is not a list).
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
                if let Some(rest) = map.rest.as_deref() {
                    let Value::Composite(others) = leaf(rest)? else {
                        return None;
                    };
                    // A rest holds the entries the pattern does not
                    // name: one that names a key again makes no map
                    // this pattern takes apart.
                    if !others.is_map()
                        || map
                            .entries
                            .iter()
                            .any(|(key, _)| !others.get(key).is_empty())
                    {
                        return None;
                    }
                    entries.extend(others.entries());
                }
                Composite::map(entries).ok()?
            }
            Pattern::List(list) => {
                let mut items = Vec::with_capacity(list.items.len());
                for pattern in &list.items {
                    items.push(pattern.build(row)?);
                }
                if let Some(run) = list.run.as_deref() {
                    let Value::Composite(others) = leaf(run)? else {
                        return None;
                    };
                    if !others.is_list() {
                        return None;
                    }
                    items.extend(others.items());
                }
                Composite::list(items).ok()?
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
    /// it in `row`. `false` when the value does not match: it is not of
    /// this shape, a constant differs, a variable is already bound to
    /// something else, or a key the pattern names holds several values
    /// (see the [module](self#keys)). `row` may then hold some of the
    /// bindings, so match into a copy.
    pub fn unify(&self, value: &Value, row: &mut Match) -> bool {
        match self {
            Pattern::Term(term) => unify_leaf(term, value, row),
            _ => self.take_apart(value, row),
        }
    }

    /// [`Self::unify`] for a pattern inside a map or a list, and for a
    /// map or a list itself. A leaf there matches a part only where it
    /// is the part: a number by the number it is, whatever type it was
    /// given under, since that is the value the pattern builds.
    ///
    /// A scan reads a value through a type: the natural 4, and a
    /// variable typed a natural, read a stored 4.3 as 4. Matched that
    /// way here, `{n: 4}` would match a record that holds 4.3 and build
    /// one that holds 4, and a rule would hold or not by which of its
    /// premises ran first: the one that binds the pattern's variables,
    /// after which the record is built and looked for, or the one that
    /// reads the record, after which it is taken apart.
    ///
    /// A part is read through the composite's view, so nothing beside
    /// it is decoded, and a nested composite is a view of the same
    /// buffer (see the [module](self#reading-in-place)).
    fn take_apart(&self, value: &Value, row: &mut Match) -> bool {
        let composite = match (self, value) {
            (Pattern::Term(Term::Constant(constant)), _) => return value.same_number(constant),
            (Pattern::Term(term), _) => {
                return unify_leaf(term, value, row)
                    && term
                        .name()
                        .and_then(|name| row.value_of(name))
                        .is_none_or(|bound| bound.same_number(value));
            }
            // An opaque record, or any scalar, is no shape.
            (_, Value::Composite(composite)) => composite,
            _ => return false,
        };
        match self {
            Pattern::Map(pattern) if composite.is_map() => {
                for (key, part) in &pattern.entries {
                    let values = composite.get(key);
                    let [value] = values.as_slice() else {
                        return false;
                    };
                    if !part.take_apart(value, row) {
                        return false;
                    }
                }
                match pattern.rest.as_deref() {
                    None => composite.len() == pattern.entries.len(),
                    Some(Term::Variable { name: None, .. }) => true,
                    Some(rest) => {
                        let others = composite.entries().filter(|(key, _)| {
                            !pattern.entries.iter().any(|(named, _)| named == key)
                        });
                        match Composite::map(others) {
                            Ok(others) => unify_leaf(rest, &Value::from(others), row),
                            Err(_) => false,
                        }
                    }
                }
            }
            Pattern::List(pattern) if composite.is_list() => {
                let named = pattern.items.len();
                let length = composite.len();
                if length < named || (pattern.run.is_none() && length != named) {
                    return false;
                }
                for (index, part) in pattern.items.iter().enumerate() {
                    let Some(item) = composite.item(index) else {
                        return false;
                    };
                    if !part.take_apart(&item, row) {
                        return false;
                    }
                }
                match pattern.run.as_deref() {
                    None | Some(Term::Variable { name: None, .. }) => true,
                    Some(run) => match Composite::list(composite.items().skip(named)) {
                        Ok(others) => unify_leaf(run, &Value::from(others), row),
                        Err(_) => false,
                    },
                }
            }
            _ => false,
        }
    }
}

/// Match a pattern that is a term against a value: a constant by the
/// value it reads as, as a scan matches one, and a variable by binding
/// it, which a variable bound to another value, or typed for another,
/// refuses. Inside a map or a list a leaf is matched more narrowly
/// (`Pattern::take_apart`).
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
                    match key {
                        Value::String(text) => write!(f, "{text:?}: {pattern}")?,
                        key => write!(f, "{key:?}: {pattern}")?,
                    }
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

/// A leaf is written as a [`Term`] is. A map or a list has no
/// wire form yet, and is refused.
impl serde::Serialize for Pattern {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error as _;
        match self {
            Pattern::Term(term) => term.serialize(serializer),
            composite => Err(S::Error::custom(format!(
                "the pattern {composite} has no wire form: only a variable or a constant is written"
            ))),
        }
    }
}

/// Reads a leaf, as a [`Term`] is read.
impl<'de> serde::Deserialize<'de> for Pattern {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Term::<Any>::deserialize(deserializer).map(Pattern::Term)
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

/// The first map or list of terms `premises` hold, as it is
/// written, when they hold one.
pub(crate) fn held<'a>(premises: impl IntoIterator<Item = &'a Premise>) -> Option<String> {
    premises.into_iter().find_map(|premise| {
        premise
            .parameters()
            .shapes()
            .next()
            .map(|(_, pattern)| pattern.to_string())
    })
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
        if !pattern.unify(value, &mut matched) {
            return None;
        }
    }
    Some(Ok(matched))
}

#[cfg(test)]
mod property_tests;
#[cfg(test)]
mod tests;
