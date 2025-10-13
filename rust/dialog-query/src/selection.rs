use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use crate::artifact::{Type, Value};
use async_stream::try_stream;
use dialog_common::ConditionalSend;
use std::collections::HashMap;

pub use futures_util::stream::{Stream, TryStream};
use std::pin::Pin;
use std::task;

use crate::{types::Scalar, Fact, InconsistencyError, QueryError, Term};

pub trait FlatMapper: ConditionalSend + 'static {
    fn map(&self, item: Match) -> impl Selection;
}

pub trait TryExpand: ConditionalSend + 'static {
    fn try_expand(&self, item: Match) -> Result<Vec<Match>, QueryError>;
}

pub trait Expand: ConditionalSend + 'static {
    fn expand(&self, item: Match) -> Vec<Match>;
}

/// Trait for streams of Answers (with fact provenance)
pub trait Answers: Stream<Item = Result<Answer, QueryError>> + 'static + ConditionalSend {
    /// Collect all answers into a Vec, propagating any errors
    #[allow(async_fn_in_trait)]
    fn try_vec(
        self,
    ) -> impl std::future::Future<Output = Result<Vec<Answer>, QueryError>> + ConditionalSend
    where
        Self: Sized,
    {
        async move { futures_util::TryStreamExt::try_collect(self).await }
    }

    fn flat_map<M: AnswersFlatMapper>(self, mapper: M) -> impl Answers
    where
        Self: Sized,
    {
        try_stream! {
            for await each in self {
                for await mapped in mapper.map(each?) {
                    yield mapped?;
                }
            }
        }
    }

    fn expand<M: AnswersExpand>(self, expander: M) -> impl Answers
    where
        Self: Sized,
    {
        try_stream! {
            for await each in self {
                for expanded in expander.expand(each?) {
                    yield expanded;
                }
            }
        }
    }

    fn try_expand<M: AnswersTryExpand>(self, expander: M) -> impl Answers
    where
        Self: Sized,
    {
        try_stream! {
            for await each in self {
                for expanded in expander.try_expand(each?)? {
                    yield expanded;
                }
            }
        }
    }
}

impl<S> Answers for S where S: Stream<Item = Result<Answer, QueryError>> + 'static + ConditionalSend {}

pub trait AnswersFlatMapper: ConditionalSend + 'static {
    fn map(&self, item: Answer) -> impl Answers;
}

pub trait AnswersTryExpand: ConditionalSend + 'static {
    fn try_expand(&self, item: Answer) -> Result<Vec<Answer>, QueryError>;
}

pub trait AnswersExpand: ConditionalSend + 'static {
    fn expand(&self, item: Answer) -> Vec<Answer>;
}

impl<F: Fn(Answer) -> Result<Vec<Answer>, QueryError> + ConditionalSend + 'static> AnswersTryExpand
    for F
{
    fn try_expand(&self, answer: Answer) -> Result<Vec<Answer>, QueryError> {
        self(answer)
    }
}

impl<F: Fn(Answer) -> Vec<Answer> + ConditionalSend + 'static> AnswersExpand for F {
    fn expand(&self, answer: Answer) -> Vec<Answer> {
        self(answer)
    }
}

impl<S: Answers, F: (Fn(Answer) -> S) + ConditionalSend + 'static> AnswersFlatMapper for F {
    fn map(&self, input: Answer) -> impl Answers {
        self(input)
    }
}

/// Deprecated: Use `Answers` trait instead which includes provenance tracking
#[deprecated(since = "0.2.0", note = "Use `Answers` trait instead for provenance tracking")]
pub trait Selection: Stream<Item = Result<Match, QueryError>> + 'static + ConditionalSend {
    /// Collect all matches into a Vec, propagating any errors
    #[allow(async_fn_in_trait)]
    fn try_vec(
        self,
    ) -> impl std::future::Future<Output = Result<Vec<Match>, QueryError>> + ConditionalSend
    where
        Self: Sized,
    {
        async move { futures_util::TryStreamExt::try_collect(self).await }
    }

    fn flat_map<M: FlatMapper>(self, mapper: M) -> impl Selection
    where
        Self: Sized,
    {
        try_stream! {
            for await each in self {
                for await mapped in mapper.map(each?) {
                    yield mapped?;
                }
            }
        }
    }

    fn expand<M: Expand>(self, expander: M) -> impl Selection
    where
        Self: Sized,
    {
        try_stream! {
            for await each in self {
                for expanded in expander.expand(each?) {
                    yield expanded;
                }
            }
        }
    }

    fn try_expand<M: TryExpand>(self, expander: M) -> impl Selection
    where
        Self: Sized,
    {
        try_stream! {
            for await each in self {
                for expanded in expander.try_expand(each?)? {
                    yield expanded;
                }
            }
        }
    }
}

impl<S> Selection for S where S: Stream<Item = Result<Match, QueryError>> + 'static + ConditionalSend
{}

impl<F: Fn(Match) -> Result<Vec<Match>, QueryError> + ConditionalSend + 'static> TryExpand for F {
    fn try_expand(&self, match_: Match) -> Result<Vec<Match>, QueryError> {
        self(match_)
    }
}

impl<F: Fn(Match) -> Vec<Match> + ConditionalSend + 'static> Expand for F {
    fn expand(&self, match_: Match) -> Vec<Match> {
        self(match_)
    }
}

impl<S: Selection, F: (Fn(Match) -> S) + ConditionalSend + 'static> FlatMapper for F {
    fn map(&self, input: Match) -> impl Selection {
        self(input)
    }
}

/// Deprecated: Use `Answer` instead which includes provenance tracking
#[deprecated(since = "0.2.0", note = "Use `Answer` instead for provenance tracking")]
#[derive(Clone, Debug, PartialEq)]
pub struct Match {
    pub variables: Arc<BTreeMap<String, Value>>,
}

impl Match {
    pub fn new() -> Self {
        Self {
            variables: Arc::new(BTreeMap::new()),
        }
    }

    // Type-safe methods using Term<T>
    pub fn get<T>(&self, term: &Term<T>) -> Result<T, InconsistencyError>
    where
        T: Scalar + std::convert::TryFrom<Value>,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(value) = self.variables.get(key) {
                        T::try_from(value.clone()).map_err(|_| {
                            // Create a proper TypeError for type conversion errors
                            InconsistencyError::TypeConversion(
                                crate::artifact::TypeError::TypeMismatch(
                                    T::TYPE.unwrap_or(Type::Bytes),
                                    value.data_type(),
                                ),
                            )
                        })
                    } else {
                        Err(InconsistencyError::UnboundVariableError(key.clone()))
                    }
                } else {
                    Err(InconsistencyError::UnboundVariableError("".to_string()))
                }
            }
            Term::Constant(constant) => Ok(constant.clone()),
        }
    }

    pub fn set<T>(&self, term: Term<T>, value: T) -> Result<Self, InconsistencyError>
    where
        T: crate::types::IntoType
            + Clone
            + Into<Value>
            + PartialEq
            + std::convert::TryFrom<Value>
            + std::fmt::Debug,
        InconsistencyError: From<<T as std::convert::TryFrom<Value>>::Error>,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    // Check if variable is already bound
                    if let Some(existing_value) = self.variables.get(&key) {
                        let existing_as_t_result = T::try_from(existing_value.clone());

                        match existing_as_t_result {
                            Ok(existing_as_t) => {
                                if existing_as_t == value {
                                    Ok(self.clone())
                                } else {
                                    Err(InconsistencyError::AssignmentError(format!(
                                    "Can not set {:?} to {:?} because it is already set to {:?}.",
                                    key,
                                    value.into(),
                                    existing_value
                                )))
                                }
                            }
                            Err(conversion_error) => {
                                // Type mismatch with existing value
                                Err(conversion_error.into())
                            }
                        }
                    } else {
                        // New binding
                        let mut variables = (*self.variables).clone();
                        variables.insert(key, value.into());
                        Ok(Self {
                            variables: Arc::new(variables),
                        })
                    }
                } else {
                    // TODO: We should still check the type here
                    Ok(self.clone())
                }
            }
            Term::Constant(constant) => {
                // For constants, we check if the value matches
                if constant == value {
                    Ok(self.clone())
                } else {
                    Err(InconsistencyError::AssignmentError(format!(
                        "Cannot set constant {:?} to different value {:?}",
                        constant, value
                    )))
                }
            }
        }
    }

    pub fn has<T>(&self, term: &Term<T>) -> bool
    where
        T: crate::types::IntoType + Clone,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    self.variables.contains_key(key)
                } else {
                    // We don't capture values for Any
                    false
                }
            }
            Term::Constant(_) => true, // Constants are always "bound"
        }
    }

    pub fn unify<T>(&self, term: Term<T>, value: Value) -> Result<Self, InconsistencyError>
    where
        T: crate::types::IntoType + Clone + Into<Value> + PartialEq<Value>,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    let mut variables = (*self.variables).clone();
                    variables.insert(key, value);

                    Ok(Self {
                        variables: Arc::new(variables),
                    })
                } else {
                    Ok(self.clone())
                }
            }
            Term::Constant(constant) => {
                let constant_value: Value = constant.into();
                if constant_value == value {
                    Ok(self.clone())
                } else {
                    Err(InconsistencyError::TypeMismatch {
                        expected: constant_value,
                        actual: value,
                    })
                }
            }
        }
    }

    pub fn unify_value<T>(&self, term: Term<T>, value: Value) -> Result<Self, InconsistencyError>
    where
        T: Scalar,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    let mut variables = (*self.variables).clone();
                    variables.insert(key, value);

                    Ok(Self {
                        variables: Arc::new(variables),
                    })
                } else {
                    Ok(self.clone())
                }
            }
            Term::Constant(constant) => {
                let constant_value = constant.as_value();
                if constant_value == value {
                    Ok(self.clone())
                } else {
                    Err(InconsistencyError::TypeMismatch {
                        expected: constant_value,
                        actual: value,
                    })
                }
            }
        }
    }

    /// Resolve a variable term into a constant term if this frame has a
    /// binding for it. Otherwise, return the original term.
    pub fn resolve<T: Scalar>(&self, term: &Term<T>) -> Term<T> {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(value) = self.variables.get(key) {
                        if let Ok(converted) = T::try_from(value.clone()) {
                            Term::Constant(converted)
                        } else {
                            // Conversion failed - return original term
                            term.clone()
                        }
                    } else {
                        term.clone()
                    }
                } else {
                    term.clone()
                }
            }
            Term::Constant(_) => term.clone(),
        }
    }

    pub fn resolve_value<T>(&self, term: &Term<T>) -> Result<Value, InconsistencyError>
    where
        T: Scalar,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(value) = self.variables.get(key) {
                        Ok(value.clone())
                    } else {
                        Err(InconsistencyError::UnboundVariableError(key.clone()))
                    }
                } else {
                    Err(InconsistencyError::UnboundVariableError("Any".to_string()))
                }
            }
            Term::Constant(constant) => Ok(constant.as_value()),
        }
    }

    /// Convert this Match into an Answer with no provenance information
    ///
    /// This creates an Answer where all variable bindings are treated as
    /// having unknown provenance. This is useful for migrating from Match-based
    /// code to Answer-based code.
    pub fn into_answer(self) -> Answer {
        use std::sync::OnceLock;
        static MATCH_CONVERSION_CELLS: OnceLock<crate::predicate::formula::Cells> = OnceLock::new();
        let cells = MATCH_CONVERSION_CELLS.get_or_init(|| crate::predicate::formula::Cells::new());

        let mut answer = Answer::new();

        // For each variable binding in the match, create a conclusion with no factors
        // This loses provenance information but maintains the bindings
        for (name, value) in self.variables.iter() {
            // Create a simple factor with the value but no provenance
            // We use a placeholder derived factor since we have no source facts
            let factor = Factor::Derived {
                value: value.clone(),
                from: HashMap::new(),
                formula: Arc::new(crate::application::formula::FormulaApplication {
                    name: "match_conversion",
                    cells,
                    parameters: crate::Parameters::new(),
                    cost: 0,
                    compute: |_| Ok(vec![]),
                }),
            };

            // Ignore errors - if conclude fails, we skip that binding
            let _ = answer.assign(&Term::var(name), &factor);
        }

        answer
    }
}

#[derive(Clone, Debug)]
pub enum Factor {
    The(Arc<Fact>),
    Of(Arc<Fact>),
    Is(Arc<Fact>),
    Cause(Arc<Fact>),
    /// Derived from a formula computation - tracks the input facts and formula used
    Derived {
        value: Value,
        /// The facts that were read to produce this derived value, keyed by parameter name
        from: HashMap<String, Factors>,
        /// The formula application that produced this value
        formula: Arc<crate::application::formula::FormulaApplication>,
    },
}

impl Factor {
    /// Get the underlying fact if this factor is directly from a fact (not derived)
    pub fn fact(&self) -> Option<&Fact> {
        match self {
            Factor::The(fact) => Some(fact.as_ref()),
            Factor::Of(fact) => Some(fact.as_ref()),
            Factor::Is(fact) => Some(fact.as_ref()),
            Factor::Cause(fact) => Some(fact.as_ref()),
            Factor::Derived { .. } => None,
        }
    }

    fn content(&self) -> Value {
        match self {
            Factor::The(fact) => Value::Symbol(fact.the().clone()),
            Factor::Of(fact) => Value::Entity(fact.of().clone()),
            Factor::Is(fact) => fact.is().clone(),
            Factor::Cause(fact) => Value::Bytes(fact.cause().clone().0.into()),
            Factor::Derived { value, .. } => value.clone(),
        }
    }

    /// Get all facts that contributed to this factor
    fn sources(&self) -> Vec<Arc<Fact>> {
        match self {
            Factor::The(fact) | Factor::Of(fact) | Factor::Is(fact) | Factor::Cause(fact) => {
                vec![Arc::clone(fact)]
            }
            Factor::Derived { from, .. } => {
                // Collect all facts from all input parameters
                from.values()
                    .flat_map(|factors| factors.evidence())
                    .filter_map(|factor| factor.fact().map(|f| Arc::new(f.clone())))
                    .collect()
            }
        }
    }
}

// Implement Hash and Eq based on Arc pointer identity for fact variants,
// and value/from/formula for Derived variant
impl std::hash::Hash for Factor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the discriminant first to distinguish between variants
        std::mem::discriminant(self).hash(state);

        match self {
            Factor::The(fact) | Factor::Of(fact) | Factor::Is(fact) | Factor::Cause(fact) => {
                // Hash based on the Arc pointer address, not the content
                let ptr = Arc::as_ptr(fact) as *const ();
                ptr.hash(state);
            }
            Factor::Derived { value, from, formula } => {
                // For derived factors, hash the value, input factors, and formula pointer
                value.hash(state);

                // Hash the from map (order-independent by using sorted keys)
                let mut keys: Vec<_> = from.keys().collect();
                keys.sort();
                for key in keys {
                    key.hash(state);
                    if let Some(factors) = from.get(key) {
                        // Hash the factors content
                        factors.content().hash(state);
                    }
                }

                // Hash the formula pointer
                let ptr = Arc::as_ptr(formula) as *const ();
                ptr.hash(state);
            }
        }
    }
}

impl PartialEq for Factor {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Factor::The(a), Factor::The(b)) => Arc::ptr_eq(a, b),
            (Factor::Of(a), Factor::Of(b)) => Arc::ptr_eq(a, b),
            (Factor::Is(a), Factor::Is(b)) => Arc::ptr_eq(a, b),
            (Factor::Cause(a), Factor::Cause(b)) => Arc::ptr_eq(a, b),
            (
                Factor::Derived {
                    value: v1,
                    from: f1,
                    formula: formula1,
                },
                Factor::Derived {
                    value: v2,
                    from: f2,
                    formula: formula2,
                },
            ) => {
                // Compare values, input factors, and formula pointer
                v1 == v2
                    && f1.len() == f2.len()
                    && f1.iter().all(|(k, factors1)| {
                        f2.get(k).map_or(false, |factors2| factors1 == factors2)
                    })
                    && Arc::ptr_eq(formula1, formula2)
            }
            _ => false,
        }
    }
}

impl Eq for Factor {}

/// Represents factors that support a variable binding.
///
/// A `Factors` instance contains one or more factors that all have the same content.
/// The first factor added becomes the primary source for the content value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Factors {
    primary: Factor,
    alternates: HashSet<Factor>,
}

impl Factors {
    /// Create a new Factors with just a primary factor
    pub fn new(primary: Factor) -> Self {
        Self {
            primary,
            alternates: HashSet::new(),
        }
    }

    /// Get the value from the factors
    pub fn content(&self) -> Value {
        self.primary.content()
    }

    /// Add a factor to this binding.
    /// Returns true if a new factor was added, false if it was already present.
    pub fn add(&mut self, factor: Factor) -> bool {
        if self.primary == factor {
            false
        } else {
            self.alternates.insert(factor)
        }
    }

    /// Iterate over all factors (primary and alternates) that support this binding.
    /// This provides evidence for where this value came from.
    pub fn evidence(&self) -> impl Iterator<Item = &Factor> + '_ {
        std::iter::once(&self.primary).chain(self.alternates.iter())
    }
}

impl From<&Factors> for Value {
    fn from(factors: &Factors) -> Self {
        factors.content()
    }
}

impl From<&Factors> for Fact {
    /// Extract the fact from factors.
    /// Uses the first factor's source fact (primary or first alternate).
    fn from(factors: &Factors) -> Self {
        if let Some(factor) = factors.evidence().next() {
            if let Some(fact) = factor.fact() {
                Fact::Assertion {
                    the: fact.the().clone(),
                    of: fact.of().clone(),
                    is: fact.is().clone(),
                    cause: fact.cause().clone(),
                }
            } else {
                // Derived factor - shouldn't happen for FactApplication
                // but provide a fallback
                panic!("Cannot convert Derived factor to Fact")
            }
        } else {
            panic!("Cannot convert empty Factors to Fact")
        }
    }
}

/// Describes answer to the query. It captures facts from which answer was
/// concluded and tracks which FactApplication produced which facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    /// Conclusions: named variable bindings where we've concluded values from facts.
    /// Maps variable names to their values with provenance (which facts support this binding).
    conclusions: HashMap<String, Factors>,
    /// Applications: maps FactApplication to the fact it matched.
    /// This allows us to realize facts even when the application had only constants/blanks.
    /// The facts stored here represent all facts that contributed to this answer.
    facts: HashMap<crate::application::fact::FactApplication, Arc<Fact>>,
}

impl Answer {
    /// Create new empty answer.
    pub fn new() -> Self {
        Self {
            conclusions: HashMap::new(),
            facts: HashMap::new(),
        }
    }

    /// Get all tracked facts from the applications.
    pub fn facts(&self) -> impl Iterator<Item = &Arc<Fact>> {
        self.facts.values()
    }

    /// Get all conclusions (named variable bindings).
    pub fn conclusions(&self) -> impl Iterator<Item = (&String, &Factors)> {
        self.conclusions.iter()
    }

    /// Record that a FactApplication matched a specific fact.
    /// Returns an error if the same application already mapped to a different fact,
    /// which would indicate an inconsistency (shouldn't happen in practice, but we check).
    pub fn record(
        &mut self,
        application: &crate::application::fact::FactApplication,
        fact: Arc<Fact>,
    ) -> Result<(), crate::error::InconsistencyError> {
        use crate::error::InconsistencyError;

        // Check if this application already has a different fact
        if let Some(existing_fact) = self.facts.get(application) {
            if !Arc::ptr_eq(existing_fact, &fact) {
                return Err(InconsistencyError::AssignmentError(format!(
                    "FactApplication {:?} already mapped to a different fact",
                    application
                )));
            }
            // Same fact - this is fine (idempotent)
        } else {
            // New mapping
            self.facts.insert(application.clone(), fact);
        }
        Ok(())
    }

    /// Realize a fact from a FactApplication.
    /// First tries to extract from named variable conclusions.
    /// Falls back to looking up the application in the recorded applications.
    pub fn realize(
        &self,
        application: &crate::application::fact::FactApplication,
    ) -> Result<Fact, crate::error::QueryError> {
        use crate::error::QueryError;
        use crate::term::Term;

        // Try to extract from a named variable conclusion first
        // This gives us the full fact with all its components

        // Try 'the' first
        if let Term::Variable { name: Some(_), .. } = application.the() {
            if let Some(factors) = self.resolve_factors(&application.the().as_unknown()) {
                return Ok(Fact::from(factors));
            }
        }

        // Try 'of' next
        if let Term::Variable { name: Some(_), .. } = application.of() {
            if let Some(factors) = self.resolve_factors(&application.of().as_unknown()) {
                return Ok(Fact::from(factors));
            }
        }

        // Try 'is' last
        if let Term::Variable { name: Some(_), .. } = application.is() {
            if let Some(factors) = self.resolve_factors(&application.is().as_unknown()) {
                return Ok(Fact::from(factors));
            }
        }

        // No named variables - look up by application
        if let Some(fact) = self.facts.get(application) {
            return Ok(Fact::Assertion {
                the: fact.the().clone(),
                of: fact.of().clone(),
                is: fact.is().clone(),
                cause: fact.cause().clone(),
            });
        }

        Err(QueryError::FactStore(
            "Could not realize fact from answer - application not found".to_string(),
        ))
    }

    /// Assign a term to a factor - just calls conclude() which handles all cases.
    /// This is provided for backward compatibility.
    pub fn assign(
        &mut self,
        term: &Term<Value>,
        factor: &Factor,
    ) -> Result<(), InconsistencyError> {
        self.conclude(term, factor)
    }

    /// Extends this answer by assigning multiple term-factor pairs.
    pub fn extend<I>(&mut self, assignments: I) -> Result<(), InconsistencyError>
    where
        I: IntoIterator<Item = (Term<Value>, Factor)>,
    {
        for (term, factor) in assignments {
            self.assign(&term, &factor)?;
        }
        Ok(())
    }

    /// Conclude a value for a named variable from a factor.
    /// This binds the variable to the value with provenance tracking.
    /// Ignores blank variables and constants (no-op for those).
    pub fn conclude(
        &mut self,
        term: &Term<Value>,
        factor: &Factor,
    ) -> Result<(), InconsistencyError> {
        match term {
            Term::Variable {
                name: Some(name), ..
            } => {
                if let Some(factors) = self.conclusions.get_mut(name) {
                    // Check if the new factor's content matches existing content
                    if factors.content() != factor.content() {
                        Err(InconsistencyError::AssignmentError(format!(
                            "Can not set {:?} to {:?} because it is already set to {:?}.",
                            name,
                            factor.content(),
                            factors.content()
                        )))
                    } else {
                        // Add the factor (idempotent if already present)
                        factors.add(factor.clone());
                        Ok(())
                    }
                } else {
                    self.conclusions
                        .insert(name.into(), Factors::new(factor.clone()));
                    Ok(())
                }
            }
            Term::Variable { name: None, .. } | Term::Constant(_) => {
                // Blank variables and constants are ignored - no-op
                Ok(())
            }
        }
    }

    /// Returns true if term can be read from this answer.
    pub fn contains<T: Scalar>(&self, term: &Term<T>) -> bool {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    self.conclusions.contains_key(key)
                } else {
                    // We don't capture values for Any
                    false
                }
            }
            Term::Constant(_) => true, // Constants are always "bound"
        }
    }

    /// Resolves factors that were assigned to the given term.
    pub fn resolve_factors<T: Scalar>(&self, term: &Term<T>) -> Option<&Factors> {
        match term {
            Term::Variable {
                name: Some(name), ..
            } => self.conclusions.get(name.into()),
            Term::Variable { name: None, .. } => None,
            Term::Constant(_) => None,
        }
    }

    /// Resolves a term to its typed value.
    /// Resolve a variable term into a constant term if this answer has a
    /// binding for it. Otherwise, return the original term.
    /// This is similar to Match::resolve but works with Answer bindings.
    pub fn resolve_term<T: Scalar>(&self, term: &Term<T>) -> Term<T> {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(factors) = self.conclusions.get(key) {
                        let value = factors.content();
                        if let Ok(converted) = T::try_from(value) {
                            Term::Constant(converted)
                        } else {
                            // Conversion failed - return original term
                            term.clone()
                        }
                    } else {
                        term.clone()
                    }
                } else {
                    term.clone()
                }
            }
            Term::Constant(_) => term.clone(),
        }
    }

    ///
    /// For variables, looks up the binding and extracts the typed value from the factor.
    /// For constants, returns the constant value directly.
    ///
    /// Returns an error if:
    /// - The variable is not bound
    /// - The value cannot be converted to type T
    pub fn resolve<T>(&self, term: &Term<T>) -> Result<T, InconsistencyError>
    where
        T: Scalar + std::convert::TryFrom<Value>,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(factors) = self.conclusions.get(key) {
                        let value = factors.content();
                        T::try_from(value.clone()).map_err(|_| {
                            // Create a proper TypeError for type conversion errors
                            InconsistencyError::TypeConversion(
                                crate::artifact::TypeError::TypeMismatch(
                                    T::TYPE.unwrap_or(Type::Bytes),
                                    value.data_type(),
                                ),
                            )
                        })
                    } else {
                        Err(InconsistencyError::UnboundVariableError(key.clone()))
                    }
                } else {
                    Err(InconsistencyError::UnboundVariableError("".to_string()))
                }
            }
            Term::Constant(constant) => Ok(constant.clone()),
        }
    }

    /// Convert this Answer into a Match, losing all provenance information
    ///
    /// This extracts just the variable bindings from the Answer, discarding
    /// all factor/provenance information. This is useful for backward compatibility
    /// with Match-based code.
    pub fn into_match(self) -> Match {
        let mut variables = BTreeMap::new();

        // Extract the value from each conclusion
        for (name, factors) in self.conclusions {
            variables.insert(name, factors.content());
        }

        Match {
            variables: Arc::new(variables),
        }
    }
}

/// An empty selection that yields no matches
///
/// This is useful as a placeholder for unimplemented rule evaluation
/// or for rules that have no valid matches.
#[derive(Debug, Clone)]
pub struct EmptySelection;

impl EmptySelection {
    pub fn new() -> Self {
        Self
    }
}

impl Default for EmptySelection {
    fn default() -> Self {
        Self::new()
    }
}

impl Stream for EmptySelection {
    type Item = Result<Match, QueryError>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        task::Poll::Ready(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Attribute, Entity};
    use crate::Term;
    use std::str::FromStr;

    #[test]
    fn test_type_safe_get_string() {
        let mut match_frame = Match::new();

        // Set a string value using the internal method
        match_frame = match_frame
            .set(Term::var("name"), "Alice".to_string())
            .unwrap();

        // Get it using the type-safe method
        let name_term = Term::var("name");
        let result = match_frame.get::<String>(&name_term);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Alice");
    }

    #[test]
    fn test_type_safe_get_type_mismatch() {
        let mut match_frame = Match::new();

        // Set a string value
        match_frame = match_frame
            .set(Term::<String>::var("age"), "not_a_number".to_string())
            .unwrap();

        // Try to get it as a u32 (should fail)
        let age_term = Term::var("age");
        let result = match_frame.get::<u32>(&age_term);

        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::TypeConversion(_) => {} // Expected
            _ => panic!("Expected TypeConversion error"),
        }
    }

    #[test]
    fn test_type_safe_set_string() {
        let match_frame = Match::new();

        let name_term = Term::<String>::var("name");
        let result = match_frame.set(name_term, "Bob".to_string());

        assert!(result.is_ok());
        let new_frame = result.unwrap();

        // Verify the value was set correctly
        let verify_term = Term::<String>::var("name");
        let stored_value: String = new_frame.get(&verify_term).unwrap();
        assert_eq!(stored_value, "Bob");
    }

    #[test]
    fn test_type_safe_set_term_integer() {
        let match_frame = Match::new();

        let age_term = Term::<u32>::var("age");
        let result = match_frame.set(age_term, 25u32);

        assert!(result.is_ok());
        let new_frame = result.unwrap();

        // Verify the value was set correctly
        let verify_term = Term::<u32>::var("age");
        let stored_value: u32 = new_frame.get(&verify_term).unwrap();
        assert_eq!(stored_value, 25u32);
    }

    #[test]
    fn test_type_safe_set_term_consistent_assignment() {
        let match_frame = Match::new();

        // Set initial value
        let name_term = Term::<String>::var("name");
        let frame1 = match_frame
            .set(name_term.clone(), "Charlie".to_string())
            .unwrap();

        // Set the same value again (should succeed)
        let result = frame1.set(name_term, "Charlie".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_safe_set_term_inconsistent_assignment() {
        let match_frame = Match::new();

        // Set initial value
        let name_term = Term::<String>::var("name");
        let frame1 = match_frame
            .set(name_term.clone(), "Diana".to_string())
            .unwrap();

        // Try to set a different value (should fail)
        let result = frame1.set(name_term, "Eve".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::AssignmentError(_) => {} // Expected
            _ => panic!("Expected AssignmentError"),
        }
    }

    #[test]
    fn test_type_safe_set_term_type_mismatch() {
        let mut match_frame = Match::new();

        // Set a string value using new API
        match_frame = match_frame
            .set(Term::<String>::var("value"), "text".to_string())
            .unwrap();

        // Try to set it as a u32 using type-safe method (should fail due to type mismatch)
        let value_term = Term::<u32>::var("value");
        let result = match_frame.set(value_term, 42u32);

        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::TypeConversion(_) => {} // Expected
            _ => panic!("Expected TypeConversion error"),
        }
    }

    #[test]
    fn test_type_safe_set_term_constant() {
        let match_frame = Match::new();

        // Set a constant term with matching value (should succeed)
        let constant_term = Term::Constant("fixed_value".to_string());
        let result = match_frame.set(constant_term, "fixed_value".to_string());
        assert!(result.is_ok());

        // Set a constant term with different value (should fail)
        let constant_term2 = Term::Constant("fixed_value".to_string());
        let result2 = match_frame.set(constant_term2, "different_value".to_string());
        assert!(result2.is_err());
        match result2.unwrap_err() {
            InconsistencyError::AssignmentError(_) => {} // Expected
            _ => panic!("Expected AssignmentError for constant mismatch"),
        }
    }

    #[test]
    fn test_type_safe_set_term_any() {
        let match_frame = Match::new();

        // Setting Any term should always succeed
        let any_term = Term::<String>::blank();
        let result = match_frame.set(any_term, "anything".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_safe_has_term() {
        let mut match_frame = Match::new();

        // Initially should not have the variable
        let name_term: Term<String> = Term::<String>::var("name");
        assert!(!match_frame.has(&name_term));

        // Set the variable
        match_frame = match_frame
            .set(Term::var("name"), "Frank".to_string())
            .unwrap();

        // Now should have the variable
        assert!(match_frame.has(&name_term));

        // Constants are always "bound"
        let constant_term = Term::Constant("value".to_string());
        assert!(match_frame.has(&constant_term));

        // Any is always "bound"
        let any_term = Term::<String>::blank();
        assert!(!match_frame.has(&any_term));
    }

    #[test]
    fn test_type_safe_entity_operations() {
        let match_frame = Match::new();
        let entity = Entity::new().unwrap();

        // Set entity using type-safe method
        let entity_term = Term::<Entity>::var("entity");
        let frame = match_frame
            .set(entity_term.clone(), entity.clone())
            .unwrap();

        // Get entity using type-safe method
        let result: Result<Entity, _> = frame.get(&entity_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), entity);
    }

    #[test]
    fn test_type_safe_attribute_operations() {
        let match_frame = Match::new();
        let attr = Attribute::from_str("user/name").unwrap();

        // Set attribute using type-safe method
        let attr_term = Term::<Attribute>::var("attr");
        let frame = match_frame.set(attr_term.clone(), attr.clone()).unwrap();

        // Get attribute using type-safe method
        let result: Result<Attribute, _> = frame.get(&attr_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), attr);
    }

    #[test]
    fn test_type_safe_mixed_types() {
        let match_frame = Match::new();

        // Set multiple types
        let name_term = Term::var("name");
        let age_term = Term::var("age");
        let active_term = Term::var("active");

        let frame1 = match_frame
            .set(name_term.clone(), "Grace".to_string())
            .unwrap();
        let frame2 = frame1.set(age_term.clone(), 30u32).unwrap();
        let frame3 = frame2.set(active_term.clone(), true).unwrap();

        // Get all values back with correct types
        let name_result: String = frame3.get(&name_term).unwrap();
        let age_result: u32 = frame3.get(&age_term).unwrap();
        let active_result: bool = frame3.get(&active_term).unwrap();

        assert_eq!(name_result, "Grace");
        assert_eq!(age_result, 30u32);
        assert_eq!(active_result, true);
    }

    #[test]
    fn test_backward_compatibility() {
        let mut match_frame = Match::new();

        // Use new API methods
        match_frame = match_frame
            .set(Term::var("name"), "Henry".to_string())
            .unwrap();
        let name_term = Term::var("name");
        assert!(match_frame.has(&name_term));
        let value: String = match_frame.get(&name_term).unwrap();
        assert_eq!(value, "Henry");

        // Mix with type-safe methods
        let typed_value: String = match_frame.get(&name_term).unwrap();
        assert_eq!(typed_value, "Henry");
    }

    // ============================================================================
    // Answer Tests
    // ============================================================================

    // Helper function to create a test fact for Answer tests
    // Since Fact requires a Cause (Blake3Hash), we create a simple helper
    fn create_test_fact(entity: Entity, attr: Attribute, value: Value) -> Fact {
        use crate::artifact::Cause;

        // Create a dummy cause for testing - Cause is a newtype around a 32-byte hash
        let cause = Cause([0u8; 32].into());

        Fact::Assertion {
            the: attr,
            of: entity,
            is: value,
            cause,
        }
    }

    #[test]
    fn test_answer_contains_bound_variable() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = Factor::Is(Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term = Term::<Value>::var("name");

        // Initially should not contain the variable
        assert!(!answer.contains(&name_term));

        // After assignment, should contain the variable
        answer.assign(&name_term, &factor).unwrap();
        assert!(answer.contains(&name_term));
    }

    #[test]
    fn test_answer_contains_unbound_variable() {
        let mut answer = Answer::new();
        let name_term = Term::<Value>::var("name");

        // Should not contain unbound variable
        assert!(!answer.contains(&name_term));
    }

    #[test]
    fn test_answer_contains_constant() {
        let mut answer = Answer::new();
        let constant_term = Term::Constant(Value::String("constant_value".to_string()));

        // Constants are always "bound"
        assert!(answer.contains(&constant_term));
    }

    #[test]
    fn test_answer_contains_blank_variable() {
        let mut answer = Answer::new();
        let blank_term = Term::<Value>::blank();

        // Blank variables (Any) are never "bound"
        assert!(!answer.contains(&blank_term));
    }

    #[test]
    fn test_answer_resolve_string() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = Factor::Is(Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term = Term::<String>::var("name");
        let name_term_value = Term::<Value>::var("name");

        // Assign the value
        answer.assign(&name_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer.resolve::<String>(&name_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Alice");
    }

    #[test]
    fn test_answer_resolve_u32() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/age").unwrap();
        let value = Value::UnsignedInt(25);
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = Factor::Is(Arc::clone(&fact));

        let mut answer = Answer::new();
        let age_term = Term::<u32>::var("age");
        let age_term_value = Term::<Value>::var("age");

        // Assign the value
        answer.assign(&age_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer.resolve::<u32>(&age_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 25);
    }

    #[test]
    fn test_answer_resolve_i32() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/score").unwrap();
        let value = Value::SignedInt(-10);
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = Factor::Is(Arc::clone(&fact));

        let mut answer = Answer::new();
        let score_term = Term::<i32>::var("score");
        let score_term_value = Term::<Value>::var("score");

        // Assign the value
        answer.assign(&score_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer.resolve::<i32>(&score_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -10);
    }

    #[test]
    fn test_answer_resolve_bool() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/active").unwrap();
        let value = Value::Boolean(true);
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = Factor::Is(Arc::clone(&fact));

        let mut answer = Answer::new();
        let active_term = Term::<bool>::var("active");
        let active_term_value = Term::<Value>::var("active");

        // Assign the value
        answer.assign(&active_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer.resolve::<bool>(&active_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_answer_resolve_entity() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/id").unwrap();
        let entity_value = Entity::new().unwrap();
        let value = Value::Entity(entity_value.clone());
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = Factor::Is(Arc::clone(&fact));

        let mut answer = Answer::new();
        let entity_term = Term::<Entity>::var("entity_id");
        let entity_term_value = Term::<Value>::var("entity_id");

        // Assign the value
        answer.assign(&entity_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer.resolve::<Entity>(&entity_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), entity_value);
    }

    #[test]
    fn test_answer_resolve_constant() {
        let mut answer = Answer::new();
        let constant_term = Term::Constant("constant_value".to_string());

        // Resolve constant directly
        let result = answer.resolve::<String>(&constant_term);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "constant_value");
    }

    #[test]
    fn test_answer_resolve_unbound_variable() {
        let mut answer = Answer::new();
        let name_term = Term::<String>::var("name");

        // Try to resolve unbound variable (should fail)
        let result = answer.resolve::<String>(&name_term);
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::UnboundVariableError(var) => {
                assert_eq!(var, "name");
            }
            _ => panic!("Expected UnboundVariableError"),
        }
    }

    #[test]
    fn test_answer_resolve_blank_variable() {
        let mut answer = Answer::new();
        let blank_term = Term::<String>::blank();

        // Try to resolve blank variable (should fail)
        let result = answer.resolve::<String>(&blank_term);
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::UnboundVariableError(_) => {} // Expected
            _ => panic!("Expected UnboundVariableError"),
        }
    }

    #[test]
    fn test_answer_resolve_type_mismatch() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = Factor::Is(Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term_value = Term::<Value>::var("name");

        // Assign a string value
        answer.assign(&name_term_value, &factor).unwrap();

        // Try to resolve it as a u32 (should fail)
        let age_term = Term::<u32>::var("name");
        let result = answer.resolve::<u32>(&age_term);
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::TypeConversion(_) => {} // Expected
            _ => panic!("Expected TypeConversion error"),
        }
    }

    #[test]
    fn test_answer_factors_evidence() {
        let entity1 = Entity::new().unwrap();
        let entity2 = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());

        // Create two different facts with the same value but different entities
        let fact1 = Arc::new(create_test_fact(
            entity1.clone(),
            attr.clone(),
            value.clone(),
        ));
        let fact2 = Arc::new(create_test_fact(
            entity2.clone(),
            attr.clone(),
            value.clone(),
        ));

        let factor1 = Factor::Is(Arc::clone(&fact1));
        let factor2 = Factor::Is(Arc::clone(&fact2));

        let mut answer = Answer::new();
        let name_term = Term::<Value>::var("name");

        // Assign the same value from two different facts
        answer.assign(&name_term, &factor1).unwrap();
        answer.assign(&name_term, &factor2).unwrap();

        // Get the factors and check evidence
        let factors = answer.resolve_factors(&name_term).unwrap();

        // The content should be the same
        assert_eq!(factors.content(), value);

        // Collect evidence
        let evidence: Vec<_> = factors.evidence().collect();

        // Should have both factors since they come from different facts
        // (even though they have the same value)
        assert_eq!(
            evidence.len(),
            2,
            "Should have 2 factors from different facts"
        );
        assert!(evidence.contains(&&factor1));
        assert!(evidence.contains(&&factor2));
    }

    #[test]
    fn test_answer_resolve_multiple_types() {
        let entity = Entity::new().unwrap();

        // Create multiple facts
        let name_attr = Attribute::from_str("user/name").unwrap();
        let name_value = Value::String("Bob".to_string());
        let name_fact = Arc::new(create_test_fact(
            entity.clone(),
            name_attr.clone(),
            name_value.clone(),
        ));
        let name_factor = Factor::Is(Arc::clone(&name_fact));

        let age_attr = Attribute::from_str("user/age").unwrap();
        let age_value = Value::UnsignedInt(30);
        let age_fact = Arc::new(create_test_fact(
            entity.clone(),
            age_attr.clone(),
            age_value.clone(),
        ));
        let age_factor = Factor::Is(Arc::clone(&age_fact));

        let active_attr = Attribute::from_str("user/active").unwrap();
        let active_value = Value::Boolean(true);
        let active_fact = Arc::new(create_test_fact(
            entity.clone(),
            active_attr.clone(),
            active_value.clone(),
        ));
        let active_factor = Factor::Is(Arc::clone(&active_fact));

        let mut answer = Answer::new();

        // Assign all values using chaining
        answer
            .assign(&Term::<Value>::var("name"), &name_factor)
            .unwrap();
        answer
            .assign(&Term::<Value>::var("age"), &age_factor)
            .unwrap();
        answer
            .assign(&Term::<Value>::var("active"), &active_factor)
            .unwrap();

        // Resolve all values with correct types
        let name_result = answer.resolve::<String>(&Term::var("name")).unwrap();
        let age_result = answer.resolve::<u32>(&Term::var("age")).unwrap();
        let active_result = answer.resolve::<bool>(&Term::var("active")).unwrap();

        assert_eq!(name_result, "Bob");
        assert_eq!(age_result, 30);
        assert_eq!(active_result, true);
    }

    #[test]
    fn test_answer_extend() {
        let entity = Entity::new().unwrap();

        // Create multiple facts
        let name_attr = Attribute::from_str("user/name").unwrap();
        let name_value = Value::String("Charlie".to_string());
        let name_fact = Arc::new(create_test_fact(
            entity.clone(),
            name_attr.clone(),
            name_value.clone(),
        ));
        let name_factor = Factor::Is(Arc::clone(&name_fact));

        let age_attr = Attribute::from_str("user/age").unwrap();
        let age_value = Value::UnsignedInt(35);
        let age_fact = Arc::new(create_test_fact(
            entity.clone(),
            age_attr.clone(),
            age_value.clone(),
        ));
        let age_factor = Factor::Is(Arc::clone(&age_fact));

        // Use extend to assign multiple values at once
        let assignments = vec![
            (Term::<Value>::var("name"), name_factor),
            (Term::<Value>::var("age"), age_factor),
        ];

        let mut answer = Answer::new();
        answer.extend(assignments).unwrap();

        // Verify all values were assigned
        let name_result = answer.resolve::<String>(&Term::var("name")).unwrap();
        let age_result = answer.resolve::<u32>(&Term::var("age")).unwrap();

        assert_eq!(name_result, "Charlie");
        assert_eq!(age_result, 35);
    }
}
