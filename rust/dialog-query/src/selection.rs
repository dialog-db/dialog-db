use std::{collections::HashSet, sync::Arc};

use crate::{
    application::FactApplication,
    artifact::{Type, Value},
};
use async_stream::try_stream;
use dialog_common::ConditionalSend;
use std::collections::HashMap;

/// Re-exported stream traits for working with answer streams.
pub use futures_util::stream::{Stream, TryStream};

use crate::{Fact, InconsistencyError, QueryError, Term, types::Scalar};

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

    /// Apply a flat-mapping operation over each answer, producing a new stream of answers.
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

    /// Expand each answer into zero or more answers using an infallible expander.
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

    /// Expand each answer into zero or more answers using a fallible expander.
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

/// Maps an answer into a stream of answers (flat-map operation).
pub trait AnswersFlatMapper: ConditionalSend + 'static {
    /// Produce a stream of answers from a single input answer.
    fn map(&self, item: Answer) -> impl Answers;
}

/// Expands an answer into multiple answers, potentially returning an error.
pub trait AnswersTryExpand: ConditionalSend + 'static {
    /// Attempt to expand a single answer into zero or more answers.
    fn try_expand(&self, item: Answer) -> Result<Vec<Answer>, QueryError>;
}

/// Expands an answer into multiple answers infallibly.
pub trait AnswersExpand: ConditionalSend + 'static {
    /// Expand a single answer into zero or more answers.
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

/// Identifies which component of a fact a factor was selected from.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Selector {
    /// The attribute component of a fact.
    The,
    /// The entity component of a fact.
    Of,
    /// The value component of a fact.
    Is,
    /// The cause (provenance hash) component of a fact.
    Cause,
}

/// Represents the origin of a value binding: selected from a fact, derived
/// from a formula, or provided as a parameter.
#[derive(Clone, Debug)]
pub enum Factor {
    /// A value selected directly from a matched fact.
    Selected {
        /// Which fact component this value came from.
        selector: Selector,
        /// The fact application that matched this fact.
        application: Arc<crate::application::FactApplication>,
        /// The matched fact itself.
        fact: Arc<Fact>,
    },
    /// Derived from a formula computation - tracks the input facts and formula used.
    Derived {
        /// The computed value.
        value: Value,
        /// The facts that were read to produce this derived value, keyed by parameter name.
        from: HashMap<String, Factors>,
        /// The formula application that produced this value.
        formula: Arc<crate::application::formula::FormulaApplication>,
    },
    /// A value provided externally as a query parameter.
    Parameter {
        /// The parameter value.
        value: Value,
    },
}

impl Factor {
    /// Get the underlying fact if this factor is directly from a fact (not derived)
    pub fn fact(&self) -> Option<&Fact> {
        match self {
            Factor::Selected { fact, .. } => Some(fact.as_ref()),
            Factor::Derived { .. } => None,
            Factor::Parameter { .. } => None,
        }
    }

    fn content(&self) -> Value {
        match self {
            Factor::Selected { selector, fact, .. } => match selector {
                Selector::The => Value::Symbol(fact.the().clone()),
                Selector::Of => Value::Entity(fact.of().clone()),
                Selector::Is => fact.is().clone(),
                Selector::Cause => Value::Bytes(fact.cause().clone().0.into()),
            },
            Factor::Derived { value, .. } => value.clone(),
            Factor::Parameter { value, .. } => value.clone(),
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
            Factor::Selected {
                selector,
                fact,
                application,
            } => {
                selector.hash(state);
                // Hash based on the Arc pointer address, not the content
                let fact_ptr = Arc::as_ptr(fact) as *const ();
                fact_ptr.hash(state);
                let app_ptr = Arc::as_ptr(application) as *const ();
                app_ptr.hash(state);
            }
            Factor::Parameter { value } => {
                value.hash(state);
            }
            Factor::Derived {
                value,
                from,
                formula,
            } => {
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
            (
                Factor::Selected {
                    selector: s1,
                    fact: a,
                    application: app1,
                },
                Factor::Selected {
                    selector: s2,
                    fact: b,
                    application: app2,
                },
            ) => s1 == s2 && Arc::ptr_eq(a, b) && Arc::ptr_eq(app1, app2),
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
                    && f1
                        .iter()
                        .all(|(k, factors1)| f2.get(k).is_some_and(|factors2| factors1 == factors2))
                    && Arc::ptr_eq(formula1, formula2)
            }
            (Factor::Parameter { value: v1 }, Factor::Parameter { value: v2 }) => v1 == v2,
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
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Answer {
    /// Conclusions: named variable bindings where we've concluded values from facts.
    /// Maps variable names to their values with provenance (which facts support this binding).
    conclusions: HashMap<String, Factors>,
    /// Applications: maps FactApplication to the fact it matched.
    /// This allows us to realize facts even when the application had only constants/blanks.
    /// The facts stored here represent all facts that contributed to this answer.
    facts: HashMap<FactApplication, Arc<Fact>>,
}

/// Evidence describing how a value was obtained, used when merging into an answer.
pub enum Evidence<'a> {
    /// Selected using fact selector.
    Selected {
        /// The fact application that produced this match.
        application: &'a FactApplication,
        /// The matched fact.
        fact: &'a Fact,
    },
    /// Derived using formula application.
    Derived {
        /// The term being bound.
        term: &'a Term<Value>,
        /// The computed value.
        value: Box<Value>,
        /// The facts that were read to produce this derived value, keyed by parameter name.
        from: HashMap<String, Factors>,
        /// The formula application that produced this value.
        formula: &'a crate::application::formula::FormulaApplication,
    },
    /// Applied parameter.
    Parameter {
        /// The term being bound.
        term: &'a Term<Value>,
        /// The parameter value.
        value: &'a Value,
    },
}

impl Answer {
    /// Create new empty answer.
    pub fn new() -> Self {
        Self::default()
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
        if let Term::Variable { name: Some(_), .. } = application.the()
            && let Some(factors) = self.resolve_factors(&application.the().as_unknown())
        {
            return Ok(Fact::from(factors));
        }

        // Try 'of' next
        if let Term::Variable { name: Some(_), .. } = application.of()
            && let Some(factors) = self.resolve_factors(&application.of().as_unknown())
        {
            return Ok(Fact::from(factors));
        }

        // Try 'is' last
        if let Term::Variable { name: Some(_), .. } = application.is()
            && let Some(factors) = self.resolve_factors(&application.is().as_unknown())
        {
            return Ok(Fact::from(factors));
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

    /// Merge evidence into this answer, recording facts and binding variables.
    pub fn merge(&mut self, evidence: Evidence<'_>) -> Result<(), InconsistencyError> {
        match evidence {
            Evidence::Selected { application, fact } => {
                let fact = Arc::new(fact.to_owned());
                self.record(application, fact.clone())?;

                let application = Arc::new(application.to_owned());
                self.assign(
                    &application.the().as_unknown(),
                    &Factor::Selected {
                        selector: Selector::The,
                        application: application.clone(),
                        fact: fact.clone(),
                    },
                )?;
                self.assign(
                    &application.of().as_unknown(),
                    &Factor::Selected {
                        selector: Selector::Of,
                        application: application.clone(),
                        fact: fact.clone(),
                    },
                )?;
                self.assign(
                    &application.is().as_unknown(),
                    &Factor::Selected {
                        selector: Selector::Is,
                        application: application.clone(),
                        fact: fact.clone(),
                    },
                )?;
                self.assign(
                    &application.cause().as_unknown(),
                    &Factor::Selected {
                        selector: Selector::Cause,
                        application: application.clone(),
                        fact,
                    },
                )?;

                Ok(())
            }
            Evidence::Parameter { term, value } => self.assign(
                term,
                &Factor::Parameter {
                    value: value.clone(),
                },
            ),
            Evidence::Derived {
                term,
                from,
                value,
                formula,
            } => self.assign(
                term,
                &Factor::Derived {
                    value: *value,
                    from,
                    formula: Arc::new(formula.to_owned()),
                },
            ),
        }
    }

    /// Look up the factors bound to a named variable term.
    pub fn lookup(&self, term: &Term<Value>) -> Option<&Factors> {
        match term {
            Term::Variable {
                name: Some(key), ..
            } => self.conclusions.get(key),
            Term::Variable { name: None, .. } => None,
            Term::Constant(_) => None,
        }
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
                        if let Factor::Selected {
                            application, fact, ..
                        } = factor
                        {
                            self.record(application, fact.clone())?;
                        }

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
            } => self.conclusions.get(name),
            Term::Variable { name: None, .. } => None,
            Term::Constant(_) => None,
        }
    }

    /// Resolve a term to its Value without type conversion.
    ///
    /// For variables, looks up the binding and returns the raw Value.
    /// For constants, converts the constant to a Value.
    ///
    /// Returns an error if the variable is not bound.
    pub fn resolve<T>(&self, term: &Term<T>) -> Result<Value, InconsistencyError>
    where
        T: Scalar,
    {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(factors) = self.conclusions.get(key) {
                        Ok(factors.content().clone())
                    } else {
                        Err(InconsistencyError::UnboundVariableError(key.clone()))
                    }
                } else {
                    Err(InconsistencyError::UnboundVariableError("_".into()))
                }
            }
            Term::Constant(value) => Ok(value.as_value()),
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

    /// Convenience method to set a variable to a value without provenance tracking.
    /// This creates a Parameter factor for the value.
    /// Useful for testing and simple cases where provenance isn't needed.
    pub fn set<T: Scalar>(mut self, term: Term<T>, value: T) -> Result<Self, InconsistencyError>
    where
        Value: From<T>,
    {
        let factor = Factor::Parameter {
            value: value.into(),
        };
        self.assign(&term.as_unknown(), &factor)?;
        Ok(self)
    }

    /// Convenience method to get a value for a variable.
    /// Similar to Match::get but works with Answer.
    pub fn get<T>(&self, term: &Term<T>) -> Result<T, InconsistencyError>
    where
        T: Scalar + std::convert::TryFrom<Value>,
    {
        let value = self.resolve(term)?;
        let value_type = value.data_type();
        T::try_from(value).map_err(|_| {
            InconsistencyError::TypeConversion(crate::artifact::TypeError::TypeMismatch(
                T::TYPE.unwrap_or(Type::Bytes),
                value_type,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;
    use crate::artifact::{Attribute, Entity};
    use std::str::FromStr;

    // Helper function to create a test fact for Answer tests
    // Since Fact requires a Cause (Blake3Hash), we create a simple helper
    fn create_test_fact(entity: Entity, attr: Attribute, value: Value) -> Fact {
        use crate::artifact::Cause;

        // Create a dummy cause for testing - Cause is a newtype around a 32-byte hash
        let cause = Cause([0u8; 32]);

        Fact::Assertion {
            the: attr,
            of: entity,
            is: value,
            cause,
        }
    }

    // Helper to create a Factor::Selected for testing
    fn create_test_factor(selector: Selector, fact: Arc<Fact>) -> Factor {
        use crate::application::FactApplication;

        // Create a minimal FactApplication for testing
        let application = Arc::new(FactApplication::new(
            Term::var("the"),
            Term::var("of"),
            Term::var("is"),
            Term::var("cause"),
            crate::Cardinality::One,
        ));

        Factor::Selected {
            selector,
            application,
            fact,
        }
    }

    #[dialog_common::test]
    fn test_answer_contains_bound_variable() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term = Term::<Value>::var("name");

        // Initially should not contain the variable
        assert!(!answer.contains(&name_term));

        // After assignment, should contain the variable
        answer.assign(&name_term, &factor).unwrap();
        assert!(answer.contains(&name_term));
    }

    #[dialog_common::test]
    fn test_answer_contains_unbound_variable() {
        let answer = Answer::new();
        let name_term = Term::<Value>::var("name");

        // Should not contain unbound variable
        assert!(!answer.contains(&name_term));
    }

    #[dialog_common::test]
    fn test_answer_contains_constant() {
        let answer = Answer::new();
        let constant_term = Term::Constant(Value::String("constant_value".to_string()));

        // Constants are always "bound"
        assert!(answer.contains(&constant_term));
    }

    #[dialog_common::test]
    fn test_answer_contains_blank_variable() {
        let answer = Answer::new();
        let blank_term = Term::<Value>::blank();

        // Blank variables (Any) are never "bound"
        assert!(!answer.contains(&blank_term));
    }

    #[dialog_common::test]
    fn test_answer_resolve_string() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term = Term::<String>::var("name");
        let name_term_value = Term::<Value>::var("name");

        // Assign the value
        answer.assign(&name_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&name_term)
            .and_then(|v| String::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Alice");
    }

    #[dialog_common::test]
    fn test_answer_resolve_u32() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/age").unwrap();
        let value = Value::UnsignedInt(25);
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let age_term = Term::<u32>::var("age");
        let age_term_value = Term::<Value>::var("age");

        // Assign the value
        answer.assign(&age_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&age_term)
            .and_then(|v| u32::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 25);
    }

    #[dialog_common::test]
    fn test_answer_resolve_i32() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/score").unwrap();
        let value = Value::SignedInt(-10);
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let score_term = Term::<i32>::var("score");
        let score_term_value = Term::<Value>::var("score");

        // Assign the value
        answer.assign(&score_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&score_term)
            .and_then(|v| i32::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), -10);
    }

    #[dialog_common::test]
    fn test_answer_resolve_bool() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/active").unwrap();
        let value = Value::Boolean(true);
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let active_term = Term::<bool>::var("active");
        let active_term_value = Term::<Value>::var("active");

        // Assign the value
        answer.assign(&active_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&active_term)
            .and_then(|v| bool::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[dialog_common::test]
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
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let entity_term = Term::<Entity>::var("entity_id");
        let entity_term_value = Term::<Value>::var("entity_id");

        // Assign the value
        answer.assign(&entity_term_value, &factor).unwrap();

        // Resolve it using the type-safe method
        let result = answer
            .resolve(&entity_term)
            .and_then(|v| Entity::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), entity_value);
    }

    #[dialog_common::test]
    fn test_answer_resolve_constant() {
        let answer = Answer::new();
        let constant_term = Term::Constant("constant_value".to_string());

        // Resolve constant directly
        let result = answer
            .resolve(&constant_term)
            .and_then(|v| String::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "constant_value");
    }

    #[dialog_common::test]
    fn test_answer_resolve_unbound_variable() {
        let answer = Answer::new();
        let name_term = Term::<String>::var("name");

        // Try to resolve unbound variable (should fail)
        let result = answer
            .resolve(&name_term)
            .and_then(|v| String::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::UnboundVariableError(var) => {
                assert_eq!(var, "name");
            }
            _ => panic!("Expected UnboundVariableError"),
        }
    }

    #[dialog_common::test]
    fn test_answer_resolve_blank_variable() {
        let answer = Answer::new();
        let blank_term = Term::<String>::blank();

        // Try to resolve blank variable (should fail)
        let result = answer
            .resolve(&blank_term)
            .and_then(|v| String::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::UnboundVariableError(_) => {} // Expected
            _ => panic!("Expected UnboundVariableError"),
        }
    }

    #[dialog_common::test]
    fn test_answer_resolve_type_mismatch() {
        let entity = Entity::new().unwrap();
        let attr = Attribute::from_str("user/name").unwrap();
        let value = Value::String("Alice".to_string());
        let fact = Arc::new(create_test_fact(
            entity.clone(),
            attr.clone(),
            value.clone(),
        ));
        let factor = create_test_factor(Selector::Is, Arc::clone(&fact));

        let mut answer = Answer::new();
        let name_term_value = Term::<Value>::var("name");

        // Assign a string value
        answer.assign(&name_term_value, &factor).unwrap();

        // Try to resolve it as a u32 (should fail)
        let age_term = Term::<u32>::var("name");
        let result = answer
            .resolve(&age_term)
            .and_then(|v| u32::try_from(v).map_err(InconsistencyError::TypeConversion));
        assert!(result.is_err());
        match result.unwrap_err() {
            InconsistencyError::TypeConversion(_) => {} // Expected
            _ => panic!("Expected TypeConversion error"),
        }
    }

    #[dialog_common::test]
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

        let factor1 = create_test_factor(Selector::Is, Arc::clone(&fact1));
        let factor2 = create_test_factor(Selector::Is, Arc::clone(&fact2));

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

    #[dialog_common::test]
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
        let name_factor = create_test_factor(Selector::Is, Arc::clone(&name_fact));

        let age_attr = Attribute::from_str("user/age").unwrap();
        let age_value = Value::UnsignedInt(30);
        let age_fact = Arc::new(create_test_fact(
            entity.clone(),
            age_attr.clone(),
            age_value.clone(),
        ));
        let age_factor = create_test_factor(Selector::Is, Arc::clone(&age_fact));

        let active_attr = Attribute::from_str("user/active").unwrap();
        let active_value = Value::Boolean(true);
        let active_fact = Arc::new(create_test_fact(
            entity.clone(),
            active_attr.clone(),
            active_value.clone(),
        ));
        let active_factor = create_test_factor(Selector::Is, Arc::clone(&active_fact));

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
        let name_result =
            String::try_from(answer.resolve::<String>(&Term::var("name")).unwrap()).unwrap();
        let age_result = u32::try_from(answer.resolve::<u32>(&Term::var("age")).unwrap()).unwrap();
        let active_result =
            bool::try_from(answer.resolve::<bool>(&Term::var("active")).unwrap()).unwrap();

        assert_eq!(name_result, "Bob");
        assert_eq!(age_result, 30);
        assert!(active_result);
    }

    #[dialog_common::test]
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
        let name_factor = create_test_factor(Selector::Is, Arc::clone(&name_fact));

        let age_attr = Attribute::from_str("user/age").unwrap();
        let age_value = Value::UnsignedInt(35);
        let age_fact = Arc::new(create_test_fact(
            entity.clone(),
            age_attr.clone(),
            age_value.clone(),
        ));
        let age_factor = create_test_factor(Selector::Is, Arc::clone(&age_fact));

        // Use extend to assign multiple values at once
        let assignments = vec![
            (Term::<Value>::var("name"), name_factor),
            (Term::<Value>::var("age"), age_factor),
        ];

        let mut answer = Answer::new();
        answer.extend(assignments).unwrap();

        // Verify all values were assigned
        let name_result =
            String::try_from(answer.resolve::<String>(&Term::var("name")).unwrap()).unwrap();
        let age_result = u32::try_from(answer.resolve::<u32>(&Term::var("age")).unwrap()).unwrap();

        assert_eq!(name_result, "Charlie");
        assert_eq!(age_result, 35);
    }
}
