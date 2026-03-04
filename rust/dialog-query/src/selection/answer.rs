use std::collections::HashMap;
use std::sync::Arc;

use crate::artifact::{Type, Value};
use crate::error::EvaluationError;
use crate::relation::query::RelationQuery;
use crate::term::Term;
use crate::types::Any;
use crate::types::Typed;
use crate::{Claim, types::Scalar};

use super::Answers;

/// A single result row produced during query evaluation.
///
/// An `Answer` accumulates variable bindings as premises are evaluated in
/// sequence. Each binding maps a variable name to its resolved [`Value`].
///
/// In addition to named bindings, an `Answer` tracks the raw [`Claim`]
/// facts matched by each [`RelationQuery`]. This allows downstream code
/// to reconstruct the matched facts for any relation in the result.
///
/// Answers flow through the evaluation pipeline as a stream
/// ([`Answers`](super::Answers)): each premise receives the stream,
/// potentially expands each answer into zero or more new answers, and
/// passes them to the next premise.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Answer {
    /// Named variable bindings: maps variable names to their resolved values.
    bindings: HashMap<String, Value>,
    /// Maps RelationQuery to the claim it matched.
    /// This allows us to realize facts even when the application had only constants/blanks.
    claims: HashMap<RelationQuery, Arc<Claim>>,
}

impl Answer {
    /// Create new empty answer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wrap this answer into a single-element `Answers` stream.
    pub fn seed(self) -> impl Answers {
        futures_util::stream::once(async { Ok(self) })
    }

    /// Get all tracked claims from the relation queries.
    pub fn claims(&self) -> impl Iterator<Item = &Arc<Claim>> {
        self.claims.values()
    }

    /// Record that a RelationQuery matched a specific claim.
    /// Returns an error if the same application already mapped to a different claim.
    pub fn record(
        &mut self,
        application: &RelationQuery,
        claim: Arc<Claim>,
    ) -> Result<(), EvaluationError> {
        if let Some(existing) = self.claims.get(application) {
            if !Arc::ptr_eq(existing, &claim) {
                return Err(EvaluationError::Assignment {
                    reason: format!(
                        "RelationQuery {:?} already mapped to a different claim",
                        application
                    ),
                });
            }
        } else {
            self.claims.insert(application.clone(), claim);
        }
        Ok(())
    }

    /// Realize a relation from a RelationQuery.
    /// Looks up the application in the recorded claims.
    pub fn realize(&self, application: &RelationQuery) -> Result<Claim, EvaluationError> {
        if let Some(claim) = self.claims.get(application) {
            return Ok(claim.as_ref().clone());
        }

        Err(EvaluationError::Store(
            "Could not realize relation from answer - application not found".to_string(),
        ))
    }

    /// Merge a relation query result into this answer, recording the claim
    /// and binding the/of/is/cause values from it.
    pub fn merge_relation(
        &mut self,
        application: &RelationQuery,
        claim: &Claim,
    ) -> Result<(), EvaluationError> {
        let claim = Arc::new(claim.to_owned());
        self.record(application, claim.clone())?;

        self.bind(
            &Term::<Any>::from(application.the()),
            Value::Symbol(claim.the()),
        )?;
        self.bind(
            &Term::<Any>::from(application.of()),
            Value::Entity(claim.of().clone()),
        )?;
        self.bind(application.is(), claim.is().clone())?;
        self.bind(
            &Term::<Any>::from(application.cause()),
            Value::Bytes(claim.cause().clone().0.into()),
        )?;

        Ok(())
    }

    /// Look up the value bound to a named variable.
    pub fn lookup(&self, param: &Term<Any>) -> Option<&Value> {
        match param {
            Term::Variable {
                name: Some(key), ..
            } => self.bindings.get(key),
            _ => None,
        }
    }

    /// Bind a term to a value. For named variables, stores the value in
    /// the bindings map; checks consistency if already bound. Constants
    /// and blanks are no-ops.
    pub fn bind(&mut self, param: &Term<Any>, value: Value) -> Result<(), EvaluationError> {
        match param {
            Term::Variable {
                name: Some(name), ..
            } => {
                if let Some(existing) = self.bindings.get(name) {
                    if *existing != value {
                        Err(EvaluationError::Assignment {
                            reason: format!(
                                "Can not set {:?} to {:?} because it is already set to {:?}.",
                                name, value, existing
                            ),
                        })
                    } else {
                        Ok(())
                    }
                } else {
                    self.bindings.insert(name.into(), value);
                    Ok(())
                }
            }
            Term::Variable { name: None, .. } | Term::Constant(_) => Ok(()),
        }
    }

    /// Extends this answer by binding multiple term-value pairs.
    pub fn extend<I>(&mut self, assignments: I) -> Result<(), EvaluationError>
    where
        I: IntoIterator<Item = (Term<Any>, Value)>,
    {
        for (param, value) in assignments {
            self.bind(&param, value)?;
        }
        Ok(())
    }

    /// Returns true if the parameter is bound in this answer.
    pub fn contains(&self, param: &Term<Any>) -> bool {
        match param {
            Term::Variable {
                name: Some(key), ..
            } => self.bindings.contains_key(key),
            Term::Variable { name: None, .. } => false,
            Term::Constant(_) => true,
        }
    }

    /// Resolve a parameter to its Value.
    ///
    /// For variables, looks up the binding and returns the raw Value.
    /// For constants, returns the constant value.
    ///
    /// Returns an error if the variable is not bound.
    pub fn resolve(&self, param: &Term<Any>) -> Result<Value, EvaluationError> {
        match param {
            Term::Variable {
                name: Some(key), ..
            } => {
                if let Some(value) = self.bindings.get(key) {
                    Ok(value.clone())
                } else {
                    Err(EvaluationError::UnboundVariable {
                        variable_name: key.clone(),
                    })
                }
            }
            Term::Variable { name: None, .. } => Err(EvaluationError::UnboundVariable {
                variable_name: "_".into(),
            }),
            Term::Constant(value) => Ok(value.clone()),
        }
    }

    /// Resolve a variable term into a constant term if this answer has a
    /// binding for it. Otherwise, return the original term.
    pub fn resolve_term<T: Scalar>(&self, term: &Term<T>) -> Term<T> {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(value) = self.bindings.get(key) {
                        if let Ok(converted) = T::try_from(value.clone()) {
                            Term::Constant(converted.into())
                        } else {
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

    /// Resolve a variable parameter into a constant parameter if this answer
    /// has a binding for it. Otherwise, return the original parameter.
    pub fn resolve_parameter(&self, param: &Term<Any>) -> Term<Any> {
        match param {
            Term::Variable {
                name: Some(key), ..
            } => {
                if let Some(value) = self.bindings.get(key) {
                    Term::Constant(value.clone())
                } else {
                    param.clone()
                }
            }
            _ => param.clone(),
        }
    }

    /// Set a variable to a value.
    pub fn set<T: Scalar>(mut self, term: Term<T>, value: T) -> Result<Self, EvaluationError>
    where
        Value: From<T>,
    {
        self.bind(&Term::<Any>::from(&term), value.into())?;
        Ok(self)
    }

    /// Get a typed value from this answer.
    pub fn get<T>(&self, term: &Term<T>) -> Result<T, EvaluationError>
    where
        T: Typed + Clone + 'static,
        Term<Any>: for<'a> From<&'a Term<T>>,
        T: std::convert::TryFrom<Value>,
    {
        let value = self.resolve(&Term::<Any>::from(term))?;
        let value_type = value.data_type();
        T::try_from(value).map_err(|_| EvaluationError::TypeMismatch {
            expected: <<T as Typed>::Descriptor as crate::types::TypeDescriptor>::TYPE
                .unwrap_or(Type::Bytes),
            actual: value_type,
        })
    }
}
