use std::collections::HashMap;
use std::sync::Arc;

use crate::artifact::{Type, TypeError, Value};
use crate::error::{InconsistencyError, QueryError};
use crate::parameter::Parameter;
use crate::relation::query::RelationQuery;
use crate::{Claim, Term, types::Scalar};

use super::{Answers, Evidence, Factor, Factors, Selector};

/// A single result row produced during query evaluation.
///
/// An `Answer` accumulates variable bindings as premises are evaluated in
/// sequence. Each binding is backed by one or more [`Factors`] that record
/// *how* the value was obtained — whether it was selected from a stored
/// fact, derived by a formula, or provided as a query parameter.
///
/// In addition to named bindings (`conclusions`), an `Answer` tracks the
/// raw [`Claim`] facts matched by each [`RelationQuery`]. This
/// allows downstream code to reconstruct the full provenance chain for any
/// value in the result.
///
/// Answers flow through the evaluation pipeline as a stream
/// ([`Answers`](super::Answers)): each premise receives the stream,
/// potentially expands each answer into zero or more new answers, and
/// passes them to the next premise.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Answer {
    /// Conclusions: named variable bindings where we've concluded values from facts.
    /// Maps variable names to their values with provenance (which facts support this binding).
    conclusions: HashMap<String, Factors>,
    /// Applications: maps RelationQuery to the fact it matched.
    /// This allows us to realize facts even when the application had only constants/blanks.
    /// The facts stored here represent all facts that contributed to this answer.
    facts: HashMap<RelationQuery, Arc<Claim>>,
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

    /// Get all tracked relations from the applications.
    pub fn facts(&self) -> impl Iterator<Item = &Arc<Claim>> {
        self.facts.values()
    }

    /// Get all conclusions (named variable bindings).
    pub fn conclusions(&self) -> impl Iterator<Item = (&String, &Factors)> {
        self.conclusions.iter()
    }

    /// Record that a RelationQuery matched a specific claim.
    /// Returns an error if the same application already mapped to a different fact,
    /// which would indicate an inconsistency (shouldn't happen in practice, but we check).
    pub fn record(
        &mut self,
        application: &RelationQuery,
        fact: Arc<Claim>,
    ) -> Result<(), InconsistencyError> {
        // Check if this application already has a different fact
        if let Some(existing_fact) = self.facts.get(application) {
            if !Arc::ptr_eq(existing_fact, &fact) {
                return Err(InconsistencyError::AssignmentError(format!(
                    "RelationQuery {:?} already mapped to a different fact",
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

    /// Realize a relation from a RelationQuery.
    /// First tries to extract from named variable conclusions.
    /// Falls back to looking up the application in the recorded applications.
    pub fn realize(&self, application: &RelationQuery) -> Result<Claim, QueryError> {
        // Try to extract from a named variable conclusion first
        // This gives us the full relation with all its components
        if let Term::Variable { name: Some(_), .. } = application.of()
            && let Some(factors) = self.resolve_factors(&Parameter::from(application.of()))
        {
            return Ok(Claim::from(factors));
        }

        if let Parameter::Variable { name: Some(_), .. } = application.is()
            && let Some(factors) = self.resolve_factors(application.is())
        {
            return Ok(Claim::from(factors));
        }

        // No named variables - look up by application
        if let Some(relation) = self.facts.get(application) {
            return Ok(relation.as_ref().clone());
        }

        Err(QueryError::FactStore(
            "Could not realize relation from answer - application not found".to_string(),
        ))
    }

    /// Merge evidence into this answer, recording facts and binding variables.
    pub fn merge(&mut self, evidence: Evidence<'_>) -> Result<(), InconsistencyError> {
        match evidence {
            Evidence::Relation { application, fact } => {
                let fact = Arc::new(fact.to_owned());
                self.record(application, fact.clone())?;

                let application = Arc::new(application.to_owned());

                self.assign(
                    &Parameter::from(application.the()),
                    &Factor::Selected {
                        selector: Selector::The,
                        application: application.clone(),
                        fact: fact.clone(),
                    },
                )?;
                self.assign(
                    &Parameter::from(application.of()),
                    &Factor::Selected {
                        selector: Selector::Of,
                        application: application.clone(),
                        fact: fact.clone(),
                    },
                )?;
                self.assign(
                    application.is(),
                    &Factor::Selected {
                        selector: Selector::Is,
                        application: application.clone(),
                        fact: fact.clone(),
                    },
                )?;
                self.assign(
                    &Parameter::from(application.cause()),
                    &Factor::Selected {
                        selector: Selector::Cause,
                        application,
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

    /// Look up the factors bound to a named variable parameter.
    pub fn lookup(&self, param: &Parameter) -> Option<&Factors> {
        match param {
            Parameter::Variable {
                name: Some(key), ..
            } => self.conclusions.get(key),
            _ => None,
        }
    }

    /// Assign a parameter to a factor.
    pub fn assign(
        &mut self,
        param: &Parameter,
        factor: &Factor,
    ) -> Result<(), InconsistencyError> {
        match param {
            Parameter::Variable {
                name: Some(name), ..
            } => {
                if let Some(factors) = self.conclusions.get_mut(name) {
                    if factors.content() != factor.content() {
                        Err(InconsistencyError::AssignmentError(format!(
                            "Can not set {:?} to {:?} because it is already set to {:?}.",
                            name,
                            factor.content(),
                            factors.content()
                        )))
                    } else {
                        factors.add(factor.clone());
                        if let Factor::Selected {
                            application, fact, ..
                        } = factor
                        {
                            self.record(application.as_ref(), fact.clone())?;
                        }
                        Ok(())
                    }
                } else {
                    self.conclusions
                        .insert(name.into(), Factors::new(factor.clone()));
                    Ok(())
                }
            }
            Parameter::Variable { name: None, .. } | Parameter::Constant(_) => Ok(()),
        }
    }

    /// Extends this answer by assigning multiple parameter-factor pairs.
    pub fn extend<I>(&mut self, assignments: I) -> Result<(), InconsistencyError>
    where
        I: IntoIterator<Item = (Parameter, Factor)>,
    {
        for (param, factor) in assignments {
            self.assign(&param, &factor)?;
        }
        Ok(())
    }

    /// Returns true if the parameter is bound in this answer.
    pub fn contains(&self, param: &Parameter) -> bool {
        match param {
            Parameter::Variable {
                name: Some(key), ..
            } => self.conclusions.contains_key(key),
            Parameter::Variable { name: None, .. } => false,
            Parameter::Constant(_) => true,
        }
    }

    /// Returns true if the term is bound in this answer.
    pub fn contains_term<T: Scalar>(&self, term: &Term<T>) -> bool {
        self.contains(&Parameter::from(term))
    }

    /// Resolves factors that were assigned to the given parameter.
    pub fn resolve_factors(&self, param: &Parameter) -> Option<&Factors> {
        match param {
            Parameter::Variable {
                name: Some(name), ..
            } => self.conclusions.get(name),
            _ => None,
        }
    }

    /// Resolve a parameter to its Value.
    ///
    /// For variables, looks up the binding and returns the raw Value.
    /// For constants, returns the constant value.
    ///
    /// Returns an error if the variable is not bound.
    pub fn resolve(&self, param: &Parameter) -> Result<Value, InconsistencyError> {
        match param {
            Parameter::Variable {
                name: Some(key), ..
            } => {
                if let Some(factors) = self.conclusions.get(key) {
                    Ok(factors.content().clone())
                } else {
                    Err(InconsistencyError::UnboundVariableError(key.clone()))
                }
            }
            Parameter::Variable { name: None, .. } => {
                Err(InconsistencyError::UnboundVariableError("_".into()))
            }
            Parameter::Constant(value) => Ok(value.clone()),
        }
    }

    /// Resolve a typed term to its Value.
    ///
    /// Convenience wrapper that converts the term to a Parameter first.
    pub fn resolve_term_value<T: Scalar>(
        &self,
        term: &Term<T>,
    ) -> Result<Value, InconsistencyError> {
        self.resolve(&Parameter::from(term))
    }

    /// Resolve a variable term into a constant term if this answer has a
    /// binding for it. Otherwise, return the original term.
    pub fn resolve_term<T: Scalar>(&self, term: &Term<T>) -> Term<T> {
        match term {
            Term::Variable { name, .. } => {
                if let Some(key) = name {
                    if let Some(factors) = self.conclusions.get(key) {
                        let value = factors.content();
                        if let Ok(converted) = T::try_from(value) {
                            Term::Constant(converted)
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
    pub fn resolve_parameter(&self, param: &Parameter) -> Parameter {
        match param {
            Parameter::Variable {
                name: Some(key), ..
            } => {
                if let Some(factors) = self.conclusions.get(key) {
                    Parameter::Constant(factors.content().clone())
                } else {
                    param.clone()
                }
            }
            _ => param.clone(),
        }
    }

    /// Set a variable to a value without provenance tracking.
    pub fn set<T: Scalar>(mut self, term: Term<T>, value: T) -> Result<Self, InconsistencyError>
    where
        Value: From<T>,
    {
        let factor = Factor::Parameter {
            value: value.into(),
        };
        self.assign(&Parameter::from(&term), &factor)?;
        Ok(self)
    }

    /// Get a typed value from this answer.
    pub fn get<T>(&self, term: &Term<T>) -> Result<T, InconsistencyError>
    where
        T: Scalar + std::convert::TryFrom<Value>,
    {
        let value = self.resolve(&Parameter::from(term))?;
        let value_type = value.data_type();
        T::try_from(value).map_err(|_| {
            InconsistencyError::TypeConversion(TypeError::TypeMismatch(
                T::TYPE.unwrap_or(Type::Bytes),
                value_type,
            ))
        })
    }
}
