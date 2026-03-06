use futures_util::stream::once;
use std::collections::HashMap;
use std::sync::Arc;

use crate::Claim;
use crate::artifact::Value;
use crate::error::EvaluationError;
use crate::term::Term;
use crate::types::Any;
use crate::types::Record;

use super::Selection;

/// A single result row produced during query evaluation.
///
/// A `Match` accumulates variable bindings as premises are evaluated in
/// sequence. Each binding maps a variable name to its resolved [`Value`].
///
/// Matches flow through the evaluation pipeline as a stream
/// ([`Selection`](super::Selection)): each premise receives the stream,
/// potentially expands each match into zero or more new matches, and
/// passes them to the next premise.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Match {
    /// Named variable bindings: maps variable names to their resolved values.
    bindings: HashMap<String, Value>,
    // TODO: Once Value::Record supports the RecordFormat trait proposed in
    // https://github.com/dialog-db/dialog-db/pull/221 claims can be stored
    // directly as Value::Record in bindings, eliminating this separate map.
    claims: HashMap<String, Arc<Claim>>,
}

impl Match {
    /// Create new empty match.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wrap this match into a single-element `Selection` stream.
    pub fn seed(self) -> impl Selection {
        once(async { Ok(self) })
    }

    /// Provide evidence for the given term: look up the claim it cites.
    pub fn prove(&self, term: &Term<Record>) -> Result<Claim, EvaluationError> {
        let key = match term {
            Term::Variable {
                name: Some(name), ..
            } => name,
            _ => {
                return Err(EvaluationError::Store(
                    "Cannot look up claim with a non-variable term".to_string(),
                ));
            }
        };

        if let Some(claim) = self.claims.get(key) {
            Ok(claim.as_ref().clone())
        } else {
            Err(EvaluationError::Store(format!(
                "No claim found for term {:?}",
                key
            )))
        }
    }

    /// Cite a claim as evidence for the given term.
    pub fn cite(&mut self, term: &Term<Record>, claim: &Claim) -> Result<(), EvaluationError> {
        if let Term::Variable {
            name: Some(name), ..
        } = term
        {
            self.claims.insert(name.clone(), Arc::new(claim.to_owned()));
        }

        Ok(())
    }

    /// Bind a term to a value. For named variables, stores the value in
    /// the bindings map; checks consistency if already bound. Constants
    /// and blanks are no-ops.
    pub fn bind(&mut self, term: &Term<Any>, value: Value) -> Result<(), EvaluationError> {
        match term {
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

    /// Returns true if the parameter is bound in this match.
    pub fn contains(&self, term: &Term<Any>) -> bool {
        match term {
            Term::Variable {
                name: Some(key), ..
            } => self.bindings.contains_key(key),
            Term::Variable { name: None, .. } => false,
            Term::Constant(_) => true,
        }
    }

    /// Look up the value bound to a term.
    ///
    /// For variables, looks up the binding. For constants, returns the
    /// constant value. Returns an error if the variable is not bound.
    pub fn lookup(&self, term: &Term<Any>) -> Result<Value, EvaluationError> {
        match term {
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
}
