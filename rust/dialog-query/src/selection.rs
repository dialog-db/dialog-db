use std::{collections::BTreeMap, sync::Arc};

use dialog_artifacts::Value;
use dialog_common::ConditionalSend;
use futures_core::Stream;

use crate::{InconsistencyError, QueryError, Term, Variable};

pub trait Selection: Stream<Item = Result<Match, QueryError>> + 'static + ConditionalSend {}

impl<S> Selection for S where S: Stream<Item = Result<Match, QueryError>> + 'static + ConditionalSend
{}

#[derive(Clone, Debug)]
pub struct Match {
    variables: Arc<BTreeMap<Variable, Value>>,
}

impl Match {
    pub fn new() -> Self {
        Self {
            variables: Arc::new(BTreeMap::new()),
        }
    }

    pub fn has(&self, variable: &Variable) -> bool {
        self.variables.contains_key(variable)
    }

    pub fn get(&self, variable: &Variable) -> Result<Value, InconsistencyError> {
        if let Some(value) = self.variables.get(variable) {
            Ok(value.clone())
        } else {
            Err(InconsistencyError::UnboundVariableError(variable.clone()))
        }
    }

    pub fn set(&self, variable: Variable, assignment: Value) -> Result<Self, InconsistencyError> {
        if let Ok(assigned) = self.get(&variable) {
            if assigned == assignment {
                return Ok(self.clone());
            } else {
                return Err(InconsistencyError::AssignmentError(format!(
                    "Can not set {:?} to {:?} because it is already set to {:?}.",
                    variable, assignment, assigned
                )));
            }
        } else {
            let mut variables = (*self.variables).clone();
            variables.insert(variable, assignment);
            Ok(Self {
                variables: Arc::new(variables),
            })
        }
    }

    pub fn unify(&self, term: Term, value: Value) -> Result<Self, InconsistencyError> {
        match term {
            Term::Variable(variable) => self.set(variable, value),
            Term::Constant(constant) => {
                if constant == value {
                    Ok(self.clone())
                } else {
                    Err(InconsistencyError::TypeMismatch { expected: constant, actual: value })
                }
            }
        }
    }

    pub fn resolve(&self, term: &Term) -> Result<Value, InconsistencyError> {
        match term {
            Term::Variable(variable) => self.get(&variable),
            Term::Constant(constant) => Ok(constant.clone()),
        }
    }
}
