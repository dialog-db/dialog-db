//! Syntax trait for query forms

use crate::fact::Scalar;
use crate::term::Term;
use std::collections::BTreeSet;

/// Tracks variable bindings during query planning
#[derive(Debug, Clone)]
pub struct VariableScope {
    /// Set of variables that have already been bound.
    pub bound_variables: BTreeSet<String>,
}

impl VariableScope {
    pub fn new() -> Self {
        Self {
            bound_variables: BTreeSet::new(),
        }
    }

    pub fn contains<T: Scalar>(&self, term: &Term<T>) -> bool {
        match term {
            // If term is a constant we return true as it is in the scope.
            Term::Constant(_) => true,
            // If term is a blank variable (_) we don't have it in the scope
            // as those don't get bound.
            Term::Variable { name: None, .. } => false,
            // Otherwise we just check if the variable name is in the bound set.
            Term::Variable {
                name: Some(name), ..
            } => self.bound_variables.contains(name),
        }
    }
}
