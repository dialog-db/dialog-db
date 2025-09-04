//! Syntax trait for query forms

use crate::artifact::Value;
use crate::fact::Scalar;
use crate::term::Term;
use std::collections::BTreeSet;

/// Tracks variable bindings during query planning
#[derive(Debug, Clone, PartialEq, Eq)]
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

    pub fn size(&self) -> usize {
        self.bound_variables.len()
    }

    pub fn add<T: Scalar>(mut self, variable: &Term<T>) -> Self {
        if let Term::Variable {
            name: Some(name), ..
        } = variable
        {
            self.bound_variables.insert(name.clone());
        }
        self
    }

    pub fn extend(&mut self, other: impl IntoIterator<Item = Term<Value>>) -> VariableScope {
        let mut delta = std::collections::BTreeSet::new();

        for variable in other {
            if let Term::Variable {
                name: Some(name), ..
            } = variable
            {
                if !self.bound_variables.contains(&name) {
                    delta.insert(name.clone());
                }
                self.bound_variables.insert(name);
            }
        }

        VariableScope {
            bound_variables: delta,
        }
    }

    pub fn union(self, other: impl IntoIterator<Item = Term<Value>>) -> VariableScope {
        self.clone().extend(other)
    }

    pub fn intersection(self, other: impl IntoIterator<Item = Term<Value>>) -> VariableScope {
        let mut intersection = Self::new();
        for variable in other {
            if let Term::Variable {
                name: Some(name), ..
            } = variable
            {
                if !self.bound_variables.contains(&name) {
                    intersection.bound_variables.insert(name.clone());
                }
            }
        }

        intersection
    }

    pub fn intersects(&self, other: &VariableScope) -> bool {
        !self.bound_variables.is_disjoint(&other.bound_variables)
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

impl IntoIterator for VariableScope {
    type Item = Term<crate::artifact::Value>;
    type IntoIter = std::vec::IntoIter<Term<crate::artifact::Value>>;

    fn into_iter(self) -> Self::IntoIter {
        self.bound_variables
            .into_iter()
            .map(|var| Term::<crate::artifact::Value>::var(&var))
            .collect::<Vec<_>>()
            .into_iter()
    }
}
