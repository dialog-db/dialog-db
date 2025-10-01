//! Syntax trait for query forms

use crate::artifact::Value;
use crate::fact::Scalar;
use crate::term::Term;
use std::collections::HashSet;

/// Tracks variable bindings during query planning
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariableScope {
    /// Set of variables that have already been bound.
    pub variables: HashSet<String>,
}

impl VariableScope {
    pub fn new() -> Self {
        Self {
            variables: HashSet::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.variables.len()
    }

    /// Adds a variable to the scope. If given term is a constant or a blank
    /// variable, it is ignored.
    pub fn add<T: Scalar>(&mut self, variable: &Term<T>) -> &mut Self {
        if let Term::Variable {
            name: Some(name), ..
        } = variable
        {
            self.variables.insert(name.clone());
        }
        self
    }

    /// Removes a variable from the scope. Returns true if the variable was present.
    /// If term is a constant or blank variable, returns false.
    pub fn remove<T: Scalar>(&mut self, variable: &Term<T>) -> bool {
        if let Term::Variable {
            name: Some(name), ..
        } = variable
        {
            self.variables.remove(name)
        } else {
            false
        }
    }

    pub fn extend(&mut self, other: impl IntoIterator<Item = Term<Value>>) -> VariableScope {
        let mut delta = HashSet::new();

        for variable in other {
            if let Term::Variable {
                name: Some(name), ..
            } = variable
            {
                if !self.variables.contains(&name) {
                    delta.insert(name.clone());
                }
                self.variables.insert(name);
            }
        }

        VariableScope { variables: delta }
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
                if !self.variables.contains(&name) {
                    intersection.variables.insert(name.clone());
                }
            }
        }

        intersection
    }

    pub fn intersects(&self, other: &VariableScope) -> bool {
        !self.variables.is_disjoint(&other.variables)
    }

    /// Returns true if the term is bound in this scope. If term is a constant,
    /// it is considered bound. If term is a blank variable it can not be bound,
    /// if term is a named variable and variable is bound in this scope we return
    /// true.
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
            } => self.variables.contains(name),
        }
    }
}

impl IntoIterator for VariableScope {
    type Item = Term<crate::artifact::Value>;
    type IntoIter = std::vec::IntoIter<Term<crate::artifact::Value>>;

    fn into_iter(self) -> Self::IntoIter {
        self.variables
            .into_iter()
            .map(|var| Term::<crate::artifact::Value>::var(&var))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl IntoIterator for &VariableScope {
    type Item = Term<crate::artifact::Value>;
    type IntoIter = std::vec::IntoIter<Term<crate::artifact::Value>>;

    fn into_iter(self) -> Self::IntoIter {
        let vars = &self.variables;

        vars.into_iter()
            .map(|var| Term::<crate::artifact::Value>::var(var))
            .collect::<Vec<_>>()
            .into_iter()
    }
}
