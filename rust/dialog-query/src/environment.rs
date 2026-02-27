//! Syntax trait for query forms

use crate::artifact::Value;
use crate::term::Term;
use crate::types::Scalar;
use std::collections::HashSet;

/// The set of variable names that have been bound so far during query planning.
///
/// As the planner selects premises for execution, each premise declares which
/// variables it will bind (via its [`Candidate`](crate::planner::Candidate)).
/// Those names are added to the `Environment`, and subsequent premises are
/// re-evaluated against it — a premise that was `Blocked` may become `Viable`
/// once the variables it needs appear here.
///
/// At execution time, `Environment` is also stored inside each [`Plan`](crate::planner::Plan)
/// to record which variables were already bound when the plan was created and
/// which the plan itself will bind.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Environment {
    /// Set of variables that have already been bound.
    pub variables: HashSet<String>,
}

impl Environment {
    /// Create a new empty environment
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of bound variables
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

    /// Extends this environment with variables from the iterator, returning the delta
    pub fn extend(&mut self, other: impl IntoIterator<Item = Term<Value>>) -> Environment {
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

        Environment { variables: delta }
    }

    /// Returns the set of new variables not already in this environment
    pub fn union(self, other: impl IntoIterator<Item = Term<Value>>) -> Environment {
        self.clone().extend(other)
    }

    /// Returns variables in the iterator that are not in this environment
    pub fn intersection(self, other: impl IntoIterator<Item = Term<Value>>) -> Environment {
        let mut intersection = Self::new();
        for variable in other {
            if let Term::Variable {
                name: Some(name), ..
            } = variable
                && !self.variables.contains(&name)
            {
                intersection.variables.insert(name.clone());
            }
        }

        intersection
    }

    /// Returns true if this environment shares any variables with another
    pub fn intersects(&self, other: &Environment) -> bool {
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

impl IntoIterator for Environment {
    type Item = Term<Value>;
    type IntoIter = std::vec::IntoIter<Term<Value>>;

    fn into_iter(self) -> Self::IntoIter {
        self.variables
            .into_iter()
            .map(|var| Term::<Value>::var(&var))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl IntoIterator for &Environment {
    type Item = Term<Value>;
    type IntoIter = std::vec::IntoIter<Term<Value>>;

    fn into_iter(self) -> Self::IntoIter {
        let vars = &self.variables;

        vars.iter()
            .map(Term::<Value>::var)
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_ignores_constants() {
        let mut scope = Environment::new();

        // Adding a constant should do nothing
        scope.add(&Term::Constant(Value::String("test".to_string())));

        assert_eq!(
            scope.size(),
            0,
            "VariableScope.add() should ignore constants"
        );
        assert!(
            !scope.variables.contains("test"),
            "Constant values should not be added to scope"
        );
    }

    #[dialog_common::test]
    fn it_ignores_blank_variables() {
        let mut scope = Environment::new();

        // Adding a blank variable (None name) should do nothing
        scope.add(&Term::<Value>::blank());

        assert_eq!(
            scope.size(),
            0,
            "VariableScope.add() should ignore blank variables"
        );
    }

    #[dialog_common::test]
    fn it_adds_named_variables() {
        let mut scope = Environment::new();

        // Only named variables should be added
        scope.add(&Term::<Value>::var("x"));
        scope.add(&Term::<Value>::var("y"));

        assert_eq!(scope.size(), 2, "Should have 2 variables");
        assert!(scope.variables.contains("x"), "Should contain 'x'");
        assert!(scope.variables.contains("y"), "Should contain 'y'");

        // Adding the same variable again should not increase size
        scope.add(&Term::<Value>::var("x"));
        assert_eq!(scope.size(), 2, "Should still have 2 variables");
    }

    #[dialog_common::test]
    fn it_tracks_names_not_values() {
        let mut scope = Environment::new();

        // Add a variable to the scope
        scope.add(&Term::<Value>::var("name"));

        assert!(
            scope.variables.contains("name"),
            "Scope should track that 'name' is bound"
        );

        // The scope doesn't care what value the variable has
        // It only tracks the variable NAME for query planning
        // The actual value is stored in Query, not VariableScope
    }
}
