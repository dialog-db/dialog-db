//! Syntax trait for query forms

use std::collections::HashSet;
use std::fmt;

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
///
/// Also used as the prerequisite set during planning: a premise declares which
/// variables must already be bound before it can execute. A premise becomes
/// viable once its prerequisites are all satisfied (i.e. present in the
/// planning environment).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Environment {
    variables: HashSet<String>,
}

impl Environment {
    /// Create a new empty environment
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a variable name to the bound set. Returns `&mut Self` for chaining.
    pub fn add(&mut self, name: impl Into<String>) -> &mut Self {
        self.variables.insert(name.into());
        self
    }

    /// Returns true if a variable name is bound in this environment.
    pub fn contains(&self, name: &str) -> bool {
        self.variables.contains(name)
    }

    /// Removes a variable name from the bound set. Returns true if it was present.
    pub fn remove(&mut self, name: &str) -> bool {
        self.variables.remove(name)
    }

    /// Returns the number of bound variables.
    pub fn len(&self) -> usize {
        self.variables.len()
    }

    /// Returns true if there are no bound variables.
    pub fn is_empty(&self) -> bool {
        self.variables.is_empty()
    }

    /// Extends this environment with all names from `other`, returning the
    /// delta (the set of names that were new to this environment).
    pub fn extend(&mut self, other: &Environment) -> Environment {
        let mut delta = Environment::new();
        for name in &other.variables {
            if !self.variables.contains(name) {
                delta.variables.insert(name.clone());
            }
            self.variables.insert(name.clone());
        }
        delta
    }

    /// Iterate over the bound variable names.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.variables.iter().map(|s| s.as_str())
    }
}

impl fmt::Display for Environment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.variables.iter();
        if let Some(name) = iter.next() {
            write!(f, "{}", name)?;
        }
        for name in iter {
            write!(f, ", {}", name)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_adds_and_contains() {
        let mut env = Environment::new();
        env.add("x");
        env.add("y");

        assert_eq!(env.len(), 2);
        assert!(env.contains("x"));
        assert!(env.contains("y"));
        assert!(!env.contains("z"));

        // Duplicate add doesn't increase size
        env.add("x");
        assert_eq!(env.len(), 2);
    }

    #[dialog_common::test]
    fn it_removes() {
        let mut env = Environment::new();
        env.add("x");
        assert!(env.remove("x"));
        assert!(!env.contains("x"));
        assert!(!env.remove("x"));
    }

    #[dialog_common::test]
    fn it_extends_returning_delta() {
        let mut env = Environment::new();
        env.add("x");

        let mut other = Environment::new();
        other.add("x");
        other.add("y");

        let delta = env.extend(&other);
        assert_eq!(env.len(), 2);
        assert_eq!(delta.len(), 1);
        assert!(delta.contains("y"));
        assert!(!delta.contains("x"));
    }

    #[dialog_common::test]
    fn it_iterates() {
        let mut env = Environment::new();
        env.add("a");
        env.add("b");

        let mut names: Vec<&str> = env.iter().collect();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[dialog_common::test]
    fn it_is_empty() {
        let env = Environment::new();
        assert!(env.is_empty());
        assert_eq!(env.len(), 0);
    }

    #[dialog_common::test]
    fn it_displays() {
        let mut env = Environment::new();
        let display = format!("{}", env);
        assert_eq!(display, "");

        env.add("x");
        let display = format!("{}", env);
        assert_eq!(display, "x");
    }
}
