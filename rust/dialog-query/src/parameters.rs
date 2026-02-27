use crate::{Term, Value};
use std::collections::HashMap;

/// A name-to-term mapping that describes how a premise is applied.
///
/// Every premise type (relation, concept, formula, constraint) exposes its
/// inputs and outputs as named parameters. A `Parameters` instance binds
/// each parameter name to a [`Term<Value>`] — either a concrete constant
/// or a named variable that will be resolved during query evaluation.
///
/// During planning, the [`Schema`](crate::Schema) is consulted to determine
/// which parameters are required vs optional, and the planner uses this
/// information together with the current [`Environment`](crate::Environment)
/// to decide whether the premise is viable.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Parameters(HashMap<String, Term<Value>>);
impl Parameters {
    /// Create a new empty parameter set
    pub fn new() -> Self {
        Self::default()
    }
    /// Returns the term associated with the given parameter name, if has one.
    pub fn get(&self, name: &str) -> Option<&Term<Value>> {
        self.0.get(name)
    }

    /// Inserts a new term binding for the given parameter name.
    /// If the parameter already exists, it will be overwritten.
    pub fn insert(&mut self, name: String, term: Term<Value>) {
        self.0.insert(name, term);
    }

    /// Checks if a term binding exists for the given parameter name.
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    /// Returns an iterator over all parameter-term pairs in this binding set.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Term<Value>)> {
        self.0.iter()
    }

    /// Returns an iterator over the parameter names in this binding set.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::term::Term;

    #[dialog_common::test]
    fn it_performs_basic_operations() {
        let mut terms = Parameters::new();

        // Test insertion and retrieval
        let name_term = Term::var("name");
        terms.insert("name".to_string(), name_term.clone());

        assert_eq!(terms.get("name"), Some(&name_term));
        assert_eq!(terms.get("nonexistent"), None);
        assert!(terms.contains("name"));
        assert!(!terms.contains("nonexistent"));

        // Test iteration
        let collected: Vec<_> = terms.iter().collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].0, &"name".to_string());
        assert_eq!(collected[0].1, &name_term);
    }
}
