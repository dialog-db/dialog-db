use crate::{Term, Value};
use std::collections::HashMap;

/// Represents set of bindings used in the rule or formula applications. It is
/// effectively a map of terms (constant or variable) keyed by parameter names.
#[derive(Debug, Clone, PartialEq)]
pub struct Parameters(HashMap<String, Term<Value>>);
impl Parameters {
    pub fn new() -> Self {
        Self(HashMap::new())
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::term::Term;

    #[test]
    fn test_terms_basic_operations() {
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
