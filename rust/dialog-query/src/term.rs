//! Term types for pattern matching and query construction

use crate::variable::TypedVariable;
use dialog_artifacts::Value;
use serde::{Deserialize, Serialize};

/// Term is either a constant value or a variable placeholder
/// Uses the new unified Variable<T> system
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Term {
    /// A concrete value
    Constant(Value),
    /// A variable placeholder (always untyped in Term for simplicity)
    Variable(TypedVariable<crate::variable::Untyped>),
}

impl Term {
    /// Check if this term is a variable
    pub fn is_variable(&self) -> bool {
        matches!(self, Term::Variable(_))
    }

    /// Check if this term is a constant
    pub fn is_constant(&self) -> bool {
        matches!(self, Term::Constant(_))
    }

    /// Get the variable if this term is one
    pub fn as_variable(&self) -> Option<&TypedVariable<crate::variable::Untyped>> {
        match self {
            Term::Variable(var) => Some(var),
            Term::Constant(_) => None,
        }
    }

    /// Get the constant value if this term is one
    pub fn as_constant(&self) -> Option<&Value> {
        match self {
            Term::Constant(value) => Some(value),
            Term::Variable(_) => None,
        }
    }
}

impl From<Value> for Term {
    fn from(value: Value) -> Self {
        Term::Constant(value)
    }
}

// Support for all Variable<T> - convert to untyped for Term
impl<T> From<TypedVariable<T>> for Term
where
    T: crate::variable::IntoValueDataType,
{
    fn from(var: TypedVariable<T>) -> Self {
        // Convert any Variable<T> to Variable<Untyped> for Term
        Term::Variable(TypedVariable::new(var.name().to_string()))
    }
}

impl From<String> for Term {
    fn from(s: String) -> Self {
        Term::Constant(Value::String(s))
    }
}

impl From<&str> for Term {
    fn from(s: &str) -> Self {
        Term::Constant(Value::String(s.to_string()))
    }
}

impl From<dialog_artifacts::Attribute> for Term {
    fn from(attr: dialog_artifacts::Attribute) -> Self {
        Term::Constant(Value::String(attr.to_string()))
    }
}

impl From<dialog_artifacts::Entity> for Term {
    fn from(entity: dialog_artifacts::Entity) -> Self {
        Term::Constant(Value::Entity(entity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_artifacts::Value;

    #[test]
    fn test_term_from_typed_variable() {
        let typed_var = TypedVariable::<String>::new("name");
        let term = Term::from(typed_var.clone());

        assert!(term.is_variable());
        assert!(!term.is_constant());

        if let Some(var) = term.as_variable() {
            assert_eq!(var.name(), "name");
            assert_eq!(var.data_type(), None); // Terms always use untyped variables
        } else {
            panic!("Expected variable term");
        }
    }

    #[test]
    fn test_term_from_untyped_variable() {
        let untyped_var = TypedVariable::<crate::variable::Untyped>::new("anything");
        let term = Term::from(untyped_var);

        assert!(term.is_variable());
        if let Some(var) = term.as_variable() {
            assert_eq!(var.name(), "anything");
            assert_eq!(var.data_type(), None);
        } else {
            panic!("Expected variable term");
        }
    }

    #[test]
    fn test_term_from_value() {
        let value = Value::String("test".to_string());
        let term = Term::from(value.clone());

        assert!(!term.is_variable());
        assert!(term.is_constant());

        if let Some(val) = term.as_constant() {
            assert_eq!(*val, value);
        } else {
            panic!("Expected constant term");
        }
    }

    #[test]
    fn test_new_variable_system_integration() {
        // Test that the new Variable<T> system works with Terms
        let string_var = TypedVariable::<String>::new("name");
        let untyped_var = TypedVariable::<crate::variable::Untyped>::new("anything");

        let string_term = Term::from(string_var);
        let untyped_term = Term::from(untyped_var);

        // Both should be variable terms
        assert!(string_term.is_variable());
        assert!(untyped_term.is_variable());

        // Both should have untyped variables in the Term (for simplicity)
        if let Some(var) = string_term.as_variable() {
            assert_eq!(var.name(), "name");
            assert_eq!(var.data_type(), None);
        }

        if let Some(var) = untyped_term.as_variable() {
            assert_eq!(var.name(), "anything");
            assert_eq!(var.data_type(), None);
        }
    }

    #[test]
    fn test_turbofish_syntax_with_terms() {
        // Test the new turbofish syntax works with Term conversion
        let name_var = TypedVariable::<String>::new("name");
        let age_var = TypedVariable::<u64>::new("age");
        let any_var = TypedVariable::<crate::variable::Untyped>::new("wildcard");

        // Convert to terms
        let name_term = Term::from(name_var);
        let age_term = Term::from(age_var);
        let any_term = Term::from(any_var);

        // All should be variable terms with untyped variables
        assert!(name_term.is_variable());
        assert!(age_term.is_variable());
        assert!(any_term.is_variable());

        // Check names are preserved
        assert_eq!(name_term.as_variable().unwrap().name(), "name");
        assert_eq!(age_term.as_variable().unwrap().name(), "age");
        assert_eq!(any_term.as_variable().unwrap().name(), "wildcard");

        // All should be untyped in Term context
        assert_eq!(name_term.as_variable().unwrap().data_type(), None);
        assert_eq!(age_term.as_variable().unwrap().data_type(), None);
        assert_eq!(any_term.as_variable().unwrap().data_type(), None);
    }
}
