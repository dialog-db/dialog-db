//! Term types for pattern matching and query construction

use crate::variable::{IntoValueDataType, TypedVariable, Untyped};
use dialog_artifacts::Value;
use serde::{Deserialize, Serialize};

/// Term is either a constant value or a variable placeholder
/// Generic over T to represent typed terms
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Term<T>
where
    T: IntoValueDataType,
{
    /// A concrete value of type T
    Constant(T),
    /// A variable placeholder (converted to untyped for compatibility)
    Variable(TypedVariable<T>),
}

impl<T> Term<T>
where
    T: IntoValueDataType,
{
    /// Check if this term is a variable
    pub fn is_variable(&self) -> bool {
        matches!(self, Term::Variable(_))
    }

    /// Check if this term is a constant
    pub fn is_constant(&self) -> bool {
        matches!(self, Term::Constant(_))
    }

    /// Get the variable if this term is one
    pub fn as_variable(&self) -> Option<&TypedVariable<T>> {
        match self {
            Term::Variable(var) => Some(var),
            Term::Constant(_) => None,
        }
    }

    /// Get the constant value if this term is one
    pub fn as_constant(&self) -> Option<&T> {
        match self {
            Term::Constant(value) => Some(value),
            Term::Variable(_) => None,
        }
    }
}

// Support for TypedVariable<T> - create variable terms (convert to untyped)
impl<T> From<TypedVariable<T>> for Term<T>
where
    T: IntoValueDataType,
{
    fn from(var: TypedVariable<T>) -> Self {
        // Convert any TypedVariable<T> to untyped for compatibility
        Term::Variable(var)
    }
}

// Convenience conversions for common types to Term<Value>
impl From<Value> for Term<Value> {
    fn from(value: Value) -> Self {
        Term::Constant(value)
    }
}

impl From<String> for Term<Value> {
    fn from(s: String) -> Self {
        Term::Constant(Value::String(s))
    }
}

impl From<&str> for Term<Value> {
    fn from(s: &str) -> Self {
        Term::Constant(Value::String(s.to_string()))
    }
}

impl From<dialog_artifacts::Attribute> for Term<Value> {
    fn from(attr: dialog_artifacts::Attribute) -> Self {
        Term::Constant(Value::String(attr.to_string()))
    }
}

impl From<dialog_artifacts::Entity> for Term<Value> {
    fn from(entity: dialog_artifacts::Entity) -> Self {
        Term::Constant(Value::Entity(entity))
    }
}

// Additional typed Term conversions
impl From<&str> for Term<dialog_artifacts::Attribute> {
    fn from(s: &str) -> Self {
        Term::Constant(s.parse().unwrap())
    }
}

impl From<String> for Term<dialog_artifacts::Attribute> {
    fn from(s: String) -> Self {
        Term::Constant(s.parse().unwrap())
    }
}

impl From<dialog_artifacts::Attribute> for Term<dialog_artifacts::Attribute> {
    fn from(attr: dialog_artifacts::Attribute) -> Self {
        Term::Constant(attr)
    }
}

impl From<dialog_artifacts::Entity> for Term<dialog_artifacts::Entity> {
    fn from(entity: dialog_artifacts::Entity) -> Self {
        Term::Constant(entity)
    }
}

// Support for converting any TypedVariable<Untyped> to Term<Value> (for backward compatibility)
impl From<TypedVariable<Untyped>> for Term<Value> {
    fn from(var: TypedVariable<Untyped>) -> Self {
        Term::Variable(TypedVariable::<Value>::new(var.name()))
    }
}

// Support for converting specific typed variables to Term<Value>
impl From<TypedVariable<String>> for Term<Value> {
    fn from(var: TypedVariable<String>) -> Self {
        Term::Variable(TypedVariable::<Value>::new(var.name()))
    }
}

impl From<TypedVariable<dialog_artifacts::Entity>> for Term<Value> {
    fn from(var: TypedVariable<dialog_artifacts::Entity>) -> Self {
        Term::Variable(TypedVariable::<Value>::new(var.name()))
    }
}

impl From<TypedVariable<dialog_artifacts::Attribute>> for Term<Value> {
    fn from(var: TypedVariable<dialog_artifacts::Attribute>) -> Self {
        Term::Variable(TypedVariable::<Value>::new(var.name()))
    }
}

// Support for converting TypedVariable to specific typed Terms
impl From<TypedVariable<Untyped>> for Term<dialog_artifacts::Entity> {
    fn from(var: TypedVariable<Untyped>) -> Self {
        Term::Variable(TypedVariable::<dialog_artifacts::Entity>::new(var.name()))
    }
}

impl From<TypedVariable<Untyped>> for Term<dialog_artifacts::Attribute> {
    fn from(var: TypedVariable<Untyped>) -> Self {
        Term::Variable(TypedVariable::<dialog_artifacts::Attribute>::new(
            var.name(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_artifacts::Value;

    #[test]
    fn test_term_from_typed_variable() {
        let typed_var = TypedVariable::<String>::new("name");
        let term: Term<String> = Term::from(typed_var.clone());

        assert!(term.is_variable());
        assert!(!term.is_constant());

        if let Some(var) = term.as_variable() {
            assert_eq!(var.name(), "name");
            assert_eq!(
                var.data_type(),
                Some(dialog_artifacts::ValueDataType::String)
            ); // Term preserves type information
        } else {
            panic!("Expected variable term");
        }
    }

    #[test]
    fn test_term_from_untyped_variable() {
        let untyped_var = TypedVariable::new("anything");
        let term: Term<crate::variable::Untyped> = Term::from(untyped_var);

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
        let term: Term<Value> = Term::from(value.clone());

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

        let string_term: Term<String> = Term::from(string_var);
        let untyped_term: Term<crate::variable::Untyped> = Term::from(untyped_var);

        // Both should be variable terms
        assert!(string_term.is_variable());
        assert!(untyped_term.is_variable());

        // Terms now preserve type information
        if let Some(var) = string_term.as_variable() {
            assert_eq!(var.name(), "name");
            assert_eq!(
                var.data_type(),
                Some(dialog_artifacts::ValueDataType::String)
            );
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

        // Convert to terms - now preserves types
        let name_term: Term<String> = Term::from(name_var);
        let age_term: Term<u64> = Term::from(age_var);
        let any_term: Term<crate::variable::Untyped> = Term::from(any_var);

        // All should be variable terms
        assert!(name_term.is_variable());
        assert!(age_term.is_variable());
        assert!(any_term.is_variable());

        // Check names are preserved
        assert_eq!(name_term.as_variable().unwrap().name(), "name");
        assert_eq!(age_term.as_variable().unwrap().name(), "age");
        assert_eq!(any_term.as_variable().unwrap().name(), "wildcard");

        // Terms now preserve type information
        assert_eq!(
            name_term.as_variable().unwrap().data_type(),
            Some(dialog_artifacts::ValueDataType::String)
        );
        assert_eq!(
            age_term.as_variable().unwrap().data_type(),
            Some(dialog_artifacts::ValueDataType::UnsignedInt)
        );
        assert_eq!(any_term.as_variable().unwrap().data_type(), None);
    }
}
