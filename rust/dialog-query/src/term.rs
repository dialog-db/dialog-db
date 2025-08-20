//! Term types for pattern matching and query construction

use std::fmt;
use std::marker::PhantomData;

use crate::types::IntoValueDataType;
use dialog_artifacts::{Value, ValueDataType};
use serde::{Deserialize, Serialize};

/// Term is either a constant value or a variable placeholder
/// Generic over T to represent typed terms
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Term<T>
where
    T: IntoValueDataType + Clone,
{
    /// A concrete value of type T
    Constant(T),
    /// A typed variable placeholder with zero-cost type safety
    TypedVariable(String, PhantomData<T>),
    /// Wildcard that matches any value
    Any,
}

impl<T> Term<T>
where
    T: IntoValueDataType + Clone,
{
    pub fn var<N: Into<String>>(name: N) -> Self {
        Term::TypedVariable(name.into(), PhantomData)
    }

    pub fn any() -> Self {
        Term::Any
    }
    /// Check if this term is a variable
    pub fn is_variable(&self) -> bool {
        matches!(self, Term::TypedVariable(_, _))
    }

    /// Check if this term is a constant
    pub fn is_constant(&self) -> bool {
        matches!(self, Term::Constant(_))
    }

    /// Check if this term is the wildcard Any
    pub fn is_any(&self) -> bool {
        matches!(self, Term::Any)
    }

    /// Get the variable name if this is a variable term
    pub fn name(&self) -> Option<&str> {
        match self {
            Term::TypedVariable(name, _) => Some(name),
            _ => None,
        }
    }

    /// Get the data type if this is a variable term
    pub fn data_type(&self) -> Option<ValueDataType> {
        match self {
            Term::TypedVariable(_, _) => T::into_value_data_type(),
            _ => None,
        }
    }

    /// Check if this term can unify with the given value
    pub fn can_unify_with(&self, value: &Value) -> bool {
        match self {
            Term::TypedVariable(_, _) => {
                // For typed variables, check if the value matches the type
                if let Some(var_type) = T::into_value_data_type() {
                    let value_type = ValueDataType::from(value);
                    value_type == var_type
                } else {
                    true // Untyped can unify with anything
                }
            }
            Term::Constant(_) => {
                // For constants, we can't easily compare without knowing if T: Into<Value>
                // For now, return true to maintain compatibility
                true
            }
            Term::Any => true, // Any can unify with anything
        }
    }

    /// Get the variable name if this term is a variable
    pub fn as_variable_name(&self) -> Option<&str> {
        match self {
            Term::TypedVariable(name, _) => Some(name),
            Term::Constant(_) | Term::Any => None,
        }
    }

    /// Get the constant value if this term is one
    pub fn as_constant(&self) -> Option<&T> {
        match self {
            Term::Constant(value) => Some(value),
            Term::TypedVariable(_, _) | Term::Any => None,
        }
    }
}

trait TermContent: IntoValueDataType + Clone {}

// Implement TermContent for all relevant types
impl TermContent for String {}
impl TermContent for bool {}
impl TermContent for u128 {}
impl TermContent for u64 {}
impl TermContent for u32 {}
impl TermContent for u16 {}
impl TermContent for u8 {}
impl TermContent for i128 {}
impl TermContent for i64 {}
impl TermContent for i32 {}
impl TermContent for i16 {}
impl TermContent for i8 {}
impl TermContent for f64 {}
impl TermContent for f32 {}
impl TermContent for Vec<u8> {}
impl TermContent for dialog_artifacts::Entity {}
impl TermContent for dialog_artifacts::Attribute {}
impl TermContent for Value {}
impl TermContent for crate::types::Untyped {}

// Display implementation for Terms
impl<T> fmt::Display for Term<T>
where
    T: IntoValueDataType + Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Constant(value) => write!(f, "{:?}", value),
            Term::TypedVariable(name, _) => {
                if let Some(data_type) = T::into_value_data_type() {
                    write!(f, "?{}<{:?}>", name, data_type)
                } else {
                    write!(f, "?{}", name)
                }
            }
            Term::Any => write!(f, "_"),
        }
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

// From implementations for convenient Term creation from values
impl From<String> for Term<String> {
    fn from(value: String) -> Self {
        Term::Constant(value)
    }
}

impl From<&str> for Term<String> {
    fn from(value: &str) -> Self {
        Term::Constant(value.to_string())
    }
}

impl From<u32> for Term<u32> {
    fn from(value: u32) -> Self {
        Term::Constant(value)
    }
}

impl From<i32> for Term<i32> {
    fn from(value: i32) -> Self {
        Term::Constant(value)
    }
}

impl From<i64> for Term<i64> {
    fn from(value: i64) -> Self {
        Term::Constant(value)
    }
}

impl From<bool> for Term<bool> {
    fn from(value: bool) -> Self {
        Term::Constant(value)
    }
}

impl From<f32> for Term<f32> {
    fn from(value: f32) -> Self {
        Term::Constant(value)
    }
}

impl From<f64> for Term<f64> {
    fn from(value: f64) -> Self {
        Term::Constant(value)
    }
}

impl From<Vec<u8>> for Term<Vec<u8>> {
    fn from(value: Vec<u8>) -> Self {
        Term::Constant(value)
    }
}

// Support for converting Term references to owned Terms
impl<T> From<&Term<T>> for Term<T>
where
    T: IntoValueDataType + Clone,
{
    fn from(term: &Term<T>) -> Self {
        term.clone()
    }
}

// Support for converting TypedVariable to Term
impl<T> From<crate::variable::TypedVariable<T>> for Term<T>
where
    T: IntoValueDataType + Clone,
{
    fn from(var: crate::variable::TypedVariable<T>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

impl<T> From<&crate::variable::TypedVariable<T>> for Term<T>
where
    T: IntoValueDataType + Clone,
{
    fn from(var: &crate::variable::TypedVariable<T>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

// Support for converting specific TypedVariables to Term<Value> (cross-type conversion)
impl From<crate::variable::TypedVariable<String>> for Term<Value> {
    fn from(var: crate::variable::TypedVariable<String>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

impl From<&crate::variable::TypedVariable<String>> for Term<Value> {
    fn from(var: &crate::variable::TypedVariable<String>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

impl From<crate::variable::TypedVariable<crate::types::Untyped>> for Term<Value> {
    fn from(var: crate::variable::TypedVariable<crate::types::Untyped>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

impl From<&crate::variable::TypedVariable<crate::types::Untyped>> for Term<Value> {
    fn from(var: &crate::variable::TypedVariable<crate::types::Untyped>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

// Support for converting untyped variables to any Term type
impl From<crate::variable::TypedVariable<crate::types::Untyped>> for Term<dialog_artifacts::Entity> {
    fn from(var: crate::variable::TypedVariable<crate::types::Untyped>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

impl From<&crate::variable::TypedVariable<crate::types::Untyped>> for Term<dialog_artifacts::Entity> {
    fn from(var: &crate::variable::TypedVariable<crate::types::Untyped>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

impl From<crate::variable::TypedVariable<crate::types::Untyped>> for Term<dialog_artifacts::Attribute> {
    fn from(var: crate::variable::TypedVariable<crate::types::Untyped>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

impl From<&crate::variable::TypedVariable<crate::types::Untyped>> for Term<dialog_artifacts::Attribute> {
    fn from(var: &crate::variable::TypedVariable<crate::types::Untyped>) -> Self {
        Term::TypedVariable(var.name().to_string(), PhantomData)
    }
}

// Support for converting specific typed Terms to Value Terms
impl From<Term<String>> for Term<Value> {
    fn from(term: Term<String>) -> Self {
        match term {
            Term::Constant(value) => Term::Constant(Value::String(value)),
            Term::TypedVariable(name, _) => Term::TypedVariable(name, PhantomData),
            Term::Any => Term::Any,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_artifacts::Value;
    use crate::variable::TypedVariable;

    #[test]
    fn test_term_from_typed_variable() {
        let typed_var = TypedVariable::<String>::new("name");
        let term: Term<String> = Term::from(typed_var.clone());

        assert!(term.is_variable());
        assert!(!term.is_constant());

        // Use the direct Term methods instead of as_variable (which is legacy)
        assert_eq!(term.name(), Some("name"));
        assert_eq!(
            term.data_type(),
            Some(dialog_artifacts::ValueDataType::String)
        ); // Term preserves type information
    }

    #[test]
    fn test_term_from_untyped_variable() {
        let untyped_var = TypedVariable::<Value>::new("anything");
        let term: Term<Value> = Term::from(untyped_var);

        assert!(term.is_variable());
        assert_eq!(term.name(), Some("anything"));
        assert_eq!(term.data_type(), None);
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
        let untyped_var = TypedVariable::<Value>::new("anything");

        let string_term: Term<String> = Term::from(string_var);
        let untyped_term: Term<Value> = Term::from(untyped_var);

        // Both should be variable terms
        assert!(string_term.is_variable());
        assert!(untyped_term.is_variable());

        // Terms now preserve type information using direct methods
        assert_eq!(string_term.name(), Some("name"));
        assert_eq!(
            string_term.data_type(),
            Some(dialog_artifacts::ValueDataType::String)
        );

        assert_eq!(untyped_term.name(), Some("anything"));
        assert_eq!(untyped_term.data_type(), None);
    }

    #[test]
    fn test_turbofish_syntax_with_terms() {
        // Test the new turbofish syntax works with Term conversion
        let name_var = TypedVariable::<String>::new("name");
        let age_var = TypedVariable::<u64>::new("age");
        let any_var = TypedVariable::<Value>::new("wildcard");

        // Convert to terms - now preserves types
        let name_term: Term<String> = Term::from(name_var);
        let age_term: Term<u64> = Term::from(age_var);
        let any_term: Term<Value> = Term::from(any_var);

        // All should be variable terms
        assert!(name_term.is_variable());
        assert!(age_term.is_variable());
        assert!(any_term.is_variable());

        // Check names are preserved
        assert_eq!(name_term.name(), Some("name"));
        assert_eq!(age_term.name(), Some("age"));
        assert_eq!(any_term.name(), Some("wildcard"));

        // Terms now preserve type information
        assert_eq!(
            name_term.data_type(),
            Some(dialog_artifacts::ValueDataType::String)
        );
        assert_eq!(
            age_term.data_type(),
            Some(dialog_artifacts::ValueDataType::UnsignedInt)
        );
        assert_eq!(any_term.data_type(), None);
    }

    #[test]
    fn test_term_from_implementations() {
        // Test String conversions
        let term1: Term<String> = "hello".into();
        let term2: Term<String> = "world".to_string().into();

        assert!(term1.is_constant());
        assert!(term2.is_constant());

        if let Term::Constant(s) = term1 {
            assert_eq!(s, "hello");
        } else {
            panic!("Expected constant string");
        }

        // Test numeric conversions
        let age_term: Term<u32> = 25u32.into();
        let score_term: Term<f64> = 3.14f64.into();
        let active_term: Term<bool> = true.into();

        assert!(age_term.is_constant());
        assert!(score_term.is_constant());
        assert!(active_term.is_constant());

        // Test that From implementations create constants, not variables
        match age_term {
            Term::Constant(n) => assert_eq!(n, 25u32),
            _ => panic!("Expected constant u32"),
        }

        match score_term {
            Term::Constant(f) => assert_eq!(f, 3.14f64),
            _ => panic!("Expected constant f64"),
        }

        match active_term {
            Term::Constant(b) => assert_eq!(b, true),
            _ => panic!("Expected constant bool"),
        }
    }

    #[test]
    fn test_term_from_variable_reference() {
        // Test that we can convert variable references to terms
        let entity_var = TypedVariable::<dialog_artifacts::Entity>::new("entity");
        let string_var = TypedVariable::<String>::new("name");

        // This should work with references (the new implementation)
        let entity_term: Term<dialog_artifacts::Entity> = (&entity_var).into();
        let string_term: Term<String> = (&string_var).into();

        // Both should be variable terms
        assert!(entity_term.is_variable());
        assert!(string_term.is_variable());

        // Check that variable names are preserved
        assert_eq!(entity_term.name(), Some("entity"));
        assert_eq!(
            entity_term.data_type(),
            Some(dialog_artifacts::ValueDataType::Entity)
        );

        assert_eq!(string_term.name(), Some("name"));
        assert_eq!(
            string_term.data_type(),
            Some(dialog_artifacts::ValueDataType::String)
        );
    }
}
