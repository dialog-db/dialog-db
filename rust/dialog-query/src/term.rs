//! Term types for pattern matching and query construction
//!
//! This module implements the core `Term<T>` type that represents either:
//! - **Variables**: Placeholders that can match any value of type T
//! - **Constants**: Concrete values of type T
//! - **Any**: Wildcard that matches any value regardless of type
//!
//! The key insight is using `TermSyntax` as an intermediate representation for JSON
//! serialization/deserialization, which allows clean separation between the API
//! (`Term<T>`) and the JSON format (`TermSyntax<T>`).

use std::fmt;
use std::marker::PhantomData;

use crate::types::IntoValueDataType;
use dialog_artifacts::{Value, ValueDataType};
use serde::{Deserialize, Serialize};

/// Term represents either a constant value or variable placeholder
///
/// This is the main API type used throughout the dialog-query system.
/// Generic over T to represent typed terms (e.g., Term<String>, Term<Value>).
///
/// # JSON Serialization
/// Terms serialize to different JSON formats:
/// - Named variables: `{ "?": { "name": "var_name", "type": "String" } }`
/// - Untyped variables (Term<Value>): `{ "?": { "name": "var_name" } }`
/// - Unnamed variables (old Any): `{ "?": {} }`
/// - Constants: Plain JSON values (e.g., `"Alice"`, `42`, `true`)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Term<T>
where
    T: IntoValueDataType + Clone + 'static,
{
    /// A variable with optional name and type information
    /// Variables with name: None don't produce bindings but still match (replaces Any)
    /// The PhantomData<T> carries the type information at compile time
    #[serde(rename = "?")]
    Variable {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(rename = "type", skip_serializing_if = "Type::<T>::is_any")]
        _type: Type<T>,
    },

    /// A concrete value of type T
    /// For Term<Value>, serializes as plain JSON (e.g., "Alice", 42, true)
    /// For other types, uses normal serde serialization
    #[serde(untagged)]
    Constant(T),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "Option<ValueDataType>")]
pub struct Type<T: IntoValueDataType + Clone + 'static>(PhantomData<T>);
impl<T: IntoValueDataType + Clone + 'static> Type<T> {
    fn is_any(&self) -> bool {
        T::into_value_data_type().is_none()
    }
}

impl<T> From<Type<T>> for Option<ValueDataType>
where
    T: IntoValueDataType + Clone + 'static,
{
    fn from(_value: Type<T>) -> Self {
        T::into_value_data_type()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum TermSyntax<T>
where
    T: IntoValueDataType + Clone + 'static,
{
    #[serde(rename = "?")]
    Variable {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
        _type: Option<ValueDataType>,
    },

    /// A concrete value of type T
    /// For Term<Value>, serializes as plain JSON (e.g., "Alice", 42, true)
    /// For other types, uses normal serde serialization
    #[serde(untagged)]
    Constant(T),
}

impl<T> From<Term<T>> for TermSyntax<T>
where
    T: IntoValueDataType + Clone + 'static,
{
    fn from(value: Term<T>) -> Self {
        match value {
            Term::Variable { name, _type } => TermSyntax::Variable {
                name,
                _type: T::into_value_data_type(),
            },
            Term::Constant(value) => TermSyntax::Constant(value),
        }
    }
}

/// Core functionality implementation for Term<T>
///
/// Provides constructor methods and introspection capabilities.
impl<T> Term<T>
where
    T: IntoValueDataType + Clone,
{
    pub fn new() -> Self {
        Term::Variable {
            name: None,
            _type: Type(PhantomData),
        }
    }

    /// Create a new typed variable with the given name
    ///
    /// The type T is carried via PhantomData and used for type information
    /// during serialization and type checking.
    pub fn var<N: Into<String>>(name: N) -> Self {
        Term::Variable {
            name: Some(name.into()),
            _type: Type(PhantomData),
        }
    }

    /// Create an unnamed variable term that matches any value without binding
    ///
    /// Replaces the old Any variant. These variables match anything but don't
    /// produce bindings. Serializes as `{ "?": {} }` in JSON
    pub fn any() -> Self {
        Term::Variable {
            name: None,
            _type: Type(PhantomData),
        }
    }

    /// Check if this term is a variable (named or unnamed)
    pub fn is_variable(&self) -> bool {
        matches!(self, Term::Variable { .. })
    }

    /// Check if this term is a constant value
    pub fn is_constant(&self) -> bool {
        matches!(self, Term::Constant(_))
    }

    /// Check if this term is an unnamed variable (old Any behavior)
    ///
    /// Unnamed variables match anything but don't produce bindings
    pub fn is_any(&self) -> bool {
        matches!(self, Term::Variable { name: None, .. })
    }

    /// Get the variable name if this is a named variable term
    ///
    /// Returns None for constants and unnamed variables
    pub fn name(&self) -> Option<&str> {
        match self {
            Term::Variable {
                name: Some(name), ..
            } => Some(name),
            _ => None,
        }
    }

    /// Get the data type for this term's type parameter T
    ///
    /// Returns Some(ValueDataType) for typed variables, None for Value type
    /// (since Value can hold any type). Always returns None for constants.
    pub fn data_type(&self) -> Option<ValueDataType> {
        match self {
            Term::Variable { .. } => T::into_value_data_type(),
            _ => None,
        }
    }

    /// Check if this term can unify with the given value
    ///
    /// Used during pattern matching to determine if a term can be bound to a value:
    /// - Variables: Check if value's type matches the variable's type (if typed)
    /// - Constants: Always return true (compatibility - actual comparison needs value conversion)
    pub fn can_unify_with(&self, value: &Value) -> bool {
        match self {
            Term::Variable { .. } => {
                // For typed variables, check if the value matches the expected type
                if let Some(var_type) = T::into_value_data_type() {
                    let value_type = ValueDataType::from(value);
                    value_type == var_type
                } else {
                    // Untyped variables (like Term<Value>) can unify with anything
                    true
                }
            }
            Term::Constant(_) => {
                // For constants, we can't easily compare without knowing if T: Into<Value>
                // Return true to maintain compatibility - actual equality should be checked elsewhere
                true
            }
        }
    }

    /// Get the variable name if this term is a variable
    ///
    /// Alias for name() method - kept for backward compatibility
    pub fn as_variable_name(&self) -> Option<&str> {
        match self {
            Term::Variable {
                name: Some(name), ..
            } => Some(name),
            _ => None,
        }
    }

    /// Get the constant value if this term is a constant
    ///
    /// Returns None for variables
    pub fn as_constant(&self) -> Option<&T> {
        match self {
            Term::Constant(value) => Some(value),
            Term::Variable { .. } => None,
        }
    }

    /// Builder method for fluent API (placeholder implementation)
    ///
    /// Currently returns self unchanged - may be expanded for query building
    pub fn is<Is: Into<Term<T>>>(self, _other: Is) -> Self {
        self
    }
}

/// Display implementation for Terms
///
/// Provides human-readable representation:
/// - Constants: Debug format of the value
/// - Named variables: ?name<Type> format (or ?name for untyped)
/// - Unnamed variables: _ (underscore)
///
impl<T> fmt::Display for Term<T>
where
    T: IntoValueDataType + Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Constants display as their debug representation
            Term::Constant(value) => write!(f, "{:?}", value),
            Term::Variable {
                name: Some(name), ..
            } => {
                // Named variables show type information, untyped don't
                if let Some(data_type) = T::into_value_data_type() {
                    write!(f, "?{}<{:?}>", name, data_type)
                } else {
                    write!(f, "?{}", name)
                }
            }
            // Unnamed variables display as underscore
            Term::Variable { name: None, .. } => write!(f, "_"),
        }
    }
}

/// Convenience conversions for common types to Term<Value>
///
/// These From implementations allow easy creation of Term<Value> constants
/// from various types, automatically wrapping them in the appropriate Value variant.
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

/// Additional typed Term conversions for dialog-artifacts types
///
/// These allow direct conversion from artifact types to their corresponding Terms.
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

/// From implementations for convenient Term creation from primitive values
///
/// These allow direct conversion from Rust primitives to their corresponding
/// typed Terms (e.g., String -> Term<String>).
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

/// Support for converting Term references to owned Terms
///
/// Allows cloning Terms when you have a reference but need an owned value.
impl<T> From<&Term<T>> for Term<T>
where
    T: IntoValueDataType + Clone,
{
    fn from(term: &Term<T>) -> Self {
        term.clone()
    }
}

/// Support for converting specific typed Terms to Value Terms
///
/// This conversion preserves the Term structure while changing the value type
/// from a specific type (like String) to the general Value enum.
impl From<Term<String>> for Term<Value> {
    fn from(term: Term<String>) -> Self {
        match term {
            Term::Constant(value) => Term::Constant(Value::String(value)),
            Term::Variable { name, .. } => Term::Variable {
                name,
                _type: Type(PhantomData),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_integration() {
        let any = Term::<Value>::new();
        assert_eq!(serde_json::to_string(&any).unwrap(), r#"{"?":{}}"#);

        let string = Term::<String>::new();
        assert_eq!(
            serde_json::to_string(&string).unwrap(),
            r#"{"?":{"type":"String"}}"#
        );

        let title = Term::<String>::var("title");
        assert_eq!(
            serde_json::to_string(&title).unwrap(),
            r#"{"?":{"name":"title","type":"String"}}"#
        );

        let _title = Term::<Value>::var("title");
        assert_eq!(
            serde_json::to_string(&_title).unwrap(),
            r#"{"?":{"name":"title"}}"#
        );
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
        let string_var = Term::<String>::var("name");
        let untyped_var = Term::<Value>::var("anything");

        let string_term = string_var; // Already a Term<String>
        let untyped_term = untyped_var; // Already a Term<Value>

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
        let name_var = Term::<String>::var("name");
        let age_var = Term::<u64>::var("age");
        let any_var = Term::<Value>::var("wildcard");

        // Convert to terms - now preserves types
        let name_term = name_var; // Already a Term<String>
        let age_term = age_var; // Already a Term<u64>
        let any_term = any_var; // Already a Term<Value>

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
        let entity_var = Term::<dialog_artifacts::Entity>::var("entity");
        let string_var = Term::<String>::var("name");

        // This should work with the new implementation
        let entity_term = entity_var.clone(); // Clone the Term
        let string_term = string_var.clone(); // Clone the Term

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
