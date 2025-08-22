//! Term types for pattern matching and query construction

use std::any::TypeId;
use std::fmt;
use std::marker::PhantomData;

use crate::types::IntoValueDataType;
use dialog_artifacts::{Value, ValueDataType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};


/// Term is either a constant value or a variable placeholder
/// Generic over T to represent typed terms
#[derive(Debug, Clone, PartialEq)]
pub enum Term<T>
where
    T: IntoValueDataType + Clone,
{
    /// A named variable with type information
    TypedVariable(String, PhantomData<T>),
    /// Wildcard that matches any value - serializes as { "?": {} }
    Any,
    /// A concrete value of type T
    Constant(T),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
enum VariableSyntax {
    Variable {
        name: String,
        #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
        _type: Option<ValueDataType>,
    },
    Any {},
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum TermSyntax<T> {
    #[serde(rename = "?")]
    Variable(VariableSyntax),
    #[serde(untagged)]
    Constant(T),
}

#[test]
fn test_term_syntax() {
    let syntax: TermSyntax<Vec<u8>> = TermSyntax::Variable(VariableSyntax::Any {});
    let serialized = serde_json::to_string(&syntax).unwrap();
    assert_eq!(serialized, r#"{"?":{}}"#);

    let var: TermSyntax<i32> = TermSyntax::Variable(VariableSyntax::Variable {
        name: "x".to_string(),
        _type: None,
    });
    let serialized = serde_json::to_string(&var).unwrap();
    assert_eq!(serialized, r#"{"?":{"name":"x"}}"#);

    let parse_any: TermSyntax<i32> = serde_json::from_str(r#"{"?": {}}"#).unwrap();
    assert_eq!(parse_any, TermSyntax::Variable(VariableSyntax::Any {}));

    let parse_var: TermSyntax<i32> = serde_json::from_str(r#"{"?": {"name": "x"}}"#).unwrap();
    assert_eq!(
        parse_var,
        TermSyntax::Variable(VariableSyntax::Variable {
            name: "x".to_string(),
            _type: None,
        })
    );

    let constant = TermSyntax::Constant(42);
    let serialized = serde_json::to_string(&constant).unwrap();
    assert_eq!(serialized, r#"42"#);

    let parse_constant: TermSyntax<u32> = serde_json::from_str(r#"42"#).unwrap();
    assert_eq!(parse_constant, TermSyntax::Constant(42));

    let parse_typed_var: TermSyntax<i32> =
        serde_json::from_str(r#"{"?": {"name": "x", "type": "SignedInt"}}"#).unwrap();
    assert_eq!(
        parse_typed_var,
        TermSyntax::Variable(VariableSyntax::Variable {
            name: "x".to_string(),
            _type: Some(ValueDataType::SignedInt),
        })
    );
}

// Convert between Term and TermSyntax
impl<T> From<Term<T>> for TermSyntax<T>
where
    T: IntoValueDataType + Clone + 'static,
{
    fn from(term: Term<T>) -> Self {
        match term {
            Term::TypedVariable(name, _) => {
                // For Value type, we don't include type information (untyped variable)
                let _type = if TypeId::of::<T>() == TypeId::of::<Value>() {
                    None
                } else {
                    T::into_value_data_type()
                };
                
                TermSyntax::Variable(VariableSyntax::Variable { name, _type })
            }
            Term::Any => TermSyntax::Variable(VariableSyntax::Any {}),
            Term::Constant(value) => TermSyntax::Constant(value),
        }
    }
}

impl<T> From<TermSyntax<T>> for Term<T>
where
    T: IntoValueDataType + Clone,
{
    fn from(syntax: TermSyntax<T>) -> Self {
        match syntax {
            TermSyntax::Variable(VariableSyntax::Any {}) => Term::Any,
            TermSyntax::Variable(VariableSyntax::Variable { name, .. }) => {
                // Type information is carried by T, not by the syntax
                Term::TypedVariable(name, PhantomData)
            }
            TermSyntax::Constant(value) => Term::Constant(value),
        }
    }
}

// Implement Serialize using the intermediate format
impl<T> Serialize for Term<T>
where
    T: IntoValueDataType + Clone + Serialize + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let syntax: TermSyntax<T> = self.clone().into();
        syntax.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Term<T>
where
    T: IntoValueDataType + Clone + Deserialize<'de> + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let syntax = TermSyntax::<T>::deserialize(deserializer)?;
        Ok(Term::from(syntax))
    }
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
            _ => None,
        }
    }

    /// Get the constant value if this term is one
    pub fn as_constant(&self) -> Option<&T> {
        match self {
            Term::Constant(value) => Some(value),
            Term::TypedVariable(_, _) | Term::Any => None,
        }
    }

    pub fn is<Is: Into<Term<T>>>(self, _other: Is) -> Self {
        self
    }
}

// TermContent trait removed - no longer needed with variable module elimination

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

// TODO: Phase 3 - TypedVariable From implementations removed as part of variable module elimination
// The functionality is preserved through Term::var() constructor

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
