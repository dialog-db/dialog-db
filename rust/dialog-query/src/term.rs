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

use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;

use crate::artifact::{Attribute, Entity, Type, Value};
use crate::types::{IntoType, Scalar};
use crate::InconsistencyError;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub enum Constraint {
//     /// Matches this specific value.
//     Is(Value),
//     /// Has this exact type.
//     Type(Type),
//     /// Numeric value that is greater than the given value.
//     GreaterThan(Value),
//     /// Numeric value that is less than the given value.
//     LessThan(Value),
//     /// Numeric value that is greater than or equal to the given value.
//     GreaterOrEqual(Value),
//     /// Numeric value that is less than or equal to the given value.
//     LessOrEqual(Value),
//     /// Value in this set.
//     In(HashSet<Vec<Value>>),

//     /// Value that is within the given range inclusive start of the range and
//     /// exclusive the end. If end is None, it means the range is unbounded.
//     Range { start: Value, end: Option<Value> },
// }

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct In(HashSet<Value>);
// impl Hash for In {
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         let mut elements: Vec<&Value> = self.0.iter().collect();
//         elements.sort();
//         elements.hash(state);
//     }
// }

// // impl Constraint {
// //     /// Attempt to merge two constraints, tightening their range of possible values.
// //     /// Returns `Ok(Some(...))` if merged into a single constraint,
// //     /// `Ok(None)` if they are unrelated (should coexist),
// //     /// or `Err(InconsistencyError)` if they are contradictory.
// //     pub fn merge(&self, other: &Constraint) -> Result<Option<Constraint>, InconsistencyError> {
// //         use Constraint::*;

// //         match (self, other) {
// //             // Identical constraints
// //             (a, b) if a == b => Ok(Some(a.into())),

// //             // Both are equality
// //             (Is(v1), Is(v2)) => {
// //                 if v1 == v2 {
// //                     Ok(Some(Is(v1.clone())))
// //                 } else {
// //                     Err(InconsistencyError)
// //                 }
// //             }

// //             // Equality with inequality
// //             (Is(v), GreaterThan(g)) | (GreaterThan(g), Is(v)) => {
// //                 if v > g {
// //                     Ok(Some(Is(v.clone())))
// //                 } else {
// //                     Err(InconsistencyError)
// //                 }
// //             }

// //             (Is(v), LessThan(l)) | (LessThan(l), Is(v)) => {
// //                 if v < l {
// //                     Ok(Some(Is(v.clone())))
// //                 } else {
// //                     Err(InconsistencyError)
// //                 }
// //             }

// //             (Is(v), In(set)) | (In(set), Is(v)) => {
// //                 if set.contains(v) {
// //                     Ok(Some(Is(v.clone())))
// //                 } else {
// //                     Err(InconsistencyError)
// //                 }
// //             }

// //             (GreaterThan(g1), GreaterThan(g2)) => {
// //                 Ok(Some(GreaterThan(std::cmp::max(g1.clone(), g2.clone()))))
// //             }

// //             (LessThan(l1), LessThan(l2)) => {
// //                 Ok(Some(LessThan(std::cmp::min(l1.clone(), l2.clone()))))
// //             }

// //             (GreaterOrEqual(g1), GreaterOrEqual(g2)) => {
// //                 Ok(Some(GreaterOrEqual(std::cmp::max(g1.clone(), g2.clone()))))
// //             }

// //             (LessOrEqual(l1), LessOrEqual(l2)) => {
// //                 Ok(Some(LessOrEqual(std::cmp::min(l1.clone(), l2.clone()))))
// //             }

// //             (GreaterThan(g), LessThan(l)) | (LessThan(l), GreaterThan(g)) => {
// //                 if g >= l {
// //                     Err(InconsistencyError)
// //                 } else {
// //                     Ok(Some(Range {
// //                         start: g.clone(),
// //                         end: Some(l.clone()),
// //                     }))
// //                 }
// //             }

// //             (Range { start: s1, end: e1 }, Range { start: s2, end: e2 }) => {
// //                 let start = std::cmp::max(s1.clone(), s2.clone());
// //                 let end = match (e1, e2) {
// //                     (Some(e1), Some(e2)) => Some(std::cmp::min(e1.clone(), e2.clone())),
// //                     (Some(e1), None) => Some(e1.clone()),
// //                     (None, Some(e2)) => Some(e2.clone()),
// //                     (None, None) => None,
// //                 };
// //                 if let Some(ref e) = end {
// //                     if start >= *e {
// //                         return Err(InconsistencyError);
// //                     }
// //                 }
// //                 Ok(Some(Range { start, end }))
// //             }

// //             (In(set1), In(set2)) => {
// //                 let intersection: HashSet<_> = set1.intersection(set2).cloned().collect();
// //                 if intersection.is_empty() {
// //                     Err(InconsistencyError)
// //                 } else {
// //                     Ok(Some(In(intersection)))
// //                 }
// //             }

// //             // Type constraints only merge if they match
// //             (Type(t1), Type(t2)) => {
// //                 if t1 == t2 {
// //                     Ok(Some(Type(t1.clone())))
// //                 } else {
// //                     Err(InconsistencyError)
// //                 }
// //             }

// //             // For simplicity — unknown or unrelated combinations just coexist
// //             _ => Ok(None),
// //         }
// //     }
// // }

// pub struct Constraints(HashSet<Constraint>);

// impl Constraints {
//     pub fn merge(&mut self, other: &Constraints) -> Result<(), InconsistencyError> {
//         panic!("Not implemented")
//     }
// }

/// Term represents either a constant value or variable constraint of the
/// predicate.
///
/// This is the main API type used throughout the dialog-query system.
/// Generic over T to represent typed terms (e.g., Term<String>, Term<Value>).
///
/// # JSON Serialization
/// Terms serialize to different JSON formats:
/// - Named typed variable: `Term<String>`: `{ "?": { "name": "var_name", "type": "String" } }`
/// - Named untyped variable `Term<Value>`: `{ "?": { "name": "var_name" } }`
/// - Anonymous typed variable `Term<String>`: `{ "?": { "type": "String" } }`
/// - Anonymous untyped variable `Term<Value>`: `{ "?": {} }`
/// - Constants: Plain JSON values (e.g., `"Alice"`, `42`, `true`)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum Term<T>
where
    T: IntoType + Clone + 'static,
{
    /// A variable term can be used as matching term across conjuncts in the
    /// predicate. If variable has name it acts as an implicit join across
    /// conjuncts. If variable has type other than `Value`, it acts as a type
    /// constraint.
    ///
    /// Two variables with the same name and different types in the same
    /// predicate will fail to unify will fail to match anything.
    #[serde(rename = "?")]
    Variable {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(
            rename = "type",
            skip_serializing_if = "ContentType::<T>::is_any",
            default = "ContentType::<T>::default"
        )]
        content_type: ContentType<T>,
    },

    /// A concrete value of type T
    /// For Term<Value>, serializes as plain JSON (e.g., "Alice", 42, true)
    /// For other types, uses normal serde serialization
    #[serde(untagged)]
    Constant(T),
}

/// Wrapper around PhantomData<T> with additional functionality so it can
/// be converted to and from Option<Type>.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(into = "Option<Type>", from = "Option<Type>")]
pub struct ContentType<T: IntoType + Clone + 'static>(PhantomData<T>);

impl<T: IntoType + Clone + 'static> Default for ContentType<T> {
    fn default() -> Self {
        ContentType(PhantomData)
    }
}

// impl<T: IntoType + Clone + 'static> From<PhantomData<T>> for Type {
//     fn from(_value: PhantomData<T>) -> Self {
//         T::into_type()
//     }
// }

impl<T: IntoType + Clone + 'static> ContentType<T> {
    /// Returns true if `T` is `Value` as it can represent all supported data
    /// types.
    fn is_any(&self) -> bool {
        T::TYPE.is_none()
    }
}

impl<T> From<ContentType<T>> for Option<Type>
where
    T: IntoType + Clone + 'static,
{
    fn from(_value: ContentType<T>) -> Self {
        T::TYPE
    }
}
impl<T> From<Option<Type>> for ContentType<T>
where
    T: IntoType + Clone + 'static,
{
    fn from(_value: Option<Type>) -> Self {
        ContentType(PhantomData)
    }
}

/// Core functionality implementation for Term<T>
///
/// Provides constructor methods and introspection capabilities.
impl<T> Term<T>
where
    T: Scalar,
{
    /// Create a new typed variable with the given name
    ///
    /// The type T is carried via PhantomData and used for type information
    /// during serialization and type checking.
    pub fn var<N: Into<String>>(name: N) -> Self {
        Term::Variable {
            name: Some(name.into()),
            content_type: ContentType(PhantomData),
        }
    }

    /// Create an anonymous variable that only used to pattern match by type
    /// unless type is `Value`. If type is `Value`, it simply matches anything.
    ///
    /// Unlike other variables, it does not performs join across conjuncts.
    pub fn blank() -> Self {
        Self::default()
    }

    /// Check if this term is a variable (named or unnamed)
    pub fn is_variable(&self) -> bool {
        matches!(self, Term::Variable { .. })
    }

    pub fn is_named_variable(&self) -> bool {
        matches!(self, Term::Variable { name: Some(_), .. })
    }

    /// Check if this term is a constant value
    pub fn is_constant(&self) -> bool {
        matches!(self, Term::Constant(_))
    }

    /// Check if this term is an unnamed variable (old Any behavior)
    ///
    /// Unnamed variables match anything but don't produce bindings
    pub fn is_blank(&self) -> bool {
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
    /// Returns Some(Type) for typed variables, None for Value type
    /// (since Value can hold any type). Always returns None for constants.
    pub fn content_type(&self) -> Option<Type> {
        match self {
            Term::Variable { .. } => T::TYPE,
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
                if let Some(var_type) = T::TYPE {
                    let value_type = Type::from(value);
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

    pub fn as_unknown(&self) -> Term<Value> {
        match self {
            Term::Constant(value) => Term::Constant(value.as_value()),
            Term::Variable {
                name,
                content_type: _type,
            } => Term::Variable {
                name: name.clone(),
                content_type: ContentType::default(),
            },
        }
    }
}

impl<T> Default for Term<T>
where
    T: IntoType + Clone,
{
    fn default() -> Self {
        Term::Variable {
            name: None,
            content_type: ContentType::default(),
        }
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
    T: IntoType + Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Constants display as their debug representation
            Term::Constant(value) => write!(f, "{:?}", value),
            Term::Variable {
                name: Some(name), ..
            } => {
                // Named variables show type information, untyped don't
                if let Some(data_type) = T::TYPE {
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

impl From<Attribute> for Term<Value> {
    fn from(attr: Attribute) -> Self {
        Term::Constant(Value::String(attr.to_string()))
    }
}

impl From<Entity> for Term<Value> {
    fn from(entity: crate::artifact::Entity) -> Self {
        Term::Constant(Value::Entity(entity))
    }
}

/// Trait for types that can be converted into Term<Attribute>
///
/// This trait is used to avoid ambiguity with From<&str> implementations
/// for other Term types while still allowing convenient attribute creation.
pub trait IntoAttributeTerm {
    /// Convert self into a Term<Attribute>
    fn into_attribute_term(self) -> Term<Attribute>;
}

impl IntoAttributeTerm for &str {
    fn into_attribute_term(self) -> Term<Attribute> {
        Term::Constant(self.parse().unwrap())
    }
}

impl IntoAttributeTerm for String {
    fn into_attribute_term(self) -> Term<Attribute> {
        Term::Constant(self.parse().unwrap())
    }
}

impl IntoAttributeTerm for Attribute {
    fn into_attribute_term(self) -> Term<Attribute> {
        Term::Constant(self)
    }
}

impl IntoAttributeTerm for Term<Attribute> {
    fn into_attribute_term(self) -> Term<Attribute> {
        self
    }
}

impl From<Attribute> for Term<Attribute> {
    fn from(attr: Attribute) -> Self {
        Term::Constant(attr)
    }
}

impl From<Entity> for Term<Entity> {
    fn from(entity: Entity) -> Self {
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
    T: IntoType + Clone,
{
    fn from(term: &Term<T>) -> Self {
        term.clone()
    }
}

impl<T: Scalar> From<&Option<Term<T>>> for Term<T> {
    fn from(term: &Option<Term<T>>) -> Self {
        if let Some(term) = term {
            term.clone()
        } else {
            Term::blank()
        }
    }
}

// Removed From<Term<String>> for Term<Value> implementation to prevent type erasure

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_integration() {
        // Test serialization
        let any = Term::<Value>::default();
        assert_eq!(serde_json::to_string(&any).unwrap(), r#"{"?":{}}"#);

        let string = Term::<String>::default();
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

        // Test deserialization
        println!("Testing deserialization...");

        // Test 1: Deserialize unnamed variable (Any)
        let json1 = r#"{"?":{}}"#;
        match serde_json::from_str::<Term<Value>>(json1) {
            Ok(term) => {
                println!("Deserialized term: {:?}", term);
                assert_eq!(term, Term::default());
            }
            Err(e) => {
                println!("Failed to deserialize: {}", e);
                panic!("Deserialization failed: {}", e);
            }
        }

        // // Test 2: Deserialize typed unnamed variable
        // let json2 = r#"{"?":{"type":"String"}}"#;
        // match serde_json::from_str::<Term<String>>(json2) {
        //     Ok(term) => {
        //         println!("✓ Deserialized typed Any: {:?}", term);
        //         assert!(term.is_any());
        //     }
        //     Err(e) => panic!("Failed to deserialize typed Any: {}", e),
        // }

        // // Test 3: Deserialize named variable with type
        // let json3 = r#"{"?":{"name":"title","type":"String"}}"#;
        // match serde_json::from_str::<Term<String>>(json3) {
        //     Ok(term) => {
        //         println!("✓ Deserialized named variable: {:?}", term);
        //         assert_eq!(term.name(), Some("title"));
        //     }
        //     Err(e) => panic!("Failed to deserialize named variable: {}", e),
        // }

        // // Test 4: Deserialize untyped named variable
        // let json4 = r#"{"?":{"name":"title"}}"#;
        // match serde_json::from_str::<Term<Value>>(json4) {
        //     Ok(term) => {
        //         println!("✓ Deserialized untyped variable: {:?}", term);
        //         assert_eq!(term.name(), Some("title"));
        //     }
        //     Err(e) => panic!("Failed to deserialize untyped variable: {}", e),
        // }

        // // Test 5: Deserialize constant
        // let json5 = r#""Alice""#;
        // match serde_json::from_str::<Term<String>>(json5) {
        //     Ok(term) => {
        //         println!("✓ Deserialized constant: {:?}", term);
        //         assert!(term.is_constant());
        //         assert_eq!(term.as_constant(), Some(&"Alice".to_string()));
        //     }
        //     Err(e) => panic!("Failed to deserialize constant: {}", e),
        // }

        // // Test 6: Deserialize Entity variable (the failing case?)
        // let json6 = r#"{"?":{"name":"user","type":"Entity"}}"#;
        // match serde_json::from_str::<Term<Entity>>(json6) {
        //     Ok(term) => {
        //         println!("✓ Deserialized Entity variable: {:?}", term);
        //         assert_eq!(term.name(), Some("user"));
        //     }
        //     Err(e) => panic!("Failed to deserialize Entity variable: {}", e),
        // }
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
        assert_eq!(string_term.content_type(), Some(Type::String));

        assert_eq!(untyped_term.name(), Some("anything"));
        assert_eq!(untyped_term.content_type(), None);
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
        assert_eq!(name_term.content_type(), Some(Type::String));
        assert_eq!(age_term.content_type(), Some(Type::UnsignedInt));
        assert_eq!(any_term.content_type(), None);
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
        let entity_var = Term::<Entity>::var("entity");
        let string_var = Term::<String>::var("name");

        // This should work with the new implementation
        let entity_term = entity_var.clone(); // Clone the Term
        let string_term = string_var.clone(); // Clone the Term

        // Both should be variable terms
        assert!(entity_term.is_variable());
        assert!(string_term.is_variable());

        // Check that variable names are preserved
        assert_eq!(entity_term.name(), Some("entity"));
        assert_eq!(entity_term.content_type(), Some(Type::Entity));

        assert_eq!(string_term.name(), Some("name"));
        assert_eq!(string_term.content_type(), Some(Type::String));
    }

    #[test]
    fn test_inference() {
        let thing = Term::var("hello");

        fn do_thing(_term: &Term<String>) {
            println!("")
        }

        do_thing(&thing);

        let data_type = thing.content_type();

        assert_eq!(data_type, Some(Type::String));

        let unknown = Term::<Value>::var("unknown");

        assert_eq!(unknown.content_type(), None);
    }
}
