//! Term types for pattern matching and query construction.
//!
//! This module implements the core `Term<T>` type that represents either:
//! - **Variables**: Named or anonymous placeholders that match values of type `T`
//! - **Constants**: Concrete values of type `T`
//!
//! The type parameter `T` provides compile-time type safety. For the
//! type-erased dynamic layer (parameter maps, planning, evaluation), see
//! [`Parameter`](crate::Parameter).

use std::fmt;

use crate::artifact::{Attribute as ArtifactAttribute, Cause, Entity, Type, Value};
use crate::constraint::{Constraint, Equality};
use crate::error::SyntaxError;
use crate::parameter::Parameter;
use crate::proposition::Proposition;
use crate::types::{Scalar, Typed};
use crate::{Attribute, Premise};
use std::hash::Hash;

/// Either a concrete value or a named variable placeholder.
///
/// `Term<T>` is the fundamental building block of query patterns. When
/// constructing a premise you fill its parameters with terms:
/// - `Term::Constant(v)` — matches only the exact value `v`.
/// - `Term::Variable { name, .. }` — matches any value and, if named,
///   acts as an implicit join across premises that share the same name.
///   Anonymous (blank) variables (`name: None`) match anything but do not
///   participate in joins.
///
/// The type parameter `T` carries a compile-time type constraint — e.g.
/// `Term<String>` can only hold string values. Type information is carried
/// at the Rust level via `T` and in the dynamic layer via
/// [`Parameter`](crate::Parameter).
///
/// # JSON Serialization
/// - Named variable: `{ "?": { "name": "var_name" } }`
/// - Anonymous variable: `{ "?": {} }`
/// - Constants: Plain JSON values (e.g., `"Alice"`, `42`, `true`)
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Term<T>
where
    T: Typed + Clone + 'static,
{
    /// A variable term — a named or anonymous placeholder that matches values
    /// during query evaluation.
    #[serde(rename = "?")]
    Variable {
        /// Optional variable name for join across conjuncts.
        /// `None` = anonymous wildcard (blank).
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },

    /// A concrete value of type T.
    #[serde(untagged)]
    Constant(T),
}

/// Core functionality implementation for Term<T>
///
/// Provides constructor methods and introspection capabilities.
impl<T> Term<T>
where
    T: Scalar,
{
    /// Create a new typed variable with the given name.
    pub fn var<N: Into<String>>(name: N) -> Self {
        Term::Variable {
            name: Some(name.into()),
        }
    }

    /// Create an anonymous variable (wildcard).
    ///
    /// Unlike named variables, blanks do not participate in joins across
    /// conjuncts.
    pub fn blank() -> Self {
        Self::default()
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

    /// Get the constant value if this term is a constant
    ///
    /// Returns None for variables
    pub fn as_constant(&self) -> Option<&T> {
        match self {
            Term::Constant(value) => Some(value),
            Term::Variable { .. } => None,
        }
    }

    /// Returns `true` if this term is bound in the given environment.
    ///
    /// Constants are always bound. Named variables are bound if their name
    /// appears in the environment. Anonymous variables are never bound.
    pub fn is_bound(&self, env: &crate::Environment) -> bool {
        match self {
            Term::Constant(_) => true,
            Term::Variable { name: None, .. } => false,
            Term::Variable { name: Some(n), .. } => env.contains(n),
        }
    }

    /// Adds this term's variable name to the environment.
    ///
    /// Only named variables are added; constants and blanks are ignored.
    pub fn bind(&self, env: &mut crate::Environment) {
        if let Term::Variable { name: Some(n), .. } = self {
            env.add(n.clone());
        }
    }

    /// Creates an equality constraint between this term and another term.
    ///
    /// This method creates a `Constraint::Equality` that enforces equality
    /// between the two terms during query evaluation. The constraint supports
    /// bidirectional inference: if one term is bound, the other will be inferred.
    ///
    /// # Example
    /// ```
    /// use dialog_query::Term;
    ///
    /// // Create a constraint that x equals y
    /// let constraint = Term::<String>::var("x").is(Term::<String>::var("y"));
    /// ```
    pub fn is<Other: Into<Term<T>>>(self, other: Other) -> Premise {
        Premise::Assert(Proposition::Constraint(Constraint::Equality(
            Equality::new(Parameter::from(&self), Parameter::from(&other.into())),
        )))
    }
}

impl<T> Default for Term<T>
where
    T: Typed + Clone,
{
    fn default() -> Self {
        Term::Variable { name: None }
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
    T: Typed + Clone + fmt::Debug,
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
                    write!(f, "?{}<Value>", name)
                }
            }
            // Unnamed variables display as underscore
            Term::Variable { name: None, .. } => write!(f, "_"),
        }
    }
}

/// Trait for types that can be converted into Term<Attribute>
///
/// This trait is used to avoid ambiguity with From<&str> implementations
/// for other Term types while still allowing convenient attribute creation.
pub trait IntoAttributeTerm {
    /// Convert self into a Term<Attribute>
    fn into_attribute_term(self) -> Term<ArtifactAttribute>;
}

impl IntoAttributeTerm for &str {
    fn into_attribute_term(self) -> Term<ArtifactAttribute> {
        Term::Constant(self.parse().unwrap())
    }
}

impl IntoAttributeTerm for String {
    fn into_attribute_term(self) -> Term<ArtifactAttribute> {
        Term::Constant(self.parse().unwrap())
    }
}

impl IntoAttributeTerm for ArtifactAttribute {
    fn into_attribute_term(self) -> Term<ArtifactAttribute> {
        Term::Constant(self)
    }
}

impl IntoAttributeTerm for Term<ArtifactAttribute> {
    fn into_attribute_term(self) -> Term<ArtifactAttribute> {
        self
    }
}

impl From<ArtifactAttribute> for Term<ArtifactAttribute> {
    fn from(attr: ArtifactAttribute) -> Self {
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

impl TryFrom<String> for Term<ArtifactAttribute> {
    type Error = SyntaxError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value
            .parse()
            .map(Term::Constant)
            .map_err(|_| SyntaxError::InvalidAttributeSyntax { actual: value })
    }
}

impl TryFrom<&str> for Term<ArtifactAttribute> {
    type Error = SyntaxError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value
            .parse()
            .map(Term::Constant)
            .map_err(|_| SyntaxError::InvalidAttributeSyntax {
                actual: value.into(),
            })
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

/// Convert an Attribute to a Term of its inner type
///
/// This allows ergonomic conversion from attribute values to terms:
/// ```rs
/// let name_term: Term<String> = employee::Name("Alice".into()).into();
/// ```
impl<A: Attribute> From<A> for Term<A::Type> {
    fn from(attr: A) -> Self {
        Term::Constant(attr.value().clone())
    }
}

/// Support for converting Term references to owned Terms
///
/// Allows cloning Terms when you have a reference but need an owned value.
impl<T> From<&Term<T>> for Term<T>
where
    T: Typed + Clone,
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

/// Convert a `Parameter` directly into a `Term<Entity>`.
impl TryFrom<Parameter> for Term<Entity> {
    type Error = crate::error::TypeError;

    fn try_from(param: Parameter) -> Result<Self, Self::Error> {
        match param {
            Parameter::Variable { ref typ, .. } if typ.is_none() || *typ == Some(Type::Entity) => {
                let Parameter::Variable { name, .. } = param else {
                    unreachable!()
                };
                Ok(Term::Variable { name })
            }
            Parameter::Constant(Value::Entity(e)) => Ok(Term::Constant(e)),
            other => Err(crate::error::TypeError::TypeMismatch {
                expected: Type::Entity,
                actual: other,
            }),
        }
    }
}

/// Convert a `Parameter` directly into a `Term<Attribute>`.
impl TryFrom<Parameter> for Term<ArtifactAttribute> {
    type Error = crate::error::TypeError;

    fn try_from(param: Parameter) -> Result<Self, Self::Error> {
        match param {
            Parameter::Variable { ref typ, .. } if typ.is_none() || *typ == Some(Type::Symbol) => {
                let Parameter::Variable { name, .. } = param else {
                    unreachable!()
                };
                Ok(Term::Variable { name })
            }
            Parameter::Constant(Value::Symbol(attr)) => Ok(Term::Constant(attr)),
            other => Err(crate::error::TypeError::TypeMismatch {
                expected: Type::Symbol,
                actual: other,
            }),
        }
    }
}

/// Convert a `Parameter` directly into a `Term<Cause>`.
impl TryFrom<Parameter> for Term<Cause> {
    type Error = crate::error::TypeError;

    fn try_from(param: Parameter) -> Result<Self, Self::Error> {
        match param {
            Parameter::Variable { ref typ, .. } if typ.is_none() || *typ == Some(Type::Bytes) => {
                let Parameter::Variable { name, .. } = param else {
                    unreachable!()
                };
                Ok(Term::Variable { name })
            }
            Parameter::Constant(Value::Bytes(b)) => {
                let cause =
                    Cause::try_from(b).map_err(|_| crate::error::TypeError::TypeMismatch {
                        expected: Type::Bytes,
                        actual: Parameter::Constant(Value::Bytes(vec![])),
                    })?;
                Ok(Term::Constant(cause))
            }
            other => Err(crate::error::TypeError::TypeMismatch {
                expected: Type::Bytes,
                actual: other,
            }),
        }
    }
}

/// Convert any typed `Term<T>` into `Term<Value>`, erasing the compile-time
/// type. Variables simply drop the type tag; constants are converted via
/// `Scalar::as_value()`.
///
/// This enables formulas with `Value`-typed fields (like `ToString`) to
/// accept any typed term:
///
/// ```
/// use dialog_query::{Term, Value};
///
/// let typed: Term<String> = Term::var("x");
/// let erased: Term<Value> = typed.into();
/// ```
impl<T: Scalar> From<Term<T>> for Term<Value> {
    fn from(term: Term<T>) -> Self {
        match term {
            Term::Variable { name } => Term::Variable { name },
            Term::Constant(v) => Term::Constant(v.as_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_serializes_and_deserializes() {
        // Blank variables serialize as {"?": {}}
        let string = Term::<String>::default();
        assert_eq!(serde_json::to_string(&string).unwrap(), r#"{"?":{}}"#);

        // Named variables serialize with name only
        let title = Term::<String>::var("title");
        assert_eq!(
            serde_json::to_string(&title).unwrap(),
            r#"{"?":{"name":"title"}}"#
        );

        // Constants serialize as plain values
        let constant = Term::Constant("hello".to_string());
        assert_eq!(serde_json::to_string(&constant).unwrap(), r#""hello""#);

        // Deserialization
        let json1 = r#"{"?":{}}"#;
        let term: Term<String> = serde_json::from_str(json1).unwrap();
        assert_eq!(term, Term::default());

        let json2 = r#"{"?":{"name":"x"}}"#;
        let term: Term<String> = serde_json::from_str(json2).unwrap();
        assert_eq!(term, Term::<String>::var("x"));

        // Extra fields like "type" are ignored during deserialization
        let json3 = r#"{"?":{"name":"x","type":"Text"}}"#;
        let term: Term<String> = serde_json::from_str(json3).unwrap();
        assert_eq!(term, Term::<String>::var("x"));

        // Parameters handle dynamic serialization
        let param = Parameter::blank();
        assert_eq!(serde_json::to_string(&param).unwrap(), r#"{"?":{}}"#);

        let param = Parameter::var("title");
        assert_eq!(
            serde_json::to_string(&param).unwrap(),
            r#"{"?":{"name":"title"}}"#
        );
    }

    #[dialog_common::test]
    fn it_integrates_variable_system() {
        let string_term = Term::<String>::var("name");
        let entity_term = Term::<Entity>::var("anything");

        assert!(string_term.is_variable());
        assert!(entity_term.is_variable());

        assert_eq!(string_term.name(), Some("name"));
        assert_eq!(string_term.content_type(), Some(Type::String));

        assert_eq!(entity_term.name(), Some("anything"));
        assert_eq!(entity_term.content_type(), Some(Type::Entity));

        // For untyped variables, use Parameter
        let untyped = Parameter::var("anything");
        assert_eq!(untyped.content_type(), None);
    }

    #[dialog_common::test]
    fn it_supports_turbofish_syntax() {
        let name_term = Term::<String>::var("name");
        let age_term = Term::<u64>::var("age");

        assert!(name_term.is_variable());
        assert!(age_term.is_variable());

        assert_eq!(name_term.name(), Some("name"));
        assert_eq!(age_term.name(), Some("age"));

        assert_eq!(name_term.content_type(), Some(Type::String));
        assert_eq!(age_term.content_type(), Some(Type::UnsignedInt));
    }

    #[dialog_common::test]
    fn it_converts_from_various_types() {
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
        let score_term: Term<f64> = 2.5f64.into();
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
            Term::Constant(f) => assert_eq!(f, 2.5f64),
            _ => panic!("Expected constant f64"),
        }

        match active_term {
            Term::Constant(b) => assert!(b),
            _ => panic!("Expected constant bool"),
        }
    }

    #[dialog_common::test]
    fn it_creates_term_from_variable_reference() {
        let entity_term = Term::<Entity>::var("entity");
        let string_term = Term::<String>::var("name");

        assert!(entity_term.is_variable());
        assert!(string_term.is_variable());
        assert_eq!(entity_term.name(), Some("entity"));
        assert_eq!(entity_term.content_type(), Some(Type::Entity));

        assert_eq!(string_term.name(), Some("name"));
        assert_eq!(string_term.content_type(), Some(Type::String));
    }

    #[dialog_common::test]
    fn it_infers_term_types() {
        let thing = Term::var("hello");

        fn do_thing(_term: &Term<String>) {
            // Just check it compiles
        }

        do_thing(&thing);

        let data_type = thing.content_type();

        assert_eq!(data_type, Some(Type::String));
    }

    #[dialog_common::test]
    fn it_creates_equality_constraint() {
        use crate::Premise;
        use crate::proposition::Proposition;

        // Create two variable terms — is() works on any Scalar type
        let x = Term::<String>::var("x");
        let y = Term::<String>::var("y");

        // Use is() to create an equality constraint
        let premise = x.is(y);

        // Verify it creates a Constraint wrapped in Proposition
        match premise {
            Premise::Assert(Proposition::Constraint(Constraint::Equality(constraint))) => {
                // Verify the constraint has the right structure
                assert_eq!(constraint.this.name(), Some("x"));
                assert_eq!(constraint.is.name(), Some("y"));
            }
            _ => panic!("Expected Constraint premise"),
        }
    }

    #[dialog_common::test]
    fn it_creates_equality_with_constant() {
        use crate::Premise;
        use crate::proposition::Proposition;

        // Create a variable and a constant
        let x = Term::<u32>::var("x");
        let constant = Term::Constant(42u32);

        // Use is() to create a constraint between variable and constant
        let premise = x.is(constant);

        // Verify it creates a Constraint wrapped in Proposition
        match premise {
            Premise::Assert(Proposition::Constraint(Constraint::Equality(constraint))) => {
                assert_eq!(constraint.this.name(), Some("x"));
                assert!(constraint.is.is_constant());
            }
            _ => panic!("Expected Constraint premise"),
        }
    }
}
