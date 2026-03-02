//! Term types for pattern matching and query construction.
//!
//! This module implements the core `Term<T>` type that represents either:
//! - **Variables**: Named or anonymous placeholders that match values of type `T`
//! - **Constants**: Concrete [`Value`]s
//!
//! The type parameter `T` must implement [`Typed`], mapping it to a
//! [`TypeDescriptor`] that is stored inside the `Variable` variant. For
//! concrete types this is a zero-sized type (e.g. [`Text`]), adding no
//! overhead. For [`Any`] it carries a runtime `Option<Type>`.
//!
//! `Term<Any>` is the unified replacement for the old `Parameter` type.

use std::fmt;

use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type, Value};
use crate::constraint::{Constraint, Equality};
use crate::error::SyntaxError;
use crate::proposition::Proposition;
use crate::types::{Any, Scalar, TypeDescriptor, Typed};
use crate::{Attribute, Premise};
use std::hash::Hash;

/// Either a concrete value or a named variable placeholder.
///
/// `Term<T>` is the fundamental building block of query patterns. When
/// constructing a premise you fill its parameters with terms:
/// - `Term::Constant(v)` — matches only the exact value `v`.
/// - `Term::Variable { name, descriptor }` — matches any value and, if named,
///   acts as an implicit join across premises that share the same name.
///   Anonymous (blank) variables (`name: None`) match anything but do not
///   participate in joins.
///
/// The type parameter `T` carries a compile-time type constraint — e.g.
/// `Term<String>` can only hold string values. The `descriptor` field
/// carries type metadata: a ZST for concrete types, `Any(Option<Type>)`
/// for dynamically-typed terms.
///
/// # JSON Serialization
/// - Named variable: `{ "?": { "name": "var_name" } }` (typed variables also include `"type"`)
/// - Anonymous variable: `{ "?": {} }`
/// - Constants: Plain JSON values (e.g., `"Alice"`, `42`, `true`)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term<T: Typed> {
    /// A variable term — a named or anonymous placeholder that matches values
    /// during query evaluation.
    Variable {
        /// Optional variable name for join across conjuncts.
        /// `None` = anonymous wildcard (blank).
        name: Option<String>,
        /// Type descriptor. For concrete types (e.g. `Text`) this is a ZST.
        /// For `Any` this carries a runtime `Option<Type>`.
        descriptor: <T as Typed>::Descriptor,
    },

    /// A concrete value. All constants are stored as [`Value`] regardless of `T`.
    Constant(Value),
}

/// Core functionality for `Term<T>` where `T` has a known static type.
impl<T> Term<T>
where
    T: Scalar,
{
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
        let this: Term<Any> = Term::<Any>::from(self);
        let is: Term<Any> = Term::<Any>::from(other.into());
        Premise::Assert(Proposition::Constraint(Constraint::Equality(
            Equality::new(this, is),
        )))
    }

    /// Get the constant as a typed value if this term is a constant.
    ///
    /// Attempts to convert the stored `Value` back to `T`.
    /// Returns `None` for variables or if conversion fails.
    pub fn as_typed_constant(&self) -> Option<T>
    where
        T: TryFrom<Value>,
    {
        match self {
            Term::Constant(value) => T::try_from(value.clone()).ok(),
            Term::Variable { .. } => None,
        }
    }
}

/// Methods available on all `Term<T>` regardless of `T`.
impl<T: Typed> Term<T> {
    /// Create a new variable with the given name.
    ///
    /// The descriptor is default-constructed: for concrete types this is a
    /// ZST carrying the static type, for `Any` it is `Any(None)`.
    pub fn var<N: Into<String>>(name: N) -> Self {
        Term::Variable {
            name: Some(name.into()),
            descriptor: <T as Typed>::Descriptor::default(),
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

    /// Check if this term is an unnamed variable (wildcard).
    ///
    /// Unnamed variables match anything but don't produce bindings.
    pub fn is_blank(&self) -> bool {
        matches!(self, Term::Variable { name: None, .. })
    }

    /// Get the variable name if this is a named variable term.
    ///
    /// Returns None for constants and unnamed variables.
    pub fn name(&self) -> Option<&str> {
        match self {
            Term::Variable {
                name: Some(name), ..
            } => Some(name),
            _ => None,
        }
    }

    /// Get the content type for this term.
    ///
    /// For variables: returns the type from the descriptor. For concrete
    /// typed terms this is always `Some(Type::...)`. For `Any` it
    /// depends on the runtime tag.
    ///
    /// For constants: inspects the stored `Value` to determine the type.
    pub fn content_type(&self) -> Option<Type> {
        match self {
            Term::Variable { descriptor, .. } => <<T as Typed>::Descriptor as TypeDescriptor>::TYPE
                .or_else(|| descriptor.content_type()),
            Term::Constant(value) => Some(Type::from(value)),
        }
    }

    /// Get the constant value if this term is a constant.
    ///
    /// Returns None for variables.
    pub fn as_constant(&self) -> Option<&Value> {
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

    /// Removes this term's variable name from the environment.
    ///
    /// Returns `true` if the name was present. Constants and blanks return `false`.
    pub fn unbind(&self, env: &mut crate::Environment) -> bool {
        match self {
            Term::Variable { name: Some(n), .. } => env.remove(n),
            _ => false,
        }
    }
}

impl<T: Typed> Default for Term<T> {
    fn default() -> Self {
        Term::Variable {
            name: None,
            descriptor: <T as Typed>::Descriptor::default(),
        }
    }
}

impl<T> fmt::Display for Term<T>
where
    T: Typed,
    <T as Typed>::Descriptor: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Constant(value) => write!(f, "{:?}", value),
            Term::Variable {
                name: Some(name),
                descriptor,
            } => {
                if let Some(data_type) = <<T as Typed>::Descriptor as TypeDescriptor>::TYPE
                    .or_else(|| descriptor.content_type())
                {
                    write!(f, "?{}<{:?}>", name, data_type)
                } else {
                    write!(f, "?{}<Value>", name)
                }
            }
            Term::Variable { name: None, .. } => write!(f, "_"),
        }
    }
}

impl From<ArtifactAttribute> for Term<ArtifactAttribute> {
    fn from(attr: ArtifactAttribute) -> Self {
        Term::Constant(Value::from(attr))
    }
}

impl From<Entity> for Term<Entity> {
    fn from(entity: Entity) -> Self {
        Term::Constant(Value::from(entity))
    }
}

impl From<crate::attribute::The> for Term<crate::attribute::The> {
    fn from(the: crate::attribute::The) -> Self {
        Term::Constant(Value::from(the))
    }
}

impl From<String> for Term<String> {
    fn from(value: String) -> Self {
        Term::Constant(Value::from(value))
    }
}

impl From<&str> for Term<String> {
    fn from(value: &str) -> Self {
        Term::Constant(Value::from(value.to_string()))
    }
}

impl TryFrom<String> for Term<ArtifactAttribute> {
    type Error = SyntaxError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value
            .parse::<ArtifactAttribute>()
            .map(|a| Term::Constant(Value::from(a)))
            .map_err(|_| SyntaxError::InvalidAttributeSyntax { actual: value })
    }
}

impl TryFrom<&str> for Term<ArtifactAttribute> {
    type Error = SyntaxError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value
            .parse::<ArtifactAttribute>()
            .map(|a| Term::Constant(Value::from(a)))
            .map_err(|_| SyntaxError::InvalidAttributeSyntax {
                actual: value.into(),
            })
    }
}

impl From<u32> for Term<u32> {
    fn from(value: u32) -> Self {
        Term::Constant(Value::from(value))
    }
}

impl From<i32> for Term<i32> {
    fn from(value: i32) -> Self {
        Term::Constant(Value::from(value))
    }
}

impl From<i64> for Term<i64> {
    fn from(value: i64) -> Self {
        Term::Constant(Value::from(value))
    }
}

impl From<bool> for Term<bool> {
    fn from(value: bool) -> Self {
        Term::Constant(Value::from(value))
    }
}

impl From<f32> for Term<f32> {
    fn from(value: f32) -> Self {
        Term::Constant(Value::from(value))
    }
}

impl From<f64> for Term<f64> {
    fn from(value: f64) -> Self {
        Term::Constant(Value::from(value))
    }
}

impl From<Vec<u8>> for Term<Vec<u8>> {
    fn from(value: Vec<u8>) -> Self {
        Term::Constant(Value::from(value))
    }
}

/// Convert an Attribute to a Term of its inner type
impl<A: Attribute> From<A> for Term<A::Type>
where
    A::Type: Scalar,
{
    fn from(attr: A) -> Self {
        Term::Constant(attr.value().clone().into())
    }
}

/// Support for converting Term references to owned Terms
impl<T: Typed + Clone> From<&Term<T>> for Term<T> {
    fn from(term: &Term<T>) -> Self {
        term.clone()
    }
}

impl<T: Scalar> From<&Option<Term<T>>> for Term<T> {
    fn from(term: &Option<Term<T>>) -> Self {
        if let Some(term) = term {
            term.clone()
        } else {
            Self::default()
        }
    }
}

/// Widen any concrete `Term<T>` to `Term<Any>`, preserving type info in the descriptor.
impl<T: Scalar> From<Term<T>> for Term<Any> {
    fn from(term: Term<T>) -> Self {
        match term {
            Term::Variable { name, .. } => Term::Variable {
                name,
                descriptor: Any(<<T as Typed>::Descriptor as TypeDescriptor>::TYPE),
            },
            Term::Constant(value) => Term::Constant(value),
        }
    }
}

/// Widen a `Term<T>` reference to `Term<Any>`.
impl<T: Scalar> From<&Term<T>> for Term<Any> {
    fn from(term: &Term<T>) -> Self {
        Term::<Any>::from(term.clone())
    }
}

/// Methods specific to `Term<Any>` — the dynamically-typed term.
impl Term<Any> {
    /// Create a named variable with a specific type constraint.
    ///
    /// Use `Term::<Any>::var("x")` for an untyped variable (inherited from
    /// `impl<T: Typed> Term<T>`). Use this method when you need a runtime
    /// type tag.
    pub fn typed_var(name: impl Into<String>, typ: Option<Type>) -> Self {
        Term::Variable {
            name: Some(name.into()),
            descriptor: Any(typ),
        }
    }

    /// Create a constant term from a scalar value.
    ///
    /// This avoids the verbose `Term::Constant(Value::from(value))` pattern.
    ///
    /// ```
    /// # use dialog_query::{Term, types::Any};
    /// let p = Term::<Any>::constant(42u32);
    /// assert!(p.is_constant());
    /// ```
    pub fn constant<T: Scalar>(value: T) -> Self {
        Term::Constant(value.into())
    }
}

/// Convert a `Term<Value>` into a `Term<Any>`.
///
/// `Value` implements `Typed` (with `Descriptor = Any`) but not `Scalar`,
/// so it needs its own conversion impl.
impl From<Term<Value>> for Term<Any> {
    fn from(term: Term<Value>) -> Self {
        match term {
            Term::Variable { name, .. } => Term::Variable {
                name,
                descriptor: Any(<<Value as Typed>::Descriptor as TypeDescriptor>::TYPE),
            },
            Term::Constant(value) => Term::Constant(value),
        }
    }
}

/// Convert a `&Term<Value>` into a `Term<Any>`.
impl From<&Term<Value>> for Term<Any> {
    fn from(term: &Term<Value>) -> Self {
        Term::<Any>::from(term.clone())
    }
}

/// Convert any typed `Term<T>` into `Term<Value>`, erasing the compile-time type.
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
            Term::Variable { name, .. } => Term::Variable {
                name,
                descriptor: Any(None),
            },
            Term::Constant(value) => Term::Constant(value),
        }
    }
}

/// Serde helper for the variable inner object: `{"name": "x", "type": "Text"}`.
#[derive(serde::Serialize, serde::Deserialize)]
struct VarInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    content_type: Option<Type>,
}

/// Serde helper enum for `Term<T>`.
///
/// Variables serialize as `{"?": {"name": "x"}}`, constants as plain JSON values.
/// Uses `#[serde(untagged)]` so variables match on the `"?"` key and constants
/// fall through to plain value deserialization.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum TermRepr {
    Variable {
        #[serde(rename = "?")]
        var: VarInfo,
    },
    Constant(Value),
}

impl<T: Typed> serde::Serialize for Term<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let repr = match self {
            Term::Variable {
                name, descriptor, ..
            } => {
                let content_type = <<T as Typed>::Descriptor as TypeDescriptor>::TYPE
                    .or_else(|| descriptor.content_type());
                TermRepr::Variable {
                    var: VarInfo {
                        name: name.clone(),
                        content_type,
                    },
                }
            }
            Term::Constant(value) => TermRepr::Constant(value.clone()),
        };
        repr.serialize(serializer)
    }
}

impl<'de, T: Typed> serde::Deserialize<'de> for Term<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Deserialize into raw JSON first so we can handle the variable `{"?": ...}`
        // case and preserve integer types (u128/i128 don't survive serde_json's
        // untagged enum deserialization for Value).
        let raw = serde_json::Value::deserialize(deserializer)?;
        match &raw {
            serde_json::Value::Object(map) if map.contains_key("?") => {
                let var: VarInfo =
                    serde_json::from_value(map["?"].clone()).map_err(serde::de::Error::custom)?;
                Ok(Term::Variable {
                    name: var.name,
                    descriptor: <T as Typed>::Descriptor::from_content_type(var.content_type),
                })
            }
            serde_json::Value::Number(n) => {
                let value = if let Some(u) = n.as_u64() {
                    Value::UnsignedInt(u as u128)
                } else if let Some(i) = n.as_i64() {
                    Value::SignedInt(i as i128)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    return Err(serde::de::Error::custom(format!("unsupported number: {n}")));
                };
                Ok(Term::Constant(value))
            }
            _ => {
                let value: Value = serde_json::from_value(raw).map_err(serde::de::Error::custom)?;
                Ok(Term::Constant(value))
            }
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
        let json = serde_json::to_string(&string).unwrap();
        // Concrete types include the type tag
        assert!(json.contains("\"?\""));

        // Named variables serialize with name and type
        let title = Term::<String>::var("title");
        let json = serde_json::to_string(&title).unwrap();
        assert!(json.contains("\"name\":\"title\""));

        // Constants serialize as plain values
        let constant: Term<String> = "hello".into();
        assert_eq!(serde_json::to_string(&constant).unwrap(), r#""hello""#);

        // Deserialization of variable
        let json2 = r#"{"?":{"name":"x"}}"#;
        let term: Term<String> = serde_json::from_str(json2).unwrap();
        assert_eq!(term.name(), Some("x"));
        assert!(term.is_variable());

        // Deserialization of constant
        let json3 = r#""hello""#;
        let term: Term<String> = serde_json::from_str(json3).unwrap();
        assert!(term.is_constant());

        // Parameters handle dynamic serialization
        let param = Term::<Any>::default();
        let json = serde_json::to_string(&param).unwrap();
        assert!(json.contains("\"?\""));

        let param: Term<Any> = Term::Variable {
            name: Some("title".into()),
            descriptor: Any(None),
        };
        let json = serde_json::to_string(&param).unwrap();
        assert!(json.contains("\"name\":\"title\""));
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

        // For untyped variables, use Term<Any>
        let untyped: Term<Any> = Term::Variable {
            name: Some("anything".into()),
            descriptor: Any(None),
        };
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
        let term1: Term<String> = "hello".into();
        let term2: Term<String> = "world".to_string().into();

        assert!(term1.is_constant());
        assert!(term2.is_constant());

        assert_eq!(term1.as_constant(), Some(&Value::String("hello".into())));

        let age_term: Term<u32> = 25u32.into();
        let score_term: Term<f64> = 2.5f64.into();
        let active_term: Term<bool> = true.into();

        assert!(age_term.is_constant());
        assert!(score_term.is_constant());
        assert!(active_term.is_constant());

        assert_eq!(age_term.as_constant(), Some(&Value::UnsignedInt(25)));
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

        fn do_thing(_term: &Term<String>) {}

        do_thing(&thing);

        let data_type = thing.content_type();
        assert_eq!(data_type, Some(Type::String));
    }

    #[dialog_common::test]
    fn it_creates_equality_constraint() {
        use crate::Premise;
        use crate::proposition::Proposition;

        let x = Term::<String>::var("x");
        let y = Term::<String>::var("y");

        let premise = x.is(y);

        match premise {
            Premise::Assert(Proposition::Constraint(Constraint::Equality(constraint))) => {
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

        let x = Term::<u32>::var("x");
        let constant: Term<u32> = 42u32.into();

        let premise = x.is(constant);

        match premise {
            Premise::Assert(Proposition::Constraint(Constraint::Equality(constraint))) => {
                assert_eq!(constraint.this.name(), Some("x"));
                assert!(constraint.is.is_constant());
            }
            _ => panic!("Expected Constraint premise"),
        }
    }

    #[dialog_common::test]
    fn it_widens_to_any() {
        let typed = Term::<String>::var("x");
        let any: Term<Any> = typed.into();

        assert_eq!(any.name(), Some("x"));
        assert_eq!(any.content_type(), Some(Type::String));

        let constant: Term<String> = "hello".into();
        let any: Term<Any> = constant.into();
        assert!(any.is_constant());
        assert_eq!(any.as_constant(), Some(&Value::String("hello".into())));
    }

    #[dialog_common::test]
    fn it_converts_from_typed_term_to_any() {
        let term = Term::<String>::var("name");
        let param = Term::<Any>::from(term);
        assert_eq!(
            param,
            Term::Variable {
                name: Some("name".into()),
                descriptor: Any(Some(Type::String))
            }
        );
    }

    #[dialog_common::test]
    fn it_converts_blank_term_to_any() {
        let term = Term::<String>::blank();
        let param = Term::<Any>::from(term);
        assert_eq!(
            param,
            Term::Variable {
                name: None,
                descriptor: Any(Some(Type::String))
            }
        );
    }

    #[dialog_common::test]
    fn it_converts_constant_term_to_any() {
        let term = Term::from(42u32);
        let param = Term::<Any>::from(term);
        assert_eq!(param, Term::Constant(Value::UnsignedInt(42)));
    }

    #[dialog_common::test]
    fn it_converts_from_term_ref_to_any() {
        let term = Term::<Entity>::var("entity");
        let param = Term::<Any>::from(&term);
        assert_eq!(
            param,
            Term::Variable {
                name: Some("entity".into()),
                descriptor: Any(Some(Type::Entity))
            }
        );
    }

    #[dialog_common::test]
    fn it_serializes_any_blank() {
        let param = Term::<Any>::blank();
        let json = serde_json::to_value(&param).unwrap();
        assert_eq!(json, serde_json::json!({"?": {}}));
    }

    #[dialog_common::test]
    fn it_serializes_any_with_type() {
        let param: Term<Any> = Term::Variable {
            name: Some("x".into()),
            descriptor: Any(Some(Type::String)),
        };
        let json = serde_json::to_value(&param).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"?": {"name": "x", "type": "Text"}})
        );
    }

    #[dialog_common::test]
    fn it_serializes_any_without_type() {
        let param: Term<Any> = Term::Variable {
            name: Some("x".into()),
            descriptor: Any(None),
        };
        let json = serde_json::to_value(&param).unwrap();
        assert_eq!(json, serde_json::json!({"?": {"name": "x"}}));
    }

    #[dialog_common::test]
    fn it_serializes_any_constant() {
        let param: Term<Any> = Term::Constant(Value::String("hello".into()));
        let json = serde_json::to_value(&param).unwrap();
        assert_eq!(json, serde_json::json!("hello"));
    }

    #[dialog_common::test]
    fn it_deserializes_any_blank() {
        let json = serde_json::json!({"?": {}});
        let param: Term<Any> = serde_json::from_value(json).unwrap();
        assert_eq!(param, Term::blank());
    }

    #[dialog_common::test]
    fn it_deserializes_any_with_type() {
        let json = serde_json::json!({"?": {"name": "x", "type": "Text"}});
        let param: Term<Any> = serde_json::from_value(json).unwrap();
        assert_eq!(param.name(), Some("x"));
    }

    #[dialog_common::test]
    fn it_deserializes_any_without_type() {
        let json = serde_json::json!({"?": {"name": "x"}});
        let param: Term<Any> = serde_json::from_value(json).unwrap();
        assert_eq!(
            param,
            Term::Variable {
                name: Some("x".into()),
                descriptor: Any(None)
            }
        );
    }

    #[dialog_common::test]
    fn it_preserves_json_integers() {
        let json = serde_json::json!(42);
        let param: Term<Any> = serde_json::from_value(json).unwrap();
        assert_eq!(param, Term::Constant(Value::UnsignedInt(42)));
    }

    #[dialog_common::test]
    fn it_preserves_negative_integers() {
        let json = serde_json::json!(-5);
        let param: Term<Any> = serde_json::from_value(json).unwrap();
        assert_eq!(param, Term::Constant(Value::SignedInt(-5)));
    }

    #[dialog_common::test]
    fn it_preserves_floats() {
        let json = serde_json::json!(3.14);
        let param: Term<Any> = serde_json::from_value(json).unwrap();
        assert_eq!(param, Term::Constant(Value::Float(3.14)));
    }

    #[dialog_common::test]
    fn it_deserializes_any_string_constant() {
        let json = serde_json::json!("hello");
        let param: Term<Any> = serde_json::from_value(json).unwrap();
        assert_eq!(param, Term::Constant(Value::String("hello".into())));
    }

    #[dialog_common::test]
    fn it_deserializes_any_boolean_constant() {
        let json = serde_json::json!(true);
        let param: Term<Any> = serde_json::from_value(json).unwrap();
        assert_eq!(param, Term::Constant(Value::Boolean(true)));
    }

    #[dialog_common::test]
    fn it_round_trips_any_through_json() {
        let cases = vec![
            Term::<Any>::blank(),
            Term::Variable {
                name: Some("y".into()),
                descriptor: Any(None),
            },
            Term::Constant(Value::String("hello".into())),
            Term::Constant(Value::Boolean(true)),
        ];

        for param in cases {
            let json = serde_json::to_value(&param).unwrap();
            let restored: Term<Any> = serde_json::from_value(json).unwrap();
            assert_eq!(param, restored, "Round-trip failed for {:?}", param);
        }
    }

    #[dialog_common::test]
    fn it_displays_any_correctly() {
        assert_eq!(Term::<Any>::blank().to_string(), "_");
        assert_eq!(
            Term::<Any>::typed_var("x", Some(Type::String)).to_string(),
            "?x<String>"
        );
        assert_eq!(Term::<Any>::var("y").to_string(), "?y<Value>");
    }

    #[dialog_common::test]
    fn it_has_any_helper_methods() {
        let blank = Term::<Any>::blank();
        assert!(blank.is_blank());
        assert!(!blank.is_constant());
        assert!(blank.is_variable());
        assert_eq!(blank.name(), None);
        assert_eq!(blank.content_type(), None);

        let variable: Term<Any> = Term::Variable {
            name: Some("x".into()),
            descriptor: Any(Some(Type::String)),
        };
        assert!(!variable.is_blank());
        assert!(!variable.is_constant());
        assert!(variable.is_variable());
        assert_eq!(variable.name(), Some("x"));
        assert_eq!(variable.content_type(), Some(Type::String));

        let constant: Term<Any> = Term::Constant(Value::UnsignedInt(42));
        assert!(!constant.is_blank());
        assert!(constant.is_constant());
        assert!(!constant.is_variable());
        assert_eq!(constant.name(), None);
        assert_eq!(constant.content_type(), Some(Type::UnsignedInt));
        assert_eq!(constant.as_constant(), Some(&Value::UnsignedInt(42)));
    }
}
