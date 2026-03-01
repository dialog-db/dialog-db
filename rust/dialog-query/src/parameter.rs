//! Type-erased parameter for the dynamic planning/evaluation layer.
//!
//! [`Parameter`] is used in contexts where concrete type information has
//! been erased — parameter maps, equality constraints, environments,
//! answers, and error types. It carries an optional runtime [`Type`] tag
//! so that the planner can still reason about type constraints.
//!
//! `Parameter` has a custom serde implementation that inspects JSON numbers
//! directly, preserving the integer/float distinction.

use crate::artifact::{Type, Value};
use crate::term::Term;
use crate::types::Scalar;
use std::fmt;

/// A type-erased query parameter — either a variable or a constant value.
///
/// The dynamic counterpart of [`Term<T>`](crate::Term). Where `Term<T>`
/// carries type information at compile time, `Parameter` carries an
/// optional runtime [`Type`] tag in the `Variable` variant.
///
/// # Variants
///
/// * `Variable` — a named or anonymous variable. Named variables participate
///   in joins across premises; anonymous variables (`name: None`) act as
///   wildcards. Optionally carries a `typ` constraint derived from `T::TYPE`.
/// * `Constant` — a concrete [`Value`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub enum Parameter {
    /// A variable — named or anonymous (wildcard).
    ///
    /// Named variables (`name: Some(...)`) participate in joins across
    /// premises and must be bound during evaluation. Anonymous variables
    /// (`name: None`) match anything but produce no binding.
    #[serde(rename = "?")]
    Variable {
        /// Variable name for joins across premises, or `None` for a wildcard.
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// Optional type constraint (e.g. `Some(Type::String)`).
        /// `None` means "any type" (unconstrained variable).
        #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
        typ: Option<Type>,
    },
    /// A concrete value.
    #[serde(untagged)]
    Constant(Value),
}

impl Parameter {
    /// Create a named variable parameter without a type constraint.
    pub fn var(name: impl Into<String>) -> Self {
        Parameter::Variable {
            name: Some(name.into()),
            typ: None,
        }
    }

    /// Create an anonymous wildcard parameter.
    pub fn blank() -> Self {
        Parameter::Variable {
            name: None,
            typ: None,
        }
    }

    /// Get the variable name, if this is a named variable.
    pub fn name(&self) -> Option<&str> {
        match self {
            Parameter::Variable {
                name: Some(name), ..
            } => Some(name),
            _ => None,
        }
    }

    /// Returns `true` if this is an anonymous wildcard variable.
    pub fn is_blank(&self) -> bool {
        matches!(self, Parameter::Variable { name: None, .. })
    }

    /// Returns `true` if this is a `Constant`.
    pub fn is_constant(&self) -> bool {
        matches!(self, Parameter::Constant(_))
    }

    /// Returns `true` if this is a named variable.
    pub fn is_variable(&self) -> bool {
        matches!(self, Parameter::Variable { name: Some(_), .. })
    }

    /// Get the type constraint, if any.
    ///
    /// * `Variable { typ, .. }` → returns `typ` (may be `None` for untyped).
    /// * `Constant(v)` → returns `Some(v.data_type())`.
    pub fn content_type(&self) -> Option<Type> {
        match self {
            Parameter::Variable { typ, .. } => *typ,
            Parameter::Constant(v) => Some(Type::from(v)),
        }
    }

    /// Get the constant value, if this is a `Constant`.
    pub fn as_constant(&self) -> Option<&Value> {
        match self {
            Parameter::Constant(v) => Some(v),
            _ => None,
        }
    }

    /// Returns `true` if this parameter is bound in the given environment.
    ///
    /// Constants are always bound. Named variables are bound if their name
    /// appears in the environment. Anonymous variables are never bound.
    pub fn is_bound(&self, env: &crate::Environment) -> bool {
        match self {
            Parameter::Constant(_) => true,
            Parameter::Variable { name: None, .. } => false,
            Parameter::Variable { name: Some(n), .. } => env.contains(n),
        }
    }

    /// Adds this parameter's variable name to the environment.
    ///
    /// Only named variables are added; constants and blanks are ignored.
    pub fn bind(&self, env: &mut crate::Environment) {
        if let Parameter::Variable { name: Some(n), .. } = self {
            env.add(n.clone());
        }
    }

    /// Removes this parameter's variable name from the environment.
    ///
    /// Returns `true` if the name was present. Constants and blanks return `false`.
    pub fn unbind(&self, env: &mut crate::Environment) -> bool {
        match self {
            Parameter::Variable { name: Some(n), .. } => env.remove(n),
            _ => false,
        }
    }
}

impl<T: Scalar> From<Term<T>> for Parameter {
    fn from(term: Term<T>) -> Self {
        match term {
            Term::Variable { name, .. } => Parameter::Variable { name, typ: T::TYPE },
            Term::Constant(value) => Parameter::Constant(value.as_value()),
        }
    }
}

impl<T: Scalar> From<&Term<T>> for Parameter {
    fn from(term: &Term<T>) -> Self {
        Parameter::from(term.clone())
    }
}

/// Convert a `Term<Value>` into a `Parameter`.
///
/// `Value` implements `Typed` but not `Scalar`, so this needs its own impl.
/// The constant case is trivial since `Value` is already the dynamic type.
impl From<Term<Value>> for Parameter {
    fn from(term: Term<Value>) -> Self {
        match term {
            Term::Variable { name, .. } => Parameter::Variable {
                name,
                typ: <Value as crate::types::Typed>::TYPE,
            },
            Term::Constant(value) => Parameter::Constant(value),
        }
    }
}

impl From<&Term<Value>> for Parameter {
    fn from(term: &Term<Value>) -> Self {
        Parameter::from(term.clone())
    }
}

/// Convert any `Scalar` value directly into a constant `Parameter`.
///
/// This avoids the verbose `Parameter::Constant(Value::from(x))` pattern.
/// ```
/// # use dialog_query::Parameter;
/// let p = Parameter::from(42u32);
/// assert!(p.is_constant());
/// ```
impl<T: Scalar> From<T> for Parameter {
    fn from(value: T) -> Self {
        Parameter::Constant(value.as_value())
    }
}

impl fmt::Display for Parameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Parameter::Variable { name: None, .. } => write!(f, "_"),
            Parameter::Variable {
                name: Some(name),
                typ: Some(t),
            } => write!(f, "?{}<{:?}>", name, t),
            Parameter::Variable {
                name: Some(name),
                typ: None,
            } => write!(f, "?{}<Value>", name),
            Parameter::Constant(value) => write!(f, "{:?}", value),
        }
    }
}

// Custom Deserialize that inspects JSON numbers directly to preserve
// the integer/float distinction.

impl<'de> serde::Deserialize<'de> for Parameter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = serde_json::Value::deserialize(deserializer)?;
        parse_parameter(raw).map_err(serde::de::Error::custom)
    }
}

fn parse_parameter(raw: serde_json::Value) -> Result<Parameter, String> {
    match raw {
        serde_json::Value::Object(ref map) if map.contains_key("?") => {
            let inner = &map["?"];
            let obj = inner
                .as_object()
                .ok_or_else(|| "\"?\" value must be an object".to_string())?;

            let name = obj.get("name").and_then(|v| v.as_str()).map(String::from);
            let typ = obj
                .get("type")
                .map(|v| serde_json::from_value::<Type>(v.clone()))
                .transpose()
                .map_err(|e| format!("invalid type: {e}"))?;

            Ok(Parameter::Variable { name, typ })
        }
        serde_json::Value::Number(ref n) => {
            if let Some(u) = n.as_u64() {
                Ok(Parameter::Constant(Value::UnsignedInt(u as u128)))
            } else if let Some(i) = n.as_i64() {
                Ok(Parameter::Constant(Value::SignedInt(i as i128)))
            } else if let Some(f) = n.as_f64() {
                Ok(Parameter::Constant(Value::Float(f)))
            } else {
                Err(format!("unsupported number: {n}"))
            }
        }
        other => {
            let value: Value =
                serde_json::from_value(other).map_err(|e| format!("invalid value: {e}"))?;
            Ok(Parameter::Constant(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Entity;

    #[dialog_common::test]
    fn it_converts_from_typed_term() {
        let term = Term::<String>::var("name");
        let param = Parameter::from(term);
        assert_eq!(
            param,
            Parameter::Variable {
                name: Some("name".into()),
                typ: Some(Type::String)
            }
        );
    }

    #[dialog_common::test]
    fn it_converts_blank_term() {
        // A typed blank carries the type constraint
        let term = Term::<String>::blank();
        let param = Parameter::from(term);
        assert_eq!(
            param,
            Parameter::Variable {
                name: None,
                typ: Some(Type::String)
            }
        );
    }

    #[dialog_common::test]
    fn it_converts_constant_term() {
        let term = Term::Constant(42u32);
        let param = Parameter::from(term);
        assert_eq!(param, Parameter::Constant(Value::UnsignedInt(42)));
    }

    #[dialog_common::test]
    fn it_converts_from_term_ref() {
        let term = Term::<Entity>::var("entity");
        let param = Parameter::from(&term);
        assert_eq!(
            param,
            Parameter::Variable {
                name: Some("entity".into()),
                typ: Some(Type::Entity)
            }
        );
    }

    #[dialog_common::test]
    fn it_serializes_blank() {
        let param = Parameter::blank();
        let json = serde_json::to_value(&param).unwrap();
        assert_eq!(json, serde_json::json!({"?": {}}));
    }

    #[dialog_common::test]
    fn it_serializes_required_with_type() {
        let param = Parameter::Variable {
            name: Some("x".into()),
            typ: Some(Type::String),
        };
        let json = serde_json::to_value(&param).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"?": {"name": "x", "type": "Text"}})
        );
    }

    #[dialog_common::test]
    fn it_serializes_required_without_type() {
        let param = Parameter::Variable {
            name: Some("x".into()),
            typ: None,
        };
        let json = serde_json::to_value(&param).unwrap();
        assert_eq!(json, serde_json::json!({"?": {"name": "x"}}));
    }

    #[dialog_common::test]
    fn it_serializes_constant() {
        let param = Parameter::Constant(Value::String("hello".into()));
        let json = serde_json::to_value(&param).unwrap();
        assert_eq!(json, serde_json::json!("hello"));
    }

    #[dialog_common::test]
    fn it_deserializes_blank() {
        let json = serde_json::json!({"?": {}});
        let param: Parameter = serde_json::from_value(json).unwrap();
        assert_eq!(param, Parameter::blank());
    }

    #[dialog_common::test]
    fn it_deserializes_required() {
        let json = serde_json::json!({"?": {"name": "x", "type": "Text"}});
        let param: Parameter = serde_json::from_value(json).unwrap();
        assert_eq!(
            param,
            Parameter::Variable {
                name: Some("x".into()),
                typ: Some(Type::String)
            }
        );
    }

    #[dialog_common::test]
    fn it_deserializes_required_without_type() {
        let json = serde_json::json!({"?": {"name": "x"}});
        let param: Parameter = serde_json::from_value(json).unwrap();
        assert_eq!(
            param,
            Parameter::Variable {
                name: Some("x".into()),
                typ: None
            }
        );
    }

    #[dialog_common::test]
    fn it_preserves_json_integers() {
        // This is THE bug fix — JSON 42 should NOT become Float(42.0)
        let json = serde_json::json!(42);
        let param: Parameter = serde_json::from_value(json).unwrap();
        assert_eq!(param, Parameter::Constant(Value::UnsignedInt(42)));
    }

    #[dialog_common::test]
    fn it_preserves_negative_integers() {
        let json = serde_json::json!(-5);
        let param: Parameter = serde_json::from_value(json).unwrap();
        assert_eq!(param, Parameter::Constant(Value::SignedInt(-5)));
    }

    #[dialog_common::test]
    fn it_preserves_floats() {
        let json = serde_json::json!(3.14);
        let param: Parameter = serde_json::from_value(json).unwrap();
        assert_eq!(param, Parameter::Constant(Value::Float(3.14)));
    }

    #[dialog_common::test]
    fn it_deserializes_string_constant() {
        let json = serde_json::json!("hello");
        let param: Parameter = serde_json::from_value(json).unwrap();
        assert_eq!(param, Parameter::Constant(Value::String("hello".into())));
    }

    #[dialog_common::test]
    fn it_deserializes_boolean_constant() {
        let json = serde_json::json!(true);
        let param: Parameter = serde_json::from_value(json).unwrap();
        assert_eq!(param, Parameter::Constant(Value::Boolean(true)));
    }

    #[dialog_common::test]
    fn it_round_trips_through_json() {
        let cases = vec![
            Parameter::blank(),
            Parameter::Variable {
                name: Some("x".into()),
                typ: Some(Type::String),
            },
            Parameter::Variable {
                name: Some("y".into()),
                typ: None,
            },
            Parameter::Constant(Value::String("hello".into())),
            Parameter::Constant(Value::Boolean(true)),
        ];

        for param in cases {
            let json = serde_json::to_value(&param).unwrap();
            let restored: Parameter = serde_json::from_value(json).unwrap();
            assert_eq!(param, restored, "Round-trip failed for {:?}", param);
        }
    }

    #[dialog_common::test]
    fn it_displays_correctly() {
        assert_eq!(Parameter::blank().to_string(), "_");
        assert_eq!(
            Parameter::Variable {
                name: Some("x".into()),
                typ: Some(Type::String)
            }
            .to_string(),
            "?x<String>"
        );
        assert_eq!(
            Parameter::Variable {
                name: Some("y".into()),
                typ: None
            }
            .to_string(),
            "?y<Value>"
        );
    }

    #[dialog_common::test]
    fn it_has_helper_methods() {
        let blank = Parameter::blank();
        assert!(blank.is_blank());
        assert!(!blank.is_constant());
        assert!(!blank.is_variable());
        assert_eq!(blank.name(), None);
        assert_eq!(blank.content_type(), None);

        let variable = Parameter::Variable {
            name: Some("x".into()),
            typ: Some(Type::String),
        };
        assert!(!variable.is_blank());
        assert!(!variable.is_constant());
        assert!(variable.is_variable());
        assert_eq!(variable.name(), Some("x"));
        assert_eq!(variable.content_type(), Some(Type::String));

        let constant = Parameter::Constant(Value::UnsignedInt(42));
        assert!(!constant.is_blank());
        assert!(constant.is_constant());
        assert!(!constant.is_variable());
        assert_eq!(constant.name(), None);
        assert_eq!(constant.content_type(), Some(Type::UnsignedInt));
        assert_eq!(constant.as_constant(), Some(&Value::UnsignedInt(42)));
    }
}
