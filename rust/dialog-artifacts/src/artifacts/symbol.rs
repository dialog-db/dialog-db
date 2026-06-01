//! [`Symbol`] is a constrained-character-set identifier.
//!
//! A [`Symbol`] is an identifier built from a restricted character set:
//! lowercase letters, digits, hyphens, and dots. Symbols form the building
//! blocks of attributes: an attribute is a pair of symbols (a namespace and
//! a name) separated by a `/` byte in the index encoding.
//!
//! The `/` byte itself is reserved and cannot appear within a `Symbol`.
//! Joint length validation against the [`ATTRIBUTE_LENGTH`] budget happens
//! at the layer that composes two symbols into an attribute, not here.

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use ::serde::{Deserialize, Serialize};

use crate::{ATTRIBUTE_LENGTH, DialogArtifactsError, TypeError, Value, ValueDataType};

/// Maximum length in bytes for a single [`Symbol`].
///
/// One byte less than the full attribute slot, reserving room for the
/// delimiter when two symbols are joined into an attribute. The joint
/// budget (`namespace + 1 + name <= ATTRIBUTE_LENGTH`) is enforced at the
/// attribute composition site.
pub const MAX_SYMBOL_LENGTH: usize = ATTRIBUTE_LENGTH - 1;

/// A validated identifier with a restricted character set.
///
/// Rules (matching the formal-notation identifier shape):
/// - Non-empty, at most [`MAX_SYMBOL_LENGTH`] bytes.
/// - Characters: lowercase letters (`a`-`z`), digits (`0`-`9`), hyphens (`-`),
///   and dots (`.`). No `/`.
/// - Must start with a lowercase letter.
/// - Must not end with a hyphen or a dot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Symbol(String);

impl Symbol {
    /// The string value of this symbol.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The bytes of this symbol, suitable for direct encoding into an index
    /// key slot. No padding or terminator.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Validate a candidate symbol string.
    fn validate(value: &str) -> Result<(), &'static str> {
        let bytes = value.as_bytes();

        if bytes.is_empty() {
            return Err("Symbol must not be empty");
        }
        if bytes.len() > MAX_SYMBOL_LENGTH {
            return Err("Symbol exceeds maximum length");
        }

        let first = bytes[0];
        if !first.is_ascii_lowercase() {
            return Err("Symbol must start with a lowercase letter");
        }

        let last = bytes[bytes.len() - 1];
        if last == b'-' || last == b'.' {
            return Err("Symbol must not end with a hyphen or dot");
        }

        for &b in bytes {
            let ok = b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.';
            if !ok {
                return Err("Symbol must contain only lowercase letters, digits, hyphens, or dots");
            }
        }

        Ok(())
    }
}

impl TryFrom<String> for Symbol {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Symbol::validate(&value).map_err(|reason| {
            DialogArtifactsError::InvalidAttribute(format!("Invalid symbol \"{value}\": {reason}"))
        })?;
        Ok(Self(value))
    }
}

impl FromStr for Symbol {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Symbol::try_from(s.to_owned())
    }
}

impl From<Symbol> for String {
    fn from(value: Symbol) -> Self {
        value.0
    }
}

impl From<&Symbol> for String {
    fn from(value: &Symbol) -> Self {
        value.0.clone()
    }
}

impl Display for Symbol {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0)
    }
}

impl From<Symbol> for Value {
    fn from(symbol: Symbol) -> Self {
        Value::String(symbol.0)
    }
}

impl TryFrom<Value> for Symbol {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(s) => Symbol::try_from(s).map_err(|e| TypeError::InvalidValue {
                expected: "Symbol",
                reason: e.to_string(),
            }),
            other => Err(TypeError::TypeMismatch(
                ValueDataType::String,
                other.data_type(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    #[dialog_common::test]
    fn it_parses_a_simple_symbol() {
        let s: Symbol = "foo".parse().unwrap();
        assert_eq!(s.as_str(), "foo");
        assert_eq!(s.as_bytes(), b"foo");
    }

    #[dialog_common::test]
    fn it_parses_a_dotted_symbol() {
        let s: Symbol = "dialog.concept.with".parse().unwrap();
        assert_eq!(s.as_str(), "dialog.concept.with");
    }

    #[dialog_common::test]
    fn it_parses_a_kebab_case_symbol() {
        let s: Symbol = "ingredient-name".parse().unwrap();
        assert_eq!(s.as_str(), "ingredient-name");
    }

    #[dialog_common::test]
    fn it_parses_a_symbol_with_digits() {
        let s: Symbol = "web3".parse().unwrap();
        assert_eq!(s.as_str(), "web3");
    }

    #[dialog_common::test]
    fn it_parses_a_single_letter_symbol() {
        let s: Symbol = "a".parse().unwrap();
        assert_eq!(s.as_str(), "a");
    }

    #[dialog_common::test]
    fn it_rejects_empty_symbol() {
        assert!("".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_symbol_with_slash() {
        assert!("foo/bar".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_symbol_starting_with_digit() {
        assert!("3foo".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_symbol_starting_with_hyphen() {
        assert!("-foo".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_symbol_starting_with_dot() {
        assert!(".foo".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_symbol_ending_with_hyphen() {
        assert!("foo-".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_symbol_ending_with_dot() {
        assert!("foo.".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_uppercase() {
        assert!("Foo".parse::<Symbol>().is_err());
        assert!("foo Bar".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_underscore() {
        assert!("foo_bar".parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_rejects_symbol_too_long() {
        let long = "a".repeat(MAX_SYMBOL_LENGTH + 1);
        assert!(long.parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_accepts_max_length_symbol() {
        let max = "a".repeat(MAX_SYMBOL_LENGTH);
        assert!(max.parse::<Symbol>().is_ok());
    }

    #[dialog_common::test]
    fn it_round_trips_through_value() {
        let s: Symbol = "dialog.concept.with".parse().unwrap();
        let v: Value = s.clone().into();
        let restored: Symbol = Symbol::try_from(v).unwrap();
        assert_eq!(s, restored);
    }

    #[dialog_common::test]
    fn it_rejects_non_string_value() {
        let v = Value::UnsignedInt(42);
        assert!(Symbol::try_from(v).is_err());
    }

    #[dialog_common::test]
    fn it_rejects_invalid_string_value() {
        let v = Value::String("Bad/Symbol".into());
        assert!(Symbol::try_from(v).is_err());
    }
}
