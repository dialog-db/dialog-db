//! [`Symbol`] is a constrained-character-set identifier, and [`Name`]
//! is the name half of an attribute: either a [`Symbol`] or a
//! fractional [`Position`].
//!
//! An attribute is a `domain/name` pair separated by a `/` byte. The
//! domain half is always a [`Symbol`]; the name half is a [`Name`],
//! which admits two syntactically disjoint shapes discriminated by
//! their first byte:
//!
//! - a [`Symbol`] — an ordinary named predicate — starts with a
//!   lowercase letter, and
//! - a [`Position`] — the sort key of an ordered relation's member
//!   (see [`crate::position`]) — starts with an uppercase major.
//!
//! The `/` byte is reserved and cannot appear within either shape.
//! Joint length validation against the [`ATTRIBUTE_LENGTH`] budget
//! happens at the layer that composes the halves into an attribute
//! ([`crate::Attribute::compose`]), not here.

use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use ::serde::{Deserialize, Serialize};

use crate::position::Position;
use crate::{ATTRIBUTE_LENGTH, DialogArtifactsError};

/// Maximum length in bytes for a single [`Symbol`].
///
/// One byte less than the full attribute slot, reserving room for the
/// delimiter when a symbol is joined with a name into an attribute.
/// The joint budget (`domain + 1 + name <= ATTRIBUTE_LENGTH`) is
/// enforced at the attribute composition site.
pub const MAX_SYMBOL_LENGTH: usize = ATTRIBUTE_LENGTH - 1;

/// A validated identifier with a restricted character set.
///
/// Rules (matching the formal-notation identifier shape):
/// - Non-empty, at most [`MAX_SYMBOL_LENGTH`] bytes.
/// - Characters: lowercase letters (`a`-`z`), digits (`0`-`9`),
///   hyphens (`-`), and dots (`.`). No `/`.
/// - Must start with a lowercase letter.
/// - Must not end with a hyphen or a dot.
///
/// Starting lowercase is what keeps symbols syntactically disjoint
/// from fractional [`Position`]s, whose first byte is always an
/// uppercase major — so the name half of an attribute discriminates
/// by its first byte alone (see [`Name`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(into = "String", try_from = "String")]
pub struct Symbol(String);

impl Symbol {
    /// The string value of this symbol.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The bytes of this symbol, suitable for direct encoding into an
    /// index key slot. No padding or terminator.
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

/// The name half of an attribute: a named predicate ([`Symbol`]) or
/// an ordered-relation sort key ([`Position`]).
///
/// The two shapes are syntactically disjoint by their first byte —
/// symbols start with a lowercase letter, positions with an uppercase
/// major — so parsing discriminates without a tag and a scan over a
/// domain can classify every entry from the name alone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Name {
    /// A named predicate.
    Symbol(Symbol),
    /// A fractional position: the sort key of an ordered relation's
    /// member.
    Position(Position),
}

impl Name {
    /// The string form of this name.
    pub fn as_str(&self) -> &str {
        match self {
            Name::Symbol(symbol) => symbol.as_str(),
            Name::Position(position) => position.as_str(),
        }
    }

    /// The symbol, when this name is a named predicate.
    pub fn symbol(&self) -> Option<&Symbol> {
        match self {
            Name::Symbol(symbol) => Some(symbol),
            Name::Position(_) => None,
        }
    }

    /// The position, when this name is an ordered-relation sort key.
    pub fn position(&self) -> Option<&Position> {
        match self {
            Name::Symbol(_) => None,
            Name::Position(position) => Some(position),
        }
    }
}

impl TryFrom<&str> for Name {
    type Error = DialogArtifactsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.as_bytes().first() {
            Some(first) if first.is_ascii_lowercase() => Symbol::from_str(value).map(Name::Symbol),
            Some(first) if first.is_ascii_uppercase() => Position::try_from(value)
                .map(Name::Position)
                .map_err(|error| {
                    DialogArtifactsError::InvalidAttribute(format!(
                        "Invalid position name \"{value}\": {error}"
                    ))
                }),
            _ => Err(DialogArtifactsError::InvalidAttribute(format!(
                "Name must start with a letter (lowercase for a symbol, \
                 uppercase for a position), but got \"{value}\""
            ))),
        }
    }
}

impl TryFrom<String> for Name {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Name::try_from(value.as_str())
    }
}

impl FromStr for Name {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Name::try_from(s)
    }
}

impl From<Symbol> for Name {
    fn from(symbol: Symbol) -> Self {
        Name::Symbol(symbol)
    }
}

impl From<Position> for Name {
    fn from(position: Position) -> Self {
        Name::Position(position)
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.as_str())
    }
}

/// The shape of a [`Name`] — its discriminant without its content.
///
/// Because the shapes are disjoint by their first byte AND the byte
/// classes are contiguous (`A`–`Z` for positions below `a`–`z` for
/// symbols), a shape is more than a filter: within a domain each
/// shape occupies one contiguous key range, so a shape-constrained
/// scan narrows to the matching half of the domain instead of
/// sweeping all of it (see `apply_prefix_bounds`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NameShape {
    /// The name half is a [`Symbol`]: a named predicate.
    Symbol,
    /// The name half is a fractional [`Position`]: an
    /// ordered-relation member's sort key.
    Position,
}

impl NameShape {
    /// Classify a name by its first byte alone: uppercase begins a
    /// position, lowercase begins a symbol, anything else is
    /// neither. This is the *coarse* classification the byte-range
    /// machinery shares (scan bounds and per-key filters must agree
    /// on the same class); it admits strings the strict [`Name`]
    /// vocabulary rejects, and callers owing strictness re-check
    /// via [`Name::try_from`] / [`crate::Attribute::split`].
    pub fn classify(first: u8) -> Option<NameShape> {
        if first.is_ascii_uppercase() {
            Some(NameShape::Position)
        } else if first.is_ascii_lowercase() {
            Some(NameShape::Symbol)
        } else {
            None
        }
    }

    /// The contiguous first-byte class this shape occupies, inclusive
    /// on both ends. Position majors span all of `A`–`Z` (`A`–`M`
    /// negative, `N`–`Z` positive); symbols start `a`–`z`.
    pub fn first_byte_class(&self) -> (u8, u8) {
        match self {
            NameShape::Symbol => (b'a', b'z'),
            NameShape::Position => (b'A', b'Z'),
        }
    }
}

impl Name {
    /// This name's shape.
    pub fn shape(&self) -> NameShape {
        match self {
            Name::Symbol(_) => NameShape::Symbol,
            Name::Position(_) => NameShape::Position,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::position::{Bias, insert};

    #[dialog_common::test]
    fn it_parses_valid_symbols() {
        for valid in ["foo", "dialog.concept.with", "ingredient-name", "web3", "a"] {
            let symbol: Symbol = valid.parse().expect(valid);
            assert_eq!(symbol.as_str(), valid);
        }
    }

    #[dialog_common::test]
    fn it_rejects_invalid_symbols() {
        for invalid in [
            "", "foo/bar", "3foo", "-foo", ".foo", "foo-", "foo.", "Foo", "foo Bar", "foo_bar",
        ] {
            assert!(invalid.parse::<Symbol>().is_err(), "{invalid:?}");
        }
    }

    #[dialog_common::test]
    fn it_enforces_symbol_length_budget() {
        assert!("a".repeat(MAX_SYMBOL_LENGTH).parse::<Symbol>().is_ok());
        assert!("a".repeat(MAX_SYMBOL_LENGTH + 1).parse::<Symbol>().is_err());
    }

    #[dialog_common::test]
    fn it_round_trips_symbol_through_serde() {
        let symbol: Symbol = "person.name".parse().unwrap();
        let json = serde_json::to_string(&symbol).unwrap();
        assert_eq!(json, "\"person.name\"");
        let restored: Symbol = serde_json::from_str(&json).unwrap();
        assert_eq!(symbol, restored);
    }

    /// A name discriminates by its first byte: lowercase parses as a
    /// symbol, uppercase as a position, anything else is invalid.
    #[dialog_common::test]
    fn it_discriminates_names_by_first_byte() {
        let name: Name = "display-name".parse().expect("symbol name parses");
        assert!(name.symbol().is_some());
        assert!(name.position().is_none());

        let position = insert(&Bias::derive(b"member"), ..).expect("position derives");
        let name: Name = position.as_str().parse().expect("position name parses");
        assert_eq!(name.position(), Some(&position));
        assert!(name.symbol().is_none());

        assert!("3nope".parse::<Name>().is_err());
        assert!("".parse::<Name>().is_err());
    }
}
