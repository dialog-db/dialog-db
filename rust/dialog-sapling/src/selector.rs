//! The grammar of a selector, which names a persona or a space: a name, a name
//! with a suffix, or a DID. See Sapling's PDD002, "Selectors".
//!
//! [`Selector::parse`] applies the rules of PDD002 in order:
//!
//! 1. An empty selector is invalid.
//! 2. A selector that starts with `did:` is a DID. It matches the thing with
//!    that DID, byte for byte.
//! 3. A selector that contains `#` is a name and a suffix, split at the first
//!    `#`. The name part must be a valid name, and the suffix 4 characters or
//!    longer. It matches the things with that name whose DID ends with that
//!    suffix.
//! 4. Any other selector is a name, which must be valid. It matches the things
//!    with that name.
//!
//! The name part is normalized as a name is (see `name`).

use std::fmt;

use crate::name::{self, NameError};
use crate::suffix;

/// A selector that is valid, with its name part normalized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selector {
    /// Rule 2: the DID of a persona.
    Did(String),
    /// Rule 3: a name, and the end of the DID of a persona with that name.
    Suffixed { name: String, suffix: String },
    /// Rule 4: a name.
    Name(String),
}

impl Selector {
    /// Parses `selector` by the rules of PDD002, in order.
    pub fn parse(selector: &str) -> Result<Self, Invalid> {
        if selector.is_empty() {
            return Err(Invalid::Empty);
        }
        if selector.starts_with("did:") {
            return Ok(Self::Did(selector.to_owned()));
        }
        if let Some((name, suffix)) = selector.split_once('#') {
            let name = name::check(name).map_err(Invalid::NamePart)?;
            let length = suffix.chars().count();
            if length < suffix::MIN {
                return Err(Invalid::ShortSuffix { length });
            }
            return Ok(Self::Suffixed {
                name,
                suffix: suffix.to_owned(),
            });
        }
        name::check(selector).map(Self::Name).map_err(Invalid::Name)
    }

    /// Whether this selector matches the thing, a persona or a space, with the
    /// name `name` and the DID `did`. The name part of a selector is a valid
    /// name, normalized, so it never equals a name that the rules refuse: only
    /// a DID selects a thing that holds such a name.
    pub fn matches(&self, name: &str, did: &str) -> bool {
        match self {
            Self::Did(selected) => did == selected,
            Self::Suffixed {
                name: selected,
                suffix,
            } => name == selected && did.ends_with(suffix.as_str()),
            Self::Name(selected) => name == selected,
        }
    }
}

/// A rule of PDD002 that a selector breaks. The message names the rule.
#[derive(Debug, PartialEq, Eq)]
pub enum Invalid {
    /// Rule 1: the selector is empty.
    Empty,
    /// Rule 3: the name before the `#` is not a valid persona name.
    NamePart(NameError),
    /// Rule 3: the suffix after the `#` has `length` characters, fewer than
    /// [`suffix::MIN`].
    ShortSuffix { length: usize },
    /// Rule 4: the selector, a name, is not a valid persona name.
    Name(NameError),
}

impl Invalid {
    /// The number of the rule of PDD002 that the selector breaks: 1, 3, or 4.
    pub fn rule(&self) -> u8 {
        match self {
            Self::Empty => 1,
            Self::NamePart(_) | Self::ShortSuffix { .. } => 3,
            Self::Name(_) => 4,
        }
    }

    /// Names the rule, for a selector of a `thing`: `persona` or `space`.
    pub fn describe(&self, f: &mut fmt::Formatter<'_>, thing: &str) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "a selector must not be empty"),
            Self::NamePart(error) => write!(
                f,
                "the name before `#` must be a valid {thing} name: {error}"
            ),
            Self::ShortSuffix { length } => write!(
                f,
                "the suffix after `#` must be {} characters or longer, \
                 and it has {length}",
                suffix::MIN
            ),
            Self::Name(error) => write!(
                f,
                "a selector with no `#` must be a valid {thing} name: {error}"
            ),
        }
    }

    /// The message of [`Invalid::describe`], as text.
    pub fn message(&self, thing: &str) -> String {
        struct Described<'a>(&'a Invalid, &'a str);
        impl fmt::Display for Described<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.describe(f, self.1)
            }
        }
        Described(self, thing).to_string()
    }
}

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.describe(f, "persona")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_each_kind_of_selector_by_the_rules_in_order() {
        let cases = [
            ("did:x#ab", Ok(Selector::Did("did:x#ab".to_owned()))),
            ("did:", Ok(Selector::Did("did:".to_owned()))),
            (
                "ana#2doK#x",
                Ok(Selector::Suffixed {
                    name: "ana".to_owned(),
                    suffix: "2doK#x".to_owned(),
                }),
            ),
            ("ana did:x", Ok(Selector::Name("ana did:x".to_owned()))),
            ("", Err(Invalid::Empty)),
            ("#abcd", Err(Invalid::NamePart(NameError::Empty))),
            (
                "ana#\u{E9}\u{E9}\u{E9}",
                Err(Invalid::ShortSuffix { length: 3 }),
            ),
        ];

        for (selector, parsed) in cases {
            assert_eq!(Selector::parse(selector), parsed, "{selector:?}");
        }
    }

    #[test]
    fn it_counts_the_suffix_in_characters() {
        assert_eq!(
            Selector::parse("ana#\u{E9}\u{E9}\u{E9}\u{E9}"),
            Ok(Selector::Suffixed {
                name: "ana".to_owned(),
                suffix: "\u{E9}".repeat(4),
            })
        );
    }

    #[test]
    fn it_names_the_rule_that_a_selector_breaks() {
        let rule = |selector: &str| Selector::parse(selector).unwrap_err().rule();
        assert_eq!(rule(""), 1);
        assert_eq!(rule("#abcd"), 3);
        assert_eq!(rule("ana#abc"), 3);
        assert_eq!(rule(" ana"), 4);
        assert_eq!(
            Selector::parse(" ana").unwrap_err().message("space"),
            "a selector with no `#` must be a valid space name: the name must not start or \
             end with a space"
        );
    }
}
