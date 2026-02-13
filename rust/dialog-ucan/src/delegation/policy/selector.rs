//! Extract values from a data structure.

pub mod error;
pub mod filter;
pub mod select;
pub mod selectable;

use self::{
    error::{ParseError, SelectorErrorReason},
    selectable::Selectable,
};

use filter::Filter;
use nom::{self, Parser, character::complete::char, multi::many0, sequence::preceded};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{cmp::Ordering, fmt, str::FromStr};
use thiserror::Error;

#[cfg(any(test, feature = "test_utils"))]
use arbitrary::Arbitrary;

/// Selector for extracting values from a data structure.
#[derive(Debug, Default, Clone, PartialEq)]
#[cfg_attr(any(test, feature = "test_utils"), derive(Arbitrary))]
pub struct Selector(pub Vec<Filter>);

impl Selector {
    /// Create a new, empty selector.
    #[must_use]
    pub const fn new() -> Self {
        Selector(vec![])
    }

    /// Check if two selectors are related,
    #[must_use]
    pub fn is_related(&self, other: &Selector) -> bool {
        self.0.iter().zip(other.0.iter()).all(|(a, b)| a == b)
    }
}

impl fmt::Display for Selector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ops = self.0.iter();

        if let Some(field) = ops.next() {
            if !field.is_dot_field() {
                write!(f, ".")?;
            }

            write!(f, "{field}")?;
        } else {
            write!(f, ".")?;
        }

        for op in ops {
            op.fmt(f)?;
        }

        Ok(())
    }
}

impl FromStr for Selector {
    type Err = nom::Err<ParseError>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with('.') {
            return Err(nom::Err::Error(ParseError::MissingStartingDot(
                s.to_string(),
            )));
        }

        if s.starts_with("..") {
            return Err(nom::Err::Error(ParseError::StartsWithDoubleDot(
                s.to_string(),
            )));
        }

        let working;
        let mut acc = vec![];

        if let Ok((more, found)) =
            nom::branch::alt((filter::parse_try_dot_field, filter::parse_dot_field)).parse(s)
        {
            working = more;
            acc.push(found);
        } else {
            working = &s[1..];
        }

        match preceded(many0(char('?')), many0(filter::parse)).parse(working) {
            Ok(("", ops)) => {
                let mut mut_ops = ops.clone();
                acc.append(&mut mut_ops);
                Ok(Selector(acc))
            }
            Ok((more, _ops)) => Err(nom::Err::Error(ParseError::TrailingInput(more.to_string()))),
            Err(err) => Err(err.map(|input| ParseError::UnknownPattern(input.to_string()))),
        }
    }
}
impl Serialize for Selector {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Selector {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Selector::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Selector error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Error)]
#[error("Selector {selector} encountered runtime error: {reason}")]
pub struct SelectorError {
    /// The selector that caused the error.
    pub selector: Selector,

    /// The reason for the error.
    pub reason: SelectorErrorReason,
}

impl SelectorError {
    /// Create a new selector error.
    #[must_use]
    pub fn from_refs(path_refs: &Vec<&Filter>, reason: SelectorErrorReason) -> SelectorError {
        SelectorError {
            selector: Selector(path_refs.iter().map(|op| (*op).clone()).collect()),
            reason,
        }
    }
}

impl PartialOrd for Selector {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self == other {
            return Some(Ordering::Equal);
        }

        if self.0.starts_with(&other.0) {
            return Some(Ordering::Greater);
        }

        if other.0.starts_with(&self.0) {
            return Some(Ordering::Less);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions as pretty;
    use testresult::TestResult;

    mod serialization {
        use super::*;

        #[test_log::test]
        fn test_bare_dot() -> TestResult {
            pretty::assert_eq!(Selector::from_str("."), Ok(Selector(vec![])));
            Ok(())
        }

        #[test_log::test]
        fn test_dot_try() -> TestResult {
            pretty::assert_eq!(Selector::from_str(".?"), Ok(Selector(vec![])));
            Ok(())
        }

        #[test_log::test]
        fn test_dot_many_tries() -> TestResult {
            pretty::assert_eq!(
                Selector::from_str(".?????????????????????"),
                Ok(Selector(vec![]))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_inner_try_is_null() -> TestResult {
            pretty::assert_eq!(
                Selector::from_str(".nope?.not"),
                Ok(Selector(vec![
                    Filter::Try(Box::new(Filter::Field("nope".into()))),
                    Filter::Field("not".into())
                ]))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_dot_many_tries_and_dot_field() -> TestResult {
            pretty::assert_eq!(
                Selector::from_str(".?????????????????????.foo"),
                Ok(Selector(vec![Filter::Field("foo".to_string())]))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_question_marks() -> TestResult {
            pretty::assert_eq!(
                Selector::from_str(".foo??????????????"),
                Ok(Selector(vec![Filter::Try(Box::new(Filter::Field(
                    "foo".to_string()
                )))]))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_fails_trailing_dot() -> TestResult {
            let got = Selector::from_str(".foo.");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_leading_double_dot() -> TestResult {
            let got = Selector::from_str("..foo");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_inner_double_dot() -> TestResult {
            let got = Selector::from_str(".foo..bar");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_multiple_leading_dots() -> TestResult {
            let got = Selector::from_str("..");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fail_missing_leading_dot() -> TestResult {
            let got = Selector::from_str("[22]");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_dot_field() -> TestResult {
            let got = Selector::from_str(".foo");
            pretty::assert_eq!(got, Ok(Selector(vec![Filter::Field("foo".to_string())])));
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_dot_fields() -> TestResult {
            let got = Selector::from_str(".foo.bar.baz");
            pretty::assert_eq!(
                got,
                Ok(Selector(vec![
                    Filter::Field("foo".to_string()),
                    Filter::Field("bar".to_string()),
                    Filter::Field("baz".to_string())
                ]))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_fairly_complex() -> TestResult {
            let got = Selector::from_str(r#".foo.bar[].baz[0][]["42"]._quux?[8]"#);
            pretty::assert_eq!(
                got,
                Ok(Selector(vec![
                    Filter::Field("foo".to_string()),
                    Filter::Field("bar".to_string()),
                    Filter::Values,
                    Filter::Field("baz".to_string()),
                    Filter::ArrayIndex(0),
                    Filter::Values,
                    Filter::Field("42".to_string()),
                    Filter::Try(Box::new(Filter::Field("_quux".to_string()))),
                    Filter::ArrayIndex(8)
                ]))
            );

            Ok(())
        }

        #[test_log::test]
        fn test_very_complex() -> TestResult {
            let got = Selector::from_str(r#".???.foo.bar[].baz[0][]["42"]._quux??[8]"#);
            pretty::assert_eq!(
                got,
                Ok(Selector(vec![
                    Filter::Field("foo".to_string()),
                    Filter::Field("bar".to_string()),
                    Filter::Values,
                    Filter::Field("baz".to_string()),
                    Filter::ArrayIndex(0),
                    Filter::Values,
                    Filter::Field("42".to_string()),
                    Filter::Try(Box::new(Filter::Field("_quux".to_string()))),
                    Filter::ArrayIndex(8)
                ]))
            );

            Ok(())
        }
    }
}
