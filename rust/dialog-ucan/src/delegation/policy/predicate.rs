//! Policy predicates.

use super::selector::{SelectorError, select::Select};
use crate::{collection::Collection, number::Number};
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use thiserror::Error;

#[cfg(any(test, feature = "test_utils"))]
use arbitrary::{self, Arbitrary, Unstructured};

#[cfg(any(test, feature = "test_utils"))]
use crate::ipld::InternalIpld;

/// Validtor for [`Ipld`] values.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Selector equality check
    Equal(Select<Ipld>, Ipld),

    /// Selector greater than check
    GreaterThan(Select<Number>, Number),

    /// Selector greater than or equal check
    GreaterThanOrEqual(Select<Number>, Number),

    /// Selector less than check
    LessThan(Select<Number>, Number),

    /// Selector less than or equal check
    LessThanOrEqual(Select<Number>, Number),

    /// Seelctor `like` matcher check (glob patterns)
    Like(Select<String>, String),

    /// Negation
    Not(Box<Predicate>),

    /// Conjunction
    And(Vec<Predicate>),

    /// Disjunction
    Or(Vec<Predicate>),

    /// Universal quantification over a collection
    ///
    /// "For all elements of a collection" (∀x ∈ xs) the precicate must hold
    All(Select<Collection>, Box<Predicate>),

    /// Existential quantification over a collection
    ///
    /// "For any element of a collection" (∃x ∈ xs) the predicate must hold
    Any(Select<Collection>, Box<Predicate>),
}

use serde::ser::SerializeTuple;

impl Serialize for Predicate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Equal(lhs, rhs) => {
                let mut triple = serializer.serialize_tuple(3)?;
                triple.serialize_element(&"==")?;
                triple.serialize_element(lhs)?;
                triple.serialize_element(rhs)?;
                triple.end()
            }
            Self::GreaterThan(lhs, rhs) => {
                let mut triple = serializer.serialize_tuple(3)?;
                triple.serialize_element(&">")?;
                triple.serialize_element(lhs)?;
                triple.serialize_element(rhs)?;
                triple.end()
            }
            Self::GreaterThanOrEqual(lhs, rhs) => {
                let mut triple = serializer.serialize_tuple(3)?;
                triple.serialize_element(&">=")?;
                triple.serialize_element(lhs)?;
                triple.serialize_element(rhs)?;
                triple.end()
            }
            Self::LessThan(lhs, rhs) => {
                let mut triple = serializer.serialize_tuple(3)?;
                triple.serialize_element(&"<")?;
                triple.serialize_element(lhs)?;
                triple.serialize_element(rhs)?;
                triple.end()
            }
            Self::LessThanOrEqual(lhs, rhs) => {
                let mut triple = serializer.serialize_tuple(3)?;
                triple.serialize_element(&"<=")?;
                triple.serialize_element(lhs)?;
                triple.serialize_element(rhs)?;
                triple.end()
            }
            Self::Like(lhs, rhs) => {
                let mut triple = serializer.serialize_tuple(3)?;
                triple.serialize_element(&"like")?;
                triple.serialize_element(lhs)?;
                triple.serialize_element(rhs)?;
                triple.end()
            }
            Self::Not(inner) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&"not")?;
                tuple.serialize_element(inner)?;
                tuple.end()
            }
            Self::And(inner) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&"and")?;
                tuple.serialize_element(inner)?;
                tuple.end()
            }
            Self::Or(inner) => {
                let mut tuple = serializer.serialize_tuple(2)?;
                tuple.serialize_element(&"or")?;
                tuple.serialize_element(inner)?;
                tuple.end()
            }
            Self::All(xs, p) => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&"all")?;
                tuple.serialize_element(xs)?;
                tuple.serialize_element(p)?;
                tuple.end()
            }
            Self::Any(xs, p) => {
                let mut tuple = serializer.serialize_tuple(3)?;
                tuple.serialize_element(&"any")?;
                tuple.serialize_element(xs)?;
                tuple.serialize_element(p)?;
                tuple.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Predicate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};

        struct PredicateVisitor;

        impl<'de> Visitor<'de> for PredicateVisitor {
            type Value = Predicate;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a predicate")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let op: String = seq.next_element()?.ok_or_else(|| {
                    de::Error::invalid_length(0, &"expected a predicate operation")
                })?;

                match op.as_str() {
                    "==" => Ok(Predicate::Equal(
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &"expected a selector"))?,
                        seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(2, &"expected an Ipld value")
                        })?,
                    )),
                    ">" => Ok(Predicate::GreaterThan(
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &"expected a selector"))?,
                        seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(2, &"expected a Number value")
                        })?,
                    )),
                    ">=" => Ok(Predicate::GreaterThanOrEqual(
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &"expected a selector"))?,
                        seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(2, &"expected a Number value")
                        })?,
                    )),
                    "<" => Ok(Predicate::LessThan(
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &"expected a selector"))?,
                        seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(2, &"expected a Number value")
                        })?,
                    )),
                    "<=" => Ok(Predicate::LessThanOrEqual(
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &"expected a selector"))?,
                        seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(2, &"expected a Number value")
                        })?,
                    )),
                    "like" => Ok(Predicate::Like(
                        seq.next_element()?
                            .ok_or_else(|| de::Error::invalid_length(1, &"expected a selector"))?,
                        seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(2, &"expected a String value")
                        })?,
                    )),
                    "not" => Ok(Predicate::Not(Box::new(seq.next_element()?.ok_or_else(
                        || de::Error::invalid_length(1, &"expected a predicate"),
                    )?))),
                    "and" => Ok(Predicate::And(seq.next_element()?.ok_or_else(|| {
                        de::Error::invalid_length(1, &"expected a predicate")
                    })?)),
                    "or" => Ok(Predicate::Or(seq.next_element()?.ok_or_else(|| {
                        de::Error::invalid_length(1, &"expected a predicate")
                    })?)),
                    "all" => Ok(Predicate::All(
                        seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(1, &"expected a collection selector")
                        })?,
                        Box::new(seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(2, &"expected a predicate")
                        })?),
                    )),
                    "any" => Ok(Predicate::Any(
                        seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(1, &"expected a collection selector")
                        })?,
                        Box::new(seq.next_element()?.ok_or_else(|| {
                            de::Error::invalid_length(2, &"expected a predicate")
                        })?),
                    )),
                    _ => Err(de::Error::unknown_variant(
                        &op,
                        &[
                            "==", ">", ">=", "<", "<=", "like", "not", "and", "or", "all", "any",
                        ],
                    )),
                }
            }
        }

        deserializer.deserialize_seq(PredicateVisitor)
    }
}

#[cfg(any(test, feature = "test_utils"))]
impl<'a> Arbitrary<'a> for Predicate {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        enum Pick {
            Equal,
            GreaterThan,
            GreaterThanOrEqual,
            LessThan,
            LessThanOrEqual,
            Or,
            And,
            Like,
            All,
            Any,
        }

        let op = u.choose(&[
            Pick::Equal,
            Pick::GreaterThan,
            Pick::GreaterThanOrEqual,
            Pick::LessThan,
            Pick::LessThanOrEqual,
            Pick::Like,
            Pick::Or,
            Pick::And,
            Pick::All,
            Pick::Any,
        ])?;

        match op {
            Pick::Equal => Ok(Predicate::Equal(
                Select::<Ipld>::arbitrary(u)?,
                InternalIpld::arbitrary(u)?.into(),
            )),
            Pick::GreaterThan => Ok(Predicate::GreaterThan(
                Select::<Number>::arbitrary(u)?,
                Number::arbitrary(u)?,
            )),
            Pick::GreaterThanOrEqual => Ok(Predicate::GreaterThanOrEqual(
                Select::<Number>::arbitrary(u)?,
                Number::arbitrary(u)?,
            )),
            Pick::LessThan => Ok(Predicate::LessThan(
                Select::<Number>::arbitrary(u)?,
                Number::arbitrary(u)?,
            )),
            Pick::LessThanOrEqual => Ok(Predicate::LessThanOrEqual(
                Select::<Number>::arbitrary(u)?,
                Number::arbitrary(u)?,
            )),
            Pick::Like => Ok(Predicate::Like(
                Select::<String>::arbitrary(u)?,
                String::arbitrary(u)?,
            )),
            Pick::Or => {
                let xs = Vec::<Predicate>::arbitrary(u)?;
                Ok(Predicate::Or(xs))
            }
            Pick::And => {
                let xs = Vec::<Predicate>::arbitrary(u)?;
                Ok(Predicate::And(xs))
            }
            Pick::All => {
                let xs = Select::<Collection>::arbitrary(u)?;
                let p = Predicate::arbitrary(u)?;
                Ok(Predicate::All(xs, Box::new(p)))
            }
            Pick::Any => {
                let xs = Select::<Collection>::arbitrary(u)?;
                let p = Predicate::arbitrary(u)?;
                Ok(Predicate::Any(xs, Box::new(p)))
            }
        }
    }
}

impl Predicate {
    // TODO make &self?
    /// Run the predicate against concrete data.
    ///
    /// # Errors
    ///
    /// Returns a [`SelectorError`] if the predicate *cannot* be evaluated against the data
    /// due to things like shape errors.
    pub fn run(self, data: &Ipld) -> Result<bool, RunError> {
        Ok(match self {
            Predicate::Equal(lhs, rhs_data) => {
                let focused_data = lhs.clone().get(data)?;
                match (&focused_data, &rhs_data) {
                    (Ipld::Integer(int), Ipld::Float(float))
                    | (Ipld::Float(float), Ipld::Integer(int)) => {
                        if !float.is_nan() && !float.is_infinite() && float.fract() == 0.0 {
                            #[allow(clippy::cast_possible_truncation)]
                            let i = *float as i128;
                            *int == i
                        } else {
                            Err(RunError::CannotCompareNonwholeFloatToInt)?
                        }
                    }
                    _ => focused_data == rhs_data,
                }
            }
            Predicate::GreaterThan(lhs, rhs_data) => lhs.get(data)? > rhs_data,
            Predicate::GreaterThanOrEqual(lhs, rhs_data) => lhs.get(data)? >= rhs_data,
            Predicate::LessThan(lhs, rhs_data) => lhs.get(data)? < rhs_data,
            Predicate::LessThanOrEqual(lhs, rhs_data) => lhs.get(data)? <= rhs_data,
            Predicate::Like(lhs, rhs_data) => glob(&lhs.get(data)?, &rhs_data),
            Predicate::Not(inner) => !inner.run(data)?,
            Predicate::And(inner) => inner
                .into_iter()
                .try_fold(true, |acc, p| Ok::<_, RunError>(acc && p.run(data)?))?,
            Predicate::Or(inner) => {
                if inner.is_empty() {
                    true
                } else {
                    inner
                        .into_iter()
                        .try_fold(false, |acc, p| Ok::<_, RunError>(acc || p.run(data)?))?
                }
            }
            Predicate::All(selector, p) => {
                let focus = selector.get(data)?;
                focus.to_vec().iter().try_fold(true, |acc, each_datum| {
                    Ok::<_, RunError>(acc && p.clone().run(each_datum)?)
                })?
            }
            Predicate::Any(selector, p) => {
                let focus = selector.get(data)?;
                if focus.is_empty() {
                    true
                } else {
                    focus.to_vec().iter().try_fold(false, |acc, each_datum| {
                        Ok::<_, RunError>(acc || p.clone().run(each_datum)?)
                    })?
                }
            }
        })
    }
}

/// Check if a string matches a glob pattern
#[must_use]
pub fn glob(input: &str, pattern: &str) -> bool {
    if pattern.is_empty() {
        return input.is_empty();
    }

    // Parsing pattern
    let (saw_escape, mut patterns, mut working) = pattern.chars().fold(
        (false, vec![], String::new()),
        |(saw_escape, mut acc, mut working), c| {
            match c {
                '*' => {
                    if saw_escape {
                        working.push('*');
                        (false, acc, working)
                    } else {
                        acc.push(working);
                        working = String::new();
                        (false, acc, working)
                    }
                }
                '\\' => {
                    if saw_escape {
                        // Push prev escape
                        working.push('\\');
                    }
                    (true, acc, working)
                }
                _ => {
                    if saw_escape {
                        working.push('\\');
                    }

                    working.push(c);
                    (false, acc, working)
                }
            }
        },
    );

    if saw_escape {
        working.push('\\');
    }

    patterns.push(working);

    // Test input against the pattern
    patterns
        .iter()
        .enumerate()
        .try_fold(input, |acc, (idx, pattern_frag)| {
            #[allow(clippy::if_same_then_else)]
            if let Some((pre, post)) = acc.split_once(pattern_frag) {
                if idx == 0 && !pattern.starts_with('*') && !pre.is_empty() {
                    Err(())
                } else if idx == patterns.len() - 1 && !pattern.ends_with('*') && !post.is_empty() {
                    Err(())
                } else {
                    Ok(post)
                }
            } else {
                Err(())
            }
        })
        .is_ok()
}

impl TryFrom<Ipld> for Predicate {
    type Error = FromIpldError;

    fn try_from(ipld: Ipld) -> Result<Self, Self::Error> {
        match ipld {
            Ipld::List(v) => match v.as_slice() {
                [Ipld::String(s), inner] if s == "not" => {
                    let inner = Box::new(Predicate::try_from(inner.clone())?);
                    Ok(Predicate::Not(inner))
                }
                [Ipld::String(op_str), Ipld::List(vec)] => match op_str.as_str() {
                    "and" => {
                        let mut inner = Vec::new();
                        for ipld in vec {
                            inner.push(Predicate::try_from(ipld.clone())?);
                        }
                        Ok(Predicate::And(inner))
                    }
                    "or" => {
                        let mut inner = Vec::new();
                        for ipld in vec {
                            inner.push(Predicate::try_from(ipld.clone())?);
                        }
                        Ok(Predicate::Or(inner))
                    }
                    _ => Err(FromIpldError::UnrecognizedPairTag(op_str.clone())),
                },
                [Ipld::String(op_str), Ipld::String(sel_str), val] => match op_str.as_str() {
                    "==" => {
                        let sel = Select::<Ipld>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidIpldSelector)?;

                        Ok(Predicate::Equal(sel, val.clone()))
                    }
                    "!=" => {
                        let sel = Select::<Ipld>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidIpldSelector)?;

                        Ok(Predicate::Not(Box::new(Predicate::Equal(sel, val.clone()))))
                    }
                    ">" => {
                        let sel = Select::<Number>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidNumberSelector)?;

                        let num = Number::try_from(val.clone())
                            .map_err(FromIpldError::CannotParseIpldNumber)?;

                        Ok(Predicate::GreaterThan(sel, num))
                    }
                    ">=" => {
                        let sel = Select::<Number>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidNumberSelector)?;

                        let num = Number::try_from(val.clone())
                            .map_err(FromIpldError::CannotParseIpldNumber)?;
                        Ok(Predicate::GreaterThanOrEqual(sel, num))
                    }
                    "<" => {
                        let sel = Select::<Number>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidNumberSelector)?;

                        let num = Number::try_from(val.clone())
                            .map_err(FromIpldError::CannotParseIpldNumber)?;

                        Ok(Predicate::LessThan(sel, num))
                    }
                    "<=" => {
                        let sel = Select::<Number>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidNumberSelector)?;

                        let num = Number::try_from(val.clone())
                            .map_err(FromIpldError::CannotParseIpldNumber)?;

                        Ok(Predicate::LessThanOrEqual(sel, num))
                    }
                    "like" => {
                        let sel = Select::<String>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidStringSelector)?;

                        if let Ipld::String(s) = val {
                            Ok(Predicate::Like(sel, s.clone()))
                        } else {
                            Err(FromIpldError::NotAString(val.clone()))
                        }
                    }
                    "all" => {
                        let sel = Select::<Collection>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidCollectionSelector)?;

                        let p = Box::new(Predicate::try_from(val.clone())?);
                        Ok(Predicate::All(sel, p))
                    }
                    "any" => {
                        let sel = Select::<Collection>::from_str(sel_str.as_str())
                            .map_err(FromIpldError::InvalidCollectionSelector)?;

                        let p = Box::new(Predicate::try_from(val.clone())?);
                        Ok(Predicate::Any(sel, p))
                    }
                    _ => Err(FromIpldError::UnrecognizedTripleTag(op_str.clone())),
                },
                _ => Err(FromIpldError::UnrecognizedShape),
            },
            Ipld::Null
            | Ipld::Bool(_)
            | Ipld::Integer(_)
            | Ipld::Float(_)
            | Ipld::String(_)
            | Ipld::Bytes(_)
            | Ipld::Map(_)
            | Ipld::Link(_) => Err(FromIpldError::NotATuple(ipld)),
        }
    }
}

/// Error converting from [`Ipld`].
#[derive(Debug, PartialEq, Error)]
pub enum FromIpldError {
    /// Invalid [`Ipld`] selector.
    #[error("Invalid Ipld selector {0:?}")]
    InvalidIpldSelector(<Select<Ipld> as FromStr>::Err),

    /// Invalid [`Number`] selector.
    #[error("Invalid Number selector {0:?}")]
    InvalidNumberSelector(<Select<Number> as FromStr>::Err),

    /// Invalid Collection selector.
    #[error("Invalid Collection selector {0:?}")]
    InvalidCollectionSelector(<Select<Collection> as FromStr>::Err),

    /// Invalid String selector.
    #[error("Invalid String selector {0:?}")]
    InvalidStringSelector(<Select<Collection> as FromStr>::Err),

    /// Cannot parse [`Number`].
    #[error("Cannot parse Number {0:?}")]
    CannotParseIpldNumber(<Number as TryFrom<Ipld>>::Error),

    /// Not a string.
    #[error("Not a string: {0:?}")]
    NotAString(Ipld),

    /// Unrecognized pair tag.
    #[error("Unrecognized pair tag {0}")]
    UnrecognizedPairTag(String),

    /// Unrecognized triple tag.
    #[error("Unrecognized triple tag {0}")]
    UnrecognizedTripleTag(String),

    /// Unrecognized shape.
    #[error("Unrecognized shape")]
    UnrecognizedShape,

    /// Not a tuple.
    #[error("Not a predicate tuple {0:?}")]
    NotATuple(Ipld),
}

impl From<Predicate> for Ipld {
    fn from(p: Predicate) -> Self {
        match p {
            Predicate::Equal(lhs, rhs) => {
                Ipld::List(vec![Ipld::String("==".to_string()), lhs.into(), rhs])
            }
            Predicate::GreaterThan(lhs, rhs) => {
                Ipld::List(vec![Ipld::String(">".to_string()), lhs.into(), rhs.into()])
            }
            Predicate::GreaterThanOrEqual(lhs, rhs) => {
                Ipld::List(vec![Ipld::String(">=".to_string()), lhs.into(), rhs.into()])
            }
            Predicate::LessThan(lhs, rhs) => {
                Ipld::List(vec![Ipld::String("<".to_string()), lhs.into(), rhs.into()])
            }
            Predicate::LessThanOrEqual(lhs, rhs) => {
                Ipld::List(vec![Ipld::String("<=".to_string()), lhs.into(), rhs.into()])
            }
            Predicate::Like(lhs, rhs) => Ipld::List(vec![
                Ipld::String("like".to_string()),
                lhs.into(),
                rhs.into(),
            ]),
            Predicate::Not(inner) => {
                let unboxed = *inner;
                Ipld::List(vec![Ipld::String("not".to_string()), unboxed.into()])
            }
            Predicate::And(inner) => {
                let inner_ipld: Vec<Ipld> = inner.into_iter().map(Into::into).collect();
                vec![Ipld::String("and".to_string()), inner_ipld.into()].into()
            }
            Predicate::Or(inner) => {
                let inner_ipld: Vec<Ipld> = inner.into_iter().map(Into::into).collect();
                vec![Ipld::String("or".to_string()), inner_ipld.into()].into()
            }
            Predicate::All(xs, p) => Ipld::List(vec![
                Ipld::String("all".to_string()),
                xs.into(),
                (*p).into(),
            ]),
            Predicate::Any(xs, p) => Ipld::List(vec![
                Ipld::String("any".to_string()),
                xs.into(),
                (*p).into(),
            ]),
        }
    }
}

/// Runtime error ocurred when running a [`Predicate`].
#[derive(Debug, Clone, Error)]
pub enum RunError {
    /// Cannot convert non-whole float to int.
    #[error("cannot convert non-whole float to int")]
    CannotCompareNonwholeFloatToInt,

    /// NaNs are not comparable.
    #[error("cannot compare NaNs")]
    CannotCompareNaNs,

    /// Selector error.
    #[error(transparent)]
    SelectorError(#[from] SelectorError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use testresult::TestResult;

    mod glob {
        use super::*;

        #[test_log::test]
        fn test_concrete() -> TestResult {
            let got = glob(&"hello world", &"hello world");
            assert!(got);
            Ok(())
        }

        #[test_log::test]
        fn test_concrete_fail() -> TestResult {
            let got = glob(&"hello world", &"NOPE");
            assert!(!got);
            Ok(())
        }

        #[test_log::test]
        fn test_empty_pattern_fail() -> TestResult {
            let got = glob(&"hello world", &"");
            assert!(!got);
            Ok(())
        }

        #[test_log::test]
        fn test_escaped_star() -> TestResult {
            let got = glob(&"*", &r#"\*"#);
            assert!(got);
            Ok(())
        }

        #[test_log::test]
        fn test_inner_escaped_star() -> TestResult {
            let got = glob(&"hello, * world*", &r#"hello*\**\*"#);
            assert!(got);
            Ok(())
        }

        #[test_log::test]
        fn test_empty_string_fail() -> TestResult {
            let got = glob(&"", &"NOPE");
            assert!(!got);
            Ok(())
        }

        #[test_log::test]
        fn test_left_star() -> TestResult {
            let got = glob(&"hello world", &"*world");
            assert!(got);
            Ok(())
        }

        #[test_log::test]
        fn test_left_star_failure() -> TestResult {
            let got = glob(&"hello world", &"*NOPE");
            assert!(!got);
            Ok(())
        }

        #[test_log::test]
        fn test_right_star() -> TestResult {
            let got = glob(&"hello world", &"hello*");
            assert!(got);
            Ok(())
        }

        #[test_log::test]
        fn test_right_star_failure() -> TestResult {
            let got = glob(&"hello world", &"NOPE*");
            assert!(!got);
            Ok(())
        }

        #[test_log::test]
        fn test_only_star() -> TestResult {
            let got = glob(&"hello world", &"*");
            assert!(got);
            Ok(())
        }

        #[test_log::test]
        fn test_two_stars() -> TestResult {
            let got = glob(&"hello world", &"* *");
            assert!(got);
            Ok(())
        }

        #[test_log::test]
        fn test_two_stars_fail() -> TestResult {
            let got = glob(&"hello world", &"*@*");
            assert!(!got);
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_inner_stars() -> TestResult {
            let got = glob(&"hello world", &"h*l*o*w*r*d");
            assert!(got);
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_inner_stars_fail() -> TestResult {
            let got = glob(&"hello world", &"a*b*c*d*e*f");
            assert!(!got);
            Ok(())
        }

        #[test_log::test]
        fn test_concrete_with_multiple_inner_stars() -> TestResult {
            let got = glob(&"hello world", &"hello* *world");
            assert!(got);
            Ok(())
        }
    }

    mod run {
        use super::*;
        use ipld_core::ipld;

        fn simple() -> Ipld {
            ipld!({
                "foo": 42,
                "bar": "baz".to_string(),
                "qux": true
            })
        }

        fn email() -> Ipld {
            ipld!({
                "from": "alice@example.com",
                "to": ["bob@example.com", "fraud@example.com"],
                "cc": ["carol@example.com"],
                "subject": "Quarterly Reports",
                "body": "Here's Q2 the reports ..."
            })
        }

        fn wasm() -> Ipld {
            ipld!({
                "mod": "data:application/wasm;base64,ANYBASE64GOESHERE",
                "fun": "test",
                "input": [0, 1, 2 ,3]
            })
        }

        #[test_log::test]
        fn test_eq() -> TestResult {
            let p = Predicate::Equal(
                Select::from_str(".from").unwrap(),
                "alice@example.com".into(),
            );

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_eq_try_null() -> TestResult {
            let p = Predicate::Equal(Select::from_str(".not_from?").unwrap(), Ipld::Null.into());

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_eq_dot_field_ending_try_null() -> TestResult {
            let p = Predicate::Equal(Select::from_str(".from.not?").unwrap(), Ipld::Null.into());

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_eq_dot_field_inner_try_null() -> TestResult {
            let p = Predicate::Equal(Select::from_str(".nope?.not").unwrap(), Ipld::Null.into());

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_eq_root_try_not_null() -> TestResult {
            let p = Predicate::Equal(Select::from_str(".?").unwrap(), Ipld::Null.into());

            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_eq_try_not_null() -> TestResult {
            let p = Predicate::Equal(
                Select::from_str(".from?").unwrap(),
                "alice@example.com".into(),
            );

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_eq_nested_try_null() -> TestResult {
            let p = Predicate::Equal(Select::from_str(".from?.not?").unwrap(), Ipld::Null.into());

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_eq_fail_same_type() -> TestResult {
            let p = Predicate::Equal(Select::from_str(".from").unwrap(), "NOPE".into());

            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_eq_bad_selector() -> TestResult {
            let p = Predicate::Equal(
                Select::from_str(".NOPE").unwrap(),
                "alice@example.com".into(),
            );

            assert!(p.run(&email()).is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_eq_fail_different_type() -> TestResult {
            let p = Predicate::Equal(Select::from_str(".from").unwrap(), 42.into());

            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_gt() -> TestResult {
            let p = Predicate::GreaterThan(Select::from_str(".foo").unwrap(), (41.9).into());
            assert!(p.run(&simple())?);
            Ok(())
        }

        #[test_log::test]
        fn test_gt_fail() -> TestResult {
            let p = Predicate::GreaterThan(Select::from_str(".foo").unwrap(), 42.into());
            assert!(!p.run(&simple())?);
            Ok(())
        }

        #[test_log::test]
        fn test_gte() -> TestResult {
            let p = Predicate::GreaterThanOrEqual(Select::from_str(".foo").unwrap(), 42.into());
            assert!(p.run(&simple())?);
            Ok(())
        }

        #[test_log::test]
        fn test_gte_fail() -> TestResult {
            let p = Predicate::GreaterThanOrEqual(Select::from_str(".foo").unwrap(), (42.1).into());
            assert!(!p.run(&simple())?);
            Ok(())
        }

        #[test_log::test]
        fn test_lt() -> TestResult {
            let p = Predicate::LessThan(Select::from_str(".foo").unwrap(), (42.1).into());
            assert!(p.run(&simple())?);
            Ok(())
        }

        #[test_log::test]
        fn test_lt_fail() -> TestResult {
            let p = Predicate::LessThan(Select::from_str(".foo").unwrap(), 42.into());
            assert!(!p.run(&simple())?);
            Ok(())
        }

        #[test_log::test]
        fn test_lte() -> TestResult {
            let p = Predicate::LessThanOrEqual(Select::from_str(".foo").unwrap(), 42.into());
            assert!(p.run(&simple())?);
            Ok(())
        }

        #[test_log::test]
        fn test_lte_fail() -> TestResult {
            let p = Predicate::LessThanOrEqual(Select::from_str(".foo").unwrap(), (41.9).into());
            assert!(!p.run(&simple())?);
            Ok(())
        }

        #[test_log::test]
        fn test_like() -> TestResult {
            let p = Predicate::Like(Select::from_str(".from").unwrap(), "alice@*".into());
            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_like_fail_concrete() -> TestResult {
            let p = Predicate::Like(Select::from_str(".from").unwrap(), "NOPE".into());
            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_like_fail_left_star() -> TestResult {
            let p = Predicate::Like(Select::from_str(".from").unwrap(), "*NOPE".into());
            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_like_fail_right_star() -> TestResult {
            let p = Predicate::Like(Select::from_str(".from").unwrap(), "NOPE*".into());
            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_like_fail_both_stars() -> TestResult {
            let p = Predicate::Like(Select::from_str(".from").unwrap(), "*NOPE*".into());
            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_not() -> TestResult {
            let p = Predicate::Not(Box::new(Predicate::Equal(
                Select::from_str(".from").unwrap(),
                "NOPE".into(),
            )));

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_double_negative() -> TestResult {
            let p = Predicate::Not(Box::new(Predicate::Not(Box::new(Predicate::Equal(
                Select::from_str(".from").unwrap(),
                "alice@example.com".into(),
            )))));

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_not_fail() -> TestResult {
            let p = Predicate::Not(Box::new(Predicate::Equal(
                Select::from_str(".from").unwrap(),
                "alice@example.com".into(),
            )));

            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_and_both_succeed() -> TestResult {
            let p = Predicate::And(vec![
                Predicate::Equal(
                    Select::from_str(".from").unwrap(),
                    "alice@example.com".into(),
                ),
                Predicate::Equal(
                    Select::from_str(".subject").unwrap(),
                    "Quarterly Reports".into(),
                ),
            ]);

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_and_left_fail() -> TestResult {
            let p = Predicate::And(vec![
                Predicate::Equal(Select::from_str(".from").unwrap(), "NOPE".into()),
                Predicate::Equal(
                    Select::from_str(".subject").unwrap(),
                    "Quarterly Reports".into(),
                ),
            ]);

            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_and_right_fail() -> TestResult {
            let p = Predicate::And(vec![
                Predicate::Equal(
                    Select::from_str(".from").unwrap(),
                    "alice@example.com".into(),
                ),
                Predicate::Equal(Select::from_str(".subject").unwrap(), "NOPE".into()),
            ]);

            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_and_both_fail() -> TestResult {
            let p = Predicate::And(vec![
                Predicate::Equal(Select::from_str(".from").unwrap(), "NOPE".into()),
                Predicate::Equal(Select::from_str(".subject").unwrap(), "NOPE".into()),
            ]);

            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_or_both_succeed() -> TestResult {
            let p = Predicate::Or(vec![
                Predicate::Equal(
                    Select::from_str(".from").unwrap(),
                    "alice@example.com".into(),
                ),
                Predicate::Equal(
                    Select::from_str(".subject").unwrap(),
                    "Quarterly Reports".into(),
                ),
            ]);

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_or_left_fail() -> TestResult {
            let p = Predicate::Or(vec![
                Predicate::Equal(Select::from_str(".from").unwrap(), "NOPE".into()),
                Predicate::Equal(
                    Select::from_str(".subject").unwrap(),
                    "Quarterly Reports".into(),
                ),
            ]);

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_or_right_fail() -> TestResult {
            let p = Predicate::Or(vec![
                Predicate::Equal(
                    Select::from_str(".from").unwrap(),
                    "alice@example.com".into(),
                ),
                Predicate::Equal(Select::from_str(".subject").unwrap(), "NOPE".into()),
            ]);

            assert!(p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_or_both_fail() -> TestResult {
            let p = Predicate::Or(vec![
                Predicate::Equal(Select::from_str(".from").unwrap(), "NOPE".into()),
                Predicate::Equal(Select::from_str(".subject").unwrap(), "NOPE".into()),
            ]);

            assert!(!p.run(&email())?);
            Ok(())
        }

        #[test_log::test]
        fn test_all() -> TestResult {
            let p = Predicate::All(
                Select::from_str(".input[]").unwrap(),
                Box::new(Predicate::LessThan(
                    Select::from_str(".").unwrap(),
                    100.into(),
                )),
            );

            assert!(p.run(&wasm())?);
            Ok(())
        }

        #[test_log::test]
        fn test_all_failure() -> TestResult {
            let p = Predicate::All(
                Select::from_str(".input[]").unwrap(),
                Box::new(Predicate::LessThan(
                    Select::from_str(".").unwrap(),
                    1.into(),
                )),
            );

            assert!(!p.run(&wasm())?);
            Ok(())
        }

        #[test_log::test]
        fn test_any_all_succeed() -> TestResult {
            let p = Predicate::Any(
                Select::from_str(".input[]").unwrap(),
                Box::new(Predicate::LessThan(
                    Select::from_str(".").unwrap(),
                    100.into(),
                )),
            );

            assert!(p.run(&wasm())?);
            Ok(())
        }

        #[test_log::test]
        fn test_any_not_all() -> TestResult {
            let p = Predicate::Any(
                Select::from_str(".input[]").unwrap(),
                Box::new(Predicate::LessThan(
                    Select::from_str(".").unwrap(),
                    1.into(),
                )),
            );

            assert!(p.run(&wasm())?);
            Ok(())
        }

        #[test_log::test]
        fn test_any_all_fail() -> TestResult {
            let p = Predicate::Any(
                Select::from_str(".input[]").unwrap(),
                Box::new(Predicate::LessThan(
                    Select::from_str(".").unwrap(),
                    0.into(),
                )),
            );

            assert!(!p.run(&wasm())?);
            Ok(())
        }

        #[test_log::test]
        fn test_alternate_all_and_any() -> TestResult {
            // ["all", ".a", ["any", ".b[]", ["==", ".", 0]]]
            let p = Predicate::All(
                Select::from_str(".a").unwrap(),
                Box::new(Predicate::Any(
                    Select::from_str(".b[]").unwrap(),
                    Box::new(Predicate::Equal(Select::from_str(".").unwrap(), 0.into())),
                )),
            );

            let nested_data = ipld!(
                {
                    "a": [
                        {
                            "b": {
                                "c": 0, // Yep
                                "d": 0, // Yep
                                "e": 1  // Nope, but ok because "any"
                            },
                            "not-b": "ignore"
                        },
                        {
                            "also-not-b": "ignore",
                            "b": [-1, 0, 1]
                        }
                    ]
                }
            );

            assert!(p.run(&nested_data)?);
            Ok(())
        }

        #[test_log::test]
        fn test_alternate_fail_all_and_any() -> TestResult {
            // ["all", ".a", ["any", ".b[]", ["==", ".", 0]]]
            let p = Predicate::All(
                Select::from_str(".a").unwrap(),
                Box::new(Predicate::Any(
                    Select::from_str(".b[]").unwrap(),
                    Box::new(Predicate::Equal(Select::from_str(".").unwrap(), 0.into())),
                )),
            );

            let nested_data = ipld!(
                {
                    "a": [
                        {
                            "b": {
                                "c": 0, // Yep
                                "d": 0, // Yep
                                "e": 1  // Nope, but ok because "any"
                            },
                            "not-b": "ignore"
                        },
                        {
                            "also-not-b": "ignore",
                            "b": [-1, 42, 1] // No 0, so fail "all"
                        }
                    ]
                }
            );

            assert!(!p.run(&nested_data)?);
            Ok(())
        }

        #[test_log::test]
        fn test_alternate_any_and_all() -> TestResult {
            // ["any", ".a", ["all", ".b[]", ["==", ".", 0]]]
            let p = Predicate::Any(
                Select::from_str(".a").unwrap(),
                Box::new(Predicate::All(
                    Select::from_str(".b[]").unwrap(),
                    Box::new(Predicate::Equal(Select::from_str(".").unwrap(), 0.into())),
                )),
            );

            let nested_data = ipld!(
                {
                    "a": [
                        {
                            "b": {
                                "c": 0, // Yep
                                "d": 0, // Yep
                                "e": 1  // Nope, so fail this all, but...
                            },
                            "not-b": "ignore"
                        },
                        {
                            "also-not-b": "ignore",
                            "b": [0, 0, 0] // This all succeeds, so the outer "any" succeeds
                        }
                    ]
                }
            );

            assert!(p.run(&nested_data)?);
            Ok(())
        }

        #[test_log::test]
        fn test_alternate_fail_any_and_all() -> TestResult {
            // ["any", ".a", ["all", ".b[]", ["==", ".", 0]]]
            let p = Predicate::Any(
                Select::from_str(".a").unwrap(),
                Box::new(Predicate::All(
                    Select::from_str(".b[]").unwrap(),
                    Box::new(Predicate::Equal(Select::from_str(".").unwrap(), 0.into())),
                )),
            );

            let nested_data = ipld!(
                {
                    "a": [
                        {
                            "b": {
                                "c": 0, // Yep
                                "d": 0, // Yep
                                "e": 1  // Nope
                            },
                            "not-b": "ignore"
                        },
                        {
                            "also-not-b": "ignore",
                            "b": [-1, 42, 1] // Also nope, so fail
                        }
                    ]
                }
            );

            assert!(!p.run(&nested_data)?);
            Ok(())
        }

        #[test_log::test]
        fn test_deeply_alternate_any_and_all() -> TestResult {
            // ["any", ".a",
            //   ["all", ".b.c[]",
            //     ["any", ".d",
            //       ["all", ".e[]",
            //         ["==", ".f.g", 0]
            //       ]
            //     ]
            //   ]
            // ]
            let p = Predicate::Any(
                Select::from_str(".a").unwrap(),
                Box::new(Predicate::All(
                    Select::from_str(".b.c[]").unwrap(),
                    Box::new(Predicate::Any(
                        Select::from_str(".d").unwrap(),
                        Box::new(Predicate::All(
                            Select::from_str(".e[]").unwrap(),
                            Box::new(Predicate::Equal(
                                Select::from_str(".f.g").unwrap(),
                                0.into(),
                            )),
                        )),
                    )),
                )),
            );

            let deeply_nested_data = ipld!(
                {
                    // Any
                    "a": [
                        {
                            "b": {
                                "c": {
                                    // All
                                    "c1": {
                                        // Any
                                        "d": [
                                            {
                                                // All
                                                "e": {
                                                    "e1": {
                                                        "f": {
                                                            "g": 0
                                                        },
                                                        "nope": -10
                                                    },
                                                    "e2": {
                                                        "_": "not selected",
                                                        "f": {
                                                            "g": 0
                                                        },
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "c2": {
                                        // Any
                                        "*": "avoid",
                                        "d": [
                                            {
                                                // All
                                                "e": {
                                                    "e1": {
                                                        "f": {
                                                            "g": 0
                                                        },
                                                        "nope": -10
                                                    },
                                                    "e2": {
                                                        "_": "not selected",
                                                        "f": {
                                                            "g": 0
                                                        },
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    ],
                    "z": "doesn't read this"
                }
            );

            assert!(p.run(&deeply_nested_data)?);
            Ok(())
        }
    }
}
