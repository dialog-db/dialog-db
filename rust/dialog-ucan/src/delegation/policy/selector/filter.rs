//! jq-inspired filters.

use super::error::ParseError;
use nom::{
    self, IResult, Parser,
    branch::alt,
    bytes::complete::tag,
    character::complete::{alphanumeric1, char, digit1, satisfy},
    combinator::map_res,
    error::context,
    multi::{many0, many1},
    sequence::{delimited, preceded, terminated},
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as DeError, SeqAccess, Visitor},
    ser::SerializeSeq,
};
use std::{
    fmt::{self, Write},
    str::FromStr,
};

#[cfg(any(test, feature = "test_utils"))]
use arbitrary::{self, Arbitrary, Unstructured};

/// Filter variants.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Extract an array index (e.g. `[2]`).
    ArrayIndex(i32),

    /// Extract a field from a map (e.g. `["key"]` or `.key`).
    Field(String),

    /// Extract values from a collection (e.g. `.[]`).
    Values,

    /// Try-filter (i.e. `?`).
    Try(Box<Filter>),
}

impl Filter {
    /// Checks if the filter is a try-filter.
    #[must_use]
    pub fn is_in(&self, other: &Self) -> bool {
        match (self, other) {
            (Filter::ArrayIndex(a), Filter::ArrayIndex(b)) => a == b,
            (Filter::Field(a), Filter::Field(b)) => a == b,
            (Filter::Values, Filter::Values) => true,
            (Filter::ArrayIndex(_a), Filter::Values) => true,
            (Filter::Field(_k), Filter::Values) => true,
            (Filter::Try(a), Filter::Try(b)) => a.is_in(b),
            _ => false,
        }
    }

    /// Checks if the filter is a dot field.
    #[must_use]
    pub fn is_dot_field(&self) -> bool {
        match self {
            Filter::Field(k) => {
                if let Some(first) = k.chars().next() {
                    (first.is_alphabetic() || first == '_')
                        && k.chars().all(|c| char::is_alphanumeric(c) || c == '_')
                } else {
                    false
                }
            }
            Filter::ArrayIndex(_) | Filter::Values | Filter::Try(_) => false,
        }
    }
}

fn write_json_string(f: &mut fmt::Formatter<'_>, s: &str) -> fmt::Result {
    f.write_str("\"")?;
    for ch in s.chars() {
        match ch {
            '\"' => f.write_str("\\\"")?,
            '\\' => f.write_str("\\\\")?,
            '\n' => f.write_str("\\n")?,
            '\r' => f.write_str("\\r")?,
            '\t' => f.write_str("\\t")?,
            '\u{08}' => f.write_str("\\b")?, // backspace
            '\u{0C}' => f.write_str("\\f")?, // form feed
            c if (c as u32) < 0x20 => {
                // C0 control → \uXXXX
                write!(f, "\\u{:04X}", c as u32)?;
            }
            c => f.write_char(c)?,
        }
    }
    f.write_str("\"")
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Filter::ArrayIndex(i) => write!(f, "[{i}]"),
            Filter::Field(k) => {
                // Be conservative: only use dot form for safe identifiers.
                let dot_ok = self.is_dot_field()
                    && !k.chars().any(|c| c.is_control() || c == '"' || c == '\\');
                if dot_ok {
                    write!(f, ".{k}")
                } else {
                    f.write_str("[")?;
                    write_json_string(f, k)?;
                    f.write_str("]")
                }
            }
            Filter::Values => f.write_str("[]"),
            Filter::Try(inner) => write!(f, "{inner}?"),
        }
    }
}

/// Parse a filter from a string.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse(input: &str) -> IResult<&str, Filter> {
    let p = alt((parse_try, parse_non_try));
    context("selector_op", p).parse(input)
}

/// Parse a try-filter (`?`).
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_try(input: &str) -> IResult<&str, Filter> {
    let p = map_res(
        terminated(parse_non_try, many1(tag("?"))),
        |found: Filter| {
            let mut work = found.clone();
            if let Filter::Try(inner) = found
                && let Filter::Try(nested) = *inner
            {
                work = *nested;
            }
            Ok::<Filter, ()>(Filter::Try(Box::new(work)))
        },
    );

    context("try", p).parse(input)
}

/// Parses a filter that is a dot field followed by `?`, e.g. `.foo?`.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_try_dot_field(input: &str) -> IResult<&str, Filter> {
    let p = map_res(
        terminated(parse_dot_field, many1(tag("?"))),
        |found: Filter| Ok::<Filter, ()>(Filter::Try(Box::new(found))),
    );

    context("try", p).parse(input)
}

/// Parses a filter not ending in `?`, e.g. `["foo"]`, `.foo`, or `[2]`.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_non_try(input: &str) -> IResult<&str, Filter> {
    let p = alt((parse_values, parse_field, parse_array_index));
    context("non_try", p).parse(input)
}

/// Parses an array index, e.g. `[2]` or `[-42]`.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_array_index(input: &str) -> IResult<&str, Filter> {
    let num = nom::combinator::recognize(preceded(nom::combinator::opt(tag("-")), digit1));

    let array_index = map_res(delimited(char('['), num, char(']')), |found| {
        let idx = i32::from_str(found).map_err(|_| ())?;
        Ok::<Filter, ()>(Filter::ArrayIndex(idx))
    });

    context("array_index", array_index).parse(input)
}

/// Parses values from a collection.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_values(input: &str) -> IResult<&str, Filter> {
    context("values", tag("[]"))
        .parse(input)
        .map(|(rest, _)| (rest, Filter::Values))
}

/// Parses a field that is either a dot field or a delimited field, e.g. `["foo"]` or `.foo`.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_field(input: &str) -> IResult<&str, Filter> {
    let p = alt((parse_delim_field, parse_dot_field));

    context("map_field", p).parse(input)
}

/// Parses a field that starts with `.`, e.g. `.foo` or `._foo`.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_dot_field(input: &str) -> IResult<&str, Filter> {
    let p = alt((parse_dot_alpha_field, parse_dot_underscore_field));
    context("dot_field", p).parse(input)
}

/// Checks if a character is allowed in a dot field.
fn is_allowed_in_dot_field(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn dot_starter(input: &str) -> IResult<&str, char> {
    let (i, _) = ch('.').parse(input)?;
    satisfy(|c| c.is_alphabetic() || c == '_').parse(i)
}

/// Parses an alphabetic field that starts with `.`, e.g. `.foo`.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_dot_alpha_field(input: &str) -> IResult<&str, Filter> {
    let (i, first) = dot_starter(input)?;
    let (i, tail) = many0(satisfy(is_allowed_in_dot_field)).parse(i)?;

    let mut s = String::with_capacity(1 + tail.len());
    s.push(first);
    for c in tail {
        s.push(c);
    }

    Ok((i, Filter::Field(s)))
}

/// Parses a field that starts with `._`, e.g. `._foo`.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_dot_underscore_field(input: &str) -> IResult<&str, Filter> {
    let p = map_res(preceded(tag("._"), alphanumeric1), |found: &str| {
        let key = format!("{}{}", '_', found);
        Ok::<Filter, ()>(Filter::Field(key))
    });

    context("dot_field", p).parse(input)
}

/// Parses a field that is empty quotes, e.g. `[""]`.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_empty_quotes_field(input: &str) -> IResult<&str, Filter> {
    let p = map_res(tag("[\"\"]"), |_: &str| {
        Ok::<Filter, ()>(Filter::Field(String::new()))
    });

    context("empty_quotes_field", p).parse(input)
}

/// Parses a string that is either a unicode character or a space.
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn unicode_or_space(input: &str) -> IResult<&str, &str> {
    #[derive(Copy, Clone, PartialEq, Debug)]
    enum Status {
        Looking,
        FoundQuote,
        Done,
        Failed,
    }

    let (status, len) =
        input
            .as_bytes()
            .iter()
            .fold((Status::Looking, 0), |(status, len), byte| {
                if status == Status::Failed {
                    return (status, len);
                }

                if status == Status::Done {
                    return (status, len);
                }

                let c = char::from(*byte);

                if status == Status::FoundQuote {
                    if c == ']' {
                        return (Status::Done, len + 1);
                    }

                    return (Status::Looking, len + 1);
                }

                if c == '"' {
                    return (Status::FoundQuote, len + 1);
                }

                if c == ' ' || (!nom_unicode::is_whitespace(c) && !nom_unicode::is_control(c)) {
                    return (Status::Looking, len + 1);
                }

                (Status::Failed, 0)
            });

    match (status, len) {
        (Status::Done, len) => Ok((&input[len - 2..], &input[..len - 2])),
        _ => Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TakeWhile1,
        ))),
    }
}

/// Parses a delimited field, e.g. `["foo"]` or `["foo bar"]`
///
/// # Errors
///
/// Returns a `nom` error if the parser fails to match.
pub fn parse_delim_field(input: &str) -> IResult<&str, Filter> {
    let p = delimited(ch('['), json_string.map(Filter::Field), ch(']'));
    context("delimited_field", alt((p, parse_empty_quotes_field))).parse(input)
}

impl Serialize for Filter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            // `[ <integer> ]`
            Filter::ArrayIndex(i) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("idx")?;
                seq.serialize_element(i)?;
                seq.end()
            }

            // `[ "field", <string> ]`
            Filter::Field(k) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("field")?;
                seq.serialize_element(k)?;
                seq.end()
            }

            // `[ "values" ]`
            Filter::Values => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                seq.serialize_element("values")?;
                seq.end()
            }

            // `[ "try", <Filter> ]`
            Filter::Try(inner) => {
                let mut seq = serializer.serialize_seq(Some(2))?;
                seq.serialize_element("try")?;
                seq.serialize_element(inner)?;
                seq.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FilterVisitor;

        impl<'de> Visitor<'de> for FilterVisitor {
            type Value = Filter;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    r#"a tagged sequence like ["idx", 3], ["field", "foo"], ["values"], or ["try", ...]"#
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let tag: String = seq
                    .next_element()?
                    .ok_or_else(|| A::Error::custom("missing tag"))?;

                match tag.as_str() {
                    "idx" => {
                        let i: i32 = seq
                            .next_element()?
                            .ok_or_else(|| A::Error::custom("missing index"))?;
                        Ok(Filter::ArrayIndex(i))
                    }
                    "field" => {
                        let k: String = seq
                            .next_element()?
                            .ok_or_else(|| A::Error::custom("missing field"))?;
                        Ok(Filter::Field(k))
                    }
                    "values" => Ok(Filter::Values),
                    "try" => {
                        let inner: Filter = seq
                            .next_element()?
                            .ok_or_else(|| A::Error::custom("missing inner"))?;
                        Ok(Filter::Try(Box::new(inner)))
                    }
                    other => Err(A::Error::custom(format!("unknown tag {other:?}"))),
                }
            }
        }

        deserializer.deserialize_seq(FilterVisitor)
    }
}

/// Errors that can occur while parsing filter text.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum FilterTextError {
    /// End of input reached unexpectedly.
    #[error("unexpected end of input")]
    Eof,

    /// Expected a specific character, but did not get it.
    #[error("expected closing {0}")]
    Expected(char),

    /// Invalid escape sequence.
    #[error("invalid escape sequence")]
    BadEscape,

    /// Invalid unicode
    #[error("invalid unicode")]
    BadUnicode,

    /// Invalid number.
    #[error("invalid number")]
    BadNumber,

    /// Unexpected trailing input after parsing.
    #[error("trailing input: {0}")]
    Trailing(String),
}

/// Errors that can occur while parsing a filter.
#[derive(Debug, Clone, Copy)]
pub enum FilterParseError {
    /// End of input reached unexpectedly.
    Eof,

    /// Expected a specific character, but did not get it.
    Expected(char),

    /// Bad escape sequence.
    BadEscape,

    /// Bad unicode escape.
    BadUnicode,
}

fn hex4(s: &str, start: usize) -> Result<(u16, usize), FilterParseError> {
    // returns (value, next_index AFTER the 4 hex digits)
    let bs = s.as_bytes();
    if start + 4 > bs.len() {
        return Err(FilterParseError::Eof);
    }
    let mut v: u16 = 0;
    for j in 0..4 {
        let focus = bs.get(start + j).ok_or(FilterParseError::Eof)?;

        v = (v << 4)
            | match focus {
                b'0'..=b'9' => u16::from(focus - b'0'),
                b'a'..=b'f' => u16::from(focus - b'a' + 10),
                b'A'..=b'F' => u16::from(focus - b'A' + 10),
                _ => return Err(FilterParseError::BadUnicode),
            };
    }
    Ok((v, start + 4))
}

/// Parse a JSON string literal starting at a `"`
///
/// # Returns
/// `(decoded_string, rest_after_closing_quote)`
fn decode_json_string_literal(input: &str) -> Result<(String, &str), FilterParseError> {
    let b = input.as_bytes();
    if b.first() != Some(&b'"') {
        return Err(FilterParseError::Expected('"'));
    }
    let mut out = String::new();
    // i points to the next byte to *read*
    let mut i = 1; // after opening quote

    #[allow(clippy::indexing_slicing)]
    while i < b.len() {
        match b[i] {
            b'"' => {
                // closing quote; return slice AFTER it
                return Ok((out, &input[i + 1..]));
            }
            b'\\' => {
                i += 1;
                if i >= b.len() {
                    return Err(FilterParseError::Eof);
                }
                match b[i] as char {
                    '"' => {
                        out.push('"');
                        i += 1;
                    }
                    '\\' => {
                        out.push('\\');
                        i += 1;
                    }
                    '/' => {
                        out.push('/');
                        i += 1;
                    }
                    'b' => {
                        out.push('\u{0008}');
                        i += 1;
                    } // backspace
                    'f' => {
                        out.push('\u{000C}');
                        i += 1;
                    } // form feed
                    'n' => {
                        out.push('\n');
                        i += 1;
                    }
                    'r' => {
                        out.push('\r');
                        i += 1;
                    }
                    't' => {
                        out.push('\t');
                        i += 1;
                    }
                    'u' => {
                        // \uXXXX (maybe followed by a surrogate pair)
                        let (cu1, next) = hex4(input, i + 1)?;
                        let u = u32::from(cu1);
                        if (0xD800..=0xDBFF).contains(&cu1) {
                            // expect \uDC00–\uDFFF next
                            let bs = input.as_bytes();
                            if next + 6 > bs.len() || bs[next] != b'\\' || bs[next + 1] != b'u' {
                                return Err(FilterParseError::BadUnicode);
                            }
                            let (cu2, next2) = hex4(input, next + 2)?;
                            if !(0xDC00..=0xDFFF).contains(&cu2) {
                                return Err(FilterParseError::BadUnicode);
                            }
                            let hi = u - 0xD800;
                            let lo = u32::from(cu2) - 0xDC00;
                            let scalar = 0x10000 + ((hi << 10) | lo);
                            let ch = char::from_u32(scalar).ok_or(FilterParseError::BadUnicode)?;
                            out.push(ch);
                            i = next2; // advance past the pair
                        } else if (0xDC00..=0xDFFF).contains(&cu1) {
                            return Err(FilterParseError::BadUnicode);
                        } else {
                            let ch = char::from_u32(u).ok_or(FilterParseError::BadUnicode)?;
                            out.push(ch);
                            i = next; // advance past XXXX
                        }
                    }
                    _ => return Err(FilterParseError::BadEscape),
                }
            }
            _ => {
                // regular UTF-8 char
                let ch = input[i..].chars().next().ok_or(FilterParseError::Eof)?;
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    Err(FilterParseError::Eof)
}

use nom::{
    character::complete::char as ch,
    error::{Error as NomError, ErrorKind},
};

fn json_string(input: &str) -> IResult<&str, String> {
    match decode_json_string_literal(input) {
        Ok((val, rest)) => Ok((rest, val)),
        Err(FilterParseError::Expected(_)) => {
            Err(nom::Err::Error(NomError::new(input, ErrorKind::Char)))
        }
        Err(FilterParseError::Eof) => Err(nom::Err::Failure(NomError::new(input, ErrorKind::Eof))),
        Err(FilterParseError::BadEscape | FilterParseError::BadUnicode) => {
            Err(nom::Err::Failure(NomError::new(input, ErrorKind::Escaped)))
        }
    }
}

impl std::str::FromStr for Filter {
    type Err = nom::Err<ParseError>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse(s).map_err(|e| nom::Err::Failure(ParseError::UnknownPattern(e.to_string())))? {
            ("", found) => Ok(found),
            (rest, _) => Err(nom::Err::Failure(ParseError::TrailingInput(rest.into()))),
        }
    }
}

#[cfg(any(test, feature = "test_utils"))]
impl<'a> Arbitrary<'a> for Filter {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self, arbitrary::Error> {
        enum Pick {
            ArrayIndex,
            Field,
            Values,
            Try,
        }

        match u.choose(&[Pick::ArrayIndex, Pick::Field, Pick::Values, Pick::Try])? {
            Pick::ArrayIndex => {
                let i = u.int_in_range(0..=99)?;
                Ok(Filter::ArrayIndex(i))
            }
            Pick::Field => {
                let s = u.arbitrary::<String>()?;
                Ok(Filter::Field(s))
            }
            Pick::Values => Ok(Filter::Values),
            Pick::Try => {
                let inner = Filter::arbitrary(u)?;
                Ok(Filter::Try(Box::new(inner)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions as pretty;
    use testresult::TestResult;

    mod serialization {
        use super::*;

        #[cfg(all(feature = "property_test", not(target_arch = "wasm32")))]
        mod proptest_tests {
            use super::*;
            use proptest::prelude::*;
            use proptest_arbitrary_interop::arb;

            proptest! {
                #[test]
                fn test_filter_round_trip_display_parse(filter in arb::<Filter>()) {
                    let serialized = filter.to_string();
                    let deserialized = serialized.parse(); //
                    let expected = {
                        let mut work = filter;
                        loop {
                            if let Filter::Try(inner) = work.clone() {
                                if let Filter::Try(nested) = *inner {
                                    work = Filter::Try(nested);
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                        work
                    };

                    prop_assert_eq!(Ok(expected), deserialized);
                }

                #[test]
                fn test_filter_round_trip_dag_cbor(filter in arb::<Filter>()) {
                    let serialized = serde_ipld_dagcbor::to_vec(&filter).unwrap();
                    let deserialized = serde_ipld_dagcbor::from_slice::<Filter>(&serialized);
                    prop_assert_eq!(Ok(filter), deserialized);
                }
            }
        }

        #[test_log::test]
        fn test_fails_on_empty() -> TestResult {
            let got = Filter::from_str("");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_on_bare_dot() -> TestResult {
            // NOTE this passes as a Selector, but not a Filter
            let got = Filter::from_str(".");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_on_multiple_bare_dots() -> TestResult {
            let got = Filter::from_str("..");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_on_leading_dots() -> TestResult {
            let got = Filter::from_str("..foo");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_on_empty_whitespace() -> TestResult {
            let got = Filter::from_str(" ");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_leading_whitespace() -> TestResult {
            let got = Filter::from_str(" .foo");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_trailing_whitespace() -> TestResult {
            let got = Filter::from_str(".foo ");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_values() -> TestResult {
            let got = Filter::from_str("[]");
            pretty::assert_eq!(got, Ok(Filter::Values));
            Ok(())
        }

        #[test_log::test]
        fn test_values_fails_inner_whitespace() -> TestResult {
            let got = Filter::from_str("[ ]");
            pretty::assert_eq!(got.is_err(), true);
            Ok(())
        }

        #[test_log::test]
        fn test_array_index_zero() -> TestResult {
            let got = Filter::from_str("[0]");
            pretty::assert_eq!(got, Ok(Filter::ArrayIndex(0)));
            Ok(())
        }

        #[test_log::test]
        fn test_array_index_small() -> TestResult {
            let got = Filter::from_str("[2]");
            pretty::assert_eq!(got, Ok(Filter::ArrayIndex(2)));
            Ok(())
        }

        #[test_log::test]
        fn test_array_index_large() -> TestResult {
            let got = Filter::from_str("[1234567890]");
            pretty::assert_eq!(got, Ok(Filter::ArrayIndex(1234567890)));
            Ok(())
        }

        #[test_log::test]
        fn test_array_from_end() -> TestResult {
            let got = Filter::from_str("[-42]");
            pretty::assert_eq!(got, Ok(Filter::ArrayIndex(-42)));
            Ok(())
        }

        #[test_log::test]
        fn test_array_fails_spaces() -> TestResult {
            let got = Filter::from_str("[ 42]");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_dot_field() -> TestResult {
            let got = Filter::from_str(".F0o");
            pretty::assert_eq!(got, Ok(Filter::Field("F0o".to_string())));
            Ok(())
        }

        #[test_log::test]
        fn test_dot_field_starting_underscore() -> TestResult {
            let got = Filter::from_str("._foo");
            pretty::assert_eq!(got, Ok(Filter::Field("_foo".to_string())));
            Ok(())
        }

        #[test_log::test]
        fn test_dot_field_trailing_underscore() -> TestResult {
            let got = Filter::from_str(".fO0_");
            pretty::assert_eq!(got, Ok(Filter::Field("fO0_".to_string())));
            Ok(())
        }

        #[test_log::test]
        fn test_fails_dot_field_with_leading_number() -> TestResult {
            let got = Filter::from_str(".1foo");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_dot_field_with_inner_symbol() -> TestResult {
            let got = Filter::from_str(".fo%o");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_delim_field() -> TestResult {
            let got = Filter::from_str(r#"["F0o"]"#);
            pretty::assert_eq!(got, Ok(Filter::Field("F0o".to_string())));
            Ok(())
        }

        #[test_log::test]
        fn test_delim_field_fails_without_quotes() -> TestResult {
            let got = Filter::from_str(r#"[F0o]"#);
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_delim_field_fails_if_missing_right_brace() -> TestResult {
            let got = Filter::from_str(r#"["F0o""#);
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_delim_field_starting_underscore() -> TestResult {
            let got = Filter::from_str(r#"["_foo"]"#);
            pretty::assert_eq!(got, Ok(Filter::Field("_foo".to_string())));
            Ok(())
        }

        #[test_log::test]
        fn test_delim_field_trailing_underscore() -> TestResult {
            let got = Filter::from_str(r#"["fO0_"]"#);
            pretty::assert_eq!(got, Ok(Filter::Field("fO0_".to_string())));
            Ok(())
        }

        #[test_log::test]
        fn test_delim_field_with_leading_number() -> TestResult {
            let got = Filter::from_str(r#"["1foo"]"#);
            pretty::assert_eq!(got, Ok(Filter::Field("1foo".to_string())));
            Ok(())
        }

        #[test_log::test]
        fn test_delim_field_with_inner_symbol() -> TestResult {
            let got = Filter::from_str(r#"[".fo%o"]"#);
            pretty::assert_eq!(got, Ok(Filter::Field(".fo%o".to_string())));
            Ok(())
        }

        #[test_log::test]
        fn test_try() -> TestResult {
            let got = Filter::from_str(".foo?");
            pretty::assert_eq!(
                got,
                Ok(Filter::Try(Box::new(Filter::Field("foo".to_string()))))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_parse_try() -> TestResult {
            let got = parse(".foo?");
            pretty::assert_eq!(
                got,
                Ok(("", Filter::Try(Box::new(Filter::Field("foo".to_string())))))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_tries_after_dot_field() -> TestResult {
            pretty::assert_eq!(
                Filter::from_str(".foo???????????????????"),
                Ok(Filter::Try(Box::new(Filter::Field("foo".to_string()))))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_parse_multiple_tries_after_dot_field() -> TestResult {
            pretty::assert_eq!(
                parse(".foo???????????????????"),
                Ok(("", Filter::Try(Box::new(Filter::Field("foo".to_string())))))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_parse_multiple_tries_after_dot_field_trailing() -> TestResult {
            pretty::assert_eq!(
                parse(".foo???????????????????abc"),
                Ok((
                    "abc",
                    Filter::Try(Box::new(Filter::Field("foo".to_string())))
                ))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_parse_many0_multiple_tries_after_dot_field() -> TestResult {
            pretty::assert_eq!(
                parse(".foo???????????????????abc"),
                Ok((
                    "abc",
                    Filter::Try(Box::new(Filter::Field("foo".to_string())))
                ))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_tries_after_delim_field() -> TestResult {
            pretty::assert_eq!(
                Filter::from_str(r#"["foo"]???????"#),
                Ok(Filter::Try(Box::new(Filter::Field("foo".to_string()))))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_tries_after_delim_field_inner_questionmarks() -> TestResult {
            let got = Filter::from_str(r#"["f?o"]???????"#);
            pretty::assert_eq!(
                got,
                Ok(Filter::Try(Box::new(Filter::Field("f?o".to_string()))))
            );
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_tries_after_values() -> TestResult {
            let got = Filter::from_str("[]???????");
            pretty::assert_eq!(got, Ok(Filter::Try(Box::new(Filter::Values))));
            Ok(())
        }

        #[test_log::test]
        fn test_multiple_tries_after_index() -> TestResult {
            let got = Filter::from_str("[42]???????");
            pretty::assert_eq!(got, Ok(Filter::Try(Box::new(Filter::ArrayIndex(42)))));
            Ok(())
        }

        #[test_log::test]
        fn test_fails_bare_try() -> TestResult {
            let got = Filter::from_str("?");
            assert!(got.is_err());
            Ok(())
        }

        #[test_log::test]
        fn test_fails_dot_try() -> TestResult {
            let got = Filter::from_str(".?");
            assert!(got.is_err());
            Ok(())
        }
    }
}
