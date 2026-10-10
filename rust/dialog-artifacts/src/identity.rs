//! The identities of values: their [merkle references](merkle_reference).
//!
//! A value's reference is keyed by its type, so the type is part of its
//! identity. Dialog's value types are:
//!
//! - [`NUMBER`] for every number, whose content is the one-band encoding
//!   [`write_number`] produces;
//! - the core text and boolean of [`merkle_reference`];
//! - plain bytes, referenced by their BLAKE3 hash like any blob;
//! - [`ENTITY`], [`SYMBOL`] and [`RECORD`], whose content is their text or
//!   bytes.
//!
//! A number's identity is its value alone, not the band the value index
//! keeps it in today or how it was written: a natural `5`, an integer `5` and
//! a float `5.0` are one number. The index still keeps naturals, integers and
//! floats apart, so until it moves to one band, two definitions whose only
//! difference is `5` against `5.0` have one identity but read different
//! claims.

use merkle_reference::{Refer, Reference, Type};

use crate::Value;

/// The type of numbers.
pub const NUMBER: Type = Type::from_reference(Reference::from_hex(
    "d57f5f02d3de4a0acf1ee1e35987e05f83bc7a6330e4bc9bae1e188f3550e89e",
));
/// The type of entities: text that parses as a URI.
pub const ENTITY: Type = Type::from_reference(Reference::from_hex(
    "50cceee37496c542516bb9c7cf727f4ed0615f61dc5856aa9b71b52df71a8d2d",
));
/// The type of symbols: `namespace/name` text.
pub const SYMBOL: Type = Type::from_reference(Reference::from_hex(
    "0f69ec853e584e7c9e983e9b44eb02794186187ca9ee7798df795cdff703a36b",
));
/// The type of records: opaque bytes.
pub const RECORD: Type = Type::from_reference(Reference::from_hex(
    "419cfed454524033b3fc368ea2d718640f0ac99760c033353c4120c822c19589",
));

/// The class byte of negative infinity.
const NEGATIVE_INFINITY: u8 = 0x01;
/// The class byte of a negative number at least one in magnitude.
const NEGATIVE_LARGE: u8 = 0x02;
/// The class byte of a negative number less than one in magnitude.
const NEGATIVE_SMALL: u8 = 0x03;
/// The class byte of zero.
const ZERO: u8 = 0x04;
/// The class byte of a positive number less than one.
const POSITIVE_SMALL: u8 = 0x05;
/// The class byte of a positive number at least one.
const POSITIVE_LARGE: u8 = 0x06;
/// The class byte of positive infinity.
const POSITIVE_INFINITY: u8 = 0x07;
/// The class byte of NaN.
const NAN: u8 = 0x08;
/// The exponent magnitudes written in one byte.
const ONE_BYTE: u32 = 248;

/// A number, as a natural, an integer or a float holds it. The three are
/// one domain: equal numbers have one encoding whichever holds them.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    /// A whole number that is not negative.
    Natural(u128),
    /// A whole number.
    Integer(i128),
    /// A float.
    Float(f64),
}

/// A finite non-zero number: a sign, the power of two of its leading bit,
/// and the bits after the leading one, left-aligned.
struct Finite {
    negative: bool,
    exponent: i32,
    fraction: u128,
}

enum Class {
    Special(u8),
    Finite(Finite),
}

impl Number {
    fn class(self) -> Class {
        match self {
            Self::Natural(0) | Self::Integer(0) => Class::Special(ZERO),
            Self::Natural(magnitude) => Class::Finite(whole(false, magnitude)),
            Self::Integer(value) => Class::Finite(whole(value < 0, value.unsigned_abs())),
            Self::Float(value) => float(value),
        }
    }
}

fn whole(negative: bool, magnitude: u128) -> Finite {
    let leading = magnitude.leading_zeros();
    Finite {
        negative,
        exponent: 127 - leading as i32,
        fraction: magnitude.checked_shl(leading + 1).unwrap_or(0),
    }
}

fn float(value: f64) -> Class {
    let raw = value.to_bits();
    let negative = raw >> 63 == 1;
    let exponent = ((raw >> 52) & 0x7ff) as i32;
    let mantissa = raw & ((1u64 << 52) - 1);
    if exponent == 0x7ff {
        return Class::Special(if mantissa != 0 {
            NAN
        } else if negative {
            NEGATIVE_INFINITY
        } else {
            POSITIVE_INFINITY
        });
    }
    if exponent == 0 && mantissa == 0 {
        return Class::Special(ZERO);
    }
    let (exponent, fraction) = if exponent == 0 {
        // Subnormal: the value is mantissa × 2^-1074.
        let leading = 63 - mantissa.leading_zeros() as i32;
        let fraction = u128::from(mantissa)
            .checked_shl(128 - leading as u32)
            .unwrap_or(0);
        (leading - 1074, fraction)
    } else {
        (exponent - 1023, u128::from(mantissa) << 76)
    };
    Class::Finite(Finite {
        negative,
        exponent,
        fraction,
    })
}

/// Appends a number's one-band encoding to `out`.
///
/// It follows SQLite4's numeric key encoding with binary digits in place of
/// decimal ones:
///
/// - a class byte: negative infinity, negative at least one in magnitude,
///   negative less than one, zero, positive less than one, positive at least
///   one, positive infinity, NaN, in that order;
/// - for a finite number other than zero, the magnitude of the power of two
///   of its leading bit: `e` itself when the number is at least one, and
///   `-e - 1` with every byte inverted when it is less than one. The
///   magnitude takes one byte below 248 and two bytes otherwise, the first
///   `248 + (m - 248) / 256`, so it has one spelling and sorts as it counts;
/// - then the bits after the leading one in groups of seven, each group
///   shifted left by one with the low bit set when another group follows,
///   and at least one group;
/// - every byte after the class inverted for a negative number.
///
/// So every number has one encoding, whether a natural, an integer or a
/// float holds it, encodings sort as their numbers do, `-0.0` is `0.0`, and
/// NaN is one value. Every 128-bit integer has a one-byte exponent.
pub fn write_number(number: Number, out: &mut Vec<u8>) {
    match number.class() {
        Class::Special(class) => out.push(class),
        Class::Finite(Finite {
            negative,
            exponent,
            mut fraction,
        }) => {
            let large = exponent >= 0;
            out.push(match (negative, large) {
                (true, true) => NEGATIVE_LARGE,
                (true, false) => NEGATIVE_SMALL,
                (false, false) => POSITIVE_SMALL,
                (false, true) => POSITIVE_LARGE,
            });
            let start = out.len();
            let magnitude = if large {
                exponent.unsigned_abs()
            } else {
                exponent.unsigned_abs() - 1
            };
            if magnitude < ONE_BYTE {
                out.push(magnitude as u8);
            } else {
                let rest = magnitude - ONE_BYTE;
                out.push((ONE_BYTE + (rest >> 8)) as u8);
                out.push((rest & 0xFF) as u8);
            }
            if !large {
                for byte in &mut out[start..] {
                    *byte = !*byte;
                }
            }
            let bits = if fraction == 0 {
                0
            } else {
                128 - fraction.trailing_zeros()
            };
            let groups = bits.div_ceil(7).max(1);
            for group in 0..groups {
                let chunk = (fraction >> 121) as u8;
                fraction <<= 7;
                let more = u8::from(group + 1 < groups);
                out.push(chunk << 1 | more);
            }
            if negative {
                for byte in &mut out[start..] {
                    *byte = !*byte;
                }
            }
        }
    }
}

/// A number's one-band encoding.
pub fn number_content(number: Number) -> Vec<u8> {
    let mut content = Vec::with_capacity(8);
    write_number(number, &mut content);
    content
}

impl Value {
    /// The value's identity: its merkle reference under its value type.
    pub fn reference(&self) -> Reference {
        match self {
            Value::Bytes(bytes) => Reference::of_bytes(bytes),
            Value::Entity(entity) => ENTITY.refer(entity.as_str().as_bytes()),
            Value::Boolean(value) => value.refer(),
            Value::String(text) => text.refer(),
            Value::UnsignedInt(value) => NUMBER.refer(&number_content(Number::Natural(*value))),
            Value::SignedInt(value) => NUMBER.refer(&number_content(Number::Integer(*value))),
            Value::Float(value) => NUMBER.refer(&number_content(Number::Float(*value))),
            Value::Record(bytes) => RECORD.refer(bytes),
            Value::Symbol(symbol) => SYMBOL.refer(symbol.to_string().as_bytes()),
        }
    }
}

impl Refer for Value {
    fn refer(&self) -> Reference {
        self.reference()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Entity, Relation};
    use std::cmp::Ordering;

    #[test]
    fn it_names_the_value_types() {
        for (name, ty) in [
            ("number:", NUMBER),
            ("entity:", ENTITY),
            ("symbol:", SYMBOL),
            ("record:", RECORD),
        ] {
            assert_eq!(Type::named(name), ty, "{name}");
        }
    }

    #[test]
    fn it_encodes_numbers_in_one_band() {
        assert_eq!(number_content(Number::Integer(0)), [0x04]);
        assert_eq!(number_content(Number::Float(-0.0)), [0x04]);
        assert_eq!(number_content(Number::Integer(1)), [0x06, 0x00, 0x00]);
        assert_eq!(number_content(Number::Integer(5)), [0x06, 0x02, 0x40]);
        assert_eq!(number_content(Number::Integer(-1)), [0x02, 0xFF, 0xFF]);
        assert_eq!(number_content(Number::Float(0.5)), [0x05, 0xFF, 0x00]);
        assert_eq!(number_content(Number::Float(0.75)), [0x05, 0xFF, 0x80]);
        // 2^-300: the exponent's magnitude, 299, takes two bytes.
        assert_eq!(
            number_content(Number::Float(2f64.powi(-300))),
            [0x05, !248, !51, 0x00]
        );
        assert_eq!(number_content(Number::Float(f64::NAN)), [0x08]);
    }

    #[test]
    fn it_encodes_a_number_the_same_whatever_holds_it() {
        for (natural, integer, float) in [
            (0u128, 0i128, 0.0f64),
            (5, 5, 5.0),
            (1 << 100, 1 << 100, 2f64.powi(100)),
        ] {
            let content = number_content(Number::Natural(natural));
            assert_eq!(number_content(Number::Integer(integer)), content);
            assert_eq!(number_content(Number::Float(float)), content);
        }
        assert_eq!(
            number_content(Number::Integer(-7)),
            number_content(Number::Float(-7.0))
        );
    }

    #[test]
    fn it_sorts_numbers_as_their_values() {
        let numbers = [
            Number::Float(f64::NEG_INFINITY),
            Number::Float(f64::MIN),
            Number::Integer(i128::MIN),
            Number::Float(-1e20),
            Number::Integer(-65_537),
            Number::Float(-1.5),
            Number::Integer(-1),
            Number::Float(-0.75),
            Number::Float(-0.5),
            Number::Float(-2f64.powi(-300)),
            Number::Float(-f64::MIN_POSITIVE),
            Number::Float(-5e-324),
            Number::Integer(0),
            Number::Float(5e-324),
            Number::Float(f64::MIN_POSITIVE),
            Number::Float(2f64.powi(-300)),
            Number::Float(2f64.powi(-247)),
            Number::Float(2f64.powi(-2)),
            Number::Float(0.5),
            Number::Float(0.75),
            Number::Natural(1),
            Number::Float(1.0 + f64::EPSILON),
            Number::Float(1.5),
            Number::Integer(2),
            Number::Natural(255),
            Number::Integer(256),
            Number::Float(1e20),
            Number::Natural(1 << 100),
            Number::Natural((1 << 100) + 1),
            Number::Integer(i128::MAX),
            Number::Float(2f64.powi(127)),
            Number::Natural(u128::MAX),
            Number::Float(2f64.powi(247)),
            Number::Float(2f64.powi(248)),
            Number::Float(f64::MAX),
            Number::Float(f64::INFINITY),
            Number::Float(f64::NAN),
        ];
        let encodings: Vec<Vec<u8>> = numbers.iter().map(|n| number_content(*n)).collect();
        for (pair, encoded) in numbers.windows(2).zip(encodings.windows(2)) {
            assert!(
                encoded[0] < encoded[1],
                "{:?} {:02x?} sorts before {:?} {:02x?}",
                pair[0],
                encoded[0],
                pair[1],
                encoded[1]
            );
        }
    }

    /// Orders two finite numbers exactly, as a test oracle.
    fn exact_order(a: Number, b: Number) -> Ordering {
        fn whole(number: Number) -> Option<i128> {
            match number {
                Number::Integer(value) => Some(value),
                Number::Natural(value) => i128::try_from(value).ok(),
                Number::Float(_) => None,
            }
        }
        // An integer against a float: compare the integer with the float's
        // whole part, then the float's fraction against zero.
        fn against(integer: i128, float: f64) -> Ordering {
            const TOP: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;
            if float >= TOP {
                return Ordering::Less;
            }
            if float < -TOP {
                return Ordering::Greater;
            }
            let truncated = float.trunc();
            integer
                .cmp(&(truncated as i128))
                .then_with(|| 0f64.partial_cmp(&(float - truncated)).unwrap())
        }
        match (a, b) {
            (Number::Float(x), Number::Float(y)) => x.partial_cmp(&y).unwrap(),
            (Number::Float(x), other) => against(whole(other).unwrap(), x).reverse(),
            (other, Number::Float(y)) => against(whole(other).unwrap(), y),
            (x, y) => whole(x).unwrap().cmp(&whole(y).unwrap()),
        }
    }

    #[test]
    fn it_sorts_integers_and_floats_together_as_their_values() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut numbers = Vec::new();
        while numbers.len() < 3000 {
            let bits = next();
            numbers.push(match bits % 4 {
                0 => {
                    let float = f64::from_bits(next());
                    if !float.is_finite() {
                        continue;
                    }
                    Number::Float(float)
                }
                // Floats that are whole numbers, to tie with integers.
                1 => Number::Float((next() as i64 >> (bits % 60)) as f64),
                2 => Number::Integer(i128::from(next() as i64) >> (bits % 60)),
                _ => {
                    Number::Integer((i128::from(next()) << 64 | i128::from(next())) >> (bits % 120))
                }
            });
        }
        numbers.sort_by(|a, b| exact_order(*a, *b));
        for pair in numbers.windows(2) {
            let (a, b) = (number_content(pair[0]), number_content(pair[1]));
            match exact_order(pair[0], pair[1]) {
                Ordering::Equal => assert_eq!(a, b, "{:?} = {:?}", pair[0], pair[1]),
                _ => assert!(a < b, "{:?} {a:02x?} < {:?} {b:02x?}", pair[0], pair[1]),
            }
        }
    }

    #[test]
    fn it_identifies_a_number_by_its_value_alone() {
        // A natural, an integer and a float of one number are one value.
        let five = Value::SignedInt(5).reference();
        assert_eq!(Value::UnsignedInt(5).reference(), five);
        assert_eq!(Value::Float(5.0).reference(), five);
        assert_ne!(Value::Float(5.5).reference(), five);
        // Negative zero is zero, and NaN is one value.
        assert_eq!(
            Value::Float(-0.0).reference(),
            Value::SignedInt(0).reference()
        );
        assert_eq!(
            Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)).reference(),
            Value::Float(f64::NAN).reference()
        );
    }

    #[test]
    fn it_tells_values_with_the_same_text_apart_by_type() {
        let entity: Entity = "foo:".parse().unwrap();
        let symbol: Relation = "person/name".parse().unwrap();
        let references = [
            Value::String("foo:".into()).reference(),
            Value::Entity(entity).reference(),
            Value::String("person/name".into()).reference(),
            Value::Symbol(symbol).reference(),
            Value::Bytes(b"person/name".to_vec()).reference(),
            Value::Record(b"person/name".to_vec()).reference(),
        ];
        for (i, a) in references.iter().enumerate() {
            for b in &references[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
