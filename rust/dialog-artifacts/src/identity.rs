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
//! A number's identity does not depend on the band the value index keeps it
//! in today: a natural `5` and an integer `5` are one number, written as an
//! integer, and `5.0` is another, written as a float. The index still keeps
//! naturals, integers and floats apart, so until it moves to one band, two
//! rules whose only difference is a natural against an integer constant in a
//! slot that declares no type have one identity but read different bands.

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
/// The class byte of a negative finite number.
const NEGATIVE: u8 = 0x02;
/// The class byte of zero.
const ZERO: u8 = 0x03;
/// The class byte of a positive finite number.
const POSITIVE: u8 = 0x04;
/// The class byte of positive infinity.
const POSITIVE_INFINITY: u8 = 0x05;
/// The class byte of NaN.
const NAN: u8 = 0x06;
/// The last byte of a number written as an integer.
const AS_INTEGER: u8 = 0x00;
/// The last byte of a number written as a float.
const AS_FLOAT: u8 = 0x01;
/// Added to a number's binary exponent so it is never negative.
const BIAS: i32 = 1200;

/// A number as it was written.
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
/// The encoding is:
///
/// - a class byte: negative infinity, negative, zero, positive, positive
///   infinity or NaN, in that order;
/// - for a finite number other than zero, the power of two of its leading
///   bit plus 1200, as two big-endian bytes, then the bits after the leading
///   one in groups of seven, each group shifted left by one with the low bit
///   set when another group follows, every one of these bytes inverted for
///   a negative number;
/// - a last byte, `0` when the number was written as an integer and `1`
///   when it was written as a float.
///
/// So every number has one encoding, encodings sort as their numbers do with
/// a number written as an integer just before the same number written as a
/// float, `-0.0` is `0.0`, and NaN is one value.
pub fn write_number(number: Number, out: &mut Vec<u8>) {
    let written = match number {
        Number::Float(_) => AS_FLOAT,
        Number::Natural(_) | Number::Integer(_) => AS_INTEGER,
    };
    match number.class() {
        Class::Special(class) => out.push(class),
        Class::Finite(Finite {
            negative,
            exponent,
            mut fraction,
        }) => {
            out.push(if negative { NEGATIVE } else { POSITIVE });
            let start = out.len();
            out.extend_from_slice(&((exponent + BIAS) as u16).to_be_bytes());
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
    out.push(written);
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
        assert_eq!(number_content(Number::Integer(0)), [0x03, 0x00]);
        assert_eq!(number_content(Number::Float(0.0)), [0x03, 0x01]);
        assert_eq!(
            number_content(Number::Integer(1)),
            [0x04, 0x04, 0xB0, 0x00, 0x00]
        );
        assert_eq!(
            number_content(Number::Integer(5)),
            [0x04, 0x04, 0xB2, 0x40, 0x00]
        );
        assert_eq!(
            number_content(Number::Float(5.0)),
            [0x04, 0x04, 0xB2, 0x40, 0x01]
        );
        assert_eq!(
            number_content(Number::Integer(-1)),
            [0x02, 0xFB, 0x4F, 0xFF, 0x00]
        );
        assert_eq!(
            number_content(Number::Float(0.5)),
            [0x04, 0x04, 0xAF, 0x00, 0x01]
        );
        assert_eq!(number_content(Number::Float(f64::NAN)), [0x06, 0x01]);
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
            Number::Float(-1.0),
            Number::Float(-f64::MIN_POSITIVE),
            Number::Float(-5e-324),
            Number::Integer(0),
            Number::Float(0.0),
            Number::Float(5e-324),
            Number::Float(f64::MIN_POSITIVE),
            Number::Float(0.5),
            Number::Natural(1),
            Number::Float(1.0),
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

    #[test]
    fn it_identifies_a_number_apart_from_its_band() {
        // A natural and an integer are one number.
        assert_eq!(
            Value::UnsignedInt(5).reference(),
            Value::SignedInt(5).reference()
        );
        // A number written as a float is another.
        assert_ne!(
            Value::SignedInt(5).reference(),
            Value::Float(5.0).reference()
        );
        assert_ne!(
            Value::SignedInt(0).reference(),
            Value::Float(0.0).reference()
        );
        // Negative zero is zero, and NaN is one value.
        assert_eq!(
            Value::Float(-0.0).reference(),
            Value::Float(0.0).reference()
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
