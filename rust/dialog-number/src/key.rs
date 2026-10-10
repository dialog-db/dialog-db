//! The encoding: SQLite4's numeric key encoding.

use crate::Number;
use crate::convert::{float, whole};
use crate::varint;

/// The class byte of NaN, which sorts before every other number.
pub(crate) const NAN: u8 = 0x06;
/// The class byte of negative infinity.
pub(crate) const NEGATIVE_INFINITY: u8 = 0x07;
/// The class byte of a negative number whose exponent is 11 or more.
pub(crate) const NEGATIVE_LARGE: u8 = 0x08;
/// The class byte of a negative number whose exponent is 0; one with
/// exponent `e` from 0 to 10 has `NEGATIVE_MEDIUM - e`.
pub(crate) const NEGATIVE_MEDIUM: u8 = 0x13;
/// The class byte of a negative number whose exponent is below zero.
pub(crate) const NEGATIVE_SMALL: u8 = 0x14;
/// The class byte of zero.
pub(crate) const ZERO: u8 = 0x15;
/// The class byte of a positive number whose exponent is below zero.
pub(crate) const POSITIVE_SMALL: u8 = 0x16;
/// The class byte of a positive number whose exponent is 0; one with
/// exponent `e` from 0 to 10 has `POSITIVE_MEDIUM + e`.
pub(crate) const POSITIVE_MEDIUM: u8 = 0x17;
/// The class byte of a positive number whose exponent is 11 or more.
pub(crate) const POSITIVE_LARGE: u8 = 0x22;
/// The class byte of positive infinity.
pub(crate) const POSITIVE_INFINITY: u8 = 0x23;
/// The largest exponent the class byte holds.
pub(crate) const MEDIUM: i32 = 10;

/// Appends `number`'s encoding to `out`.
///
/// The encoding is SQLite4's numeric key encoding
/// (`sqlite.org/src4/doc/trunk/www/key_encoding.wiki`):
///
/// - NaN, negative infinity, zero and positive infinity are one class byte
///   each;
/// - a finite number `±0.d₁…dₙ × 100^e` is a class byte, then `e` where the
///   class cannot hold it, then the digits:
///   - for `e` from 0 to 10 the class byte holds it: `0x17 + e` positive,
///     `0x13 - e` negative;
///   - for `e` of 11 or more the class is `0x22` (`0x08` negative), followed
///     by `e` as an order-preserving varint;
///   - for `e` below zero the class is `0x16` (`0x14` negative), followed by
///     `-e` as a varint, inverted for a positive number;
///   - each digit `X` is the byte `2X + 1`, the last `2X`, with no trailing
///     zero digits;
///   - for a negative number every byte after the class is inverted.
///
/// Byte-by-byte comparison of encodings orders numbers by value, with NaN
/// first.
pub fn encode(number: &Number, out: &mut Vec<u8>) {
    number.encode(out);
}

/// Appends the encoding of the integer `value` to `out`.
pub fn encode_i128(value: i128, out: &mut Vec<u8>) {
    if value == 0 {
        out.push(ZERO);
    } else {
        let digits = whole(value.unsigned_abs());
        write_finite(value < 0, digits.exponent, digits.digits(), out);
    }
}

/// Appends the encoding of the natural `value` to `out`.
pub fn encode_u128(value: u128, out: &mut Vec<u8>) {
    if value == 0 {
        out.push(ZERO);
    } else {
        let digits = whole(value);
        write_finite(false, digits.exponent, digits.digits(), out);
    }
}

/// Appends the encoding of the float `value` to `out`: the number its
/// shortest decimal names.
pub fn encode_f64(value: f64, out: &mut Vec<u8>) {
    if value.is_nan() {
        out.push(NAN);
    } else if value == 0.0 {
        out.push(ZERO);
    } else if value.is_infinite() {
        out.push(if value < 0.0 {
            NEGATIVE_INFINITY
        } else {
            POSITIVE_INFINITY
        });
    } else {
        let digits = float(value.abs());
        write_finite(value < 0.0, digits.exponent, digits.digits(), out);
    }
}

/// Appends a finite number other than zero: `±0.digits × 100^exponent`, the
/// digits base-100 with neither the first nor the last zero.
pub(crate) fn write_finite(negative: bool, exponent: i32, digits: &[u8], out: &mut Vec<u8>) {
    debug_assert!(!digits.is_empty());
    debug_assert!(exponent.unsigned_abs() <= varint::MAX);
    let class = if (0..=MEDIUM).contains(&exponent) {
        if negative {
            NEGATIVE_MEDIUM - exponent as u8
        } else {
            POSITIVE_MEDIUM + exponent as u8
        }
    } else if exponent > MEDIUM {
        if negative {
            NEGATIVE_LARGE
        } else {
            POSITIVE_LARGE
        }
    } else if negative {
        NEGATIVE_SMALL
    } else {
        POSITIVE_SMALL
    };
    out.push(class);
    let start = out.len();
    if exponent > MEDIUM {
        varint::write(exponent as u32, false, out);
    } else if exponent < 0 {
        // A smaller exponent is a smaller positive number, so its varint is
        // inverted; the negative case inverts again below.
        varint::write(exponent.unsigned_abs(), true, out);
    }
    let last = digits.len() - 1;
    out.extend(
        digits
            .iter()
            .enumerate()
            .map(|(index, digit)| digit * 2 + u8::from(index != last)),
    );
    if negative {
        for byte in &mut out[start..] {
            *byte = !*byte;
        }
    }
}
