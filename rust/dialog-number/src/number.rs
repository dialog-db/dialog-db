//! A number, decoded.

use std::{cmp::Ordering, error, fmt, str};

use crate::convert::{float, whole};
use crate::key::{
    MEDIUM, NAN, NEGATIVE_INFINITY, NEGATIVE_LARGE, NEGATIVE_MEDIUM, NEGATIVE_SMALL,
    POSITIVE_INFINITY, POSITIVE_LARGE, POSITIVE_MEDIUM, POSITIVE_SMALL, ZERO, write_finite,
};
use crate::varint;

/// A number: zero, an infinity, NaN, or a finite decimal
/// `±0.d₁d₂…dₙ × 100^e` with base-100 digits, the first and the last not
/// zero. Each number has one representation, so equal numbers compare equal
/// whatever they came from.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Number(Repr);

#[derive(Clone, PartialEq, Eq, Hash)]
enum Repr {
    NaN,
    Infinity {
        negative: bool,
    },
    Zero,
    Finite {
        negative: bool,
        exponent: i32,
        digits: Vec<u8>,
    },
}

/// Why bytes are not a number's encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// The bytes end before the number does.
    Truncated,
    /// Bytes follow the number.
    Trailing,
    /// The first byte is not a number's class.
    Class,
    /// The bytes are a number's, but not in the one form the encoding
    /// writes: a digit above 99, a leading or trailing zero digit, or an
    /// exponent in the wrong class.
    NotCanonical,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Truncated => "number encoding ends early",
            Self::Trailing => "bytes follow the number encoding",
            Self::Class => "not a number's class byte",
            Self::NotCanonical => "number encoding is not canonical",
        })
    }
}

impl error::Error for DecodeError {}

impl Number {
    /// NaN.
    pub const NAN: Self = Self(Repr::NaN);
    /// Zero.
    pub const ZERO: Self = Self(Repr::Zero);
    /// Positive infinity.
    pub const INFINITY: Self = Self(Repr::Infinity { negative: false });
    /// Negative infinity.
    pub const NEG_INFINITY: Self = Self(Repr::Infinity { negative: true });

    /// Appends the number's encoding to `out`; see [`encode`](crate::encode).
    pub fn encode(&self, out: &mut Vec<u8>) {
        match &self.0 {
            Repr::NaN => out.push(NAN),
            Repr::Zero => out.push(ZERO),
            Repr::Infinity { negative: true } => out.push(NEGATIVE_INFINITY),
            Repr::Infinity { negative: false } => out.push(POSITIVE_INFINITY),
            Repr::Finite {
                negative,
                exponent,
                digits,
            } => write_finite(*negative, *exponent, digits, out),
        }
    }

    /// The number's encoding.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8);
        self.encode(&mut bytes);
        bytes
    }

    /// The number whose encoding is exactly `bytes`.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let (number, read) = Self::read(bytes)?;
        if read == bytes.len() {
            Ok(number)
        } else {
            Err(DecodeError::Trailing)
        }
    }

    /// Reads one number from the start of `bytes`, returning it and how many
    /// bytes its encoding took.
    pub fn read(bytes: &[u8]) -> Result<(Self, usize), DecodeError> {
        let (head, read) = Head::read(bytes)?;
        let number = match head {
            Head::Special(special) => special,
            Head::Finite {
                negative,
                exponent,
                digits,
            } => Repr::Finite {
                negative,
                exponent,
                digits: digits.iter().map(|byte| digit(*byte, negative)).collect(),
            },
        };
        Ok((Self(number), read))
    }

    /// Whether the number is a whole number.
    pub fn is_integral(&self) -> bool {
        match &self.0 {
            Repr::Zero => true,
            Repr::Finite {
                exponent, digits, ..
            } => *exponent >= 0 && digits.len() <= *exponent as usize,
            Repr::NaN | Repr::Infinity { .. } => false,
        }
    }

    /// Whether the number is below zero.
    pub fn is_negative(&self) -> bool {
        matches!(
            self.0,
            Repr::Infinity { negative: true } | Repr::Finite { negative: true, .. }
        )
    }

    /// The number as an `i128`, if it is a whole number that fits.
    pub fn to_i128(&self) -> Option<i128> {
        let magnitude = self.magnitude()?;
        if self.is_negative() {
            0i128.checked_sub_unsigned(magnitude)
        } else {
            i128::try_from(magnitude).ok()
        }
    }

    /// The number as a `u128`, if it is a whole number at least zero that
    /// fits.
    pub fn to_u128(&self) -> Option<u128> {
        if self.is_negative() {
            None
        } else {
            self.magnitude()
        }
    }

    /// The nearest `f64`. A number that came from a float gives back that
    /// float.
    pub fn to_f64(&self) -> f64 {
        match &self.0 {
            Repr::NaN => f64::NAN,
            Repr::Zero => 0.0,
            Repr::Infinity { negative: true } => f64::NEG_INFINITY,
            Repr::Infinity { negative: false } => f64::INFINITY,
            Repr::Finite { exponent, .. } => {
                let mut text = String::with_capacity(48);
                self.write_scientific(&mut text, *exponent);
                // Parsing a decimal is correctly rounded, so a number that
                // is a float's shortest decimal reads back as that float.
                text.parse().unwrap_or(f64::NAN)
            }
        }
    }

    /// The magnitude of a whole number, if it fits in a `u128`.
    fn magnitude(&self) -> Option<u128> {
        match &self.0 {
            Repr::Zero => Some(0),
            Repr::Finite {
                exponent, digits, ..
            } if self.is_integral() => {
                let mut value: u128 = 0;
                for place in 0..*exponent as usize {
                    let digit = digits.get(place).copied().unwrap_or(0);
                    value = value.checked_mul(100)?.checked_add(u128::from(digit))?;
                }
                Some(value)
            }
            _ => None,
        }
    }

    /// Writes `±0.digits e(2 × exponent)` in decimal.
    fn write_scientific(&self, text: &mut String, exponent: i32) {
        if let Repr::Finite {
            negative, digits, ..
        } = &self.0
        {
            if *negative {
                text.push('-');
            }
            text.push_str("0.");
            for digit in digits {
                text.push(char::from(b'0' + digit / 10));
                text.push(char::from(b'0' + digit % 10));
            }
            text.push('e');
            text.push_str(&(2 * exponent).to_string());
        }
    }
}

/// The value of a digit byte of a number's encoding.
fn digit(byte: u8, negative: bool) -> u8 {
    (if negative { !byte } else { byte }) >> 1
}

/// A number's encoding, read without collecting its digits.
enum Head<'a> {
    Special(Repr),
    Finite {
        negative: bool,
        exponent: i32,
        /// The digit bytes as encoded, inverted for a negative number.
        digits: &'a [u8],
    },
}

impl<'a> Head<'a> {
    /// Reads and checks one number's encoding from the start of `bytes`,
    /// returning it and how many bytes it took.
    fn read(bytes: &'a [u8]) -> Result<(Self, usize), DecodeError> {
        let class = *bytes.first().ok_or(DecodeError::Truncated)?;
        let special = match class {
            NAN => Some(Repr::NaN),
            ZERO => Some(Repr::Zero),
            NEGATIVE_INFINITY => Some(Repr::Infinity { negative: true }),
            POSITIVE_INFINITY => Some(Repr::Infinity { negative: false }),
            _ => None,
        };
        if let Some(special) = special {
            return Ok((Self::Special(special), 1));
        }
        let negative = class < ZERO;
        let mut index = 1;
        let exponent = match class {
            NEGATIVE_LARGE | POSITIVE_LARGE => {
                let (value, read) =
                    varint::read(&bytes[index..], negative).ok_or(DecodeError::Truncated)?;
                index += read;
                let exponent = value as i32;
                if exponent <= MEDIUM {
                    return Err(DecodeError::NotCanonical);
                }
                exponent
            }
            NEGATIVE_SMALL | POSITIVE_SMALL => {
                // Inverted for a positive number, and again for a negative.
                let (value, read) =
                    varint::read(&bytes[index..], !negative).ok_or(DecodeError::Truncated)?;
                index += read;
                if value == 0 {
                    return Err(DecodeError::NotCanonical);
                }
                -(value as i32)
            }
            0x09..=NEGATIVE_MEDIUM => i32::from(NEGATIVE_MEDIUM - class),
            POSITIVE_MEDIUM..=0x21 => i32::from(class - POSITIVE_MEDIUM),
            _ => return Err(DecodeError::Class),
        };
        let start = index;
        loop {
            let byte = *bytes.get(index).ok_or(DecodeError::Truncated)?;
            index += 1;
            let plain = if negative { !byte } else { byte };
            if plain >> 1 > 99 {
                return Err(DecodeError::NotCanonical);
            }
            if plain & 1 == 0 {
                break;
            }
        }
        let digits = &bytes[start..index];
        if digit(digits[0], negative) == 0 || digit(digits[digits.len() - 1], negative) == 0 {
            return Err(DecodeError::NotCanonical);
        }
        Ok((
            Self::Finite {
                negative,
                exponent,
                digits,
            },
            index,
        ))
    }

    /// Reads exactly one number's encoding.
    fn decode(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        let (head, read) = Self::read(bytes)?;
        if read == bytes.len() {
            Ok(head)
        } else {
            Err(DecodeError::Trailing)
        }
    }
}

/// How many bytes the number encoded at the start of `bytes` takes, or
/// `None` if they do not start with one. Allocates nothing.
///
/// It reads the class byte, the exponent if the class does not hold it,
/// then up to the digit that ends the number, without checking the digits:
/// a key parser finds where a number ends this way on every key it splits.
pub fn encoded_len(bytes: &[u8]) -> Option<usize> {
    let class = *bytes.first()?;
    let negative = match class {
        NAN | ZERO | NEGATIVE_INFINITY | POSITIVE_INFINITY => return Some(1),
        NEGATIVE_LARGE..=NEGATIVE_SMALL => true,
        POSITIVE_SMALL..=POSITIVE_LARGE => false,
        _ => return None,
    };
    let start = match class {
        NEGATIVE_LARGE | POSITIVE_LARGE => 1 + varint::read(bytes.get(1..)?, negative)?.1,
        NEGATIVE_SMALL | POSITIVE_SMALL => 1 + varint::read(bytes.get(1..)?, !negative)?.1,
        _ => 1,
    };
    // The last digit's byte is even, and odd once a negative number's
    // bytes are inverted.
    let last = u8::from(negative);
    let end = bytes
        .get(start..)?
        .iter()
        .position(|byte| byte & 1 == last)?;
    Some(start + end + 1)
}

/// The integer whose encoding is exactly `bytes`, or `None` if the number
/// it encodes is not a whole number that fits in an `i128`. Faster than
/// [`Number::decode`] followed by [`Number::to_i128`]: it allocates nothing.
pub fn decode_i128(bytes: &[u8]) -> Result<Option<i128>, DecodeError> {
    Ok(match Head::decode(bytes)? {
        Head::Special(Repr::Zero) => Some(0),
        Head::Special(_) => None,
        Head::Finite {
            negative,
            exponent,
            digits,
        } => {
            if exponent < 0 || digits.len() > exponent as usize {
                return Ok(None);
            }
            let mut magnitude: u128 = 0;
            let mut places = exponent as usize;
            for byte in digits {
                magnitude = match magnitude
                    .checked_mul(100)
                    .and_then(|value| value.checked_add(u128::from(digit(*byte, negative))))
                {
                    Some(value) => value,
                    None => return Ok(None),
                };
                places -= 1;
            }
            for _ in 0..places {
                magnitude = match magnitude.checked_mul(100) {
                    Some(value) => value,
                    None => return Ok(None),
                };
            }
            if negative {
                0i128.checked_sub_unsigned(magnitude)
            } else {
                i128::try_from(magnitude).ok()
            }
        }
    })
}

/// The nearest `f64` to the number whose encoding is exactly `bytes`. A
/// number that came from a float gives back that float. Faster than
/// [`Number::decode`] followed by [`Number::to_f64`]: it allocates nothing
/// for a number of up to 24 base-100 digits.
pub fn decode_f64(bytes: &[u8]) -> Result<f64, DecodeError> {
    Ok(match Head::decode(bytes)? {
        Head::Special(special) => special_f64(&special),
        Head::Finite {
            negative,
            exponent,
            digits,
        } => match finite_f64(negative, exponent, digits) {
            Some(float) => float,
            None => Number::decode(bytes)?.to_f64(),
        },
    })
}

/// A number that is not finite or is zero, as a float.
fn special_f64(special: &Repr) -> f64 {
    match special {
        Repr::NaN => f64::NAN,
        Repr::Infinity { negative: true } => f64::NEG_INFINITY,
        Repr::Infinity { negative: false } => f64::INFINITY,
        _ => 0.0,
    }
}

/// The nearest `f64` to `±0.<digits> × 100^exponent`, correctly rounded, or
/// `None` past 24 base-100 digits.
fn finite_f64(negative: bool, exponent: i32, digits: &[u8]) -> Option<f64> {
    // Clinger's fast path: a significand below 2^53 and a power of ten up
    // to 10^22 are both exact in a float, so one multiplication or division
    // rounds once, correctly. Prices and measurements take it; a float's
    // full 17-digit decimal does not, and is parsed below.
    const EXACT: u64 = 1 << 53;
    const POWERS: [f64; 23] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
        1e17, 1e18, 1e19, 1e20, 1e21, 1e22,
    ];
    if digits.len() <= 8 {
        let mut significand: u64 = 0;
        for byte in digits {
            significand = significand * 100 + u64::from(digit(*byte, negative));
        }
        let power = 2 * (exponent - digits.len() as i32);
        if significand <= EXACT && power.unsigned_abs() <= 22 {
            let magnitude = significand as f64;
            let scale = POWERS[power.unsigned_abs() as usize];
            let float = if power >= 0 {
                magnitude * scale
            } else {
                magnitude / scale
            };
            return Some(if negative { -float } else { float });
        }
    }
    // `-0.<digits>e<2 × exponent>`, parsed with correct rounding.
    const LIMIT: usize = 24;
    if digits.len() > LIMIT {
        return None;
    }
    let mut text = [0u8; 3 + 2 * LIMIT + 8];
    let mut length = 0;
    if negative {
        text[length] = b'-';
        length += 1;
    }
    text[length] = b'0';
    text[length + 1] = b'.';
    length += 2;
    for byte in digits {
        let value = digit(*byte, negative);
        text[length] = b'0' + value / 10;
        text[length + 1] = b'0' + value % 10;
        length += 2;
    }
    text[length] = b'e';
    length += 1;
    let mut power = 2 * exponent;
    if power < 0 {
        text[length] = b'-';
        length += 1;
        power = -power;
    }
    let mut reversed = [0u8; 8];
    let mut count = 0;
    loop {
        reversed[count] = b'0' + (power % 10) as u8;
        count += 1;
        power /= 10;
        if power == 0 {
            break;
        }
    }
    for index in (0..count).rev() {
        text[length] = reversed[index];
        length += 1;
    }
    Some(
        str::from_utf8(&text[..length])
            .ok()
            .and_then(|text| text.parse().ok())
            .unwrap_or(f64::NAN),
    )
}

/// What a number reads back as where nothing records which type wrote it:
/// see [`read_scalar`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scalar {
    /// A whole number an `i128` holds.
    Integer(i128),
    /// A whole number past `i128::MAX` that a `u128` holds.
    Natural(u128),
    /// Any other number, as the nearest float.
    Float(f64),
}

/// Reads the number encoded at the start of `bytes` in one pass, returning
/// it and how many bytes it took: a whole number as an integer, one too
/// large for an integer as a natural, and any other as the nearest float.
/// Allocates nothing for a number of up to 24 base-100 digits.
pub fn read_scalar(bytes: &[u8]) -> Result<(Scalar, usize), DecodeError> {
    let (head, read) = Head::read(bytes)?;
    let scalar = match head {
        Head::Special(Repr::Zero) => Scalar::Integer(0),
        Head::Special(special) => Scalar::Float(special_f64(&special)),
        Head::Finite {
            negative,
            exponent,
            digits,
        } => match whole_magnitude(negative, exponent, digits) {
            Some(magnitude) if negative => match 0i128.checked_sub_unsigned(magnitude) {
                Some(integer) => Scalar::Integer(integer),
                None => Scalar::Float(float_of(bytes, read, negative, exponent, digits)?),
            },
            Some(magnitude) => match i128::try_from(magnitude) {
                Ok(integer) => Scalar::Integer(integer),
                Err(_) => Scalar::Natural(magnitude),
            },
            None => Scalar::Float(float_of(bytes, read, negative, exponent, digits)?),
        },
    };
    Ok((scalar, read))
}

/// The float a finite number's encoding names, by the fast path or else
/// through [`Number`].
fn float_of(
    bytes: &[u8],
    read: usize,
    negative: bool,
    exponent: i32,
    digits: &[u8],
) -> Result<f64, DecodeError> {
    match finite_f64(negative, exponent, digits) {
        Some(float) => Ok(float),
        None => Ok(Number::decode(&bytes[..read])?.to_f64()),
    }
}

/// The magnitude of a whole number that fits in a `u128`, or `None` for a
/// fraction or a larger number.
fn whole_magnitude(negative: bool, exponent: i32, digits: &[u8]) -> Option<u128> {
    if exponent < 0 || digits.len() > exponent as usize {
        return None;
    }
    let mut magnitude: u128 = 0;
    for byte in digits {
        magnitude = magnitude
            .checked_mul(100)?
            .checked_add(u128::from(digit(*byte, negative)))?;
    }
    for _ in digits.len()..exponent as usize {
        magnitude = magnitude.checked_mul(100)?;
    }
    Some(magnitude)
}

impl From<i128> for Number {
    fn from(value: i128) -> Self {
        if value == 0 {
            return Self::ZERO;
        }
        let digits = whole(value.unsigned_abs());
        Self(Repr::Finite {
            negative: value < 0,
            exponent: digits.exponent,
            digits: digits.digits().to_vec(),
        })
    }
}

impl From<u128> for Number {
    fn from(value: u128) -> Self {
        if value == 0 {
            return Self::ZERO;
        }
        let digits = whole(value);
        Self(Repr::Finite {
            negative: false,
            exponent: digits.exponent,
            digits: digits.digits().to_vec(),
        })
    }
}

impl From<f64> for Number {
    fn from(value: f64) -> Self {
        if value.is_nan() {
            Self::NAN
        } else if value == 0.0 {
            Self::ZERO
        } else if value.is_infinite() {
            Self(Repr::Infinity {
                negative: value < 0.0,
            })
        } else {
            let digits = float(value.abs());
            Self(Repr::Finite {
                negative: value < 0.0,
                exponent: digits.exponent,
                digits: digits.digits().to_vec(),
            })
        }
    }
}

impl Ord for Number {
    /// Orders numbers by value, NaN first, as their encodings sort.
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_bytes().cmp(&other.to_bytes())
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Repr::NaN => f.write_str("NaN"),
            Repr::Zero => f.write_str("0"),
            Repr::Infinity { negative: true } => f.write_str("-inf"),
            Repr::Infinity { negative: false } => f.write_str("inf"),
            Repr::Finite { exponent, .. } => {
                let mut text = String::new();
                self.write_scientific(&mut text, *exponent);
                f.write_str(&text)
            }
        }
    }
}

impl fmt::Debug for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Number({self})")
    }
}
