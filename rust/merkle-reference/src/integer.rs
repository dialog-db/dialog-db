use std::{error, fmt};

/// An integer of any size, the content of [`Type::INTEGER`](crate::Type::INTEGER).
///
/// Its content is a header, then the magnitude in big-endian bytes with no
/// leading zero byte:
///
/// - for an integer that is not negative and whose magnitude takes `k` bytes,
///   the header is the byte `0x80 + k` when `k < 127`, and otherwise the byte
///   `0xFF` followed by the content of the integer `k - 127`;
/// - for a negative integer, every byte of its magnitude's content, header
///   included, is inverted.
///
/// Zero is `[0x80]`, `1` is `[0x81, 0x01]` and `-1` is `[0x7E, 0xFE]`. Every
/// integer has exactly one content, and contents sort as their integers do.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Integer {
    negative: bool,
    magnitude: Vec<u8>,
}

/// Why bytes are not the content of an [`Integer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegerError {
    /// The bytes end before the integer does.
    Truncated,
    /// Bytes follow the integer.
    Trailing,
    /// The magnitude starts with a zero byte, so a shorter content exists.
    LeadingZero,
    /// The content says zero is negative.
    NegativeZero,
    /// The magnitude's length does not fit in memory.
    Oversized,
}

impl fmt::Display for IntegerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Truncated => "integer content ends early",
            Self::Trailing => "bytes follow the integer content",
            Self::LeadingZero => "integer magnitude has a leading zero byte",
            Self::NegativeZero => "integer content is a negative zero",
            Self::Oversized => "integer magnitude is too long",
        })
    }
}

impl error::Error for IntegerError {}

/// The longest magnitude the one-byte header can hold.
const SHORT: usize = 126;
/// The header of a magnitude longer than [`SHORT`].
const LONG: u8 = 0xFF;
/// The header of zero.
const ZERO: u8 = 0x80;

impl Integer {
    /// The integer with the given sign and big-endian magnitude.
    ///
    /// Leading zero bytes are dropped, and zero is never negative.
    pub fn new(negative: bool, magnitude: &[u8]) -> Self {
        let start = magnitude
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(magnitude.len());
        let magnitude = magnitude[start..].to_vec();
        Self {
            negative: negative && !magnitude.is_empty(),
            magnitude,
        }
    }

    /// Whether the integer is less than zero.
    pub fn is_negative(&self) -> bool {
        self.negative
    }

    /// The integer's magnitude: big-endian, with no leading zero byte.
    pub fn magnitude(&self) -> &[u8] {
        &self.magnitude
    }

    /// The integer's content.
    pub fn content(&self) -> Vec<u8> {
        let mut content = Vec::with_capacity(self.magnitude.len() + 1);
        self.write_content(&mut content);
        content
    }

    /// Appends the integer's content to `out`.
    pub fn write_content(&self, out: &mut Vec<u8>) {
        let start = out.len();
        write_magnitude(&self.magnitude, out);
        if self.negative {
            for byte in &mut out[start..] {
                *byte = !*byte;
            }
        }
    }

    /// The integer whose content is `content`, refusing anything that is not
    /// exactly one integer's content.
    pub fn from_content(content: &[u8]) -> Result<Self, IntegerError> {
        let (integer, read) = Self::read(content)?;
        if read == content.len() {
            Ok(integer)
        } else {
            Err(IntegerError::Trailing)
        }
    }

    /// Reads one integer's content from the start of `bytes`, returning the
    /// integer and how many bytes its content took.
    pub fn read(bytes: &[u8]) -> Result<(Self, usize), IntegerError> {
        let header = *bytes.first().ok_or(IntegerError::Truncated)?;
        let negative = header < ZERO;
        let (magnitude, read) = read_magnitude(bytes, negative)?;
        if negative && magnitude.is_empty() {
            return Err(IntegerError::NegativeZero);
        }
        Ok((
            Self {
                negative,
                magnitude,
            },
            read,
        ))
    }
}

/// Writes the content of the non-negative integer whose magnitude is
/// `magnitude`.
fn write_magnitude(magnitude: &[u8], out: &mut Vec<u8>) {
    let length = magnitude.len();
    if length <= SHORT {
        out.push(ZERO + length as u8);
    } else {
        out.push(LONG);
        let rest = (length - SHORT - 1).to_be_bytes();
        let start = rest
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(rest.len());
        write_magnitude(&rest[start..], out);
    }
    out.extend_from_slice(magnitude);
}

/// Reads a magnitude's content, every byte inverted when `inverted`.
fn read_magnitude(bytes: &[u8], inverted: bool) -> Result<(Vec<u8>, usize), IntegerError> {
    let flip = |byte: u8| if inverted { !byte } else { byte };
    let header = flip(*bytes.first().ok_or(IntegerError::Truncated)?);
    if header < ZERO {
        // Only a length can be read here, and a length is never negative.
        return Err(IntegerError::NegativeZero);
    }
    let (length, start) = if header == LONG {
        let (rest, read) = read_magnitude(&bytes[1..], inverted)?;
        if rest.len() > size_of::<usize>() {
            return Err(IntegerError::Oversized);
        }
        let mut buffer = [0u8; size_of::<usize>()];
        buffer[size_of::<usize>() - rest.len()..].copy_from_slice(&rest);
        let length = usize::from_be_bytes(buffer)
            .checked_add(SHORT + 1)
            .ok_or(IntegerError::Oversized)?;
        (length, 1 + read)
    } else {
        ((header - ZERO) as usize, 1)
    };
    let end = start.checked_add(length).ok_or(IntegerError::Oversized)?;
    let magnitude: Vec<u8> = bytes
        .get(start..end)
        .ok_or(IntegerError::Truncated)?
        .iter()
        .map(|byte| flip(*byte))
        .collect();
    if magnitude.first() == Some(&0) {
        return Err(IntegerError::LeadingZero);
    }
    Ok((magnitude, end))
}

macro_rules! from_unsigned {
    ($($ty:ty),*) => {$(
        impl From<$ty> for Integer {
            fn from(value: $ty) -> Self {
                Self::new(false, &value.to_be_bytes())
            }
        }
    )*};
}

macro_rules! from_signed {
    ($($ty:ty),*) => {$(
        impl From<$ty> for Integer {
            fn from(value: $ty) -> Self {
                Self::new(value < 0, &value.unsigned_abs().to_be_bytes())
            }
        }
    )*};
}

from_unsigned!(u8, u16, u32, u64, u128, usize);
from_signed!(i8, i16, i32, i64, i128, isize);

impl TryFrom<&Integer> for i128 {
    type Error = IntegerError;

    fn try_from(integer: &Integer) -> Result<Self, Self::Error> {
        let magnitude = u128::try_from(&Integer::new(false, integer.magnitude()))?;
        if integer.negative {
            0i128
                .checked_sub_unsigned(magnitude)
                .ok_or(IntegerError::Oversized)
        } else {
            i128::try_from(magnitude).map_err(|_| IntegerError::Oversized)
        }
    }
}

impl TryFrom<&Integer> for u128 {
    type Error = IntegerError;

    fn try_from(integer: &Integer) -> Result<Self, Self::Error> {
        if integer.negative || integer.magnitude.len() > 16 {
            return Err(IntegerError::Oversized);
        }
        let mut buffer = [0u8; 16];
        buffer[16 - integer.magnitude.len()..].copy_from_slice(&integer.magnitude);
        Ok(u128::from_be_bytes(buffer))
    }
}

impl fmt::Debug for Integer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match i128::try_from(self) {
            Ok(value) => write!(f, "Integer({value})"),
            Err(_) => {
                write!(f, "Integer({}0x", if self.negative { "-" } else { "" })?;
                for byte in &self.magnitude {
                    write!(f, "{byte:02x}")?;
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(value: i128) -> Vec<u8> {
        Integer::from(value).content()
    }

    #[test]
    fn it_encodes_small_integers() {
        assert_eq!(content(0), [0x80]);
        assert_eq!(content(1), [0x81, 0x01]);
        assert_eq!(content(255), [0x81, 0xFF]);
        assert_eq!(content(256), [0x82, 0x01, 0x00]);
        assert_eq!(content(-1), [0x7E, 0xFE]);
        assert_eq!(content(-256), [0x7D, 0xFE, 0xFF]);
    }

    #[test]
    fn it_has_one_content_per_integer() {
        assert_eq!(Integer::new(false, &[0, 0, 7]), Integer::from(7u8));
        assert_eq!(Integer::new(true, &[0, 0]), Integer::from(0u8));
        assert_eq!(Integer::from(5i64), Integer::from(5u128));
        assert_eq!(
            Integer::from_content(&[0x82, 0x00, 0x07]),
            Err(IntegerError::LeadingZero)
        );
        assert_eq!(
            Integer::from_content(&[0x7F]),
            Err(IntegerError::NegativeZero)
        );
    }

    #[test]
    fn it_round_trips_and_sorts_as_integers() {
        let values: Vec<i128> = vec![
            i128::MIN,
            i128::MIN + 1,
            -(1 << 64),
            -65_536,
            -256,
            -255,
            -2,
            -1,
            0,
            1,
            2,
            127,
            128,
            255,
            256,
            65_535,
            1 << 64,
            i128::MAX,
        ];
        let contents: Vec<Vec<u8>> = values.iter().map(|value| content(*value)).collect();
        for (value, bytes) in values.iter().zip(&contents) {
            let decoded = Integer::from_content(bytes).unwrap();
            assert_eq!(i128::try_from(&decoded).unwrap(), *value);
        }
        for pair in contents.windows(2) {
            assert!(pair[0] < pair[1], "{:02x?} < {:02x?}", pair[0], pair[1]);
        }
    }

    #[test]
    fn it_encodes_magnitudes_of_any_length() {
        let lengths = [125, 126, 127, 128, 300, 70_000];
        let mut previous: Option<(Vec<u8>, Vec<u8>)> = None;
        for length in lengths {
            let magnitude = vec![0xAB; length];
            for negative in [false, true] {
                let integer = Integer::new(negative, &magnitude);
                let bytes = integer.content();
                assert_eq!(Integer::from_content(&bytes).unwrap(), integer);
                let mut truncated = bytes.clone();
                truncated.pop();
                assert_eq!(
                    Integer::from_content(&truncated),
                    Err(IntegerError::Truncated)
                );
                let mut trailing = bytes.clone();
                trailing.push(0);
                assert_eq!(
                    Integer::from_content(&trailing),
                    Err(IntegerError::Trailing)
                );
            }
            let positive = Integer::new(false, &magnitude).content();
            let negative = Integer::new(true, &magnitude).content();
            if let Some((last_positive, last_negative)) = previous {
                assert!(last_positive < positive);
                assert!(negative < last_negative);
            }
            previous = Some((positive, negative));
        }
    }

    #[test]
    fn it_marks_long_magnitudes_after_the_short_ones() {
        let short = Integer::new(false, &[1; 126]).content();
        let long = Integer::new(false, &[1; 127]).content();
        assert_eq!(short[0], 0x80 + 126);
        assert_eq!(&long[..2], &[0xFF, 0x80]);
        assert_eq!(long.len(), 2 + 127);
    }
}
