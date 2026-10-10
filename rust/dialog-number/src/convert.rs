//! Numbers from what holds them: base-100 digits and an exponent.

/// The most base-100 digits a 128-bit integer has.
pub(crate) const WHOLE_DIGITS: usize = 20;
/// The most base-100 digits a float's shortest decimal has: 17 decimal
/// digits, one more when the decimal point falls between a pair.
pub(crate) const FLOAT_DIGITS: usize = 10;

/// Powers of ten a float's significand can be scaled by.
const POWERS_OF_TEN: [u64; 20] = {
    let mut powers = [1u64; 20];
    let mut index = 1;
    while index < 20 {
        powers[index] = powers[index - 1] * 10;
        index += 1;
    }
    powers
};

/// A finite number other than zero as base-100 digits: `0.d₁d₂…dₙ × 100^e`,
/// the first digit and the last not zero.
pub(crate) struct Digits<const N: usize> {
    pub(crate) exponent: i32,
    buffer: [u8; N],
    start: usize,
    end: usize,
}

impl<const N: usize> Digits<N> {
    pub(crate) fn digits(&self) -> &[u8] {
        &self.buffer[self.start..self.end]
    }
}

/// The base-100 digits of a whole number other than zero.
pub(crate) fn whole(mut magnitude: u128) -> Digits<WHOLE_DIGITS> {
    debug_assert!(magnitude != 0);
    // Eighteen decimal digits at a time: an even count, so a chunk splits
    // into whole base-100 digits, and small enough for 64-bit arithmetic.
    const CHUNK: u128 = 1_000_000_000_000_000_000;
    let mut buffer = [0u8; WHOLE_DIGITS];
    let mut start = WHOLE_DIGITS;
    loop {
        let mut chunk = (magnitude % CHUNK) as u64;
        magnitude /= CHUNK;
        if magnitude == 0 {
            while chunk > 0 {
                start -= 1;
                buffer[start] = (chunk % 100) as u8;
                chunk /= 100;
            }
            break;
        }
        for _ in 0..9 {
            start -= 1;
            buffer[start] = (chunk % 100) as u8;
            chunk /= 100;
        }
    }
    let mut end = WHOLE_DIGITS;
    while buffer[end - 1] == 0 {
        end -= 1;
    }
    Digits {
        exponent: (WHOLE_DIGITS - start) as i32,
        buffer,
        start,
        end,
    }
}

/// The base-100 digits of a finite float's shortest decimal, the float
/// neither zero nor negative.
///
/// The shortest decimal is the one with fewest significant digits that reads
/// back as the float, the nearest to it where several have that many. It is
/// unique, so any correct shortest printer gives the same digits; `zmij` is
/// the fastest at hand, and a test pins it to `ryu`.
pub(crate) fn float(value: f64) -> Digits<FLOAT_DIGITS> {
    debug_assert!(value.is_finite() && value > 0.0);
    let mut printer = zmij::Buffer::new();
    shortest(printer.format_finite(value).as_bytes())
}

/// The base-100 digits of a positive decimal written as a shortest float
/// printer writes it: digits with an optional point, then an optional
/// exponent, `e` with an optional sign. It has at most 17 significant
/// digits.
pub(crate) fn shortest(text: &[u8]) -> Digits<FLOAT_DIGITS> {
    // The value is `significand × 10^power`. Each digit after the point
    // lowers the power by one. Zeros after the last nonzero digit so far
    // are counted, not multiplied in: if a nonzero digit follows they are
    // interior and multiplied in then, and otherwise they only scale the
    // value, which adding them back to the power does at the end. So the
    // significand holds only the significant digits, at most 17.
    let mut significand: u64 = 0;
    let mut power: i32 = 0;
    let mut zeros: u32 = 0;
    let mut fraction = false;
    let mut index = 0;
    while index < text.len() {
        let byte = text[index];
        if byte.is_ascii_digit() {
            if fraction {
                power -= 1;
            }
            let digit = u64::from(byte - b'0');
            if digit == 0 {
                if significand != 0 {
                    zeros += 1;
                }
            } else {
                significand = significand * POWERS_OF_TEN[zeros as usize + 1] + digit;
                zeros = 0;
            }
        } else if byte == b'.' {
            fraction = true;
        } else {
            break;
        }
        index += 1;
    }
    power += zeros as i32;
    if index < text.len() {
        // `e`, an optional sign, digits.
        index += 1;
        let negative = text.get(index) == Some(&b'-');
        if matches!(text.get(index), Some(b'-' | b'+')) {
            index += 1;
        }
        let mut exponent: i32 = 0;
        for digit in &text[index..] {
            exponent = exponent * 10 + i32::from(digit - b'0');
        }
        power += if negative { -exponent } else { exponent };
    }
    // Make the power even so the significand splits into base-100 digits.
    if power.rem_euclid(2) == 1 {
        significand *= 10;
        power -= 1;
    }
    let mut buffer = [0u8; FLOAT_DIGITS];
    let mut start = FLOAT_DIGITS;
    while significand > 0 {
        start -= 1;
        buffer[start] = (significand % 100) as u8;
        significand /= 100;
    }
    let mut end = FLOAT_DIGITS;
    while buffer[end - 1] == 0 {
        end -= 1;
    }
    Digits {
        exponent: (FLOAT_DIGITS - start) as i32 + power / 2,
        buffer,
        start,
        end,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn whole_of(value: u128) -> (i32, Vec<u8>) {
        let digits = whole(value);
        (digits.exponent, digits.digits().to_vec())
    }

    fn float_of(value: f64) -> (i32, Vec<u8>) {
        let digits = float(value);
        (digits.exponent, digits.digits().to_vec())
    }

    #[test]
    fn it_takes_whole_numbers_to_base_100() {
        assert_eq!(whole_of(1), (1, vec![1]));
        assert_eq!(whole_of(10), (1, vec![10]));
        assert_eq!(whole_of(99), (1, vec![99]));
        assert_eq!(whole_of(100), (2, vec![1]));
        assert_eq!(whole_of(1234), (2, vec![12, 34]));
        assert_eq!(whole_of(12345), (3, vec![1, 23, 45]));
        assert_eq!(
            whole_of(9_223_372_036_854_775_807),
            (10, vec![9, 22, 33, 72, 3, 68, 54, 77, 58, 7])
        );
        assert_eq!(
            whole_of(u128::MAX),
            (
                20,
                vec![
                    3, 40, 28, 23, 66, 92, 9, 38, 46, 34, 63, 37, 46, 7, 43, 17, 68, 21, 14, 55
                ]
            )
        );
    }

    #[test]
    fn it_agrees_with_decimal_printing_for_whole_numbers() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..20_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let shift = (state % 128) as u32;
            let value = (u128::from(state) << 64 | u128::from(state.rotate_left(17))) >> shift;
            if value == 0 {
                continue;
            }
            let text = value.to_string();
            let padded = if text.len() % 2 == 1 {
                format!("0{text}")
            } else {
                text
            };
            let mut digits: Vec<u8> = padded
                .as_bytes()
                .chunks(2)
                .map(|pair| (pair[0] - b'0') * 10 + (pair[1] - b'0'))
                .collect();
            let exponent = digits.len() as i32;
            while digits.last() == Some(&0) {
                digits.pop();
            }
            assert_eq!(whole_of(value), (exponent, digits), "{value}");
        }
    }

    /// Base-100 digits of a decimal text, by string operations: a reference
    /// that shares no code with [`shortest`].
    fn reference(text: &str) -> (i32, Vec<u8>) {
        let (mantissa, power) = match text.split_once(['e', 'E']) {
            Some((mantissa, power)) => (mantissa, power.parse::<i32>().unwrap()),
            None => (text, 0),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let digits = format!("{whole}{fraction}");
        // value = 0.digits × 10^(len(whole) + power), then strip zeros.
        let mut k = whole.len() as i32 + power;
        let trimmed = digits.trim_start_matches('0');
        k -= (digits.len() - trimmed.len()) as i32;
        let trimmed = trimmed.trim_end_matches('0');
        let padded = if k.rem_euclid(2) == 1 {
            k += 1;
            format!("0{trimmed}")
        } else {
            trimmed.to_string()
        };
        let padded = if padded.len() % 2 == 1 {
            format!("{padded}0")
        } else {
            padded
        };
        let pairs = padded
            .as_bytes()
            .chunks(2)
            .map(|pair| (pair[0] - b'0') * 10 + (pair[1] - b'0'))
            .collect();
        (k / 2, pairs)
    }

    /// The shortest decimal is unique, so two correct printers agree; this
    /// pins the one in use to `ryu`.
    #[test]
    fn it_prints_the_same_shortest_decimal_as_ryu() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut checked = 0;
        while checked < 600_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Half from any bits, half of moderate size, which printers
            // write as plain decimals with zeros on either side.
            let value = if checked % 2 == 0 {
                f64::from_bits(state).abs()
            } else {
                (state >> (state % 64)) as f64 / 10f64.powi((state % 23) as i32)
            };
            if !value.is_finite() || value == 0.0 {
                continue;
            }
            let mut printer = ryu::Buffer::new();
            let theirs = reference(printer.format_finite(value));
            let ours = float(value);
            assert_eq!((ours.exponent, ours.digits().to_vec()), theirs, "{value:e}");
            checked += 1;
        }
    }

    #[test]
    fn it_takes_floats_to_their_shortest_decimal() {
        assert_eq!(float_of(1.0), (1, vec![1]));
        assert_eq!(float_of(3.0), whole_of(3));
        assert_eq!(float_of(0.1), (0, vec![10]));
        assert_eq!(float_of(0.123), (0, vec![12, 30]));
        assert_eq!(float_of(0.0123), (0, vec![1, 23]));
        assert_eq!(float_of(0.00123), (-1, vec![12, 30]));
        assert_eq!(float_of(12.345), (1, vec![12, 34, 50]));
        assert_eq!(float_of(1234.5), (2, vec![12, 34, 50]));
        assert_eq!(float_of(123_450.0), whole_of(123_450));
        assert_eq!(float_of(1e23), (12, vec![10]));
        assert_eq!(float_of(5e-324), (-161, vec![5]));
    }
}
