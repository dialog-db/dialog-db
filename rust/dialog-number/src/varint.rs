//! SQLite4's order-preserving varint, for an exponent the class byte cannot
//! hold. Each value has exactly one encoding, and encodings compare byte by
//! byte as their values do.
//!
//! - `0..=240`: one byte, the value;
//! - `241..=2287`: two bytes, `241 + (v - 240) / 256` then `(v - 240) % 256`;
//! - `2288..=67823`: three bytes, `249` then `v - 2288` as two big-endian
//!   bytes.
//!
//! SQLite4 continues to larger values, which no number dialog encodes needs.

/// The largest value [`write`] encodes.
pub(crate) const MAX: u32 = 67_823;

/// Appends `value`'s encoding, inverted when `inverted`.
pub(crate) fn write(value: u32, inverted: bool, out: &mut Vec<u8>) {
    let start = out.len();
    if value <= 240 {
        out.push(value as u8);
    } else if value <= 2287 {
        let rest = value - 240;
        out.push((241 + rest / 256) as u8);
        out.push((rest % 256) as u8);
    } else {
        debug_assert!(value <= MAX);
        let rest = value - 2288;
        out.push(249);
        out.push((rest >> 8) as u8);
        out.push((rest & 0xFF) as u8);
    }
    if inverted {
        for byte in &mut out[start..] {
            *byte = !*byte;
        }
    }
}

/// Reads a value encoded by [`write`] from the start of `bytes`, returning it
/// and how many bytes it took, or `None` if the bytes are not one.
pub(crate) fn read(bytes: &[u8], inverted: bool) -> Option<(u32, usize)> {
    let flip = |byte: u8| if inverted { !byte } else { byte };
    let first = flip(*bytes.first()?);
    match first {
        0..=240 => Some((u32::from(first), 1)),
        241..=248 => {
            let second = flip(*bytes.get(1)?);
            Some((240 + 256 * u32::from(first - 241) + u32::from(second), 2))
        }
        249 => {
            let high = flip(*bytes.get(1)?);
            let low = flip(*bytes.get(2)?);
            Some((2288 + (u32::from(high) << 8 | u32::from(low)), 3))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_round_trips_and_sorts_as_it_counts() {
        let mut values: Vec<u32> = (0..=MAX).step_by(7).collect();
        values.extend([240, 241, 2287, 2288, MAX]);
        values.sort_unstable();
        values.dedup();
        let mut previous: Option<Vec<u8>> = None;
        for value in values {
            for inverted in [false, true] {
                let mut bytes = Vec::new();
                write(value, inverted, &mut bytes);
                assert_eq!(read(&bytes, inverted), Some((value, bytes.len())));
            }
            let mut bytes = Vec::new();
            write(value, false, &mut bytes);
            if let Some(previous) = &previous {
                assert!(previous < &bytes, "{value}");
            }
            previous = Some(bytes);
        }
    }
}
