//! Order-preserving byte encoding for key components.
//!
//! The search tree sorts keys by raw byte comparison, and value range queries
//! (the VAE index) require that byte order equal semantic order. So every
//! component placed in a key is encoded here such that
//! `encode(a) < encode(b)` (lexicographically) **iff** `a < b` (semantically).
//!
//! The encodings follow the format-epoch plan (section 3.1), which lifts the
//! numeric encodings from TerminusDB's internals:
//!
//! - **unsigned int**: big-endian (most significant byte first) so numeric
//!   order matches byte order.
//! - **signed int**: big-endian with the sign bit flipped, so negatives sort
//!   below non-negatives and both sort numerically.
//! - **float (f64)**: IEEE-754 bits, then a sign-dependent complement (flip
//!   all bits if negative, flip only the sign bit if non-negative) so the
//!   total bit order matches numeric order.
//! - **string / bytes**: `0x00` escaped as `0x00 0xFF`, terminated by a lone
//!   `0x00`. This keeps a variable-length component *prefix-safe* in a
//!   non-terminal key position: without it, `"car"` and `"carpet"` interleave
//!   depending on the following component's bytes.
//! - **bool**: a single byte, `0` or `1`.
//!
//! Encodings are self-delimiting (numerics are fixed width; strings are
//! terminated), so components concatenate into a key and a reader can split
//! them back out without a separate length table.

use std::borrow::Cow;

/// The terminator byte for escaped variable-length components.
const TERMINATOR: u8 = 0x00;
/// The escape suffix: a `0x00` byte in the payload is written as
/// `0x00 0xFF` so it never looks like the terminator.
///
/// Composition invariant: within a key, the component FOLLOWING a
/// terminated byte string must not begin with this escape byte, or the
/// terminator followed by that first byte reads back as an escaped zero
/// (and sorts wrong for the same reason). Real artifact keys satisfy this
/// (UTF-8 fields and small tag bytes never start with `0xFF`); synthetic
/// bounds must too, which is why `varkey`'s max filler byte is `0xFE`.
const ESCAPE: u8 = 0xFF;

/// Encodes a `u128` big-endian: byte order equals numeric order.
pub fn encode_u128(value: u128, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Decodes a big-endian `u128` from the front of `bytes`, returning the value
/// and the remaining bytes.
pub fn decode_u128(bytes: &[u8]) -> Option<(u128, &[u8])> {
    let (head, rest) = bytes.split_at_checked(16)?;
    Some((u128::from_be_bytes(head.try_into().ok()?), rest))
}

/// Encodes an `i128` big-endian with the sign bit flipped, so negatives sort
/// below non-negatives and both sort numerically.
pub fn encode_i128(value: i128, out: &mut Vec<u8>) {
    let biased = (value as u128) ^ (1u128 << 127);
    out.extend_from_slice(&biased.to_be_bytes());
}

/// Decodes a sign-flipped big-endian `i128`.
pub fn decode_i128(bytes: &[u8]) -> Option<(i128, &[u8])> {
    let (head, rest) = bytes.split_at_checked(16)?;
    let biased = u128::from_be_bytes(head.try_into().ok()?);
    Some(((biased ^ (1u128 << 127)) as i128, rest))
}

/// Encodes an `f64` so the byte order matches numeric order (NaN sorts at an
/// end; `-0.0` and `+0.0` compare equal in value but encode distinctly, which
/// is acceptable for a total byte order).
pub fn encode_f64(value: f64, out: &mut Vec<u8>) {
    let bits = value.to_bits();
    // If the sign bit is set (negative), flip every bit; otherwise flip only
    // the sign bit. This maps the IEEE-754 bit pattern onto a monotonic
    // unsigned order.
    let ordered = if bits >> 63 == 1 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    };
    out.extend_from_slice(&ordered.to_be_bytes());
}

/// Decodes an order-preserving `f64`.
pub fn decode_f64(bytes: &[u8]) -> Option<(f64, &[u8])> {
    let (head, rest) = bytes.split_at_checked(8)?;
    let ordered = u64::from_be_bytes(head.try_into().ok()?);
    let bits = if ordered >> 63 == 1 {
        ordered ^ (1u64 << 63)
    } else {
        !ordered
    };
    Some((f64::from_bits(bits), rest))
}

/// Encodes a bool as a single byte.
pub fn encode_bool(value: bool, out: &mut Vec<u8>) {
    out.push(u8::from(value));
}

/// Decodes a bool byte.
pub fn decode_bool(bytes: &[u8]) -> Option<(bool, &[u8])> {
    let (&byte, rest) = bytes.split_first()?;
    Some((byte != 0, rest))
}

/// Encodes a byte string prefix-safely: each `0x00` becomes `0x00 0xFF`, then
/// a lone `0x00` terminates. Byte order of the encoding equals byte order of
/// the input, and no encoding is a prefix of another with different content.
pub fn encode_bytes(value: &[u8], out: &mut Vec<u8>) {
    for &byte in value {
        out.push(byte);
        if byte == TERMINATOR {
            out.push(ESCAPE);
        }
    }
    out.push(TERMINATOR);
}

/// Decodes a terminated, escaped byte string *without copying when it has no
/// escapes*. Returns the decoded bytes (borrowed from `bytes` when escape-free,
/// owned only when a `0x00 0xFF` escape had to be resolved) and the bytes past
/// the terminator. `None` on a missing terminator or malformed escape.
///
/// Entities and attributes are UTF-8 (URIs, `namespace/predicate`) and so never
/// contain a `0x00` byte, so the escape-free borrow is the norm on the scan
/// path; the owned branch exists only for correctness on `0x00`-bearing bytes.
pub fn decode_bytes_cow(bytes: &[u8]) -> Option<(Cow<'_, [u8]>, &[u8])> {
    let mut at = 0usize;
    while at < bytes.len() {
        match bytes[at] {
            TERMINATOR => match bytes.get(at + 1) {
                // An escaped zero: fall back to the owned, un-escaping decoder
                // from the start (rare; only for `0x00`-bearing components).
                Some(&ESCAPE) => {
                    let (owned, rest) = decode_bytes(bytes)?;
                    return Some((Cow::Owned(owned), rest));
                }
                // A lone terminator: the component is escape-free, so borrow it.
                _ => return Some((Cow::Borrowed(&bytes[..at]), &bytes[at + 1..])),
            },
            _ => at += 1,
        }
    }
    // Ran off the end without a terminator.
    None
}

/// Decodes a terminated, escaped byte string, returning it and the bytes past
/// its terminator. Returns `None` if the terminator is missing or an escape
/// is malformed.
pub fn decode_bytes(bytes: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while at < bytes.len() {
        let byte = bytes[at];
        if byte == TERMINATOR {
            match bytes.get(at + 1) {
                // `0x00 0xFF` is an escaped zero byte.
                Some(&ESCAPE) => {
                    out.push(TERMINATOR);
                    at += 2;
                }
                // A lone `0x00` terminates the string.
                _ => return Some((out, &bytes[at + 1..])),
            }
        } else {
            out.push(byte);
            at += 1;
        }
    }
    // Ran off the end without a terminator.
    None
}

/// Encodes a UTF-8 string prefix-safely (delegates to [`encode_bytes`]).
pub fn encode_str(value: &str, out: &mut Vec<u8>) {
    encode_bytes(value.as_bytes(), out);
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]
    // The dialog_common::test macro requires async test fns; these pure-codec
    // tests await nothing.
    #![allow(clippy::unused_async)]

    use super::*;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn enc_u128(v: u128) -> Vec<u8> {
        let mut out = Vec::new();
        encode_u128(v, &mut out);
        out
    }
    fn enc_i128(v: i128) -> Vec<u8> {
        let mut out = Vec::new();
        encode_i128(v, &mut out);
        out
    }
    fn enc_f64(v: f64) -> Vec<u8> {
        let mut out = Vec::new();
        encode_f64(v, &mut out);
        out
    }
    fn enc_str(v: &str) -> Vec<u8> {
        let mut out = Vec::new();
        encode_str(v, &mut out);
        out
    }

    /// Unsigned integer encoding sorts numerically and round-trips.
    #[dialog_common::test]
    async fn it_orders_and_round_trips_unsigned() -> anyhow::Result<()> {
        let values = [0u128, 1, 255, 256, 65535, u64::MAX as u128, u128::MAX];
        for pair in values.windows(2) {
            assert!(enc_u128(pair[0]) < enc_u128(pair[1]), "{pair:?}");
        }
        for &v in &values {
            let encoded = enc_u128(v);
            let (decoded, rest) = decode_u128(&encoded).unwrap();
            assert_eq!(decoded, v);
            assert!(rest.is_empty());
        }
        Ok(())
    }

    /// Signed integer encoding sorts numerically across the sign boundary.
    #[dialog_common::test]
    async fn it_orders_and_round_trips_signed() -> anyhow::Result<()> {
        let values = [i128::MIN, -256, -1, 0, 1, 256, i128::MAX];
        for pair in values.windows(2) {
            assert!(
                enc_i128(pair[0]) < enc_i128(pair[1]),
                "{} < {}",
                pair[0],
                pair[1]
            );
        }
        for &v in &values {
            let encoded = enc_i128(v);
            let (decoded, rest) = decode_i128(&encoded).unwrap();
            assert_eq!(decoded, v);
            assert!(rest.is_empty());
        }
        Ok(())
    }

    /// Float encoding sorts numerically including across zero and negatives.
    #[dialog_common::test]
    async fn it_orders_and_round_trips_floats() -> anyhow::Result<()> {
        let values = [
            f64::NEG_INFINITY,
            -1e300,
            -1.0,
            -f64::MIN_POSITIVE,
            0.0,
            f64::MIN_POSITIVE,
            1.0,
            1e300,
            f64::INFINITY,
        ];
        for pair in values.windows(2) {
            assert!(
                enc_f64(pair[0]) < enc_f64(pair[1]),
                "{} < {}",
                pair[0],
                pair[1]
            );
        }
        for &v in &values {
            let encoded = enc_f64(v);
            let (decoded, rest) = decode_f64(&encoded).unwrap();
            assert_eq!(decoded.to_bits(), v.to_bits());
            assert!(rest.is_empty());
        }
        Ok(())
    }

    /// The `"car"` / `"carpet"` case: a shorter string sorts before a longer
    /// one that extends it, and the terminator keeps it prefix-safe even with
    /// a following component appended.
    #[dialog_common::test]
    async fn it_orders_strings_prefix_safely() -> anyhow::Result<()> {
        assert!(enc_str("car") < enc_str("carpet"));
        assert!(enc_str("car") < enc_str("cat"));
        assert!(enc_str("") < enc_str("a"));

        // Append a following component's bytes to each and confirm the order
        // is decided by the string, not the follower (the interleaving bug).
        let mut car_then_z = enc_str("car");
        car_then_z.push(b'z');
        let mut carpet_then_a = enc_str("carpet");
        carpet_then_a.push(b'a');
        assert!(
            car_then_z < carpet_then_a,
            "terminator must decide order before the follower"
        );
        Ok(())
    }

    /// Strings containing `0x00` round-trip and stay ordered (escaping).
    #[dialog_common::test]
    async fn it_escapes_and_round_trips_zero_bytes() -> anyhow::Result<()> {
        let cases: [&[u8]; 5] = [b"", b"\x00", b"a\x00b", b"\x00\x00", b"a"];
        for case in cases {
            let mut encoded = Vec::new();
            encode_bytes(case, &mut encoded);
            let (decoded, rest) = decode_bytes(&encoded).unwrap();
            assert_eq!(decoded, case, "round-trip {case:?}");
            assert!(rest.is_empty());
        }

        // A string with an embedded zero still sorts correctly against one
        // without: "a\x00" (a, then zero) sorts after "a" (a, terminator).
        let mut a = Vec::new();
        encode_bytes(b"a", &mut a);
        let mut a_zero = Vec::new();
        encode_bytes(b"a\x00", &mut a_zero);
        assert!(a < a_zero, "escaped zero must sort after the terminator");
        Ok(())
    }

    /// Two components concatenated decode back in order without a length table.
    #[dialog_common::test]
    async fn it_self_delimits_concatenated_components() -> anyhow::Result<()> {
        let mut key = Vec::new();
        encode_str("entity", &mut key);
        encode_str("attribute", &mut key);
        encode_u128(42, &mut key);

        let (entity, rest) = decode_bytes(&key).unwrap();
        let (attribute, rest) = decode_bytes(rest).unwrap();
        let (value, rest) = decode_u128(rest).unwrap();
        assert_eq!(entity, b"entity");
        assert_eq!(attribute, b"attribute");
        assert_eq!(value, 42);
        assert!(rest.is_empty());
        Ok(())
    }
}
