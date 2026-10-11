//! Order-preserving encoding of a [`Value`] for placement inside a key.
//!
//! This is what makes value range queries work: a value encoded here sorts, by
//! raw byte comparison, in its semantic order *within its type band* (the value
//! type byte precedes the encoded value in the VAE key, so different types
//! occupy disjoint, contiguous bands and never interleave).
//!
//! Small values are encoded inline and are range-queryable. A value whose
//! encoded form would exceed the tree's inline threshold is spilled: the key
//! carries the value's 32-byte reference instead, which is equality-only. This
//! module produces the encoded value bytes and reports whether they fit inline;
//! the spill decision (threshold and reference substitution) is applied by the
//! caller that knows the tree's manifest.

#[cfg(not(feature = "number-band"))]
use crate::artifacts::ordkey::{encode_f64, encode_i128, encode_u128};
use crate::{
    Value, ValueDataType,
    artifacts::ordkey::{
        decode_bool, decode_bytes, decode_f64, decode_i128, decode_u128, encode_bool, encode_bytes,
    },
};

/// Encodes a [`Value`] into its order-preserving byte form (without the value
/// type byte, which the key carries separately). Byte order equals semantic
/// order for values of the same type.
pub fn encode_value(value: &Value, out: &mut Vec<u8>) {
    match value {
        #[cfg(feature = "number-band")]
        Value::UnsignedInt(number) => dialog_number::encode_u128(*number, out),
        #[cfg(feature = "number-band")]
        Value::SignedInt(number) => dialog_number::encode_i128(*number, out),
        #[cfg(feature = "number-band")]
        Value::Float(number) => dialog_number::encode_f64(*number, out),
        #[cfg(not(feature = "number-band"))]
        Value::UnsignedInt(number) => encode_u128(*number, out),
        #[cfg(not(feature = "number-band"))]
        Value::SignedInt(number) => encode_i128(*number, out),
        #[cfg(not(feature = "number-band"))]
        Value::Float(number) => encode_f64(*number, out),
        Value::Boolean(boolean) => encode_bool(*boolean, out),
        Value::String(string) => encode_bytes(string.as_bytes(), out),
        Value::Bytes(bytes) => encode_bytes(bytes, out),
        Value::Record(bytes) => encode_bytes(bytes, out),
        Value::Entity(entity) => encode_bytes(entity.as_str().as_bytes(), out),
        Value::Symbol(attribute) => encode_bytes(attribute.to_string().as_bytes(), out),
    }
}

/// The order-preserving encoding of a [`Value`], as an owned byte vector.
pub fn encode_value_owned(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value(value, &mut out);
    out
}

/// Decodes an order-preserving value of the given type from the front of
/// `bytes`, reconstructing the [`Value`]. Returns the value and the remaining
/// bytes, or `None` on malformed input.
pub fn decode_value(value_type: ValueDataType, bytes: &[u8]) -> Option<(Value, &[u8])> {
    #[cfg(feature = "number-band")]
    if matches!(
        value_type,
        ValueDataType::UnsignedInt | ValueDataType::SignedInt | ValueDataType::Float
    ) {
        return decode_number(value_type, bytes);
    }
    Some(match value_type {
        ValueDataType::UnsignedInt => {
            let (number, rest) = decode_u128(bytes)?;
            (Value::UnsignedInt(number), rest)
        }
        ValueDataType::SignedInt => {
            let (number, rest) = decode_i128(bytes)?;
            (Value::SignedInt(number), rest)
        }
        ValueDataType::Float => {
            let (number, rest) = decode_f64(bytes)?;
            (Value::Float(number), rest)
        }
        ValueDataType::Boolean => {
            let (boolean, rest) = decode_bool(bytes)?;
            (Value::Boolean(boolean), rest)
        }
        ValueDataType::String => {
            let (raw, rest) = decode_bytes(bytes)?;
            (Value::String(String::from_utf8(raw).ok()?), rest)
        }
        ValueDataType::Bytes => {
            let (raw, rest) = decode_bytes(bytes)?;
            (Value::Bytes(raw), rest)
        }
        ValueDataType::Record => {
            let (raw, rest) = decode_bytes(bytes)?;
            (Value::Record(raw), rest)
        }
        ValueDataType::Entity => {
            let (raw, rest) = decode_bytes(bytes)?;
            let entity = String::from_utf8(raw).ok()?.parse().ok()?;
            (Value::Entity(entity), rest)
        }
        ValueDataType::Symbol => {
            let (raw, rest) = decode_bytes(bytes)?;
            let attribute = String::from_utf8(raw).ok()?.try_into().ok()?;
            (Value::Symbol(attribute), rest)
        }
    })
}

/// Decodes a number in the one band. The band's own type byte (the
/// integer one) reads a whole number as an integer, one too large for an
/// integer as a natural, and any other as a float; a natural or float
/// asked for by name reads as that type, or not at all.
#[cfg(feature = "number-band")]
fn decode_number(value_type: ValueDataType, bytes: &[u8]) -> Option<(Value, &[u8])> {
    let len = dialog_number::encoded_len(bytes)?;
    let (number, rest) = bytes.split_at(len);
    let value = match value_type {
        ValueDataType::Float => Value::Float(dialog_number::decode_f64(number).ok()?),
        ValueDataType::UnsignedInt => {
            Value::UnsignedInt(dialog_number::Number::decode(number).ok()?.to_u128()?)
        }
        _ => match dialog_number::decode_i128(number).ok()? {
            Some(integer) => Value::SignedInt(integer),
            None => {
                // Not a whole number an integer holds: a fraction, which
                // reads as a float without going through `Number`, or a
                // whole number past `i128::MAX`, which a natural may hold.
                let float = dialog_number::decode_f64(number).ok()?;
                match float.fract() == 0.0 && float > 0.0 {
                    true => match dialog_number::Number::decode(number).ok()?.to_u128() {
                        Some(natural) => Value::UnsignedInt(natural),
                        None => Value::Float(float),
                    },
                    false => Value::Float(float),
                }
            }
        },
    };
    Some((value, rest))
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

    /// Every value type round-trips encode -> decode unchanged.
    #[dialog_common::test]
    async fn it_round_trips_every_value_type() -> anyhow::Result<()> {
        let values = vec![
            Value::UnsignedInt(42),
            Value::SignedInt(-7),
            Value::Float(3.5),
            Value::Boolean(true),
            Value::String("hello".into()),
            Value::Bytes(vec![1, 2, 3]),
            Value::Record(vec![9, 8, 7]),
        ];
        for value in values {
            let encoded = encode_value_owned(&value);
            let (decoded, rest) = decode_value(value.data_type(), &encoded).expect("decodes");
            assert_eq!(decoded, value, "round-trip {value:?}");
            assert!(rest.is_empty());
        }
        Ok(())
    }

    /// Encoded unsigned integers sort in numeric order, so a VAE range scan
    /// over the encoding returns the right values.
    #[dialog_common::test]
    async fn it_orders_integers_for_range_scans() -> anyhow::Result<()> {
        let mut encodings: Vec<Vec<u8>> = [3u128, 1, 2, 256, 255, 0]
            .iter()
            .map(|&n| encode_value_owned(&Value::UnsignedInt(n)))
            .collect();
        encodings.sort();
        let decoded: Vec<u128> = encodings
            .iter()
            .map(
                |e| match decode_value(ValueDataType::UnsignedInt, e).unwrap().0 {
                    Value::UnsignedInt(n) => n,
                    _ => unreachable!(),
                },
            )
            .collect();
        assert_eq!(decoded, vec![0, 1, 2, 3, 255, 256], "sorted numerically");
        Ok(())
    }

    /// Signed integers sort across the sign boundary.
    #[dialog_common::test]
    async fn it_orders_signed_across_zero() -> anyhow::Result<()> {
        let neg = encode_value_owned(&Value::SignedInt(-1));
        let zero = encode_value_owned(&Value::SignedInt(0));
        let pos = encode_value_owned(&Value::SignedInt(1));
        assert!(neg < zero && zero < pos);
        Ok(())
    }

    /// Strings sort lexically and are prefix-safe (the terminator).
    #[dialog_common::test]
    async fn it_orders_strings_prefix_safely() -> anyhow::Result<()> {
        let car = encode_value_owned(&Value::String("car".into()));
        let carpet = encode_value_owned(&Value::String("carpet".into()));
        assert!(car < carpet);
        Ok(())
    }
}
