//! Number semantics at the value layer: the one band's key identity, the
//! narrowest-type decode, the views a number is read through, what a type
//! holds, and when two values are one number.

#![allow(unexpected_cfgs)]

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use serde::Deserialize as _;
use serde::de::IntoDeserializer as _;
use serde::de::value::{Error, F64Deserializer};

use crate::{NUMBER_BAND, Value, ValueDataType, decode_value, encode_key_value_owned};

/// The band key of `value`.
fn key(value: &Value) -> Vec<u8> {
    encode_key_value_owned(value, NUMBER_BAND)
}

/// The value the band reads back from `value`'s key.
fn read(value: &Value) -> Value {
    let bytes = key(value);
    let (decoded, rest) = decode_value(ValueDataType::Number, &bytes).expect("decodes");
    assert!(rest.is_empty(), "{value:?} leaves {rest:?}");
    decoded
}

const TWO_POW_53: u128 = 1 << 53;
const TWO_POW_64: u128 = 1 << 64;
/// `2^64` as a float is the decimal it prints as, which is not `2^64`.
const TWO_POW_64_AS_FLOAT_PRINTS: u128 = 18_446_744_073_709_552_000;
/// `2^127` as a float is the decimal it prints as, inside `i128`.
const TWO_POW_127_AS_FLOAT_PRINTS: i128 = 170_141_183_460_469_230_000_000_000_000_000_000_000;
const TEN_POW_30: u128 = 1_000_000_000_000_000_000_000_000_000_000;

/// Whole numbers every number type holds exactly and whose shortest decimal
/// is the number itself: a natural, an integer and a float of one of them
/// are one key in the band.
const COMMON_WHOLE: [u128; 11] = [
    0,
    1,
    2,
    3,
    100,
    255,
    256,
    65_536,
    1 << 32,
    TWO_POW_53 - 1,
    TWO_POW_53,
];

/// S1: in the band the natural `n`, the integer `n` and the float `n` are
/// one key, byte for byte, for every whole number all three hold exactly;
/// negative integers and floats agree the same way.
#[dialog_common::test]
fn it_keys_one_number_the_same_whatever_type_holds_it() {
    for n in COMMON_WHOLE {
        let natural = key(&Value::UnsignedInt(n));
        assert_eq!(natural, key(&Value::SignedInt(n as i128)), "integer {n}");
        assert_eq!(natural, key(&Value::Float(n as f64)), "float {n}");
        if n != 0 {
            let integer = key(&Value::SignedInt(-(n as i128)));
            assert_eq!(integer, key(&Value::Float(-(n as f64))), "float -{n}");
            assert_ne!(integer, natural, "-{n} is not {n}");
        }
    }
}

/// S1: negative zero is zero: the float `-0.0` keys as the natural `0`.
#[dialog_common::test]
fn it_keys_negative_zero_as_zero() {
    assert_eq!(key(&Value::Float(-0.0)), key(&Value::UnsignedInt(0)));
    assert_eq!(key(&Value::Float(-0.0)), key(&Value::Float(0.0)));
    assert_eq!(read(&Value::Float(-0.0)), Value::UnsignedInt(0));
}

/// Every edge number once, written as its narrowest type, in numeric
/// order. The decode of any of them is itself.
fn edges_in_order() -> Vec<Value> {
    vec![
        Value::Float(f64::NEG_INFINITY),
        Value::SignedInt(i128::MIN),
        Value::SignedInt(-(TEN_POW_30 as i128)),
        Value::SignedInt(i64::MIN as i128),
        Value::SignedInt(-3),
        Value::Float(-2.5),
        Value::SignedInt(-1),
        Value::Float(-0.5),
        Value::UnsignedInt(0),
        Value::Float(5e-324),
        Value::Float(f64::MIN_POSITIVE),
        Value::Float(0.1),
        Value::Float(0.5),
        Value::UnsignedInt(1),
        Value::Float(2.5),
        Value::UnsignedInt(TWO_POW_53 + 1),
        Value::UnsignedInt(u64::MAX as u128),
        Value::UnsignedInt(TWO_POW_64),
        Value::UnsignedInt(TEN_POW_30),
        Value::UnsignedInt(i128::MAX as u128 + 1),
        Value::UnsignedInt(u128::MAX),
        Value::Float(1e39),
        Value::Float(f64::MAX),
        Value::Float(f64::INFINITY),
    ]
}

/// S1: different numbers have different band keys, and the keys order as
/// the numbers do, across natural, integer and float.
#[dialog_common::test]
fn it_orders_band_keys_by_number_across_types() {
    let keys: Vec<Vec<u8>> = edges_in_order().iter().map(key).collect();
    for pair in keys.windows(2) {
        assert!(pair[0] < pair[1], "{:02x?} < {:02x?}", pair[0], pair[1]);
    }
}

/// S2: the band reads a number back as the narrowest type that holds it: a
/// whole number as a natural when it is not negative, as an integer when it
/// is, and any other as a float, with infinities and whole floats past
/// `u128` staying floats. Large integers and fractions are exact: no float
/// detour for `u128::MAX`, `i128::MIN` or `2^53 + 1`, and `0.1`, `5e-324`
/// and `f64::MAX` come back bit for bit.
#[dialog_common::test]
fn it_decodes_the_narrowest_type_exactly() {
    let cases = [
        (Value::UnsignedInt(0), Value::UnsignedInt(0)),
        (Value::SignedInt(0), Value::UnsignedInt(0)),
        (Value::Float(0.0), Value::UnsignedInt(0)),
        (Value::SignedInt(3), Value::UnsignedInt(3)),
        (Value::Float(3.0), Value::UnsignedInt(3)),
        (Value::Float(1.0), Value::UnsignedInt(1)),
        (Value::SignedInt(-1), Value::SignedInt(-1)),
        (Value::Float(-1.0), Value::SignedInt(-1)),
        (Value::Float(2.5), Value::Float(2.5)),
        (Value::Float(-2.5), Value::Float(-2.5)),
        (Value::Float(0.5), Value::Float(0.5)),
        (Value::Float(0.1), Value::Float(0.1)),
        (Value::Float(5e-324), Value::Float(5e-324)),
        (
            Value::Float(f64::MIN_POSITIVE),
            Value::Float(f64::MIN_POSITIVE),
        ),
        (Value::Float(f64::MAX), Value::Float(f64::MAX)),
        (Value::Float(1e39), Value::Float(1e39)),
        (Value::Float(f64::INFINITY), Value::Float(f64::INFINITY)),
        (
            Value::Float(f64::NEG_INFINITY),
            Value::Float(f64::NEG_INFINITY),
        ),
        (
            Value::UnsignedInt(u64::MAX as u128),
            Value::UnsignedInt(u64::MAX as u128),
        ),
        (
            Value::UnsignedInt(TWO_POW_64),
            Value::UnsignedInt(TWO_POW_64),
        ),
        (
            Value::UnsignedInt(TWO_POW_53 + 1),
            Value::UnsignedInt(TWO_POW_53 + 1),
        ),
        (
            Value::SignedInt(TWO_POW_53 as i128 + 1),
            Value::UnsignedInt(TWO_POW_53 + 1),
        ),
        (Value::UnsignedInt(u128::MAX), Value::UnsignedInt(u128::MAX)),
        (Value::SignedInt(i128::MIN), Value::SignedInt(i128::MIN)),
        (
            Value::SignedInt(i64::MIN as i128),
            Value::SignedInt(i64::MIN as i128),
        ),
        (
            Value::SignedInt(i128::MAX),
            Value::UnsignedInt(i128::MAX as u128),
        ),
    ];
    for (written, expected) in cases {
        assert_eq!(read(&written), expected, "{written:?}");
    }
    for value in edges_in_order() {
        assert_eq!(read(&value), value, "{value:?} is its own narrowest type");
    }
}

/// A float is the decimal it prints as, so a whole float whose shortest
/// decimal is a whole number reads back as that natural or integer:
/// `1e30` is the natural `10^30`.
#[dialog_common::test]
fn it_reads_a_whole_float_as_the_whole_number_it_prints_as() {
    assert_eq!(read(&Value::Float(1e30)), Value::UnsignedInt(TEN_POW_30));
    assert_eq!(
        read(&Value::Float(-1e30)),
        Value::SignedInt(-(TEN_POW_30 as i128))
    );
    assert_eq!(
        key(&Value::Float(1e30)),
        key(&Value::UnsignedInt(TEN_POW_30))
    );
}

/// NaN is not a value: a serialized float that is NaN is refused.
#[dialog_common::test]
fn it_refuses_nan_on_deserialization() {
    for nan in [f64::NAN, -f64::NAN, f64::from_bits(0x7ff8_0000_0000_0001)] {
        let deserializer: F64Deserializer<Error> = nan.into_deserializer();
        let error = Value::deserialize(deserializer).expect_err("NaN is refused");
        assert_eq!(error.to_string(), "NaN is not a value");
    }
    let deserializer: F64Deserializer<Error> = f64::INFINITY.into_deserializer();
    assert_eq!(
        Value::deserialize(deserializer).expect("infinity is a value"),
        Value::Float(f64::INFINITY)
    );
}

/// S6: the unsized views. A natural view sees a number that is not
/// negative, floored, and nothing it cannot hold; an integer view sees any
/// number it can hold, floored; a float view sees every number as the
/// nearest float; `number:` sees every number as it is. No integer view
/// sees an infinity.
#[dialog_common::test]
fn it_reads_numbers_through_the_unsized_views() {
    use ValueDataType::{Float, Number, SignedInt, UnsignedInt};
    let cases: Vec<(Value, ValueDataType, Option<Value>)> = vec![
        (Value::UnsignedInt(3), SignedInt, Some(Value::SignedInt(3))),
        (Value::UnsignedInt(3), Float, Some(Value::Float(3.0))),
        (Value::UnsignedInt(3), Number, Some(Value::UnsignedInt(3))),
        (
            Value::SignedInt(3),
            UnsignedInt,
            Some(Value::UnsignedInt(3)),
        ),
        (Value::SignedInt(-1), UnsignedInt, None),
        (Value::SignedInt(-1), Float, Some(Value::Float(-1.0))),
        (Value::SignedInt(-1), Number, Some(Value::SignedInt(-1))),
        (Value::Float(2.5), UnsignedInt, Some(Value::UnsignedInt(2))),
        (Value::Float(2.5), SignedInt, Some(Value::SignedInt(2))),
        (Value::Float(2.5), Float, Some(Value::Float(2.5))),
        (Value::Float(2.5), Number, Some(Value::Float(2.5))),
        (Value::Float(-2.5), UnsignedInt, None),
        (Value::Float(-2.5), SignedInt, Some(Value::SignedInt(-3))),
        (Value::Float(0.5), UnsignedInt, Some(Value::UnsignedInt(0))),
        (Value::Float(0.5), SignedInt, Some(Value::SignedInt(0))),
        (Value::Float(-0.5), UnsignedInt, None),
        (Value::Float(-0.5), SignedInt, Some(Value::SignedInt(-1))),
        (Value::Float(-0.0), UnsignedInt, Some(Value::UnsignedInt(0))),
        (Value::Float(-0.0), SignedInt, Some(Value::SignedInt(0))),
        (Value::Float(0.1), Float, Some(Value::Float(0.1))),
        (
            Value::Float(5e-324),
            UnsignedInt,
            Some(Value::UnsignedInt(0)),
        ),
        (Value::Float(5e-324), Float, Some(Value::Float(5e-324))),
        // A float is the decimal it prints as, so an integer view reads
        // `2^64` as a float as 18446744073709552000, the band's reading.
        (
            Value::Float(TWO_POW_64 as f64),
            UnsignedInt,
            Some(Value::UnsignedInt(TWO_POW_64_AS_FLOAT_PRINTS)),
        ),
        (
            Value::Float(TWO_POW_64 as f64),
            SignedInt,
            Some(Value::SignedInt(TWO_POW_64_AS_FLOAT_PRINTS as i128)),
        ),
        (Value::Float(1e39), UnsignedInt, None),
        (Value::Float(1e39), SignedInt, None),
        (Value::Float(1e39), Float, Some(Value::Float(1e39))),
        (Value::Float(1e39), Number, Some(Value::Float(1e39))),
        (Value::Float(-1e30), UnsignedInt, None),
        (
            Value::Float(-1e30),
            SignedInt,
            Some(Value::SignedInt(-(TEN_POW_30 as i128))),
        ),
        (Value::Float(f64::MAX), UnsignedInt, None),
        (Value::Float(f64::MAX), SignedInt, None),
        (Value::Float(f64::MAX), Float, Some(Value::Float(f64::MAX))),
        (Value::Float(f64::INFINITY), UnsignedInt, None),
        (Value::Float(f64::INFINITY), SignedInt, None),
        (
            Value::Float(f64::INFINITY),
            Float,
            Some(Value::Float(f64::INFINITY)),
        ),
        (
            Value::Float(f64::INFINITY),
            Number,
            Some(Value::Float(f64::INFINITY)),
        ),
        (Value::Float(f64::NEG_INFINITY), UnsignedInt, None),
        (Value::Float(f64::NEG_INFINITY), SignedInt, None),
        (
            Value::Float(f64::NEG_INFINITY),
            Float,
            Some(Value::Float(f64::NEG_INFINITY)),
        ),
        (
            Value::UnsignedInt(u128::MAX),
            UnsignedInt,
            Some(Value::UnsignedInt(u128::MAX)),
        ),
        (Value::UnsignedInt(u128::MAX), SignedInt, None),
        (
            Value::UnsignedInt(u128::MAX),
            Float,
            Some(Value::Float(
                340_282_366_920_938_463_463_374_607_431_768_211_455.0,
            )),
        ),
        (
            Value::UnsignedInt(i128::MAX as u128),
            SignedInt,
            Some(Value::SignedInt(i128::MAX)),
        ),
        (Value::UnsignedInt(i128::MAX as u128 + 1), SignedInt, None),
        (Value::SignedInt(i128::MIN), UnsignedInt, None),
        (
            Value::SignedInt(i128::MIN),
            SignedInt,
            Some(Value::SignedInt(i128::MIN)),
        ),
        (
            Value::SignedInt(i128::MIN),
            Float,
            Some(Value::Float(
                -170_141_183_460_469_231_731_687_303_715_884_105_728.0,
            )),
        ),
        // `±2^127` as floats print as ±170141183460469230000000000000000000000,
        // inside `i128`; `2^128` prints as a decimal past `u128`.
        (
            Value::Float(i128::MIN as f64),
            SignedInt,
            Some(Value::SignedInt(-TWO_POW_127_AS_FLOAT_PRINTS)),
        ),
        (
            Value::Float(i128::MAX as f64),
            SignedInt,
            Some(Value::SignedInt(TWO_POW_127_AS_FLOAT_PRINTS)),
        ),
        (Value::Float(u128::MAX as f64), UnsignedInt, None),
        (
            Value::UnsignedInt(TWO_POW_53 + 1),
            Float,
            Some(Value::Float(9_007_199_254_740_992.0)),
        ),
        (
            Value::UnsignedInt(TWO_POW_53 + 1),
            SignedInt,
            Some(Value::SignedInt(TWO_POW_53 as i128 + 1)),
        ),
    ];
    for (value, view, expected) in cases {
        assert_eq!(
            value.clone().viewed_as(view),
            expected,
            "{value:?} as {view}"
        );
    }
}

/// S6: a sized view sees only the numbers inside its range, and sees a
/// number inside it at its precision: an integer view floors a fraction,
/// so `max + 0.5` is seen as `max` while `max + 1` and `min - 1` are not
/// seen at all; `float:32` rounds through `f32` and does not see a finite
/// number past its range; `float:64` keeps the number as it is. Every
/// width's exact edges are pinned, as values of each number type.
#[dialog_common::test]
fn it_reads_the_edges_of_every_sized_view() {
    use ValueDataType::{F32, F64, I8, I16, I32, I64, I128, U8, U16, U32, U64, U128};
    let natural = |n: u128| Value::UnsignedInt(n);
    let integer = |n: i128| Value::SignedInt(n);
    let float = |f: f64| Value::Float(f);
    let mut cases: Vec<(Value, ValueDataType, Option<Value>)> = Vec::new();

    // Natural views: [0, 2^bits).
    for (view, bits) in [(U8, 8u32), (U16, 16), (U32, 32), (U64, 64)] {
        let max = (1u128 << bits) - 1;
        cases.extend([
            (natural(max), view, Some(natural(max))),
            (integer(max as i128), view, Some(natural(max))),
            (natural(max + 1), view, None),
            (integer(max as i128 + 1), view, None),
            (natural(0), view, Some(natural(0))),
            (integer(0), view, Some(natural(0))),
            (float(-0.0), view, Some(natural(0))),
            (integer(-1), view, None),
            (float(-0.5), view, None),
            (float(0.5), view, Some(natural(0))),
        ]);
        if bits <= 32 {
            // Only here does `max + 0.5` exist as a float.
            cases.extend([
                (float(max as f64), view, Some(natural(max))),
                (float(max as f64 + 0.5), view, Some(natural(max))),
                (float(max as f64 + 1.0), view, None),
            ]);
        } else {
            // `u64::MAX` rounds to `2^64` as a float, which the view does
            // not see.
            cases.push((float(max as f64), view, None));
        }
    }
    cases.extend([
        (natural(u128::MAX), U128, Some(natural(u128::MAX))),
        (integer(i128::MAX), U128, Some(natural(i128::MAX as u128))),
        (natural(0), U128, Some(natural(0))),
        (integer(-1), U128, None),
        (float(-0.5), U128, None),
        (float(0.5), U128, Some(natural(0))),
        (float(u128::MAX as f64), U128, None),
        (float(1e39), U128, None),
        (float(f64::INFINITY), U128, None),
        (
            float(TWO_POW_64 as f64),
            U128,
            Some(natural(TWO_POW_64_AS_FLOAT_PRINTS)),
        ),
    ]);

    // Integer views: [-2^(bits-1), 2^(bits-1)).
    for (view, bits) in [(I8, 8u32), (I16, 16), (I32, 32), (I64, 64)] {
        let max = (1i128 << (bits - 1)) - 1;
        let min = -(1i128 << (bits - 1));
        cases.extend([
            (integer(max), view, Some(integer(max))),
            (natural(max as u128), view, Some(integer(max))),
            (integer(max + 1), view, None),
            (natural(max as u128 + 1), view, None),
            (integer(min), view, Some(integer(min))),
            (integer(min - 1), view, None),
            (float(-0.5), view, Some(integer(-1))),
            (float(0.5), view, Some(integer(0))),
        ]);
        if bits <= 32 {
            cases.extend([
                (float(min as f64), view, Some(integer(min))),
                (float(max as f64), view, Some(integer(max))),
                (float(max as f64 + 0.5), view, Some(integer(max))),
                (float(max as f64 + 1.0), view, None),
                (float(min as f64 - 0.5), view, None),
            ]);
        } else {
            // `i64::MAX` rounds to `2^63` as a float, past the view, and
            // `i64::MIN` as a float prints as -9223372036854776000, below
            // it.
            cases.push((float(max as f64), view, None));
            cases.push((float(min as f64), view, None));
        }
    }
    cases.extend([
        (integer(i128::MAX), I128, Some(integer(i128::MAX))),
        (natural(i128::MAX as u128), I128, Some(integer(i128::MAX))),
        (natural(i128::MAX as u128 + 1), I128, None),
        (natural(u128::MAX), I128, None),
        (integer(i128::MIN), I128, Some(integer(i128::MIN))),
        (
            float(i128::MIN as f64),
            I128,
            Some(integer(-TWO_POW_127_AS_FLOAT_PRINTS)),
        ),
        (
            float(i128::MAX as f64),
            I128,
            Some(integer(TWO_POW_127_AS_FLOAT_PRINTS)),
        ),
        (float(-1e30), I128, Some(integer(-(TEN_POW_30 as i128)))),
        (float(1e39), I128, None),
        (float(f64::NEG_INFINITY), I128, None),
    ]);

    // Float views.
    let past_f32 = f64::from(f32::MAX) * (1.0 + f64::EPSILON);
    cases.extend([
        (
            float(f64::from(f32::MAX)),
            F32,
            Some(float(f64::from(f32::MAX))),
        ),
        (
            float(-f64::from(f32::MAX)),
            F32,
            Some(float(-f64::from(f32::MAX))),
        ),
        (float(past_f32), F32, None),
        (float(-past_f32), F32, None),
        (float(1e39), F32, None),
        (float(f64::MAX), F32, None),
        (float(f64::INFINITY), F32, Some(float(f64::INFINITY))),
        (
            float(f64::NEG_INFINITY),
            F32,
            Some(float(f64::NEG_INFINITY)),
        ),
        (float(0.1), F32, Some(float(f64::from(0.1f32)))),
        (float(2.5), F32, Some(float(2.5))),
        (natural(16_777_216), F32, Some(float(16_777_216.0))),
        (natural(16_777_217), F32, Some(float(16_777_216.0))),
        (integer(-3), F32, Some(float(-3.0))),
        (float(f64::MIN_POSITIVE), F32, Some(float(0.0))),
        (float(5e-324), F32, Some(float(0.0))),
        (float(f64::MAX), F64, Some(float(f64::MAX))),
        (float(-f64::MAX), F64, Some(float(-f64::MAX))),
        (float(1e39), F64, Some(float(1e39))),
        (float(0.1), F64, Some(float(0.1))),
        (float(5e-324), F64, Some(float(5e-324))),
        (float(f64::INFINITY), F64, Some(float(f64::INFINITY))),
        (natural(TWO_POW_53), F64, Some(float(TWO_POW_53 as f64))),
        (natural(TWO_POW_53 + 1), F64, Some(float(TWO_POW_53 as f64))),
        (integer(i64::MIN as i128), F64, Some(float(i64::MIN as f64))),
        (natural(u128::MAX), F64, Some(float(u128::MAX as f64))),
    ]);

    for (value, view, expected) in cases {
        assert_eq!(
            value.clone().viewed_as(view),
            expected,
            "{value:?} as {view}"
        );
    }
}

/// S6: a sized view's reading is the same on every target, so the readings
/// that depend on float rounding are pinned as exact bits.
#[dialog_common::test]
fn it_rounds_through_views_identically_on_every_target() {
    use ValueDataType::{F32, F64, SignedInt, UnsignedInt};
    fn bits(value: Option<Value>) -> String {
        match value {
            Some(Value::Float(float)) => format!("f{:016x}", float.to_bits()),
            Some(Value::UnsignedInt(natural)) => format!("u{natural}"),
            Some(Value::SignedInt(integer)) => format!("i{integer}"),
            Some(other) => format!("{other:?}"),
            None => "none".into(),
        }
    }
    let cases: [(Value, ValueDataType, &str); 12] = [
        (Value::Float(0.1), F32, "f3fb99999a0000000"),
        (Value::Float(5e-324), F32, "f0000000000000000"),
        (Value::Float(f64::MIN_POSITIVE), F32, "f0000000000000000"),
        (Value::Float(f64::from(f32::MAX)), F32, "f47efffffe0000000"),
        (Value::UnsignedInt(16_777_217), F32, "f4170000000000000"),
        (Value::UnsignedInt(TWO_POW_53 + 1), F64, "f4340000000000000"),
        (Value::UnsignedInt(u128::MAX), F64, "f47f0000000000000"),
        (Value::SignedInt(i128::MIN), F64, "fc7e0000000000000"),
        (Value::Float(1e39), UnsignedInt, "none"),
        (
            Value::Float(-1e30),
            SignedInt,
            "i-1000000000000000000000000000000",
        ),
        (Value::Float(f64::MAX), SignedInt, "none"),
        (
            Value::Float(TWO_POW_64 as f64),
            UnsignedInt,
            "u18446744073709552000",
        ),
    ];
    for (value, view, expected) in cases {
        assert_eq!(
            bits(value.clone().viewed_as(view)),
            expected,
            "{value:?} as {view}"
        );
    }
}

/// S6: `holds` is strict. `natural:8` holds 255, the integer 255 and the
/// float 255.0, and not 254.5, 256 or -1; every sized view holds exactly
/// the numbers it reads unchanged; `number:` holds every number, an
/// infinity included; an unsized type holds values of its own type only,
/// so `float:` does not hold the natural 3 while `float:64` does.
#[dialog_common::test]
fn it_holds_what_a_view_reads_unchanged() {
    use ValueDataType::{
        F32, F64, Float, I8, I64, I128, Number, SignedInt, U8, U64, U128, UnsignedInt,
    };
    let cases: Vec<(ValueDataType, Value, bool)> = vec![
        (U8, Value::UnsignedInt(255), true),
        (U8, Value::SignedInt(255), true),
        (U8, Value::Float(255.0), true),
        (U8, Value::Float(254.5), false),
        (U8, Value::UnsignedInt(256), false),
        (U8, Value::SignedInt(-1), false),
        (U8, Value::Float(-0.0), true),
        (U8, Value::Float(f64::INFINITY), false),
        (I8, Value::SignedInt(-128), true),
        (I8, Value::Float(-128.0), true),
        (I8, Value::SignedInt(-129), false),
        (I8, Value::Float(-127.5), false),
        (I8, Value::UnsignedInt(127), true),
        (I8, Value::UnsignedInt(128), false),
        (U64, Value::UnsignedInt(u64::MAX as u128), true),
        (U64, Value::UnsignedInt(TWO_POW_64), false),
        (U64, Value::Float(TWO_POW_64 as f64), false),
        (I64, Value::SignedInt(i64::MIN as i128), true),
        (I64, Value::Float(i64::MIN as f64), false),
        (I128, Value::Float(i64::MIN as f64), true),
        (I64, Value::SignedInt(i64::MIN as i128 - 1), false),
        (U128, Value::UnsignedInt(u128::MAX), true),
        (U128, Value::SignedInt(-1), false),
        (I128, Value::SignedInt(i128::MIN), true),
        (I128, Value::UnsignedInt(u128::MAX), false),
        (F32, Value::Float(2.5), true),
        (F32, Value::Float(0.1), false),
        (F32, Value::UnsignedInt(16_777_216), true),
        (F32, Value::UnsignedInt(16_777_217), false),
        (F32, Value::Float(1e39), false),
        (F32, Value::Float(f64::INFINITY), true),
        (F64, Value::Float(0.1), true),
        (F64, Value::Float(f64::MAX), true),
        (F64, Value::Float(5e-324), true),
        (F64, Value::Float(f64::NEG_INFINITY), true),
        (F64, Value::UnsignedInt(3), true),
        (F64, Value::UnsignedInt(TWO_POW_53), true),
        (F64, Value::UnsignedInt(TWO_POW_53 + 1), false),
        (F64, Value::UnsignedInt(u128::MAX), false),
        // `-2^127` as a float prints as another decimal, so `float:64`
        // reads it changed; `-10^30` it reads as it is.
        (F64, Value::SignedInt(i128::MIN), false),
        (F64, Value::SignedInt(-(TEN_POW_30 as i128)), true),
        (Number, Value::UnsignedInt(u128::MAX), true),
        (Number, Value::SignedInt(i128::MIN), true),
        (Number, Value::Float(2.5), true),
        (Number, Value::Float(f64::INFINITY), true),
        (Number, Value::Float(f64::NEG_INFINITY), true),
        (Number, Value::Boolean(true), false),
        (Number, Value::String("3".into()), false),
        // An unsized number type holds the numbers its view reads
        // unchanged, as a sized one does.
        (UnsignedInt, Value::UnsignedInt(3), true),
        (UnsignedInt, Value::SignedInt(3), true),
        (UnsignedInt, Value::Float(3.0), true),
        (UnsignedInt, Value::SignedInt(-3), false),
        (UnsignedInt, Value::Float(2.5), false),
        (UnsignedInt, Value::Float(f64::INFINITY), false),
        (SignedInt, Value::SignedInt(-3), true),
        (SignedInt, Value::UnsignedInt(3), true),
        (SignedInt, Value::Float(-3.0), true),
        (SignedInt, Value::Float(-2.5), false),
        (SignedInt, Value::UnsignedInt(u128::MAX), false),
        (Float, Value::Float(3.0), true),
        (Float, Value::Float(f64::INFINITY), true),
        (Float, Value::UnsignedInt(3), true),
        (Float, Value::SignedInt(3), true),
        (Float, Value::UnsignedInt(TWO_POW_53 + 1), false),
        (Float, Value::UnsignedInt(TEN_POW_30), true),
        (Float, Value::UnsignedInt(TWO_POW_64), false),
    ];
    for (kind, value, expected) in cases {
        assert_eq!(kind.holds(&value), expected, "{kind} holds {value:?}");
    }
}

/// S6: two values are the same number whatever types hold them when they
/// name one number exactly: the natural 3, the integer 3 and the float
/// 3.0 are one; `-0.0` is 0; a float equals a whole number only when it is
/// that number, so `2^53 + 1` is not the float nearest to it.
#[dialog_common::test]
fn it_tells_the_same_number_across_types() {
    let same: [(Value, Value); 11] = [
        (Value::UnsignedInt(3), Value::SignedInt(3)),
        (Value::UnsignedInt(3), Value::Float(3.0)),
        (Value::SignedInt(3), Value::Float(3.0)),
        (Value::SignedInt(-3), Value::Float(-3.0)),
        (Value::UnsignedInt(0), Value::Float(-0.0)),
        (Value::SignedInt(0), Value::Float(-0.0)),
        (Value::Float(0.0), Value::Float(-0.0)),
        (Value::Float(2.5), Value::Float(2.5)),
        (
            Value::UnsignedInt(TWO_POW_64_AS_FLOAT_PRINTS),
            Value::Float(TWO_POW_64 as f64),
        ),
        (Value::UnsignedInt(TEN_POW_30), Value::Float(1e30)),
        (Value::Float(f64::INFINITY), Value::Float(f64::INFINITY)),
    ];
    for (a, b) in same {
        assert!(a.same_number(&b), "{a:?} is {b:?}");
        assert!(b.same_number(&a), "{b:?} is {a:?}");
    }
    let different: [(Value, Value); 10] = [
        (Value::UnsignedInt(3), Value::SignedInt(-3)),
        (
            Value::UnsignedInt(TWO_POW_64),
            Value::Float(TWO_POW_64 as f64),
        ),
        (Value::UnsignedInt(1), Value::SignedInt(-1)),
        (Value::Float(2.5), Value::SignedInt(2)),
        (Value::Float(2.5), Value::UnsignedInt(3)),
        (
            Value::UnsignedInt(TWO_POW_53 + 1),
            Value::Float(TWO_POW_53 as f64),
        ),
        (
            Value::UnsignedInt(u128::MAX),
            Value::Float(u128::MAX as f64),
        ),
        (Value::Float(f64::INFINITY), Value::Float(f64::NEG_INFINITY)),
        (Value::Float(f64::INFINITY), Value::UnsignedInt(u128::MAX)),
        (Value::UnsignedInt(3), Value::String("3".into())),
    ];
    for (a, b) in different {
        assert!(!a.same_number(&b), "{a:?} is not {b:?}");
        assert!(!b.same_number(&a), "{b:?} is not {a:?}");
    }
}

/// S6: a sized number type is stored as its unsized kind and keyed in the
/// band with it; the unsized kinds and `number:` are no view.
#[dialog_common::test]
fn it_bands_every_number_type_together() {
    use ValueDataType::{
        F32, F64, Float, I8, I16, I32, I64, I128, Number, SignedInt, U8, U16, U32, U64, U128,
        UnsignedInt,
    };
    for view in [U8, U16, U32, U64, U128] {
        assert!(view.is_view());
        assert_eq!(view.storage(), UnsignedInt);
        assert_eq!(view.band(NUMBER_BAND), Number);
        assert_eq!(view.band(0), UnsignedInt);
    }
    for view in [I8, I16, I32, I64, I128] {
        assert!(view.is_view());
        assert_eq!(view.storage(), SignedInt);
        assert_eq!(view.band(NUMBER_BAND), Number);
        assert_eq!(view.band(0), SignedInt);
    }
    for view in [F32, F64] {
        assert!(view.is_view());
        assert_eq!(view.storage(), Float);
        assert_eq!(view.band(NUMBER_BAND), Number);
        assert_eq!(view.band(0), Float);
    }
    for kind in [UnsignedInt, SignedInt, Float, Number] {
        assert!(!kind.is_view());
        assert!(kind.is_number());
        assert_eq!(kind.storage(), kind);
        assert_eq!(kind.band(NUMBER_BAND), Number);
    }
    assert_eq!(
        ValueDataType::String.band(NUMBER_BAND),
        ValueDataType::String
    );
    assert!(!ValueDataType::String.is_number());
}
