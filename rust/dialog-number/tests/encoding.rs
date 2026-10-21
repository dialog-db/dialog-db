//! The encoding's promises: SQLite4's bytes, one encoding per number, byte
//! order equal to numeric order, and lossless round trips.

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::cmp::Ordering;

use dialog_number::{
    DecodeError, Number, Scalar, decode_f64, decode_i128, encode_f64, encode_i128, encode_u128,
    encoded_len, read_scalar,
};

fn float(value: f64) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_f64(value, &mut bytes);
    bytes
}

fn integer(value: i128) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_i128(value, &mut bytes);
    bytes
}

fn natural(value: u128) -> Vec<u8> {
    let mut bytes = Vec::new();
    encode_u128(value, &mut bytes);
    bytes
}

/// A deterministic generator, so failures reproduce.
fn generator(seed: u64) -> impl FnMut() -> u64 {
    let mut state = seed;
    move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    }
}

#[dialog_common::test]
fn it_writes_sqlite4s_bytes() {
    // From SQLite4's key encoding note: the exponent (in the class byte for
    // 0 to 10, `0x17 + e`) and the significand bytes. The note's rows for
    // 123450, 1234.5 and 12.345 give exponents one too large, so they are
    // left out.
    let cases: &[(f64, &[u8])] = &[
        (1.0, &[0x18, 0x02]),
        (10.0, &[0x18, 0x14]),
        (99.0, &[0x18, 0xc6]),
        (99.01, &[0x18, 0xc7, 0x02]),
        (99.0001, &[0x18, 0xc7, 0x01, 0x02]),
        (100.0, &[0x19, 0x02]),
        (100.01, &[0x19, 0x03, 0x01, 0x02]),
        (100.1, &[0x19, 0x03, 0x01, 0x14]),
        (1234.0, &[0x19, 0x19, 0x44]),
        (9999.0, &[0x19, 0xc7, 0xc6]),
        (9999.000001, &[0x19, 0xc7, 0xc7, 0x01, 0x01, 0x02]),
        (9999.000009, &[0x19, 0xc7, 0xc7, 0x01, 0x01, 0x12]),
        (9999.00001, &[0x19, 0xc7, 0xc7, 0x01, 0x01, 0x14]),
        (9999.00009, &[0x19, 0xc7, 0xc7, 0x01, 0x01, 0xb4]),
        (9999.000099, &[0x19, 0xc7, 0xc7, 0x01, 0x01, 0xc6]),
        (9999.0001, &[0x19, 0xc7, 0xc7, 0x01, 0x02]),
        (9999.001, &[0x19, 0xc7, 0xc7, 0x01, 0x14]),
        (9999.01, &[0x19, 0xc7, 0xc7, 0x02]),
        (9999.1, &[0x19, 0xc7, 0xc7, 0x14]),
        (10000.0, &[0x1a, 0x02]),
        (10001.0, &[0x1a, 0x03, 0x01, 0x02]),
        (12345.0, &[0x1a, 0x03, 0x2f, 0x5a]),
        (0.123, &[0x17, 0x19, 0x3c]),
        (0.0123, &[0x17, 0x03, 0x2e]),
        // Exponent -1: class 0x16, then the varint of 1 inverted.
        (0.00123, &[0x16, 0xfe, 0x19, 0x3c]),
    ];
    for (value, bytes) in cases {
        assert_eq!(float(*value), *bytes, "{value}");
    }
    assert_eq!(float(0.0), [0x15]);
    assert_eq!(float(f64::NAN), [0x06]);
    assert_eq!(float(f64::NEG_INFINITY), [0x07]);
    assert_eq!(float(f64::INFINITY), [0x23]);
    // A negative number inverts every byte after its class.
    assert_eq!(float(-1.0), [0x12, !0x02]);
    assert_eq!(float(-0.00123), [0x14, 0x01, !0x19, !0x3c]);
}

#[dialog_common::test]
fn it_encodes_a_number_once_whatever_holds_it() {
    assert_eq!(integer(3), float(3.0));
    assert_eq!(natural(3), float(3.0));
    assert_eq!(integer(-7), float(-7.0));
    assert_eq!(integer(0), float(-0.0));
    assert_eq!(
        float(f64::from_bits(0x7ff8_0000_0000_0001)),
        float(f64::NAN)
    );
    let mut next = generator(0x9E37_79B9_7F4A_7C15);
    for _ in 0..10_000 {
        let value = (next() >> 12) as i64 - (1 << 51);
        assert_eq!(integer(i128::from(value)), float(value as f64), "{value}");
    }
}

#[dialog_common::test]
fn it_takes_a_float_as_the_decimal_it_prints_as() {
    // The float 1e23 is exactly 99999999999999991611392; as a number it is
    // 10^23, above an integer that lies between the two.
    let ten_to_23 = 100_000_000_000_000_000_000_000u128;
    assert_eq!(float(1e23), natural(ten_to_23));
    assert!(natural(99_999_999_999_999_995_000_000) < float(1e23));
    // 0.1 is the decimal 0.1.
    assert_eq!(Number::from(0.1).to_string(), "0.10e0");
}

#[dialog_common::test]
fn it_round_trips_every_float() {
    let mut next = generator(0x2545_F491_4F6C_DD1D);
    let mut checked = 0;
    while checked < 200_000 {
        let value = f64::from_bits(next());
        if !value.is_finite() {
            continue;
        }
        let bytes = float(value);
        let number = Number::decode(&bytes).expect("decodes");
        assert_eq!(number, Number::from(value));
        assert_eq!(number.to_bytes(), bytes);
        let back = number.to_f64();
        assert!(back == value, "{value:e} came back as {back:e}");
        assert!(decode_f64(&bytes).unwrap() == value, "{value:e}");
        checked += 1;
    }
    for value in [
        f64::MIN_POSITIVE,
        5e-324,
        f64::MAX,
        f64::MIN,
        0.1,
        1e23,
        2f64.powi(100),
    ] {
        assert_eq!(Number::decode(&float(value)).unwrap().to_f64(), value);
    }
}

/// `encoded_len` finds where a number ends without decoding it, so a key
/// parser can split a key: it must agree with the decoder on every number,
/// with whatever follows the number in the key.
#[dialog_common::test]
fn it_finds_where_a_number_ends() {
    let mut next = generator(0x9E37_79B9_7F4A_7C15);
    let mut values: Vec<Vec<u8>> = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 0.0, -0.0]
        .into_iter()
        .map(float)
        .collect();
    for _ in 0..100_000 {
        let bits = next();
        values.push(float(f64::from_bits(bits)));
        values.push(integer(bits as i64 as i128));
        values.push(integer((i128::from(bits) << 64) | i128::from(next())));
        values.push(natural(u128::from(bits) << 64 | u128::from(next())));
        values.push(float((bits % 100_000) as f64 / 100.0));
    }
    for bytes in values {
        let (_, read) = Number::read(&bytes).expect("decodes");
        assert_eq!(read, bytes.len());
        for tail in [&[][..], &[0x00], &[0xFF, 0x01], &[0x2A; 3]] {
            let mut key = bytes.clone();
            key.extend_from_slice(tail);
            assert_eq!(
                encoded_len(&key),
                Some(read),
                "{bytes:02x?} then {tail:02x?}"
            );
        }
    }
    assert_eq!(encoded_len(&[]), None);
    assert_eq!(encoded_len(&[0x00]), None, "not a class byte");
    assert_eq!(encoded_len(&[0x18, 0x03]), None, "no last digit");
}

#[dialog_common::test]
fn it_round_trips_every_integer() {
    let mut next = generator(0xD1B5_4A32_D192_ED03);
    for value in [0, 1, -1, 99, 100, -100, i128::MIN, i128::MAX] {
        assert_eq!(
            Number::decode(&integer(value)).unwrap().to_i128(),
            Some(value)
        );
    }
    assert_eq!(
        Number::decode(&natural(u128::MAX)).unwrap().to_u128(),
        Some(u128::MAX)
    );
    for _ in 0..100_000 {
        let value = (i128::from(next()) << 64 | i128::from(next())) >> (next() % 127);
        let number = Number::decode(&integer(value)).unwrap();
        assert!(number.is_integral());
        assert_eq!(number.to_i128(), Some(value));
        assert_eq!(decode_i128(&integer(value)), Ok(Some(value)));
    }
}

/// Orders an integer against a float exactly.
fn against(integer: i128, float: f64) -> Ordering {
    let truncated = float.trunc();
    integer
        .cmp(&(truncated as i128))
        .then_with(|| 0f64.partial_cmp(&(float - truncated)).unwrap())
}

#[derive(Clone, Copy, Debug)]
enum Held {
    Integer(i128),
    Float(f64),
}

impl Held {
    fn bytes(self) -> Vec<u8> {
        match self {
            Held::Integer(value) => integer(value),
            Held::Float(value) => float(value),
        }
    }
}

/// Exact order. A float below 2^52 in magnitude prints as a decimal on the
/// same side of every integer as the float itself, so comparing it exactly
/// in binary is comparing the numbers.
fn exact(a: Held, b: Held) -> Ordering {
    match (a, b) {
        (Held::Integer(x), Held::Integer(y)) => x.cmp(&y),
        (Held::Float(x), Held::Float(y)) => x.partial_cmp(&y).unwrap(),
        (Held::Integer(x), Held::Float(y)) => against(x, y),
        (Held::Float(x), Held::Integer(y)) => against(y, x).reverse(),
    }
}

#[dialog_common::test]
fn it_sorts_numbers_by_value() {
    let mut next = generator(0x94D0_49BB_1331_11EB);
    let mut held = Vec::new();
    while held.len() < 20_000 {
        let bits = next();
        held.push(match bits % 5 {
            0 => {
                let value = f64::from_bits(next());
                if !value.is_finite() || value.abs() >= 2f64.powi(52) {
                    continue;
                }
                Held::Float(value)
            }
            1 => Held::Float(((next() >> 12) as i64 - (1 << 51)) as f64 / 1024.0),
            2 => Held::Float(((next() >> 12) as i64 - (1 << 51)) as f64),
            3 => Held::Integer(i128::from(next() as i64) >> (bits % 63)),
            _ => Held::Integer((i128::from(next()) << 64 | i128::from(next())) >> (bits % 120)),
        });
    }
    held.sort_by(|a, b| exact(*a, *b));
    for pair in held.windows(2) {
        let (a, b) = (pair[0].bytes(), pair[1].bytes());
        match exact(pair[0], pair[1]) {
            Ordering::Equal => assert_eq!(a, b, "{:?} = {:?}", pair[0], pair[1]),
            _ => assert!(a < b, "{:?} {a:02x?} < {:?} {b:02x?}", pair[0], pair[1]),
        }
    }
}

#[dialog_common::test]
fn it_sorts_every_float_by_value() {
    let mut next = generator(0xBF58_476D_1CE4_E5B9);
    let mut floats: Vec<f64> = Vec::new();
    while floats.len() < 50_000 {
        let value = f64::from_bits(next());
        if value.is_finite() {
            floats.push(value);
        }
    }
    floats.extend([f64::MIN, f64::MAX, 5e-324, -5e-324, 1e23, 1e-300]);
    floats.sort_by(|a, b| a.partial_cmp(b).unwrap());
    floats.dedup();
    for pair in floats.windows(2) {
        assert!(
            float(pair[0]) < float(pair[1]),
            "{:e} < {:e}",
            pair[0],
            pair[1]
        );
    }
    assert!(float(f64::NAN) < float(f64::NEG_INFINITY));
    assert!(float(f64::NEG_INFINITY) < float(f64::MIN));
    assert!(float(f64::MAX) < float(f64::INFINITY));
}

#[dialog_common::test]
fn it_refuses_bytes_that_are_not_canonical() {
    // 1 with a trailing zero digit: 0.01 00 × 100^1.
    assert_eq!(
        Number::decode(&[0x18, 0x03, 0x00]),
        Err(DecodeError::NotCanonical)
    );
    // A leading zero digit.
    assert_eq!(
        Number::decode(&[0x18, 0x01, 0x02]),
        Err(DecodeError::NotCanonical)
    );
    // A digit above 99.
    assert_eq!(
        Number::decode(&[0x18, 0xd0]),
        Err(DecodeError::NotCanonical)
    );
    // Exponent 5 written in the large class.
    assert_eq!(
        Number::decode(&[0x22, 0x05, 0x02]),
        Err(DecodeError::NotCanonical)
    );
    // Exponent 0 written in the small class.
    assert_eq!(
        Number::decode(&[0x16, 0xff, 0x02]),
        Err(DecodeError::NotCanonical)
    );
    assert_eq!(Number::decode(&[0x18, 0x03]), Err(DecodeError::Truncated));
    assert_eq!(
        Number::decode(&[0x18, 0x02, 0x00]),
        Err(DecodeError::Trailing)
    );
    assert_eq!(Number::decode(&[0x40]), Err(DecodeError::Class));
    assert_eq!(
        decode_i128(&[0x18, 0x03, 0x00]),
        Err(DecodeError::NotCanonical)
    );
    // Not whole, or too large: no integer.
    assert_eq!(decode_i128(&float(0.5)), Ok(None));
    assert_eq!(decode_i128(&float(1e300)), Ok(None));
}

/// Short decimals (prices, measurements) decode by multiplying or dividing
/// an exact significand by an exact power of ten; that must round exactly
/// as parsing the decimal does.
#[dialog_common::test]
fn it_decodes_short_decimals_as_parsing_them_does() {
    let mut next = generator(0xD1B5_4A32_D192_ED03);
    for _ in 0..200_000 {
        let significand = next() % 10_000_000_000_000_000;
        let places = (next() % 45) as i32 - 22;
        let text = format!("{significand}e{places}");
        let value: f64 = text.parse().expect("parses");
        for value in [value, -value] {
            assert!(
                decode_f64(&float(value)).unwrap() == value,
                "{text} came back as {:e}",
                decode_f64(&float(value)).unwrap()
            );
        }
    }
    for cents in 1..100_000u64 {
        let price = cents as f64 / 100.0;
        assert!(decode_f64(&float(price)).unwrap() == price, "{price}");
    }
}

/// `read_scalar` reads a whole number an `i128` holds as an integer, a
/// larger one as a natural, and anything else as the float `decode_f64`
/// gives, taking exactly the number's bytes.
#[dialog_common::test]
fn it_reads_a_scalar_as_the_decoders_do() {
    let mut next = generator(0x94D0_49BB_1331_11EB);
    let mut cases: Vec<Vec<u8>> = vec![float(f64::NAN), float(f64::INFINITY), float(0.0)];
    for _ in 0..100_000 {
        let bits = next();
        cases.push(float(f64::from_bits(bits)));
        cases.push(float((bits % 1_000_000) as f64 / 100.0));
        cases.push(integer(bits as i64 as i128));
        cases.push(integer(i128::from(bits) << 64 | i128::from(next())));
        cases.push(natural(u128::MAX - u128::from(bits)));
    }
    for bytes in cases {
        let mut key = bytes.clone();
        key.extend_from_slice(&[0x00, 0x2A]);
        let (scalar, read) = read_scalar(&key).expect("reads");
        assert_eq!(read, bytes.len());
        match scalar {
            Scalar::Integer(integer) => assert_eq!(decode_i128(&bytes).unwrap(), Some(integer)),
            Scalar::Natural(natural) => {
                assert_eq!(decode_i128(&bytes).unwrap(), None);
                assert_eq!(Number::decode(&bytes).unwrap().to_u128(), Some(natural));
            }
            Scalar::Float(value) => {
                assert_eq!(decode_i128(&bytes).unwrap(), None);
                let expected = decode_f64(&bytes).unwrap();
                assert!(
                    value == expected || (value.is_nan() && expected.is_nan()),
                    "{value:e} against {expected:e}"
                );
            }
        }
    }
}
