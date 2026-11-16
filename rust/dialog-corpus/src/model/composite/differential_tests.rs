//! A composite's bytes checked against a reader and a writer of
//! dag-cbor that share no code with the ones here, over composites
//! drawn from a sequence that is the same every run, and against
//! themselves once a byte is changed.

#![allow(unexpected_cfgs)]

use ipld_core::ipld::Ipld;

use super::*;
use crate::{Entity, Value};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Numbers that are the same every run (splitmix64).
struct Dice(u64);

impl Dice {
    fn roll(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.roll() % bound as u64) as usize
    }

    fn pick<T: Clone>(&mut self, from: &[T]) -> T {
        from[self.below(from.len())].clone()
    }

    fn bytes(&mut self, most: usize) -> Vec<u8> {
        (0..self.below(most + 1))
            .map(|_| self.roll() as u8)
            .collect()
    }

    fn text(&mut self) -> String {
        let pieces = [
            "",
            "a",
            "title",
            "é",
            "日本",
            "\u{0}",
            "?",
            "...",
            "text:",
            " ",
            "\u{10ffff}",
            "key",
        ];
        (0..self.below(4)).map(|_| self.pick(&pieces)).collect()
    }
}

/// A value that is no composite, of any type, among them the numbers
/// either side of where the way a number is written changes.
fn leaf(dice: &mut Dice) -> Value {
    let most = u128::from(u64::MAX);
    let (drawn, other) = (dice.roll(), dice.roll());
    match dice.below(9) {
        0 => Value::String(dice.text()),
        1 => Value::Bytes(dice.bytes(40)),
        2 => Value::Boolean(dice.below(2) == 0),
        3 => Value::UnsignedInt(dice.pick(&[
            0,
            23,
            24,
            255,
            256,
            65_535,
            65_536,
            u128::from(u32::MAX),
            u128::from(u32::MAX) + 1,
            most,
            most + 1,
            u128::MAX,
            u128::from(drawn),
            u128::from(other) << 40,
        ])),
        4 => Value::SignedInt(dice.pick(&[
            -1,
            -24,
            -25,
            -256,
            -257,
            7,
            i128::from(i64::MIN),
            i128::from(i64::MIN) - 1,
            -i128::from(u64::MAX),
            -i128::from(u64::MAX) - 1,
            -i128::from(u64::MAX) - 2,
            i128::MIN,
            i128::MAX,
            -i128::from(drawn),
        ])),
        5 => {
            let drawn = f64::from_bits(drawn);
            Value::Float(dice.pick(&[
                0.5,
                -0.5,
                3.0,
                -0.0,
                1e30,
                -1e30,
                1e300,
                f64::MIN_POSITIVE,
                f64::MAX,
                f64::INFINITY,
                f64::NEG_INFINITY,
                1.0e-320,
                if drawn.is_nan() { 0.25 } else { drawn },
            ]))
        }
        6 => {
            let name: String = (0..1 + dice.below(40))
                .map(|_| dice.pick(&['a', 'z', '0', '-']))
                .collect();
            Value::Entity(format!("id:{name}").parse::<Entity>().expect("an entity"))
        }
        7 => Value::Symbol(
            format!("xyz.{}/open", dice.pick(&["todo", "a", "list-item"]))
                .parse()
                .expect("a symbol"),
        ),
        // A record that is no composite: any bytes.
        _ => Value::Record(dice.bytes(30)),
    }
}

/// A map or a list, of leaves and of composites down to `depth`.
fn composite(dice: &mut Dice, depth: usize) -> Composite {
    let part = |dice: &mut Dice| {
        if depth > 0 && dice.below(3) == 0 {
            Value::from(&composite(dice, depth - 1))
        } else {
            leaf(dice)
        }
    };
    let size = dice.below(6);
    if dice.below(2) == 0 {
        Composite::list((0..size).map(|_| part(dice)).collect::<Vec<_>>())
    } else {
        // A key drawn twice is drawn once.
        let mut entries: Vec<(String, Value)> = Vec::new();
        for _ in 0..size {
            let key = dice.text();
            if entries.iter().all(|(taken, _)| *taken != key) {
                entries.push((key, part(dice)));
            }
        }
        Composite::map(entries).expect("no key twice")
    }
}

/// What a reader built on the libraries makes of a value a composite
/// holds, or `None` where it holds a value past what their link does.
fn expected(value: &Value) -> Option<Ipld> {
    Some(match value {
        Value::String(text) => Ipld::String(text.clone()),
        Value::Bytes(bytes) => Ipld::Bytes(bytes.clone()),
        Value::Boolean(truth) => Ipld::Bool(*truth),
        Value::UnsignedInt(natural) if *natural <= u128::from(u64::MAX) => {
            Ipld::Integer(*natural as i128)
        }
        Value::SignedInt(integer) if *integer >= -i128::from(u64::MAX) - 1 => {
            Ipld::Integer(*integer)
        }
        Value::Float(float) if float.is_finite() => Ipld::Float(*float),
        record @ Value::Record(_) => match record.composite() {
            Ok(Some(Composite::List(items))) => {
                Ipld::List(items.iter().map(expected).collect::<Option<_>>()?)
            }
            Ok(Some(Composite::Map(entries))) => Ipld::Map(
                entries
                    .iter()
                    .map(|(key, value)| Some((key.to_string(), expected(value)?)))
                    .collect::<Option<_>>()?,
            ),
            _ => return linked(record),
        },
        other => return linked(other),
    })
}

/// The link a value with no kind of its own is read as, where the
/// libraries' link holds it.
fn linked(value: &Value) -> Option<Ipld> {
    let mut written = Vec::new();
    write::part(value, &mut written);
    match serde_ipld_dagcbor::from_slice::<Ipld>(&written) {
        Ok(link @ Ipld::Link(_)) => Some(link),
        Ok(other) => panic!("{value:?} is written as {other:?}, which is no link"),
        Err(_) => None,
    }
}

/// Every composite drawn comes back from its bytes as it was, under
/// the name it had. What follows the version byte is read by the
/// libraries as the same parts, and written by them as the same bytes:
/// it is dag-cbor in the one way dag-cbor is written.
#[dialog_common::test]
fn it_writes_what_the_libraries_read_and_write_alike() {
    let mut dice = Dice(1);
    let mut compared = 0;
    for _ in 0..3000 {
        let drawn = composite(&mut dice, 3);
        let bytes = drawn.to_bytes();
        let back = Composite::try_from_bytes(&bytes)
            .unwrap_or_else(|error| panic!("{drawn:?} does not read back: {error}"));
        assert_eq!(back, drawn);
        assert_eq!(back.to_bytes(), bytes);
        assert_eq!(back.reference(), drawn.reference());

        let Some(parts) = expected(&Value::from(&drawn)) else {
            // It holds a value longer than the libraries' link does.
            assert!(serde_ipld_dagcbor::from_slice::<Ipld>(&bytes[1..]).is_err());
            continue;
        };
        let read: Ipld = serde_ipld_dagcbor::from_slice(&bytes[1..])
            .unwrap_or_else(|error| panic!("{drawn:?} is not dag-cbor: {error}"));
        assert_eq!(read, parts, "{drawn:?}");
        let written = serde_ipld_dagcbor::to_vec(&read).expect("is written");
        assert_eq!(written, bytes[1..], "{drawn:?}");
        compared += 1;
    }
    assert!(compared > 2000, "{compared} of 3000 were compared");
}

/// Bytes are a composite in one way. So whatever is read from bytes
/// that were changed is written back as those bytes, and reading them
/// never panics.
#[dialog_common::test]
fn it_reads_only_bytes_it_would_write() {
    let mut dice = Dice(2);
    let mut read = 0;
    for _ in 0..1500 {
        let bytes = composite(&mut dice, 2).to_bytes();
        for _ in 0..24 {
            let mut changed = bytes.clone();
            let at = dice.below(changed.len());
            match dice.below(5) {
                0 => changed[at] = dice.roll() as u8,
                1 => changed[at] ^= 1 << dice.below(8),
                2 => changed.truncate(at),
                3 => changed.insert(at, dice.roll() as u8),
                _ => {
                    changed.remove(at);
                }
            }
            if let Ok(composite) = Composite::try_from_bytes(&changed) {
                assert_eq!(composite.to_bytes(), changed, "{composite:?}");
                read += 1;
            }
        }
    }
    // Some changes leave a composite, another one or the same: a byte
    // of text changed, or a change that changed nothing.
    assert!(read > 0);
}
