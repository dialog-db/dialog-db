//! The identities of values: their [merkle references](merkle_reference).
//!
//! A value's reference is keyed by its type, so the type is part of its
//! identity. Dialog's value types are:
//!
//! - [`NUMBER`] for every number, whose content is its
//!   [`dialog_numeric`] encoding: SQLite4's decimal key encoding, the bytes
//!   the value index will key the number by;
//! - the core text and boolean of [`merkle_reference`];
//! - plain bytes, referenced by their BLAKE3 hash like any blob;
//! - [`ENTITY`], [`SYMBOL`] and [`RECORD`], whose content is their text or
//!   bytes.
//!
//! A number's identity is its value alone, not the band the value index
//! keeps it in today or how it was written: a natural `5`, an integer `5` and
//! a float `5.0` are one number, and a float is the decimal it prints as. The index still keeps naturals, integers and
//! floats apart, so until it moves to one band, two definitions whose only
//! difference is `5` against `5.0` have one identity but read different
//! claims.

use merkle_reference::{Refer, Reference, Type};

use crate::Value;

/// The type of numbers.
pub const NUMBER: Type = Type::from_reference(Reference::from_hex(
    "d57f5f02d3de4a0acf1ee1e35987e05f83bc7a6330e4bc9bae1e188f3550e89e",
));
/// The type of entities: text that parses as a URI.
pub const ENTITY: Type = Type::from_reference(Reference::from_hex(
    "50cceee37496c542516bb9c7cf727f4ed0615f61dc5856aa9b71b52df71a8d2d",
));
/// The type of symbols: `namespace/name` text.
pub const SYMBOL: Type = Type::from_reference(Reference::from_hex(
    "0f69ec853e584e7c9e983e9b44eb02794186187ca9ee7798df795cdff703a36b",
));
/// The type of records: opaque bytes.
pub const RECORD: Type = Type::from_reference(Reference::from_hex(
    "419cfed454524033b3fc368ea2d718640f0ac99760c033353c4120c822c19589",
));

/// A number's identity content: its [`dialog_numeric`] encoding.
fn number(encode: impl FnOnce(&mut Vec<u8>)) -> Reference {
    let mut content = Vec::with_capacity(12);
    encode(&mut content);
    NUMBER.refer(&content)
}

impl Value {
    /// The value's identity: its merkle reference under its value type.
    pub fn reference(&self) -> Reference {
        match self {
            Value::Bytes(bytes) => Reference::of_bytes(bytes),
            Value::Entity(entity) => ENTITY.refer(entity.as_str().as_bytes()),
            Value::Boolean(value) => value.refer(),
            Value::String(text) => text.refer(),
            Value::UnsignedInt(value) => number(|out| dialog_numeric::encode_u128(*value, out)),
            Value::SignedInt(value) => number(|out| dialog_numeric::encode_i128(*value, out)),
            Value::Float(value) => number(|out| dialog_numeric::encode_f64(*value, out)),
            Value::Record(bytes) => RECORD.refer(bytes),
            Value::Symbol(symbol) => SYMBOL.refer(symbol.to_string().as_bytes()),
        }
    }
}

impl Refer for Value {
    fn refer(&self) -> Reference {
        self.reference()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Entity, Relation};

    #[test]
    fn it_names_the_value_types() {
        for (name, ty) in [
            ("number:", NUMBER),
            ("entity:", ENTITY),
            ("symbol:", SYMBOL),
            ("record:", RECORD),
        ] {
            assert_eq!(Type::named(name), ty, "{name}");
        }
    }

    #[test]
    fn it_takes_number_content_from_dialog_numeric() {
        let mut content = Vec::new();
        dialog_numeric::encode_f64(19.99, &mut content);
        assert_eq!(Value::Float(19.99).reference(), NUMBER.refer(&content));
    }

    #[test]
    fn it_identifies_a_number_by_its_value_alone() {
        // A natural, an integer and a float of one number are one value.
        let five = Value::SignedInt(5).reference();
        assert_eq!(Value::UnsignedInt(5).reference(), five);
        assert_eq!(Value::Float(5.0).reference(), five);
        assert_ne!(Value::Float(5.5).reference(), five);
        // A float is the decimal it prints as.
        assert_eq!(
            Value::Float(1e23).reference(),
            Value::UnsignedInt(100_000_000_000_000_000_000_000).reference()
        );
        // Negative zero is zero, and NaN is one value.
        assert_eq!(
            Value::Float(-0.0).reference(),
            Value::SignedInt(0).reference()
        );
        assert_eq!(
            Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)).reference(),
            Value::Float(f64::NAN).reference()
        );
    }

    #[test]
    fn it_tells_values_with_the_same_text_apart_by_type() {
        let entity: Entity = "foo:".parse().unwrap();
        let symbol: Relation = "person/name".parse().unwrap();
        let references = [
            Value::String("foo:".into()).reference(),
            Value::Entity(entity).reference(),
            Value::String("person/name".into()).reference(),
            Value::Symbol(symbol).reference(),
            Value::Bytes(b"person/name".to_vec()).reference(),
            Value::Record(b"person/name".to_vec()).reference(),
        ];
        for (i, a) in references.iter().enumerate() {
            for b in &references[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }
}
