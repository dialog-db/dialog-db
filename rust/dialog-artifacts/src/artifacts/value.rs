//! Value types for semantic triple objects.
//!
//! This module defines the [`Value`] enum which represents all possible value
//! types that can be stored as the object part of semantic triples, along with
//! type information and serialization utilities.

use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem,
    str::FromStr,
};

use crate::{Cause, DialogArtifactsError, Entity, Relation, TypeError, make_reference};
use base58::{FromBase58, ToBase58};
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// All value type representations that may be stored by [`Artifacts`]
///
/// # Serialization
///
/// A binary format (one whose serializer is not human readable, such as
/// dag-cbor) writes a value as a one-entry map from the entity that
/// names its type to its payload: `{"text:": "foo:"}`,
/// `{"entity:": "foo:"}`, `{"natural:": 5}`. Those are the bytes a rule
/// or an attribute hashes into its identity, so two values of different
/// types never share an identity, and a value decodes as the type it was
/// written as.
///
/// A human-readable format (JSON) writes the bare payload, `"foo:"` or
/// `5`, wherever it reads back as the same value, and the tagged form
/// where it would not: text that parses as a URI, a symbol, a
/// non-negative signed integer and a record. It reads a bare payload as
/// the first type it fits: a string that parses as a URI is an entity,
/// other text is text, and an integer is natural unless it is negative.
/// Every format reads the tagged form, and the bare payloads earlier
/// releases wrote in binary.
#[derive(Debug, Clone, PartialOrd)]
pub enum Value {
    /// A byte buffer
    Bytes(Vec<u8>),
    /// An [`Entity`]
    Entity(Entity),
    /// A boolean
    Boolean(bool),
    /// A UTF-8 string
    String(String),
    /// A 128-bit unsigned integer
    // TODO: Use a different encoding?
    UnsignedInt(u128),
    /// A 128-bit signed integer
    SignedInt(i128),
    /// A floating point number
    Float(f64),
    /// TBD structured data (flatbuffers?)
    Record(Vec<u8>),
    /// A symbol type, used to distinguish attributes from other strings
    Symbol(Relation),
}

/// A value as its bare payload, untagged: how a human-readable format
/// writes a value, and how every format wrote one before values were
/// tagged. Variants that share a payload shape share an encoding, so
/// reading picks the first variant the payload fits.
#[derive(Serialize)]
#[serde(untagged)]
enum Bare<'a> {
    Bytes(&'a Vec<u8>),
    Entity(&'a Entity),
    Boolean(bool),
    String(&'a String),
    UnsignedInt(u128),
    SignedInt(i128),
    Float(f64),
    Record(&'a Vec<u8>),
    Symbol(&'a Relation),
}

impl Value {
    fn bare(&self) -> Bare<'_> {
        match self {
            Value::Bytes(bytes) => Bare::Bytes(bytes),
            Value::Entity(entity) => Bare::Entity(entity),
            Value::Boolean(boolean) => Bare::Boolean(*boolean),
            Value::String(string) => Bare::String(string),
            Value::UnsignedInt(natural) => Bare::UnsignedInt(*natural),
            Value::SignedInt(integer) => Bare::SignedInt(*integer),
            Value::Float(float) => Bare::Float(*float),
            Value::Record(record) => Bare::Record(record),
            Value::Symbol(relation) => Bare::Symbol(relation),
        }
    }
}

impl Value {
    /// This number read through the number type `kind`, or `None` when the
    /// view does not see it. Number types are views over one domain of
    /// numbers: a float view reads every number as a float, an integer view
    /// reads every number with a fraction as its floor, and a natural view
    /// reads the numbers that are not negative the same way. A view coerces
    /// only where it loses precision and filters what is out of its range
    /// (`notes/identity-storage-notation.md`, Views read and write). A value
    /// that is not a number, or a `kind` that is not a number type, is
    /// returned as it is.
    pub fn viewed_as(self, kind: ValueDataType) -> Option<Value> {
        fn floor(float: f64) -> Option<f64> {
            float.is_finite().then(|| float.floor())
        }
        if kind.is_view() {
            return self.viewed_through(kind);
        }
        Some(match (self, kind) {
            (Value::UnsignedInt(natural), ValueDataType::SignedInt) => {
                Value::SignedInt(i128::try_from(natural).ok()?)
            }
            (Value::UnsignedInt(natural), ValueDataType::Float) => Value::Float(natural as f64),
            (Value::SignedInt(integer), ValueDataType::UnsignedInt) => {
                Value::UnsignedInt(u128::try_from(integer).ok()?)
            }
            (Value::SignedInt(integer), ValueDataType::Float) => Value::Float(integer as f64),
            (Value::Float(float), ValueDataType::SignedInt) => {
                let whole = floor(float)?;
                (whole >= i128::MIN as f64 && whole < i128::MAX as f64)
                    .then_some(Value::SignedInt(whole as i128))?
            }
            (Value::Float(float), ValueDataType::UnsignedInt) => {
                let whole = floor(float)?;
                (whole >= 0.0 && whole < u128::MAX as f64)
                    .then_some(Value::UnsignedInt(whole as u128))?
            }
            (value, _) => value,
        })
    }

    /// This number read through the Rust numeric view `view`: read through
    /// its storage type, then filtered to its range (`u8:` does not see
    /// 256) or, for `f32:`, rounded to the nearest single-precision float
    /// and filtered where a finite number is past its range.
    fn viewed_through(self, view: ValueDataType) -> Option<Value> {
        if !self.data_type().is_number() {
            return Some(self);
        }
        let viewed = self.viewed_as(view.storage())?;
        let fits = match (&viewed, view) {
            (Value::UnsignedInt(natural), _) => {
                let bits = match view {
                    ValueDataType::U8 => 8,
                    ValueDataType::U16 => 16,
                    ValueDataType::U32 => 32,
                    ValueDataType::U64 => 64,
                    _ => 128,
                };
                bits == 128 || *natural < 1u128 << bits
            }
            (Value::SignedInt(integer), _) => {
                let bits = match view {
                    ValueDataType::I8 => 8,
                    ValueDataType::I16 => 16,
                    ValueDataType::I32 => 32,
                    ValueDataType::I64 => 64,
                    _ => 128,
                };
                bits == 128 || (-(1i128 << (bits - 1))..1i128 << (bits - 1)).contains(integer)
            }
            (Value::Float(float), ValueDataType::F32) => {
                !float.is_finite() || float.abs() <= f64::from(f32::MAX)
            }
            _ => true,
        };
        if !fits {
            return None;
        }
        Some(match (viewed, view) {
            // NaN is one value: narrowing it may keep or drop its payload
            // depending on the target, so it reads as the canonical NaN on
            // every target.
            (Value::Float(float), ValueDataType::F32) if float.is_nan() => Value::Float(f64::NAN),
            (Value::Float(float), ValueDataType::F32) => Value::Float(f64::from(float as f32)),
            (viewed, _) => viewed,
        })
    }

    /// Whether this and `other` are the same number, whatever types hold
    /// them: the natural 5, the integer 5 and the float 5.0 are one. Values
    /// that are not both numbers are compared as they are.
    pub fn same_number(&self, other: &Value) -> bool {
        fn exact(value: &Value) -> Option<(bool, u128, f64)> {
            match value {
                Value::UnsignedInt(natural) => Some((false, *natural, *natural as f64)),
                Value::SignedInt(integer) => {
                    Some((*integer < 0, integer.unsigned_abs(), *integer as f64))
                }
                _ => None,
            }
        }
        match (self, other) {
            (Value::Float(a), Value::Float(b)) => a == b || (a.is_nan() && b.is_nan()),
            (Value::Float(float), whole) | (whole, Value::Float(float)) => match exact(whole) {
                Some((_, _, approximate)) => {
                    // A float equals a whole number only if it is that
                    // number exactly, not merely the nearest float to it.
                    *float == approximate
                        && whole.clone().viewed_as(ValueDataType::Float).is_some()
                        && Value::Float(*float)
                            .viewed_as(whole.data_type())
                            .is_some_and(|back| back == *whole)
                }
                None => false,
            },
            (a, b) => match (exact(a), exact(b)) {
                (Some((sa, ma, _)), Some((sb, mb, _))) => sa == sb && ma == mb,
                _ => a == b,
            },
        }
    }

    /// This value as `kind`, where `kind` reads the same spelling or
    /// number: text as an entity or a symbol when it parses as one, an
    /// entity as text or a symbol, a natural number as a signed integer
    /// and back when it fits, bytes as a record and back. Any other value
    /// is returned as it is.
    ///
    /// A bare constant decodes as the first type its payload fits, so a
    /// constant written bare (by JSON, or by a release before constants
    /// were tagged) may name the right value under the wrong type; a slot
    /// that declares its type conforms it.
    pub fn conform(self, kind: ValueDataType) -> Value {
        match (self, kind.storage()) {
            (Value::Entity(entity), ValueDataType::String) => Value::String(entity.to_string()),
            (Value::String(text), ValueDataType::Entity) => match Entity::from_str(&text) {
                Ok(entity) => Value::Entity(entity),
                Err(_) => Value::String(text),
            },
            (Value::String(text), ValueDataType::Symbol) => match Relation::from_str(&text) {
                Ok(relation) => Value::Symbol(relation),
                Err(_) => Value::String(text),
            },
            (Value::Entity(entity), ValueDataType::Symbol) => {
                match Relation::from_str(&entity.to_string()) {
                    Ok(relation) => Value::Symbol(relation),
                    Err(_) => Value::Entity(entity),
                }
            }
            (Value::Symbol(relation), ValueDataType::String) => Value::String(relation.to_string()),
            (Value::UnsignedInt(natural), ValueDataType::SignedInt) => {
                match i128::try_from(natural) {
                    Ok(integer) => Value::SignedInt(integer),
                    Err(_) => Value::UnsignedInt(natural),
                }
            }
            (Value::SignedInt(integer), ValueDataType::UnsignedInt) => {
                match u128::try_from(integer) {
                    Ok(natural) => Value::UnsignedInt(natural),
                    Err(_) => Value::SignedInt(integer),
                }
            }
            (Value::Bytes(bytes), ValueDataType::Record) => Value::Record(bytes),
            (Value::Record(record), ValueDataType::Bytes) => Value::Bytes(record),
            (value, _) => value,
        }
    }

    /// This value as its bare payload in every format, untagged. Two
    /// values of different types can share it (the text `foo:` and the
    /// entity `foo:`), so it suits only an encoding that has to keep the
    /// spelling earlier releases wrote, such as an identity minted from
    /// values before they were tagged.
    pub fn untagged(&self) -> impl Serialize + '_ {
        self.bare()
    }

    /// Whether this value's bare payload reads back as this value: the
    /// first type the payload fits is its own. Text that parses as a URI
    /// reads back as an entity, a symbol as text, a non-negative signed
    /// integer as a natural number and a record as bytes.
    fn reads_back_bare(&self) -> bool {
        match self {
            Value::String(text) => Entity::from_str(text).is_err(),
            Value::SignedInt(integer) => *integer < 0,
            Value::Symbol(_) | Value::Record(_) => false,
            Value::Bytes(_)
            | Value::Entity(_)
            | Value::Boolean(_)
            | Value::UnsignedInt(_)
            | Value::Float(_) => true,
        }
    }
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() && self.reads_back_bare() {
            return self.bare().serialize(serializer);
        }
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.data_type().uri(), &self.bare())?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(ValueVisitor)
    }
}

/// An integer payload, read through `deserialize_any`. Reading a `u128`
/// or an `i128` directly asks for `deserialize_u128` or
/// `deserialize_i128`, which the buffer serde reads an untagged or
/// flattened map into does not implement, so a tagged integer inside
/// one would fail to decode.
struct Integral(i128);

impl<'de> Deserialize<'de> for Integral {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = Integral;
            fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
                formatter.write_str("an integer")
            }
            fn visit_u64<E: de::Error>(self, natural: u64) -> Result<Integral, E> {
                Ok(Integral(natural.into()))
            }
            fn visit_i64<E: de::Error>(self, integer: i64) -> Result<Integral, E> {
                Ok(Integral(integer.into()))
            }
            fn visit_u128<E: de::Error>(self, natural: u128) -> Result<Integral, E> {
                i128::try_from(natural)
                    .map(Integral)
                    .map_err(|_| E::custom(format!("{natural} is out of range")))
            }
            fn visit_i128<E: de::Error>(self, integer: i128) -> Result<Integral, E> {
                Ok(Integral(integer))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

/// Reads a value in its tagged form, or as a bare payload, which it
/// reads as the first of [`Bare`]'s variants the payload fits: a
/// sequence of bytes is bytes, a string that parses as a URI is an
/// entity and any other string is text, and an integer is natural
/// unless it is negative.
struct ValueVisitor;

impl<'de> de::Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str("a value, tagged by its type entity or bare")
    }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let Some(name) = map.next_key::<String>()? else {
            return Err(de::Error::custom("an empty map names no type"));
        };
        let kind = ValueDataType::ALL
            .into_iter()
            .find(|kind| kind.uri() == name)
            .ok_or_else(|| de::Error::custom(format!("`{name}` names no type")))?;
        let value =
            match kind {
                ValueDataType::Bytes => Value::Bytes(map.next_value()?),
                ValueDataType::Entity => Value::Entity(map.next_value()?),
                ValueDataType::Boolean => Value::Boolean(map.next_value()?),
                ValueDataType::String => Value::String(map.next_value()?),
                ValueDataType::UnsignedInt => {
                    let Integral(integer) = map.next_value()?;
                    Value::UnsignedInt(u128::try_from(integer).map_err(|_| {
                        de::Error::custom(format!("{integer} is not a natural number"))
                    })?)
                }
                ValueDataType::SignedInt => {
                    let Integral(integer) = map.next_value()?;
                    Value::SignedInt(integer)
                }
                ValueDataType::Float => Value::Float(map.next_value()?),
                ValueDataType::Record => Value::Record(map.next_value()?),
                ValueDataType::Symbol => Value::Symbol(map.next_value()?),
                // Any number (`number:`) or one a Rust numeric view holds
                // (`u8:` .. `f64:`), read as its bare spelling reads and
                // held in the view's storage type.
                number => {
                    let value: Value = map.next_value()?;
                    if !number.holds(&value) {
                        return Err(de::Error::custom(format!(
                            "{value:?} is not a {}",
                            number.uri()
                        )));
                    }
                    value.clone().viewed_as(number).unwrap_or(value)
                }
            };
        if map.next_key::<de::IgnoredAny>()?.is_some() {
            return Err(de::Error::custom("a tagged value names one type"));
        }
        Ok(value)
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<Value, E> {
        self.visit_string(text.to_owned())
    }

    fn visit_string<E: de::Error>(self, text: String) -> Result<Value, E> {
        Ok(match Entity::from_str(&text) {
            Ok(entity) => Value::Entity(entity),
            Err(_) => Value::String(text),
        })
    }

    fn visit_bool<E: de::Error>(self, boolean: bool) -> Result<Value, E> {
        Ok(Value::Boolean(boolean))
    }

    fn visit_u64<E: de::Error>(self, natural: u64) -> Result<Value, E> {
        Ok(Value::UnsignedInt(natural.into()))
    }

    fn visit_u128<E: de::Error>(self, natural: u128) -> Result<Value, E> {
        Ok(Value::UnsignedInt(natural))
    }

    fn visit_i64<E: de::Error>(self, integer: i64) -> Result<Value, E> {
        self.visit_i128(integer.into())
    }

    fn visit_i128<E: de::Error>(self, integer: i128) -> Result<Value, E> {
        Ok(match u128::try_from(integer) {
            Ok(natural) => Value::UnsignedInt(natural),
            Err(_) => Value::SignedInt(integer),
        })
    }

    fn visit_f64<E: de::Error>(self, float: f64) -> Result<Value, E> {
        Ok(Value::Float(float))
    }

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Value, E> {
        Ok(Value::Bytes(bytes.to_vec()))
    }

    fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Value, E> {
        Ok(Value::Bytes(bytes))
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(byte) = seq.next_element::<u8>()? {
            bytes.push(byte);
        }
        Ok(Value::Bytes(bytes))
    }
}

impl Value {
    /// Get the [`ValueDataType`] that corresponds to this variant of [`Value`]
    pub fn data_type(&self) -> ValueDataType {
        match self {
            Value::Bytes(_) => ValueDataType::Bytes,
            Value::Entity(_) => ValueDataType::Entity,
            Value::Boolean(_) => ValueDataType::Boolean,
            Value::String(_) => ValueDataType::String,
            Value::UnsignedInt(_) => ValueDataType::UnsignedInt,
            Value::SignedInt(_) => ValueDataType::SignedInt,
            Value::Float(_) => ValueDataType::Float,
            Value::Record(_) => ValueDataType::Record,
            Value::Symbol(_) => ValueDataType::Symbol,
        }
    }

    /// Convert this [`Value`] to its byte representation
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Bytes(bytes) => bytes.to_owned(),
            Value::Entity(entity) => entity.as_str().as_bytes().to_owned(),
            Value::Boolean(value) => vec![u8::from(*value)],
            Value::String(string) => string.as_bytes().to_vec(),
            Value::UnsignedInt(value) => value.to_le_bytes().to_vec(),
            Value::SignedInt(value) => value.to_le_bytes().to_vec(),
            Value::Float(value) => value.to_le_bytes().to_vec(),
            Value::Record(value) => value.to_owned(),
            // TODO: Change this to bytes of string representation
            Value::Symbol(value) => value.key_bytes().to_vec(),
        }
    }

    /// Serialize this [`Value`] to a UTF-8 string
    pub fn to_utf8(&self) -> String {
        match self {
            Value::Bytes(bytes) => format!("bytes:{}", bytes.to_base58()),
            Value::Entity(raw) => format!("entity:{}", raw),
            Value::Boolean(value) => format!("boolean:{}", value),
            Value::String(string) => format!("string:{}", string),
            Value::UnsignedInt(number) => format!("uint:{}", number),
            Value::SignedInt(number) => format!("sint:{}", number),
            Value::Float(number) => format!("float:{}", number),
            Value::Record(record) => format!("record:{}", record.to_base58()),
            Value::Symbol(attribute) => format!("attribute:{}", attribute),
        }
    }

    /// Produce a hash reference to this [`Value`]
    pub fn to_reference(&self) -> Blake3Hash {
        make_reference(self.to_bytes())
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Float(f) => f.to_le_bytes().hash(state),
            Value::Bytes(b) => b.hash(state),
            Value::Entity(e) => e.hash(state),
            Value::Boolean(b) => b.hash(state),
            Value::String(s) => s.hash(state),
            Value::UnsignedInt(u) => u.hash(state),
            Value::SignedInt(i) => i.hash(state),
            Value::Record(r) => r.hash(state),
            Value::Symbol(s) => s.hash(state),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            (Value::Entity(a), Value::Entity(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::UnsignedInt(a), Value::UnsignedInt(b)) => a == b,
            (Value::SignedInt(a), Value::SignedInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_le_bytes() == b.to_le_bytes(),
            (Value::Record(a), Value::Record(b)) => a == b,
            (Value::Symbol(a), Value::Symbol(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

pub(crate) fn to_utf8<S>(value: &Value, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let serialized = value.to_utf8();
    serialized.serialize(serializer)
}

pub(crate) fn from_utf8<'de, D>(deserializer: D) -> Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(|error| de::Error::custom(format!("{error}")))
}

impl FromStr for Value {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let Some((variant, value)) = s.split_once(':') else {
            return Err(DialogArtifactsError::InvalidValue(format!(
                "Unsupported variant or invalid format: \"{}\"",
                s
            )));
        };

        fn to_dialog_error_debug<E>(error: E) -> DialogArtifactsError
        where
            E: Debug,
        {
            DialogArtifactsError::InvalidValue(format!("{:?}", error))
        }

        fn to_dialog_error<E>(error: E) -> DialogArtifactsError
        where
            E: Display,
        {
            DialogArtifactsError::InvalidValue(format!("{}", error))
        }

        Ok(match variant {
            "bytes" => Value::Bytes(value.from_base58().map_err(to_dialog_error_debug)?),
            "entity" => Value::Entity(Entity::from_str(value)?),
            "boolean" => Value::Boolean(bool::from_str(value).map_err(to_dialog_error)?),
            "string" => Value::String(value.to_owned()),
            "uint" => Value::UnsignedInt(value.parse().map_err(to_dialog_error)?),
            "sint" => Value::SignedInt(value.parse().map_err(to_dialog_error)?),
            "float" => Value::Float(value.parse().map_err(to_dialog_error)?),
            "record" => Value::Record(value.from_base58().map_err(to_dialog_error_debug)?),
            "attribute" => Value::Symbol(Relation::from_str(value)?),
            _ => {
                return Err(DialogArtifactsError::InvalidValue(
                    "Value part of serialized string is empty".into(),
                ));
            }
        })
    }
}

impl TryFrom<(ValueDataType, Vec<u8>)> for Value {
    type Error = DialogArtifactsError;

    fn try_from((value_data_type, value): (ValueDataType, Vec<u8>)) -> Result<Self, Self::Error> {
        Ok(match value_data_type {
            // A Rust numeric view's value is held as its storage type.
            view @ (ValueDataType::U8
            | ValueDataType::U16
            | ValueDataType::U32
            | ValueDataType::U64
            | ValueDataType::U128
            | ValueDataType::I8
            | ValueDataType::I16
            | ValueDataType::I32
            | ValueDataType::I64
            | ValueDataType::I128
            | ValueDataType::F32
            | ValueDataType::F64) => return Value::try_from((view.storage(), value)),
            // A key's band, whose bytes are dialog-number's, never a value's
            // raw bytes.
            ValueDataType::Number => {
                return Err(DialogArtifactsError::InvalidValue(
                    "`number:` is an index band, not a value's type".to_string(),
                ));
            }
            ValueDataType::Bytes => Value::Bytes(value),
            ValueDataType::Entity => Value::Entity(Entity::try_from(value)?),
            // TODO: How strictly validated must a bool representation be?
            ValueDataType::Boolean => match value.first() {
                Some(byte) if value.len() == 1 => Value::Boolean(*byte != 0),
                _ => {
                    return Err(DialogArtifactsError::InvalidValue(format!(
                        "Wrong byte length for boolean (expected 1, got {})",
                        value.len()
                    )));
                }
            },
            ValueDataType::String => match String::from_utf8(value) {
                Ok(value) => Value::String(value),
                Err(error) => {
                    return Err(DialogArtifactsError::InvalidValue(format!(
                        "Not a valid UTF-8 string: {error}"
                    )));
                }
            },
            // TODO: Use a different encoding strategy for numerics? Varint for
            // integer? What about floats?
            ValueDataType::UnsignedInt => Value::UnsignedInt(u128::from_le_bytes(
                value.try_into().map_err(|value: Vec<u8>| {
                    DialogArtifactsError::InvalidValue(format!(
                        "Wrong number of bytes for u128 (expected 16, got {})",
                        value.len()
                    ))
                })?,
            )),
            ValueDataType::SignedInt => Value::SignedInt(i128::from_le_bytes(
                value.try_into().map_err(|value: Vec<u8>| {
                    DialogArtifactsError::InvalidValue(format!(
                        "Wrong number of bytes for i128 (expected 16, got {})",
                        value.len()
                    ))
                })?,
            )),
            ValueDataType::Float => Value::Float(f64::from_le_bytes(value.try_into().map_err(
                |value: Vec<u8>| {
                    DialogArtifactsError::InvalidValue(format!(
                        "Wrong number of bytes for f64 (expected 8, got {})",
                        value.len()
                    ))
                },
            )?)),
            // A record is opaque bytes at this layer; interpretation is the
            // reader's concern (see e.g. `history::RevisionRecord`). Its byte
            // representation is its raw bytes (`to_bytes` returns them
            // verbatim), so reconstruction is the identity. This is also the
            // spilled-value read-back path: a record above the inline
            // threshold must round-trip, not panic.
            ValueDataType::Record => Value::Record(value),
            ValueDataType::Symbol => match String::from_utf8(value) {
                Ok(value) => Value::Symbol(Relation::try_from(
                    value.split('\u{0000}').take(1).collect::<String>(),
                )?),
                Err(error) => {
                    return Err(DialogArtifactsError::InvalidValue(format!(
                        "Not a valid UTF-8 string: {error}"
                    )));
                }
            },
        })
    }
}

impl TryFrom<Value> for Relation {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Symbol(attribute) => Ok(attribute),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::Symbol,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for Entity {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Entity(entity) => Ok(entity),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::Entity,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(string) => Ok(string),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::String,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Boolean(boolean) => Ok(boolean),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::Boolean,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for usize {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UnsignedInt(uint) => usize::try_from(uint).map_err(|_| {
                TypeError::TypeMismatch(ValueDataType::UnsignedInt, value.data_type())
            }),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::UnsignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for u128 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UnsignedInt(uint) => Ok(uint),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::UnsignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for u64 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UnsignedInt(uint) => u64::try_from(uint).map_err(|_| {
                TypeError::TypeMismatch(ValueDataType::UnsignedInt, value.data_type())
            }),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::UnsignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for u32 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UnsignedInt(uint) => u32::try_from(uint).map_err(|_| {
                TypeError::TypeMismatch(ValueDataType::UnsignedInt, value.data_type())
            }),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::UnsignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for u16 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UnsignedInt(uint) => u16::try_from(uint).map_err(|_| {
                TypeError::TypeMismatch(ValueDataType::UnsignedInt, value.data_type())
            }),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::UnsignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for u8 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::UnsignedInt(uint) => u8::try_from(uint).map_err(|_| {
                TypeError::TypeMismatch(ValueDataType::UnsignedInt, value.data_type())
            }),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::UnsignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for isize {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SignedInt(uint) => isize::try_from(uint)
                .map_err(|_| TypeError::TypeMismatch(ValueDataType::SignedInt, value.data_type())),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::UnsignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for i128 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SignedInt(sint) => Ok(sint),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::SignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SignedInt(sint) => i64::try_from(sint)
                .map_err(|_| TypeError::TypeMismatch(ValueDataType::SignedInt, value.data_type())),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::SignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for i32 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SignedInt(sint) => i32::try_from(sint)
                .map_err(|_| TypeError::TypeMismatch(ValueDataType::SignedInt, value.data_type())),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::SignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for i16 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SignedInt(sint) => i16::try_from(sint)
                .map_err(|_| TypeError::TypeMismatch(ValueDataType::SignedInt, value.data_type())),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::SignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for i8 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::SignedInt(sint) => i8::try_from(sint)
                .map_err(|_| TypeError::TypeMismatch(ValueDataType::SignedInt, value.data_type())),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::SignedInt,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(float) => Ok(float),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::Float,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for f32 {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Float(float) => Ok(float as f32),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::Float,
                value.data_type(),
            )),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bytes(bytes) => Ok(bytes),
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::Bytes,
                value.data_type(),
            )),
        }
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl From<Entity> for Value {
    fn from(value: Entity) -> Self {
        Value::Entity(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<u128> for Value {
    fn from(value: u128) -> Self {
        Value::UnsignedInt(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::UnsignedInt(value.into())
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::UnsignedInt(value.into())
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Value::UnsignedInt(value.into())
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Value::UnsignedInt(value.into())
    }
}

impl From<i128> for Value {
    fn from(value: i128) -> Self {
        Value::SignedInt(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::SignedInt(value.into())
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::SignedInt(value.into())
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Value::SignedInt(value.into())
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Value::SignedInt(value.into())
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(value.into())
    }
}

impl From<Relation> for Value {
    fn from(value: Relation) -> Self {
        Value::Symbol(value)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Value::UnsignedInt(value as u128)
    }
}

impl From<Cause> for Value {
    fn from(value: Cause) -> Self {
        Value::Bytes(value.to_vec())
    }
}

impl PartialEq<Value> for Entity {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::Entity(entity) => self == entity,
            _ => false,
        }
    }
}

impl PartialEq<Value> for Relation {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::Symbol(attr) => self == attr,
            _ => false,
        }
    }
}

impl PartialEq<Value> for String {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::String(s) => self == s,
            _ => false,
        }
    }
}

impl PartialEq<Value> for bool {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::Boolean(b) => self == b,
            _ => false,
        }
    }
}

impl PartialEq<Value> for u128 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::UnsignedInt(u) => self == u,
            _ => false,
        }
    }
}

impl PartialEq<Value> for u64 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::UnsignedInt(u) => *self as u128 == *u,
            _ => false,
        }
    }
}

impl PartialEq<Value> for u32 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::UnsignedInt(u) => *self as u128 == *u,
            _ => false,
        }
    }
}

impl PartialEq<Value> for u16 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::UnsignedInt(u) => *self as u128 == *u,
            _ => false,
        }
    }
}

impl PartialEq<Value> for u8 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::UnsignedInt(u) => *self as u128 == *u,
            _ => false,
        }
    }
}

impl PartialEq<Value> for i128 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::SignedInt(i) => self == i,
            _ => false,
        }
    }
}

impl PartialEq<Value> for i64 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::SignedInt(i) => *self as i128 == *i,
            _ => false,
        }
    }
}

impl PartialEq<Value> for i32 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::SignedInt(i) => *self as i128 == *i,
            _ => false,
        }
    }
}

impl PartialEq<Value> for i16 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::SignedInt(i) => *self as i128 == *i,
            _ => false,
        }
    }
}

impl PartialEq<Value> for i8 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::SignedInt(i) => *self as i128 == *i,
            _ => false,
        }
    }
}

impl PartialEq<Value> for f64 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::Float(f) => self == f,
            _ => false,
        }
    }
}

impl PartialEq<Value> for f32 {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::Float(f) => *self as f64 == *f,
            _ => false,
        }
    }
}

impl PartialEq<Value> for Vec<u8> {
    fn eq(&self, other: &Value) -> bool {
        match other {
            Value::Bytes(bytes) => self == bytes,
            _ => false,
        }
    }
}

impl From<&Value> for ValueDataType {
    fn from(value: &Value) -> Self {
        value.data_type()
    }
}

impl From<Value> for ValueDataType {
    fn from(value: Value) -> Self {
        Self::from(&value)
    }
}

/// The manifest `value_encoding` that keys every number in one band.
pub const NUMBER_BAND: u32 = 1;

/// [`ValueDataType`] embodies all types that are able to be represented
/// as a [`Value`].
///
/// A type is named by an entity: `text:`, `integer:`, `natural:`,
/// `float:`, `boolean:`, `bytes:`, `entity:`, `symbol:` and `record:`.
/// That is how it is written wherever it is serialized and what an
/// attribute's identity hashes. The names the release before type
/// entities wrote (`Text`, `SignedInteger`, ...) are still read, so
/// what it stored decodes; see [`ValueDataType::legacy_name`].
#[repr(u8)]
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValueDataType {
    /// A byte buffer: `bytes:`
    #[default]
    Bytes = 0,
    /// An [`Entity`]: `entity:`
    Entity = 1,
    /// A boolean: `boolean:`
    Boolean = 2,
    /// A UTF-8 string: `text:`
    String = 3,
    /// A 128-bit unsigned integer: `natural:`
    UnsignedInt = 4,
    /// A 128-bit signed integer: `integer:`
    SignedInt = 5,
    /// A floating point number: `float:`
    Float = 6,
    /// TBD structured data (flatbuffers?): `record:`
    Record = 7,
    /// A symbol type, used to distinguish attributes from other
    /// strings: `symbol:`
    Symbol = 8,
    /// Any number, natural, integer or float, as it is: `number:`. The
    /// other number types are views of it (see [`Value::viewed_as`]). No
    /// [`Value`] is of this type itself; it is what an attribute declares to
    /// hold numbers of every type, and the index band a tree whose manifest
    /// sets `value_encoding` to 1 keys every number in.
    Number = 9,
    /// A natural number that fits in 8 bits: `u8:`. Like every Rust
    /// numeric type it is a view of [`Number`](Self::Number) (see
    /// [`Value::viewed_as`]), never a type of a stored value, which is
    /// held under the view's storage type ([`Self::storage`]).
    U8 = 10,
    /// A natural number that fits in 16 bits: `u16:`.
    U16 = 11,
    /// A natural number that fits in 32 bits: `u32:`.
    U32 = 12,
    /// A natural number that fits in 64 bits: `u64:`.
    U64 = 13,
    /// A natural number that fits in 128 bits: `u128:`.
    U128 = 14,
    /// An integer that fits in 8 bits: `i8:`.
    I8 = 15,
    /// An integer that fits in 16 bits: `i16:`.
    I16 = 16,
    /// An integer that fits in 32 bits: `i32:`.
    I32 = 17,
    /// An integer that fits in 64 bits: `i64:`.
    I64 = 18,
    /// An integer that fits in 128 bits: `i128:`.
    I128 = 19,
    /// A number read as the nearest single-precision float: `f32:`.
    F32 = 20,
    /// A number read as the nearest double-precision float: `f64:`.
    F64 = 21,
}

impl ValueDataType {
    /// Every type, in discriminant order.
    pub const ALL: [ValueDataType; 22] = [
        ValueDataType::Bytes,
        ValueDataType::Entity,
        ValueDataType::Boolean,
        ValueDataType::String,
        ValueDataType::UnsignedInt,
        ValueDataType::SignedInt,
        ValueDataType::Float,
        ValueDataType::Record,
        ValueDataType::Symbol,
        ValueDataType::Number,
        ValueDataType::U8,
        ValueDataType::U16,
        ValueDataType::U32,
        ValueDataType::U64,
        ValueDataType::U128,
        ValueDataType::I8,
        ValueDataType::I16,
        ValueDataType::I32,
        ValueDataType::I64,
        ValueDataType::I128,
        ValueDataType::F32,
        ValueDataType::F64,
    ];

    /// The entity that names this type, such as `text:`.
    pub fn uri(&self) -> &'static str {
        match self {
            ValueDataType::Bytes => "bytes:",
            ValueDataType::Entity => "entity:",
            ValueDataType::Boolean => "boolean:",
            ValueDataType::String => "text:",
            ValueDataType::UnsignedInt => "natural:",
            ValueDataType::SignedInt => "integer:",
            ValueDataType::Float => "float:",
            ValueDataType::Record => "record:",
            ValueDataType::Symbol => "symbol:",
            ValueDataType::Number => "number:",
            ValueDataType::U8 => "u8:",
            ValueDataType::U16 => "u16:",
            ValueDataType::U32 => "u32:",
            ValueDataType::U64 => "u64:",
            ValueDataType::U128 => "u128:",
            ValueDataType::I8 => "i8:",
            ValueDataType::I16 => "i16:",
            ValueDataType::I32 => "i32:",
            ValueDataType::I64 => "i64:",
            ValueDataType::I128 => "i128:",
            ValueDataType::F32 => "f32:",
            ValueDataType::F64 => "f64:",
        }
    }

    /// The name the release before type entities wrote for this type,
    /// which identities it computed hash.
    pub fn legacy_name(&self) -> &'static str {
        match self {
            ValueDataType::Bytes => "Bytes",
            ValueDataType::Entity => "Entity",
            ValueDataType::Boolean => "Boolean",
            ValueDataType::String => "Text",
            ValueDataType::UnsignedInt => "UnsignedInteger",
            ValueDataType::SignedInt => "SignedInteger",
            ValueDataType::Float => "Float",
            ValueDataType::Record => "Record",
            ValueDataType::Symbol => "Symbol",
            ValueDataType::Number => "Number",
            ValueDataType::U8 => "U8",
            ValueDataType::U16 => "U16",
            ValueDataType::U32 => "U32",
            ValueDataType::U64 => "U64",
            ValueDataType::U128 => "U128",
            ValueDataType::I8 => "I8",
            ValueDataType::I16 => "I16",
            ValueDataType::I32 => "I32",
            ValueDataType::I64 => "I64",
            ValueDataType::I128 => "I128",
            ValueDataType::F32 => "F32",
            ValueDataType::F64 => "F64",
        }
    }

    /// The type `name` names: its entity (`text:`), or the name the
    /// release before type entities wrote (`Text`).
    pub fn named(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.uri() == name || kind.legacy_name() == name)
    }
}

impl Serialize for ValueDataType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.uri())
    }
}

impl<'de> Deserialize<'de> for ValueDataType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Named;
        impl de::Visitor<'_> for Named {
            type Value = ValueDataType;
            fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
                formatter.write_str("a type entity such as `text:`")
            }
            fn visit_str<E: de::Error>(self, name: &str) -> Result<ValueDataType, E> {
                ValueDataType::named(name)
                    .ok_or_else(|| E::custom(format!("`{name}` names no type")))
            }
        }
        deserializer.deserialize_str(Named)
    }
}

impl ValueDataType {
    /// The type byte an index key carries for values of this type, under
    /// the manifest's `value_encoding`: each type its own band (0), or
    /// every number in the [`Number`](Self::Number) band (1).
    pub fn band(self, value_encoding: u32) -> Self {
        match self.storage() {
            ValueDataType::UnsignedInt | ValueDataType::SignedInt | ValueDataType::Float
                if value_encoding == NUMBER_BAND =>
            {
                ValueDataType::Number
            }
            other => other,
        }
    }

    /// The type a value of this type is stored as: a Rust numeric view's
    /// natural, integer or float, and any other type itself.
    pub fn storage(self) -> Self {
        match self {
            ValueDataType::U8
            | ValueDataType::U16
            | ValueDataType::U32
            | ValueDataType::U64
            | ValueDataType::U128 => ValueDataType::UnsignedInt,
            ValueDataType::I8
            | ValueDataType::I16
            | ValueDataType::I32
            | ValueDataType::I64
            | ValueDataType::I128 => ValueDataType::SignedInt,
            ValueDataType::F32 | ValueDataType::F64 => ValueDataType::Float,
            other => other,
        }
    }

    /// Whether every number this Rust numeric view holds, `other` holds
    /// too: `u8:` nests in `u16:`, `i16:` and `f32:`; `f32:` in `f64:`.
    pub fn nests_in(self, other: ValueDataType) -> bool {
        fn bits(view: ValueDataType) -> u32 {
            match view {
                ValueDataType::U8 | ValueDataType::I8 => 8,
                ValueDataType::U16 | ValueDataType::I16 => 16,
                ValueDataType::U32 | ValueDataType::I32 | ValueDataType::F32 => 32,
                ValueDataType::U64 | ValueDataType::I64 | ValueDataType::F64 => 64,
                _ => 128,
            }
        }
        use ValueDataType::{F32, F64, Float, SignedInt, UnsignedInt};
        if self == other {
            return true;
        }
        match (self.storage(), other.storage()) {
            (UnsignedInt, UnsignedInt) | (SignedInt, SignedInt) => bits(self) <= bits(other),
            (UnsignedInt, SignedInt) => bits(self) < bits(other),
            (Float, Float) => self == F32 && other == F64,
            // A float holds a whole number exactly while it fits in its
            // significand: 24 bits for `f32:`, 53 for `f64:`.
            (UnsignedInt | SignedInt, Float) => match other {
                F32 => bits(self) <= 16,
                F64 => bits(self) <= 32,
                _ => false,
            },
            _ => false,
        }
    }

    /// Whether this is a Rust numeric view (`u8:` through `f64:`).
    pub fn is_view(self) -> bool {
        self.storage() != self
    }

    /// Whether a value of this type holds `value` exactly: one of its own
    /// type, any number where this is [`Number`](Self::Number), and for a
    /// Rust numeric view a number its view reads unchanged, so a write
    /// through `u8:` takes 255 and refuses 256 and 2.5.
    pub fn holds(self, value: &Value) -> bool {
        let actual = value.data_type();
        if self.is_view() {
            return actual.is_number()
                && value
                    .clone()
                    .viewed_as(self)
                    .is_some_and(|viewed| viewed.same_number(value));
        }
        actual == self || (self == ValueDataType::Number && actual.is_number())
    }

    /// Whether this is one of the number types.
    pub fn is_number(self) -> bool {
        matches!(
            self,
            ValueDataType::UnsignedInt
                | ValueDataType::SignedInt
                | ValueDataType::Float
                | ValueDataType::Number
        ) || self.is_view()
    }

    /// The smallest [`ValueDataType`] in discriminant order
    pub fn min() -> Self {
        ValueDataType::Bytes
    }

    /// The largest [`ValueDataType`] in discriminant order
    pub fn max() -> Self {
        ValueDataType::Number
    }

    /// Check if the given value is of this type.
    pub fn check(&self, value: &Value) -> Result<(), TypeError> {
        if self.holds(value) {
            return Ok(());
        }
        let other = value.data_type();
        self.unify(&other).and(Ok(()))
    }

    /// Unifies this type with the other type.
    pub fn unify(&self, other: &ValueDataType) -> Result<ValueDataType, TypeError> {
        if mem::discriminant(self) != mem::discriminant(other) {
            Err(TypeError::TypeMismatch(*self, *other))
        } else {
            Ok(*self)
        }
    }
}

impl Display for ValueDataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ValueDataType::Bytes => write!(f, "Bytes"),
            ValueDataType::Entity => write!(f, "Entity"),
            ValueDataType::Boolean => write!(f, "Boolean"),
            ValueDataType::String => write!(f, "String"),
            ValueDataType::UnsignedInt => write!(f, "UnsignedInt"),
            ValueDataType::SignedInt => write!(f, "SignedInt"),
            ValueDataType::Float => write!(f, "Float"),
            ValueDataType::Record => write!(f, "Record"),
            ValueDataType::Symbol => write!(f, "Symbol"),
            ValueDataType::Number => write!(f, "Number"),
            view => write!(f, "{}", view.legacy_name()),
        }
    }
}

impl From<u8> for ValueDataType {
    fn from(value: u8) -> Self {
        Self::from(&value)
    }
}

impl From<&u8> for ValueDataType {
    fn from(value: &u8) -> Self {
        match value {
            0 => ValueDataType::Bytes,
            1 => ValueDataType::Entity,
            2 => ValueDataType::Boolean,
            3 => ValueDataType::String,
            4 => ValueDataType::UnsignedInt,
            5 => ValueDataType::SignedInt,
            6 => ValueDataType::Float,
            7 => ValueDataType::Record,
            8 => ValueDataType::Symbol,
            9 => ValueDataType::Number,
            // A lossy fallback for infallible callers; the key-parse path
            // rejects unknown discriminants before reaching this (see
            // `varkey::value_payload_len`), so no persisted-byte path relies
            // on the default.
            _ => ValueDataType::Bytes,
        }
    }
}

impl From<ValueDataType> for u8 {
    fn from(value: ValueDataType) -> Self {
        value as u8
    }
}

impl From<String> for ValueDataType {
    fn from(_: String) -> Self {
        Self::String
    }
}

impl From<bool> for ValueDataType {
    fn from(_value: bool) -> Self {
        Self::Boolean
    }
}

impl From<ValueDataType> for PhantomData<ValueDataType> {
    fn from(_value: ValueDataType) -> Self {
        PhantomData
    }
}

#[cfg(test)]
mod deserialize_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    /// A multi-line text value must deserialize as `Value::String`, byte
    /// for byte — NOT be captured by the untagged enum's `Entity` variant
    /// and mangled through `url::Url` (which strips newlines and
    /// lowercases the leading token). Regression for a corruption where a
    /// text value beginning with a `scheme:`-shaped token (e.g. the
    /// `Tonk-Prose-Version: 1\r\n…` envelope) round-tripped as a broken
    /// entity.
    #[dialog_common::test]
    fn it_deserializes_a_multiline_text_value_as_string() {
        let text = "Tonk-Prose-Version: 1\r\nETag: \"999\"\r\n\r\n# Hello\n\nbody";
        let json = serde_json::to_string(text).expect("serialize");
        let value: Value = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            value,
            Value::String(text.to_string()),
            "a multi-line text value must decode as a byte-exact String, not a mangled Entity",
        );
    }

    /// A plain text value with a colon and a space is text, not an entity.
    #[dialog_common::test]
    fn it_deserializes_colon_text_as_string() {
        for text in ["note: buy milk", "value: with colon", "http status: 200"] {
            let json = serde_json::to_string(text).expect("serialize");
            let value: Value = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(value, Value::String(text.to_string()), "{text:?}");
        }
    }

    /// Real entity URIs still deserialize as `Value::Entity` — the fix
    /// only excludes whitespace-bearing strings, which no canonical URI
    /// contains.
    #[dialog_common::test]
    fn it_still_deserializes_entity_uris_as_entity() {
        for uri in [
            "did:key:z6MkhcqMSivySW3g74SchZQBgEUHHR3EQ1uUp4exY9cev6Ug",
            "did:web:cdata.earth",
            "id:prose/doc",
            "db:transient",
        ] {
            let json = serde_json::to_string(uri).expect("serialize");
            let value: Value = serde_json::from_str(&json).expect("deserialize");
            assert!(
                matches!(value, Value::Entity(_)),
                "{uri:?} should decode as an Entity, got {value:?}",
            );
        }
    }
}

/// What a value's encoding keeps of its type. Bare, variants that share
/// a payload shape share bytes: text and an entity are both strings, a
/// symbol is a string, a signed and an unsigned integer are both
/// integers, and bytes and a record are both byte sequences. Tagged by
/// its type entity, each value decodes as itself and hashes apart from
/// every value of another type.
#[cfg(test)]
mod encoding_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    fn cbor(value: &Value) -> Vec<u8> {
        serde_ipld_dagcbor::to_vec(value).expect("encodes")
    }

    fn round_trip(value: &Value) -> Value {
        serde_ipld_dagcbor::from_slice(&cbor(value)).expect("decodes")
    }

    /// Text that is a valid URI comes back as text, not as an entity.
    #[dialog_common::test]
    fn it_round_trips_text_shaped_like_a_uri() {
        let text = Value::String("https://example.com".into());
        assert_eq!(round_trip(&text), text);
        let bare = Value::String("foo:".into());
        assert_eq!(round_trip(&bare), bare);
    }

    /// A symbol comes back as a symbol, not as text.
    #[dialog_common::test]
    fn it_round_trips_a_symbol() {
        let symbol = Value::Symbol("person/name".parse().expect("a relation"));
        assert_eq!(round_trip(&symbol), symbol);
    }

    /// An integer comes back as the integer it was, and a record as a
    /// record.
    #[dialog_common::test]
    fn it_round_trips_integers_and_a_record() {
        let unsigned = Value::UnsignedInt(5);
        assert_eq!(round_trip(&unsigned), unsigned);
        let signed = Value::SignedInt(5);
        assert_eq!(round_trip(&signed), signed);
        let record = Value::Record(vec![1, 2, 3]);
        assert_eq!(round_trip(&record), record);
    }

    /// A value conforms to a type that reads its spelling or number, and
    /// stays as it is otherwise.
    #[dialog_common::test]
    fn it_conforms_a_value_to_a_type_that_reads_it() {
        let entity = |uri: &str| Value::Entity(uri.parse().expect("an entity"));
        assert_eq!(
            entity("case:active").conform(ValueDataType::String),
            Value::String("case:active".into())
        );
        assert_eq!(
            Value::String("case:active".into()).conform(ValueDataType::Entity),
            entity("case:active")
        );
        assert_eq!(
            Value::String("person/name".into()).conform(ValueDataType::Symbol),
            Value::Symbol("person/name".parse().expect("a relation"))
        );
        assert_eq!(
            Value::UnsignedInt(5).conform(ValueDataType::SignedInt),
            Value::SignedInt(5)
        );
        assert_eq!(
            Value::SignedInt(-5).conform(ValueDataType::UnsignedInt),
            Value::SignedInt(-5),
            "a negative integer is no natural number"
        );
        assert_eq!(
            Value::String("not an entity".into()).conform(ValueDataType::Entity),
            Value::String("not an entity".into())
        );
        assert_eq!(
            Value::Boolean(true).conform(ValueDataType::String),
            Value::Boolean(true)
        );
    }

    /// Every value survives a round trip through JSON, written bare where
    /// its bare payload reads back as itself and tagged where it does not.
    #[dialog_common::test]
    fn it_round_trips_every_value_through_json() {
        let cases = [
            (Value::String("Alice".into()), serde_json::json!("Alice")),
            (
                Value::String("https://example.com".into()),
                serde_json::json!({ "text:": "https://example.com" }),
            ),
            (
                Value::Entity("did:web:cdata.earth".parse().expect("an entity")),
                serde_json::json!("did:web:cdata.earth"),
            ),
            (
                Value::Symbol("person/name".parse().expect("a relation")),
                serde_json::json!({ "symbol:": "person/name" }),
            ),
            (Value::UnsignedInt(5), serde_json::json!(5)),
            (Value::SignedInt(-5), serde_json::json!(-5)),
            (Value::SignedInt(5), serde_json::json!({ "integer:": 5 })),
            (Value::Float(5.5), serde_json::json!(5.5)),
            (Value::Boolean(true), serde_json::json!(true)),
            (Value::Bytes(vec![1, 2]), serde_json::json!([1, 2])),
            (
                Value::Record(vec![1, 2]),
                serde_json::json!({ "record:": [1, 2] }),
            ),
        ];
        for (value, json) in cases {
            assert_eq!(
                serde_json::to_value(&value).expect("encodes"),
                json,
                "{value:?}"
            );
            let decoded: Value = serde_json::from_value(json).expect("decodes");
            assert_eq!(decoded, value);
        }
    }

    /// A value written bare in binary by an earlier release reads as the
    /// first type its payload fits, as it always did.
    #[dialog_common::test]
    fn it_reads_the_bare_payloads_earlier_releases_wrote() {
        let bare = |value: &Value| serde_ipld_dagcbor::to_vec(&value.untagged()).expect("encodes");
        let read =
            |bytes: Vec<u8>| -> Value { serde_ipld_dagcbor::from_slice(&bytes).expect("decodes") };
        assert_eq!(
            read(bare(&Value::String("Alice".into()))),
            Value::String("Alice".into())
        );
        assert_eq!(
            read(bare(&Value::String("did:web:cdata.earth".into()))),
            Value::Entity("did:web:cdata.earth".parse().expect("an entity"))
        );
        assert_eq!(read(bare(&Value::UnsignedInt(5))), Value::UnsignedInt(5));
        assert_eq!(read(bare(&Value::SignedInt(-5))), Value::SignedInt(-5));
        assert_eq!(
            read(bare(&Value::Bytes(vec![1, 2]))),
            Value::Bytes(vec![1, 2])
        );
    }

    /// Values of different types encode to different bytes, so a hash
    /// over the encoding tells them apart.
    #[dialog_common::test]
    fn it_encodes_values_of_different_types_differently() {
        let pairs = [
            (
                Value::String("foo:".into()),
                Value::Entity("foo:".parse().expect("an entity")),
            ),
            (
                Value::String("person/name".into()),
                Value::Symbol("person/name".parse().expect("a relation")),
            ),
            (Value::UnsignedInt(5), Value::SignedInt(5)),
            (Value::Bytes(vec![1]), Value::Record(vec![1])),
        ];
        for (left, right) in pairs {
            assert_ne!(cbor(&left), cbor(&right), "{left:?} and {right:?}");
        }
    }
}

/// The Rust numeric views read numbers by range and precision: they filter
/// what is out of range, coerce only where they lose precision, and hold
/// exactly the numbers they read unchanged.
#[cfg(test)]
mod view_tests {
    #![allow(unexpected_cfgs)]

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    #[dialog_common::test]
    fn it_reads_numbers_through_rust_numeric_views() {
        use ValueDataType::*;
        let cases: [(Value, ValueDataType, Option<Value>); 16] = [
            (Value::SignedInt(255), U8, Some(Value::UnsignedInt(255))),
            (Value::UnsignedInt(256), U8, None),
            (Value::SignedInt(-1), U8, None),
            (Value::Float(2.7), U8, Some(Value::UnsignedInt(2))),
            (Value::Float(-2.5), I8, Some(Value::SignedInt(-3))),
            (Value::SignedInt(-129), I8, None),
            (Value::SignedInt(-128), I8, Some(Value::SignedInt(-128))),
            (Value::UnsignedInt(1 << 40), U32, None),
            (
                Value::UnsignedInt(u128::MAX),
                U128,
                Some(Value::UnsignedInt(u128::MAX)),
            ),
            (Value::UnsignedInt(1 << 63), I64, None),
            (
                Value::Float(0.1),
                F32,
                Some(Value::Float(f64::from(0.1f32))),
            ),
            (Value::Float(1e40), F32, None),
            (
                Value::Float(f64::INFINITY),
                F32,
                Some(Value::Float(f64::INFINITY)),
            ),
            (Value::UnsignedInt(3), F64, Some(Value::Float(3.0))),
            (Value::Float(0.1), F64, Some(Value::Float(0.1))),
            (
                Value::String("3".into()),
                U8,
                Some(Value::String("3".into())),
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

    /// Every replica must read a number through a view to the same bytes,
    /// so the edge cases are pinned as exact bits rather than compared with
    /// what this target computes: the test runs on wasm32 as well as
    /// natively, and both must produce these constants.
    #[dialog_common::test]
    fn it_views_identically_on_every_target() {
        use ValueDataType::{F32, F64, I8, I64, U8, U64};
        fn bits(value: &Option<Value>) -> String {
            match value {
                Some(Value::Float(float)) => format!("f{:016x}", float.to_bits()),
                Some(Value::UnsignedInt(natural)) => format!("u{natural}"),
                Some(Value::SignedInt(integer)) => format!("i{integer}"),
                Some(other) => format!("{other:?}"),
                None => "none".into(),
            }
        }
        let quiet_nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let cases: [(Value, ValueDataType, &str); 24] = [
            (Value::Float(255.0), U8, "u255"),
            (Value::Float(255.999), U8, "u255"),
            (Value::Float(256.0), U8, "none"),
            (Value::Float(-0.0), U8, "u0"),
            (Value::Float(-0.5), U8, "none"),
            (Value::Float(f64::NAN), U8, "none"),
            (Value::Float(f64::INFINITY), U8, "none"),
            (Value::Float(-128.0), I8, "i-128"),
            (Value::Float(-128.5), I8, "none"),
            (Value::Float(127.999), I8, "i127"),
            (Value::Float(128.0), I8, "none"),
            (Value::SignedInt(-129), I8, "none"),
            (
                Value::UnsignedInt(u64::MAX as u128),
                U64,
                "u18446744073709551615",
            ),
            (Value::Float(18_446_744_073_709_551_616.0), U64, "none"),
            (
                Value::Float(-9_223_372_036_854_775_808.0),
                I64,
                "i-9223372036854775808",
            ),
            (Value::Float(1e-45), F32, "f36a0000000000000"),
            (Value::Float(f64::from(f32::MAX)), F32, "f47efffffe0000000"),
            (Value::Float(3.402_823_7e38), F32, "none"),
            (Value::Float(0.1), F32, "f3fb99999a0000000"),
            (Value::Float(-0.0), F32, "f8000000000000000"),
            (Value::Float(quiet_nan), F32, "f7ff8000000000000"),
            (Value::Float(f64::NEG_INFINITY), F32, "ffff0000000000000"),
            (Value::UnsignedInt(16_777_217), F32, "f4170000000000000"),
            (Value::SignedInt(-3), F64, "fc008000000000000"),
        ];
        for (value, view, expected) in cases {
            assert_eq!(
                bits(&value.clone().viewed_as(view)),
                expected,
                "{value:?} as {view}"
            );
        }
    }

    #[dialog_common::test]
    fn it_writes_only_what_a_view_holds_exactly() {
        use ValueDataType::*;
        assert!(U8.holds(&Value::UnsignedInt(255)));
        assert!(
            U8.holds(&Value::SignedInt(7)),
            "an integer 7 is the natural 7"
        );
        assert!(!U8.holds(&Value::UnsignedInt(256)));
        assert!(!U8.holds(&Value::Float(2.5)));
        assert!(U8.holds(&Value::Float(3.0)), "3.0 is the number 3");
        assert!(I8.holds(&Value::SignedInt(-128)));
        assert!(!I8.holds(&Value::SignedInt(128)));
        assert!(F32.holds(&Value::Float(f64::from(0.1f32))));
        assert!(!F32.holds(&Value::Float(0.1)), "0.1 is not an f32");
        assert!(F64.holds(&Value::Float(0.1)));
        assert!(!U8.holds(&Value::String("3".into())));
    }

    #[dialog_common::test]
    fn it_compares_numbers_by_value() {
        assert!(Value::UnsignedInt(5).same_number(&Value::SignedInt(5)));
        assert!(Value::SignedInt(5).same_number(&Value::Float(5.0)));
        assert!(!Value::SignedInt(-5).same_number(&Value::UnsignedInt(5)));
        assert!(!Value::Float(5.5).same_number(&Value::SignedInt(5)));
        // The nearest float to 2^60 + 1 is 2^60, not the same number.
        assert!(!Value::UnsignedInt((1 << 60) + 1).same_number(&Value::Float(2f64.powi(60))));
        assert!(Value::UnsignedInt(1 << 60).same_number(&Value::Float(2f64.powi(60))));
    }

    #[dialog_common::test]
    fn it_nests_views_by_range_and_precision() {
        use ValueDataType::*;
        for (inner, outer) in [
            (U8, U16),
            (U8, I16),
            (U16, F32),
            (U32, F64),
            (I32, I64),
            (F32, F64),
            (U64, U128),
            (U64, I128),
        ] {
            assert!(inner.nests_in(outer), "{inner} in {outer}");
            assert!(
                inner == outer || !outer.nests_in(inner),
                "{outer} not in {inner}"
            );
        }
        for (a, b) in [(U8, I8), (I8, U16), (U32, F32), (U64, F64), (F64, U64)] {
            assert!(!a.nests_in(b), "{a} not in {b}");
        }
    }
}
