use std::{fmt::Display, str::FromStr};

use crate::{Attribute, DialogArtifactsError, Entity, RawEntity, make_reference};

use base58::{FromBase58, ToBase58};
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// All value type representations that may be stored by [`Artifacts`]
#[derive(Debug, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// A byte buffer
    Bytes(Vec<u8>),
    /// An [`Entity`]
    Entity(RawEntity),
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
    Symbol(Attribute),
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
            Value::Entity(entity) => entity.as_ref().to_vec(),
            Value::Boolean(value) => vec![u8::from(*value)],
            Value::String(string) => string.as_bytes().to_vec(),
            Value::UnsignedInt(value) => value.to_le_bytes().to_vec(),
            Value::SignedInt(value) => value.to_le_bytes().to_vec(),
            Value::Float(value) => value.to_le_bytes().to_vec(),
            Value::Record(value) => value.to_owned(),
            Value::Symbol(value) => value.key_bytes().to_vec(),
        }
    }

    /// Serialize this [`Value`] to a UTF-8 string
    pub fn to_utf8(&self) -> String {
        match self {
            Value::Bytes(bytes) => format!("bytes:{}", bytes.to_base58()),
            Value::Entity(raw) => format!("entity:{}", raw.to_base58()),
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
        .map_err(|error| serde::de::Error::custom(format!("{error}")))
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
            E: std::fmt::Debug,
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
            "entity" => Value::Entity(*Entity::try_from(value.to_owned())?),
            "boolean" => Value::Boolean(bool::from_str(value).map_err(to_dialog_error)?),
            "string" => Value::String(value.to_owned()),
            "uint" => Value::UnsignedInt(value.parse().map_err(to_dialog_error)?),
            "sint" => Value::SignedInt(value.parse().map_err(to_dialog_error)?),
            "float" => Value::Float(value.parse().map_err(to_dialog_error)?),
            "record" => Value::Record(value.from_base58().map_err(to_dialog_error_debug)?),
            "attribute" => Value::Symbol(Attribute::from_str(value)?),
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
            ValueDataType::Bytes => Value::Bytes(value),
            ValueDataType::Entity => Value::Entity(*Entity::try_from(value)?),
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
                        "Wrong number of bytes for f64 (expected 16, got {})",
                        value.len()
                    ))
                },
            )?)),
            ValueDataType::Record => unimplemented!("TBD but probably flatbuffers?"),
            ValueDataType::Symbol => match String::from_utf8(value) {
                Ok(value) => Value::Symbol(Attribute::try_from(
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

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl From<RawEntity> for Value {
    fn from(value: RawEntity) -> Self {
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

impl From<Attribute> for Value {
    fn from(value: Attribute) -> Self {
        Value::Symbol(value)
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

/// [`ValueDataType`] embodies all types that are able to be represented
/// as a [`Value`].
#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    wasm_bindgen::prelude::wasm_bindgen
)]
#[repr(u8)]
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValueDataType {
    /// A byte buffer
    #[default]
    Bytes = 0,
    /// An [`Entity`]
    Entity = 1,
    /// A boolean
    Boolean = 2,
    /// A UTF-8 string
    String = 3,
    /// A 128-bit unsigned integer
    UnsignedInt = 4,
    /// A 128-bit signed integer
    SignedInt = 5,
    /// A floating point number
    Float = 6,
    /// TBD structured data (flatbuffers?)
    Record = 7,
    /// A symbol type, used to distinguish attributes from other strings
    Symbol = 8,
}

impl ValueDataType {
    /// The smallest [`ValueDataType`] in discriminant order
    pub fn min() -> Self {
        ValueDataType::Bytes
    }

    /// The largest [`ValueDataType`] in discriminant order
    pub fn max() -> Self {
        ValueDataType::Symbol
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
            _ => {
                println!(
                    "WARNING! Encountered unsupported value tag '{value}'; defaulting to bytes..."
                );
                ValueDataType::Bytes
            }
        }
    }
}

impl From<ValueDataType> for u8 {
    fn from(value: ValueDataType) -> Self {
        value as u8
    }
}
