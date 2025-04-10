use crate::{Attribute, Blake3Hash, ENTITY_LENGTH, RawEntity, XFactsError, make_reference};

/// All value type representations that may be stored by [`Facts`]
#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum Value {
    /// An empty (null) value
    Null,
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
    Structured(Vec<u8>),
    /// A symbol type, used to distinguish attributes from other strings
    Symbol(Attribute),
}

impl Value {
    /// Get the [`ValueDataType`] that corresponds to this variant of [`Value`]
    pub fn data_type(&self) -> ValueDataType {
        match self {
            Value::Null => ValueDataType::Null,
            Value::Bytes(_) => ValueDataType::Bytes,
            Value::Entity(_) => ValueDataType::Entity,
            Value::Boolean(_) => ValueDataType::Boolean,
            Value::String(_) => ValueDataType::String,
            Value::UnsignedInt(_) => ValueDataType::UnsignedInt,
            Value::SignedInt(_) => ValueDataType::SignedInt,
            Value::Float(_) => ValueDataType::Float,
            Value::Structured(_) => ValueDataType::Structured,
            Value::Symbol(_) => ValueDataType::Symbol,
        }
    }

    /// Convert this [`Value`] to its byte representation
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![],
            Value::Bytes(bytes) => bytes.to_owned(),
            Value::Entity(entity) => entity.as_ref().to_vec(),
            Value::Boolean(value) => vec![u8::from(*value)],
            Value::String(string) => string.as_bytes().to_vec(),
            Value::UnsignedInt(value) => value.to_le_bytes().to_vec(),
            Value::SignedInt(value) => value.to_le_bytes().to_vec(),
            Value::Float(value) => value.to_le_bytes().to_vec(),
            Value::Structured(value) => value.to_owned(),
            Value::Symbol(value) => value.key_bytes().to_vec(),
        }
    }

    /// Produce a hash reference to this [`Value`]
    pub fn to_reference(&self) -> Blake3Hash {
        make_reference(&self.to_bytes())
    }
}

impl TryFrom<(ValueDataType, Vec<u8>)> for Value {
    type Error = XFactsError;

    fn try_from((value_data_type, value): (ValueDataType, Vec<u8>)) -> Result<Self, Self::Error> {
        Ok(match value_data_type {
            ValueDataType::Null => Value::Null,
            ValueDataType::Bytes => Value::Bytes(value),
            ValueDataType::Entity => {
                Value::Entity(value.try_into().map_err(|value: Vec<u8>| {
                    XFactsError::InvalidEntity(format!(
                        "Wrong byte length for entity (expected {}, got {})",
                        ENTITY_LENGTH,
                        value.len()
                    ))
                })?)
            }
            // TODO: How strictly validated must a bool representation be?
            ValueDataType::Boolean => match value.get(0) {
                Some(byte) if value.len() == 1 => Value::Boolean(*byte != 0),
                _ => {
                    return Err(XFactsError::InvalidValue(format!(
                        "Wrong byte length for boolean (expected 1, got {})",
                        value.len()
                    )));
                }
            },
            ValueDataType::String => match String::from_utf8(value) {
                Ok(value) => Value::String(value),
                Err(error) => {
                    return Err(XFactsError::InvalidValue(format!(
                        "Not a valid UTF-8 string: {error}"
                    )));
                }
            },
            // TODO: Use a different encoding strategy for numerics? Varint for
            // integer? What about floats?
            ValueDataType::UnsignedInt => Value::UnsignedInt(u128::from_le_bytes(
                value.try_into().map_err(|value: Vec<u8>| {
                    XFactsError::InvalidValue(format!(
                        "Wrong number of bytes for u128 (expected 16, got {})",
                        value.len()
                    ))
                })?,
            )),
            ValueDataType::SignedInt => Value::SignedInt(i128::from_le_bytes(
                value.try_into().map_err(|value: Vec<u8>| {
                    XFactsError::InvalidValue(format!(
                        "Wrong number of bytes for i128 (expected 16, got {})",
                        value.len()
                    ))
                })?,
            )),
            ValueDataType::Float => Value::Float(f64::from_le_bytes(value.try_into().map_err(
                |value: Vec<u8>| {
                    XFactsError::InvalidValue(format!(
                        "Wrong number of bytes for f64 (expected 16, got {})",
                        value.len()
                    ))
                },
            )?)),
            ValueDataType::Structured => unimplemented!("TBD but probably flatbuffers?"),
            ValueDataType::Symbol => match String::from_utf8(value) {
                Ok(value) => Value::Symbol(Attribute::try_from(value)?),
                Err(error) => {
                    return Err(XFactsError::InvalidValue(format!(
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
#[repr(u8)]
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValueDataType {
    /// An empty (null) value
    #[default]
    Null = 0,
    /// A byte buffer
    Bytes = 1,
    /// An [`Entity`]
    Entity = 2,
    /// A boolean
    Boolean = 3,
    /// A UTF-8 string
    String = 4,
    /// A 128-bit unsigned integer
    UnsignedInt = 5,
    /// A 128-bit signed integer
    SignedInt = 6,
    /// A floating point number
    Float = 7,
    /// TBD structured data (flatbuffers?)
    Structured = 8,
    /// A symbol type, used to distinguish attributes from other strings
    Symbol = 9,
}

impl ValueDataType {
    /// The smallest [`ValueDataType`] in discriminant order
    pub fn min() -> Self {
        ValueDataType::Null
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
            1 => ValueDataType::Bytes,
            2 => ValueDataType::Entity,
            3 => ValueDataType::Boolean,
            4 => ValueDataType::String,
            5 => ValueDataType::UnsignedInt,
            6 => ValueDataType::SignedInt,
            7 => ValueDataType::Float,
            8 => ValueDataType::Structured,
            _ => ValueDataType::Null,
        }
    }
}

impl From<ValueDataType> for u8 {
    fn from(value: ValueDataType) -> Self {
        value as u8
    }
}
