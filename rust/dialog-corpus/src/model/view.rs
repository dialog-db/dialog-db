//! A stored value read in place: what a reader that only hands a value on
//! (an answer's encoder, a comparison of two scanned rows) reads instead of
//! materializing a [`Value`].

use std::borrow::Cow;
use std::str::from_utf8;

use crate::{
    DialogCorpusError, Uri, Value, ValueDataType, decode_value, model::ordkey::decode_bytes_cow,
};

/// A [`Value`] borrowed from the bytes that store it: a scanned key's
/// inline payload, or a spilled value's block. Text, bytes, entities and
/// symbols borrow; numbers and booleans are read out, as they are small.
/// Only a component that carried an escaped zero byte is owned.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueView<'a> {
    /// A byte buffer.
    Bytes(Cow<'a, [u8]>),
    /// An entity, as its canonical URI.
    Entity(Cow<'a, str>),
    /// A boolean.
    Boolean(bool),
    /// A UTF-8 string.
    String(Cow<'a, str>),
    /// A 128-bit unsigned integer.
    UnsignedInt(u128),
    /// A 128-bit signed integer.
    SignedInt(i128),
    /// A floating point number.
    Float(f64),
    /// A record's bytes.
    Record(Cow<'a, [u8]>),
    /// A symbol (an attribute used as a value), as its name.
    Symbol(Cow<'a, str>),
}

impl<'a> From<&'a Value> for ValueView<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Bytes(bytes) => ValueView::Bytes(Cow::Borrowed(bytes)),
            Value::Entity(entity) => ValueView::Entity(Cow::Borrowed(entity.as_str())),
            Value::Boolean(boolean) => ValueView::Boolean(*boolean),
            Value::String(string) => ValueView::String(Cow::Borrowed(string)),
            Value::UnsignedInt(number) => ValueView::UnsignedInt(*number),
            Value::SignedInt(number) => ValueView::SignedInt(*number),
            Value::Float(number) => ValueView::Float(*number),
            Value::Record(bytes) => ValueView::Record(Cow::Borrowed(bytes)),
            Value::Symbol(relation) => ValueView::Symbol(Cow::Borrowed(relation.as_str())),
        }
    }
}

impl ValueView<'_> {
    /// The type of the value this views.
    pub fn data_type(&self) -> ValueDataType {
        match self {
            ValueView::Bytes(_) => ValueDataType::Bytes,
            ValueView::Entity(_) => ValueDataType::Entity,
            ValueView::Boolean(_) => ValueDataType::Boolean,
            ValueView::String(_) => ValueDataType::String,
            ValueView::UnsignedInt(_) => ValueDataType::UnsignedInt,
            ValueView::SignedInt(_) => ValueDataType::SignedInt,
            ValueView::Float(_) => ValueDataType::Float,
            ValueView::Record(_) => ValueDataType::Record,
            ValueView::Symbol(_) => ValueDataType::Symbol,
        }
    }

    /// Materializes the [`Value`] this views.
    pub fn to_value(&self) -> Result<Value, DialogCorpusError> {
        Ok(match self {
            ValueView::Bytes(bytes) => Value::Bytes(bytes.to_vec()),
            ValueView::Entity(uri) => Value::Entity(
                crate::Entity::from_stored(uri).map_err(|error| corrupt(format!("{error}")))?,
            ),
            ValueView::Boolean(boolean) => Value::Boolean(*boolean),
            ValueView::String(string) => Value::String(string.to_string()),
            ValueView::UnsignedInt(number) => Value::UnsignedInt(*number),
            ValueView::SignedInt(number) => Value::SignedInt(*number),
            ValueView::Float(number) => Value::Float(*number),
            ValueView::Record(bytes) => Value::Record(bytes.to_vec()),
            ValueView::Symbol(name) => Value::Symbol(name.to_string().try_into()?),
        })
    }
}

fn corrupt(reason: String) -> DialogCorpusError {
    DialogCorpusError::CorruptEntry(reason)
}

/// Borrows a key component as UTF-8, owning it only when it was escaped.
pub(crate) fn cow_str(bytes: Cow<'_, [u8]>) -> Result<Cow<'_, str>, DialogCorpusError> {
    match bytes {
        Cow::Borrowed(bytes) => from_utf8(bytes)
            .map(Cow::Borrowed)
            .map_err(|error| corrupt(format!("key component is not UTF-8: {error}"))),
        Cow::Owned(bytes) => String::from_utf8(bytes)
            .map(Cow::Owned)
            .map_err(|error| corrupt(format!("key component is not UTF-8: {error}"))),
    }
}

/// An inline escaped component, borrowed unless it carried an escape.
fn inline_raw(bytes: &[u8]) -> Result<Cow<'_, [u8]>, DialogCorpusError> {
    let (raw, rest) =
        decode_bytes_cow(bytes).ok_or_else(|| corrupt("inline value is unterminated".into()))?;
    if !rest.is_empty() {
        return Err(corrupt("inline value payload had trailing bytes".into()));
    }
    Ok(raw)
}

fn inline_text(bytes: &[u8]) -> Result<Cow<'_, str>, DialogCorpusError> {
    cow_str(inline_raw(bytes)?)
}

/// Decodes an order-preserving inline value of `value_type` in place: the
/// borrowing counterpart of [`decode_value`]. Validates what it would (text
/// is UTF-8, an entity a canonical URI, a symbol a relation's shape), so a
/// view that decodes is a value that materializes.
pub fn decode_value_view(
    value_type: ValueDataType,
    bytes: &[u8],
) -> Result<ValueView<'_>, DialogCorpusError> {
    Ok(match value_type.storage() {
        ValueDataType::String => ValueView::String(inline_text(bytes)?),
        ValueDataType::Bytes => ValueView::Bytes(inline_raw(bytes)?),
        ValueDataType::Record => ValueView::Record(inline_raw(bytes)?),
        ValueDataType::Entity => {
            let uri = inline_text(bytes)?;
            Uri::check_stored(&uri).map_err(|error| corrupt(format!("{error}")))?;
            ValueView::Entity(uri)
        }
        ValueDataType::Symbol => {
            let name = inline_text(bytes)?;
            check_relation(&name)?;
            ValueView::Symbol(name)
        }
        _ => {
            let (value, rest) = decode_value(value_type, bytes)
                .ok_or_else(|| corrupt("inline value payload did not decode".into()))?;
            if !rest.is_empty() {
                return Err(corrupt("inline value payload had trailing bytes".into()));
            }
            match value {
                Value::UnsignedInt(number) => ValueView::UnsignedInt(number),
                Value::SignedInt(number) => ValueView::SignedInt(number),
                Value::Float(number) => ValueView::Float(number),
                Value::Boolean(boolean) => ValueView::Boolean(boolean),
                other => {
                    return Err(corrupt(format!(
                        "a scalar type decoded as {:?}",
                        other.data_type()
                    )));
                }
            }
        }
    })
}

/// Reads a spilled value's block in place: the borrowing counterpart of
/// `Value::try_from((value_type, bytes))`.
pub fn spilled_value_view(
    value_type: ValueDataType,
    bytes: &[u8],
) -> Result<ValueView<'_>, DialogCorpusError> {
    Ok(match value_type.storage() {
        ValueDataType::String => {
            ValueView::String(Cow::Borrowed(from_utf8(bytes).map_err(|error| {
                DialogCorpusError::InvalidValue(format!("Not a valid UTF-8 string: {error}"))
            })?))
        }
        ValueDataType::Bytes => ValueView::Bytes(Cow::Borrowed(bytes)),
        ValueDataType::Record => ValueView::Record(Cow::Borrowed(bytes)),
        // The rare spilled scalar or reference: read it as main does.
        other => {
            let value = Value::try_from((other, bytes.to_vec()))?;
            match value {
                Value::Entity(entity) => ValueView::Entity(Cow::Owned(entity.as_str().to_owned())),
                Value::Symbol(relation) => {
                    ValueView::Symbol(Cow::Owned(relation.as_str().to_owned()))
                }
                Value::UnsignedInt(number) => ValueView::UnsignedInt(number),
                Value::SignedInt(number) => ValueView::SignedInt(number),
                Value::Float(number) => ValueView::Float(number),
                Value::Boolean(boolean) => ValueView::Boolean(boolean),
                Value::String(string) => ValueView::String(Cow::Owned(string)),
                Value::Bytes(bytes) => ValueView::Bytes(Cow::Owned(bytes)),
                Value::Record(bytes) => ValueView::Record(Cow::Owned(bytes)),
            }
        }
    })
}

/// What [`Relation`](crate::Relation) construction checks, on a borrowed
/// name: no longer than a relation may be, `domain/name`, no NUL.
pub(crate) fn check_relation(name: &str) -> Result<(), DialogCorpusError> {
    if name.len() > crate::RELATION_LENGTH || !name.contains('/') || name.as_bytes().contains(&0) {
        return Err(corrupt(format!("stored relation is malformed: {name:?}")));
    }
    Ok(())
}
