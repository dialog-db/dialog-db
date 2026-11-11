//! A record as its attribute and value pairs, each laid out as a key's
//! tail: the attribute (`encode_bytes` of its relation, as a key writes
//! it), the value's type byte, and the value's order-preserving encoding
//! (numbers in the one band). Pairs are sorted by their bytes, duplicates
//! dropped, and a lone `0x00` where the next attribute would start ends the
//! record: no relation is empty, so no attribute starts with it. A field
//! holding a record holds its encoding inline, after the record type byte,
//! not escaped as a byte string.
//!
//! Every part delimits itself and compares in the order the whole does, so
//! the encoding compares as its pairs do, a shorter record sorting before a
//! longer one it begins, and splits into parts the way a key does.

use dialog_corpus::Value;
use dialog_corpus::{
    ValueDataType, decode_bytes_cow, decode_value, encode_bytes, encode_key_value,
};

/// The `value_encoding` that keys every number in one band.
pub const NUMBER_BAND: u32 = 1;

/// What ends a record: a lone zero where an attribute would start.
pub const END: u8 = 0x00;

/// A record as a writer holds it: its pairs, in any order, repeats allowed.
#[derive(Clone, Debug, PartialEq)]
pub struct Record(pub Vec<(String, Field)>);

/// What a field holds: a value, or a record.
#[derive(Clone, Debug, PartialEq)]
pub enum Field {
    Value(Value),
    Record(Record),
}

/// The type byte a value is keyed under: every number under the band's.
fn type_byte(value: &Value) -> u8 {
    match value {
        Value::UnsignedInt(_) | Value::SignedInt(_) | Value::Float(_) => {
            ValueDataType::Number as u8
        }
        other => other.data_type() as u8,
    }
}

/// One pair's bytes: the attribute, then the field's type byte and value.
fn pair(attribute: &str, field: &Field) -> Vec<u8> {
    let mut out = Vec::with_capacity(attribute.len() + 16);
    encode_bytes(attribute.as_bytes(), &mut out);
    match field {
        Field::Value(value) => {
            out.push(type_byte(value));
            encode_key_value(value, NUMBER_BAND, &mut out);
        }
        Field::Record(record) => {
            out.push(ValueDataType::Record as u8);
            encode_into(record, &mut out);
        }
    }
    out
}

/// The canonical bytes of `record`.
pub fn encode(record: &Record) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(record, &mut out);
    out
}

fn encode_into(record: &Record, out: &mut Vec<u8>) {
    let mut pairs: Vec<Vec<u8>> = record
        .0
        .iter()
        .map(|(attribute, field)| pair(attribute, field))
        .collect();
    pairs.sort_unstable();
    pairs.dedup();
    for pair in pairs {
        out.extend_from_slice(&pair);
    }
    out.push(END);
}

/// The length of one `0x00`-escaped, `0x00`-terminated string at the
/// front of `bytes`, terminator included.
fn terminated_len(bytes: &[u8]) -> Option<usize> {
    let mut at = 0;
    loop {
        let zero = at + memchr::memchr(END, &bytes[at..])?;
        match bytes.get(zero + 1) {
            Some(0xFF) => at = zero + 2,
            _ => return Some(zero + 1),
        }
    }
}

/// The length of a value of `kind` at the front of `bytes`, without
/// decoding it.
fn value_len(kind: u8, bytes: &[u8]) -> Option<usize> {
    if kind == ValueDataType::Number as u8 {
        return dialog_number::encoded_len(bytes);
    }
    if kind == ValueDataType::Record as u8 {
        return record_len(bytes);
    }
    match ValueDataType::from(kind) {
        ValueDataType::Boolean => Some(1),
        ValueDataType::UnsignedInt | ValueDataType::SignedInt => Some(16),
        ValueDataType::Float => Some(8),
        ValueDataType::String
        | ValueDataType::Bytes
        | ValueDataType::Entity
        | ValueDataType::Symbol => terminated_len(bytes),
        _ => None,
    }
}

/// The length of the record at the front of `bytes`, its end included.
pub fn record_len(bytes: &[u8]) -> Option<usize> {
    let mut at = 0;
    loop {
        if *bytes.get(at)? == END {
            return Some(at + 1);
        }
        at += terminated_len(&bytes[at..])?;
        let kind = *bytes.get(at)?;
        at += 1 + value_len(kind, &bytes[at + 1..])?;
    }
}

/// One pair of an encoded record, as spans of it: the attribute's encoding
/// and the value slot (type byte first).
#[derive(Clone, Copy, Debug)]
pub struct Part<'a> {
    pub attribute: &'a [u8],
    pub slot: &'a [u8],
}

/// The pairs of an encoded record, in order, without decoding any value.
pub fn parts(bytes: &[u8]) -> Option<Vec<Part<'_>>> {
    let mut out = Vec::new();
    let mut at = 0;
    loop {
        if *bytes.get(at)? == END {
            return Some(out);
        }
        let attribute_len = terminated_len(&bytes[at..])?;
        let attribute = &bytes[at..at + attribute_len];
        at += attribute_len;
        let kind = *bytes.get(at)?;
        let slot_len = 1 + value_len(kind, &bytes[at + 1..])?;
        out.push(Part {
            attribute,
            slot: &bytes[at..at + slot_len],
        });
        at += slot_len;
    }
}

/// The value slots `attribute` holds in an encoded record: a walk that
/// skips the other values without decoding them and stops past the
/// attribute, since pairs are sorted.
pub fn slots<'a>(bytes: &'a [u8], attribute: &[u8]) -> Option<Vec<&'a [u8]>> {
    let mut out = Vec::new();
    let mut at = 0;
    loop {
        if *bytes.get(at)? == END {
            return Some(out);
        }
        let attribute_len = terminated_len(&bytes[at..])?;
        let here = &bytes[at..at + attribute_len];
        at += attribute_len;
        let kind = *bytes.get(at)?;
        let slot_len = 1 + value_len(kind, &bytes[at + 1..])?;
        match here.cmp(attribute) {
            std::cmp::Ordering::Equal => out.push(&bytes[at..at + slot_len]),
            std::cmp::Ordering::Greater => return Some(out),
            std::cmp::Ordering::Less => {}
        }
        at += slot_len;
    }
}

/// An attribute as a record keys it.
pub fn attribute_key(attribute: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(attribute.len() + 1);
    encode_bytes(attribute.as_bytes(), &mut out);
    out
}

/// The field a value slot holds: a value decoded, or the record's bytes.
pub fn field_of(slot: &[u8]) -> Option<Field> {
    let (&kind, rest) = slot.split_first()?;
    if kind == ValueDataType::Record as u8 {
        return decode(rest).map(Field::Record);
    }
    let (value, _) = decode_value(ValueDataType::from(kind), rest)?;
    Some(Field::Value(value))
}

/// The record `bytes` encode.
pub fn decode(bytes: &[u8]) -> Option<Record> {
    let mut pairs = Vec::new();
    for part in parts(bytes)? {
        let (attribute, _) = decode_bytes_cow(part.attribute)?;
        let attribute = String::from_utf8(attribute.into_owned()).ok()?;
        pairs.push((attribute, field_of(part.slot)?));
    }
    Some(Record(pairs))
}

/// The leading whole pairs of `bytes` that fit in `limit` bytes, ended:
/// a spilled record's prefix, itself a record that sorts before the
/// record it was cut from.
pub fn prefix(bytes: &[u8], limit: usize) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    for part in parts(bytes)? {
        if out.len() + part.attribute.len() + part.slot.len() + 1 > limit {
            break;
        }
        out.extend_from_slice(part.attribute);
        out.extend_from_slice(part.slot);
    }
    out.push(END);
    Some(out)
}
