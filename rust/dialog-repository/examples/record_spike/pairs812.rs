//! The composite as draft pull request #812 built it, kept to measure
//! against: a header of every name once, sorted, then fields by index, a
//! nested composite's fields naming the outer names. Reading a field parses
//! every name and walks the fields before it, and a nested composite read
//! out is written again as a composite of its own.
//!
//! Its encoding and its `get` are #812's (`rust/dialog-corpus/src/model/composite.rs`
//! at `fd61b75f`), with the parts measurement does not use left out and a
//! nested composite held as a [`Field::Nested`] instead of a `Value`.

use dialog_corpus::{
    NUMBER_BAND, Relation, Value, ValueDataType, decode_bytes_cow, decode_value, encode_bytes,
    encode_key_value,
};
use std::collections::{BTreeMap, BTreeSet};

/// What ends a list of fields: the index no field has.
const END: u8 = 0;

/// A field's value: a scalar, a nested composite's fields to write, or a
/// nested composite read out, written again as one of its own.
#[derive(Clone, Debug, PartialEq)]
pub enum Field {
    Value(Value),
    Nested(Vec<(Relation, Field)>),
    Read(Pairs),
}

/// A value as #812 holds it: its canonical bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct Pairs(pub Vec<u8>);

impl Pairs {
    pub fn new(fields: Vec<(Relation, Field)>) -> Self {
        Pairs(encode(&fields))
    }

    /// #812's `get`: the values `attribute` holds, a nested composite
    /// written again as one of its own.
    pub fn get(&self, attribute: &Relation) -> Vec<Field> {
        let mut wanted = Vec::new();
        encode_bytes(attribute.as_str().as_bytes(), &mut wanted);
        let Some((names, mut at)) = header(&self.0) else {
            return Vec::new();
        };
        let Some(index) = names.iter().position(|name| *name == wanted.as_slice()) else {
            return Vec::new();
        };
        let index = index + 1;
        let mut values = Vec::new();
        while let Some((found, used)) = read_varint(&self.0[at..]) {
            at += used;
            if found == 0 || found > index {
                break;
            }
            let Some(len) = slot_len(&self.0[at..]) else {
                break;
            };
            if found == index
                && let Some(item) = read_slot(&self.0[at..at + len], &names)
            {
                values.push(item);
            }
            at += len;
        }
        values
    }
}

fn varint(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut n = 0usize;
    for (at, &byte) in bytes.iter().enumerate().take(9) {
        n |= ((byte & 0x7f) as usize) << (7 * at);
        if byte & 0x80 == 0 {
            return Some((n, at + 1));
        }
    }
    None
}

fn type_byte(value: &Value) -> u8 {
    match value {
        Value::UnsignedInt(_) | Value::SignedInt(_) | Value::Float(_) => {
            ValueDataType::Number as u8
        }
        other => other.data_type() as u8,
    }
}

fn name_of(attribute: &Relation) -> Vec<u8> {
    let mut name = Vec::new();
    encode_bytes(attribute.as_str().as_bytes(), &mut name);
    name
}

fn names(fields: &[(Relation, Field)], into: &mut BTreeSet<Vec<u8>>) {
    for (attribute, field) in fields {
        into.insert(name_of(attribute));
        if let Field::Nested(nested) = field {
            names(nested, into);
        }
    }
}

fn encode(fields: &[(Relation, Field)]) -> Vec<u8> {
    let mut table = BTreeSet::new();
    names(fields, &mut table);
    let index: BTreeMap<Vec<u8>, usize> = table
        .iter()
        .enumerate()
        .map(|(at, name)| (name.clone(), at + 1))
        .collect();
    let mut out = Vec::new();
    varint(table.len(), &mut out);
    for name in &table {
        out.extend_from_slice(name);
    }
    encode_fields(fields, &index, &mut out);
    out
}

fn encode_fields(
    fields: &[(Relation, Field)],
    index: &BTreeMap<Vec<u8>, usize>,
    out: &mut Vec<u8>,
) {
    let mut held: Vec<(usize, Vec<u8>)> = fields
        .iter()
        .map(|(attribute, field)| {
            let mut slot = Vec::new();
            match field {
                Field::Value(value) => {
                    slot.push(type_byte(value));
                    encode_key_value(value, NUMBER_BAND, &mut slot);
                }
                Field::Nested(nested) => {
                    slot.push(ValueDataType::Composite as u8);
                    encode_fields(nested, index, &mut slot);
                }
                Field::Read(_) => unreachable!("a composite read out is not written into another"),
            }
            (index[&name_of(attribute)], slot)
        })
        .collect();
    held.sort_unstable();
    held.dedup();
    for (at, slot) in held {
        varint(at, out);
        out.extend_from_slice(&slot);
    }
    out.push(END);
}

fn header(bytes: &[u8]) -> Option<(Vec<&[u8]>, usize)> {
    let (count, mut at) = read_varint(bytes)?;
    let mut names = Vec::with_capacity(count.min(bytes.len()));
    for _ in 0..count {
        let len = terminated_len(bytes.get(at..)?)?;
        names.push(&bytes[at..at + len]);
        at += len;
    }
    Some((names, at))
}

fn terminated_len(bytes: &[u8]) -> Option<usize> {
    let mut at = 0;
    loop {
        let zero = at + memchr::memchr(0, bytes.get(at..)?)?;
        match bytes.get(zero + 1) {
            Some(0xFF) => at = zero + 2,
            _ => return Some(zero + 1),
        }
    }
}

fn fields_len(bytes: &[u8]) -> Option<usize> {
    let mut at = 0;
    loop {
        let (index, used) = read_varint(bytes.get(at..)?)?;
        at += used;
        if index == 0 {
            return Some(at);
        }
        at += slot_len(bytes.get(at..)?)?;
    }
}

fn slot_len(bytes: &[u8]) -> Option<usize> {
    let (&kind, rest) = bytes.split_first()?;
    let len = if kind == ValueDataType::Number as u8 {
        dialog_number::encoded_len(rest)?
    } else if kind == ValueDataType::Composite as u8 {
        fields_len(rest)?
    } else if kind == ValueDataType::Boolean as u8 {
        1
    } else {
        terminated_len(rest)?
    };
    Some(1 + len)
}

fn read_fields(bytes: &[u8], names: &[&[u8]]) -> Option<(Vec<(Relation, Field)>, usize)> {
    let mut pairs = Vec::new();
    let mut at = 0;
    loop {
        let (index, used) = read_varint(bytes.get(at..)?)?;
        at += used;
        if index == 0 {
            return Some((pairs, at));
        }
        let name = names.get(index - 1)?;
        let (attribute, _) = decode_bytes_cow(name)?;
        let attribute = Relation::try_from(String::from_utf8(attribute.into_owned()).ok()?).ok()?;
        let len = slot_len(bytes.get(at..)?)?;
        // A nested composite inside the one read out stays its fields.
        let field = match read_slot(&bytes[at..at + len], names)? {
            Field::Read(_) => {
                let (content, _) = read_fields(&bytes[at + 1..at + len], names)?;
                Field::Nested(content)
            }
            field => field,
        };
        pairs.push((attribute, field));
        at += len;
    }
}

/// A slot's value; a nested composite is read whole and written again as a
/// composite of its own, as #812's `Item::into_value` does.
fn read_slot(slot: &[u8], names: &[&[u8]]) -> Option<Field> {
    let (&kind, rest) = slot.split_first()?;
    if kind == ValueDataType::Composite as u8 {
        let (content, _) = read_fields(rest, names)?;
        return Some(Field::Read(Pairs::new(content)));
    }
    let (value, left) = decode_value(ValueDataType::from(kind), rest)?;
    left.is_empty().then_some(Field::Value(value))
}
