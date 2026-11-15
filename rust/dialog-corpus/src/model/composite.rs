//! A composite: one value made of attribute and value pairs.
//!
//! A composite is atomic, as any value is: it is written and replaced whole.
//! Its fields are named by attributes (relations), a field may hold several
//! values, and a value may itself be a composite. Two composites are equal
//! exactly when they hold the same pairs, whatever order they were given in
//! and however many times a pair was repeated.
//!
//! # Canonical bytes
//!
//! ```text
//! composite := count · names · fields
//! names     := name 00 …              every attribute the composite (and every
//!                                     composite nested in it) names, once,
//!                                     sorted, as a key writes an attribute
//! fields    := (index · type · value)* · 00
//!                                     a field names its attribute by its place
//!                                     in `names` (from 1; 0 ends the fields),
//!                                     sorted by index, then by its value's bytes
//! value     := the value as a key encodes it, every number in the one band;
//!              a nested composite is its fields, naming the outer `names`
//! count     := the number of names, as a LEB128 varint; so is an index
//! ```
//!
//! Each composite has exactly one encoding: [`Composite::from_bytes`] reads
//! the bytes back and refuses any that are not the encoding of what they
//! hold. Equal bytes are therefore equal composites, so equality and hashing
//! in memory compare bytes.
//!
//! # Identity
//!
//! A composite's [merkle reference](Composite::reference) is a map under
//! `composite:` from each attribute (its relation as a `symbol:`) to its
//! value's reference, or to the `set:` of its values' references when it
//! holds several. A nested composite is its own reference, so nothing of the
//! bytes' layout enters a composite's identity.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use merkle_reference::{Reference, Type};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser::SerializeSeq};

use crate::identity::{COMPOSITE, SYMBOL};
use crate::{
    DialogCorpusError, NUMBER_BAND, Relation, Value, ValueDataType, decode_bytes_cow, decode_value,
    encode_bytes, encode_key_value,
};

/// What ends a list of fields: the index no field has.
const END: u8 = 0;

/// A value made of attribute and value pairs, held as its canonical bytes.
#[derive(Clone)]
pub struct Composite(Arc<[u8]>);

/// A composite's content: its pairs, in canonical order, a nested
/// composite as its own content.
#[derive(Clone, Debug, PartialEq)]
struct Content(Vec<(Relation, Item)>);

#[derive(Clone, Debug, PartialEq)]
enum Item {
    Value(Value),
    Composite(Content),
}

impl Composite {
    /// The composite holding `fields`: each an attribute and a value, in any
    /// order, an attribute repeated for each value it holds. A value may be
    /// a composite. A pair given twice is held once, and so are two numbers
    /// that are one number (the natural `3` and the float `3.0`).
    pub fn new<I>(fields: I) -> Result<Self, DialogCorpusError>
    where
        I: IntoIterator<Item = (Relation, Value)>,
    {
        let mut pairs = Vec::new();
        for (attribute, value) in fields {
            pairs.push((attribute, item(value)?));
        }
        Ok(Self(encode(&Content(pairs)).into()))
    }

    /// The composite whose canonical bytes are `bytes`, refusing bytes that
    /// do not read as a composite or are not the canonical encoding of the
    /// one they read as.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DialogCorpusError> {
        let content = read(bytes).ok_or_else(|| invalid("bytes that do not read as one"))?;
        if encode(&content) != bytes {
            return Err(invalid("bytes that are not its canonical encoding"));
        }
        Ok(Self(bytes.into()))
    }

    /// The canonical bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Every pair, in canonical order: by attribute, then by value. A nested
    /// composite is given as a composite of its own.
    pub fn fields(&self) -> Vec<(Relation, Value)> {
        self.content()
            .0
            .into_iter()
            .map(|(attribute, item)| (attribute, item.into_value()))
            .collect()
    }

    /// The values `attribute` holds, in canonical order: none when the
    /// composite does not name it. Only the fields it holds are decoded.
    pub fn get(&self, attribute: &Relation) -> Vec<Value> {
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
                values.push(item.into_value());
            }
            at += len;
        }
        values
    }

    /// How many pairs it holds at its top level.
    pub fn len(&self) -> usize {
        self.content().0.len()
    }

    /// Whether it holds no pair.
    pub fn is_empty(&self) -> bool {
        self.0.as_ref() == [0, END]
    }

    /// Its identity: a map under `composite:` from each attribute to its
    /// value's reference, or to the `set:` of its values' references when
    /// it holds several.
    pub fn reference(&self) -> Reference {
        reference(&self.content())
    }

    /// How many bytes at the front of `bytes` a composite's canonical
    /// encoding takes, without checking it is canonical.
    pub fn encoded_len(bytes: &[u8]) -> Option<usize> {
        let (_, at) = header(bytes)?;
        Some(at + fields_len(&bytes[at..])?)
    }

    fn content(&self) -> Content {
        read(&self.0).expect("a composite holds canonical bytes")
    }
}

impl Item {
    fn into_value(self) -> Value {
        match self {
            Item::Value(value) => value,
            Item::Composite(content) => Value::Composite(Composite(encode(&content).into())),
        }
    }
}

fn invalid(what: &str) -> DialogCorpusError {
    DialogCorpusError::InvalidValue(format!("a composite cannot hold {what}"))
}

/// A value as a composite holds it: a nested composite as its content, and
/// no value a number of which no type holds (NaN).
fn item(value: Value) -> Result<Item, DialogCorpusError> {
    match value {
        Value::Composite(composite) => Ok(Item::Composite(composite.content())),
        Value::Float(float) if float.is_nan() => Err(invalid("NaN, which is not a value")),
        value => Ok(Item::Value(value)),
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

/// A varint at the front of `bytes`, and how many bytes it took. A varint
/// is minimal: no trailing zero group, so each number has one encoding.
fn read_varint(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut n = 0usize;
    for (at, &byte) in bytes.iter().enumerate().take(9) {
        n |= ((byte & 0x7f) as usize) << (7 * at);
        if byte & 0x80 == 0 {
            if at > 0 && byte == 0 {
                return None;
            }
            return Some((n, at + 1));
        }
    }
    None
}

/// The type byte a value is keyed under in a composite: every number under
/// the band's.
fn type_byte(value: &Value) -> u8 {
    match value {
        Value::UnsignedInt(_) | Value::SignedInt(_) | Value::Float(_) => {
            ValueDataType::Number as u8
        }
        other => other.data_type() as u8,
    }
}

fn names(content: &Content, into: &mut BTreeSet<Vec<u8>>) {
    for (attribute, item) in &content.0 {
        let mut name = Vec::with_capacity(attribute.as_str().len() + 1);
        encode_bytes(attribute.as_str().as_bytes(), &mut name);
        into.insert(name);
        if let Item::Composite(nested) = item {
            names(nested, into);
        }
    }
}

fn encode(content: &Content) -> Vec<u8> {
    let mut table = BTreeSet::new();
    names(content, &mut table);
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
    encode_fields(content, &index, &mut out);
    out
}

fn encode_fields(content: &Content, index: &BTreeMap<Vec<u8>, usize>, out: &mut Vec<u8>) {
    let mut fields: Vec<(usize, Vec<u8>)> = content
        .0
        .iter()
        .map(|(attribute, item)| {
            let mut name = Vec::new();
            encode_bytes(attribute.as_str().as_bytes(), &mut name);
            let mut slot = Vec::new();
            match item {
                Item::Value(value) => {
                    slot.push(type_byte(value));
                    encode_key_value(value, NUMBER_BAND, &mut slot);
                }
                Item::Composite(nested) => {
                    slot.push(ValueDataType::Composite as u8);
                    encode_fields(nested, index, &mut slot);
                }
            }
            (index[&name], slot)
        })
        .collect();
    fields.sort_unstable();
    fields.dedup();
    for (at, slot) in fields {
        varint(at, out);
        out.extend_from_slice(&slot);
    }
    out.push(END);
}

/// The names a composite's header lists, and where its fields start.
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

/// The length of one `0x00`-escaped, `0x00`-terminated string at the front
/// of `bytes`, its terminator included.
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

/// The length of a list of fields at the front of `bytes`, its end
/// included.
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

/// The length of a value slot (its type byte and its value) at the front
/// of `bytes`.
fn slot_len(bytes: &[u8]) -> Option<usize> {
    let (&kind, rest) = bytes.split_first()?;
    let len = if kind == ValueDataType::Number as u8 {
        dialog_number::encoded_len(rest)?
    } else if kind == ValueDataType::Composite as u8 {
        fields_len(rest)?
    } else if kind == ValueDataType::Boolean as u8 {
        1
    } else if [
        ValueDataType::Bytes,
        ValueDataType::Entity,
        ValueDataType::String,
        ValueDataType::Record,
        ValueDataType::Symbol,
    ]
    .iter()
    .any(|t| *t as u8 == kind)
    {
        terminated_len(rest)?
    } else {
        return None;
    };
    Some(1 + len)
}

fn read(bytes: &[u8]) -> Option<Content> {
    let (names, at) = header(bytes)?;
    let (content, used) = read_fields(&bytes[at..], &names)?;
    (at + used == bytes.len()).then_some(content)
}

fn read_fields(bytes: &[u8], names: &[&[u8]]) -> Option<(Content, usize)> {
    let mut pairs = Vec::new();
    let mut at = 0;
    loop {
        let (index, used) = read_varint(bytes.get(at..)?)?;
        at += used;
        if index == 0 {
            return Some((Content(pairs), at));
        }
        let name = names.get(index - 1)?;
        let (attribute, _) = decode_bytes_cow(name)?;
        let attribute = Relation::try_from(String::from_utf8(attribute.into_owned()).ok()?).ok()?;
        let len = slot_len(bytes.get(at..)?)?;
        pairs.push((attribute, read_slot(&bytes[at..at + len], names)?));
        at += len;
    }
}

fn read_slot(slot: &[u8], names: &[&[u8]]) -> Option<Item> {
    let (&kind, rest) = slot.split_first()?;
    if kind == ValueDataType::Composite as u8 {
        let (content, _) = read_fields(rest, names)?;
        return Some(Item::Composite(content));
    }
    let (value, left) = decode_value(ValueDataType::from(kind), rest)?;
    left.is_empty().then_some(Item::Value(value))
}

fn reference(content: &Content) -> Reference {
    let mut by_attribute: BTreeMap<&Relation, Vec<Reference>> = BTreeMap::new();
    for (attribute, item) in &content.0 {
        let value = match item {
            Item::Value(value) => value.reference(),
            Item::Composite(nested) => reference(nested),
        };
        by_attribute.entry(attribute).or_default().push(value);
    }
    COMPOSITE.map(by_attribute.into_iter().map(|(attribute, values)| {
        let key = SYMBOL.reference_of(attribute.as_str().as_bytes());
        let value = match values.as_slice() {
            [one] => *one,
            _ => Type::SET.set(values),
        };
        (key, value)
    }))
}

impl PartialEq for Composite {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Composite {}

impl Hash for Composite {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl PartialOrd for Composite {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Composite {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Debug for Composite {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_map()
            .entries(
                self.fields()
                    .iter()
                    .map(|(a, v)| (a.as_str().to_owned(), v.clone())),
            )
            .finish()
    }
}

/// A human-readable format writes a composite as its pairs, each an
/// attribute and a value, in canonical order (a list, since an attribute may
/// hold several values); a binary format writes its canonical bytes.
impl Serialize for Composite {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if !serializer.is_human_readable() {
            return serde_bytes::Bytes::new(&self.0).serialize(serializer);
        }
        let fields = self.fields();
        let mut seq = serializer.serialize_seq(Some(fields.len()))?;
        for (attribute, value) in &fields {
            seq.serialize_element(&(attribute.as_str(), value))?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Composite {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if !deserializer.is_human_readable() {
            let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
            return Composite::from_bytes(&bytes).map_err(de::Error::custom);
        }
        let pairs: Vec<(String, Value)> = Vec::deserialize(deserializer)?;
        let mut fields = Vec::with_capacity(pairs.len());
        for (attribute, value) in pairs {
            fields.push((
                Relation::try_from(attribute).map_err(de::Error::custom)?,
                value,
            ));
        }
        Composite::new(fields).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests;
