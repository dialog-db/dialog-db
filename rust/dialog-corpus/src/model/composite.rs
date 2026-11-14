//! A value made of values: a map or a list.
//!
//! A composite is kept whole as a [`Value::Record`]: one version byte,
//! then the dag-cbor of its parts.
//!
//! ```text
//! record    = version item
//! version   = 0x01
//! item      = { "map:":   [[part, part], ...] }     entries by their keys as written
//!           | { "list:":  [part, ...] }
//! part      = [type, bytes]                          a value, as a claim writes one
//! ```
//!
//! A part is a [`Value`] of any type, written as the history index
//! writes a claim's value: the byte of its type and the bytes
//! [`Value::to_bytes`] gives. A part that is itself a composite is a
//! record like any other, under its own version byte, so composites
//! nest as deep as they are written and a reader looks one level in at
//! a time.
//!
//! A number is kept as the number band keeps one: under the narrowest
//! type that holds it, a whole number as a natural when it is not
//! negative and as an integer when it is, and any other as a float. The
//! natural `3`, the integer `3` and the float `3.0` are one number with
//! one reference, and so one part with one byte string.
//!
//! The version byte is a CBOR unsigned integer below 24, so a versioned
//! record is two CBOR items in a row and never one. A record of before,
//! such as a revision's, is one dag-cbor item, so the two tell apart by
//! their first byte and length, and a record of before reads, compares
//! and is named as it always was. A record under a version this release
//! does not know is refused by its number when it is looked into, and
//! is otherwise carried as the bytes it is.
//!
//! A composite is named by the merkle reference of its parts
//! ([`Composite::reference`]), which no layout of its bytes moves. A map
//! holds no key twice and writes its entries in the order of their keys
//! as written, so one map has one byte string whatever order its
//! entries were given in.

use std::borrow::Cow;
use std::mem;
use std::ops::Deref;

use merkle_reference::{Reference, Type};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::{
    DialogCorpusError, NUMBER_BAND, Value, ValueDataType, decode_value, encode_key_value_owned,
};

/// The version byte this release writes in front of a composite.
pub const COMPOSITE_VERSION: u8 = 1;

/// The greatest byte that is a version: a CBOR unsigned integer of one
/// byte. A record whose first byte is above it is a record of before.
const GREATEST_VERSION: u8 = 0x17;

/// How deep [`Composite::reference`] follows composites nested in
/// composites. A part nested deeper is named as the record it is, by
/// its bytes.
///
/// It bounds the work of naming, not the stack: naming keeps the
/// composites it is inside of on the heap and uses the same stack
/// however deep a record nests. Each level followed reads what is
/// nested in it again, so a record of `n` bytes nested without a bound
/// would cost `n` times its depth to name. The depth is part of what a
/// name is: change it and a record nested deeper than the lesser of
/// the two has another name.
pub(crate) const NAMED_DEPTH: usize = 128;

/// Why bytes are not a composite, or parts do not make one.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CompositeError {
    /// The record is under a version this release does not read.
    #[error(
        "the record is in version {version}, and this release reads version {COMPOSITE_VERSION}"
    )]
    UnknownVersion {
        /// The version the record names.
        version: u8,
    },
    /// The bytes after the version byte are not a composite's.
    #[error("the record is not a composite: {0}")]
    Malformed(String),
    /// The bytes hold a composite, but not as this release writes it:
    /// a map's entries out of order, or a number under a wider type.
    #[error("the record holds a composite in bytes other than the ones it is written in")]
    NotCanonical,
    /// A map was given one key twice.
    #[error("a map holds no key twice, and {0:?} was given twice")]
    DuplicateKey(Value),
}

impl From<CompositeError> for DialogCorpusError {
    fn from(error: CompositeError) -> Self {
        DialogCorpusError::InvalidValue(error.to_string())
    }
}

/// `value` as a part is kept: a number under the narrowest type that
/// holds it, as the number band reads one back, and any other value as
/// it is.
fn kept(value: Value) -> Value {
    if !value.data_type().is_number() {
        return value;
    }
    let band = encode_key_value_owned(&value, NUMBER_BAND);
    match decode_value(ValueDataType::Number, &band) {
        Some((read, [])) => read,
        _ => value,
    }
}

/// A value made of values. See the [module](self) for how one is kept.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Composite {
    /// A map from a value to a value, holding no key twice.
    Map(CompositeMap),
    /// Values in order, of any number.
    List(CompositeItems),
}

/// The items of a list, each as a part is kept. Reads as a
/// slice of values.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CompositeItems(Vec<Value>);

impl CompositeItems {
    /// `items`, in order, each as a part is kept.
    pub fn new(items: impl IntoIterator<Item = Value>) -> Self {
        Self(items.into_iter().map(kept).collect())
    }

    /// The items, as the values they are.
    pub fn into_values(self) -> Vec<Value> {
        self.0
    }
}

impl Deref for CompositeItems {
    type Target = [Value];

    fn deref(&self) -> &[Value] {
        &self.0
    }
}

impl FromIterator<Value> for CompositeItems {
    fn from_iter<I: IntoIterator<Item = Value>>(items: I) -> Self {
        Self::new(items)
    }
}

/// The entries of a map, in the order of their keys as written.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CompositeMap {
    entries: Vec<Entry>,
}

/// One entry, with what orders it: its key as it is written.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Entry {
    order: Vec<u8>,
    key: Value,
    value: Value,
}

/// What orders a key among a map's entries and tells two keys apart:
/// the byte of its type, then its bytes. Two keys kept as parts are one
/// value exactly when these are equal.
fn order_of(key: &Value) -> Vec<u8> {
    let bytes = key.to_bytes();
    let mut order = Vec::with_capacity(bytes.len() + 1);
    order.push(u8::from(key.data_type()));
    order.extend_from_slice(&bytes);
    order
}

impl CompositeMap {
    /// The map of `entries`, in whatever order they come, each key and
    /// value as a part is kept. Refuses a key given twice: the natural
    /// `3` and the float `3.0` are one key.
    pub fn new(entries: impl IntoIterator<Item = (Value, Value)>) -> Result<Self, CompositeError> {
        let mut entries: Vec<Entry> = entries
            .into_iter()
            .map(|(key, value)| {
                let key = kept(key);
                Entry {
                    order: order_of(&key),
                    key,
                    value: kept(value),
                }
            })
            .collect();
        entries.sort_by(|a, b| a.order.cmp(&b.order));
        if let Some(pair) = entries
            .windows(2)
            .find(|pair| pair[0].order == pair[1].order)
        {
            return Err(CompositeError::DuplicateKey(pair[1].key.clone()));
        }
        Ok(Self { entries })
    }

    /// The value under `key`, if the map holds it.
    pub fn get(&self, key: &Value) -> Option<&Value> {
        let order = order_of(&kept(key.clone()));
        self.entries
            .binary_search_by(|entry| entry.order.cmp(&order))
            .ok()
            .map(|index| &self.entries[index].value)
    }

    /// Whether the map holds `key`.
    pub fn contains(&self, key: &Value) -> bool {
        self.get(key).is_some()
    }

    /// The entries, in the order they are written in.
    pub fn iter(&self) -> impl Iterator<Item = (&Value, &Value)> {
        self.entries.iter().map(|entry| (&entry.key, &entry.value))
    }

    /// How many entries the map holds.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map holds no entry.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// This map without the entries under `keys`.
    pub fn without<'a>(&self, keys: impl IntoIterator<Item = &'a Value>) -> Self {
        let dropped: Vec<Vec<u8>> = keys
            .into_iter()
            .map(|key| order_of(&kept(key.clone())))
            .collect();
        Self {
            entries: self
                .entries
                .iter()
                .filter(|entry| !dropped.contains(&entry.order))
                .cloned()
                .collect(),
        }
    }

    /// This map with `entries` added. Refuses a key the map already
    /// holds, or one given twice.
    pub fn with(
        &self,
        entries: impl IntoIterator<Item = (Value, Value)>,
    ) -> Result<Self, CompositeError> {
        Self::new(
            self.iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .chain(entries),
        )
    }
}

impl Composite {
    /// A map of `entries`, in whatever order they come. Refuses a key
    /// given twice.
    pub fn map(entries: impl IntoIterator<Item = (Value, Value)>) -> Result<Self, CompositeError> {
        CompositeMap::new(entries).map(Composite::Map)
    }

    /// A list of `items`.
    pub fn list(items: impl IntoIterator<Item = Value>) -> Self {
        Composite::List(CompositeItems::new(items))
    }

    /// The bytes this composite is kept as: the version byte, then the
    /// dag-cbor of its parts. One composite has one byte string.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![COMPOSITE_VERSION];
        let (shape, parts) = match self {
            Composite::Map(map) => (MAP, map.len()),
            Composite::List(items) => (LIST, items.len()),
        };
        write::head(write::MAP, 1, &mut bytes);
        write::head(write::TEXT, shape.len() as u64, &mut bytes);
        bytes.extend_from_slice(shape.as_bytes());
        write::head(write::ARRAY, parts as u64, &mut bytes);
        match self {
            Composite::Map(map) => {
                for entry in &map.entries {
                    write::head(write::ARRAY, 2, &mut bytes);
                    write::part(&entry.key, &mut bytes);
                    write::part(&entry.value, &mut bytes);
                }
            }
            Composite::List(items) => {
                for item in items.iter() {
                    write::part(item, &mut bytes);
                }
            }
        }
        bytes
    }

    /// The composite `bytes` hold. Refuses bytes under a version this
    /// release does not read, by its number, and bytes that are not the
    /// ones the composite they hold is written in.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, CompositeError> {
        match version_of(bytes) {
            Some(COMPOSITE_VERSION) => {}
            Some(version) => return Err(CompositeError::UnknownVersion { version }),
            None => {
                return Err(CompositeError::Malformed(
                    "it does not start with a version byte".into(),
                ));
            }
        }
        let Read(composite) = serde_ipld_dagcbor::from_slice(&bytes[1..])
            .map_err(|error| CompositeError::Malformed(error.to_string()))?;
        if composite.to_bytes() != bytes {
            return Err(CompositeError::NotCanonical);
        }
        Ok(composite)
    }

    /// The composite's name: the merkle reference of its parts. A map
    /// is named under [`Type::MAP`] and a list under [`Type::LIST`],
    /// each part by its own [`Value::reference`]. The bytes the composite is kept as are not
    /// part of it.
    pub fn reference(&self) -> Reference {
        self.reference_within(NAMED_DEPTH)
    }

    /// [`reference`](Self::reference), following composites nested in
    /// this one `depth` deep.
    ///
    /// A part that is a composite is named before the composite it is
    /// in, and the one it is in waits. What waits is kept in a list on
    /// the heap, innermost last, and not in a call for each level: a
    /// call for each held about ten kilobytes of stack, a record nested
    /// to the depth a name follows needed 1.4 megabytes of it, and a
    /// stack of one megabyte, which is what a wasm module has, ran out.
    fn reference_within(&self, depth: usize) -> Reference {
        let mut waiting: Vec<Naming<'_>> = Vec::new();
        let mut naming = Naming::of(Cow::Borrowed(self), depth);
        loop {
            // What is nested in this composite is followed one level
            // less deep, and at no depth left is named by its bytes.
            let below = naming.depth.saturating_sub(1);
            let step = match naming.next() {
                None => Step::Named,
                Some(part) => match (below > 0).then(|| part.composite()) {
                    Some(Ok(Some(nested))) => Step::Into(nested),
                    _ => Step::Part(part.scalar_reference()),
                },
            };
            match step {
                Step::Part(name) => naming.names.push(name),
                Step::Into(nested) => {
                    let outer = mem::replace(&mut naming, Naming::of(Cow::Owned(nested), below));
                    waiting.push(outer);
                }
                Step::Named => {
                    let name = naming.name();
                    match waiting.pop() {
                        Some(mut outer) => {
                            outer.names.push(name);
                            naming = outer;
                        }
                        None => return name,
                    }
                }
            }
        }
    }
}

/// A composite part-way through being named: the names of its first
/// parts, in the order the parts are kept in. A map's parts are each
/// entry's key and then its value.
struct Naming<'a> {
    composite: Cow<'a, Composite>,
    /// How deep the composites nested in this one are followed.
    depth: usize,
    names: Vec<Reference>,
}

/// What naming a composite does next.
enum Step {
    /// A part is named, by its bytes.
    Part(Reference),
    /// A part is a composite, to be named by its own parts first.
    Into(Composite),
    /// Every part is named, and so the composite can be.
    Named,
}

impl<'a> Naming<'a> {
    fn of(composite: Cow<'a, Composite>, depth: usize) -> Self {
        Self {
            composite,
            depth,
            names: Vec::new(),
        }
    }

    /// The part to name next, if any is left.
    fn next(&self) -> Option<&Value> {
        let at = self.names.len();
        match self.composite.as_ref() {
            Composite::Map(map) => {
                let entry = map.entries.get(at / 2)?;
                Some(if at.is_multiple_of(2) {
                    &entry.key
                } else {
                    &entry.value
                })
            }
            Composite::List(items) => items.get(at),
        }
    }

    /// The composite's name, from the names of all its parts.
    fn name(self) -> Reference {
        match self.composite.as_ref() {
            Composite::Map(_) => {
                let (entries, _) = self.names.as_chunks::<2>();
                Type::MAP.map(entries.iter().map(|[key, value]| (*key, *value)))
            }
            Composite::List(_) => Type::LIST.list(self.names),
        }
    }
}

/// The version a record names: its first byte, when that byte is a
/// version and a composite follows it. `None` for a record of before.
fn version_of(bytes: &[u8]) -> Option<u8> {
    match bytes {
        [version, _, ..] if (1..=GREATEST_VERSION).contains(version) => Some(*version),
        _ => None,
    }
}

impl Value {
    /// The composite this value is, when it is one: `None` for a
    /// scalar and for a record of before, which starts with no version
    /// byte. A record under a version this release does not read is
    /// refused by its number, and one under this release's version
    /// whose bytes are not a composite's is refused as malformed.
    pub fn composite(&self) -> Result<Option<Composite>, CompositeError> {
        match self {
            Value::Record(bytes) if version_of(bytes).is_some() => {
                Composite::try_from_bytes(bytes).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// Whether this value is a record that starts with a version byte:
    /// a composite of this release's, or a record of a later one.
    pub fn is_versioned_record(&self) -> bool {
        matches!(self, Value::Record(bytes) if version_of(bytes).is_some())
    }

    /// [`Value::reference`], following composites nested in composites
    /// `depth` deep.
    pub(crate) fn reference_within(&self, depth: usize) -> Reference {
        if depth > 0
            && let Ok(Some(composite)) = self.composite()
        {
            return composite.reference_within(depth);
        }
        self.scalar_reference()
    }
}

impl From<&Composite> for Value {
    fn from(composite: &Composite) -> Self {
        Value::Record(composite.to_bytes())
    }
}

impl From<Composite> for Value {
    fn from(composite: Composite) -> Self {
        Value::from(&composite)
    }
}

impl TryFrom<&Value> for Composite {
    type Error = CompositeError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.composite()?.ok_or_else(|| {
            CompositeError::Malformed("it does not start with a version byte".into())
        })
    }
}

/// A value as a `(type, bytes)` pair: the byte of its type and the
/// bytes [`Value::to_bytes`] gives. It is how the history index writes
/// a claim's value and how a composite writes a part. serde's untagged
/// spelling of a value cannot round-trip every variant (an unsigned
/// integer resurfaces as a float), and dag-cbor holds no integer past
/// 64 bits, where this holds every value.
pub(crate) mod type_and_bytes {
    use super::*;

    pub fn serialize<S: Serializer>(value: &Value, serializer: S) -> Result<S::Ok, S::Error> {
        let data_type: u8 = value.data_type().into();
        (data_type, serde_bytes::Bytes::new(&value.to_bytes())).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
        let (data_type, bytes): (u8, serde_bytes::ByteBuf) =
            Deserialize::deserialize(deserializer)?;
        Value::try_from((ValueDataType::from(data_type), bytes.into_vec()))
            .map_err(D::Error::custom)
    }
}

/// The name a map's parts are written under.
const MAP: &str = "map:";
/// The name a list's parts are written under.
const LIST: &str = "list:";

/// The dag-cbor a composite is written as. Written by hand, since
/// nothing here can fail: a composite is text, arrays and `(type,
/// bytes)` pairs, whatever its values are.
mod write {
    use crate::Value;

    pub const UNSIGNED: u8 = 0;
    pub const BYTES: u8 = 2;
    pub const TEXT: u8 = 3;
    pub const ARRAY: u8 = 4;
    pub const MAP: u8 = 5;

    /// The head of an item of `major` type: its argument in the fewest
    /// bytes that hold it, as dag-cbor requires.
    pub fn head(major: u8, argument: u64, out: &mut Vec<u8>) {
        let major = major << 5;
        if argument < 24 {
            out.push(major | argument as u8);
        } else if let Ok(argument) = u8::try_from(argument) {
            out.push(major | 24);
            out.push(argument);
        } else if let Ok(argument) = u16::try_from(argument) {
            out.push(major | 25);
            out.extend_from_slice(&argument.to_be_bytes());
        } else if let Ok(argument) = u32::try_from(argument) {
            out.push(major | 26);
            out.extend_from_slice(&argument.to_be_bytes());
        } else {
            out.push(major | 27);
            out.extend_from_slice(&argument.to_be_bytes());
        }
    }

    /// A part: the pair of its type's byte and its bytes.
    pub fn part(value: &Value, out: &mut Vec<u8>) {
        let bytes = value.to_bytes();
        head(ARRAY, 2, out);
        head(UNSIGNED, u64::from(u8::from(value.data_type())), out);
        head(BYTES, bytes.len() as u64, out);
        out.extend_from_slice(&bytes);
    }
}

/// A part as it is read. The type byte is one a stored value has: the
/// lossy fallback [`ValueDataType::from`] gives an unknown byte would
/// read a later release's type as bytes.
struct Owned(Value);

impl<'de> Deserialize<'de> for Owned {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (data_type, bytes): (u8, serde_bytes::ByteBuf) =
            Deserialize::deserialize(deserializer)?;
        if data_type > ValueDataType::Symbol as u8 {
            return Err(D::Error::custom(format!(
                "{data_type} is not the type of a stored value"
            )));
        }
        Value::try_from((ValueDataType::from(data_type), bytes.into_vec()))
            .map(Owned)
            .map_err(D::Error::custom)
    }
}

/// A composite as it is read from under the version byte.
struct Read(Composite);

#[derive(Deserialize)]
enum Stored {
    #[serde(rename = "map:")]
    Map(Vec<(Owned, Owned)>),
    #[serde(rename = "list:")]
    List(Vec<Owned>),
}

impl<'de> Deserialize<'de> for Read {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let values = |parts: Vec<Owned>| parts.into_iter().map(|Owned(value)| value);
        Ok(Read(match Stored::deserialize(deserializer)? {
            Stored::Map(entries) => Composite::map(
                entries
                    .into_iter()
                    .map(|(Owned(key), Owned(value))| (key, value)),
            )
            .map_err(D::Error::custom)?,
            Stored::List(items) => Composite::list(values(items)),
        }))
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use super::*;
    use crate::history::{Edition, Origin, REVISION_RECORD_FORMAT, RevisionRecord, Version};
    use crate::identity::RECORD;
    use crate::{Entity, Relation, encode_value_owned};
    use serde::de::IgnoredAny;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn text(text: &str) -> Value {
        Value::String(text.into())
    }

    /// A part as serde writes it, for bytes written another way than
    /// [`Composite::to_bytes`] writes them.
    struct Part<'a>(&'a Value);

    impl Serialize for Part<'_> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            type_and_bytes::serialize(self.0, serializer)
        }
    }

    #[derive(Serialize)]
    enum Written<'a> {
        #[serde(rename = "map:")]
        Map(Vec<(Part<'a>, Part<'a>)>),
        #[serde(rename = "list:")]
        List(Vec<Part<'a>>),
    }

    /// The bytes written by hand are the dag-cbor serde writes for the
    /// same shape, so a dag-cbor reader reads them.
    #[dialog_common::test]
    fn it_writes_the_dag_cbor_serde_writes() {
        let long = Value::Bytes(vec![7; 70_000]);
        let values = every_value();
        let list = Composite::list(values.iter().chain([&long]).cloned());
        let written = Written::List(values.iter().chain([&long]).map(Part).collect());
        let mut expected = vec![COMPOSITE_VERSION];
        serde_ipld_dagcbor::to_writer(&mut expected, &written).expect("encodes");
        assert_eq!(list.to_bytes(), expected);

        let map = Composite::map([(text("a"), long.clone())]).expect("a map");
        let mut expected = vec![COMPOSITE_VERSION];
        serde_ipld_dagcbor::to_writer(
            &mut expected,
            &Written::Map(vec![(Part(&text("a")), Part(&long))]),
        )
        .expect("encodes");
        assert_eq!(map.to_bytes(), expected);
    }

    /// A value of every type, the ones dag-cbor's own kinds do not hold
    /// among them.
    fn every_value() -> Vec<Value> {
        vec![
            Value::Bytes(vec![0, 1, 255]),
            Value::Entity("id:alice".parse::<Entity>().expect("an entity")),
            Value::Boolean(true),
            text("hello"),
            Value::UnsignedInt(u128::MAX),
            Value::SignedInt(i128::MIN),
            Value::Float(-0.5),
            Value::Float(f64::INFINITY),
            Value::Record(vec![0xa1, 0x61, 0x61, 0x01]),
            Value::Symbol("note/text".parse::<Relation>().expect("a relation")),
        ]
    }

    /// A revision record signed by a fixed key, as a release before
    /// composites wrote it: these are the bytes of its `Value::Record`.
    const RECORD_OF_BEFORE: &str = concat!(
        "a765736b69707381a2666f726967696e582003030303030303030303030303030303030303030303",
        "030303030303030303036765646974696f6e04666272616e63686969643a6272616e636866666f72",
        "6d6174006669737375657278386469643a6b65793a7a364d6b764471475435346358657359477641",
        "42704631556170564e776a43715263616669345078365468763554335a67706172656e747381a266",
        "6f726967696e58200303030303030303030303030303030303030303030303030303030303030303",
        "6765646974696f6e0469617574686f7269747978386469643a6b65793a7a364d6b76447147543534",
        "635865735947764142704631556170564e776a43715263616669345078365468763554335a697369",
        "676e617475726558402115788573d03b9a2784796530808cbb69118d2825fcf2cdf2148a84b0c636",
        "f04312cb04e5672c51360758d285a13eb1facd29afb113c4135fa2a2d6e0c76305",
    );

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn unhex(digits: &str) -> Vec<u8> {
        (0..digits.len() / 2)
            .map(|at| u8::from_str_radix(&digits[2 * at..2 * at + 2], 16).expect("hex"))
            .collect()
    }

    /// The record [`RECORD_OF_BEFORE`] holds, built and signed again.
    fn revision_record() -> RevisionRecord {
        use base58::ToBase58 as _;
        use ed25519_dalek::Signer as _;

        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let issuer = {
            let mut bytes = vec![0xed, 0x01];
            bytes.extend_from_slice(key.verifying_key().as_bytes());
            format!("did:key:z{}", bytes.to_base58())
        };
        let parent = Version::new(Origin::from([3u8; 32]), Edition::new(4));
        let mut record = RevisionRecord {
            format: REVISION_RECORD_FORMAT,
            branch: "id:branch".parse().expect("an entity"),
            issuer: issuer.clone(),
            authority: issuer,
            parents: vec![parent],
            skips: vec![parent],
            signature: Vec::new(),
        };
        record.signature = key
            .sign(&record.payload().expect("a payload"))
            .to_bytes()
            .to_vec();
        record
    }

    /// A list and a map of every type of value come back as they were
    /// written.
    #[dialog_common::test]
    fn it_round_trips_every_shape_over_every_value() {
        let list = Composite::list(every_value());
        let map = Composite::map(
            every_value()
                .into_iter()
                .zip(every_value().into_iter().rev()),
        )
        .expect("no key twice");
        for composite in [list, map] {
            let value = Value::from(&composite);
            assert!(matches!(value, Value::Record(_)));
            assert_eq!(value.composite(), Ok(Some(composite)));
        }
    }

    /// A composite nested in a composite is a part like any other, and
    /// is read a level at a time.
    #[dialog_common::test]
    fn it_nests_as_deep_as_written() {
        let inner = Composite::map([(text("x"), Value::UnsignedInt(1))]).expect("a map");
        let middle = Composite::list([Value::from(&inner), text("beside")]);
        let outer = Composite::list([Value::from(&middle), Value::Boolean(false)]);

        let Some(Composite::List(items)) = Value::from(&outer).composite().expect("reads") else {
            panic!("a list");
        };
        let Some(Composite::List(items)) = items[0].composite().expect("reads") else {
            panic!("a list");
        };
        assert_eq!(items[0].composite(), Ok(Some(inner)));
        assert_eq!(items[1], text("beside"));
    }

    /// Two maps given in two orders are one value: equal, with one
    /// name and one byte string.
    #[dialog_common::test]
    fn it_keeps_a_map_in_one_order_whatever_order_it_was_given_in() {
        let entries = vec![
            (text("title"), text("Buy milk")),
            (text("done"), Value::Boolean(false)),
            (Value::UnsignedInt(7), text("seven")),
            (
                Value::Symbol("note/text".parse().expect("a relation")),
                Composite::list([text("a"), text("b")]).into(),
            ),
        ];
        let forward = Composite::map(entries.clone()).expect("a map");
        let backward = Composite::map(entries.into_iter().rev()).expect("a map");

        assert_eq!(forward, backward);
        assert_eq!(forward.reference(), backward.reference());
        assert_eq!(forward.to_bytes(), backward.to_bytes());
        assert_eq!(Value::from(&forward), Value::from(&backward));
        assert_eq!(
            Value::from(&forward).reference(),
            Value::from(&backward).reference()
        );
    }

    /// A map refuses a key given twice, and two spellings of one number
    /// are one key.
    #[dialog_common::test]
    fn it_refuses_a_key_given_twice() {
        let twice = Composite::map([(text("a"), text("x")), (text("a"), text("y"))]);
        assert_eq!(twice, Err(CompositeError::DuplicateKey(text("a"))));

        let one_number = Composite::map([
            (Value::UnsignedInt(3), text("x")),
            (Value::Float(3.0), text("y")),
        ]);
        assert_eq!(
            one_number,
            Err(CompositeError::DuplicateKey(Value::UnsignedInt(3)))
        );
    }

    /// A number is kept under the narrowest type that holds it, as the
    /// number band reads one back, so one number is one part with one
    /// byte string whatever type it was given in. Bytes that hold it
    /// under a wider type are not the ones the composite is written in.
    #[dialog_common::test]
    fn it_keeps_a_number_as_the_number_band_reads_it() {
        let three = Composite::list([Value::UnsignedInt(3)]);
        for spelling in [Value::SignedInt(3), Value::Float(3.0)] {
            let other = Composite::list([spelling]);
            assert_eq!(other, three);
            assert_eq!(other.to_bytes(), three.to_bytes());
            assert_eq!(other.reference(), three.reference());
        }
        let Composite::List(items) = Composite::list([
            Value::Float(-2.0),
            Value::Float(2.5),
            Value::Float(-0.0),
            Value::Float(1e30),
            Value::Float(f64::NEG_INFINITY),
            Value::UnsignedInt(u128::MAX),
        ]) else {
            panic!("a list");
        };
        assert_eq!(
            items.to_vec(),
            vec![
                Value::SignedInt(-2),
                Value::Float(2.5),
                Value::UnsignedInt(0),
                Value::UnsignedInt(10u128.pow(30)),
                Value::Float(f64::NEG_INFINITY),
                Value::UnsignedInt(u128::MAX),
            ]
        );

        let map = Composite::map([(Value::Float(3.0), Value::SignedInt(4))]).expect("a map");
        let Composite::Map(entries) = &map else {
            panic!("a map");
        };
        assert_eq!(
            entries.get(&Value::SignedInt(3)),
            Some(&Value::UnsignedInt(4))
        );

        let wide = Value::Float(3.0);
        let mut bytes = vec![COMPOSITE_VERSION];
        serde_ipld_dagcbor::to_writer(&mut bytes, &Written::List(vec![Part(&wide)]))
            .expect("encodes");
        assert_eq!(
            Composite::try_from_bytes(&bytes),
            Err(CompositeError::NotCanonical)
        );
    }

    /// A map finds a value by its key, and gives and takes entries.
    #[dialog_common::test]
    fn it_reads_a_map_by_key() {
        let map = CompositeMap::new([(text("a"), Value::UnsignedInt(1)), (text("b"), text("two"))])
            .expect("a map");
        assert_eq!(map.get(&text("a")), Some(&Value::UnsignedInt(1)));
        assert_eq!(map.get(&text("c")), None);
        assert_eq!(map.len(), 2);

        let rest = map.without([&text("a")]);
        assert_eq!(
            rest.iter().collect::<Vec<_>>(),
            vec![(&text("b"), &text("two"))]
        );
        assert_eq!(
            rest.with([(text("a"), Value::UnsignedInt(1))]),
            Ok(map.clone())
        );
        assert_eq!(
            map.with([(text("a"), Value::UnsignedInt(9))]),
            Err(CompositeError::DuplicateKey(text("a")))
        );
    }

    /// A composite is named by its parts: a map under the map type and
    /// a list under the list type, each part by its own reference. The name is not a hash of the bytes.
    #[dialog_common::test]
    fn it_names_a_composite_by_its_parts() {
        let parts = [text("a"), Value::UnsignedInt(2)];
        let references = || parts.iter().map(Value::reference);

        let list = Composite::list(parts.clone());
        assert_eq!(list.reference(), Type::LIST.list(references()));
        assert_eq!(Value::from(&list).reference(), list.reference());
        assert_ne!(list.reference(), RECORD.reference_of(&list.to_bytes()));

        let map = Composite::map([(parts[0].clone(), parts[1].clone())]).expect("a map");
        assert_eq!(
            map.reference(),
            Type::MAP.map([(parts[0].reference(), parts[1].reference())])
        );

        // A part that is a composite goes in by its own name, so a part
        // held inline and the same part known by its reference give the
        // parent one name.
        let nested = Composite::list([Value::from(&list), text("z")]);
        assert_eq!(
            nested.reference(),
            Type::LIST.list([list.reference(), text("z").reference()])
        );
        // One number is one part, whatever type holds it.
        assert_eq!(
            Composite::list([Value::UnsignedInt(3)]).reference(),
            Composite::list([Value::Float(3.0)]).reference()
        );
    }

    /// A revision's record is a bare dag-cbor map. One written before
    /// composites starts with no version byte, so it reads as the record
    /// it was written as, verifies, is named by its bytes as before, and
    /// keeps the bytes and the key it had.
    #[dialog_common::test]
    fn it_reads_a_record_of_before_as_before() {
        assert_eq!(
            hex(&revision_record().to_bytes().expect("encodes")),
            RECORD_OF_BEFORE,
            "a revision record is still written as it was"
        );
        let bytes = unhex(RECORD_OF_BEFORE);
        assert_eq!(bytes[0], 0xa7, "a revision record is a map of seven fields");

        let value = Value::Record(bytes.clone());
        assert!(!value.is_versioned_record());
        assert_eq!(value.composite(), Ok(None));
        assert_eq!(value.reference(), RECORD.reference_of(&bytes));
        assert_eq!(value.to_bytes(), bytes);

        let record = RevisionRecord::try_from_bytes(&value.to_bytes()).expect("reads");
        assert_eq!(record, revision_record());
        record
            .verify(&record.version())
            .expect("the record verifies at its own version");
        let fact = record.to_fact(&record.version()).expect("a fact");
        assert_eq!(fact.is, value, "the fact holds the bytes it held");

        // Equality, ordering and the key encoding go on treating a
        // record as its bytes, a composite's included.
        let composite = Value::from(Composite::list([text("a")]));
        let Value::Record(written) = &composite else {
            panic!("a record");
        };
        assert_eq!(composite, Value::Record(written.clone()));
        assert_eq!(
            encode_value_owned(&composite),
            encode_value_owned(&Value::Bytes(written.clone()))
        );
        assert_eq!(composite.to_bytes(), *written);
    }

    /// No dag-cbor item starts a versioned record: a version byte is a
    /// whole item, and something follows it. A record of one byte, and
    /// one whose first byte is no version, are records of before.
    #[dialog_common::test]
    fn it_tells_a_versioned_record_from_one_of_before() {
        for bytes in [
            vec![],
            vec![COMPOSITE_VERSION],
            vec![0x00, 0x01],
            vec![0x18, 0x18],
            vec![0xa0],
            vec![0x80],
            vec![0xff, 0x00, 0x13],
        ] {
            let value = Value::Record(bytes.clone());
            assert!(!value.is_versioned_record(), "{bytes:?}");
            assert_eq!(value.composite(), Ok(None), "{bytes:?}");
            assert_eq!(value.reference(), RECORD.reference_of(&bytes), "{bytes:?}");
        }
        // A whole dag-cbor item never has anything after its first item.
        let versioned = Composite::list([text("a")]).to_bytes();
        assert!(
            serde_ipld_dagcbor::from_slice::<IgnoredAny>(&versioned).is_err(),
            "a versioned record is not one dag-cbor item"
        );
    }

    /// A reader refuses a version it does not know, by its number, and
    /// goes on carrying the record as its bytes, named by them.
    #[dialog_common::test]
    fn it_refuses_a_version_it_does_not_know_by_its_number() {
        let mut bytes = Composite::list([text("a")]).to_bytes();
        bytes[0] = 2;
        let later = Value::Record(bytes.clone());

        assert!(later.is_versioned_record());
        assert_eq!(
            later.composite(),
            Err(CompositeError::UnknownVersion { version: 2 })
        );
        assert_eq!(
            Composite::try_from_bytes(&bytes),
            Err(CompositeError::UnknownVersion { version: 2 })
        );
        let reason = CompositeError::UnknownVersion { version: 2 }.to_string();
        assert!(reason.contains("version 2"), "{reason}");
        assert_eq!(later.reference(), RECORD.reference_of(&bytes));
        assert_eq!(later.to_bytes(), bytes);
    }

    /// Bytes under this release's version that are not a composite's
    /// are refused when looked into, and named as the record they are.
    #[dialog_common::test]
    fn it_refuses_bytes_that_are_not_a_composite() {
        let opaque = Value::Record(vec![COMPOSITE_VERSION, 2, 3]);
        assert!(matches!(
            opaque.composite(),
            Err(CompositeError::Malformed(_))
        ));
        assert_eq!(opaque.reference(), RECORD.reference_of(&[1, 2, 3]));
    }

    /// One composite has one byte string: a map whose entries are
    /// written out of order, and a map with a key twice, are refused.
    #[dialog_common::test]
    fn it_refuses_bytes_a_composite_is_not_written_in() {
        let map = Composite::map([(text("a"), text("x")), (text("b"), text("y"))]).expect("a map");
        let Composite::Map(entries) = &map else {
            panic!("a map");
        };
        let mut swapped = vec![COMPOSITE_VERSION];
        serde_ipld_dagcbor::to_writer(
            &mut swapped,
            &Written::Map(
                entries
                    .entries
                    .iter()
                    .rev()
                    .map(|entry| (Part(&entry.key), Part(&entry.value)))
                    .collect(),
            ),
        )
        .expect("encodes");
        assert_ne!(swapped, map.to_bytes());
        assert_eq!(
            Composite::try_from_bytes(&swapped),
            Err(CompositeError::NotCanonical)
        );
        assert_eq!(
            Value::Record(swapped.clone()).reference(),
            RECORD.reference_of(&swapped)
        );

        let mut twice = vec![COMPOSITE_VERSION];
        let (key, value) = (text("a"), text("x"));
        serde_ipld_dagcbor::to_writer(
            &mut twice,
            &Written::Map(vec![(Part(&key), Part(&value)), (Part(&key), Part(&value))]),
        )
        .expect("encodes");
        assert!(matches!(
            Composite::try_from_bytes(&twice),
            Err(CompositeError::Malformed(_))
        ));
    }

    /// A part's type is one a stored value has.
    #[dialog_common::test]
    fn it_refuses_a_part_of_no_stored_type() {
        let mut bytes = vec![COMPOSITE_VERSION];
        #[derive(Serialize)]
        enum Foreign {
            #[serde(rename = "list:")]
            List(Vec<(u8, serde_bytes::ByteBuf)>),
        }
        serde_ipld_dagcbor::to_writer(
            &mut bytes,
            &Foreign::List(vec![(200, serde_bytes::ByteBuf::from(vec![1]))]),
        )
        .expect("encodes");
        assert!(matches!(
            Composite::try_from_bytes(&bytes),
            Err(CompositeError::Malformed(_))
        ));
    }

    /// A list of one item nested `nesting` deep around a text.
    fn nested(nesting: usize) -> Value {
        let mut value = text("leaf");
        for _ in 0..nesting {
            value = Composite::list([value]).into();
        }
        value
    }

    /// Naming a record nested past the depth a name follows runs on the
    /// stack a test is given on every target, which on wasm is the one
    /// megabyte a module has: the composites a part is nested in wait
    /// on the heap, not in a call each. A call each needed 1.4
    /// megabytes for this record and ran out there.
    #[dialog_common::test]
    fn it_names_a_record_nested_past_any_depth() {
        let _ = nested(NAMED_DEPTH * 4).reference();
    }

    /// The stack naming uses does not grow with how deep a record
    /// nests: a record nested four times the depth a name follows is
    /// named on a thread given a quarter of a megabyte, a fifth of
    /// what a call a level needed, and has the name it has anywhere.
    ///
    /// Native only: a wasm test has no thread to give a stack to. There
    /// the bound is the module's own stack, and
    /// `it_names_a_record_nested_past_any_depth` names the same record
    /// under it.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn it_names_a_deep_record_on_a_small_stack() {
        use std::thread::Builder;

        let value = nested(NAMED_DEPTH * 4);
        let name = value.reference();
        let named = Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || value.reference())
            .expect("a thread")
            .join()
            .expect("the record is named");
        assert_eq!(named, name);
    }

    /// A record nested in records is named by its parts down to the
    /// depth a name follows, and what is nested deeper goes in by its
    /// bytes. The names are the ones a call a level gave (43d49a5), so
    /// naming without one moved no name.
    #[dialog_common::test]
    fn it_names_a_nested_record_by_its_parts_down_to_a_depth() {
        let named = |nesting: usize| {
            // The part the last level followed holds: named as any
            // value is where the nesting ends above the depth, and by
            // its bytes where it goes on below.
            let (levels, mut name) = if nesting < NAMED_DEPTH {
                (nesting, text("leaf").reference())
            } else {
                let below = nested(nesting - NAMED_DEPTH);
                (NAMED_DEPTH, below.scalar_reference())
            };
            for _ in 0..levels {
                name = Type::LIST.list([name]);
            }
            name
        };
        for (nesting, name) in [
            (
                1,
                "8c836a04ccd2cd271fc632e0cac1909876050be1099bd3e30aaca0ebda433f04",
            ),
            (
                2,
                "579fb308b3511aebcb1d60c1a29f22f3af5c82aedf9357ae45dd1107ac5476ca",
            ),
            (
                NAMED_DEPTH - 1,
                "5f8730f430b77da56d183af269798e61b60f4575ae57bd0f0bc79f988c188970",
            ),
            (
                NAMED_DEPTH,
                "12fe0412bc22863ce46468770690a9bfddd30ff036f3ab25986f4e3b651f70be",
            ),
            (
                NAMED_DEPTH + 1,
                "3ac3da0bfc3557f3a12a5c67e3f8a9e23f9b14ce32d946cdb0e5cc79bb2d831d",
            ),
            (
                NAMED_DEPTH * 4,
                "bfa65cda9f6779faddaeaea2d89389b1a3d894803ceab8a05af9b596f04fcf9e",
            ),
        ] {
            let value = nested(nesting);
            assert_eq!(value.reference(), named(nesting), "nesting {nesting}");
            assert_eq!(
                value.reference(),
                Reference::from_hex(name),
                "nesting {nesting}"
            );
        }
        // At the depth the leaf is still named as a text; one deeper it
        // is inside a record named by its bytes, which is another name.
        assert_ne!(
            nested(NAMED_DEPTH + 1).reference(),
            {
                let mut name = text("leaf").reference();
                for _ in 0..=NAMED_DEPTH {
                    name = Type::LIST.list([name]);
                }
                name
            },
            "what is nested past the depth is not named by its parts"
        );
    }

    /// Composites nested in composites are named from the inside out,
    /// each by the names of its parts: a map by each entry's key and
    /// value, whatever order they were given in, and a list by its
    /// items in order.
    #[dialog_common::test]
    fn it_names_composites_nested_in_composites_by_their_parts() {
        let pair = Composite::list([text("t"), Value::Boolean(false)]);
        let map = Composite::map([
            (text("k"), Value::UnsignedInt(1)),
            (text("j"), Value::from(&pair)),
        ])
        .expect("a map");
        let list = Composite::list([Value::from(&map), text("z"), Value::from(&map)]);
        let outer = Composite::list([Value::from(&list), Value::Boolean(true)]);

        let pair_name = Type::LIST.list([text("t").reference(), Value::Boolean(false).reference()]);
        assert_eq!(pair.reference(), pair_name);
        let map_name = Type::MAP.map([
            (text("j").reference(), pair_name),
            (text("k").reference(), Value::UnsignedInt(1).reference()),
        ]);
        assert_eq!(map.reference(), map_name);
        let list_name = Type::LIST.list([map_name, text("z").reference(), map_name]);
        assert_eq!(list.reference(), list_name);
        let outer_name = Type::LIST.list([list_name, Value::Boolean(true).reference()]);
        assert_eq!(outer.reference(), outer_name);
        assert_eq!(Value::from(&outer).reference(), outer_name);
    }
}
