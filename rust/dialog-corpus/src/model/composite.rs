//! A value made of values: a map or a list.
//!
//! A composite is kept whole as a [`Value::Record`]: one version byte,
//! then one dag-cbor item, a map or a list.
//!
//! ```text
//! record    = version item
//! version   = 0x01
//! item      = { text: part, ... }          a map, its keys text
//!           | [ part, ... ]                a list
//! part      = text | bytes | true | false
//!           | an integer                   a whole number dag-cbor holds
//!           | a float                      a number that is not whole
//!           | item                         a composite, nested
//!           | a link                       any other value, inline
//! ```
//!
//! What follows the version byte is plain dag-cbor, so anything that
//! reads dag-cbor reads a composite. A value dag-cbor has a kind for is
//! written as that kind. A value it has none for is written as a link
//! that holds the value itself: a CID whose hash is the identity, over
//! the bytes [`Value::to_bytes`] gives, and whose codec says the
//! value's type ([`VALUE_CODEC`]). An entity and a symbol are
//! written so, and so is a whole number past the 64 bits a dag-cbor
//! integer holds, a float that is not finite, and a record that is no
//! composite.
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
//! holds no key twice and writes its entries in the order dag-cbor
//! gives keys, the shorter first and then by their bytes, so one map
//! has one byte string whatever order its entries were given in.

use std::borrow::Cow;
use std::cmp::Ordering;
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

/// The first of the codecs of the link a value is written as when
/// dag-cbor has no kind for it, in the range multicodec keeps for
/// private use. The codec of a value's link is this and the byte of the
/// value's type, and what the link holds is the bytes
/// [`Value::to_bytes`] gives. The type is in the codec and not in what
/// is held so that the longest symbol, of 64 bytes, is a link of the 64
/// bytes the links of the libraries hold.
pub const VALUE_CODEC: u32 = 0x30_d1a0;

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
    /// a map's entries out of order, a length in more bytes than it
    /// needs, a number under a wider type, or a value written as a link
    /// that dag-cbor has a kind for.
    #[error("the record holds a composite in bytes other than the ones it is written in")]
    NotCanonical,
    /// A map was given one key twice.
    #[error("a map holds no key twice, and {0:?} was given twice")]
    DuplicateKey(String),
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
    /// A map from a text to a value, holding no key twice.
    Map(CompositeMap),
    /// Values in order, of any number.
    List(CompositeItems),
}

/// The items of a list, each as a part is kept. Reads as a
/// slice of values.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CompositeItems(Vec<Value>);

impl CompositeItems {
    /// The items `items` are, each as a part is kept.
    pub fn new(items: impl IntoIterator<Item = Value>) -> Self {
        Self(items.into_iter().map(kept).collect())
    }

    /// The values, in order.
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

/// The entries of a map, in the order they are written in: by key, the
/// shorter first and then by their bytes, each value as a part is kept.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CompositeMap {
    entries: Vec<(String, Value)>,
}

/// The order dag-cbor writes a map's keys in: the shorter first, and
/// two of one length by their bytes.
fn order(left: &str, right: &str) -> Ordering {
    left.len()
        .cmp(&right.len())
        .then_with(|| left.as_bytes().cmp(right.as_bytes()))
}

impl CompositeMap {
    /// The map of `entries`, in whatever order they come, each value as
    /// a part is kept. Refuses a key given twice.
    pub fn new<K: Into<String>>(
        entries: impl IntoIterator<Item = (K, Value)>,
    ) -> Result<Self, CompositeError> {
        let mut entries: Vec<(String, Value)> = entries
            .into_iter()
            .map(|(key, value)| (key.into(), kept(value)))
            .collect();
        entries.sort_by(|(left, _), (right, _)| order(left, right));
        if let Some(pair) = entries.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(CompositeError::DuplicateKey(pair[1].0.clone()));
        }
        Ok(Self { entries })
    }

    /// The value under `key`, if the map holds it.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .binary_search_by(|(held, _)| order(held, key))
            .ok()
            .map(|index| &self.entries[index].1)
    }

    /// Whether the map holds `key`.
    pub fn contains(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// The entries, in the order they are written in.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_str(), value))
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
    pub fn without<'a>(&self, keys: impl IntoIterator<Item = &'a str>) -> Self {
        let dropped: Vec<&str> = keys.into_iter().collect();
        Self {
            entries: self
                .entries
                .iter()
                .filter(|(key, _)| !dropped.contains(&key.as_str()))
                .cloned()
                .collect(),
        }
    }

    /// This map with `entries` added. Refuses a key the map already
    /// holds, or one given twice.
    pub fn with<K: Into<String>>(
        &self,
        entries: impl IntoIterator<Item = (K, Value)>,
    ) -> Result<Self, CompositeError> {
        Self::new(
            self.entries
                .iter()
                .cloned()
                .chain(entries.into_iter().map(|(key, value)| (key.into(), value))),
        )
    }
}

impl Composite {
    /// A map of `entries`, in whatever order they come. Refuses a key
    /// given twice.
    pub fn map<K: Into<String>>(
        entries: impl IntoIterator<Item = (K, Value)>,
    ) -> Result<Self, CompositeError> {
        CompositeMap::new(entries).map(Composite::Map)
    }

    /// A list of `items`, in order.
    pub fn list(items: impl IntoIterator<Item = Value>) -> Self {
        Composite::List(CompositeItems::new(items))
    }

    /// The bytes this composite is kept as: the version byte, then the
    /// dag-cbor of its parts. One composite has one byte string.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![COMPOSITE_VERSION];
        match self {
            Composite::Map(map) => {
                write::head(write::MAP, map.len() as u64, &mut bytes);
                for (key, value) in &map.entries {
                    write::head(write::TEXT, key.len() as u64, &mut bytes);
                    bytes.extend_from_slice(key.as_bytes());
                    write::part(value, &mut bytes);
                }
            }
            Composite::List(items) => {
                write::head(write::ARRAY, items.len() as u64, &mut bytes);
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
            Some(COMPOSITE_VERSION) => read::composite(&bytes[1..]),
            Some(version) => Err(CompositeError::UnknownVersion { version }),
            None => Err(CompositeError::Malformed(
                "it does not start with a version byte".into(),
            )),
        }
    }

    /// The composite's name: the merkle reference of its parts. A map
    /// is named under [`Type::MAP`] and a list under [`Type::LIST`],
    /// each part by its own [`Value::reference`] and a key as the text
    /// it is. The bytes the composite is kept as are not part of it.
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
                Some(Part::Key(key)) => Step::Part(Value::String(key.into()).scalar_reference()),
                Some(Part::Value(part)) => match (below > 0).then(|| part.composite()) {
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

/// What is named next in a composite.
enum Part<'a> {
    /// A map's key, which is text.
    Key(&'a str),
    /// A value: a list's item, or what a map holds under a key.
    Value(&'a Value),
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
    fn next(&self) -> Option<Part<'_>> {
        let at = self.names.len();
        match self.composite.as_ref() {
            Composite::Map(map) => {
                let (key, value) = map.entries.get(at / 2)?;
                Some(if at.is_multiple_of(2) {
                    Part::Key(key)
                } else {
                    Part::Value(value)
                })
            }
            Composite::List(items) => items.get(at).map(Part::Value),
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
/// a claim's value. serde's untagged spelling of a value cannot
/// round-trip every variant (an unsigned integer resurfaces as a
/// float), and dag-cbor holds no integer past 64 bits, where this holds
/// every value.
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

/// The bytes a link to a value of `data_type` starts with: the byte
/// dag-cbor puts in front of every link, then a CID of version 1 under
/// the codec of that type, hashed by the identity. The length of what
/// it holds follows, and then what it holds.
fn link_of(data_type: u8) -> [u8; 7] {
    let code = VALUE_CODEC + u32::from(data_type);
    [
        0x00,
        0x01,
        (code & 0x7f) as u8 | 0x80,
        ((code >> 7) & 0x7f) as u8 | 0x80,
        ((code >> 14) & 0x7f) as u8 | 0x80,
        (code >> 21) as u8,
        0x00,
    ]
}

/// Whether `value` is written as a link, there being no dag-cbor kind
/// that holds it. A record that is a composite is neither: it is nested
/// as the item it is.
fn linked(value: &Value) -> bool {
    match value {
        Value::Bytes(_) | Value::Boolean(_) | Value::String(_) => false,
        Value::UnsignedInt(natural) => u64::try_from(*natural).is_err(),
        Value::SignedInt(integer) => !(-(1i128 << 64)..0).contains(integer),
        Value::Float(float) => !float.is_finite(),
        Value::Entity(_) | Value::Symbol(_) | Value::Record(_) => true,
    }
}

/// The dag-cbor a composite is written as. Written by hand, since
/// nothing here can fail and a link may hold more than the 64 bytes the
/// link of the libraries does.
mod write {
    use super::{COMPOSITE_VERSION, link_of, read, version_of};
    use crate::Value;

    pub const UNSIGNED: u8 = 0;
    pub const NEGATIVE: u8 = 1;
    pub const BYTES: u8 = 2;
    pub const TEXT: u8 = 3;
    pub const ARRAY: u8 = 4;
    pub const MAP: u8 = 5;
    pub const TAG: u8 = 6;
    pub const SIMPLE: u8 = 7;

    /// The tag dag-cbor writes a link under.
    pub const LINK_TAG: u64 = 42;
    pub const FALSE: u8 = 20;
    pub const TRUE: u8 = 21;
    pub const FLOAT: u8 = 27;

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

    /// A length as a CID writes one: seven bits a byte, the least
    /// first, every byte but the last marked.
    pub fn varint(mut length: u64, out: &mut Vec<u8>) {
        while length >= 0x80 {
            out.push(length as u8 | 0x80);
            length >>= 7;
        }
        out.push(length as u8);
    }

    /// A part: the dag-cbor kind that holds the value, the item a
    /// composite is, or a link that holds any other value.
    pub fn part(value: &Value, out: &mut Vec<u8>) {
        match value {
            Value::Boolean(false) => out.push(SIMPLE << 5 | FALSE),
            Value::Boolean(true) => out.push(SIMPLE << 5 | TRUE),
            Value::String(text) => {
                head(TEXT, text.len() as u64, out);
                out.extend_from_slice(text.as_bytes());
            }
            Value::Bytes(bytes) => {
                head(BYTES, bytes.len() as u64, out);
                out.extend_from_slice(bytes);
            }
            Value::UnsignedInt(natural) if !super::linked(value) => {
                head(UNSIGNED, *natural as u64, out);
            }
            Value::SignedInt(integer) if !super::linked(value) => {
                head(NEGATIVE, (-1 - *integer) as u64, out);
            }
            Value::Float(float) if !super::linked(value) => {
                out.push(SIMPLE << 5 | FLOAT);
                out.extend_from_slice(&float.to_be_bytes());
            }
            // A composite is nested as the item it is, which is what
            // follows its version byte.
            Value::Record(bytes)
                if version_of(bytes) == Some(COMPOSITE_VERSION)
                    && read::composite(&bytes[1..]).is_ok() =>
            {
                out.extend_from_slice(&bytes[1..]);
            }
            _ => {
                let held = value.to_bytes();
                let link = link_of(u8::from(value.data_type()));
                let mut length = Vec::new();
                varint(held.len() as u64, &mut length);
                head(TAG, LINK_TAG, out);
                head(BYTES, (link.len() + length.len() + held.len()) as u64, out);
                out.extend_from_slice(&link);
                out.extend_from_slice(&length);
                out.extend_from_slice(&held);
            }
        }
    }
}

/// The dag-cbor a composite is read from. Read by hand, a level at a
/// time and without a call for each level, so a record nested deep is
/// read on any stack; and strictly, so the bytes read are the bytes the
/// composite is written in.
mod read {
    use std::cmp::Ordering;

    use super::write::{
        ARRAY, BYTES, FALSE, FLOAT, LINK_TAG, MAP, NEGATIVE, SIMPLE, TAG, TEXT, TRUE, UNSIGNED,
    };
    use super::{
        COMPOSITE_VERSION, Composite, CompositeError, CompositeItems, CompositeMap, kept, link_of,
        linked, order,
    };
    use crate::{Value, ValueDataType};

    fn malformed(why: &str) -> CompositeError {
        CompositeError::Malformed(why.into())
    }

    /// One item, as far as its head says: a value, or how many parts
    /// the list or the map it opens holds.
    enum Item {
        Value(Value),
        List(u64),
        Map(u64),
    }

    /// A list or a map being read past: how many of its parts are left,
    /// and for a map the key last read.
    struct Open<'a> {
        left: u64,
        key: Option<Option<&'a str>>,
    }

    struct Cursor<'a> {
        bytes: &'a [u8],
        at: usize,
    }

    impl<'a> Cursor<'a> {
        fn take(&mut self, length: u64) -> Result<&'a [u8], CompositeError> {
            let end = usize::try_from(length)
                .ok()
                .and_then(|length| self.at.checked_add(length))
                .filter(|end| *end <= self.bytes.len())
                .ok_or_else(|| malformed("it ends before an item does"))?;
            let taken = &self.bytes[self.at..end];
            self.at = end;
            Ok(taken)
        }

        fn byte(&mut self) -> Result<u8, CompositeError> {
            Ok(self.take(1)?[0])
        }

        /// The head of the next item: its major type, and its argument
        /// read from the fewest bytes that hold it.
        fn head(&mut self) -> Result<(u8, u64), CompositeError> {
            let first = self.byte()?;
            let (major, short) = (first >> 5, first & 0x1f);
            if major == SIMPLE {
                return Ok((major, u64::from(short)));
            }
            let (argument, least) = match short {
                0..24 => (u64::from(short), 0),
                24 => (u64::from(self.byte()?), 24),
                25 => {
                    let bytes = self.take(2)?;
                    (u64::from(u16::from_be_bytes([bytes[0], bytes[1]])), 1 << 8)
                }
                26 => {
                    let bytes: [u8; 4] = self.take(4)?.try_into().expect("four bytes");
                    (u64::from(u32::from_be_bytes(bytes)), 1 << 16)
                }
                27 => {
                    let bytes: [u8; 8] = self.take(8)?.try_into().expect("eight bytes");
                    (u64::from_be_bytes(bytes), 1 << 32)
                }
                _ => return Err(malformed("a length of no fixed size")),
            };
            if argument < least {
                return Err(CompositeError::NotCanonical);
            }
            Ok((major, argument))
        }

        fn text(&mut self, length: u64) -> Result<&'a str, CompositeError> {
            str::from_utf8(self.take(length)?).map_err(|_| malformed("a text is not UTF-8"))
        }

        /// A map's next key, which follows `last` in the order keys are
        /// written in.
        fn key(&mut self, last: Option<&str>) -> Result<&'a str, CompositeError> {
            let (TEXT, length) = self.head()? else {
                return Err(malformed("a map's key is not text"));
            };
            let key = self.text(length)?;
            match last {
                Some(last) if order(last, key) != Ordering::Less => {
                    Err(CompositeError::NotCanonical)
                }
                _ => Ok(key),
            }
        }

        /// The value a link holds, which is one dag-cbor has no kind
        /// for.
        fn link(&mut self) -> Result<Value, CompositeError> {
            let (BYTES, length) = self.head()? else {
                return Err(malformed("a link is not bytes"));
            };
            let mut link = Cursor {
                bytes: self.take(length)?,
                at: 0,
            };
            // The codec says the type: every type's link starts as the
            // first type's does but for the byte the type is in.
            let first = link_of(0);
            let start = link.take(first.len() as u64)?;
            let data_type = start[2].wrapping_sub(first[2]);
            if data_type > ValueDataType::Symbol as u8 || start != link_of(data_type) {
                return Err(malformed("a link is not one that holds a value"));
            }
            let mut held: u64 = 0;
            let mut shift = 0;
            loop {
                let byte = link.byte()?;
                if shift > 56 || (byte == 0 && shift > 0) {
                    return Err(CompositeError::NotCanonical);
                }
                held |= u64::from(byte & 0x7f) << shift;
                shift += 7;
                if byte & 0x80 == 0 {
                    break;
                }
            }
            let held = link.take(held)?;
            if link.at != link.bytes.len() {
                return Err(malformed("a link holds more than its length says"));
            }
            let value = Value::try_from((ValueDataType::from(data_type), held.to_vec()))
                .map_err(|error| CompositeError::Malformed(error.to_string()))?;
            // What dag-cbor has a kind for is written as that kind, a
            // number under its narrowest type, and a composite as the
            // item it is.
            let nested = matches!(value.composite(), Ok(Some(_)));
            if !linked(&value) || nested || kept(value.clone()) != value {
                return Err(CompositeError::NotCanonical);
            }
            // A value is held as the bytes it is written in. A symbol is
            // read from more than those: whatever follows its name is
            // read past.
            if value.to_bytes() != held {
                return Err(CompositeError::NotCanonical);
            }
            Ok(value)
        }

        fn item(&mut self) -> Result<Item, CompositeError> {
            let (major, argument) = self.head()?;
            let value = match major {
                UNSIGNED => Value::UnsignedInt(u128::from(argument)),
                NEGATIVE => Value::SignedInt(-1 - i128::from(argument)),
                BYTES => Value::Bytes(self.take(argument)?.to_vec()),
                TEXT => Value::String(self.text(argument)?.into()),
                ARRAY => return Ok(Item::List(argument)),
                MAP => return Ok(Item::Map(argument)),
                TAG if argument == LINK_TAG => return self.link().map(Item::Value),
                TAG => return Err(malformed("a tag other than a link's")),
                _ => match argument as u8 {
                    FALSE => Value::Boolean(false),
                    TRUE => Value::Boolean(true),
                    FLOAT => {
                        let bytes: [u8; 8] = self.take(8)?.try_into().expect("eight bytes");
                        let float = Value::Float(f64::from_be_bytes(bytes));
                        if linked(&float) || kept(float.clone()) != float {
                            return Err(CompositeError::NotCanonical);
                        }
                        float
                    }
                    _ => return Err(malformed("a value of no kind a composite holds")),
                },
            };
            Ok(Item::Value(value))
        }

        /// Reads past the list or the map just opened, all of it and
        /// everything nested in it, checking each part as it goes. What
        /// is open waits in a list, innermost last.
        fn past(&mut self, opened: Open<'a>) -> Result<(), CompositeError> {
            let mut open = vec![opened];
            while let Some(innermost) = open.last_mut() {
                if innermost.left == 0 {
                    open.pop();
                    continue;
                }
                innermost.left -= 1;
                if let Some(last) = innermost.key {
                    innermost.key = Some(Some(self.key(last)?));
                }
                match self.item()? {
                    Item::Value(_) => {}
                    Item::List(left) => open.push(Open { left, key: None }),
                    Item::Map(left) => open.push(Open {
                        left,
                        key: Some(None),
                    }),
                }
            }
            Ok(())
        }

        /// The next part: a value, or the composite a nested item is,
        /// as the record it is kept as.
        fn part(&mut self) -> Result<Value, CompositeError> {
            let start = self.at;
            let opened = match self.item()? {
                Item::Value(value) => return Ok(value),
                Item::List(left) => Open { left, key: None },
                Item::Map(left) => Open {
                    left,
                    key: Some(None),
                },
            };
            self.past(opened)?;
            let mut record = vec![COMPOSITE_VERSION];
            record.extend_from_slice(&self.bytes[start..self.at]);
            Ok(Value::Record(record))
        }
    }

    /// The composite `bytes`, which follow a version byte, hold: its
    /// own parts, each nested composite checked and left as the record
    /// it is.
    pub fn composite(bytes: &[u8]) -> Result<Composite, CompositeError> {
        let mut cursor = Cursor { bytes, at: 0 };
        let composite = match cursor.item()? {
            Item::Value(_) => return Err(malformed("it is neither a map nor a list")),
            Item::List(length) => {
                let mut items = Vec::new();
                for _ in 0..length {
                    items.push(cursor.part()?);
                }
                Composite::List(CompositeItems(items))
            }
            Item::Map(length) => {
                let mut entries: Vec<(String, Value)> = Vec::new();
                let mut last = None;
                for _ in 0..length {
                    let key = cursor.key(last)?;
                    last = Some(key);
                    entries.push((key.into(), cursor.part()?));
                }
                Composite::Map(CompositeMap { entries })
            }
        };
        if cursor.at != bytes.len() {
            return Err(malformed("something follows the composite"));
        }
        Ok(composite)
    }
}

#[cfg(test)]
mod differential_tests;

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use super::*;
    use crate::history::{Edition, Origin, REVISION_RECORD_FORMAT, RevisionRecord, Version};
    use crate::identity::RECORD;
    use crate::{Entity, Relation, encode_value_owned};
    use ipld_core::cid::Cid;
    use ipld_core::ipld::Ipld;
    use serde::de::IgnoredAny;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn text(text: &str) -> Value {
        Value::String(text.into())
    }

    /// The link `value` is written as.
    fn link(value: &Value) -> Vec<u8> {
        let mut written = Vec::new();
        write::part(value, &mut written);
        written
    }

    /// The bytes of a composite whose one item is `part`, as written.
    fn list_of(part: &[u8]) -> Vec<u8> {
        let mut bytes = vec![COMPOSITE_VERSION, 0x81];
        bytes.extend_from_slice(part);
        bytes
    }

    /// What follows the version byte is dag-cbor a reader of dag-cbor
    /// reads: text, numbers, a truth value and bytes as its own kinds, a
    /// nested composite as a map or a list, and a value it has no kind
    /// for as a link that holds the value. The bytes are written out, so
    /// a change to how a composite is kept shows here.
    #[dialog_common::test]
    fn it_writes_dag_cbor_a_reader_of_dag_cbor_reads() {
        let alice = Value::Entity("id:alice".parse::<Entity>().expect("an entity"));
        let note = Composite::map([
            ("title", text("Buy milk")),
            ("done", Value::Boolean(false)),
            ("count", Value::UnsignedInt(3)),
            ("ratio", Value::Float(-0.5)),
            ("by", alice.clone()),
            (
                "tags",
                Composite::list([text("a"), Value::SignedInt(-2)]).into(),
            ),
            ("raw", Value::Bytes(vec![0, 255])),
        ])
        .expect("a map");
        let bytes = note.to_bytes();
        assert_eq!(
            hex(&bytes),
            concat!(
                "01a7",
                "626279d82a500001a1a3c301000869643a616c696365",
                "637261774200ff",
                "64646f6e65f4",
                "6474616773826161",
                "21",
                "65636f756e7403",
                "65726174696ffbbfe0000000000000",
                "657469746c6568427579206d696c6b",
            )
        );

        let read: Ipld = serde_ipld_dagcbor::from_slice(&bytes[1..]).expect("dag-cbor");
        let Ipld::Map(entries) = read else {
            panic!("a map");
        };
        assert_eq!(entries["title"], Ipld::String("Buy milk".into()));
        assert_eq!(entries["done"], Ipld::Bool(false));
        assert_eq!(entries["count"], Ipld::Integer(3));
        assert_eq!(entries["ratio"], Ipld::Float(-0.5));
        assert_eq!(entries["raw"], Ipld::Bytes(vec![0, 255]));
        assert_eq!(
            entries["tags"],
            Ipld::List(vec![Ipld::String("a".into()), Ipld::Integer(-2)])
        );
        let Ipld::Link(link) = &entries["by"] else {
            panic!("a link");
        };
        assert_eq!(
            link.codec(),
            u64::from(VALUE_CODEC) + u64::from(u8::from(ValueDataType::Entity))
        );
        assert_eq!(link.hash().code(), 0, "the identity");
        assert_eq!(link.hash().digest(), b"id:alice");
        assert_eq!(Value::from(&note).composite(), Ok(Some(note)));
    }

    /// A link holds a value of any length. The link of the libraries
    /// holds 64 bytes at most (`cid`'s `Cid` is `CidGeneric<64>`), so a
    /// value longer than that is read here and is not read by a reader
    /// built on them. This test says so for as long as it is so.
    #[dialog_common::test]
    fn it_links_a_value_longer_than_the_link_of_the_libraries_holds() {
        let long = format!("did:web:{}", "a".repeat(57));
        assert_eq!(long.len(), 65);
        let entity = Value::Entity(long.parse::<Entity>().expect("an entity"));
        let list = Composite::list([entity]);
        assert_eq!(Value::from(&list).composite(), Ok(Some(list.clone())));
        assert!(
            serde_ipld_dagcbor::from_slice::<Ipld>(&list.to_bytes()[1..]).is_err(),
            "the libraries read a link this long now: say so in the module"
        );

        let longest = format!("ns/{}", "a".repeat(61));
        assert_eq!(longest.len(), 64);
        assert!(format!("{longest}a").parse::<Relation>().is_err());
        let short = Composite::list([Value::Symbol(longest.parse().expect("a symbol"))]);
        let Ok(Ipld::List(items)) = serde_ipld_dagcbor::from_slice::<Ipld>(&short.to_bytes()[1..])
        else {
            panic!("the longest symbol is a link the libraries read");
        };
        assert!(matches!(items[0], Ipld::Link(Cid { .. })));
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
                .enumerate()
                .map(|(at, value)| (format!("key{at}"), value)),
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
        let inner = Composite::map([("x", Value::UnsignedInt(1))]).expect("a map");
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
            ("title", text("Buy milk")),
            ("done", Value::Boolean(false)),
            ("b", text("seven")),
            ("aa", Composite::list([text("a"), text("b")]).into()),
        ];
        let forward = Composite::map(entries.clone()).expect("a map");
        let backward = Composite::map(entries.into_iter().rev()).expect("a map");

        assert_eq!(forward, backward);
        let Composite::Map(kept) = &forward else {
            panic!("a map");
        };
        assert_eq!(
            kept.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            vec!["b", "aa", "done", "title"],
            "the shorter key first, and two of one length by their bytes"
        );
        assert_eq!(forward.reference(), backward.reference());
        assert_eq!(forward.to_bytes(), backward.to_bytes());
        assert_eq!(Value::from(&forward), Value::from(&backward));
        assert_eq!(
            Value::from(&forward).reference(),
            Value::from(&backward).reference()
        );
    }

    /// A map refuses a key given twice.
    #[dialog_common::test]
    fn it_refuses_a_key_given_twice() {
        let twice = Composite::map([("a", text("x")), ("a", text("y"))]);
        assert_eq!(twice, Err(CompositeError::DuplicateKey("a".into())));
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

        let map = Composite::map([("n", Value::SignedInt(4))]).expect("a map");
        let Composite::Map(entries) = &map else {
            panic!("a map");
        };
        assert_eq!(entries.get("n"), Some(&Value::UnsignedInt(4)));

        // A whole number is an integer, a number past 64 bits a link,
        // and one that is not whole a float.
        assert_eq!(three.to_bytes(), vec![COMPOSITE_VERSION, 0x81, 0x03]);
        assert_eq!(
            Composite::list([Value::SignedInt(-2)]).to_bytes(),
            vec![COMPOSITE_VERSION, 0x81, 0x21]
        );
        let most = Value::UnsignedInt(u128::from(u64::MAX));
        assert_eq!(
            Composite::list([most]).to_bytes(),
            [&[COMPOSITE_VERSION, 0x81, 0x1b][..], &[0xff; 8]].concat()
        );
        let past = Value::UnsignedInt(u128::from(u64::MAX) + 1);
        assert_eq!(
            Composite::list([past.clone()]).to_bytes(),
            list_of(&link(&past))
        );
        assert_eq!(link(&past)[..2], [0xd8, 0x2a], "a link");

        // Bytes that hold a whole number as a float are not the ones
        // the composite is written in.
        let wide = [&[0xfb][..], &3.0f64.to_be_bytes()].concat();
        assert_eq!(
            Composite::try_from_bytes(&list_of(&wide)),
            Err(CompositeError::NotCanonical)
        );
    }

    /// A map finds a value by its key, and gives and takes entries.
    #[dialog_common::test]
    fn it_reads_a_map_by_key() {
        let map =
            CompositeMap::new([("a", Value::UnsignedInt(1)), ("b", text("two"))]).expect("a map");
        assert_eq!(map.get("a"), Some(&Value::UnsignedInt(1)));
        assert_eq!(map.get("c"), None);
        assert_eq!(map.len(), 2);

        let rest = map.without(["a"]);
        assert_eq!(rest.iter().collect::<Vec<_>>(), vec![("b", &text("two"))]);
        assert_eq!(rest.with([("a", Value::UnsignedInt(1))]), Ok(map.clone()));
        assert_eq!(
            map.with([("a", Value::UnsignedInt(9))]),
            Err(CompositeError::DuplicateKey("a".into()))
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

        let map = Composite::map([("a", parts[1].clone())]).expect("a map");
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

    /// One composite has one byte string. Bytes that hold a composite
    /// another way are refused: a map's entries out of order or a key
    /// twice, a length in more bytes than it needs, a value written as a
    /// link that dag-cbor has a kind for, and a composite written as a
    /// link where it nests.
    #[dialog_common::test]
    fn it_refuses_bytes_a_composite_is_not_written_in() {
        let map = Composite::map([("a", text("x")), ("b", text("y"))]).expect("a map");
        let written = [0xa2, 0x61, b'a', 0x61, b'x', 0x61, b'b', 0x61, b'y'];
        assert_eq!(map.to_bytes()[1..], written);

        let swapped = vec![
            COMPOSITE_VERSION,
            0xa2,
            0x61,
            b'b',
            0x61,
            b'y',
            0x61,
            b'a',
            0x61,
            b'x',
        ];
        assert_eq!(
            Composite::try_from_bytes(&swapped),
            Err(CompositeError::NotCanonical)
        );
        assert_eq!(
            Value::Record(swapped.clone()).reference(),
            RECORD.reference_of(&swapped)
        );

        let twice = [
            &[COMPOSITE_VERSION, 0xa2][..],
            &written[1..5],
            &written[1..5],
        ]
        .concat();
        assert_eq!(
            Composite::try_from_bytes(&twice),
            Err(CompositeError::NotCanonical)
        );

        // The same inside a nested map, which is read past and checked.
        let nested = [0xa2, 0x61, b'b', 0xf4, 0x61, b'a', 0xf4];
        assert_eq!(
            Composite::try_from_bytes(&list_of(&nested)),
            Err(CompositeError::NotCanonical)
        );

        // A text of one byte, its length in a byte of its own.
        assert_eq!(
            Composite::try_from_bytes(&list_of(&[0x78, 0x01, b'a'])),
            Err(CompositeError::NotCanonical)
        );

        // A text, a small number and a composite, each held in a link.
        for held in [
            text("a"),
            Value::UnsignedInt(3),
            Value::from(Composite::list([text("a")])),
        ] {
            let payload = held.to_bytes();
            let link = link_of(u8::from(held.data_type()));
            let mut linked = vec![0xd8, 0x2a];
            let length = link.len() + 1 + payload.len();
            write::head(write::BYTES, length as u64, &mut linked);
            linked.extend_from_slice(&link);
            linked.push(payload.len() as u8);
            linked.extend_from_slice(&payload);
            assert_eq!(
                Composite::try_from_bytes(&list_of(&linked)),
                Err(CompositeError::NotCanonical),
                "{held:?}"
            );
        }

        // A symbol is 64 bytes, its name and nothing after it. With a
        // byte after the name it still reads as the symbol, so the link
        // that holds it is refused. Found by changing bytes
        // (`it_reads_only_bytes_it_would_write`).
        let symbol = Value::Symbol("xyz.todo/open".parse::<Relation>().expect("a symbol"));
        let held = Composite::list([symbol.clone()]).to_bytes();
        assert_eq!(
            Composite::try_from_bytes(&held),
            Ok(Composite::list([symbol.clone()]))
        );
        let mut padded = held.clone();
        *padded.last_mut().expect("bytes") = 2;
        assert_eq!(
            Value::try_from((ValueDataType::Symbol, padded[padded.len() - 64..].to_vec())).ok(),
            Some(symbol),
            "the symbol is read past what follows its name"
        );
        assert_eq!(
            Composite::try_from_bytes(&padded),
            Err(CompositeError::NotCanonical)
        );
    }

    /// What a composite holds is of a kind it is written in: no null,
    /// no float of fewer than 64 bits, no tag but a link's, no link but
    /// one that holds a value, and no value of a type that is not
    /// stored. Nor is a composite anything but a map or a list, with
    /// nothing after it and no key that is not text.
    #[dialog_common::test]
    fn it_refuses_a_part_of_no_kind_a_composite_holds() {
        let unknown_type = {
            // A codec past the last stored type's.
            let start = link_of(ValueDataType::Symbol as u8 + 1);
            let mut link = vec![0xd8, 0x2a];
            write::head(write::BYTES, (start.len() + 2) as u64, &mut link);
            link.extend_from_slice(&start);
            link.extend_from_slice(&[1, 1]);
            link
        };
        let foreign_link = {
            // A CID of version 1 under dag-cbor, hashed by sha2-256.
            let mut link = vec![0xd8, 0x2a, 0x58, 37, 0x00, 0x01, 0x71, 0x12, 0x20];
            link.extend_from_slice(&[7; 32]);
            link
        };
        for part in [
            vec![0xf6],
            vec![0xf9, 0x3e, 0x00],
            vec![0xc1, 0x01],
            unknown_type,
            foreign_link,
        ] {
            assert!(
                matches!(
                    Composite::try_from_bytes(&list_of(&part)),
                    Err(CompositeError::Malformed(_))
                ),
                "{part:?}"
            );
        }
        for bytes in [
            vec![COMPOSITE_VERSION, 0x61, b'a'],
            vec![COMPOSITE_VERSION, 0x80, 0x80],
            vec![COMPOSITE_VERSION, 0x82, 0xf4],
            vec![COMPOSITE_VERSION, 0xa1, 0x01, 0xf4],
        ] {
            assert!(
                matches!(
                    Composite::try_from_bytes(&bytes),
                    Err(CompositeError::Malformed(_))
                ),
                "{bytes:?}"
            );
        }
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
    /// bytes. Down to the depth the names are the ones a call a level
    /// gave (43d49a5), so naming without one moved no name, and so are
    /// they whatever bytes a composite is kept as. Past the depth a
    /// name is of bytes, and is the name of the bytes written today.
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
                "b8ddfad12d38308119e15bc908209410e95ea6a30c2070dddbdf176b8ccc7636",
            ),
            (
                NAMED_DEPTH * 4,
                "aa62510da9e7a9a14a77f6e4719b1a94a27416ac96428a03dabeb635528a8a12",
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
        let map = Composite::map([("k", Value::UnsignedInt(1)), ("j", Value::from(&pair))])
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
