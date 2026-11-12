//! Data: what a syntax tree is kept in.
//!
//! Three things that change at different rates are kept apart, so the
//! one that changes most often touches the least:
//!
//! - **The data types.** [`Data`] is a value Dialog-DB has (a truth, a
//!   number, text, an entity or a symbol), a list, or a map from data
//!   to data. Nothing here knows what a piece of syntax means, or that
//!   there is syntax: a [`Syntax`](crate::Syntax) node is a map that
//!   says its `type`, and it is the layer above that reads it as one.
//! - **The name.** A [`reference`](Data::reference) is a merkle
//!   reference, made of the references of the parts under the type of
//!   each. It does not depend on the bytes the data is laid out in, nor
//!   on the order a map's entries were given in.
//! - **The layout.** [`encode`](Data::encode) writes data as bytes
//!   under a version, and [`decode`](Data::decode) refuses a version it
//!   does not know, by number. The bytes are what a `record:` value
//!   holds. A new layout is a new version, and no name changes with it.
//!
//! | data | Dialog-DB value | referenced under |
//! |---|---|---|
//! | text | `Value::String` | `text:` |
//! | number | natural, integer, float | the number type |
//! | truth | `Value::Boolean` | `boolean:` |
//! | entity | `Value::Entity` | `entity:` |
//! | symbol, of a domain or of none | `Value::Symbol`, `xyz.todo/text`, `/todo` | `symbol:` |
//! | list | none yet | the list type |
//! | map | none yet | the map type |
//!
//! This is a stand-in for the composite value Dialog-DB is getting
//! (`design/patterns.md`), which is the reason the layout carries a
//! version: when that value lands, this module is what it replaces.

use std::str::FromStr;

use bijoux::{Decode as _, Encode as _};
use dialog_corpus::{Entity, Relation, Value};
use merkle_reference::{Reference, Type};

use crate::Error;

/// The layout [`Data::encode`] writes.
pub const VERSION: u8 = 2;

const TRUTH: u8 = 1;
const NATURAL: u8 = 2;
const INTEGER: u8 = 3;
const FLOAT: u8 = 4;
const TEXT: u8 = 5;
const ENTITY: u8 = 6;
const SYMBOL: u8 = 7;
const LIST: u8 = 8;
const MAP: u8 = 9;

fn refused(what: &str, error: impl std::fmt::Display) -> Error {
    Error::Refused(format!("{what}: {error}"))
}

/// A value, a list of data, or a map from data to data.
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    /// A truth, a number, text, an entity or a symbol.
    Value(Value),
    /// Data in order.
    List(Vec<Data>),
    /// Data under keys, each key once. The entries are held in the
    /// order of their keys as laid out, whatever order they were given
    /// in; [`Data::map`] is what makes one.
    Map(Vec<(Data, Data)>),
}

impl Data {
    /// Text.
    pub fn text(text: impl Into<String>) -> Self {
        Data::Value(Value::String(text.into()))
    }

    /// The symbol a name is: itself where it has a domain, and of no
    /// domain otherwise.
    pub fn symbol(name: &str) -> Result<Self, Error> {
        let spelled = if domain(name) {
            name.to_string()
        } else {
            format!("/{name}")
        };
        Relation::from_str(&spelled)
            .map(|relation| Data::Value(Value::Symbol(relation)))
            .map_err(|error| refused("not a symbol", error))
    }

    /// The entity a URI is.
    pub fn entity(uri: &str) -> Result<Self, Error> {
        Entity::from_str(uri)
            .map(|entity| Data::Value(Value::Entity(entity)))
            .map_err(|error| refused("not an entity", error))
    }

    /// A map of `entries`, in whatever order. A key given twice is
    /// refused.
    pub fn map(entries: impl IntoIterator<Item = (Data, Data)>) -> Result<Self, Error> {
        let mut keyed = Vec::new();
        for (key, value) in entries {
            let mut at = Vec::new();
            key.write(&mut at)?;
            keyed.push((at, (key, value)));
        }
        keyed.sort_by(|(a, _), (b, _)| a.cmp(b));
        if keyed.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(Error::Refused("a map holds a key twice".into()));
        }
        Ok(Data::Map(
            keyed.into_iter().map(|(_, entry)| entry).collect(),
        ))
    }

    /// A map whose keys are names: symbols of no domain, or of the
    /// domain a name is written with.
    pub fn record<'a>(fields: impl IntoIterator<Item = (&'a str, Data)>) -> Result<Self, Error> {
        Data::map(
            fields
                .into_iter()
                .map(|(name, value)| Ok((Data::symbol(name)?, value)))
                .collect::<Result<Vec<_>, Error>>()?,
        )
    }

    /// What a map holds under the name `field`.
    pub fn get(&self, field: &str) -> Option<&Data> {
        let key = Data::symbol(field).ok()?;
        match self {
            Data::Map(entries) => entries
                .iter()
                .find_map(|(at, value)| (*at == key).then_some(value)),
            _ => None,
        }
    }

    /// The text this is, if it is text.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Data::Value(Value::String(text)) => Some(text),
            _ => None,
        }
    }

    /// A symbol's name as written: without the `/` of no domain.
    pub fn as_symbol(&self) -> Option<String> {
        match self {
            Data::Value(Value::Symbol(relation)) => {
                let spelled = relation.to_string();
                Some(match spelled.strip_prefix('/') {
                    Some(name) if !domain(&spelled) => name.to_string(),
                    _ => spelled,
                })
            }
            _ => None,
        }
    }

    /// The members of a list.
    pub fn as_list(&self) -> Option<&[Data]> {
        match self {
            Data::List(items) => Some(items),
            _ => None,
        }
    }

    /// The entries of a map, in the order of their keys.
    pub fn as_map(&self) -> Option<&[(Data, Data)]> {
        match self {
            Data::Map(entries) => Some(entries),
            _ => None,
        }
    }

    /// What names the data: a merkle reference, made of the references
    /// of its parts, a list under the type every list has and a map
    /// under the type every map has. No layout has a part in it.
    pub fn reference(&self) -> Reference {
        match self {
            Data::Value(value) => value.reference(),
            Data::List(items) => Type::LIST.list(items.iter().map(Data::reference)),
            Data::Map(entries) => Type::MAP.map(
                entries
                    .iter()
                    .map(|(key, value)| (key.reference(), value.reference())),
            ),
        }
    }

    /// The data as bytes: the version, then the data. One piece of data
    /// has one byte string.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut out = vec![VERSION];
        self.write(&mut out)?;
        Ok(out)
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), Error> {
        match self {
            Data::Value(Value::Boolean(truth)) => out.extend([TRUTH, u8::from(*truth)]),
            Data::Value(Value::UnsignedInt(natural)) => {
                out.push(NATURAL);
                u64::try_from(*natural)
                    .map_err(|error| refused("a natural this layout has no room for", error))?
                    .encode(out);
            }
            Data::Value(Value::SignedInt(integer)) => {
                out.push(INTEGER);
                out.extend(
                    i64::try_from(*integer)
                        .map_err(|error| refused("an integer this layout has no room for", error))?
                        .to_be_bytes(),
                );
            }
            Data::Value(Value::Float(float)) => {
                out.push(FLOAT);
                out.extend(float.to_bits().to_be_bytes());
            }
            Data::Value(Value::String(text)) => {
                out.push(TEXT);
                bytes(text.as_bytes(), out);
            }
            Data::Value(Value::Entity(entity)) => {
                out.push(ENTITY);
                bytes(entity.as_str().as_bytes(), out);
            }
            Data::Value(Value::Symbol(relation)) => {
                out.push(SYMBOL);
                bytes(relation.to_string().as_bytes(), out);
            }
            Data::Value(Value::Bytes(_) | Value::Record(_)) => {
                return Err(Error::Refused(
                    "bytes and records are not data this layout writes".into(),
                ));
            }
            Data::List(items) => {
                out.push(LIST);
                length(items.len(), out);
                for item in items {
                    item.write(out)?;
                }
            }
            Data::Map(entries) => {
                out.push(MAP);
                length(entries.len(), out);
                for (key, value) in entries {
                    key.write(out)?;
                    value.write(out)?;
                }
            }
        }
        Ok(())
    }

    /// The data `bytes` hold. A version this does not know is refused
    /// by its number, and so are bytes that are not the one way the
    /// data is written.
    pub fn decode(bytes: &[u8]) -> Result<Data, Error> {
        let (version, rest) = bytes
            .split_first()
            .ok_or_else(|| Error::Read("data of no bytes".into()))?;
        if *version != VERSION {
            return Err(Error::Read(format!(
                "data laid out as version {version}, and this reads version {VERSION}"
            )));
        }
        let mut reader = Reader { bytes: rest };
        let data = reader.data()?;
        if !reader.bytes.is_empty() {
            return Err(Error::Read("bytes after the data".into()));
        }
        if data.encode()? != bytes {
            return Err(Error::Read(
                "bytes that are not the one way the data is written".into(),
            ));
        }
        Ok(data)
    }
}

/// Whether a name is written with a domain: something stands before
/// its first `/`. `/`, the operator, and `//` are names of none.
fn domain(name: &str) -> bool {
    name.split_once('/')
        .is_some_and(|(domain, _)| !domain.is_empty())
}

fn length(count: usize, out: &mut Vec<u8>) {
    (count as u64).encode(out);
}

fn bytes(content: &[u8], out: &mut Vec<u8>) {
    length(content.len(), out);
    out.extend_from_slice(content);
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl Reader<'_> {
    fn short() -> Error {
        Error::Read("data that ends early".into())
    }

    fn take(&mut self, count: usize) -> Result<&[u8], Error> {
        if self.bytes.len() < count {
            return Err(Self::short());
        }
        let (taken, rest) = self.bytes.split_at(count);
        self.bytes = rest;
        Ok(taken)
    }

    fn natural(&mut self) -> Result<u64, Error> {
        let (value, consumed) = u64::decode(self.bytes).map_err(|_| Self::short())?;
        self.bytes = &self.bytes[consumed..];
        Ok(value)
    }

    fn length(&mut self) -> Result<usize, Error> {
        usize::try_from(self.natural()?).map_err(|_| Self::short())
    }

    fn text(&mut self) -> Result<String, Error> {
        let count = self.length()?;
        String::from_utf8(self.take(count)?.to_vec())
            .map_err(|_| Error::Read("text that is not UTF-8".into()))
    }

    fn data(&mut self) -> Result<Data, Error> {
        let tag = self.take(1)?[0];
        Ok(match tag {
            TRUTH => Data::Value(Value::Boolean(self.take(1)?[0] != 0)),
            NATURAL => Data::Value(Value::UnsignedInt(self.natural()?.into())),
            INTEGER => {
                let mut integer = [0; 8];
                integer.copy_from_slice(self.take(8)?);
                Data::Value(Value::SignedInt(i64::from_be_bytes(integer).into()))
            }
            FLOAT => {
                let mut bits = [0; 8];
                bits.copy_from_slice(self.take(8)?);
                Data::Value(Value::Float(f64::from_bits(u64::from_be_bytes(bits))))
            }
            TEXT => Data::text(self.text()?),
            ENTITY => {
                Data::entity(&self.text()?).map_err(|error| Error::Read(error.to_string()))?
            }
            SYMBOL => Relation::from_str(&self.text()?)
                .map(|relation| Data::Value(Value::Symbol(relation)))
                .map_err(|error| Error::Read(format!("not a symbol: {error}")))?,
            LIST => {
                let count = self.length()?;
                Data::List(
                    (0..count)
                        .map(|_| self.data())
                        .collect::<Result<_, Error>>()?,
                )
            }
            // The entries are put in order here, so a map laid out in
            // another order does not write back as the bytes it was
            // read from, which is how the caller refuses it.
            MAP => {
                let count = self.length()?;
                Data::map(
                    (0..count)
                        .map(|_| Ok((self.data()?, self.data()?)))
                        .collect::<Result<Vec<_>, Error>>()?,
                )
                .map_err(|error| Error::Read(error.to_string()))?
            }
            other => {
                return Err(Error::Read(format!("data of no kind this reads: {other}")));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn natural(n: u64) -> Data {
        Data::Value(Value::UnsignedInt(n.into()))
    }

    #[test]
    fn it_names_a_map_whatever_order_its_entries_came_in() -> Result<(), Error> {
        let one = Data::record([("a", natural(1)), ("b", natural(2))])?;
        let other = Data::record([("b", natural(2)), ("a", natural(1))])?;
        assert_eq!(one, other);
        assert_eq!(one.reference(), other.reference());
        assert_eq!(one.encode()?, other.encode()?);
        assert_ne!(
            one.reference(),
            Data::record([("a", natural(1)), ("b", natural(3))])?.reference()
        );
        assert_eq!(one.get("b"), Some(&natural(2)));
        assert_eq!(one.get("c"), None);
        Ok(())
    }

    #[test]
    fn it_spells_a_name_of_no_domain_and_reads_it_back() -> Result<(), Error> {
        for name in ["todo", "xyz.todo", "xyz.todo/text", "?x", "+", "/", "", "_"] {
            assert_eq!(Data::symbol(name)?.as_symbol().as_deref(), Some(name));
        }
        assert_ne!(Data::symbol("")?, Data::symbol("/")?);
        Ok(())
    }

    #[test]
    fn it_takes_any_data_as_a_key() -> Result<(), Error> {
        let map = Data::map([
            (Data::List(vec![natural(1), natural(2)]), Data::text("pair")),
            (Data::entity("open:")?, Data::Value(Value::Boolean(true))),
            (Data::record([("k", natural(1))])?, Data::text("map")),
        ])?;
        assert_eq!(Data::decode(&map.encode()?)?, map);
        Ok(())
    }

    /// The natural 3 and the float 3.0 are one number to Dialog-DB and
    /// two byte strings here: a name does not follow the layout.
    #[test]
    fn it_names_apart_from_the_layout() -> Result<(), Error> {
        let natural = Data::List(vec![natural(3)]);
        let float = Data::List(vec![Data::Value(Value::Float(3.0))]);
        assert_eq!(natural.reference(), float.reference());
        assert_ne!(natural.encode()?, float.encode()?);
        Ok(())
    }

    #[test]
    fn it_refuses_a_version_it_does_not_know() -> Result<(), Error> {
        let mut bytes = Data::record([("a", natural(1))])?.encode()?;
        bytes[0] = VERSION + 1;
        assert_eq!(
            Data::decode(&bytes),
            Err(Error::Read(format!(
                "data laid out as version {}, and this reads version {VERSION}",
                VERSION + 1
            )))
        );
        Ok(())
    }

    #[test]
    fn it_refuses_a_key_twice_and_entries_out_of_order() -> Result<(), Error> {
        assert!(matches!(
            Data::record([("a", natural(1)), ("a", natural(2))]),
            Err(Error::Refused(_))
        ));

        // Two entries of one length, swapped in the bytes.
        let both = Data::record([("a", natural(1)), ("b", natural(2))])?.encode()?;
        let entry = (both.len() - 3) / 2;
        let mut swapped = both[..3].to_vec();
        swapped.extend(&both[3 + entry..]);
        swapped.extend(&both[3..3 + entry]);
        assert_ne!(swapped, both);
        assert_eq!(
            Data::decode(&swapped),
            Err(Error::Read(
                "bytes that are not the one way the data is written".into()
            ))
        );
        Ok(())
    }
}
