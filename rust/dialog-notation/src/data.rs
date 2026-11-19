//! Data: what a syntax tree is kept in.
//!
//! A tree is kept as the composite value Dialog-DB has
//! ([`Composite`]): a map, a list, or a value that is neither. Nothing
//! here knows what a piece of syntax means, or that there is syntax: a
//! [`Syntax`](crate::Syntax) node is a map that says its `type`, and it
//! is the layer above that reads it as one.
//!
//! | data | Dialog-DB value | referenced under |
//! |---|---|---|
//! | text | `Value::String` | `text:` |
//! | number | natural, integer, float: one number, kept under the narrowest | the number type |
//! | truth | `Value::Boolean` | `boolean:` |
//! | entity | `Value::Entity` | `entity:` |
//! | symbol, of a domain or of none | `Value::Symbol`, `xyz.todo/text`, `/todo` | `symbol:` |
//! | list | a `Value::Record` that holds a [`Composite::List`] | the list type |
//! | map | a `Value::Record` that holds a [`Composite::Map`] | the map type |
//!
//! The composite gives what a layout of the notation's own gave before
//! it: a name that is the merkle reference of the parts, which no
//! layout of the bytes moves; one byte string for one piece of data,
//! whatever order a map's entries were given in; and a version byte, so
//! bytes of a layout this release does not read are refused by its
//! number.
//!
//! What is left here is what the notation adds: how a name is spelled
//! as a symbol, and a map whose keys are names.

use std::str::FromStr;

use dialog_corpus::{Composite, CompositeError, CompositeMap, Entity, Relation, Value};

use crate::Error;

fn refused(what: &str, error: impl std::fmt::Display) -> Error {
    Error::Refused(format!("{what}: {error}"))
}

/// Text.
pub fn text(text: impl Into<String>) -> Value {
    Value::String(text.into())
}

/// The symbol a name is: itself where it has a domain, and of no
/// domain otherwise.
pub fn symbol(name: &str) -> Result<Value, Error> {
    let spelled = if domain(name) {
        name.to_string()
    } else {
        format!("/{name}")
    };
    Relation::from_str(&spelled)
        .map(Value::Symbol)
        .map_err(|error| refused("not a symbol", error))
}

/// A symbol's name as written: without the `/` of no domain.
pub fn spelled(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(relation) => {
            let spelled = relation.to_string();
            Some(match spelled.strip_prefix('/') {
                Some(name) if !domain(&spelled) => name.to_string(),
                _ => spelled,
            })
        }
        _ => None,
    }
}

/// The entity a URI is.
pub fn entity(uri: &str) -> Result<Value, Error> {
    Entity::from_str(uri)
        .map(Value::Entity)
        .map_err(|error| refused("not an entity", error))
}

/// A list of `items`.
pub fn list(items: impl IntoIterator<Item = Value>) -> Value {
    Composite::list(items).into()
}

/// A key of a map, as data keeps it. It is text today; the type is the
/// module's own, so a key that is no text can be one later without
/// anything above this module saying what a key is made of.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(String);

impl Key {
    /// The key that is `text`.
    pub fn new(text: impl Into<String>) -> Self {
        Key(text.into())
    }

    /// The text the key is, where it is text.
    pub fn text(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Key {
    fn from(text: &str) -> Self {
        Key::new(text)
    }
}

impl From<String> for Key {
    fn from(text: String) -> Self {
        Key(text)
    }
}

impl From<&String> for Key {
    fn from(text: &String) -> Self {
        Key(text.clone())
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A map, read: what it holds under each key.
#[derive(Clone, Debug, PartialEq)]
pub struct Map(CompositeMap);

impl Map {
    /// What it holds under `key`.
    pub fn get(&self, key: impl Into<Key>) -> Option<&Value> {
        self.0.get(key.into().text())
    }

    /// Its entries, in the order it keeps them.
    pub fn entries(&self) -> impl Iterator<Item = (Key, &Value)> {
        self.0.iter().map(|(key, value)| (Key::new(key), value))
    }
}

/// What a value is, told apart: a map, a list, or neither.
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    /// A map.
    Map(Map),
    /// A list, its items in order.
    List(Vec<Value>),
    /// A value that is neither.
    Scalar,
}

/// What `value` is. A record under a version this release does not read
/// is refused by its number.
pub fn shape(value: &Value) -> Result<Data, Error> {
    Ok(match value.composite().map_err(unread)? {
        Some(Composite::Map(map)) => Data::Map(Map(map)),
        Some(Composite::List(items)) => Data::List(items.into_values()),
        None => Data::Scalar,
    })
}

/// Whether `value` is bytes the notation does not read: a byte string,
/// or a record that holds no map or list this release reads.
pub fn opaque(value: &Value) -> bool {
    matches!(value, Value::Bytes(_) | Value::Record(_))
}

/// The bytes `value` is, where it is bytes or a record.
pub fn bytes(value: &Value) -> Option<&[u8]> {
    match value {
        Value::Bytes(bytes) | Value::Record(bytes) => Some(bytes),
        _ => None,
    }
}

/// The bytes a map or a list is kept as.
pub fn encode(value: &Value) -> Result<Vec<u8>, Error> {
    match value {
        Value::Record(bytes) => Ok(bytes.clone()),
        other => Err(Error::Read(format!("a node that is no record: {other:?}"))),
    }
}

/// The map or list `bytes` keep, as [`encode`] gave them.
pub fn decode(bytes: &[u8]) -> Value {
    Value::Record(bytes.to_vec())
}

/// A map of `entries`, in whatever order. A key given twice is refused.
pub fn map<K: Into<Key>>(entries: impl IntoIterator<Item = (K, Value)>) -> Result<Value, Error> {
    Composite::map(
        entries
            .into_iter()
            .map(|(key, value)| (key.into().0, value)),
    )
    .map(Value::from)
    .map_err(|error| Error::Refused(error.to_string()))
}

/// A map whose keys are names, each the text it is written as.
pub fn record<'a>(fields: impl IntoIterator<Item = (&'a str, Value)>) -> Result<Value, Error> {
    map(fields)
}

fn unread(error: CompositeError) -> Error {
    Error::Read(error.to_string())
}

/// The map `value` is. A value that is no map is not one, and a record
/// under a version this release does not read is refused by its
/// number.
pub fn entries(value: &Value) -> Result<Option<Map>, Error> {
    match shape(value)? {
        Data::Map(map) => Ok(Some(map)),
        _ => Ok(None),
    }
}

/// The members of the list `value` is.
pub fn items(value: &Value) -> Result<Option<Vec<Value>>, Error> {
    match shape(value)? {
        Data::List(items) => Ok(Some(items)),
        _ => Ok(None),
    }
}

/// What a map holds under `key`.
pub fn field(map: &Map, key: impl Into<Key>) -> Option<&Value> {
    map.get(key)
}

/// The item of the list `value` is at `index`.
pub fn item(value: &Value, index: usize) -> Result<Option<Value>, Error> {
    Ok(items(value)?.and_then(|mut items| (index < items.len()).then(|| items.swap_remove(index))))
}

/// Whether a name is written with a domain: something stands before
/// its first `/`. `/`, the operator, and `//` are names of none.
fn domain(name: &str) -> bool {
    name.split_once('/')
        .is_some_and(|(domain, _)| !domain.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn natural(n: u64) -> Value {
        Value::UnsignedInt(n.into())
    }

    #[test]
    fn it_names_a_map_whatever_order_its_entries_came_in() -> Result<(), Error> {
        let one = record([("a", natural(1)), ("b", natural(2))])?;
        let other = record([("b", natural(2)), ("a", natural(1))])?;
        assert_eq!(one, other);
        assert_eq!(one.reference(), other.reference());
        assert_ne!(
            one.reference(),
            record([("a", natural(1)), ("b", natural(3))])?.reference()
        );
        let map = entries(&one)?.expect("a map");
        assert_eq!(field(&map, "b"), Some(&natural(2)));
        assert_eq!(field(&map, "c"), None);
        Ok(())
    }

    #[test]
    fn it_spells_a_name_of_no_domain_and_reads_it_back() -> Result<(), Error> {
        for name in ["todo", "xyz.todo", "xyz.todo/text", "?x", "+", "/", "", "_"] {
            assert_eq!(spelled(&symbol(name)?).as_deref(), Some(name));
        }
        assert_ne!(symbol("")?, symbol("/")?);
        Ok(())
    }

    /// A map's key is text: a name as it is written, or any text.
    #[test]
    fn it_keys_a_map_by_text() -> Result<(), Error> {
        let pair = list([natural(1), natural(2)]);
        let kept = map([
            ("pair", pair.clone()),
            ("xyz.todo/text", text("attribute")),
            ("N1", Value::Boolean(true)),
        ])?;
        let read = entries(&kept)?.expect("a map");
        assert_eq!(read.get("pair"), Some(&pair));
        assert_eq!(field(&read, "xyz.todo/text"), Some(&text("attribute")));
        assert_eq!(read.get("N1"), Some(&Value::Boolean(true)));
        assert_eq!(items(&pair)?, Some(vec![natural(1), natural(2)]));
        assert!(map([("a", natural(1)), ("a", natural(2))]).is_err());
        Ok(())
    }

    /// The natural 3 and the float 3.0 are one number to Dialog-DB, so
    /// they are one piece of data: one name and one byte string.
    #[test]
    fn it_keeps_one_number_one_way() {
        let natural = list([natural(3)]);
        let float = list([Value::Float(3.0)]);
        assert_eq!(natural.reference(), float.reference());
        assert_eq!(natural, float);
    }

    #[test]
    fn it_refuses_a_version_it_does_not_know() -> Result<(), Error> {
        let Value::Record(mut bytes) = record([("a", natural(1))])? else {
            panic!("a record");
        };
        bytes[0] += 1;
        assert_eq!(
            entries(&Value::Record(bytes)),
            Err(Error::Read(
                CompositeError::UnknownVersion { version: 2 }.to_string()
            ))
        );
        Ok(())
    }

    #[test]
    fn it_refuses_a_key_twice() {
        assert!(matches!(
            record([("a", natural(1)), ("a", natural(2))]),
            Err(Error::Refused(_))
        ));
    }
}
