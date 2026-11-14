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

/// A map of `entries`, in whatever order. A key given twice is
/// refused.
pub fn map(entries: impl IntoIterator<Item = (Value, Value)>) -> Result<Value, Error> {
    Composite::map(entries)
        .map(Value::from)
        .map_err(|error| Error::Refused(error.to_string()))
}

/// A map whose keys are names: symbols of no domain, or of the domain
/// a name is written with.
pub fn record<'a>(fields: impl IntoIterator<Item = (&'a str, Value)>) -> Result<Value, Error> {
    map(fields
        .into_iter()
        .map(|(name, value)| Ok((symbol(name)?, value)))
        .collect::<Result<Vec<_>, Error>>()?)
}

fn unread(error: CompositeError) -> Error {
    Error::Read(error.to_string())
}

/// The map `value` is. A value that is no map is not one, and a record
/// under a version this release does not read is refused by its
/// number.
pub fn entries(value: &Value) -> Result<Option<CompositeMap>, Error> {
    match value.composite().map_err(unread)? {
        Some(Composite::Map(map)) => Ok(Some(map)),
        _ => Ok(None),
    }
}

/// The members of the list `value` is.
pub fn items(value: &Value) -> Result<Option<Vec<Value>>, Error> {
    match value.composite().map_err(unread)? {
        Some(Composite::List(items)) => Ok(Some(items.into_values())),
        _ => Ok(None),
    }
}

/// What a map holds under the name `field`.
pub fn field<'a>(map: &'a CompositeMap, field: &str) -> Option<&'a Value> {
    map.get(&symbol(field).ok()?)
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

    #[test]
    fn it_takes_any_data_as_a_key() -> Result<(), Error> {
        let pair = list([natural(1), natural(2)]);
        let inner = record([("k", natural(1))])?;
        let kept = map([
            (pair.clone(), text("pair")),
            (entity("open:")?, Value::Boolean(true)),
            (inner.clone(), text("map")),
        ])?;
        let read = entries(&kept)?.expect("a map");
        assert_eq!(read.get(&pair), Some(&text("pair")));
        assert_eq!(read.get(&inner), Some(&text("map")));
        assert_eq!(items(&pair)?, Some(vec![natural(1), natural(2)]));
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
