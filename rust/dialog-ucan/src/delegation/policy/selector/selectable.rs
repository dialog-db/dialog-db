//! The `Selectable` trait.

use super::error::SelectorErrorReason;
use crate::{collection::Collection, number::Number};
use ipld_core::ipld::Ipld;
use std::collections::BTreeMap;

/// A trait for types that can be selected from [`Ipld`]
pub trait Selectable: Sized {
    /// Attempt to select on some [`Ipld`].
    ///
    /// # Errors
    ///
    /// If the [`Ipld`] is not of the expected shape,
    /// [`SelectErrorReason`] is returned.
    fn try_select(ipld: Ipld) -> Result<Self, SelectorErrorReason>;
}

impl Selectable for Ipld {
    fn try_select(ipld: Ipld) -> Result<Ipld, SelectorErrorReason> {
        Ok(ipld)
    }
}

impl Selectable for Number {
    fn try_select(ipld: Ipld) -> Result<Number, SelectorErrorReason> {
        match ipld {
            Ipld::Integer(i) => Ok(Number::Integer(i)),
            Ipld::Float(f) => Ok(Number::Float(f)),
            Ipld::Null
            | Ipld::Bool(_)
            | Ipld::String(_)
            | Ipld::Bytes(_)
            | Ipld::List(_)
            | Ipld::Map(_)
            | Ipld::Link(_) => Err(SelectorErrorReason::NotANumber),
        }
    }
}

impl Selectable for String {
    fn try_select(ipld: Ipld) -> Result<Self, SelectorErrorReason> {
        match ipld {
            Ipld::String(s) => Ok(s),
            Ipld::Null
            | Ipld::Bool(_)
            | Ipld::Integer(_)
            | Ipld::Float(_)
            | Ipld::Bytes(_)
            | Ipld::List(_)
            | Ipld::Map(_)
            | Ipld::Link(_) => Err(SelectorErrorReason::NotAString),
        }
    }
}

impl Selectable for Collection {
    fn try_select(ipld: Ipld) -> Result<Collection, SelectorErrorReason> {
        match ipld {
            Ipld::List(xs) => Ok(Collection::Array(xs.into_iter().try_fold(
                vec![],
                |mut acc, v| {
                    acc.push(Selectable::try_select(v)?);
                    Ok(acc)
                },
            )?)),
            Ipld::Map(xs) => Ok(Collection::Map(xs.into_iter().try_fold(
                BTreeMap::new(),
                |mut map, (k, v)| {
                    let value = Selectable::try_select(v)?;
                    map.insert(k, value);
                    Ok(map)
                },
            )?)),
            Ipld::Null
            | Ipld::Bool(_)
            | Ipld::Integer(_)
            | Ipld::Float(_)
            | Ipld::String(_)
            | Ipld::Bytes(_)
            | Ipld::Link(_) => Err(SelectorErrorReason::NotACollection),
        }
    }
}
