//! Distributed promises

use ipld_core::{cid::Cid, ipld::Ipld};
use serde::{Deserialize, Serialize, Serializer, de, ser::SerializeMap};
use std::collections::BTreeMap;
use thiserror::Error;

/// Top-level union of all UCAN Promise options
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Promise<T, E> {
    /// The `ucan/await/ok` promise
    Ok(T),

    /// The `ucan/await/err` promise
    Err(E),

    /// The `ucan/await/ok` promise
    PendingOk(Cid),

    /// The `ucan/await/err` promise
    PendingErr(Cid),

    /// The `ucan/await/*` promise
    PendingAny(Cid),

    /// The `ucan/await` promise
    PendingTagged(Cid),
}

/// A recursive data structure whose leaves may be [`Ipld`] or promises.
///
/// [`Promised`] resolves to regular [`Ipld`].
#[derive(Debug, Clone, PartialEq)]
pub enum Promised {
    /// Resolved null.
    Null,

    /// Resolved Boolean.
    Bool(bool),

    /// Resolved integer.
    Integer(i128),

    /// Resolved float.
    Float(f64),

    /// Resolved string.
    String(String),

    /// Resolved bytes.
    Bytes(Vec<u8>),

    /// Resolved link.
    Link(Cid),

    /// Promise pending the `ok` branch.
    WaitOk(Cid),

    /// Promise pending the `err` branch.
    WaitErr(Cid),

    /// Promise pending either branch.
    WaitAny(Cid),

    /// Recursively promised list.
    List(Vec<Promised>),

    /// Recursively promised map.
    Map(BTreeMap<String, Promised>),
}

impl Serialize for Promised {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Bool(b) => serializer.serialize_bool(*b),
            Self::Integer(i) => serializer.serialize_i128(*i),
            Self::Float(f) => serializer.serialize_f64(*f),
            Self::String(s) => serializer.serialize_str(s),
            Self::Bytes(b) => {
                // Serialize as IPLD bytes
                let ipld = Ipld::Bytes(b.clone());
                ipld.serialize(serializer)
            }
            Self::Link(c) => {
                let ipld = Ipld::Link(*c);
                ipld.serialize(serializer)
            }
            Self::WaitOk(c) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("ucan/await/ok", c)?;
                map.end()
            }
            Self::WaitErr(c) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("ucan/await/err", c)?;
                map.end()
            }
            Self::WaitAny(c) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("ucan/await/*", c)?;
                map.end()
            }
            Self::List(l) => l.serialize(serializer),
            Self::Map(m) => m.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Promised {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        // Deserialize as Ipld first, then convert
        let ipld = Ipld::deserialize(deserializer)?;
        Ok(from_ipld(ipld))
    }
}

/// Convert an `Ipld` value into a `Promised`, detecting promise maps.
fn from_ipld(ipld: Ipld) -> Promised {
    match ipld {
        Ipld::Null => Promised::Null,
        Ipld::Bool(b) => Promised::Bool(b),
        Ipld::Integer(i) => Promised::Integer(i),
        Ipld::Float(f) => Promised::Float(f),
        Ipld::String(s) => Promised::String(s),
        Ipld::Bytes(b) => Promised::Bytes(b),
        Ipld::Link(c) => Promised::Link(c),
        Ipld::List(l) => Promised::List(l.into_iter().map(from_ipld).collect()),
        Ipld::Map(m) => {
            // Check for promise maps: single-entry maps with a promise key
            if m.len() == 1 {
                if let Some(Ipld::Link(cid)) = m.get("ucan/await/ok") {
                    return Promised::WaitOk(*cid);
                }
                if let Some(Ipld::Link(cid)) = m.get("ucan/await/err") {
                    return Promised::WaitErr(*cid);
                }
                if let Some(Ipld::Link(cid)) = m.get("ucan/await/*") {
                    return Promised::WaitAny(*cid);
                }
            }
            Promised::Map(m.into_iter().map(|(k, v)| (k, from_ipld(v))).collect())
        }
    }
}

impl TryFrom<&Promised> for Ipld {
    type Error = WaitingOn;

    fn try_from(promised: &Promised) -> Result<Self, Self::Error> {
        match promised {
            Promised::Null => Ok(Ipld::Null),
            Promised::Bool(b) => Ok(Ipld::Bool(*b)),
            Promised::Integer(i) => Ok(Ipld::Integer(*i)),
            Promised::Float(f) => Ok(Ipld::Float(*f)),
            Promised::String(s) => Ok(Ipld::String(s.clone())),
            Promised::Bytes(b) => Ok(Ipld::Bytes(b.clone())),
            Promised::Link(c) => Ok(Ipld::Link(*c)),
            Promised::WaitOk(c) => Err(WaitingOn::WaitOk(*c)),
            Promised::WaitErr(c) => Err(WaitingOn::WaitErr(*c)),
            Promised::WaitAny(c) => Err(WaitingOn::WaitAny(*c)),
            Promised::List(l) => {
                let mut resolved = Vec::new();
                for item in l {
                    resolved.push(Ipld::try_from(item)?);
                }
                Ok(Ipld::List(resolved))
            }
            Promised::Map(m) => {
                let mut resolved = BTreeMap::new();
                for (k, v) in m {
                    resolved.insert(k.clone(), Ipld::try_from(v)?);
                }
                Ok(Ipld::Map(resolved))
            }
        }
    }
}

/// Still waiting to resolve a [`Promised`] value.
#[derive(Debug, Clone, Copy, Error)]
pub enum WaitingOn {
    /// Waiting on the `Ok` branch of a promise that is not yet resolved.
    #[error("Waiting on an `ok` promise {0}")]
    WaitOk(Cid),

    /// Waiting on the `Err` branch of a promise that is not yet resolved.
    #[error("Waiting on an `err` promise {0}")]
    WaitErr(Cid),

    /// Waiting on either branch of a promise that is not yet resolved.
    #[error("Waiting on an `any` promise {0}")]
    WaitAny(Cid),
}
