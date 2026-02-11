//! Internal IPLD representation.
//!
//! This is here becuase `ipld-core` doesn't implement various traits.
//! It is not a simple newtype wrapper because IPLD has recursive values,
//! and this implementation is simpler. If it is a performance bottleneck,
//! please let the maintainers know.

use ipld_core::{cid::Cid, ipld::Ipld};
use std::collections::BTreeMap;

use crate::delegation::policy::selector::{error::SelectorErrorReason, selectable::Selectable};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(any(test, feature = "arb"), derive(arbitrary::Arbitrary))]
pub enum InternalIpld {
    /// Represents the absence of a value or the value undefined.
    Null,
    /// Represents a boolean value.
    Bool(bool),
    /// Represents an integer.
    Integer(i128),
    /// Represents a floating point value.
    Float(f64),
    /// Represents an UTF-8 string.
    String(String),
    /// Represents a sequence of bytes.
    Bytes(Vec<u8>),
    /// Represents a list.
    List(Vec<InternalIpld>),
    /// Represents a map of strings.
    Map(BTreeMap<String, InternalIpld>),
    /// Represents a map of integers.
    Link(Cid),
}

// Helps with tests
#[allow(dead_code)]
pub(crate) fn eq_with_float_nans_and_infinities(a: &InternalIpld, b: &InternalIpld) -> bool {
    match (a, b) {
        (InternalIpld::Float(x), InternalIpld::Float(y)) => {
            (x.is_nan() && y.is_nan()) || x.is_infinite() && y.is_infinite() || (x == y)
        }
        (InternalIpld::List(a_list), InternalIpld::List(b_list)) => {
            if a_list.len() != b_list.len() {
                return false;
            }
            for (a_item, b_item) in a_list.iter().zip(b_list.iter()) {
                if !eq_with_float_nans_and_infinities(a_item, b_item) {
                    return false;
                }
            }
            true
        }
        (InternalIpld::Map(a_map), InternalIpld::Map(b_map)) => {
            if a_map.len() != b_map.len() {
                return false;
            }
            for (key, a_value) in a_map {
                match b_map.get(key) {
                    Some(b_value) => {
                        if !eq_with_float_nans_and_infinities(a_value, b_value) {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        _ => a == b,
    }
}

impl From<InternalIpld> for Ipld {
    fn from(value: InternalIpld) -> Self {
        match value {
            InternalIpld::Null => Ipld::Null,
            InternalIpld::Bool(b) => Ipld::Bool(b),
            InternalIpld::Integer(i) => Ipld::Integer(i),
            InternalIpld::Float(f) => Ipld::Float(f),
            InternalIpld::String(s) => Ipld::String(s),
            InternalIpld::Bytes(b) => Ipld::Bytes(b),
            InternalIpld::List(l) => Ipld::List(l.into_iter().map(Into::into).collect()),
            InternalIpld::Map(m) => {
                let map = m.into_iter().map(|(k, v)| (k, v.into())).collect();
                Ipld::Map(map)
            }
            InternalIpld::Link(cid) => Ipld::Link(cid),
        }
    }
}

impl From<Ipld> for InternalIpld {
    fn from(ipld: Ipld) -> Self {
        match ipld {
            Ipld::Null => InternalIpld::Null,
            Ipld::Bool(b) => InternalIpld::Bool(b),
            Ipld::Integer(i) => InternalIpld::Integer(i),
            Ipld::Float(f) => InternalIpld::Float(f),
            Ipld::String(s) => InternalIpld::String(s),
            Ipld::Bytes(b) => InternalIpld::Bytes(b),
            Ipld::List(l) => {
                let list = l.into_iter().map(Into::into).collect();
                InternalIpld::List(list)
            }
            Ipld::Map(m) => {
                let map = m.into_iter().map(|(k, v)| (k, v.into())).collect();
                InternalIpld::Map(map)
            }
            Ipld::Link(cid) => InternalIpld::Link(cid),
        }
    }
}

impl Selectable for InternalIpld {
    fn try_select(ipld: Ipld) -> Result<Self, SelectorErrorReason> {
        Ok(InternalIpld::from(ipld))
    }
}
