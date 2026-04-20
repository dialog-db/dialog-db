//! [`Ipld`] numerics.

use std::cmp::Ordering;

use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(any(test, feature = "test_utils"))]
use arbitrary::Arbitrary;

/// The union of [`Ipld`] numeric types
///
/// This is helpful when comparing different numeric types, such as
/// bounds checking in [`Predicate`]s.
///
/// [`Predicate`]: crate::delegation::policy::predicate::Predicate
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[cfg_attr(any(test, feature = "test_utils"), derive(Arbitrary))]
pub enum Number {
    /// Designate a floating point number
    Float(f64),

    /// Designate an integer
    Integer(i128),
}

impl PartialOrd for Number {
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Number::Float(a), Number::Float(b)) => a.partial_cmp(b),
            (Number::Integer(a), Number::Integer(b)) => a.partial_cmp(b),
            (Number::Float(a), Number::Integer(b)) => {
                if *b > (f64::MAX as i128) {
                    return Some(Ordering::Less);
                }

                if *b < (f64::MIN as i128) {
                    return Some(Ordering::Greater);
                }

                a.partial_cmp(&(*b as f64))
            }
            (Number::Integer(a), Number::Float(b)) => {
                if *a > (f64::MAX as i128) {
                    return Some(Ordering::Greater);
                }

                if *a < (f64::MIN as i128) {
                    return Some(Ordering::Less);
                }

                (*a as f64).partial_cmp(b)
            }
        }
    }
}

impl From<Number> for Ipld {
    fn from(number: Number) -> Self {
        match number {
            Number::Float(f) => Ipld::Float(f),
            Number::Integer(i) => Ipld::Integer(i),
        }
    }
}

impl TryFrom<Ipld> for Number {
    type Error = NotANumber;

    fn try_from(ipld: Ipld) -> Result<Self, Self::Error> {
        match ipld {
            Ipld::Integer(i) => Ok(Number::Integer(i)),
            Ipld::Float(f) => Ok(Number::Float(f)),
            Ipld::Null
            | Ipld::Bool(_)
            | Ipld::String(_)
            | Ipld::Bytes(_)
            | Ipld::Map(_)
            | Ipld::List(_)
            | Ipld::Link(_) => Err(NotANumber(ipld)),
        }
    }
}

/// Error type for [`Number::try_from`]
#[derive(Debug, Clone, PartialEq, Error)]
#[error("Expected Ipld numeric, got: {0:?}")]
pub struct NotANumber(Ipld);

impl From<i128> for Number {
    fn from(i: i128) -> Number {
        Number::Integer(i)
    }
}

impl From<f64> for Number {
    fn from(f: f64) -> Number {
        Number::Float(f)
    }
}
