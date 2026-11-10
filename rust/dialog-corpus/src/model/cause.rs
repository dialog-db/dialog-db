//! Causal references for fact versioning.
//!
//! This module defines the [`Cause`] type which represents causal relationships
//! between facts, enabling proper versioning and update semantics in the
//! triple store.

use std::fmt::{Display, Formatter, Result as FmtResult};

use base58::ToBase58;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

use crate::{make_reference, reference_type};

use crate::{TypeError, ValueDataType};

use super::{Blake3Hash, Fact, Value};

/// A [`Cause`] is a reference to a [`Fact`] that preceded a more recent
/// version of the same [`Fact`] (where same implies same [`Entity`] and
/// same [`Relation`]).
#[repr(transparent)]
#[derive(
    Clone,
    Debug,
    PartialEq,
    PartialOrd,
    Serialize,
    Deserialize,
    Eq,
    Hash,
    Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Cause(pub Blake3Hash);

impl From<&Fact> for Cause {
    fn from(fact: &Fact) -> Self {
        Cause(make_reference(
            [
                fact.the.as_str().as_bytes().to_vec(),
                fact.of.as_str().as_bytes().to_vec(),
                fact.is.to_bytes(),
                (fact.cause.as_ref())
                    .map(|cause| (*cause).to_vec())
                    .unwrap_or_default(),
            ]
            .concat(),
        ))
    }
}

impl Display for Cause {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", self.0.to_base58())
    }
}

impl TryFrom<Value> for Cause {
    type Error = TypeError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bytes(b) => {
                let mut hash_bytes = [0u8; 32];
                let len = b.len().min(32);
                hash_bytes[..len].copy_from_slice(&b[..len]);
                Ok(Cause(hash_bytes))
            }
            _ => Err(TypeError::TypeMismatch(
                ValueDataType::Bytes,
                value.data_type(),
            )),
        }
    }
}

reference_type!(Cause);
