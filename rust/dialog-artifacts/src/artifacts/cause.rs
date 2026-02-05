//! Causal references for artifact versioning.
//!
//! This module defines the [`Cause`] type which represents causal relationships
//! between artifacts, enabling proper versioning and update semantics in the
//! triple store.

use std::fmt::Display;

use base58::ToBase58;
use serde::{Deserialize, Serialize};

use crate::{make_reference, reference_type};

use crate::{TypeError, ValueDataType};

use super::{Artifact, Blake3Hash, Value};

/// A [`Cause`] is a reference to an [`Artifact`] that preceded a more recent
/// version of the same [`Artifact`] (where same implies same [`Entity`] and
/// same [`Attribute`]).
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Eq, Hash)]
pub struct Cause(pub Blake3Hash);

impl From<&Artifact> for Cause {
    fn from(artifact: &Artifact) -> Self {
        Cause(make_reference(
            [
                artifact.the.key_bytes().to_vec(),
                artifact.of.key_bytes().to_vec(),
                artifact.is.to_bytes(),
                (artifact.cause.as_ref())
                    .map(|cause| (*cause).to_vec())
                    .unwrap_or_default(),
            ]
            .concat(),
        ))
    }
}

impl Display for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
