use std::fmt::Display;

use base58::ToBase58;
use serde::{Deserialize, Serialize};

use crate::{make_reference, reference_type};

use super::{Artifact, Blake3Hash};

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

reference_type!(Cause);
