use serde::{Deserialize, Serialize};

use crate::{make_reference, reference_type};

use super::{Artifact, Blake3Hash};

/// A [`Cause`] is a reference to an [`Artifact`] that preceded a more recent
/// version of the same [`Artifact`] (where same implies same [`Entity`] and
/// same [`Attribute`]).
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Cause(Blake3Hash);

impl From<&Artifact> for Cause {
    fn from(artifact: &Artifact) -> Self {
        Cause(make_reference(
            [
                artifact.the.key_bytes().to_vec(),
                (*artifact.of).to_vec(),
                artifact.is.to_bytes(),
                (artifact.cause.as_ref())
                    .map(|cause| (*cause).to_vec())
                    .unwrap_or_default(),
            ]
            .concat(),
        ))
    }
}

reference_type!(Cause);
