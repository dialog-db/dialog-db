use crate::ValueType;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::{Artifact, ArtifactStore};

/// A [`State`] represents the presence or absence of an [`Artifact`] within a
/// [`ArtifactStore`]
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum State<Datum> {
    /// An [`Artifact`] that has been asserted
    Added(Datum),
    /// An [`Artifact`] that has been retracted
    Removed,
}

impl<Datum> ValueType for State<Datum> where Datum: ValueType {}
