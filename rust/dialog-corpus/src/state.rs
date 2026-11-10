use crate::ValueType;
use rkyv::Archive;
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::Fact;

/// A [`State`] represents the presence or absence of a [`Fact`] within an
/// fact index
#[derive(
    Clone, Debug, PartialEq, Serialize, Deserialize, Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum State<Datum> {
    /// A [`Fact`] that has been asserted
    Added(Datum),
    /// A [`Fact`] that has been retracted
    Removed,
}

impl<Datum> ValueType for State<Datum> where Datum: ValueType {}
