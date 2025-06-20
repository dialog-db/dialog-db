use dialog_prolly_tree::ValueType;
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Serialize};

use crate::{Artifact, Cause, make_reference};

#[cfg(doc)]
use crate::{Artifacts, Attribute, Entity};

/// A [`Datum`] is the layout of data stored in one of the indexes of [`Artifacts`]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Datum {
    /// The stringified [`Entity`] associated with this [`Datum`]
    pub entity: String,
    /// The stringified [`Attribute`] associated with this [`Datum`]
    pub attribute: String,
    /// The type of the [`Value`] associated with this [`Datum`]
    pub value_type: u8,
    /// The raw byte representation of the [`Value`] associated with this [`Datum`]
    pub value: Vec<u8>,
    /// Get the [`Cause`] of this [`ValueDatum`], if any
    pub cause: Option<Cause>,
}

impl Datum {
    /// The hash reference that corresponds to this [`Datum`]'s [`Value`]
    pub fn value_reference(&self) -> Blake3Hash {
        // TODO: Cache this
        make_reference(&self.value)
    }
}

impl ValueType for Datum {}

impl From<Artifact> for Datum {
    fn from(artifact: Artifact) -> Self {
        Self {
            entity: artifact.of.to_string(),
            attribute: artifact.the.to_string(),
            value_type: artifact.is.data_type().into(),
            value: artifact.is.to_bytes(),
            cause: artifact.cause,
        }
    }
}
