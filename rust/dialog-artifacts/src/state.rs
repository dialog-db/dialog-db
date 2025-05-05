use dialog_prolly_tree::ValueType;
use serde::{Deserialize, Serialize};

use crate::DialogArtifactsError;

#[cfg(doc)]
use crate::{Artifact, ArtifactStore};

/// A [`State`] represents the presence or absence of an [`Artifact`] within a
/// [`ArtifactStore`]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum State<Datum> {
    /// An [`Artifact`] that has been asserted
    Added(Datum),
    /// An [`Artifact`] that has been retracted
    Removed,
}

impl<Datum> ValueType for State<Datum>
where
    Datum: ValueType,
    DialogArtifactsError: From<<Datum as TryFrom<Vec<u8>>>::Error>,
{
    fn serialize(&self) -> Vec<u8> {
        match self {
            State::Added(datum) => [vec![1], ValueType::serialize(datum)].concat(),
            State::Removed => vec![0],
        }
    }
}

impl<Datum> TryFrom<Vec<u8>> for State<Datum>
where
    Datum: ValueType,
    DialogArtifactsError: From<<Datum as TryFrom<Vec<u8>>>::Error>,
{
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let Some((first_byte, rest)) = value.split_first() else {
            return Err(DialogArtifactsError::InvalidState(
                "At least one byte is required".into(),
            ));
        };
        Ok(match first_byte {
            0 => State::Removed,
            1 => State::Added(Datum::try_from(rest.to_vec())?),
            any => {
                return Err(DialogArtifactsError::InvalidState(format!(
                    "Unrecognized state variant: {any}"
                )));
            }
        })
    }
}
