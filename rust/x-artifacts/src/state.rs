use x_prolly_tree::ValueType;

use crate::XArtifactsError;

#[cfg(doc)]
use crate::{Artifact, FactStore};

/// A [`State`] represents the presence or absence of a [`Artifact`] within a
/// [`FactStore`]
#[derive(Clone, Debug)]
pub enum State<Datum>
where
    Datum: ValueType,
{
    /// A [`Artifact`] that has been asserted
    Added(Datum),
    /// A [`Artifact`] that has been retracted
    Removed,
}

impl<Datum> ValueType for State<Datum>
where
    Datum: ValueType,
    XArtifactsError: From<<Datum as TryFrom<Vec<u8>>>::Error>,
{
    fn to_vec(&self) -> Vec<u8> {
        match self {
            State::Added(datum) => [vec![1], datum.to_vec()].concat(),
            State::Removed => vec![0],
        }
    }
}

impl<Datum> TryFrom<Vec<u8>> for State<Datum>
where
    Datum: ValueType,
    XArtifactsError: From<<Datum as TryFrom<Vec<u8>>>::Error>,
{
    type Error = XArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let Some((first_byte, rest)) = value.split_first() else {
            return Err(XArtifactsError::InvalidState(
                "At least one byte is required".into(),
            ));
        };
        Ok(match first_byte {
            0 => State::Removed,
            1 => State::Added(Datum::try_from(rest.to_vec())?),
            any => {
                return Err(XArtifactsError::InvalidState(format!(
                    "Unrecognized state variant: {any}"
                )));
            }
        })
    }
}
