use std::{fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{Datum, DialogArtifactsError};

use super::{Attribute, Cause, Entity, Value, ValueDataType};

/// A [`Artifact`] embodies a datum - a semantic triple - that may be stored in or
/// retrieved from a [`ArtifactStore`].
#[derive(Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Artifact {
    /// The [`Attribute`] of the [`Artifact`]; the predicate of the triple
    pub the: Attribute,
    /// The [`Entity`] of the [`Artifact`]; the subject of the triple
    #[serde(
        serialize_with = "crate::artifacts::entity::to_utf8",
        deserialize_with = "crate::artifacts::entity::from_utf8"
    )]
    pub of: Entity,
    /// The [`Value`] of the [`Artifact`]; the object of the triple
    // TODO: This is in support of Artifacts<->CSV but we probably want
    // different (de)serialization for Artifacts<->JSON (assuming we ever
    // want that.
    #[serde(
        serialize_with = "crate::artifacts::value::to_utf8",
        deserialize_with = "crate::artifacts::value::from_utf8"
    )]
    pub is: Value,
    /// The [`Cause`] of the [`Artifact`], which is a reference to an ancester
    /// version with a different [`Value`].
    pub cause: Option<Cause>,
}

impl Artifact {
    /// Change the value of the [`Artifact`], assigning the hash of its
    /// antecedent as the `cause`.
    pub fn update(self, value: Value) -> Self {
        let cause = Some(Cause::from(&self));
        Self {
            is: value,
            cause,
            ..self
        }
    }
}

impl std::fmt::Debug for Artifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Artifact")
            .field("the", &self.the.to_string())
            .field("of", &self.of.to_string())
            .field("is", &self.is)
            .field("cause", &self.cause.as_ref().map(|cause| cause.to_string()))
            .finish()
    }
}

impl Display for Artifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attribute = self.the.to_string();
        let entity = format!("{}", &self.of);
        let value = self.is.to_utf8();

        write!(f, "Artifact: the '{attribute}' of '{entity}' is '{value}'")
    }
}

impl TryFrom<Datum> for Artifact {
    type Error = DialogArtifactsError;

    fn try_from(value: Datum) -> Result<Self, Self::Error> {
        Ok(Artifact {
            the: Attribute::from_str(&value.attribute)?,
            of: Entity::from_str(&value.entity)?,
            is: Value::try_from((ValueDataType::from(value.value_type), value.value))?,
            cause: value.cause,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use anyhow::Result;

    use crate::{Attribute, Cause, Entity, Value};

    use super::Artifact;

    #[test]
    fn it_points_to_causal_ancestor_when_updated() -> Result<()> {
        let artifact = Artifact {
            the: Attribute::from_str("test/predicate")?,
            of: Entity::new()?,
            is: Value::Boolean(false),
            cause: None,
        };
        let causal_reference = Cause::from(&artifact);
        let descendent = artifact.update(Value::Boolean(true));

        assert_eq!(descendent.is, Value::Boolean(true));
        assert_eq!(descendent.cause, Some(causal_reference));

        Ok(())
    }
}
