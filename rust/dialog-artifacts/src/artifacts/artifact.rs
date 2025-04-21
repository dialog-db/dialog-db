use crate::{AttributeKey, DialogArtifactsError, EntityKey, ValueDatum};

use super::{Attribute, Cause, Entity, Value};

/// A [`Artifact`] embodies a datum - a semantic triple - that may be stored in or
/// retrieved from a [`ArtifactStore`].
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Artifact {
    /// The [`Attribute`] of the [`Artifact`]; the predicate of the triple
    pub the: Attribute,
    /// The [`Entity`] of the [`Artifact`]; the subject of the triple
    pub of: Entity,
    /// The [`Value`] of the [`Artifact`]; the object of the triple
    pub is: Value,
    /// The [`Cause`] of the [`Artifact`], which is a reference to an ancester
    /// version with a different [`Value`].
    pub cause: Option<Cause>,
}

impl Artifact {
    pub fn update(self, value: Value) -> Self {
        let cause = Some(Cause::from(&self));
        Self {
            is: value,
            cause,
            ..self
        }
    }
}

impl TryFrom<(EntityKey, ValueDatum)> for Artifact {
    type Error = DialogArtifactsError;

    fn try_from((key, datum): (EntityKey, ValueDatum)) -> Result<Self, Self::Error> {
        let (is, cause) = datum.into_value_and_cause(key.value_type())?;

        Ok(Artifact {
            the: Attribute::try_from(key.attribute())?,
            of: Entity::from(key.entity()),
            is,
            cause,
        })
    }
}

impl TryFrom<(AttributeKey, ValueDatum)> for Artifact {
    type Error = DialogArtifactsError;

    fn try_from((key, datum): (AttributeKey, ValueDatum)) -> Result<Self, Self::Error> {
        let (is, cause) = datum.into_value_and_cause(key.value_type())?;

        Ok(Artifact {
            the: Attribute::try_from(key.attribute())?,
            of: Entity::from(key.entity()),
            is,
            cause,
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
            of: Entity::new(),
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
