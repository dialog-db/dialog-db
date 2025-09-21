use crate::application::ConcetApplication;
use crate::artifact::Artifact;
use crate::attribute::Relation;
use crate::claim::concept::ConceptClaim;
use crate::error::SchemaError;
use crate::fact::Scalar;
use crate::{Application, Attribute, Claim, Entity, Parameters, Value};
use dialog_artifacts::DialogArtifactsError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a concept which is a set of attributes that define an entity type.
/// Concepts are similar to tables in relational databases but are more flexible
/// as they can be derived from rules rather than just stored directly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Concept {
    /// Concept identifier used to look concepts up by.
    pub operator: String,
    /// Map of attribute names to their definitions for this concept.
    pub attributes: HashMap<String, Attribute<Value>>,
}

#[derive(Debug, Clone)]
pub struct Model {
    pub this: Entity,
    pub attributes: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    this: Entity,
    with: Vec<Relation>,
}
impl Instance {
    pub fn this(&self) -> &'_ Entity {
        &self.this
    }
    pub fn relations(&self) -> &'_ Vec<Relation> {
        &self.with
    }

    pub fn into_artifacts(self) -> Vec<Artifact> {
        self.into()
    }
}

impl From<Instance> for Vec<Artifact> {
    fn from(value: Instance) -> Self {
        let mut artifacts = vec![];
        for relation in value.with {
            artifacts.push(Artifact {
                the: relation.the,
                of: value.this.clone(),
                is: relation.is,
                cause: None,
            })
        }

        artifacts
    }
}

impl Concept {
    pub fn apply(&self, parameters: Parameters) -> Application {
        Application::Realize(ConcetApplication {
            terms: parameters,
            concept: self.clone(),
        })
    }

    pub fn conform(&self, model: Model) -> Result<Instance, SchemaError> {
        let mut relations = vec![];
        for (name, attribute) in &self.attributes {
            if let Some(value) = model.attributes.get(name) {
                let relation = attribute.conform(value.clone())?;
                relations.push(relation);
            } else {
                return Err(SchemaError::MissingProperty {
                    property: name.into(),
                });
            }
        }
        Ok(Instance {
            this: model.this,
            with: relations,
        })
    }

    pub fn edit(&self, entity: Entity) -> Builder {
        Builder::edit(entity, &self)
    }
    pub fn new(&self) -> Result<Builder, DialogArtifactsError> {
        Builder::new(self)
    }

    pub fn assert(&self, model: Model) -> Result<ConceptClaim, SchemaError> {
        Ok(ConceptClaim::Assert(self.conform(model)?))
    }
    pub fn retract(&self, model: Model) -> Result<ConceptClaim, SchemaError> {
        Ok(ConceptClaim::Retract(self.conform(model)?))
    }
}

#[derive(Debug, Clone)]
pub struct Builder<'a> {
    concept: &'a Concept,
    model: Model,
}
impl<'a> Builder<'a> {
    pub fn new(concept: &'a Concept) -> Result<Self, DialogArtifactsError> {
        Ok(Self::edit(Entity::new()?, concept))
    }
    pub fn edit(this: Entity, concept: &'a Concept) -> Self {
        Builder {
            concept,
            model: Model {
                this,
                attributes: HashMap::new(),
            },
        }
    }

    pub fn with<T: Scalar>(mut self, name: &str, value: T) -> Self {
        self.model.attributes.insert(name.into(), value.as_value());
        self
    }

    pub fn build(self) -> Result<Instance, SchemaError> {
        self.concept.conform(self.model)
    }

    pub fn assert(self) -> Result<Claim, SchemaError> {
        Ok(ConceptClaim::Assert(self.build()?).into())
    }

    pub fn retract(self) -> Result<Claim, SchemaError> {
        Ok(ConceptClaim::Retract(self.build()?).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::ValueDataType;

    #[test]
    fn test_concept_serialization_to_specific_json() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "name".to_string(),
            Attribute::new("user", "name", "User's name", ValueDataType::String),
        );
        attributes.insert(
            "age".to_string(),
            Attribute::new("user", "age", "User's age", ValueDataType::UnsignedInt),
        );

        let concept = Concept {
            operator: "user".to_string(),
            attributes,
        };

        // Test serialization to JSON
        let json = serde_json::to_string(&concept).expect("Should serialize");

        // Parse the JSON to verify structure (since HashMap order isn't guaranteed)
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let obj = parsed.as_object().expect("Should be object");

        // Check operator
        assert_eq!(obj["operator"], "user");

        // Check attributes structure
        let attributes_obj = obj["attributes"]
            .as_object()
            .expect("Should have attributes object");
        assert_eq!(attributes_obj.len(), 2);

        // Check name attribute
        let name_attr = attributes_obj["name"]
            .as_object()
            .expect("Should have name attribute");
        assert_eq!(name_attr["namespace"], "user");
        assert_eq!(name_attr["name"], "name");
        assert_eq!(name_attr["description"], "User's name");
        assert_eq!(name_attr["data_type"], "String");

        // Check age attribute
        let age_attr = attributes_obj["age"]
            .as_object()
            .expect("Should have age attribute");
        assert_eq!(age_attr["namespace"], "user");
        assert_eq!(age_attr["name"], "age");
        assert_eq!(age_attr["description"], "User's age");
        assert_eq!(age_attr["data_type"], "UnsignedInt");
    }

    #[test]
    fn test_concept_deserialization_from_specific_json() {
        let json = r#"{
            "operator": "person",
            "attributes": {
                "email": {
                    "namespace": "person",
                    "name": "email",
                    "description": "Person's email address",
                    "data_type": "String"
                },
                "active": {
                    "namespace": "person",
                    "name": "active",
                    "description": "Whether person is active",
                    "data_type": "Boolean"
                }
            }
        }"#;

        let concept: Concept = serde_json::from_str(json).expect("Should deserialize");

        assert_eq!(concept.operator, "person");
        assert_eq!(concept.attributes.len(), 2);

        let email_attr = concept
            .attributes
            .get("email")
            .expect("Should have email attribute");
        assert_eq!(email_attr.namespace, "person");
        assert_eq!(email_attr.name, "email");
        assert_eq!(email_attr.description, "Person's email address");
        assert_eq!(email_attr.data_type, ValueDataType::String);

        let active_attr = concept
            .attributes
            .get("active")
            .expect("Should have active attribute");
        assert_eq!(active_attr.namespace, "person");
        assert_eq!(active_attr.name, "active");
        assert_eq!(active_attr.description, "Whether person is active");
        assert_eq!(active_attr.data_type, ValueDataType::Boolean);
    }

    #[test]
    fn test_concept_round_trip_serialization() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "score".to_string(),
            Attribute::new("game", "score", "Game score", ValueDataType::UnsignedInt),
        );

        let original = Concept {
            operator: "game".to_string(),
            attributes,
        };

        // Serialize then deserialize
        let json = serde_json::to_string(&original).expect("Should serialize");
        let deserialized: Concept = serde_json::from_str(&json).expect("Should deserialize");

        // Should be identical
        assert_eq!(original.operator, deserialized.operator);
        assert_eq!(original.attributes.len(), deserialized.attributes.len());

        let orig_score = original.attributes.get("score").unwrap();
        let deser_score = deserialized.attributes.get("score").unwrap();
        assert_eq!(orig_score.namespace, deser_score.namespace);
        assert_eq!(orig_score.name, deser_score.name);
        assert_eq!(orig_score.description, deser_score.description);
        assert_eq!(orig_score.data_type, deser_score.data_type);
    }
}
