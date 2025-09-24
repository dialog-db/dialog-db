use crate::application::ConcetApplication;
use crate::artifact::Artifact;
use crate::attribute::Relation;
use crate::claim::concept::ConceptClaim;
use crate::error::SchemaError;
use crate::fact::Scalar;
use crate::{Application, Attribute, Claim, Dependencies, Entity, Parameters, Value};
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

/// A model representing the data for a concept instance before validation.
///
/// This is an intermediate representation that holds raw values for each attribute
/// before they are validated against the concept's schema and converted into an Instance.
#[derive(Debug, Clone)]
pub struct Model {
    /// The entity that this model represents
    pub this: Entity,
    /// Raw attribute values keyed by attribute name
    pub attributes: HashMap<String, Value>,
}

/// A validated instance of a concept.
///
/// This represents a concept instance that has been validated against its schema,
/// with all attributes properly typed and confirmed to exist. Can be converted
/// to artifacts for storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    /// The entity this instance represents
    this: Entity,
    /// The validated relations (attribute-value pairs) for this instance
    with: Vec<Relation>,
}
impl Instance {
    /// Returns a reference to the entity this instance represents.
    pub fn this(&self) -> &'_ Entity {
        &self.this
    }

    /// Returns a reference to the validated relations for this instance.
    pub fn relations(&self) -> &'_ Vec<Relation> {
        &self.with
    }

    /// Converts this instance into a vector of artifacts for storage.
    ///
    /// This is a convenience method that delegates to the `From` implementation.
    pub fn into_artifacts(self) -> Vec<Artifact> {
        self.into()
    }
}

impl From<Instance> for Vec<Artifact> {
    /// Converts a concept instance into a vector of artifacts.
    ///
    /// Each relation in the instance becomes an artifact with:
    /// - `the`: The attribute identifier from the relation
    /// - `of`: The entity this instance represents
    /// - `is`: The value from the relation
    /// - `cause`: None (no causal information)
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
    /// Checks if the concept includes the given parameter name.
    /// The special "this" parameter is always considered present as it represents
    /// the entity that the concept applies to.
    pub fn contains(&self, name: &str) -> bool {
        name == "this" || self.attributes.contains_key(name)
    }

    /// Finds a parameter that is absent from the provided dependencies.
    pub fn absent(&self, dependencies: &Dependencies) -> Option<&str> {
        if !dependencies.contains("this") {
            Some("this")
        } else {
            self.attributes
                .keys()
                .find(|name| !dependencies.contains(name))
                .map(|name| name.as_str())
        }
    }

    /// Creates an application for this concept.
    pub fn apply(&self, parameters: Parameters) -> Application {
        Application::Realize(ConcetApplication {
            terms: parameters,
            concept: self.clone(),
        })
    }

    /// Validates a model against this concept's schema and creates an instance.
    ///
    /// This method:
    /// 1. Checks that all required attributes are present in the model
    /// 2. Validates that each attribute value matches the expected data type
    /// 3. Creates relations for each validated attribute-value pair
    ///
    /// # Arguments
    /// * `model` - The model containing raw attribute values to validate
    ///
    /// # Returns
    /// * `Ok(Instance)` - A validated instance if all attributes conform to schema
    /// * `Err(SchemaError)` - If any attribute is missing or has wrong type
    ///
    /// # Errors
    /// * `SchemaError::MissingProperty` - If a required attribute is missing
    /// * `SchemaError::TypeError` - If an attribute value has the wrong type
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

    /// Creates a builder for editing an existing entity with this concept's schema.
    ///
    /// # Arguments
    /// * `entity` - The entity to edit
    ///
    /// # Returns
    /// A builder that can be used to set attribute values for the entity
    pub fn edit(&self, entity: Entity) -> Builder {
        Builder::edit(entity, &self)
    }

    /// Creates a builder for creating a new entity with this concept's schema.
    ///
    /// # Returns
    /// * `Ok(Builder)` - A builder for the new entity
    /// * `Err(DialogArtifactsError)` - If entity creation fails
    pub fn new(&self) -> Result<Builder, DialogArtifactsError> {
        Builder::new(self)
    }

    /// Creates an assertion claim for a model validated against this concept.
    ///
    /// # Arguments
    /// * `model` - The model to validate and assert
    ///
    /// # Returns
    /// * `Ok(ConceptClaim)` - An assertion claim for the validated instance
    /// * `Err(SchemaError)` - If model validation fails
    pub fn assert(&self, model: Model) -> Result<ConceptClaim, SchemaError> {
        Ok(ConceptClaim::Assert(self.conform(model)?))
    }

    /// Creates a retraction claim for a model validated against this concept.
    ///
    /// # Arguments
    /// * `model` - The model to validate and retract
    ///
    /// # Returns
    /// * `Ok(ConceptClaim)` - A retraction claim for the validated instance
    /// * `Err(SchemaError)` - If model validation fails
    pub fn retract(&self, model: Model) -> Result<ConceptClaim, SchemaError> {
        Ok(ConceptClaim::Retract(self.conform(model)?))
    }
}

/// A builder for constructing concept instances with validation.
///
/// The builder pattern allows for step-by-step construction of a concept instance,
/// setting attribute values one by one before final validation and conversion to claims.
#[derive(Debug, Clone)]
pub struct Builder<'a> {
    /// Reference to the concept schema this builder validates against
    concept: &'a Concept,
    /// The model being built with attribute values
    model: Model,
}
impl<'a> Builder<'a> {
    /// Creates a new builder for a fresh entity.
    ///
    /// # Arguments
    /// * `concept` - The concept schema to validate against
    ///
    /// # Returns
    /// * `Ok(Builder)` - A new builder with a fresh entity
    /// * `Err(DialogArtifactsError)` - If entity creation fails
    pub fn new(concept: &'a Concept) -> Result<Self, DialogArtifactsError> {
        Ok(Self::edit(Entity::new()?, concept))
    }

    /// Creates a new builder for editing an existing entity.
    ///
    /// # Arguments
    /// * `this` - The entity to edit
    /// * `concept` - The concept schema to validate against
    ///
    /// # Returns
    /// A new builder for the specified entity
    pub fn edit(this: Entity, concept: &'a Concept) -> Self {
        Builder {
            concept,
            model: Model {
                this,
                attributes: HashMap::new(),
            },
        }
    }

    /// Sets an attribute value for the concept instance being built.
    ///
    /// # Arguments
    /// * `name` - The name of the attribute to set
    /// * `value` - The value to set (must implement Scalar)
    ///
    /// # Returns
    /// Self for method chaining
    ///
    /// # Example
    /// ```ignore
    /// let instance = concept.new()?
    ///     .with("name", "Alice")
    ///     .with("age", 30)
    ///     .build()?;
    /// ```
    pub fn with<T: Scalar>(mut self, name: &str, value: T) -> Self {
        self.model.attributes.insert(name.into(), value.as_value());
        self
    }

    /// Builds and validates the concept instance.
    ///
    /// # Returns
    /// * `Ok(Instance)` - A validated instance if all attributes are valid
    /// * `Err(SchemaError)` - If validation fails
    pub fn build(self) -> Result<Instance, SchemaError> {
        self.concept.conform(self.model)
    }

    /// Builds the instance and creates an assertion claim.
    ///
    /// # Returns
    /// * `Ok(Claim)` - An assertion claim for the validated instance
    /// * `Err(SchemaError)` - If validation fails
    pub fn assert(self) -> Result<Claim, SchemaError> {
        Ok(ConceptClaim::Assert(self.build()?).into())
    }

    /// Builds the instance and creates a retraction claim.
    ///
    /// # Returns
    /// * `Ok(Claim)` - A retraction claim for the validated instance
    /// * `Err(SchemaError)` - If validation fails
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
