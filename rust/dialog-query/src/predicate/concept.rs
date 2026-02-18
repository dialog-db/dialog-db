use crate::application::ConceptApplication;
use crate::attribute::{AttributeSchema, Attribution};
use crate::claim::Revert;
use crate::error::SchemaError;
use crate::types::Scalar;
use crate::{
    Application, Cardinality, Claim, Entity, Field, Parameters, Relation, Requirement, Schema,
    Type, Value,
};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Not;

#[derive(Debug, Clone, PartialEq)]
pub enum Attributes {
    /// Static attributes from compile-time generated code (const-compatible)
    Static(&'static [(&'static str, AttributeSchema<Value>)]),
    /// Dynamic attributes from runtime construction
    Dynamic(Vec<(String, AttributeSchema<Value>)>),
}

/// Iterator over attribute (name, value) pairs
pub enum AttributesIter<'a> {
    Static(std::slice::Iter<'a, (&'static str, AttributeSchema<Value>)>),
    Dynamic(std::slice::Iter<'a, (String, AttributeSchema<Value>)>),
}

impl<'a> Iterator for AttributesIter<'a> {
    type Item = (&'a str, &'a AttributeSchema<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            AttributesIter::Static(iter) => iter.next().map(|(k, v)| (*k, v)),
            AttributesIter::Dynamic(iter) => iter.next().map(|(k, v)| (k.as_str(), v)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            AttributesIter::Static(iter) => iter.size_hint(),
            AttributesIter::Dynamic(iter) => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for AttributesIter<'_> {
    fn len(&self) -> usize {
        match self {
            AttributesIter::Static(iter) => iter.len(),
            AttributesIter::Dynamic(iter) => iter.len(),
        }
    }
}

impl Default for Attributes {
    fn default() -> Self {
        Self::new()
    }
}

impl Attributes {
    /// Returns an iterator over all dependencies as (name, requirement) pairs.
    pub fn iter(&self) -> AttributesIter<'_> {
        match self {
            Attributes::Static(slice) => AttributesIter::Static(slice.iter()),
            Attributes::Dynamic(vec) => AttributesIter::Dynamic(vec.iter()),
        }
    }

    pub fn count(&self) -> usize {
        match self {
            Attributes::Static(slice) => slice.len(),
            Attributes::Dynamic(vec) => vec.len(),
        }
    }

    pub fn new() -> Self {
        Attributes::Dynamic(Vec::new())
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.iter().map(|(k, _)| k)
    }

    /// Conforms the provided parameters conform to the schema of the cells.
    pub fn conform(&self, parameters: Parameters) -> Result<Parameters, SchemaError> {
        for (name, attribute) in self.iter() {
            let parameter = parameters.get(name);
            attribute
                .conform(parameter)
                .map_err(|e| e.at(name.into()))?;
        }

        Ok(parameters)
    }
}

impl<const N: usize> From<[(&str, AttributeSchema<Value>); N]> for Attributes {
    fn from(arr: [(&str, AttributeSchema<Value>); N]) -> Self {
        Attributes::Dynamic(
            arr.into_iter()
                .map(|(name, attr)| (name.to_string(), attr))
                .collect(),
        )
    }
}

impl<const N: usize> From<[(String, AttributeSchema<Value>); N]> for Attributes {
    fn from(arr: [(String, AttributeSchema<Value>); N]) -> Self {
        Attributes::Dynamic(arr.into_iter().collect())
    }
}

impl From<Vec<(&str, AttributeSchema<Value>)>> for Attributes {
    fn from(vec: Vec<(&str, AttributeSchema<Value>)>) -> Self {
        Attributes::Dynamic(
            vec.into_iter()
                .map(|(name, attr)| (name.to_string(), attr))
                .collect(),
        )
    }
}

impl From<Vec<(String, AttributeSchema<Value>)>> for Attributes {
    fn from(vec: Vec<(String, AttributeSchema<Value>)>) -> Self {
        Attributes::Dynamic(vec)
    }
}

impl From<HashMap<String, AttributeSchema<Value>>> for Attributes {
    fn from(map: HashMap<String, AttributeSchema<Value>>) -> Self {
        Attributes::Dynamic(map.into_iter().collect())
    }
}

// From static slice - creates const-compatible Static variant
impl From<&'static [(&'static str, AttributeSchema<Value>)]> for Attributes {
    fn from(slice: &'static [(&'static str, AttributeSchema<Value>)]) -> Self {
        Attributes::Static(slice)
    }
}

// Custom Serialize implementation that converts to HashMap
impl Serialize for Attributes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let map: HashMap<&str, &AttributeSchema<Value>> = self.iter().collect();
        map.serialize(serializer)
    }
}

// Custom Deserialize implementation that creates Dynamic variant
impl<'de> Deserialize<'de> for Attributes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map = HashMap::<String, AttributeSchema<Value>>::deserialize(deserializer)?;
        Ok(Attributes::from(map))
    }
}

impl From<&Attributes> for Schema {
    fn from(attributes: &Attributes) -> Self {
        let mut schema = Schema::new();
        for (name, attribute) in attributes.iter() {
            schema.insert(
                name.into(),
                Field {
                    description: attribute.description.into(),
                    content_type: attribute.content_type,
                    requirement: Requirement::Optional,
                    cardinality: attribute.cardinality,
                },
            );
        }

        // This is implied in the schema.
        if !schema.contains("this") {
            schema.insert(
                "this".into(),
                Field {
                    description: "The entity that this model represents".into(),
                    content_type: Some(Type::Entity),
                    requirement: Requirement::Optional,
                    cardinality: Cardinality::One,
                },
            );
        }

        schema
    }
}

/// Represents a concept which is a set of attributes that define an entity type.
/// Concepts are similar to tables in relational databases but are more flexible
/// as they can be derived from rules rather than just stored directly.
///
/// Concepts are identified by a blake3 hash of their attribute set, encoded
/// as a URI in the format `concept:{hash}`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Concept {
    Dynamic {
        description: String,
        attributes: Attributes,
    },
    Static {
        description: &'static str,
        attributes: &'static Attributes,
    },
}

// Manual Deserialize implementation that only supports the Dynamic variant
impl<'de> Deserialize<'de> for Concept {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct DynamicConcept {
            #[serde(default)]
            description: String,
            attributes: Attributes,
        }

        let dynamic = DynamicConcept::deserialize(deserializer)?;
        Ok(Concept::Dynamic {
            description: dynamic.description,
            attributes: dynamic.attributes,
        })
    }
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
pub struct Conception {
    /// The entity this instance represents
    pub this: Entity,
    /// The validated relations (attribute-value pairs) for this instance
    pub with: Vec<Attribution>,
}
impl Conception {
    /// Returns a reference to the entity this instance represents.
    pub fn this(&self) -> &'_ Entity {
        &self.this
    }

    /// Returns a reference to the validated relations for this instance.
    pub fn attributes(&self) -> &'_ Vec<Attribution> {
        &self.with
    }
}

impl Claim for Conception {
    fn assert(self, transaction: &mut crate::Transaction) {
        for attribution in self.with {
            transaction.associate(Relation::new(
                attribution.the,
                self.this.clone(),
                attribution.is,
            ));
        }
    }
    fn retract(self, transaction: &mut crate::Transaction) {
        for attribution in self.with {
            transaction.dissociate(Relation::new(
                attribution.the,
                self.this.clone(),
                attribution.is,
            ));
        }
    }
}

impl Not for Conception {
    type Output = Revert<Self>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}

impl Concept {
    /// Creates a new dynamic concept with the given attributes.
    pub fn new(attributes: Attributes) -> Self {
        Concept::Dynamic {
            description: String::new(),
            attributes,
        }
    }

    pub fn attributes(&self) -> &Attributes {
        match self {
            Self::Dynamic { attributes, .. } => attributes,
            Self::Static { attributes, .. } => attributes,
        }
    }

    /// Returns the concept identifier as a URI.
    ///
    /// This is a computed value based on the blake3 hash of the concept's
    /// attribute set, in the format `concept:{hash}`.
    ///
    /// Note: This method returns a String (not &str) because the identifier
    /// is computed on-demand rather than stored.
    pub fn operator(&self) -> String {
        self.to_uri()
    }

    pub fn operands(&self) -> impl Iterator<Item = &str> {
        std::iter::once("this").chain(self.attributes().keys())
    }

    pub fn schema(&self) -> Schema {
        self.attributes().into()
    }

    /// Encode this concept as CBOR for hashing
    ///
    /// Creates a CBOR-encoded representation as a map where:
    /// - Keys are attribute URIs (the:{hash}) in sorted order
    /// - Values are empty objects {}
    pub fn to_cbor_bytes(&self) -> Vec<u8> {
        use serde::Serialize;
        use std::collections::BTreeMap;

        #[derive(Serialize)]
        struct EmptyObject {}

        // Collect attribute URIs
        let mut attr_map: BTreeMap<String, EmptyObject> = BTreeMap::new();

        for (_name, schema) in self.attributes().iter() {
            let uri = schema.to_uri();
            // Use empty object as value
            attr_map.insert(uri, EmptyObject {});
        }

        serde_ipld_dagcbor::to_vec(&attr_map).expect("CBOR encoding should not fail")
    }

    /// Compute blake3 hash of this concept
    ///
    /// Returns a 32-byte blake3 hash of the CBOR-encoded concept
    pub fn hash(&self) -> blake3::Hash {
        let cbor_bytes = self.to_cbor_bytes();
        blake3::hash(&cbor_bytes)
    }

    /// Format this concept's hash as a URI
    ///
    /// Returns a string in the format: `concept:{blake3_hash_hex}`
    pub fn to_uri(&self) -> String {
        format!("concept:{}", self.hash().to_hex())
    }

    /// Parse a concept URI and extract the hash
    ///
    /// Expects format: `concept:{blake3_hash_hex}`
    /// Returns None if the format is invalid
    pub fn parse_uri(uri: &str) -> Option<blake3::Hash> {
        let uri = uri.strip_prefix("concept:")?;
        blake3::Hash::from_hex(uri).ok()
    }

    /// Creates an application for this concept.
    pub fn apply(&self, parameters: Parameters) -> Result<Application, SchemaError> {
        Ok(Application::Concept(ConceptApplication {
            terms: self.attributes().conform(parameters)?,
            concept: self.clone(),
        }))
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
    pub fn conform(&self, model: Model) -> Result<Conception, SchemaError> {
        let mut relations = vec![];
        for (name, attribute) in self.attributes().iter() {
            if let Some(value) = model.attributes.get(name) {
                let relation = attribute
                    .resolve(value.clone())
                    .map_err(|e| e.at(name.to_string()))?;
                relations.push(relation);
            } else {
                return Err(SchemaError::OmittedRequirement {
                    binding: name.into(),
                });
            }
        }
        Ok(Conception {
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
    pub fn edit(&'_ self, entity: Entity) -> Builder<'_> {
        Builder::edit(entity, self)
    }

    /// Creates a builder for creating a new entity with this concept's schema.
    ///
    /// # Returns
    /// * `Ok(Builder)` - A builder for the new entity
    /// * `Err(DialogArtifactsError)` - If entity creation fails
    pub fn create(&'_ self) -> Builder<'_> {
        Builder::new(self)
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
    pub fn new(concept: &'a Concept) -> Self {
        Self::edit(
            Entity::new().expect("should be able to generate new entity"),
            concept,
        )
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
    pub fn build(self) -> Result<Conception, SchemaError> {
        self.concept.conform(self.model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Type;

    #[dialog_common::test]
    fn test_concept_serialization_to_specific_json() {
        let attributes = <Attributes as From<_>>::from([
            (
                "name",
                AttributeSchema::<Value>::new("user", "name", "User's name", Type::String),
            ),
            (
                "age",
                AttributeSchema::<Value>::new("user", "age", "User's age", Type::UnsignedInt),
            ),
        ]);

        let concept = Concept::Dynamic {
            description: String::new(),
            attributes,
        };

        // Test serialization to JSON
        let json = serde_json::to_string(&concept).expect("Should serialize");

        // Parse the JSON to verify structure (since HashMap order isn't guaranteed)
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let obj = parsed.as_object().expect("Should be object");

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
        assert_eq!(name_attr["type"], "String");

        // Check age attribute
        let age_attr = attributes_obj["age"]
            .as_object()
            .expect("Should have age attribute");
        assert_eq!(age_attr["namespace"], "user");
        assert_eq!(age_attr["name"], "age");
        assert_eq!(age_attr["description"], "User's age");
        assert_eq!(age_attr["type"], "UnsignedInt");
    }

    #[dialog_common::test]
    fn test_concept_deserialization_from_specific_json() {
        let json = r#"{
            "operator": "person",
            "attributes": {
                "email": {
                    "namespace": "person",
                    "name": "email",
                    "description": "Person's email address",
                    "type": "String"
                },
                "active": {
                    "namespace": "person",
                    "name": "active",
                    "description": "Whether person is active",
                    "type": "Boolean"
                }
            }
        }"#;

        let concept: Concept = serde_json::from_str(json).expect("Should deserialize");

        assert!(
            concept.operator().starts_with("concept:"),
            "Operator should be a concept URI"
        );
        assert_eq!(concept.attributes().count(), 2);

        let email_attr = concept
            .attributes()
            .iter()
            .find(|(k, _)| *k == "email")
            .map(|(_, v)| v)
            .expect("Should have email attribute");
        assert_eq!(email_attr.namespace, "person");
        assert_eq!(email_attr.name, "email");
        assert_eq!(email_attr.description, "Person's email address");
        assert_eq!(email_attr.content_type, Some(Type::String));

        let active_attr = concept
            .attributes()
            .iter()
            .find(|(k, _)| *k == "active")
            .map(|(_, v)| v)
            .expect("Should have active attribute");
        assert_eq!(active_attr.namespace, "person");
        assert_eq!(active_attr.name, "active");
        assert_eq!(active_attr.description, "Whether person is active");
        assert_eq!(active_attr.content_type, Some(Type::Boolean));
    }

    #[dialog_common::test]
    fn test_concept_round_trip_serialization() {
        let original = Concept::Dynamic {
            description: String::new(),
            attributes: [(
                "score",
                AttributeSchema::<Value>::new("game", "score", "Game score", Type::UnsignedInt),
            )]
            .into(),
        };

        // Serialize then deserialize
        let json = serde_json::to_string(&original).expect("Should serialize");
        let deserialized: Concept = serde_json::from_str(&json).expect("Should deserialize");

        // Should be identical
        assert_eq!(original.operator(), deserialized.operator());
        assert_eq!(
            original.attributes().count(),
            deserialized.attributes().count()
        );

        let orig_score = original
            .attributes()
            .iter()
            .find(|(k, _)| *k == "score")
            .map(|(_, v)| v)
            .unwrap();
        let deser_score = deserialized
            .attributes()
            .iter()
            .find(|(k, _)| *k == "score")
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(orig_score.namespace, deser_score.namespace);
        assert_eq!(orig_score.name, deser_score.name);
        assert_eq!(orig_score.description, deser_score.description);
        assert_eq!(orig_score.content_type, deser_score.content_type);
    }

    #[dialog_common::test]
    fn test_expected_json_structure() {
        let concept = Concept::Dynamic {
            description: String::new(),
            attributes: Attributes::from(vec![(
                "id".to_string(),
                AttributeSchema::new("product", "id", "Product ID", Type::UnsignedInt),
            )]),
        };

        let json = serde_json::to_string_pretty(&concept).expect("Should serialize");

        let expected_structure = r#"{
  "description": "",
  "attributes": {
    "id": {
      "namespace": "product",
      "name": "id",
      "description": "Product ID",
      "type": "UnsignedInt"
    }
  }
}"#;

        let actual: serde_json::Value = serde_json::from_str(&json).expect("Should parse actual");
        let expected: serde_json::Value =
            serde_json::from_str(expected_structure).expect("Should parse expected");

        assert_eq!(
            actual, expected,
            "JSON structure should match expected format"
        );
    }

    #[dialog_common::test]
    fn test_description_serialization_and_deserialization() {
        let original_description = "A comprehensive product catalog item";

        let concept = Concept::Dynamic {
            description: original_description.to_string(),
            attributes: Attributes::from(vec![(
                "sku".to_string(),
                AttributeSchema::new("product", "sku", "Stock Keeping Unit", Type::String),
            )]),
        };

        let json = serde_json::to_string_pretty(&concept).expect("Should serialize");

        assert!(
            json.contains(original_description),
            "Serialized JSON should contain the description"
        );

        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        assert_eq!(
            parsed["description"], original_description,
            "Description field should be serialized correctly"
        );

        let deserialized: Concept = serde_json::from_str(&json).expect("Should deserialize");

        match deserialized {
            Concept::Dynamic { description, .. } => {
                assert_eq!(
                    description, original_description,
                    "Description should be preserved through round-trip"
                );
            }
            _ => panic!("Expected Dynamic concept"),
        }
    }

    #[dialog_common::test]
    fn test_concept_field_names_do_not_affect_hash() {
        let attributes1 = Attributes::from(vec![
            (
                "field_a".to_string(),
                AttributeSchema::new("person", "name", "Person's name", Type::String),
            ),
            (
                "field_b".to_string(),
                AttributeSchema::new("person", "age", "Person's age", Type::UnsignedInt),
            ),
        ]);

        let attributes2 = Attributes::from(vec![
            (
                "different_field_1".to_string(),
                AttributeSchema::new("person", "name", "Person's name", Type::String),
            ),
            (
                "different_field_2".to_string(),
                AttributeSchema::new("person", "age", "Person's age", Type::UnsignedInt),
            ),
        ]);

        let concept1 = Concept::Dynamic {
            description: String::new(),
            attributes: attributes1,
        };

        let concept2 = Concept::Dynamic {
            description: String::new(),
            attributes: attributes2,
        };

        assert_eq!(
            concept1.hash(),
            concept2.hash(),
            "Concepts with same attributes but different field names should have same hash"
        );

        assert_eq!(
            concept1.to_uri(),
            concept2.to_uri(),
            "Concepts with same attributes but different field names should have same URI"
        );
    }

    #[dialog_common::test]
    fn test_concept_description_does_not_affect_hash() {
        let attributes = Attributes::from(vec![(
            "name".to_string(),
            AttributeSchema::new("user", "name", "User's name", Type::String),
        )]);

        let concept1 = Concept::Dynamic {
            description: "A user in the system".to_string(),
            attributes: attributes.clone(),
        };

        let concept2 = Concept::Dynamic {
            description: "System user account".to_string(),
            attributes,
        };

        assert_eq!(
            concept1.hash(),
            concept2.hash(),
            "Concepts with different descriptions should have same hash"
        );

        assert_eq!(
            concept1.to_uri(),
            concept2.to_uri(),
            "Concepts with different descriptions should have same URI"
        );
    }

    #[dialog_common::test]
    fn test_concept_attribute_order_does_not_affect_hash() {
        let attributes1 = Attributes::from(vec![
            (
                "name".to_string(),
                AttributeSchema::new("person", "name", "Name", Type::String),
            ),
            (
                "age".to_string(),
                AttributeSchema::new("person", "age", "Age", Type::UnsignedInt),
            ),
        ]);

        let attributes2 = Attributes::from(vec![
            (
                "age".to_string(),
                AttributeSchema::new("person", "age", "Age", Type::UnsignedInt),
            ),
            (
                "name".to_string(),
                AttributeSchema::new("person", "name", "Name", Type::String),
            ),
        ]);

        let concept1 = Concept::Dynamic {
            description: String::new(),
            attributes: attributes1,
        };

        let concept2 = Concept::Dynamic {
            description: String::new(),
            attributes: attributes2,
        };

        assert_eq!(
            concept1.hash(),
            concept2.hash(),
            "Concepts with same attributes in different order should have same hash"
        );

        assert_eq!(
            concept1.to_uri(),
            concept2.to_uri(),
            "Concepts with same attributes in different order should have same URI"
        );
    }

    #[dialog_common::test]
    fn test_concept_different_attributes_different_hash() {
        let attributes1 = Attributes::from(vec![(
            "name".to_string(),
            AttributeSchema::new("person", "name", "Name", Type::String),
        )]);

        let attributes2 = Attributes::from(vec![(
            "email".to_string(),
            AttributeSchema::new("person", "email", "Email", Type::String),
        )]);

        let concept1 = Concept::Dynamic {
            description: String::new(),
            attributes: attributes1,
        };

        let concept2 = Concept::Dynamic {
            description: String::new(),
            attributes: attributes2,
        };

        assert_ne!(
            concept1.hash(),
            concept2.hash(),
            "Concepts with different attributes should have different hashes"
        );

        assert_ne!(
            concept1.to_uri(),
            concept2.to_uri(),
            "Concepts with different attributes should have different URIs"
        );
    }
}
