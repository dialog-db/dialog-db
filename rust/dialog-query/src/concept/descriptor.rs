/// Named attribute collections for concept descriptors.
mod named_attributes;
pub use named_attributes::NamedAttributes;

use crate::Predicate;
use crate::assertion::Retraction;
use crate::attribute::{AttributeDescriptor, Attribution};
use crate::concept::application::ConceptQuery;
use crate::concept::{Concept, Conclusion};
use crate::error::SchemaError;
use crate::query::{Application, Source};
use crate::selection::{Answer, Answers};
use crate::term::Term;
use crate::types::Scalar;
use crate::{
    Assertion, Association, Cardinality, Entity, Field, Parameters, Proposition, QueryError,
    Requirement, Schema, Transaction, Type, Value,
};

use base58::ToBase58;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Not;

/// A concept descriptor — a named set of attribute descriptors that together
/// describe an entity type. Concepts are similar to tables in relational
/// databases but are more flexible as they can be derived from rules rather
/// than just stored directly.
///
/// Concepts are identified by a blake3 hash of their attribute set, encoded
/// as a URI in the format `concept:{hash}`.
///
/// Serializes to the formal notation:
/// ```json
/// { "description": "...", "with": { "fieldName": { "the": "domain/name", ... } } }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConceptDescriptor {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    with: NamedAttributes,
    /// Optional attributes — not yet supported by the query engine.
    ///
    /// Accepted during deserialization to ensure documents written against
    /// the full schema are validated now rather than silently accepted and
    /// broken later when `maybe` support lands.  Skipped during
    /// serialization because the engine never produces them.
    ///
    /// An empty map deserializes to `None` (treated as absent).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_maybe"
    )]
    maybe: Option<NamedAttributes>,
}

impl ConceptDescriptor {
    /// Returns the description of this concept, if any.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns a reference to the named attributes.
    pub fn with(&self) -> &NamedAttributes {
        &self.with
    }

    /// Validates the provided parameters against the schema of the attributes.
    pub fn conform(&self, parameters: Parameters) -> Result<Parameters, SchemaError> {
        for (name, attribute) in self.with().iter() {
            let parameter = parameters.get(name);
            attribute
                .conform(parameter)
                .map_err(|e| e.at(name.into()))?;
        }

        Ok(parameters)
    }

    /// Returns an iterator over operand names, starting with "this" followed by attribute keys.
    pub fn operands(&self) -> impl Iterator<Item = &str> {
        std::iter::once("this").chain(self.with().keys())
    }

    /// Derives a `Schema` from this descriptor's attributes.
    pub fn schema(&self) -> Schema {
        Schema::from(self)
    }

    /// Encode this concept as CBOR for hashing.
    ///
    /// Creates a CBOR-encoded representation as a map where:
    /// - Keys are attribute URIs (the:{hash}) in sorted order
    /// - Values are empty objects {}
    pub fn to_cbor_bytes(&self) -> Vec<u8> {
        use serde::Serialize;
        use std::collections::BTreeMap;

        #[derive(Serialize)]
        struct EmptyObject {}

        let mut attr_map: BTreeMap<String, EmptyObject> = BTreeMap::new();

        for (_name, schema) in self.with().iter() {
            let uri = schema.to_uri();
            attr_map.insert(uri, EmptyObject {});
        }

        serde_ipld_dagcbor::to_vec(&attr_map).expect("CBOR encoding should not fail")
    }

    /// Compute blake3 hash of this concept.
    ///
    /// Returns a 32-byte blake3 hash of the CBOR-encoded concept.
    pub fn hash(&self) -> blake3::Hash {
        let cbor_bytes = self.to_cbor_bytes();
        blake3::hash(&cbor_bytes)
    }

    /// Identityfier for this concept (as in type identifier and not instance
    /// identifier)
    pub fn this(&self) -> Entity {
        let encoded = self.hash().as_bytes().as_ref().to_base58();
        format!("concept:{encoded}")
            .parse()
            .expect("valid entity URI")
    }

    /// Creates a query application for this concept descriptor.
    pub fn apply(&self, parameters: Parameters) -> Result<Proposition, SchemaError> {
        Ok(Proposition::Concept(ConceptQuery {
            terms: self.conform(parameters)?,
            predicate: self.clone(),
        }))
    }

    /// Validates a model against this descriptor's schema and creates an instance.
    fn conform_model(&self, model: Model) -> Result<Conception, SchemaError> {
        let mut relations = vec![];
        for (name, attribute) in self.with().iter() {
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

    /// Creates a builder for editing an existing entity with this descriptor's schema.
    pub fn edit(&self, entity: Entity) -> Builder<'_> {
        Builder::edit(entity, self)
    }

    /// Creates a builder for creating a new entity with this descriptor's schema.
    pub fn create(&self) -> Builder<'_> {
        Builder::new(self)
    }
}

impl<const N: usize> From<[(&str, AttributeDescriptor); N]> for ConceptDescriptor {
    fn from(arr: [(&str, AttributeDescriptor); N]) -> Self {
        ConceptDescriptor {
            description: None,
            maybe: None,
            with: NamedAttributes::from(arr),
        }
    }
}

impl<const N: usize> From<[(String, AttributeDescriptor); N]> for ConceptDescriptor {
    fn from(arr: [(String, AttributeDescriptor); N]) -> Self {
        ConceptDescriptor {
            description: None,
            maybe: None,
            with: NamedAttributes::from(arr),
        }
    }
}

impl From<Vec<(&str, AttributeDescriptor)>> for ConceptDescriptor {
    fn from(vec: Vec<(&str, AttributeDescriptor)>) -> Self {
        ConceptDescriptor {
            description: None,
            maybe: None,
            with: NamedAttributes::from(vec),
        }
    }
}

impl From<Vec<(String, AttributeDescriptor)>> for ConceptDescriptor {
    fn from(vec: Vec<(String, AttributeDescriptor)>) -> Self {
        ConceptDescriptor {
            description: None,
            maybe: None,
            with: NamedAttributes::from(vec),
        }
    }
}

impl From<HashMap<String, AttributeDescriptor>> for ConceptDescriptor {
    fn from(map: HashMap<String, AttributeDescriptor>) -> Self {
        ConceptDescriptor {
            description: None,
            maybe: None,
            with: NamedAttributes::from(map),
        }
    }
}

impl From<&ConceptDescriptor> for Schema {
    fn from(predicate: &ConceptDescriptor) -> Self {
        let mut schema = Schema::new();
        for (name, attribute) in predicate.with().iter() {
            schema.insert(
                name.into(),
                Field {
                    description: attribute.description().into(),
                    content_type: attribute.content_type(),
                    requirement: Requirement::Optional,
                    cardinality: attribute.cardinality(),
                },
            );
        }

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

/// Deserializes an optional `NamedAttributes` map, treating an empty map as `None`.
fn deserialize_maybe<'de, D>(deserializer: D) -> Result<Option<NamedAttributes>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let map = HashMap::<String, AttributeDescriptor>::deserialize(deserializer)?;
    if map.is_empty() {
        Ok(None)
    } else {
        Ok(Some(NamedAttributes::from(map)))
    }
}

/// A model representing the data for a concept instance before validation.
#[derive(Debug, Clone)]
struct Model {
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

impl Assertion for Conception {
    fn assert(self, transaction: &mut Transaction) {
        for attribution in self.with {
            transaction.associate(Association::new(
                attribution.the.into(),
                self.this.clone(),
                attribution.is,
            ));
        }
    }
    fn retract(self, transaction: &mut Transaction) {
        for attribution in self.with {
            transaction.dissociate(Association::new(
                attribution.the.into(),
                self.this.clone(),
                attribution.is,
            ));
        }
    }
}

impl Not for Conception {
    type Output = Retraction<Self>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}

/// A builder for constructing concept instances with validation.
#[derive(Debug, Clone)]
pub struct Builder<'a> {
    predicate: &'a ConceptDescriptor,
    model: Model,
}
impl<'a> Builder<'a> {
    /// Creates a new builder for a fresh entity.
    pub fn new(predicate: &'a ConceptDescriptor) -> Self {
        Self::edit(
            Entity::new().expect("should be able to generate new entity"),
            predicate,
        )
    }

    /// Creates a new builder for editing an existing entity.
    pub fn edit(this: Entity, predicate: &'a ConceptDescriptor) -> Self {
        Builder {
            predicate,
            model: Model {
                this,
                attributes: HashMap::new(),
            },
        }
    }

    /// Sets an attribute value for the concept instance being built.
    pub fn with<T: Scalar>(mut self, name: &str, value: T) -> Self {
        self.model.attributes.insert(name.into(), value.as_value());
        self
    }

    /// Builds and validates the concept instance.
    pub fn build(self) -> Result<Conception, SchemaError> {
        self.predicate.conform_model(self.model)
    }
}

/// A dynamic conclusion — an entity with its resolved field values.
///
/// Field values are accessed by the term bindings from the query.
/// The `terms` map provides the mapping from field names to variable terms
/// used in the answer.
#[derive(Debug, Clone)]
pub struct ConceptConclusion {
    this: Entity,
    terms: Parameters,
    answer: Answer,
}

impl ConceptConclusion {
    /// Returns the entity this conclusion describes.
    pub fn entity(&self) -> &Entity {
        &self.this
    }

    /// Look up a field value by its concept field name (e.g. "name", "age").
    pub fn get<T>(&self, field: &str) -> Result<T, QueryError>
    where
        T: Scalar + std::convert::TryFrom<Value>,
    {
        let term = self
            .terms
            .get(field)
            .ok_or_else(|| QueryError::UnboundVariable {
                variable_name: field.to_string(),
            })?;
        // Extract the variable name from the Term<Value> and create a typed Term<T>
        let typed_term: Term<T> = match term {
            Term::Variable { name, .. } => {
                let var_name = name.as_ref().ok_or_else(|| QueryError::UnboundVariable {
                    variable_name: field.to_string(),
                })?;
                Term::var(var_name.clone())
            }
            Term::Constant(value) => {
                return T::try_from(value.clone()).map_err(|_| QueryError::UnboundVariable {
                    variable_name: field.to_string(),
                });
            }
        };
        self.answer.get(&typed_term).map_err(QueryError::from)
    }

    /// Returns a reference to the raw answer.
    pub fn answer(&self) -> &Answer {
        &self.answer
    }
}

impl Conclusion for ConceptConclusion {
    fn this(&self) -> &Entity {
        &self.this
    }
}

impl From<ConceptDescriptor> for Entity {
    fn from(predicate: ConceptDescriptor) -> Self {
        predicate.this()
    }
}

impl From<ConceptQuery> for ConceptDescriptor {
    fn from(app: ConceptQuery) -> Self {
        app.predicate
    }
}

impl Application for ConceptQuery {
    type Conclusion = ConceptConclusion;

    fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        ConceptQuery::evaluate(self, answers, source)
    }

    fn realize(&self, source: Answer) -> Result<Self::Conclusion, QueryError> {
        let this_term = self
            .terms
            .get("this")
            .ok_or_else(|| QueryError::UnboundVariable {
                variable_name: "this".to_string(),
            })?;
        let entity: Entity = match this_term {
            Term::Variable { name, .. } => {
                let var_name = name.as_ref().ok_or_else(|| QueryError::UnboundVariable {
                    variable_name: "this".to_string(),
                })?;
                let typed_term: Term<Entity> = Term::var(var_name.clone());
                source.get(&typed_term)?
            }
            Term::Constant(value) => match value {
                Value::Entity(e) => e.clone(),
                _ => {
                    return Err(QueryError::UnboundVariable {
                        variable_name: "this".to_string(),
                    });
                }
            },
        };
        Ok(ConceptConclusion {
            this: entity,
            terms: self.terms.clone(),
            answer: source,
        })
    }
}

impl Predicate for ConceptDescriptor {
    type Conclusion = ConceptConclusion;
    type Application = ConceptQuery;
    type Descriptor = ConceptDescriptor;
}

impl Concept for ConceptDescriptor {
    type Term = ();

    fn this(&self) -> Entity {
        let encoded = self.hash().as_bytes().as_ref().to_base58();
        format!("concept:{encoded}")
            .parse()
            .expect("valid entity URI")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Type;
    use crate::the;

    #[dialog_common::test]
    fn it_serializes_to_expected_json() {
        let predicate = ConceptDescriptor::from([
            (
                "name",
                AttributeDescriptor::new(
                    the!("user/name"),
                    "User's name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("user/age"),
                    "User's age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        let json = serde_json::to_string(&predicate).expect("Should serialize");

        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let obj = parsed.as_object().expect("Should be object");

        let with_obj = obj["with"].as_object().expect("Should have 'with' wrapper");
        assert_eq!(with_obj.len(), 2);

        let name_attr = with_obj["name"]
            .as_object()
            .expect("Should have name attribute");
        assert_eq!(name_attr["the"], "user/name");
        assert_eq!(name_attr["description"], "User's name");
        assert_eq!(name_attr["cardinality"], "one");
        assert_eq!(name_attr["as"], "Text");

        let age_attr = with_obj["age"]
            .as_object()
            .expect("Should have age attribute");
        assert_eq!(age_attr["the"], "user/age");
        assert_eq!(age_attr["description"], "User's age");
        assert_eq!(age_attr["cardinality"], "one");
        assert_eq!(age_attr["as"], "UnsignedInteger");
    }

    #[dialog_common::test]
    fn it_deserializes_from_json() {
        let json = r#"{
            "with": {
                "email": {
                    "the": "person/email",
                    "description": "Person's email address",
                    "as": "Text"
                },
                "active": {
                    "the": "person/active",
                    "description": "Whether person is active",
                    "as": "Boolean"
                }
            }
        }"#;

        let predicate: ConceptDescriptor = serde_json::from_str(json).expect("Should deserialize");

        assert!(
            predicate.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );
        assert_eq!(predicate.with().iter().count(), 2);

        let email_attr = predicate
            .with()
            .iter()
            .find(|(k, _)| *k == "email")
            .map(|(_, v)| v)
            .expect("Should have email attribute");
        assert_eq!(email_attr.domain(), "person");
        assert_eq!(email_attr.name(), "email");
        assert_eq!(email_attr.description(), "Person's email address");
        assert_eq!(email_attr.content_type(), Some(Type::String));

        let active_attr = predicate
            .with()
            .iter()
            .find(|(k, _)| *k == "active")
            .map(|(_, v)| v)
            .expect("Should have active attribute");
        assert_eq!(active_attr.domain(), "person");
        assert_eq!(active_attr.name(), "active");
        assert_eq!(active_attr.description(), "Whether person is active");
        assert_eq!(active_attr.content_type(), Some(Type::Boolean));
    }

    #[dialog_common::test]
    fn it_deserializes_with_description() {
        let json = r#"{
            "description": "A user profile",
            "with": {
                "name": {
                    "the": "user/name",
                    "description": "User's name",
                    "as": "Text"
                }
            }
        }"#;

        let predicate: ConceptDescriptor = serde_json::from_str(json).expect("Should deserialize");
        assert_eq!(predicate.description(), Some("A user profile"));
        assert_eq!(predicate.with().iter().count(), 1);
    }

    #[dialog_common::test]
    fn it_serializes_with_description() {
        let mut predicate = ConceptDescriptor::from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User's name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);
        predicate.description = Some("A user profile".to_string());

        let json = serde_json::to_string(&predicate).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        assert_eq!(parsed["description"], "A user profile");
        assert!(parsed["with"].is_object());
    }

    #[dialog_common::test]
    fn it_omits_null_description_in_json() {
        let predicate = ConceptDescriptor::from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User's name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        let json = serde_json::to_string(&predicate).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        assert!(parsed.get("description").is_none());
        assert!(parsed["with"].is_object());
    }

    #[dialog_common::test]
    fn it_round_trips_through_json() {
        let original = ConceptDescriptor::from([(
            "score",
            AttributeDescriptor::new(
                the!("game/score"),
                "Game score",
                Cardinality::One,
                Some(Type::UnsignedInt),
            ),
        )]);

        let json = serde_json::to_string(&original).expect("Should serialize");
        let deserialized: ConceptDescriptor =
            serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(original.this(), deserialized.this());
        assert_eq!(
            original.with().iter().count(),
            deserialized.with().iter().count()
        );

        let orig_score = original
            .with()
            .iter()
            .find(|(k, _)| *k == "score")
            .map(|(_, v)| v)
            .unwrap();
        let deser_score = deserialized
            .with()
            .iter()
            .find(|(k, _)| *k == "score")
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(orig_score.domain(), deser_score.domain());
        assert_eq!(orig_score.name(), deser_score.name());
        assert_eq!(orig_score.description(), deser_score.description());
        assert_eq!(orig_score.content_type(), deser_score.content_type());
    }

    #[dialog_common::test]
    fn it_produces_expected_json_structure() {
        let predicate = ConceptDescriptor::from(vec![(
            "id".to_string(),
            AttributeDescriptor::new(
                the!("product/id"),
                "Product ID",
                Cardinality::One,
                Some(Type::UnsignedInt),
            ),
        )]);

        let json = serde_json::to_string_pretty(&predicate).expect("Should serialize");

        let expected_structure = r#"{
  "with": {
    "id": {
      "the": "product/id",
      "description": "Product ID",
      "cardinality": "one",
      "as": "UnsignedInteger"
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
    fn it_ignores_field_names_in_hash() {
        let pred1 = ConceptDescriptor::from(vec![
            (
                "field_a".to_string(),
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person's name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "field_b".to_string(),
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person's age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        let pred2 = ConceptDescriptor::from(vec![
            (
                "different_field_1".to_string(),
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Person's name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "different_field_2".to_string(),
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Person's age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        assert_eq!(
            pred1.hash(),
            pred2.hash(),
            "Concepts with same attributes but different field names should have same hash"
        );

        assert_eq!(
            pred1.this().to_string(),
            pred2.this().to_string(),
            "Concepts with same attributes but different field names should have same URI"
        );
    }

    #[dialog_common::test]
    fn it_ignores_attribute_order_in_hash() {
        let pred1 = ConceptDescriptor::from(vec![
            (
                "name".to_string(),
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age".to_string(),
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        let pred2 = ConceptDescriptor::from(vec![
            (
                "age".to_string(),
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
            (
                "name".to_string(),
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ]);

        assert_eq!(
            pred1.hash(),
            pred2.hash(),
            "Concepts with same attributes in different order should have same hash"
        );

        assert_eq!(
            pred1.this().to_string(),
            pred2.this().to_string(),
            "Concepts with same attributes in different order should have same URI"
        );
    }

    #[dialog_common::test]
    fn it_hashes_differently_for_different_attributes() {
        let pred1 = ConceptDescriptor::from(vec![(
            "name".to_string(),
            AttributeDescriptor::new(
                the!("person/name"),
                "Name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        let pred2 = ConceptDescriptor::from(vec![(
            "email".to_string(),
            AttributeDescriptor::new(
                the!("person/email"),
                "Email",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        assert_ne!(
            pred1.hash(),
            pred2.hash(),
            "Concepts with different attributes should have different hashes"
        );

        assert_ne!(
            pred1.this().to_string(),
            pred2.this().to_string(),
            "Concepts with different attributes should have different URIs"
        );
    }

    /// Validates serialized output conforms to dialog-schema.json Concept definition:
    /// - Top-level object
    /// - "with" key is required, maps field names to Attribute objects
    /// - "description" key is optional string
    /// - Each Attribute has "the" (required), optional "description", "cardinality", "as"
    /// - No unexpected top-level keys (only "description", "with")
    #[dialog_common::test]
    fn it_conforms_to_json_schema() {
        let predicate = ConceptDescriptor::from([
            (
                "name",
                AttributeDescriptor::new(
                    the!("user/name"),
                    "User's name",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("user/age"),
                    "User's age",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);

        let json = serde_json::to_string(&predicate).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let obj = parsed.as_object().expect("Top-level must be object");

        // Only allowed top-level keys per schema: "description", "with", "maybe"
        for key in obj.keys() {
            assert!(
                ["description", "with", "maybe"].contains(&key.as_str()),
                "Unexpected top-level key: {key}"
            );
        }

        // "with" is required per schema
        let with = obj
            .get("with")
            .expect("'with' is required by schema")
            .as_object()
            .expect("'with' must be an object (NamedRelations)");

        // "with" must have at least one entry (minProperties: 1)
        assert!(
            !with.is_empty(),
            "'with' must have at least one attribute (minProperties: 1)"
        );

        // Each attribute in "with" must conform to Attribute schema
        for (field_name, attr_value) in with {
            let attr = attr_value
                .as_object()
                .unwrap_or_else(|| panic!("Attribute '{field_name}' must be an object"));

            // "the" is required per Attribute schema
            let the = attr
                .get("the")
                .unwrap_or_else(|| panic!("Attribute '{field_name}' must have 'the'"));
            let the_str = the
                .as_str()
                .unwrap_or_else(|| panic!("'the' in '{field_name}' must be a string"));

            // "the" must match domain/name pattern
            assert!(
                the_str.contains('/'),
                "'the' in '{field_name}' must be in domain/name format, got: {the_str}"
            );

            // Only allowed attribute keys per schema
            for key in attr.keys() {
                assert!(
                    ["the", "description", "cardinality", "as"].contains(&key.as_str()),
                    "Unexpected key '{key}' in attribute '{field_name}'"
                );
            }

            // "cardinality" if present must be "one" or "many"
            if let Some(card) = attr.get("cardinality") {
                let card_str = card.as_str().expect("cardinality must be a string");
                assert!(
                    ["one", "many"].contains(&card_str),
                    "Invalid cardinality '{card_str}' in '{field_name}'"
                );
            }
        }
    }

    /// Validates that a schema-conformant Concept fixture (as an external user
    /// would write it) round-trips correctly through deserialization and
    /// re-serialization.
    #[dialog_common::test]
    fn it_round_trips_schema_conformant_fixture() {
        let fixture = r#"{
            "description": "A recipe ingredient with quantity and unit",
            "with": {
                "quantity": {
                    "the": "diy.cook/quantity",
                    "description": "How much of this ingredient",
                    "cardinality": "one",
                    "as": "UnsignedInteger"
                },
                "name": {
                    "the": "diy.cook/ingredient-name",
                    "description": "Name of the ingredient",
                    "as": "Text"
                }
            }
        }"#;

        let concept: ConceptDescriptor =
            serde_json::from_str(fixture).expect("Schema-conformant fixture should deserialize");

        assert_eq!(
            concept.description(),
            Some("A recipe ingredient with quantity and unit")
        );
        assert_eq!(concept.with().iter().count(), 2);

        // Re-serialize and verify the structure is preserved
        let json = serde_json::to_string(&concept).expect("Should re-serialize");
        let reparsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        assert_eq!(
            reparsed["description"],
            "A recipe ingredient with quantity and unit"
        );

        let with = reparsed["with"].as_object().expect("Should have 'with'");
        assert_eq!(with["quantity"]["the"], "diy.cook/quantity");
        assert_eq!(with["name"]["the"], "diy.cook/ingredient-name");
        assert_eq!(with["name"]["as"], "Text");
    }

    /// Validates that a minimal schema-conformant fixture (only required fields)
    /// deserializes correctly.
    #[dialog_common::test]
    fn it_accepts_minimal_schema_fixture() {
        let fixture = r#"{
            "with": {
                "status": {
                    "the": "task/status"
                }
            }
        }"#;

        let concept: ConceptDescriptor =
            serde_json::from_str(fixture).expect("Minimal fixture should deserialize");

        assert_eq!(concept.description(), None);
        assert_eq!(concept.with().iter().count(), 1);

        let (name, attr) = concept.with().iter().next().unwrap();
        assert_eq!(name, "status");
        assert_eq!(attr.domain(), "task");
        assert_eq!(attr.name(), "status");
    }

    #[dialog_common::test]
    fn it_rejects_missing_with() {
        let json = r#"{
            "description": "No attributes"
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(result.is_err(), "Should reject object without 'with'");
    }

    #[dialog_common::test]
    fn it_rejects_flat_format() {
        // Pre-wrapper format: attributes at top level instead of under "with"
        let json = r#"{
            "name": {
                "the": "user/name",
                "description": "User's name",
                "as": "Text"
            }
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(
            result.is_err(),
            "Should reject flat format (no 'with' wrapper)"
        );
    }

    #[dialog_common::test]
    fn it_rejects_with_as_array() {
        let json = r#"{
            "with": [
                { "the": "user/name" }
            ]
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(result.is_err(), "Should reject 'with' as array");
    }

    #[dialog_common::test]
    fn it_rejects_with_as_string() {
        let json = r#"{
            "with": "user/name"
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(result.is_err(), "Should reject 'with' as string");
    }

    #[dialog_common::test]
    fn it_rejects_attribute_missing_the() {
        let json = r#"{
            "with": {
                "name": {
                    "description": "Missing the field",
                    "as": "Text"
                }
            }
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(
            result.is_err(),
            "Should reject attribute without required 'the'"
        );
    }

    #[dialog_common::test]
    fn it_rejects_description_as_number() {
        let json = r#"{
            "description": 42,
            "with": {
                "name": { "the": "user/name" }
            }
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(result.is_err(), "Should reject non-string description");
    }

    #[dialog_common::test]
    fn it_rejects_the_without_slash() {
        let json = r#"{
            "with": {
                "name": {
                    "the": "invalid",
                    "as": "Text"
                }
            }
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(
            result.is_err(),
            "Should reject 'the' without domain/name format"
        );
    }

    #[dialog_common::test]
    fn it_rejects_invalid_cardinality() {
        let json = r#"{
            "with": {
                "tags": {
                    "the": "post/tags",
                    "cardinality": "several"
                }
            }
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(result.is_err(), "Should reject invalid cardinality value");
    }

    #[dialog_common::test]
    fn it_rejects_empty_object() {
        let json = r#"{}"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(result.is_err(), "Should reject empty object");
    }

    #[dialog_common::test]
    fn it_accepts_maybe_field() {
        let json = r#"{
            "with": {
                "name": { "the": "user/name", "as": "Text" }
            },
            "maybe": {
                "bio": { "the": "user/bio", "as": "Text" }
            }
        }"#;

        let concept: ConceptDescriptor =
            serde_json::from_str(json).expect("Should accept 'maybe' field");

        assert_eq!(concept.with().iter().count(), 1);
    }

    #[dialog_common::test]
    fn it_validates_maybe_on_parse() {
        let json = r#"{
            "with": {
                "name": { "the": "user/name", "as": "Text" }
            },
            "maybe": {
                "bio": { "the": "invalid" }
            }
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(
            result.is_err(),
            "Should validate 'maybe' attributes even though they are not yet supported"
        );
    }

    #[dialog_common::test]
    fn it_omits_maybe_in_serialization() {
        let concept = ConceptDescriptor::from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User's name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        let json = serde_json::to_string(&concept).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        assert!(
            parsed.get("maybe").is_none(),
            "Should not serialize 'maybe' when absent"
        );
    }

    #[dialog_common::test]
    fn it_deserializes_empty_maybe_as_none() {
        let json = r#"{
            "with": {
                "name": { "the": "user/name", "as": "Text" }
            },
            "maybe": {}
        }"#;

        let concept: ConceptDescriptor =
            serde_json::from_str(json).expect("Should accept empty 'maybe'");

        assert_eq!(concept.maybe, None, "Empty 'maybe' should become None");
    }
}
