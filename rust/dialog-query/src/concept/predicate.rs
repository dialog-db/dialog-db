use crate::Predicate;
use crate::attribute::{AttributeDescriptor, Attribution};
use crate::claim::Revert;
use crate::concept::application::ConceptApplication;
use crate::concept::{Concept, ConceptProof};
use crate::error::SchemaError;
use crate::query::{Application, Source};
use crate::selection::{Answer, Answers};
use crate::term::Term;
use crate::types::Scalar;
use crate::{
    Assertion, Cardinality, Claim, Entity, Field, Parameters, Proposition, QueryError, Requirement,
    Schema, Transaction, Type, Value,
};

use base58::ToBase58;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Not;

/// A concept predicate — a named set of attribute descriptors that together
/// describe an entity type. Concepts are similar to tables in relational
/// databases but are more flexible as they can be derived from rules rather
/// than just stored directly.
///
/// Concepts are identified by a blake3 hash of their attribute set, encoded
/// as a URI in the format `concept:{hash}`.
#[derive(Debug, Clone, PartialEq)]
pub struct ConceptPredicate(Vec<(String, AttributeDescriptor)>);

impl Default for ConceptPredicate {
    fn default() -> Self {
        Self::new()
    }
}

impl ConceptPredicate {
    /// Creates an empty concept predicate.
    pub fn new() -> Self {
        ConceptPredicate(Vec::new())
    }

    /// Returns an iterator over all attributes as (name, descriptor) pairs.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &AttributeDescriptor)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Returns the number of attributes in this predicate.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if this predicate has no attributes.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over attribute names.
    pub fn keys(&self) -> impl Iterator<Item = &str> + '_ {
        self.0.iter().map(|(k, _)| k.as_str())
    }

    /// Conforms the provided parameters to the schema of the attributes.
    pub fn conform(&self, parameters: Parameters) -> Result<Parameters, SchemaError> {
        for (name, attribute) in self.iter() {
            let parameter = parameters.get(name);
            attribute
                .conform(parameter)
                .map_err(|e| e.at(name.into()))?;
        }

        Ok(parameters)
    }

    /// Returns an iterator over operand names, starting with "this" followed by attribute keys.
    pub fn operands(&self) -> impl Iterator<Item = &str> {
        std::iter::once("this").chain(self.keys())
    }

    /// Derives a `Schema` from this predicate's attributes.
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

        for (_name, schema) in self.iter() {
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

    /// Creates an application for this concept predicate.
    pub fn apply(&self, parameters: Parameters) -> Result<Proposition, SchemaError> {
        Ok(Proposition::Concept(ConceptApplication {
            terms: self.conform(parameters)?,
            predicate: self.clone(),
        }))
    }

    /// Validates a model against this predicate's schema and creates an instance.
    fn conform_model(&self, model: Model) -> Result<Conception, SchemaError> {
        let mut relations = vec![];
        for (name, attribute) in self.iter() {
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

    /// Creates a builder for editing an existing entity with this predicate's schema.
    pub fn edit(&self, entity: Entity) -> Builder<'_> {
        Builder::edit(entity, self)
    }

    /// Creates a builder for creating a new entity with this predicate's schema.
    pub fn create(&self) -> Builder<'_> {
        Builder::new(self)
    }
}

impl<const N: usize> From<[(&str, AttributeDescriptor); N]> for ConceptPredicate {
    fn from(arr: [(&str, AttributeDescriptor); N]) -> Self {
        ConceptPredicate(
            arr.into_iter()
                .map(|(name, attr)| (name.to_string(), attr))
                .collect(),
        )
    }
}

impl<const N: usize> From<[(String, AttributeDescriptor); N]> for ConceptPredicate {
    fn from(arr: [(String, AttributeDescriptor); N]) -> Self {
        ConceptPredicate(arr.into_iter().collect())
    }
}

impl From<Vec<(&str, AttributeDescriptor)>> for ConceptPredicate {
    fn from(vec: Vec<(&str, AttributeDescriptor)>) -> Self {
        ConceptPredicate(
            vec.into_iter()
                .map(|(name, attr)| (name.to_string(), attr))
                .collect(),
        )
    }
}

impl From<Vec<(String, AttributeDescriptor)>> for ConceptPredicate {
    fn from(vec: Vec<(String, AttributeDescriptor)>) -> Self {
        ConceptPredicate(vec)
    }
}

impl From<HashMap<String, AttributeDescriptor>> for ConceptPredicate {
    fn from(map: HashMap<String, AttributeDescriptor>) -> Self {
        ConceptPredicate(map.into_iter().collect())
    }
}

impl Serialize for ConceptPredicate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let map: HashMap<&str, &AttributeDescriptor> = self.iter().collect();
        map.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ConceptPredicate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let map = HashMap::<String, AttributeDescriptor>::deserialize(deserializer)?;
        Ok(ConceptPredicate::from(map))
    }
}

impl From<&ConceptPredicate> for Schema {
    fn from(predicate: &ConceptPredicate) -> Self {
        let mut schema = Schema::new();
        for (name, attribute) in predicate.iter() {
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

impl Claim for Conception {
    fn assert(self, transaction: &mut Transaction) {
        for attribution in self.with {
            transaction.associate(Assertion::new(
                attribution.the,
                self.this.clone(),
                attribution.is,
            ));
        }
    }
    fn retract(self, transaction: &mut Transaction) {
        for attribution in self.with {
            transaction.dissociate(Assertion::new(
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

/// A builder for constructing concept instances with validation.
#[derive(Debug, Clone)]
pub struct Builder<'a> {
    predicate: &'a ConceptPredicate,
    model: Model,
}
impl<'a> Builder<'a> {
    /// Creates a new builder for a fresh entity.
    pub fn new(predicate: &'a ConceptPredicate) -> Self {
        Self::edit(
            Entity::new().expect("should be able to generate new entity"),
            predicate,
        )
    }

    /// Creates a new builder for editing an existing entity.
    pub fn edit(this: Entity, predicate: &'a ConceptPredicate) -> Self {
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

/// A dynamic proof — an entity with its resolved field values.
///
/// Field values are accessed by the term bindings from the query.
/// The `terms` map provides the mapping from field names to variable terms
/// used in the answer.
#[derive(Debug, Clone)]
pub struct DynamicProof {
    this: Entity,
    terms: Parameters,
    answer: Answer,
}

impl DynamicProof {
    /// Returns the entity this proof describes.
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

impl ConceptProof for DynamicProof {
    fn this(&self) -> &Entity {
        &self.this
    }
}

impl From<ConceptPredicate> for Entity {
    fn from(predicate: ConceptPredicate) -> Self {
        predicate.this()
    }
}

impl From<ConceptApplication> for ConceptPredicate {
    fn from(app: ConceptApplication) -> Self {
        app.predicate
    }
}

impl Application for ConceptApplication {
    type Proof = DynamicProof;

    fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        ConceptApplication::evaluate(self, answers, source)
    }

    fn realize(&self, source: Answer) -> Result<Self::Proof, QueryError> {
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
        Ok(DynamicProof {
            this: entity,
            terms: self.terms.clone(),
            answer: source,
        })
    }
}

impl Predicate for ConceptPredicate {
    type Proof = DynamicProof;
    type Application = ConceptApplication;
    type Descriptor = ConceptPredicate;
}

impl Concept for ConceptPredicate {
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
    fn test_concept_serialization_to_specific_json() {
        let predicate = ConceptPredicate::from([
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

        assert_eq!(obj.len(), 2);

        let name_attr = obj["name"].as_object().expect("Should have name attribute");
        assert_eq!(name_attr["namespace"], "user");
        assert_eq!(name_attr["name"], "name");
        assert_eq!(name_attr["description"], "User's name");
        assert_eq!(name_attr["type"], "String");

        let age_attr = obj["age"].as_object().expect("Should have age attribute");
        assert_eq!(age_attr["namespace"], "user");
        assert_eq!(age_attr["name"], "age");
        assert_eq!(age_attr["description"], "User's age");
        assert_eq!(age_attr["type"], "UnsignedInt");
    }

    #[dialog_common::test]
    fn test_concept_deserialization_from_specific_json() {
        let json = r#"{
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
        }"#;

        let predicate: ConceptPredicate = serde_json::from_str(json).expect("Should deserialize");

        assert!(
            predicate.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );
        assert_eq!(predicate.len(), 2);

        let email_attr = predicate
            .iter()
            .find(|(k, _)| *k == "email")
            .map(|(_, v)| v)
            .expect("Should have email attribute");
        assert_eq!(email_attr.namespace(), "person");
        assert_eq!(email_attr.name(), "email");
        assert_eq!(email_attr.description(), "Person's email address");
        assert_eq!(email_attr.content_type(), Some(Type::String));

        let active_attr = predicate
            .iter()
            .find(|(k, _)| *k == "active")
            .map(|(_, v)| v)
            .expect("Should have active attribute");
        assert_eq!(active_attr.namespace(), "person");
        assert_eq!(active_attr.name(), "active");
        assert_eq!(active_attr.description(), "Whether person is active");
        assert_eq!(active_attr.content_type(), Some(Type::Boolean));
    }

    #[dialog_common::test]
    fn test_concept_round_trip_serialization() {
        let original = ConceptPredicate::from([(
            "score",
            AttributeDescriptor::new(
                the!("game/score"),
                "Game score",
                Cardinality::One,
                Some(Type::UnsignedInt),
            ),
        )]);

        let json = serde_json::to_string(&original).expect("Should serialize");
        let deserialized: ConceptPredicate =
            serde_json::from_str(&json).expect("Should deserialize");

        assert_eq!(original.this(), deserialized.this());
        assert_eq!(original.len(), deserialized.len());

        let orig_score = original
            .iter()
            .find(|(k, _)| *k == "score")
            .map(|(_, v)| v)
            .unwrap();
        let deser_score = deserialized
            .iter()
            .find(|(k, _)| *k == "score")
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(orig_score.namespace(), deser_score.namespace());
        assert_eq!(orig_score.name(), deser_score.name());
        assert_eq!(orig_score.description(), deser_score.description());
        assert_eq!(orig_score.content_type(), deser_score.content_type());
    }

    #[dialog_common::test]
    fn test_expected_json_structure() {
        let predicate = ConceptPredicate::from(vec![(
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
  "id": {
    "namespace": "product",
    "name": "id",
    "description": "Product ID",
    "type": "UnsignedInt"
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
    fn test_concept_field_names_do_not_affect_hash() {
        let pred1 = ConceptPredicate::from(vec![
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

        let pred2 = ConceptPredicate::from(vec![
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
    fn test_concept_attribute_order_does_not_affect_hash() {
        let pred1 = ConceptPredicate::from(vec![
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

        let pred2 = ConceptPredicate::from(vec![
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
    fn test_concept_different_attributes_different_hash() {
        let pred1 = ConceptPredicate::from(vec![(
            "name".to_string(),
            AttributeDescriptor::new(
                the!("person/name"),
                "Name",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        let pred2 = ConceptPredicate::from(vec![(
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
}
