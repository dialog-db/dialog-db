/// Named attribute collections for concept descriptors.
mod named_attributes;
pub use named_attributes::{ConceptFieldDescriptor, NamedAttributes};

use std::iter;

use crate::Predicate;
use crate::attribute::{AttributeDescriptor, Attribution};
use crate::concept::query::ConceptQuery;
use crate::concept::{Concept, Conclusion};
use crate::error::TypeError;
use crate::query::Application;
use crate::selection::{Match, Selection};
use crate::source::SelectRules;
use crate::statement::Retraction;
use crate::term::Term;
use crate::types::Scalar;
use crate::{
    Cardinality, Entity, EvaluationError, Field, Parameters, Proposition, Requirement, Schema,
    Statement, Type, Value,
};
use dialog_artifacts::Select;
use dialog_artifacts::Update;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;

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
/// { "description": "...", "with": { "field-name": { "the": "domain/name", ... } } }
/// ```
///
/// Field-name keys follow the kebab-case convention used elsewhere in the
/// formal notation; Rust field names are normalized on derive (e.g. a
/// `display_name`, `displayName`, or `DisplayName` field all serialize as
/// `"display-name"`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConceptDescriptor {
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// All of the concept's attributes, required and optional alike.
    /// Optionality is carried per-attribute by
    /// [`AttributeDescriptor::is_optional`] (serialized as
    /// `"optional": true` on the wire), so there is no separate
    /// `maybe` block. An optional attribute is emitted into the rule
    /// body as a set-widened `AttributeQuery`: a missing fact yields
    /// a fallback row with the slot bound to
    /// [`Binding::Absent`](crate::Binding) rather than dropping the
    /// row. A concept must still declare at least one *required*
    /// attribute.
    with: NamedAttributes,
}

impl ConceptDescriptor {
    /// Returns the description of this concept, if any.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns this descriptor with `description` set.
    ///
    /// The `From<[..]>` / `From<Vec<..>>` constructors only carry
    /// the attribute map and leave `description` as `None`; this
    /// is the builder-style way to attach one. An empty string is
    /// treated as absent (`description` stays `None`) so a concept
    /// with no doc comment doesn't serialize a blank field.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        let description = description.into();
        self.description = if description.is_empty() {
            None
        } else {
            Some(description)
        };
        self
    }

    /// Returns a reference to this concept's attributes (required
    /// and optional alike). Use [`AttributeDescriptor::is_optional`]
    /// to tell them apart.
    pub fn with(&self) -> &NamedAttributes {
        &self.with
    }

    /// Validates the provided parameters against the schema of the attributes.
    pub fn conform(&self, parameters: Parameters) -> Result<Parameters, TypeError> {
        for (name, field) in self.with().iter() {
            field
                .descriptor()
                .conform(parameters.get(name))
                .map_err(|e| e.at(name.into()))?;
        }

        Ok(parameters)
    }

    /// Returns an iterator over the concept's *required* operand
    /// names, starting with `"this"` followed by the required
    /// attribute keys.
    ///
    /// Optional attributes are excluded: they may resolve to
    /// `Absent`, so they are not subject to the required-head
    /// grounding check. Use [`Self::with`] to enumerate every
    /// attribute including optional ones.
    pub fn operands(&self) -> impl Iterator<Item = &str> {
        iter::once("this").chain(
            self.with()
                .iter()
                .filter(|(_, attribute)| !attribute.is_optional())
                .map(|(name, _)| name),
        )
    }

    /// Derives a `Schema` from this descriptor's attributes.
    pub fn schema(&self) -> Schema {
        Schema::from(self)
    }

    /// Encode this concept as CBOR for hashing.
    ///
    /// Creates a CBOR-encoded map where:
    /// - Keys are attribute URIs (`the:{hash}`) in sorted order.
    /// - The value for a **required** attribute is an empty map `{}`.
    /// - The value for an **optional** attribute is `{ "optional":
    ///   true }`.
    ///
    /// A concept with no optional attributes therefore hashes
    /// exactly as it did before optionality existed (every value is
    /// `{}`), so existing concept identities are preserved. Marking
    /// an attribute optional changes its value object, and thus the
    /// concept's identity.
    pub fn to_cbor_bytes(&self) -> Vec<u8> {
        use serde::Serialize;
        use std::collections::BTreeMap;

        /// Per-attribute hash value: `{}` for required,
        /// `{ "optional": true }` for optional. `optional` is omitted
        /// when false, so a required attribute encodes as an empty
        /// map — byte-identical to the pre-optionality encoding.
        #[derive(Serialize)]
        struct AttributeIdentity {
            #[serde(skip_serializing_if = "std::ops::Not::not")]
            optional: bool,
        }

        let mut attr_map: BTreeMap<String, AttributeIdentity> = BTreeMap::new();

        for (_name, schema) in self.with().iter() {
            let uri = schema.to_uri();
            attr_map.insert(
                uri,
                AttributeIdentity {
                    optional: schema.is_optional(),
                },
            );
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
    pub fn apply(&self, parameters: Parameters) -> Result<Proposition, TypeError> {
        Ok(Proposition::Concept(ConceptQuery {
            terms: self.conform(parameters)?,
            predicate: self.clone(),
        }))
    }

    /// Validates a model against this descriptor's schema and creates an instance.
    fn conform_model(&self, model: Model) -> Result<ConceptStatement, TypeError> {
        let mut relations = vec![];
        for (name, field) in self.with().iter() {
            if let Some(value) = model.attributes.get(name) {
                let relation = field
                    .descriptor()
                    .resolve(value.clone())
                    .map_err(|e| e.at(name.to_string()))?;
                relations.push(relation);
            } else if !field.is_optional() {
                return Err(TypeError::OmittedRequirement {
                    binding: name.into(),
                });
            }
        }
        Ok(ConceptStatement {
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

/// Build a [`ConceptDescriptor`] from a validated [`NamedAttributes`]
/// set. Private: the only way to obtain a `NamedAttributes` is
/// through its own (fallible) constructors, which guarantee at least
/// one required attribute, so reaching here means the invariant
/// already holds.
fn descriptor_from_with(with: NamedAttributes) -> ConceptDescriptor {
    ConceptDescriptor {
        description: None,
        with,
    }
}

/// Wrap a `(name, AttributeDescriptor)` pair as a *required* concept
/// field. The collection `TryFrom` impls accept bare attribute
/// descriptors for ergonomics — a hand-written
/// `ConceptDescriptor::try_from([("name", AttributeDescriptor::new(..))])`
/// means "this attribute is required." Optional fields are built via
/// [`ConceptFieldDescriptor::optional`] and the
/// `ConceptFieldDescriptor`-keyed `TryFrom` impls.
fn required_field<K: Into<String>>(
    (name, attr): (K, AttributeDescriptor),
) -> (String, ConceptFieldDescriptor) {
    (name.into(), ConceptFieldDescriptor::required(attr))
}

impl<const N: usize> TryFrom<[(&str, AttributeDescriptor); N]> for ConceptDescriptor {
    type Error = TypeError;

    fn try_from(arr: [(&str, AttributeDescriptor); N]) -> Result<Self, Self::Error> {
        let pairs: Vec<_> = arr.into_iter().map(required_field).collect();
        Ok(descriptor_from_with(NamedAttributes::try_from(pairs)?))
    }
}

impl<const N: usize> TryFrom<[(String, AttributeDescriptor); N]> for ConceptDescriptor {
    type Error = TypeError;

    fn try_from(arr: [(String, AttributeDescriptor); N]) -> Result<Self, Self::Error> {
        let pairs: Vec<_> = arr.into_iter().map(required_field).collect();
        Ok(descriptor_from_with(NamedAttributes::try_from(pairs)?))
    }
}

impl TryFrom<Vec<(&str, AttributeDescriptor)>> for ConceptDescriptor {
    type Error = TypeError;

    fn try_from(vec: Vec<(&str, AttributeDescriptor)>) -> Result<Self, Self::Error> {
        let pairs: Vec<_> = vec.into_iter().map(required_field).collect();
        Ok(descriptor_from_with(NamedAttributes::try_from(pairs)?))
    }
}

impl TryFrom<Vec<(String, AttributeDescriptor)>> for ConceptDescriptor {
    type Error = TypeError;

    fn try_from(vec: Vec<(String, AttributeDescriptor)>) -> Result<Self, Self::Error> {
        let pairs: Vec<_> = vec.into_iter().map(required_field).collect();
        Ok(descriptor_from_with(NamedAttributes::try_from(pairs)?))
    }
}

impl TryFrom<HashMap<String, AttributeDescriptor>> for ConceptDescriptor {
    type Error = TypeError;

    fn try_from(map: HashMap<String, AttributeDescriptor>) -> Result<Self, Self::Error> {
        let pairs: Vec<_> = map.into_iter().map(required_field).collect();
        Ok(descriptor_from_with(NamedAttributes::try_from(pairs)?))
    }
}

/// Build a descriptor directly from already-wrapped concept fields
/// (required and/or optional). Used by the `#[derive(Concept)]`
/// macro path, which tags each field's optionality before building.
impl TryFrom<Vec<(String, ConceptFieldDescriptor)>> for ConceptDescriptor {
    type Error = TypeError;

    fn try_from(fields: Vec<(String, ConceptFieldDescriptor)>) -> Result<Self, Self::Error> {
        Ok(descriptor_from_with(NamedAttributes::try_from(fields)?))
    }
}

impl From<&ConceptDescriptor> for Schema {
    fn from(predicate: &ConceptDescriptor) -> Self {
        use crate::type_system::Type as Kind;
        let mut schema = Schema::new();
        for (name, field) in predicate.with().iter() {
            schema.insert(
                name.into(),
                Field {
                    description: field.description().into(),
                    content_type: field.content_type().map(Kind::primitive),
                    // Every concept slot is `Optional` at the schema
                    // layer: a `concept:` query *produces* its fields
                    // rather than requiring the caller to supply them.
                    // This is orthogonal to a field's `is_optional()`
                    // (set-widened / Absent-on-miss) concept-membership
                    // flag, which is carried into the rule body via the
                    // `is` term's kind, not the schema requirement.
                    requirement: Requirement::Optional,
                    cardinality: field.cardinality(),
                },
            );
        }

        if !schema.contains("this") {
            schema.insert(
                "this".into(),
                Field {
                    description: "The entity that this model represents".into(),
                    content_type: Some(Kind::primitive(Type::Entity)),
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
pub struct ConceptStatement {
    /// The entity this instance represents
    pub this: Entity,
    /// The validated relations (attribute-value pairs) for this instance
    pub with: Vec<Attribution>,
}
impl ConceptStatement {
    /// Returns a reference to the entity this instance represents.
    pub fn this(&self) -> &'_ Entity {
        &self.this
    }

    /// Returns a reference to the validated relations for this instance.
    pub fn attributes(&self) -> &'_ Vec<Attribution> {
        &self.with
    }
}

impl Statement for ConceptStatement {
    fn assert(self, update: &mut impl Update) {
        for attribution in self.with {
            update.associate(attribution.the, self.this.clone(), attribution.is);
        }
    }
    fn retract(self, update: &mut impl Update) {
        for attribution in self.with {
            update.dissociate(attribution.the, self.this.clone(), attribution.is);
        }
    }
}

impl Not for ConceptStatement {
    type Output = Retraction<Self>;

    fn not(self) -> Self::Output {
        Retraction(self)
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
        self.model.attributes.insert(name.into(), value.into());
        self
    }

    /// Builds and validates the concept instance.
    pub fn build(self) -> Result<ConceptStatement, TypeError> {
        self.predicate.conform_model(self.model)
    }
}

/// A dynamic conclusion — an entity with its resolved field values.
///
/// Field values are accessed by the term bindings from the query.
/// The `terms` map provides the mapping from field names to variable terms
/// used in the match.
#[derive(Debug, Clone)]
pub struct ConceptConclusion {
    this: Entity,
    terms: Parameters,
    source: Match,
}

impl ConceptConclusion {
    /// Returns the entity this conclusion describes.
    pub fn entity(&self) -> &Entity {
        &self.this
    }

    /// Look up a field value by its concept field name (e.g. "name", "age").
    pub fn get<T>(&self, field: &str) -> Result<T, EvaluationError>
    where
        T: Scalar + TryFrom<Value>,
    {
        let param = self
            .terms
            .get(field)
            .ok_or_else(|| EvaluationError::UnboundVariable {
                variable_name: field.to_string(),
            })?;
        match param {
            Term::Variable {
                name: Some(name), ..
            } => {
                let typed_term: Term<T> = Term::var(name.clone());
                T::try_from(self.source.lookup(&Term::from(&typed_term))?.content()?).map_err(
                    |_| EvaluationError::UnboundVariable {
                        variable_name: field.to_string(),
                    },
                )
            }
            Term::Constant(value) => {
                T::try_from(value.clone()).map_err(|_| EvaluationError::UnboundVariable {
                    variable_name: field.to_string(),
                })
            }
            Term::Variable { name: None, .. } => Err(EvaluationError::UnboundVariable {
                variable_name: field.to_string(),
            }),
        }
    }

    /// Returns a reference to the raw match.
    pub fn source(&self) -> &Match {
        &self.source
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

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        ConceptQuery::evaluate(self, selection, env)
    }

    fn realize(&self, source: Match) -> Result<Self::Conclusion, EvaluationError> {
        let this_param =
            self.terms
                .get("this")
                .ok_or_else(|| EvaluationError::UnboundVariable {
                    variable_name: "this".to_string(),
                })?;
        let entity: Entity = match this_param {
            Term::Variable {
                name: Some(name), ..
            } => {
                let typed_term: Term<Entity> = Term::var(name.clone());
                Entity::try_from(source.lookup(&Term::from(&typed_term))?.content()?)?
            }
            Term::Constant(value) => match value {
                Value::Entity(e) => e.clone(),
                _ => {
                    return Err(EvaluationError::UnboundVariable {
                        variable_name: "this".to_string(),
                    });
                }
            },
            Term::Variable { name: None, .. } => {
                return Err(EvaluationError::UnboundVariable {
                    variable_name: "this".to_string(),
                });
            }
        };
        Ok(ConceptConclusion {
            this: entity,
            terms: self.terms.clone(),
            source,
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
        let predicate = ConceptDescriptor::try_from([
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
        ])
        .unwrap();

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
        let mut predicate = ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User's name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        predicate.description = Some("A user profile".to_string());

        let json = serde_json::to_string(&predicate).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        assert_eq!(parsed["description"], "A user profile");
        assert!(parsed["with"].is_object());
    }

    #[dialog_common::test]
    fn it_omits_null_description_in_json() {
        let predicate = ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User's name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let json = serde_json::to_string(&predicate).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        assert!(parsed.get("description").is_none());
        assert!(parsed["with"].is_object());
    }

    #[dialog_common::test]
    fn it_round_trips_through_json() {
        let original = ConceptDescriptor::try_from([(
            "score",
            AttributeDescriptor::new(
                the!("game/score"),
                "Game score",
                Cardinality::One,
                Some(Type::UnsignedInt),
            ),
        )])
        .unwrap();

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
        let predicate = ConceptDescriptor::try_from(vec![(
            "id".to_string(),
            AttributeDescriptor::new(
                the!("product/id"),
                "Product ID",
                Cardinality::One,
                Some(Type::UnsignedInt),
            ),
        )])
        .unwrap();

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
        let pred1 = ConceptDescriptor::try_from(vec![
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
        ])
        .unwrap();

        let pred2 = ConceptDescriptor::try_from(vec![
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
        ])
        .unwrap();

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
        let pred1 = ConceptDescriptor::try_from(vec![
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
        ])
        .unwrap();

        let pred2 = ConceptDescriptor::try_from(vec![
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
        ])
        .unwrap();

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
        let pred1 = ConceptDescriptor::try_from(vec![(
            "name".to_string(),
            AttributeDescriptor::new(
                the!("person/name"),
                "Name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let pred2 = ConceptDescriptor::try_from(vec![(
            "email".to_string(),
            AttributeDescriptor::new(
                the!("person/email"),
                "Email",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

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
    /// - Each Attribute has "the" (required), optional "description", "cardinality", "as", "optional"
    /// - No unexpected top-level keys (only "description", "with")
    #[dialog_common::test]
    fn it_conforms_to_json_schema() {
        let predicate = ConceptDescriptor::try_from([
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
        ])
        .unwrap();

        let json = serde_json::to_string(&predicate).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");
        let obj = parsed.as_object().expect("Top-level must be object");

        // Only allowed top-level keys per schema: "description", "with"
        for key in obj.keys() {
            assert!(
                ["description", "with"].contains(&key.as_str()),
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
                    ["the", "description", "cardinality", "as", "optional"].contains(&key.as_str()),
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

    /// A concept must have at least one required (`with`) attribute.
    /// An empty `with` map describes a concept every entity trivially
    /// matches — the same degenerate shape as a concept with no
    /// attributes at all — so the parse layer rejects it.
    #[dialog_common::test]
    fn it_rejects_empty_with_at_parse() {
        let json = r#"{
            "with": {}
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(
            result.is_err(),
            "Should reject a concept whose `with` map is empty"
        );
    }

    /// A concept whose every attribute is optional is just as
    /// degenerate: with no required attribute, the rule body binds
    /// nothing that constrains the entity, so every entity matches.
    /// Rejected at the parse layer.
    #[dialog_common::test]
    fn it_rejects_all_optional_concept_at_parse() {
        let json = r#"{
            "with": {
                "bio": { "the": "user/bio", "as": "Text", "optional": true }
            }
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(
            result.is_err(),
            "Should reject a concept with no required attributes \
             (every `with` entry flagged optional)"
        );
    }

    #[dialog_common::test]
    fn it_accepts_optional_field() {
        let json = r#"{
            "with": {
                "name": { "the": "user/name", "as": "Text" },
                "bio": { "the": "user/bio", "as": "Text", "optional": true }
            }
        }"#;

        let concept: ConceptDescriptor =
            serde_json::from_str(json).expect("Should accept an optional field");

        assert_eq!(concept.with().iter().count(), 2);

        let bio = concept
            .with()
            .iter()
            .find(|(name, _)| *name == "bio")
            .map(|(_, field)| field)
            .expect("bio field present");
        assert!(bio.is_optional(), "bio must be optional");

        let name = concept
            .with()
            .iter()
            .find(|(name, _)| *name == "name")
            .map(|(_, field)| field)
            .expect("name field present");
        assert!(!name.is_optional(), "name must be required");
    }

    #[dialog_common::test]
    fn it_validates_optional_field_on_parse() {
        let json = r#"{
            "with": {
                "name": { "the": "user/name", "as": "Text" },
                "bio": { "the": "invalid", "optional": true }
            }
        }"#;

        let result = serde_json::from_str::<ConceptDescriptor>(json);
        assert!(
            result.is_err(),
            "Should validate optional attributes the same as required ones"
        );
    }

    /// A required-only concept must serialize with no `optional`
    /// keys anywhere, byte-compatible with the pre-optionality
    /// encoding. There is no separate top-level `maybe` block either.
    #[dialog_common::test]
    fn it_omits_optional_flag_for_required_fields() {
        let concept = ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User's name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        let json = serde_json::to_string(&concept).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        assert!(
            parsed.get("maybe").is_none(),
            "There is no top-level 'maybe' block"
        );

        let with = parsed["with"].as_object().expect("Should have 'with'");
        for (field_name, attr) in with {
            assert!(
                attr.as_object()
                    .expect("attribute is an object")
                    .get("optional")
                    .is_none(),
                "Required field '{field_name}' must omit the 'optional' key"
            );
        }
    }

    /// An optional field round-trips, carrying `"optional": true` on
    /// the wire and parsing back as optional.
    #[dialog_common::test]
    fn it_round_trips_an_optional_field() {
        let concept = ConceptDescriptor::try_from(vec![
            (
                "name".to_string(),
                ConceptFieldDescriptor::required(AttributeDescriptor::new(
                    the!("user/name"),
                    "User's name",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
            (
                "bio".to_string(),
                ConceptFieldDescriptor::optional(AttributeDescriptor::new(
                    the!("user/bio"),
                    "User's bio",
                    Cardinality::One,
                    Some(Type::String),
                )),
            ),
        ])
        .unwrap();

        let json = serde_json::to_string(&concept).expect("Should serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("Should parse");

        let with = parsed["with"].as_object().expect("Should have 'with'");
        assert_eq!(
            with["bio"]["optional"],
            serde_json::json!(true),
            "Optional field must carry 'optional': true"
        );
        assert!(
            with["name"].as_object().unwrap().get("optional").is_none(),
            "Required field must omit 'optional'"
        );

        let round_tripped: ConceptDescriptor =
            serde_json::from_str(&json).expect("Should deserialize");
        assert_eq!(round_tripped.this(), concept.this());

        let bio = round_tripped
            .with()
            .iter()
            .find(|(name, _)| *name == "bio")
            .map(|(_, field)| field)
            .expect("bio present");
        assert!(bio.is_optional());
    }

    #[dialog_common::test]
    fn it_strips_raw_identifier_prefix_from_concept_field_names() {
        use crate as dialog_query;
        use crate::{Attribute, Concept, Entity};

        #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("dialog.test")]
        pub struct TypeAttr(pub String);

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Sample {
            pub this: Entity,
            /// `type` is a Rust keyword so the field has to be written
            /// `r#type` — but the descriptor's `with` map key must be
            /// the cooked name `"type"`, not `"r#type"`.
            pub r#type: TypeAttr,
        }

        let descriptor: ConceptDescriptor = Sample::descriptor().clone();

        // Round-trip the descriptor through JSON and assert the field
        // key in the `with` map is `type`, not `r#type`.
        let json = serde_json::to_value(&descriptor).expect("serialize");
        let with = json["with"].as_object().expect("with map");

        assert!(
            with.contains_key("type"),
            "descriptor should expose `type`, got keys: {:?}",
            with.keys().collect::<Vec<_>>()
        );
        assert!(
            !with.contains_key("r#type"),
            "descriptor should NOT expose raw-ident `r#type`",
        );

        let round_tripped: ConceptDescriptor = serde_json::from_value(json).expect("deserialize");
        assert_eq!(
            round_tripped.this(),
            descriptor.this(),
            "JSON round-trip must preserve the concept's content hash"
        );
    }

    #[dialog_common::test]
    #[allow(nonstandard_style)]
    fn it_kebab_cases_concept_field_names() {
        use crate as dialog_query;
        use crate::{Attribute, Concept, Entity};

        // The descriptor's `with` map keys follow the formal-notation
        // convention: lower-case kebab. snake_case, camelCase, and
        // PascalCase field names all collapse to kebab-case. A field
        // already in single-word lower form is unchanged.

        #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("dialog.test")]
        pub struct A(pub String);

        #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("dialog.test")]
        pub struct B(pub String);

        #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("dialog.test")]
        pub struct C(pub String);

        #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("dialog.test")]
        pub struct D(pub String);

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Sample {
            pub this: Entity,
            /// snake_case → kebab
            pub display_name: A,
            /// camelCase → kebab
            pub firstName: B,
            /// PascalCase → kebab
            pub LastName: C,
            /// already lowercase single-word → unchanged
            pub email: D,
        }

        let descriptor: ConceptDescriptor = Sample::descriptor().clone();

        let json = serde_json::to_value(&descriptor).expect("serialize");
        let with = json["with"].as_object().expect("with map");
        let keys: Vec<&String> = with.keys().collect();

        for expected in ["display-name", "first-name", "last-name", "email"] {
            assert!(
                with.contains_key(expected),
                "descriptor should expose `{expected}`, got keys: {keys:?}",
            );
        }
        for unexpected in ["display_name", "firstName", "LastName"] {
            assert!(
                !with.contains_key(unexpected),
                "descriptor should NOT expose raw `{unexpected}`",
            );
        }

        let round_tripped: ConceptDescriptor = serde_json::from_value(json).expect("deserialize");
        assert_eq!(
            round_tripped.this(),
            descriptor.this(),
            "JSON round-trip must preserve the concept's content hash"
        );
    }

    /// Marking a field optional changes the concept's identity, but a
    /// required-only concept hashes exactly as it would have before
    /// optionality existed: building the same attribute as required
    /// yields the same `this`/`hash`, while flipping it to optional
    /// changes both.
    #[dialog_common::test]
    fn it_hashes_optionality_into_identity() {
        let name_attr = || {
            AttributeDescriptor::new(
                the!("user/name"),
                "User's name",
                Cardinality::One,
                Some(Type::String),
            )
        };
        let bio_attr = || {
            AttributeDescriptor::new(
                the!("user/bio"),
                "User's bio",
                Cardinality::One,
                Some(Type::String),
            )
        };

        // Two required-only concepts built from the same attributes
        // are identical, regardless of construction path.
        let required_array =
            ConceptDescriptor::try_from([("name", name_attr()), ("bio", bio_attr())]).unwrap();
        let required_fields = ConceptDescriptor::try_from(vec![
            (
                "name".to_string(),
                ConceptFieldDescriptor::required(name_attr()),
            ),
            (
                "bio".to_string(),
                ConceptFieldDescriptor::required(bio_attr()),
            ),
        ])
        .unwrap();
        assert_eq!(
            required_array.hash(),
            required_fields.hash(),
            "required-only concept identity is independent of construction path"
        );

        // Flipping `bio` to optional changes the identity.
        let with_optional = ConceptDescriptor::try_from(vec![
            (
                "name".to_string(),
                ConceptFieldDescriptor::required(name_attr()),
            ),
            (
                "bio".to_string(),
                ConceptFieldDescriptor::optional(bio_attr()),
            ),
        ])
        .unwrap();
        assert_ne!(
            required_array.hash(),
            with_optional.hash(),
            "marking a field optional must change the concept's hash"
        );
        assert_ne!(
            required_array.this(),
            with_optional.this(),
            "marking a field optional must change the concept's URI"
        );
    }

    /// `From<&ConceptDescriptor> for Schema` lifts a typed
    /// attribute's content_type into the unified `type_system::Type`.
    #[dialog_common::test]
    fn schema_from_concept_uses_unified_type() {
        let descriptor = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        let schema = Schema::from(&descriptor);
        let name = schema.get("name").expect("name field present");
        let content = name.content_type().expect("content_type present");
        assert!(!content.is_optional());
        assert_eq!(content.as_value_type(), Some(Type::String));
    }

    /// An attribute descriptor with `None` content type produces
    /// a Field with `None` content_type — unknown.
    #[dialog_common::test]
    fn schema_from_concept_untyped_attribute_produces_none() {
        let descriptor = ConceptDescriptor::try_from(vec![(
            "tag",
            AttributeDescriptor::new(the!("misc/tag"), "", Cardinality::One, None),
        )])
        .unwrap();
        let schema = Schema::from(&descriptor);
        let tag = schema.get("tag").expect("tag field present");
        assert!(tag.content_type().is_none());
    }

    /// The synthesized `this` field always declares
    /// a singleton primitive over `Entity`.
    #[dialog_common::test]
    fn schema_from_concept_synthesizes_this_as_entity() {
        let descriptor = ConceptDescriptor::try_from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();
        let schema = Schema::from(&descriptor);
        let this = schema.get("this").expect("this field present");
        let content = this.content_type().expect("entity kind present");
        assert_eq!(content.as_value_type(), Some(Type::Entity));
    }

    #[dialog_common::test]
    fn it_carries_the_concept_doc_comment_into_the_descriptor() {
        use crate::{Attribute, Concept, Entity};

        // The `#[derive(Concept)]` macro captures the struct doc
        // comment for the `Concept::description` trait method. The
        // `From`-built descriptor must carry the same text: a
        // `concept:` query reads the descriptor, so without this
        // the description never reaches a consumer.

        #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("dialog.test")]
        pub struct Title(pub String);

        /// A cooking recipe.
        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Recipe {
            pub this: Entity,
            /// The recipe's title.
            pub title: Title,
        }

        let descriptor: ConceptDescriptor = Recipe::descriptor().clone();

        assert_eq!(
            descriptor.description(),
            Some("A cooking recipe."),
            "the descriptor must carry the struct doc comment",
        );
        // The trait method and the descriptor are two paths off
        // the same doc comment; they must agree.
        assert_eq!(descriptor.description(), Some(Recipe::description()));

        // The description has to survive the JSON round-trip a
        // `concept:` query serializes the descriptor through.
        let json = serde_json::to_value(&descriptor).expect("serialize");
        assert_eq!(json["description"], serde_json::json!("A cooking recipe."));
        let round_tripped: ConceptDescriptor = serde_json::from_value(json).expect("deserialize");
        assert_eq!(round_tripped.description(), descriptor.description());
    }

    #[dialog_common::test]
    fn it_leaves_description_none_for_an_undocumented_concept() {
        use crate::{Attribute, Concept, Entity};

        // A struct with no doc comment yields an empty
        // description; `with_description` treats that as absent so
        // no blank `description` field is serialized.

        #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
        #[domain("dialog.test")]
        pub struct Title(pub String);

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Recipe {
            pub this: Entity,
            /// The recipe's title.
            pub title: Title,
        }

        let descriptor: ConceptDescriptor = Recipe::descriptor().clone();

        assert_eq!(descriptor.description(), None);
        let json = serde_json::to_value(&descriptor).expect("serialize");
        assert!(
            json.get("description").is_none(),
            "an empty description must not serialize a blank field",
        );
    }

    #[dialog_common::test]
    fn it_sets_the_description_via_with_description() {
        let descriptor = ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("user/name"),
                "User's name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap();

        // `From<[..]>` leaves `description` as `None`.
        assert_eq!(descriptor.description(), None);

        let described = descriptor.clone().with_description("A user account.");
        assert_eq!(described.description(), Some("A user account."));

        // An empty string is treated as absent.
        assert_eq!(described.with_description("").description(), None);
    }
}
