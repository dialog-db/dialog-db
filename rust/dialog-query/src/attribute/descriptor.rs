use crate::artifact::{Attribute as ArtifactsAttribute, Entity, Value};
use crate::attribute::The;
use crate::error::{SchemaError, TypeError};
use crate::relation::descriptor::RelationDescriptor;
use crate::relation::query::RelationQuery;
use crate::schema::Cardinality;
use crate::types::{Scalar, Type};
use crate::{Parameters, Term};

use base58::ToBase58;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A validated attribute–value pair with its cardinality, produced by
/// [`AttributeDescriptor::resolve`]. Used inside [`Conception`](crate::concept::descriptor::Conception)
/// to represent the set of facts that make up a concept instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribution {
    /// The fully-qualified attribute selector.
    pub the: ArtifactsAttribute,
    /// The resolved value for this attribute.
    pub is: Value,
    /// Whether this attribute allows one or many values per entity.
    pub cardinality: Cardinality,
}

/// Static metadata for a single attribute: its storage-level selector
/// ([`The`]), human-readable description, value type, and cardinality.
///
/// `AttributeDescriptor` is used in two contexts:
/// 1. Inside a [`ConceptDescriptor`](crate::concept::descriptor::ConceptDescriptor)
///    to describe each attribute that makes up the concept.
/// 2. During query construction, where [`resolve`](AttributeDescriptor::resolve)
///    validates a runtime value against the descriptor's type and produces
///    an [`Attribution`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeDescriptor {
    the: The,
    description: String,
    cardinality: Cardinality,
    content_type: Option<Type>,
}

impl AttributeDescriptor {
    /// Creates a new descriptor from a validated [`The`] selector.
    pub fn new(
        the: The,
        description: impl Into<String>,
        cardinality: Cardinality,
        content_type: Option<Type>,
    ) -> Self {
        Self {
            the,
            description: description.into(),
            cardinality,
            content_type,
        }
    }

    /// Returns a relation identifier comprised of the attribute's domain and name.
    pub fn the(&self) -> &The {
        &self.the
    }

    /// Returns the attribute domain.
    pub fn domain(&self) -> &str {
        self.the.domain()
    }

    /// Returns the attribute name.
    pub fn name(&self) -> &str {
        self.the.name()
    }

    /// Returns the human-readable description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the cardinality.
    pub fn cardinality(&self) -> Cardinality {
        self.cardinality
    }

    /// Returns the expected value type, or `None` if any type is accepted.
    pub fn content_type(&self) -> Option<Type> {
        self.content_type
    }

    /// Checks that the given term's type is compatible with this attribute's
    /// content type. Returns the term unchanged on success.
    pub fn check<'a, U: Scalar>(&self, term: &'a Term<U>) -> Result<&'a Term<U>, TypeError> {
        match (self.content_type(), term.content_type()) {
            // if expected is any (has no type) it checks
            (None, _) => Ok(term),
            // if attribute is of some type and we're given term of unknown
            // type that's also fine.
            (_, None) => Ok(term),
            // if expected isn't any (has no type) it must be equal
            // to actual or it's a type missmatch.
            (Some(_expected), _actual) => Ok(term),
        }
    }

    /// Type-checks an optional term against this attribute. Returns `Ok(None)`
    /// if the term is absent, or delegates to [`check`](Self::check) if present.
    pub fn conform<'a, U: Scalar>(
        &self,
        term: Option<&'a Term<U>>,
    ) -> Result<Option<&'a Term<U>>, TypeError> {
        if let Some(term) = term {
            self.check(term)?;
        }

        Ok(term)
    }

    /// Validates a concrete [`Value`] against this attribute's content type and
    /// produces an [`Attribution`] — a validated (attribute, value, cardinality)
    /// triple ready for storage.
    pub fn resolve(&self, value: Value) -> Result<Attribution, TypeError> {
        let type_matches = match self.content_type() {
            Some(expected) => value.data_type() == expected,
            None => true,
        };

        if type_matches {
            Ok(Attribution {
                the: ArtifactsAttribute::from(&self.the),
                is: value.clone(),
                cardinality: self.cardinality(),
            })
        } else {
            Err(TypeError::TypeMismatch {
                expected: self.content_type().unwrap(), // Safe because we checked Some above
                actual: Term::Constant(value),
            })
        }
    }

    /// Estimates the cost of a fact query on this attribute given what's known.
    ///
    /// # Parameters
    /// - `the`: Is the attribute known? (usually true for Attribute)
    /// - `of`: Is the entity known?
    /// - `is`: Is the value known?
    pub fn estimate(&self, of: bool, is: bool) -> usize {
        self.cardinality()
            .estimate(true, of, is)
            .expect("Should succeed if we know attribute")
    }

    /// Builds a [`RelationQuery`] from named parameters, type-checking each
    /// binding against this attribute's schema.
    pub fn apply(&self, parameters: Parameters) -> Result<RelationQuery, SchemaError> {
        // Check that type of the `is` parameter matches the attribute's data type
        self.conform(parameters.get("is"))
            .map_err(|e| e.at("is".to_string()))?;

        // Check that if `this` parameter is provided, it has entity type.
        if let Some(this) = parameters.get("this")
            && let Some(actual) = this.content_type()
            && actual != Type::Entity
        {
            return Err(SchemaError::TypeError {
                binding: "this".to_string(),
                expected: Type::Entity,
                actual: this.clone(),
            });
        }

        // Get the entity term (this), converting from Term<Value>
        let of = parameters
            .get("this")
            .and_then(|t| t.clone().try_into().ok())
            .unwrap_or(Term::blank());

        // Get the value term (is)
        let is = parameters.get("is").cloned().unwrap_or(Term::blank());

        // Get the cause term
        let cause = parameters
            .get("cause")
            .and_then(|t| t.clone().try_into().ok())
            .unwrap_or(Term::blank());

        Ok(RelationQuery::new(
            Term::Constant(self.domain().to_string()),
            Term::Constant(self.name().to_string()),
            of,
            is,
            cause,
            Some(RelationDescriptor::new(
                self.content_type(),
                self.cardinality(),
            )),
        ))
    }

    /// Encode this attribute descriptor as CBOR for hashing
    ///
    /// Creates a CBOR-encoded representation with fields:
    /// - domain: domain
    /// - name: name
    /// - cardinality: cardinality
    /// - type: content_type
    ///
    /// Description is excluded from the encoding.
    pub fn to_cbor_bytes(&self) -> Vec<u8> {
        use serde::Serialize;

        #[derive(Serialize)]
        struct CborAttributeDescriptor<'a> {
            domain: &'a str,
            name: &'a str,
            cardinality: Cardinality,
            #[serde(rename = "type")]
            content_type: Option<Type>,
        }

        let schema = CborAttributeDescriptor {
            domain: self.domain(),
            name: self.name(),
            cardinality: self.cardinality(),
            content_type: self.content_type(),
        };

        serde_ipld_dagcbor::to_vec(&schema).expect("CBOR encoding should not fail")
    }

    /// Compute blake3 hash of this attribute descriptor
    ///
    /// Returns a 32-byte blake3 hash of the CBOR-encoded descriptor
    pub fn hash(&self) -> blake3::Hash {
        let cbor_bytes = self.to_cbor_bytes();
        blake3::hash(&cbor_bytes)
    }

    /// Format this attribute's hash as a URI
    ///
    /// Returns a string in the format: `the:{base58(blake3)}`
    pub fn to_uri(&self) -> String {
        let encoded = self.hash().as_bytes().as_ref().to_base58();
        format!("the:{encoded}")
    }

    /// Parse an attribute URI and extract the hash
    ///
    /// Expects format: `the:{base58(blake3)}`
    /// Returns None if the format is invalid
    pub fn parse_uri(uri: &str) -> Option<blake3::Hash> {
        let encoded = uri.strip_prefix("the:")?;
        let bytes = base58::FromBase58::from_base58(encoded).ok()?;
        if bytes.len() != 32 {
            return None;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(blake3::Hash::from(arr))
    }
}

impl From<AttributeDescriptor> for Entity {
    fn from(descriptor: AttributeDescriptor) -> Self {
        descriptor.to_uri().parse().expect("valid entity URI")
    }
}

impl From<&AttributeDescriptor> for ArtifactsAttribute {
    fn from(descriptor: &AttributeDescriptor) -> Self {
        ArtifactsAttribute::from(&descriptor.the)
    }
}

impl From<AttributeDescriptor> for ArtifactsAttribute {
    fn from(descriptor: AttributeDescriptor) -> Self {
        ArtifactsAttribute::from(descriptor.the)
    }
}

impl Serialize for AttributeDescriptor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Attribute", 4)?;
        state.serialize_field("domain", self.domain())?;
        state.serialize_field("name", self.name())?;
        state.serialize_field("description", self.description())?;
        state.serialize_field("type", &self.content_type())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for AttributeDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Domain,
            Name,
            Description,
            #[serde(rename = "type")]
            DataType,
        }

        struct AttributeVisitor;

        impl<'de> Visitor<'de> for AttributeVisitor {
            type Value = AttributeDescriptor;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Attribute")
            }

            fn visit_map<V>(self, mut map: V) -> Result<AttributeDescriptor, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut domain: Option<String> = None;
                let mut name: Option<String> = None;
                let mut description: Option<String> = None;
                let mut data_type = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Domain => {
                            if domain.is_some() {
                                return Err(de::Error::duplicate_field("domain"));
                            }
                            domain = Some(map.next_value()?);
                        }
                        Field::Name => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        Field::Description => {
                            if description.is_some() {
                                return Err(de::Error::duplicate_field("description"));
                            }
                            description = Some(map.next_value()?);
                        }
                        Field::DataType => {
                            if data_type.is_some() {
                                return Err(de::Error::duplicate_field("data_type"));
                            }
                            data_type = Some(map.next_value()?);
                        }
                    }
                }

                let domain = domain.ok_or_else(|| de::Error::missing_field("domain"))?;
                let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                let description =
                    description.ok_or_else(|| de::Error::missing_field("description"))?;
                let data_type = data_type.ok_or_else(|| de::Error::missing_field("data_type"))?;

                let the = format!("{domain}/{name}")
                    .parse::<The>()
                    .map_err(de::Error::custom)?;
                Ok(AttributeDescriptor::new(
                    the,
                    description,
                    Cardinality::One,
                    data_type,
                ))
            }
        }

        deserializer.deserialize_struct(
            "Attribute",
            &["domain", "name", "description", "data_type"],
            AttributeVisitor,
        )
    }
}
