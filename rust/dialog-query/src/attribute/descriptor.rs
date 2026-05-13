use crate::Parameters;
use crate::artifact::{ArtifactsAttribute, Entity, Value};
use crate::attribute::The;
use crate::attribute::query::AttributeQuery;
use crate::error::{FieldTypeError, TypeError};
use crate::schema::Cardinality;
use crate::term::Term;
use crate::type_system::{Composite, Type as Kind};
use crate::types::Any;
use crate::types::Type as ValueType;

use base58::ToBase58;
use serde::{Deserialize, Serialize};

/// A validated attribute–value pair with its cardinality, produced by
/// [`AttributeDescriptor::resolve`]. Used inside [`ConceptStatement`](crate::concept::descriptor::ConceptStatement)
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributeDescriptor {
    the: The,
    #[serde(default)]
    description: String,
    #[serde(default)]
    cardinality: Cardinality,
    #[serde(
        rename = "as",
        default,
        skip_serializing_if = "Option::is_none",
        with = "content_type_serde"
    )]
    content_type: Option<Kind>,
}

impl AttributeDescriptor {
    /// Creates a new descriptor from a validated [`The`] selector
    /// with a primitive content type.
    ///
    /// For richer content types (e.g. [`Composite::Directory`]) use
    /// [`AttributeDescriptor::with_kind`].
    pub fn new(
        the: The,
        description: impl Into<String>,
        cardinality: Cardinality,
        content_type: Option<ValueType>,
    ) -> Self {
        Self::with_kind(
            the,
            description,
            cardinality,
            content_type.map(Kind::primitive),
        )
    }

    /// Creates a new descriptor from a validated [`The`] selector
    /// with an arbitrary [`Kind`] content type.
    pub fn with_kind(
        the: The,
        description: impl Into<String>,
        cardinality: Cardinality,
        content_type: Option<Kind>,
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

    /// Returns the expected content type, or `None` if any type is
    /// accepted.
    pub fn content_type(&self) -> Option<&Kind> {
        self.content_type.as_ref()
    }

    /// Legacy storage-codec view: returns the primitive [`ValueType`]
    /// when the content type collapses to a single primitive.
    /// Returns `None` for composite content types (e.g.
    /// [`Composite::Directory`]) and for absent content types.
    pub fn value_type(&self) -> Option<ValueType> {
        self.content_type.as_ref().and_then(|k| k.as_value_type())
    }

    /// Checks that the given parameter's type is compatible with this
    /// attribute's content type.
    pub fn check(&self, parameter: &Term<Any>) -> Result<(), FieldTypeError> {
        match (self.content_type(), parameter.content_type()) {
            (None, _) => Ok(()),
            (_, None) => Ok(()),
            (Some(_expected), _actual) => Ok(()),
        }
    }

    /// Type-checks an optional parameter against this attribute.
    pub fn conform(&self, parameter: Option<&Term<Any>>) -> Result<(), FieldTypeError> {
        if let Some(param) = parameter {
            self.check(param)?;
        }
        Ok(())
    }

    /// The expected primitive type of a single matched row's value
    /// for this attribute. For a primitive content_type this is the
    /// primitive itself; for a [`Composite::Directory`] content_type
    /// it is the directory's inner primitive type (since each row
    /// binds one value of that type). Returns `None` for anything
    /// else, including absent content_type.
    fn expected_value_type(&self) -> Option<ValueType> {
        match self.content_type()? {
            Kind::Primitive(_) => self.content_type()?.as_value_type(),
            Kind::Composite(_, composites) => composites.iter().find_map(|c| match c {
                Composite::Directory(inner) => inner.as_value_type(),
                _ => None,
            }),
        }
    }

    /// Validates a concrete [`Value`] against this attribute's content type and
    /// produces an [`Attribution`] — a validated (attribute, value, cardinality)
    /// triple ready for storage.
    ///
    /// For a [`Composite::Directory`] content type, the value is
    /// type-checked against the directory's inner primitive type:
    /// each row holds a single value of the inner type, the
    /// directory aggregation lives a layer above.
    pub fn resolve(&self, value: Value) -> Result<Attribution, FieldTypeError> {
        let type_matches = match self.expected_value_type() {
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
            Err(FieldTypeError::TypeMismatch {
                expected: self.expected_value_type().unwrap(),
                actual: Box::new(Term::Constant(value.clone())),
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

    /// Builds an [`AttributeQuery`] from named parameters, type-checking each
    /// binding against this attribute's schema.
    pub fn apply(&self, parameters: Parameters) -> Result<AttributeQuery, TypeError> {
        // Check that type of the `is` parameter matches the attribute's data type
        self.conform(parameters.get("is"))
            .map_err(|e| e.at("is".to_string()))?;

        // Check that if `this` parameter is provided, it has entity type.
        if let Some(this) = parameters.get("this")
            && let Some(actual) = this.content_type()
            && actual != ValueType::Entity
        {
            return Err(TypeError::TypeMismatch {
                binding: "this".to_string(),
                expected: ValueType::Entity,
                actual: Box::new(this.clone()),
            });
        }

        // Get the entity term (this), converting from Parameter to Term<Entity>
        let of = match parameters.get("this").cloned() {
            Some(Term::Variable {
                name: Some(name), ..
            }) => Term::var(name),
            Some(Term::Variable { name: None, .. }) => Term::blank(),
            Some(Term::Constant(value)) => Term::Constant(value),
            None => Term::blank(),
        };

        // Get the value parameter (is) -- passed directly as Parameter
        let is = parameters
            .get("is")
            .cloned()
            .unwrap_or_else(Term::<Any>::blank);

        // Get the cause term
        let cause = match parameters.get("cause").cloned() {
            Some(Term::Variable {
                name: Some(name), ..
            }) => Term::var(name),
            Some(Term::Variable { name: None, .. }) => Term::blank(),
            Some(Term::Constant(value)) => Term::Constant(value),
            None => Term::blank(),
        };

        Ok(AttributeQuery::new(
            Term::Constant(Value::from(self.the().clone())),
            of,
            is,
            cause,
            Some(self.cardinality()),
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
            content_type: Option<&'a Kind>,
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

/// Schema-document shorthand for the `as:` field of an
/// [`AttributeDescriptor`].
///
/// The descriptor stores its content type as a full
/// [`type_system::Type`], which has a verbose JSON encoding. Schema
/// authors write the value in shorthand:
///
/// - A bare primitive name, e.g. `"Text"` or `"Entity"`, denotes
///   the primitive type.
/// - `{"directory": "<TypeName>"}` denotes a [`Composite::Directory`]
///   wrapping the named primitive.
///
/// Other [`type_system::Type`] shapes (variant, product, unions,
/// `Nothing`-widened optionality, etc.) are not part of the
/// schema-document vocabulary at this time and round-trip through
/// the descriptor only via direct Rust construction.
mod content_type_serde {
    use super::{Composite, Kind, ValueType};
    use serde::ser::Error as SerError;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum Schema {
        Scalar(ValueType),
        Directory { directory: ValueType },
    }

    /// Reduce a [`Kind`] to its schema-document shorthand. Returns
    /// `None` when the kind is not expressible in the shorthand
    /// vocabulary.
    fn to_schema(kind: &Kind) -> Option<Schema> {
        match kind {
            Kind::Primitive(_) => kind.as_value_type().map(Schema::Scalar),
            Kind::Composite(_, composites) => {
                // Look for a single Directory shape with a singleton
                // primitive inner. Other composite shapes aren't
                // expressible in the shorthand.
                let mut iter = composites.iter();
                let only = iter.next()?;
                if iter.next().is_some() {
                    return None;
                }
                match only {
                    Composite::Directory(inner) => inner
                        .as_value_type()
                        .map(|vt| Schema::Directory { directory: vt }),
                    _ => None,
                }
            }
        }
    }

    fn from_schema(schema: Schema) -> Kind {
        match schema {
            Schema::Scalar(vt) => Kind::primitive(vt),
            Schema::Directory { directory } => Kind::directory(Kind::primitive(directory)),
        }
    }

    pub fn serialize<S>(value: &Option<Kind>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(kind) => {
                let schema = to_schema(kind).ok_or_else(|| {
                    <S::Error as SerError>::custom(format!(
                        "content type {kind:?} is not expressible in schema-document shorthand"
                    ))
                })?;
                schema.serialize(serializer)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Kind>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt = Option::<Schema>::deserialize(deserializer)?;
        Ok(opt.map(from_schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::the;
    use crate::types::Type;

    #[dialog_common::test]
    fn it_serializes_all_fields() {
        let attr = AttributeDescriptor::new(
            the!("io.gozala.person/name"),
            "Name of the person",
            Cardinality::One,
            Some(Type::String),
        );
        let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
        assert_eq!(json["the"], "io.gozala.person/name");
        assert_eq!(json["description"], "Name of the person");
        assert_eq!(json["cardinality"], "one");
        assert_eq!(json["as"], "Text");
    }

    #[dialog_common::test]
    fn it_serializes_many_cardinality() {
        let attr = AttributeDescriptor::new(
            the!("person/email"),
            "Email addresses",
            Cardinality::Many,
            Some(Type::String),
        );
        let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
        assert_eq!(json["cardinality"], "many");
    }

    #[dialog_common::test]
    fn it_omits_as_when_type_is_none() {
        let attr = AttributeDescriptor::new(
            the!("person/data"),
            "Arbitrary data",
            Cardinality::One,
            None,
        );
        let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
        assert!(json.get("as").is_none() || json["as"].is_null());
    }

    #[dialog_common::test]
    fn it_serializes_all_value_types() {
        let cases: Vec<(Type, &str)> = vec![
            (Type::Bytes, "Bytes"),
            (Type::Entity, "Entity"),
            (Type::Boolean, "Boolean"),
            (Type::String, "Text"),
            (Type::UnsignedInt, "UnsignedInteger"),
            (Type::SignedInt, "SignedInteger"),
            (Type::Float, "Float"),
            (Type::Symbol, "Symbol"),
        ];
        for (ty, expected_name) in cases {
            let attr =
                AttributeDescriptor::new(the!("test/field"), "test", Cardinality::One, Some(ty));
            let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
            assert_eq!(
                json["as"], expected_name,
                "Type {:?} should serialize as {expected_name}",
                ty
            );
        }
    }

    #[dialog_common::test]
    fn it_serializes_directory_content_type() {
        let attr = AttributeDescriptor::with_kind(
            the!("person/favorites"),
            "Favorites by name",
            Cardinality::One,
            Some(Kind::directory(Kind::primitive(Type::Entity))),
        );
        let json: serde_json::Value = serde_json::to_value(&attr).unwrap();
        assert_eq!(json["as"], serde_json::json!({ "directory": "Entity" }));
    }

    #[dialog_common::test]
    fn it_deserializes_directory_content_type() {
        let json = r#"{
            "the": "person/favorites",
            "as": { "directory": "Entity" }
        }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.content_type(),
            Some(&Kind::directory(Kind::primitive(Type::Entity)))
        );
        // value_type() returns None for composite content types.
        assert_eq!(attr.value_type(), None);
    }

    #[dialog_common::test]
    fn it_round_trips_directory_content_type() {
        let original = AttributeDescriptor::with_kind(
            the!("person/favorites"),
            "Favorites by name",
            Cardinality::One,
            Some(Kind::directory(Kind::primitive(Type::String))),
        );
        let json = serde_json::to_string(&original).unwrap();
        let restored: AttributeDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[dialog_common::test]
    fn it_deserializes_all_fields() {
        let json = r#"{
            "the": "io.gozala.person/name",
            "description": "Name of the person",
            "cardinality": "one",
            "as": "Text"
        }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(attr.domain(), "io.gozala.person");
        assert_eq!(attr.name(), "name");
        assert_eq!(attr.description(), "Name of the person");
        assert_eq!(attr.cardinality(), Cardinality::One);
        assert_eq!(attr.value_type(), Some(Type::String));
    }

    #[dialog_common::test]
    fn it_defaults_optional_fields() {
        let json = r#"{ "the": "person/name" }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(attr.domain(), "person");
        assert_eq!(attr.name(), "name");
        assert_eq!(attr.description(), "");
        assert_eq!(attr.cardinality(), Cardinality::One);
        assert_eq!(attr.content_type(), None);
    }

    #[dialog_common::test]
    fn it_deserializes_many_cardinality() {
        let json = r#"{
            "the": "person/email",
            "cardinality": "many",
            "as": "Text"
        }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(attr.cardinality(), Cardinality::Many);
    }

    #[dialog_common::test]
    fn it_round_trips() {
        let original = AttributeDescriptor::new(
            the!("diy.cook/quantity"),
            "Amount needed",
            Cardinality::Many,
            Some(Type::UnsignedInt),
        );
        let json = serde_json::to_string(&original).unwrap();
        let restored: AttributeDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[dialog_common::test]
    fn it_rejects_missing_the() {
        let json = r#"{ "description": "oops", "as": "Text" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject attribute without 'the'");
    }

    #[dialog_common::test]
    fn it_rejects_the_without_slash() {
        let json = r#"{ "the": "no-slash-here" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject 'the' without '/' separator");
    }

    #[dialog_common::test]
    fn it_rejects_empty_the() {
        let json = r#"{ "the": "" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject empty 'the'");
    }

    #[dialog_common::test]
    fn it_ignores_type_field() {
        let json = r#"{ "the": "person/name", "type": "Text" }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(
            attr.content_type(),
            None,
            "'type' field should be ignored - must use 'as'"
        );
    }

    #[dialog_common::test]
    fn it_rejects_unknown_type() {
        let json = r#"{ "the": "person/name", "as": "Blob" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject unknown type name 'Blob'");
    }

    #[dialog_common::test]
    fn it_rejects_invalid_cardinality() {
        let json = r#"{ "the": "person/name", "cardinality": "few" }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject invalid cardinality 'few'");
    }

    #[dialog_common::test]
    fn it_rejects_the_exceeding_max_length() {
        let long = format!("{}/{}", "a".repeat(50), "b".repeat(50));
        let json = format!(r#"{{ "the": "{long}" }}"#);
        let result = serde_json::from_str::<AttributeDescriptor>(&json);
        assert!(
            result.is_err(),
            "should reject 'the' exceeding max selector length"
        );
    }

    #[dialog_common::test]
    fn it_rejects_old_domain_name_format() {
        let json = r#"{
            "domain": "person",
            "name": "email",
            "description": "Email",
            "type": "String"
        }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(
            result.is_err(),
            "should reject old format using domain/name/type fields"
        );
    }

    #[dialog_common::test]
    fn it_rejects_non_string_type() {
        let json = r#"{ "the": "person/name", "as": 42 }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(result.is_err(), "should reject non-string type value");
    }

    #[dialog_common::test]
    fn it_rejects_non_string_cardinality() {
        let json = r#"{ "the": "person/name", "cardinality": 1 }"#;
        let result = serde_json::from_str::<AttributeDescriptor>(json);
        assert!(
            result.is_err(),
            "should reject non-string cardinality value"
        );
    }
}
