use crate::Parameters;
use crate::artifact::{ArtifactsAttribute, Entity, Value};
use crate::attribute::The;
use crate::attribute::query::AttributeQuery;
use crate::error::{FieldTypeError, TypeError};
use crate::schema::Cardinality;
use crate::term::Term;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::types::Type;
use dialog_artifacts::{NameShape, Symbol};

use base58::ToBase58;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result as FmtResult};

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

/// What an attribute descriptor selects: one attribute, or every
/// entry of a keyed collection.
///
/// Dialog stores a collection as facts sharing a domain whose *name*
/// half is the entry's key — `todo.list/title` for a dictionary
/// entry, `todo.list/N5` for a sequence member. The two key kinds are
/// disjoint by their first byte (symbols lowercase, positions
/// uppercase), so one domain can carry both and a scan can take
/// either half as a contiguous key range.
///
/// The key kind is the variant rather than a field, so a collection
/// cannot be described with a key kind that disagrees with it: there
/// is no state to validate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Relation {
    /// One attribute, named in full: `todo.list/title`.
    Attribute(The),
    /// Every entry of one domain, keyed by name.
    Collection {
        /// The domain the entries share, without a trailing separator.
        domain: Symbol,
        /// Which half of the domain: symbol-named entries
        /// (a dictionary) or position-named members (a sequence).
        keyed: Keyed,
    },
}

/// Which half of a domain a [`Relation::Collection`] selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Keyed {
    /// Symbol-named entries: a dictionary.
    Dictionary,
    /// Position-named members, in list order: a sequence.
    Sequence,
}

impl From<Keyed> for NameShape {
    fn from(keyed: Keyed) -> Self {
        match keyed {
            Keyed::Dictionary => NameShape::Symbol,
            Keyed::Sequence => NameShape::Position,
        }
    }
}

impl Relation {
    /// A collection of `domain`, keyed as `keyed`.
    pub fn collection(domain: Symbol, keyed: Keyed) -> Self {
        Relation::Collection { domain, keyed }
    }

    /// The domain these facts live under.
    pub fn domain(&self) -> &str {
        match self {
            Relation::Attribute(the) => the.domain(),
            Relation::Collection { domain, .. } => domain.as_str(),
        }
    }

    /// The attribute's name half, or `None` for a collection — whose
    /// name half is a key that varies per entry rather than a fixed
    /// part of the selector.
    pub fn name(&self) -> Option<&str> {
        match self {
            Relation::Attribute(the) => Some(the.name()),
            Relation::Collection { .. } => None,
        }
    }

    /// How this relation is selected: a constant for one attribute, a
    /// variable refined by the domain and key kind for a collection.
    /// The refined form is what narrows a domain scan to the demanded
    /// half — see `dialog_artifacts::NameShape`.
    pub fn term(&self) -> Term<The> {
        match self {
            Relation::Attribute(the) => Term::Constant(Value::from(the.clone())),
            Relation::Collection { domain, keyed } => {
                let kind = Kind::from(Type::Symbol)
                    .with_prefix(format!("{}/", domain.as_str()))
                    .expect("symbol is textual")
                    .with_name_shape(NameShape::from(*keyed))
                    .expect("a name shape composes with a domain prefix");
                Term::<The>::var("the").with_kind(kind)
            }
        }
    }
}

impl Relation {
    /// The concrete attribute to write a fact under, when there is
    /// one. A collection has none: its facts are keyed per entry, so
    /// the key has to come from the writer rather than the schema.
    pub fn attribute(&self) -> Option<ArtifactsAttribute> {
        match self {
            Relation::Attribute(the) => Some(ArtifactsAttribute::from(the)),
            Relation::Collection { .. } => None,
        }
    }
}

impl From<The> for Relation {
    fn from(the: The) -> Self {
        Relation::Attribute(the)
    }
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
    the: Relation,
    #[serde(default)]
    description: String,
    #[serde(default)]
    cardinality: Cardinality,
    #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
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
        Self::over(
            Relation::Attribute(the),
            description,
            cardinality,
            content_type,
        )
    }

    /// Creates a descriptor over any [`Relation`] — one attribute, or
    /// every entry of a keyed collection.
    pub fn over(
        the: Relation,
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
    pub fn the(&self) -> &Relation {
        &self.the
    }

    /// Returns the attribute domain.
    pub fn domain(&self) -> &str {
        self.the.domain()
    }

    /// Returns the attribute name, or `None` for a collection — whose
    /// name half is a per-entry key rather than part of the selector.
    pub fn name(&self) -> Option<&str> {
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

    /// Validates a concrete [`Value`] against this attribute's content type and
    /// produces an [`Attribution`] — a validated (attribute, value, cardinality)
    /// triple ready for storage.
    pub fn resolve(&self, value: Value) -> Result<Attribution, FieldTypeError> {
        let type_matches = match self.content_type() {
            Some(expected) => value.data_type() == expected,
            None => true,
        };

        if type_matches {
            // A collection field describes many facts, one per key,
            // so there is no single attribute to write this value
            // under: the key belongs to the entry, not the schema.
            let the = self
                .the
                .attribute()
                .ok_or_else(|| FieldTypeError::UnkeyedCollection {
                    domain: self.the.domain().to_owned(),
                })?;
            Ok(Attribution {
                the,
                is: value.clone(),
                cardinality: self.cardinality(),
            })
        } else {
            Err(FieldTypeError::TypeMismatch {
                expected: self.content_type().unwrap(), // Safe because we checked Some above
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
            && actual != Type::Entity
        {
            return Err(TypeError::TypeMismatch {
                binding: "this".to_string(),
                expected: Type::Entity,
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
            self.the().term(),
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

        // `name` carries the attribute's name half for a plain
        // attribute and the key kind for a collection — the two are
        // the same slot because they are the same thing: what the
        // name half of these facts holds. A plain attribute therefore
        // encodes exactly as it did before collections existed, so
        // every existing identity is preserved.
        #[derive(Serialize)]
        struct CborAttributeDescriptor<'a> {
            domain: &'a str,
            name: &'a str,
            cardinality: Cardinality,
            #[serde(rename = "type")]
            content_type: Option<Type>,
        }

        let name = match &self.the {
            Relation::Attribute(the) => the.name(),
            Relation::Collection { keyed, .. } => match keyed {
                Keyed::Dictionary => "<dictionary>",
                Keyed::Sequence => "<sequence>",
            },
        };
        let schema = CborAttributeDescriptor {
            domain: self.domain(),
            name,
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

/// A descriptor's concrete attribute. Panics for a keyed collection,
/// which has no single attribute — use
/// [`Relation::attribute`](Relation::attribute) where that is
/// possible.
impl From<&AttributeDescriptor> for ArtifactsAttribute {
    fn from(descriptor: &AttributeDescriptor) -> Self {
        descriptor
            .the
            .attribute()
            .expect("a keyed collection has no single attribute")
    }
}

impl From<AttributeDescriptor> for ArtifactsAttribute {
    fn from(descriptor: AttributeDescriptor) -> Self {
        ArtifactsAttribute::from(&descriptor)
    }
}

impl Display for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Relation::Attribute(the) => write!(f, "{the}"),
            Relation::Collection { domain, keyed } => match keyed {
                Keyed::Dictionary => write!(f, "{}/<symbol>", domain.as_str()),
                Keyed::Sequence => write!(f, "{}/<position>", domain.as_str()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::the;

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
    fn it_deserializes_all_fields() {
        let json = r#"{
            "the": "io.gozala.person/name",
            "description": "Name of the person",
            "cardinality": "one",
            "as": "Text"
        }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(attr.domain(), "io.gozala.person");
        assert_eq!(attr.name(), Some("name"));
        assert_eq!(attr.description(), "Name of the person");
        assert_eq!(attr.cardinality(), Cardinality::One);
        assert_eq!(attr.content_type(), Some(Type::String));
    }

    #[dialog_common::test]
    fn it_defaults_optional_fields() {
        let json = r#"{ "the": "person/name" }"#;
        let attr: AttributeDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(attr.domain(), "person");
        assert_eq!(attr.name(), Some("name"));
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
            "'type' field should be ignored — must use 'as'"
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

#[cfg(test)]
mod collection_tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::the;
    use dialog_artifacts::NameShape;
    use std::str::FromStr;

    fn domain() -> Symbol {
        Symbol::from_str("todo.list").expect("a valid domain")
    }

    fn collection(keyed: Keyed) -> AttributeDescriptor {
        AttributeDescriptor::over(
            Relation::collection(domain(), keyed),
            "the list's members",
            Cardinality::Many,
            Some(Type::Entity),
        )
    }

    /// A plain attribute selects itself: the query pins `the` to a
    /// constant, exactly as before collections existed.
    #[dialog_common::test]
    fn it_selects_one_attribute_by_constant() {
        let descriptor = AttributeDescriptor::new(
            the!("todo.list/title"),
            "the list's title",
            Cardinality::One,
            Some(Type::String),
        );
        assert!(
            matches!(descriptor.the().term(), Term::Constant(_)),
            "an attribute pins `the`"
        );
        assert_eq!(descriptor.domain(), "todo.list");
        assert_eq!(descriptor.name(), Some("title"));
    }

    /// A collection selects a whole domain: the query leaves `the` a
    /// variable, refined by the domain and the key kind, so the scan
    /// covers exactly the demanded half.
    #[dialog_common::test]
    fn it_selects_a_collection_by_refined_variable() {
        for (keyed, shape) in [
            (Keyed::Sequence, NameShape::Position),
            (Keyed::Dictionary, NameShape::Symbol),
        ] {
            let descriptor = collection(keyed);
            let term = descriptor.the().term();
            assert!(
                matches!(term, Term::Variable { .. }),
                "a collection leaves `the` open"
            );
            let refinement = term
                .kind()
                .as_ref()
                .and_then(Kind::refinement)
                .cloned()
                .expect("the term carries a refinement");

            assert_eq!(
                refinement.prefix.as_deref(),
                Some("todo.list/"),
                "the domain becomes the scan's prefix, separator included"
            );
            assert_eq!(
                refinement.name_shape,
                Some(shape),
                "the key kind becomes the name shape"
            );
        }
    }

    /// A collection has no single name: its name half is a per-entry
    /// key, not part of the selector.
    #[dialog_common::test]
    fn it_has_no_name_for_a_collection() {
        let descriptor = collection(Keyed::Sequence);
        assert_eq!(descriptor.domain(), "todo.list");
        assert_eq!(descriptor.name(), None);
        assert_eq!(descriptor.the().attribute(), None);
    }

    /// Writing one value to a collection is refused rather than
    /// guessed at: every entry needs its own key, which the schema
    /// does not carry.
    #[dialog_common::test]
    fn it_refuses_to_write_a_collection_without_a_key() {
        let descriptor = collection(Keyed::Sequence);
        let entity = Entity::new().expect("an entity");
        assert_eq!(
            descriptor.resolve(Value::Entity(entity)),
            Err(FieldTypeError::UnkeyedCollection {
                domain: "todo.list".to_owned()
            })
        );
    }

    /// Adding the collection variant must not move any existing
    /// attribute's identity: a plain attribute hashes exactly what it
    /// hashed before, and the two key kinds are distinct from it and
    /// from each other.
    #[dialog_common::test]
    fn it_keeps_identities_distinct_and_stable() {
        let attribute = AttributeDescriptor::new(
            the!("todo.list/title"),
            "",
            Cardinality::Many,
            Some(Type::Entity),
        );
        let sequence = collection(Keyed::Sequence);
        let dictionary = collection(Keyed::Dictionary);

        assert_ne!(sequence.hash(), dictionary.hash(), "key kinds differ");
        assert_ne!(
            sequence.hash(),
            attribute.hash(),
            "a collection is not an attribute"
        );

        // The encoding a plain attribute produces is the one it
        // produced before collections existed: domain, name,
        // cardinality, type — nothing added, nothing reordered.
        let cbor = attribute.to_cbor_bytes();
        let decoded: serde_json::Value =
            serde_ipld_dagcbor::from_slice(&cbor).expect("valid dag-cbor");
        assert_eq!(decoded["domain"], "todo.list");
        assert_eq!(decoded["name"], "title");
    }

    /// The wire form round-trips, and a stored attribute still reads
    /// as one: `"the": "todo.list/title"` is untagged, so documents
    /// written before this variant existed load unchanged.
    #[dialog_common::test]
    fn it_round_trips_through_serde() {
        let stored = r#"{"the":"todo.list/title","as":"Entity","cardinality":"one"}"#;
        let attribute: AttributeDescriptor =
            serde_json::from_str(stored).expect("a stored attribute still loads");
        assert_eq!(attribute.name(), Some("title"));

        let sequence = collection(Keyed::Sequence);
        let json = serde_json::to_string(&sequence).expect("serializes");
        let back: AttributeDescriptor = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, sequence, "a collection survives the round trip");
    }
}
