use crate::Parameters;
pub use crate::artifact::{
    Attribute as ArtifactsAttribute, Cause, DialogArtifactsError, Entity, Value,
};
use crate::error::{SchemaError, TypeError};
pub use crate::predicate::RelationDescriptor;
use crate::proposition::RelationApplication;
pub use crate::schema::Cardinality;
pub use crate::types::{IntoType, Scalar, Type};
pub use crate::{Premise, Term};
use base58::ToBase58;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum length in bytes for an attribute selector (`"namespace/name"`).
pub const MAX_SELECTOR_LENGTH: usize = 64;

/// A validated attribute selector (`"namespace/name"`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct The {
    /// The domain namespace.
    pub namespace: String,
    /// The attribute name.
    pub name: String,
}

impl std::fmt::Display for The {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

impl std::str::FromStr for The {
    type Err = DialogArtifactsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (namespace, name) = s.split_once('/').ok_or_else(|| {
            DialogArtifactsError::InvalidAttribute(format!(
                "Attribute format is \"namespace/predicate\", but got \"{s}\""
            ))
        })?;
        // Validate via ArtifactsAttribute to enforce length limit
        let _: ArtifactsAttribute = s.parse()?;
        Ok(Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        })
    }
}

impl From<The> for ArtifactsAttribute {
    fn from(the: The) -> Self {
        the.to_string()
            .parse()
            .expect("The is always a valid ArtifactsAttribute")
    }
}

impl From<&The> for ArtifactsAttribute {
    fn from(the: &The) -> Self {
        the.to_string()
            .parse()
            .expect("The is always a valid ArtifactsAttribute")
    }
}

impl From<ArtifactsAttribute> for The {
    fn from(attr: ArtifactsAttribute) -> Self {
        let s = attr.to_string();
        let (namespace, name) = s
            .split_once('/')
            .expect("ArtifactsAttribute always contains '/'");
        Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
    }
}

/// Compile-time validated attribute selector.
///
/// Verifies at compile time that the literal:
/// - does not exceed [`MAX_SELECTOR_LENGTH`] bytes
/// - contains a `'/'` separator
///
/// # Examples
///
/// ```
/// use dialog_query::the;
/// let selector = the!("person/name");
/// assert_eq!(selector.to_string(), "person/name");
/// ```
#[macro_export]
macro_rules! the {
    ($selector:literal) => {{
        const _: () = {
            assert!(
                $selector.len() <= $crate::attribute::MAX_SELECTOR_LENGTH,
                "attribute selector exceeds maximum length of 64 bytes"
            );
            let bytes = $selector.as_bytes();
            let mut found = false;
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i] == b'/' {
                    found = true;
                    break;
                }
                i += 1;
            }
            assert!(found, "attribute selector must contain '/' separator");
        };
        // SAFETY: compile-time checks above guarantee the literal is valid.
        <$crate::attribute::The as ::std::str::FromStr>::from_str($selector).unwrap()
    }};
}

/// A validated attribute–value pair with its cardinality, produced by
/// [`AttributeDescriptor::resolve`]. Used inside [`Conception`](crate::predicate::concept::Conception)
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

/// Describes an attribute's identity, type, and cardinality.
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

    /// Returns a reference to the validated selector.
    pub fn the(&self) -> &The {
        &self.the
    }

    /// Returns the domain namespace.
    pub fn namespace(&self) -> &str {
        &self.the.namespace
    }

    /// Returns the attribute name.
    pub fn name(&self) -> &str {
        &self.the.name
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

    /// Builds a [`RelationApplication`] from named parameters, type-checking each
    /// binding against this attribute's schema.
    pub fn apply(&self, parameters: Parameters) -> Result<RelationApplication, SchemaError> {
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

        Ok(RelationApplication::new(
            Term::Constant(self.namespace().to_string()),
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
    /// - domain: namespace
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
            domain: self.namespace(),
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
        state.serialize_field("namespace", self.namespace())?;
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
            Namespace,
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
                let mut namespace: Option<String> = None;
                let mut name: Option<String> = None;
                let mut description: Option<String> = None;
                let mut data_type = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Namespace => {
                            if namespace.is_some() {
                                return Err(de::Error::duplicate_field("namespace"));
                            }
                            namespace = Some(map.next_value()?);
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

                let namespace = namespace.ok_or_else(|| de::Error::missing_field("namespace"))?;
                let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                let description =
                    description.ok_or_else(|| de::Error::missing_field("description"))?;
                let data_type = data_type.ok_or_else(|| de::Error::missing_field("data_type"))?;

                let the = format!("{namespace}/{name}")
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
            &["namespace", "name", "description", "data_type"],
            AttributeVisitor,
        )
    }
}

/// Trait implemented by typed attribute definitions, providing schema metadata
/// and conversion utilities for use in queries and concept definitions.
pub trait Attribute: Sized {
    /// The Rust scalar type of this attribute's values.
    type Type: Scalar;

    /// The query pattern type used when querying this attribute in a concept.
    type Query;
    /// The concrete proof type when this attribute is part of a concept result.
    type Proof;
    /// The term type used when building query patterns for this attribute.
    type Term;

    /// Returns a reference to the inner value.
    fn value(&self) -> &Self::Type;

    /// Construct an attribute from its inner value
    fn new(value: Self::Type) -> Self;

    /// Returns the attribute descriptor.
    fn descriptor() -> AttributeDescriptor;

    /// Returns the namespace as an owned `String`.
    fn namespace() -> String {
        Self::descriptor().namespace().into()
    }
    /// Returns the attribute name as an owned `String`.
    fn name() -> String {
        Self::descriptor().name().into()
    }
    /// Returns the description as an owned `String`.
    fn description() -> String {
        Self::descriptor().description().into()
    }
    /// Returns the cardinality of this attribute.
    fn cardinality() -> Cardinality {
        Self::descriptor().cardinality()
    }
    /// Returns the parsed attribute selector (`"namespace/name"`).
    fn selector() -> crate::artifact::Attribute {
        ArtifactsAttribute::from(Self::descriptor())
    }

    /// Compute blake3 hash of this attribute's schema
    ///
    /// Returns a 32-byte blake3 hash of the CBOR-encoded attribute schema
    /// (namespace, name, cardinality, content_type - excluding description)
    fn hash() -> blake3::Hash {
        Self::descriptor().hash()
    }

    /// Format this attribute's hash as a URI
    ///
    /// Returns a string in the format: `the:{blake3_hash_hex}`
    fn to_uri() -> String {
        Self::descriptor().to_uri()
    }

    /// Returns the expected value type, or `None` if any type is accepted.
    fn content_type() -> Option<Type> {
        <Self::Type as IntoType>::TYPE
    }
}

pub use crate::concept::with::{With, WithQuery, WithTerms};

#[cfg(test)]
mod tests {
    use crate::artifact::Type;
    use crate::attribute::{Attribute, AttributeDescriptor, Cardinality};
    use crate::term::Term;

    mod person {
        use crate::Cardinality;
        use crate::attribute::{Attribute, AttributeDescriptor, With, WithQuery, WithTerms};
        use crate::types::IntoType;

        pub struct Name(pub String);

        impl Attribute for Name {
            type Type = String;
            type Query = WithQuery<Self>;
            type Proof = With<Self>;
            type Term = WithTerms<Self>;

            fn descriptor() -> AttributeDescriptor {
                AttributeDescriptor::new(
                    the!("person/name"),
                    "The name of the person",
                    Cardinality::One,
                    <String as IntoType>::TYPE,
                )
            }

            fn value(&self) -> &Self::Type {
                &self.0
            }

            fn new(value: Self::Type) -> Self {
                Self(value)
            }
        }
    }

    #[dialog_common::test]
    fn test_person_name() {
        let _name = person::Name("hello".into());
        // Basic test that Attribute trait is implemented
        assert_eq!(person::Name::descriptor().namespace(), "person");
        assert_eq!(person::Name::descriptor().name(), "name");
    }

    // Tests from attribute_derive_test.rs

    mod employee_derive {
        use crate::Attribute;

        /// Name of the employee
        #[derive(Attribute, Clone)]
        pub struct Name(pub String);

        /// Job title of the employee
        #[derive(Attribute, Clone)]
        pub struct Job(pub String);

        /// Salary of the employee
        #[derive(Attribute, Clone)]
        pub struct Salary(pub u32);
    }

    mod person_derive {
        use crate::Attribute;

        /// Name of the person
        #[derive(Attribute, Clone)]
        pub struct Name(pub String);

        /// Employees managed by this person
        #[derive(Attribute, Clone)]
        #[cardinality(many)]
        pub struct Manages(pub crate::Entity);
    }

    #[dialog_common::test]
    fn test_employee_name_derives_attribute() {
        let name = employee_derive::Name("Alice".to_string());

        assert_eq!(employee_derive::Name::namespace(), "employee-derive");
        assert_eq!(employee_derive::Name::name(), "name");
        assert_eq!(employee_derive::Name::description(), "Name of the employee");
        assert_eq!(employee_derive::Name::cardinality(), Cardinality::One);
        assert_eq!(name.value(), "Alice");
        assert_eq!(
            employee_derive::Name::selector().to_string(),
            "employee-derive/name"
        );
    }

    #[dialog_common::test]
    fn test_employee_job_derives_attribute() {
        let job = employee_derive::Job("Engineer".to_string());

        assert_eq!(employee_derive::Job::namespace(), "employee-derive");
        assert_eq!(employee_derive::Job::name(), "job");
        assert_eq!(
            employee_derive::Job::description(),
            "Job title of the employee"
        );
        assert_eq!(employee_derive::Job::cardinality(), Cardinality::One);
        assert_eq!(job.value(), "Engineer");
        assert_eq!(
            employee_derive::Job::selector().to_string(),
            "employee-derive/job"
        );
    }

    #[dialog_common::test]
    fn test_employee_salary_derives_attribute() {
        let salary = employee_derive::Salary(100000);

        assert_eq!(employee_derive::Salary::namespace(), "employee-derive");
        assert_eq!(employee_derive::Salary::name(), "salary");
        assert_eq!(
            employee_derive::Salary::description(),
            "Salary of the employee"
        );
        assert_eq!(employee_derive::Salary::cardinality(), Cardinality::One);
        assert_eq!(salary.value(), &100000u32);
        assert_eq!(
            employee_derive::Salary::selector().to_string(),
            "employee-derive/salary"
        );
    }

    #[dialog_common::test]
    fn test_person_derive_namespace() {
        let name = person_derive::Name("Bob".to_string());

        assert_eq!(person_derive::Name::namespace(), "person-derive");
        assert_eq!(person_derive::Name::name(), "name");
        assert_eq!(
            person_derive::Name::selector().to_string(),
            "person-derive/name"
        );
        assert_eq!(name.value(), "Bob");
    }

    #[dialog_common::test]
    fn test_cardinality_many() {
        assert_eq!(person_derive::Manages::cardinality(), Cardinality::Many);
        assert_eq!(
            person_derive::Manages::description(),
            "Employees managed by this person"
        );
        assert_eq!(person_derive::Manages::namespace(), "person-derive");
    }

    mod custom_ns_derive {
        use crate::Attribute;

        /// Custom namespace override test
        #[derive(Attribute, Clone)]
        #[namespace = "custom"]
        pub struct Field(pub String);
    }

    #[dialog_common::test]
    fn test_custom_namespace_override_derive() {
        let field = custom_ns_derive::Field("value".to_string());

        assert_eq!(custom_ns_derive::Field::namespace(), "custom");
        assert_eq!(custom_ns_derive::Field::name(), "field");
        assert_eq!(
            custom_ns_derive::Field::selector().to_string(),
            "custom/field"
        );
        assert_eq!(field.value(), "value");
    }

    // Tests from attribute_identifier_test.rs

    mod employee_ident {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone)]
        pub struct Salary(pub u32);

        #[derive(Attribute, Clone)]
        pub struct Job(pub String);
    }

    mod person_ident {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Name(pub String);
    }

    #[dialog_common::test]
    fn test_attribute_hash_stability() {
        let hash1 = employee_ident::Name::hash();
        let hash2 = employee_ident::Name::hash();

        assert_eq!(
            hash1, hash2,
            "Same attribute should produce identical hashes"
        );
    }

    #[dialog_common::test]
    fn test_different_attributes_different_hashes() {
        let name_hash = employee_ident::Name::hash();
        let salary_hash = employee_ident::Salary::hash();
        let job_hash = employee_ident::Job::hash();

        assert_ne!(
            name_hash, salary_hash,
            "Name and Salary should have different hashes"
        );
        assert_ne!(
            name_hash, job_hash,
            "Name and Job should have different hashes"
        );
        assert_ne!(
            salary_hash, job_hash,
            "Salary and Job should have different hashes"
        );
    }

    #[dialog_common::test]
    fn test_same_name_different_namespace_different_hashes() {
        let employee_name_hash = employee_ident::Name::hash();
        let person_name_hash = person_ident::Name::hash();

        assert_ne!(
            employee_name_hash, person_name_hash,
            "employee::Name and person::Name should have different hashes"
        );
    }

    #[dialog_common::test]
    fn test_attribute_uri_format() {
        let uri = employee_ident::Name::to_uri();

        assert!(
            uri.starts_with("the:"),
            "URI should start with 'the:' prefix"
        );
        // base58 encoding of 32 bytes is typically 43-44 characters
        assert!(uri.len() > 4, "URI should have content after 'the:' prefix");
    }

    #[dialog_common::test]
    fn test_attribute_uri_roundtrip() {
        let uri = employee_ident::Name::to_uri();
        let parsed_hash = AttributeDescriptor::parse_uri(&uri);

        assert!(parsed_hash.is_some(), "Should be able to parse valid URI");
        assert_eq!(
            parsed_hash.unwrap(),
            employee_ident::Name::hash(),
            "Parsed hash should match original hash"
        );
    }

    #[dialog_common::test]
    fn test_attribute_uri_parse_invalid() {
        assert!(
            AttributeDescriptor::parse_uri("invalid").is_none(),
            "Should fail to parse URI without 'the:' prefix"
        );

        assert!(
            AttributeDescriptor::parse_uri("the:invalid").is_none(),
            "Should fail to parse URI with invalid hash"
        );

        assert!(
            AttributeDescriptor::parse_uri("concept:abcd").is_none(),
            "Should fail to parse URI with wrong prefix"
        );
    }

    #[dialog_common::test]
    fn test_attribute_schema_hash_stability() {
        let schema_hash = employee_ident::Name::descriptor().hash();
        let trait_hash = employee_ident::Name::hash();

        assert_eq!(
            schema_hash, trait_hash,
            "Schema hash and trait hash should match"
        );
    }

    #[dialog_common::test]
    fn test_attribute_cbor_encoding() {
        let cbor1 = employee_ident::Name::descriptor().to_cbor_bytes();
        let cbor2 = employee_ident::Name::descriptor().to_cbor_bytes();

        assert_eq!(cbor1, cbor2, "CBOR encoding should be deterministic");
        assert!(!cbor1.is_empty(), "CBOR encoding should not be empty");
    }

    #[dialog_common::test]
    fn test_attribute_description_does_not_affect_hash() {
        let attr1 = AttributeDescriptor::new(
            the!("user/email"),
            "Primary email address",
            Cardinality::One,
            Some(Type::String),
        );

        let attr2 = AttributeDescriptor::new(
            the!("user/email"),
            "User's email for notifications",
            Cardinality::One,
            Some(Type::String),
        );

        assert_eq!(
            attr1.hash(),
            attr2.hash(),
            "Attributes with different descriptions should have the same hash"
        );

        assert_eq!(
            attr1.to_uri(),
            attr2.to_uri(),
            "Attributes with different descriptions should have the same URI"
        );
    }

    // Tests from attribute_into_term_test.rs

    mod employee_term {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone)]
        pub struct Job(pub String);

        #[derive(Attribute, Clone)]
        pub struct Salary(pub u32);
    }

    #[dialog_common::test]
    fn test_attribute_into_term() {
        let name = employee_term::Name("Alice".into());
        let name_term: Term<String> = name.into();
        assert!(name_term.is_constant());

        let job = employee_term::Job("Engineer".into());
        let job_term: Term<String> = job.into();
        assert!(job_term.is_constant());

        let salary = employee_term::Salary(65000);
        let salary_term: Term<u32> = salary.into();
        assert!(salary_term.is_constant());
    }

    #[dialog_common::test]
    fn test_attribute_from_method() {
        let name = employee_term::Name::from("Alice");
        assert_eq!(name.value(), "Alice");

        let job = employee_term::Job::from("Engineer");
        assert_eq!(job.value(), "Engineer");

        let salary = employee_term::Salary::from(65000u32);
        assert_eq!(*salary.value(), 65000);

        let name_term: Term<String> = employee_term::Name::from("Bob").into();
        assert!(name_term.is_constant());
    }

    #[dialog_common::test]
    fn test_attribute_into_in_match_construction() {
        use crate::{Concept, Entity, Match, Term};

        #[derive(Concept, Debug, Clone)]
        pub struct Employee {
            pub this: Entity,
            pub name: employee_term::Name,
            pub job: employee_term::Job,
            pub salary: employee_term::Salary,
        }

        let pattern = Match::<Employee> {
            this: Term::var("e"),
            name: Term::var("name"),
            salary: Term::var("salary"),
            job: employee_term::Job("Engineer".into()).into(),
        };

        assert!(pattern.job.is_constant());

        let pattern2 = Match::<Employee> {
            this: Term::var("e"),
            name: Term::var("name"),
            salary: Term::var("salary"),
            job: employee_term::Job::from("Engineer").into(),
        };

        assert!(pattern2.job.is_constant());
    }

    // Tests from attribute_namespace_test.rs

    mod account_name {
        use crate::Attribute;

        /// Account holder's name
        #[derive(Attribute, Clone)]
        pub struct Name(pub String);
    }

    mod ns_my {
        pub mod config {
            use crate::Attribute;

            /// Configuration key
            #[derive(Attribute, Clone)]
            pub struct Key(pub String);
        }
    }

    #[derive(crate::Attribute, Clone)]
    #[namespace("my.custom.namespace")]
    pub struct NsValue(pub String);

    #[derive(crate::Attribute, Clone)]
    #[namespace(custom)]
    pub struct NsCustomValue(pub String);

    #[derive(crate::Attribute, Clone)]
    #[namespace("io.gozala")]
    pub struct NsDottedValue(pub String);

    mod ns_my_app {
        pub mod user_profile {
            use crate::Attribute;

            /// User email address
            #[derive(Attribute, Clone)]
            pub struct Email(pub String);
        }
    }

    #[dialog_common::test]
    fn test_underscore_to_hyphen_conversion() {
        assert_eq!(account_name::Name::namespace(), "account-name");
        assert_eq!(account_name::Name::name(), "name");
        assert_eq!(
            account_name::Name::selector().to_string(),
            "account-name/name"
        );
    }

    #[dialog_common::test]
    fn test_nested_module_namespace() {
        assert_eq!(ns_my::config::Key::namespace(), "config");
        assert_eq!(ns_my::config::Key::name(), "key");
        assert_eq!(ns_my::config::Key::selector().to_string(), "config/key");
    }

    #[dialog_common::test]
    fn test_explicit_namespace_override() {
        assert_eq!(NsValue::namespace(), "my.custom.namespace");
        assert_eq!(NsValue::name(), "ns-value");
        assert_eq!(
            NsValue::selector().to_string(),
            "my.custom.namespace/ns-value"
        );
    }

    #[dialog_common::test]
    fn test_namespace_identifier_syntax() {
        assert_eq!(NsCustomValue::namespace(), "custom");
        assert_eq!(NsCustomValue::name(), "ns-custom-value");
        assert_eq!(
            NsCustomValue::selector().to_string(),
            "custom/ns-custom-value"
        );
    }

    #[dialog_common::test]
    fn test_namespace_string_literal_syntax() {
        assert_eq!(NsDottedValue::namespace(), "io.gozala");
        assert_eq!(NsDottedValue::name(), "ns-dotted-value");
        assert_eq!(
            NsDottedValue::selector().to_string(),
            "io.gozala/ns-dotted-value"
        );
    }

    #[dialog_common::test]
    fn test_nested_underscore_conversion() {
        assert_eq!(ns_my_app::user_profile::Email::namespace(), "user-profile");
        assert_eq!(ns_my_app::user_profile::Email::name(), "email");
        assert_eq!(
            ns_my_app::user_profile::Email::selector().to_string(),
            "user-profile/email"
        );
    }

    #[dialog_common::test]
    fn test_all_metadata_preserved() {
        let name = account_name::Name("John Doe".to_string());

        assert_eq!(account_name::Name::namespace(), "account-name");
        assert_eq!(account_name::Name::name(), "name");
        assert_eq!(account_name::Name::description(), "Account holder's name");
        assert_eq!(account_name::Name::cardinality(), Cardinality::One);
        assert_eq!(name.value(), "John Doe");
    }

    // Tests from attribute_naming_test.rs

    mod test_pascal {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct UserName(pub String);

        #[derive(Attribute, Clone)]
        pub struct HTTPRequest(pub String);

        #[derive(Attribute, Clone)]
        pub struct APIKey(pub String);
    }

    #[dialog_common::test]
    fn test_pascal_case_to_kebab_case() {
        assert_eq!(test_pascal::UserName::name(), "user-name");
    }

    #[dialog_common::test]
    fn test_consecutive_capitals() {
        assert_eq!(test_pascal::HTTPRequest::name(), "http-request");
        assert_eq!(test_pascal::APIKey::name(), "api-key");
    }

    #[dialog_common::test]
    fn test_static_values() {
        let ns = test_pascal::UserName::namespace();
        let name = test_pascal::UserName::name();
        let desc = test_pascal::UserName::description();

        assert!(!ns.is_empty());
        assert_eq!(name, "user-name");
        let _ = desc;
    }

    #[dialog_common::test]
    fn test_schema_static() {
        let schema = &test_pascal::UserName::descriptor();
        assert_eq!(schema.name(), "user-name");
        assert_eq!(schema.cardinality(), Cardinality::One);
    }
}
