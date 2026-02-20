use crate::application::FactApplication;
pub use crate::artifact::{Attribute as ArtifactsAttribute, Cause, Entity, Value};
use crate::error::{SchemaError, TypeError};
pub use crate::predicate::Fact;
pub use crate::schema::Cardinality;
pub use crate::types::{IntoType, Scalar, Type};
use crate::{Application, Parameters};
pub use crate::{Premise, Term};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub use std::marker::PhantomData;

/// A validated attribute–value pair with its cardinality, produced by
/// [`AttributeSchema::resolve`]. Used inside [`Conception`](crate::predicate::concept::Conception)
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

/// Static schema describing an attribute's identity, type, and cardinality.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeSchema<T: Scalar> {
    /// The domain namespace this attribute belongs to (e.g. `"person"`).
    pub namespace: &'static str,
    /// The attribute name within its namespace (e.g. `"name"`).
    pub name: &'static str,
    /// Human-readable description of the attribute.
    pub description: &'static str,
    /// Whether this attribute allows one or many values per entity.
    pub cardinality: Cardinality,
    /// The expected value type, or `None` if any type is accepted.
    pub content_type: Option<Type>,
    /// Phantom data to carry the Rust-level scalar type.
    pub marker: PhantomData<T>,
}

impl<T: Scalar> AttributeSchema<T> {
    /// Creates a new schema with [`Cardinality::One`] and the given content type.
    pub fn new(
        namespace: &'static str,
        name: &'static str,
        description: &'static str,
        content_type: Type,
    ) -> Self {
        Self {
            namespace,
            name,
            description,
            cardinality: Cardinality::One,
            content_type: Some(content_type),
            marker: PhantomData,
        }
    }

    /// Returns the fully-qualified attribute selector (`"namespace/name"`).
    pub fn the(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    /// Binds this attribute to an entity term, producing a [`Match`] that
    /// can be used in queries.
    pub fn of<Of: Into<Term<Entity>>>(&self, term: Of) -> Match<T> {
        Match {
            attribute: self.clone(),
            of: term.into(),
        }
    }

    /// Returns the expected value type for this attribute, or `None` if it
    /// accepts any type.
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
        let type_matches = match self.content_type {
            Some(expected) => value.data_type() == expected,
            None => true,
        };

        if type_matches {
            let the =
                self.the()
                    .parse::<ArtifactsAttribute>()
                    .map_err(|_| TypeError::TypeMismatch {
                        expected: Type::Symbol,
                        actual: Term::Constant(Value::String(self.the().clone())),
                    })?;

            Ok(Attribution {
                the,
                is: value.clone(),
                cardinality: self.cardinality,
            })
        } else {
            Err(TypeError::TypeMismatch {
                expected: self.content_type.unwrap(), // Safe because we checked Some above
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
        self.cardinality
            .estimate(true, of, is)
            .expect("Should succeed if we know attribute")
    }

    /// Builds a [`FactApplication`] from named parameters, type-checking each
    /// binding against this attribute's schema.
    pub fn apply(&self, parameters: Parameters) -> Result<FactApplication, SchemaError> {
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

        // Get the attribute term - parse the string name to an Attribute
        let the = Term::Constant(
            self.the()
                .parse::<ArtifactsAttribute>()
                .expect("Failed to parse attribute name"),
        );

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

        Ok(FactApplication::new(the, of, is, cause, self.cardinality))
    }

    /// Encode this attribute schema as CBOR for hashing
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
        struct CborAttributeSchema<'a> {
            domain: &'a str,
            name: &'a str,
            cardinality: &'a Cardinality,
            #[serde(rename = "type")]
            content_type: &'a Option<Type>,
        }

        let schema = CborAttributeSchema {
            domain: self.namespace,
            name: self.name,
            cardinality: &self.cardinality,
            content_type: &self.content_type,
        };

        serde_ipld_dagcbor::to_vec(&schema).expect("CBOR encoding should not fail")
    }

    /// Compute blake3 hash of this attribute schema
    ///
    /// Returns a 32-byte blake3 hash of the CBOR-encoded schema
    pub fn hash(&self) -> blake3::Hash {
        let cbor_bytes = self.to_cbor_bytes();
        blake3::hash(&cbor_bytes)
    }

    /// Format this attribute's hash as a URI
    ///
    /// Returns a string in the format: `the:{blake3_hash_hex}`
    pub fn to_uri(&self) -> String {
        format!("the:{}", self.hash().to_hex())
    }

    /// Parse an attribute URI and extract the hash
    ///
    /// Expects format: `the:{blake3_hash_hex}`
    /// Returns None if the format is invalid
    pub fn parse_uri(uri: &str) -> Option<blake3::Hash> {
        let uri = uri.strip_prefix("the:")?;
        blake3::Hash::from_hex(uri).ok()
    }
}

impl<T: Scalar> Serialize for AttributeSchema<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Attribute", 4)?;
        state.serialize_field("namespace", self.namespace)?;
        state.serialize_field("name", self.name)?;
        state.serialize_field("description", self.description)?;
        state.serialize_field("type", &self.content_type)?;
        state.end()
    }
}

impl<'de, T: Scalar> Deserialize<'de> for AttributeSchema<T> {
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

        struct AttributeVisitor<T>(PhantomData<T>);

        impl<'de, T: Scalar> Visitor<'de> for AttributeVisitor<T> {
            type Value = AttributeSchema<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Attribute")
            }

            fn visit_map<V>(self, mut map: V) -> Result<AttributeSchema<T>, V::Error>
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

                // Convert String to &'static str by leaking memory
                // This is the trade-off for keeping &'static str fields
                let namespace: &'static str = Box::leak(namespace.into_boxed_str());
                let name: &'static str = Box::leak(name.into_boxed_str());
                let description: &'static str = Box::leak(description.into_boxed_str());

                Ok(AttributeSchema {
                    namespace,
                    name,
                    description,
                    cardinality: Cardinality::One,
                    content_type: data_type,
                    marker: PhantomData,
                })
            }
        }

        deserializer.deserialize_struct(
            "Attribute",
            &["namespace", "name", "description", "data_type"],
            AttributeVisitor(PhantomData),
        )
    }
}

/// A query pattern binding an [`AttributeSchema`] to an entity term.
#[derive(Clone, Debug, PartialEq)]
pub struct Match<T: Scalar> {
    /// The attribute schema this match targets.
    pub attribute: AttributeSchema<T>,
    /// The entity term to match against.
    pub of: Term<Entity>,
}

impl<T: Scalar> Match<T> {
    /// Creates a new match from raw attribute metadata and an entity term.
    pub fn new(
        namespace: &'static str,
        name: &'static str,
        description: &'static str,
        content_type: Type,
        of: Term<Entity>,
    ) -> Self {
        Self {
            attribute: AttributeSchema::new(namespace, name, description, content_type),
            of,
        }
    }

    /// Returns a clone of the entity term.
    pub fn of(&self) -> Term<Entity> {
        self.of.clone()
    }
    /// Returns the fully-qualified attribute selector.
    pub fn the(&self) -> String {
        self.attribute.the()
    }

    /// Constrains this match to a specific value, producing a [`FactApplication`].
    pub fn is<Is: Into<Term<T>>>(self, term: Is) -> FactApplication {
        Fact::new()
            .the(self.the())
            .of(self.of())
            .is(term.into().as_unknown())
            .into()
    }
    /// Negates this match for a specific value, producing a [`Premise`].
    pub fn not<Is: Into<Term<T>>>(self, term: Is) -> Premise {
        Application::Fact(self.is(term)).not()
    }
}

/// Trait implemented by typed attribute definitions, providing schema metadata
/// and conversion utilities for use in queries and concept definitions.
pub trait Attribute: Sized {
    /// The Rust scalar type of this attribute's values.
    type Type: Scalar;

    /// The match pattern type used when querying this attribute in a concept.
    type Match;
    /// The concrete instance type when this attribute is part of a concept result.
    type Instance;
    /// The term type used when building query patterns for this attribute.
    type Term;

    /// The domain namespace (e.g. `"person"`).
    const NAMESPACE: &'static str;
    /// The attribute name within its namespace (e.g. `"name"`).
    const NAME: &'static str;
    /// Human-readable description of this attribute.
    const DESCRIPTION: &'static str;
    /// Whether this attribute allows one or many values per entity.
    const CARDINALITY: Cardinality;
    /// The full static schema for this attribute.
    const SCHEMA: AttributeSchema<Self::Type>;
    /// The concept definition that this attribute belongs to.
    const CONCEPT: crate::predicate::concept::Concept;

    /// Returns a reference to the inner value.
    fn value(&self) -> &Self::Type;

    /// Construct an attribute from its inner value
    fn new(value: Self::Type) -> Self;

    /// Returns the namespace as an owned `String`.
    fn namespace() -> String {
        Self::NAMESPACE.into()
    }
    /// Returns the attribute name as an owned `String`.
    fn name() -> String {
        Self::NAME.into()
    }
    /// Returns the description as an owned `String`.
    fn description() -> String {
        Self::DESCRIPTION.into()
    }
    /// Returns the cardinality of this attribute.
    fn cardinality() -> Cardinality {
        Self::CARDINALITY
    }
    /// Returns the parsed attribute selector (`"namespace/name"`).
    fn selector() -> crate::artifact::Attribute {
        format!("{}/{}", Self::NAMESPACE, Self::NAME)
            .parse()
            .expect("Failed to parse attribute")
    }

    /// Compute blake3 hash of this attribute's schema
    ///
    /// Returns a 32-byte blake3 hash of the CBOR-encoded attribute schema
    /// (namespace, name, cardinality, content_type - excluding description)
    fn hash() -> blake3::Hash {
        let cbor_bytes = Self::SCHEMA.to_cbor_bytes();
        blake3::hash(&cbor_bytes)
    }

    /// Format this attribute's hash as a URI
    ///
    /// Returns a string in the format: `the:{blake3_hash_hex}`
    fn to_uri() -> String {
        Self::SCHEMA.to_uri()
    }

    /// Create a query builder for a specific entity
    fn of<E: Into<Term<Entity>>>(entity: E) -> AttributeQueryBuilder<Self::Type> {
        AttributeQueryBuilder {
            schema: &Self::SCHEMA,
            entity: entity.into(),
            content_type: PhantomData,
        }
    }

    /// Returns the expected value type, or `None` if any type is accepted.
    fn content_type() -> Option<Type> {
        <Self::Type as IntoType>::TYPE
    }
}

/// Query builder for attribute queries, created via [`Attribute::of`].
pub struct AttributeQueryBuilder<T: Scalar> {
    schema: &'static AttributeSchema<T>,
    entity: Term<Entity>,
    content_type: PhantomData<T>,
}

impl<T: Scalar> AttributeQueryBuilder<T> {
    /// Constrains the attribute to a value term, producing a [`Match`].
    pub fn is<V: Into<Term<T>>>(self, _value: V) -> Match<T> {
        Match {
            attribute: self.schema.clone(),
            of: self.entity,
        }
    }
}

/// Query pattern for attributes - enables Match::<AttributeType> { of, is } syntax
#[derive(Clone, Debug, PartialEq)]
pub struct AttributeMatch<A: Attribute> {
    /// The entity term to match against.
    pub of: Term<Entity>,
    /// The value term to match against.
    pub is: Term<A::Type>,
    /// Phantom data to carry the attribute type.
    pub content_type: PhantomData<A>,
}

impl<A: Attribute> AttributeMatch<A> {
    /// Create a new attribute match pattern
    pub fn new(of: Term<Entity>, is: Term<A::Type>) -> Self {
        Self {
            of,
            is,
            content_type: PhantomData,
        }
    }
}

impl<A: Attribute> Default for AttributeMatch<A> {
    fn default() -> Self {
        Self {
            of: Term::var("of"),
            is: Term::var("is"),
            content_type: PhantomData,
        }
    }
}

pub use crate::concept::with::{With, WithMatch, WithTerms};

#[cfg(test)]
mod tests {
    use crate::attribute::Attribute;

    mod person {
        use crate::Cardinality;
        use crate::attribute::Attribute;

        pub struct Name(pub String);

        const NAME_CONCEPT: crate::predicate::concept::Concept = {
            const ATTRS: crate::predicate::concept::Attributes =
                crate::predicate::concept::Attributes::Static(&[(
                    "name",
                    crate::attribute::AttributeSchema {
                        namespace: "person",
                        name: "name",
                        description: "The name of the person",
                        cardinality: Cardinality::One,
                        content_type: <String as crate::types::IntoType>::TYPE,
                        marker: std::marker::PhantomData,
                    },
                )]);
            crate::predicate::concept::Concept::Static {
                description: "",
                attributes: &ATTRS,
            }
        };

        impl Attribute for Name {
            type Type = String;
            type Match = crate::attribute::WithMatch<Self>;
            type Instance = crate::attribute::With<Self>;
            type Term = crate::attribute::WithTerms<Self>;

            const NAMESPACE: &'static str = "person";
            const NAME: &'static str = "name";
            const DESCRIPTION: &'static str = "The name of the person";
            const CARDINALITY: Cardinality = Cardinality::One;
            const SCHEMA: crate::attribute::AttributeSchema<Self::Type> =
                crate::attribute::AttributeSchema {
                    namespace: Self::NAMESPACE,
                    name: Self::NAME,
                    description: Self::DESCRIPTION,
                    cardinality: Self::CARDINALITY,
                    content_type: <String as crate::types::IntoType>::TYPE,
                    marker: std::marker::PhantomData,
                };
            const CONCEPT: crate::predicate::concept::Concept = NAME_CONCEPT;

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
        assert_eq!(person::Name::NAMESPACE, "person");
        assert_eq!(person::Name::NAME, "name");
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
        use crate::Cardinality;

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
        use crate::Cardinality;

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
        use crate::Cardinality;

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
        use crate::Cardinality;

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
        assert_eq!(
            uri.len(),
            4 + 64,
            "URI should be 'the:' + 64 hex chars (32 bytes)"
        );
    }

    #[dialog_common::test]
    fn test_attribute_uri_roundtrip() {
        let uri = employee_ident::Name::to_uri();
        let parsed_hash = crate::attribute::AttributeSchema::<String>::parse_uri(&uri);

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
            crate::attribute::AttributeSchema::<String>::parse_uri("invalid").is_none(),
            "Should fail to parse URI without 'the:' prefix"
        );

        assert!(
            crate::attribute::AttributeSchema::<String>::parse_uri("the:invalid").is_none(),
            "Should fail to parse URI with invalid hash"
        );

        assert!(
            crate::attribute::AttributeSchema::<String>::parse_uri("concept:abcd").is_none(),
            "Should fail to parse URI with wrong prefix"
        );
    }

    #[dialog_common::test]
    fn test_attribute_schema_hash_stability() {
        let schema_hash = employee_ident::Name::SCHEMA.hash();
        let trait_hash = employee_ident::Name::hash();

        assert_eq!(
            schema_hash, trait_hash,
            "Schema hash and trait hash should match"
        );
    }

    #[dialog_common::test]
    fn test_attribute_cbor_encoding() {
        let cbor1 = employee_ident::Name::SCHEMA.to_cbor_bytes();
        let cbor2 = employee_ident::Name::SCHEMA.to_cbor_bytes();

        assert_eq!(cbor1, cbor2, "CBOR encoding should be deterministic");
        assert!(!cbor1.is_empty(), "CBOR encoding should not be empty");
    }

    #[dialog_common::test]
    fn test_attribute_description_does_not_affect_hash() {
        use crate::artifact::Type;
        use crate::attribute::AttributeSchema;

        let attr1 =
            AttributeSchema::<String>::new("user", "email", "Primary email address", Type::String);

        let attr2 = AttributeSchema::<String>::new(
            "user",
            "email",
            "User's email for notifications",
            Type::String,
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
        use crate::Term;

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
        use crate::Term;

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
        use crate::Attribute;

        assert_eq!(account_name::Name::NAMESPACE, "account-name");
        assert_eq!(account_name::Name::NAME, "name");
        assert_eq!(
            account_name::Name::selector().to_string(),
            "account-name/name"
        );
    }

    #[dialog_common::test]
    fn test_nested_module_namespace() {
        use crate::Attribute;

        assert_eq!(ns_my::config::Key::NAMESPACE, "config");
        assert_eq!(ns_my::config::Key::NAME, "key");
        assert_eq!(ns_my::config::Key::selector().to_string(), "config/key");
    }

    #[dialog_common::test]
    fn test_explicit_namespace_override() {
        use crate::Attribute;

        assert_eq!(NsValue::NAMESPACE, "my.custom.namespace");
        assert_eq!(NsValue::NAME, "ns-value");
        assert_eq!(
            NsValue::selector().to_string(),
            "my.custom.namespace/ns-value"
        );
    }

    #[dialog_common::test]
    fn test_namespace_identifier_syntax() {
        use crate::Attribute;

        assert_eq!(NsCustomValue::NAMESPACE, "custom");
        assert_eq!(NsCustomValue::NAME, "ns-custom-value");
        assert_eq!(
            NsCustomValue::selector().to_string(),
            "custom/ns-custom-value"
        );
    }

    #[dialog_common::test]
    fn test_namespace_string_literal_syntax() {
        use crate::Attribute;

        assert_eq!(NsDottedValue::NAMESPACE, "io.gozala");
        assert_eq!(NsDottedValue::NAME, "ns-dotted-value");
        assert_eq!(
            NsDottedValue::selector().to_string(),
            "io.gozala/ns-dotted-value"
        );
    }

    #[dialog_common::test]
    fn test_nested_underscore_conversion() {
        use crate::Attribute;

        assert_eq!(ns_my_app::user_profile::Email::NAMESPACE, "user-profile");
        assert_eq!(ns_my_app::user_profile::Email::NAME, "email");
        assert_eq!(
            ns_my_app::user_profile::Email::selector().to_string(),
            "user-profile/email"
        );
    }

    #[dialog_common::test]
    fn test_all_metadata_preserved() {
        use crate::{Attribute, Cardinality};

        let name = account_name::Name("John Doe".to_string());

        assert_eq!(account_name::Name::NAMESPACE, "account-name");
        assert_eq!(account_name::Name::NAME, "name");
        assert_eq!(account_name::Name::DESCRIPTION, "Account holder's name");
        assert_eq!(account_name::Name::CARDINALITY, Cardinality::One);
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
        assert_eq!(test_pascal::UserName::NAME, "user-name");
    }

    #[dialog_common::test]
    fn test_consecutive_capitals() {
        assert_eq!(test_pascal::HTTPRequest::NAME, "http-request");
        assert_eq!(test_pascal::APIKey::NAME, "api-key");
    }

    #[dialog_common::test]
    fn test_static_values() {
        let ns = test_pascal::UserName::NAMESPACE;
        let name = test_pascal::UserName::NAME;
        let desc = test_pascal::UserName::DESCRIPTION;

        assert!(!ns.is_empty());
        assert_eq!(name, "user-name");
        let _ = desc;
    }

    #[dialog_common::test]
    fn test_schema_static() {
        use crate::Cardinality;

        let schema = &test_pascal::UserName::SCHEMA;
        assert_eq!(schema.name, "user-name");
        assert_eq!(schema.cardinality, Cardinality::One);
    }
}
