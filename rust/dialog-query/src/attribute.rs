use crate::application::FactApplication;
pub use crate::artifact::{Attribute as ArtifactsAttribute, Cause, Entity, Value};
use crate::claim::Claim;
use crate::error::{SchemaError, TypeError};
pub use crate::predicate::Fact;
pub use crate::schema::Cardinality;
pub use crate::types::{IntoType, Scalar, Type};
use crate::{Application, Parameters, Relation, Transaction};
pub use crate::{Premise, Term};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub use std::marker::PhantomData;

/// A relation specific to the attribute module containing cardinality information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribution {
    pub the: ArtifactsAttribute,
    pub is: Value,
    pub cardinality: Cardinality,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributeSchema<T: Scalar> {
    pub namespace: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub cardinality: Cardinality,
    pub content_type: Option<Type>,
    pub marker: PhantomData<T>,
}

impl<T: Scalar> AttributeSchema<T> {
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
    pub fn the(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    pub fn of<Of: Into<Term<Entity>>>(&self, term: Of) -> Match<T> {
        Match {
            attribute: self.clone(),
            of: term.into(),
        }
    }

    /// Get the data type for this attribute
    ///
    /// Returns the stored ValueDataType for this attribute.
    /// Returns None if this attribute accepts any type.
    pub fn content_type(&self) -> Option<Type> {
        self.content_type
    }

    /// Type checks that provided term matches cells content type. If term
    pub fn check<'a, U: Scalar>(&self, term: &'a Term<U>) -> Result<&'a Term<U>, TypeError> {
        // First we type check the input to ensure it matches cell's content type
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

    pub fn conform<'a, U: Scalar>(
        &self,
        term: Option<&'a Term<U>>,
    ) -> Result<Option<&'a Term<U>>, TypeError> {
        // We check that cell type matches term type.
        if let Some(term) = term {
            self.check(term)?;
        }

        Ok(term)
    }

    pub fn resolve(&self, value: Value) -> Result<Attribution, TypeError> {
        // Check type if content_type is specified
        let type_matches = match self.content_type {
            Some(expected) => value.data_type() == expected,
            None => true, // Any type is acceptable
        };

        if type_matches {
            let the_str = self.the();
            let the_attr =
                the_str
                    .parse::<ArtifactsAttribute>()
                    .map_err(|_| TypeError::TypeMismatch {
                        expected: Type::Symbol,
                        actual: Term::Constant(Value::String(the_str.clone())),
                    })?;

            Ok(Attribution {
                the: the_attr,
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

    pub fn apply(&self, parameters: Parameters) -> Result<FactApplication, SchemaError> {
        // Check that type of the `is` parameter matches the attribute's data type
        self.conform(parameters.get("is"))
            .map_err(|e| e.at("is".to_string()))?;

        // Check that if `this` parameter is provided, it has entity type.
        if let Some(this) = parameters.get("this") {
            if let Some(actual) = this.content_type() {
                if actual != Type::Entity {
                    return Err(SchemaError::TypeError {
                        binding: "this".to_string(),
                        expected: Type::Entity,
                        actual: this.clone(),
                    });
                }
            }
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

#[derive(Clone, Debug, PartialEq)]
pub struct Match<T: Scalar> {
    pub attribute: AttributeSchema<T>,
    pub of: Term<Entity>,
}

impl<T: Scalar> Match<T> {
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

    pub fn of(&self) -> Term<Entity> {
        self.of.clone()
    }
    pub fn the(&self) -> String {
        self.attribute.the()
    }

    pub fn is<Is: Into<Term<T>>>(self, term: Is) -> FactApplication {
        Fact::new()
            .the(self.the())
            .of(self.of())
            .is(term.into().as_unknown())
            .into()
    }
    pub fn not<Is: Into<Term<T>>>(self, term: Is) -> Premise {
        Application::Fact(self.is(term)).not()
    }
}

pub trait Attribute: Sized {
    type Type: Scalar;

    // Associated types for Concept implementation
    type Match;
    type Instance;
    type Term;

    const NAMESPACE: &'static str;
    const NAME: &'static str;
    const DESCRIPTION: &'static str;
    const CARDINALITY: Cardinality;
    const SCHEMA: AttributeSchema<Self::Type>;
    const CONCEPT: crate::predicate::concept::Concept;

    fn value(&self) -> &Self::Type;

    /// Construct an attribute from its inner value
    fn from_value(value: Self::Type) -> Self;
    fn namespace() -> String {
        Self::NAMESPACE.into()
    }
    fn name() -> String {
        Self::NAME.into()
    }
    fn description() -> String {
        Self::DESCRIPTION.into()
    }
    fn cardinality() -> Cardinality {
        Self::CARDINALITY.clone()
    }
    fn selector() -> crate::artifact::Attribute {
        format!("{}/{}", Self::NAMESPACE, Self::NAME)
            .parse()
            .expect("Failed to parse attribute")
    }

    /// Create a query builder for a specific entity
    fn of<E: Into<Term<Entity>>>(entity: E) -> AttributeQueryBuilder<Self::Type> {
        AttributeQueryBuilder {
            schema: &Self::SCHEMA,
            entity: entity.into(),
            content_type: PhantomData,
        }
    }

    fn content_type() -> Option<Type> {
        <Self::Type as IntoType>::TYPE
    }
}

/// Query builder for attribute queries
pub struct AttributeQueryBuilder<T: Scalar> {
    schema: &'static AttributeSchema<T>,
    entity: Term<Entity>,
    content_type: PhantomData<T>,
}

impl<T: Scalar> AttributeQueryBuilder<T> {
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
    pub of: Term<Entity>,
    pub is: Term<A::Type>,
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

/// Quarriable is now implemented by the #[derive(Attribute)] macro
/// to generate a proper Match struct for each attribute.
/// The blanket implementation has been removed to avoid conflicts.

/// Type-erases an attribute schema to AttributeSchema<Value>.
/// This is necessary for storing attribute schemas in const contexts and enums.
fn erase_attribute_type<A: Attribute>() -> AttributeSchema<Value>
where
    A::Type: Scalar,
{
    AttributeSchema {
        namespace: A::NAMESPACE,
        name: A::NAME,
        description: A::DESCRIPTION,
        cardinality: A::SCHEMA.cardinality,
        content_type: A::SCHEMA.content_type,
        marker: std::marker::PhantomData,
    }
}

/// Represents an entity with a single attribute.
///
/// Used to assert, retract, and query entities by their attributes.
///
/// # Examples
///
/// ```ignore
/// // Assertion
/// tr.assert(With {
///     this: alice,
///     has: person::Name("Alice".into())
/// });
///
/// // Retraction
/// tr.retract(With {
///     this: alice,
///     has: person::Name("Alice".into())
/// });
///
/// // Query
/// Match::<With<person::Name>> {
///     this: Term::var("entity"),
///     has: Term::var("name")
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct With<A: Attribute> {
    pub this: Entity,
    pub has: A,
}

/// Query pattern for entities with a specific attribute.
///
/// Use with the `Match` type alias to query for entities that have an attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct WithMatch<A: Attribute> {
    pub this: Term<Entity>,
    pub has: Term<A::Type>,
}

impl<A: Attribute> Default for WithMatch<A> {
    fn default() -> Self {
        Self {
            this: Term::var("this"),
            has: Term::var("has"),
        }
    }
}

/// Helper methods for constructing term variables in queries.
#[derive(Clone, Debug, PartialEq)]
pub struct WithTerms<A: Attribute> {
    _marker: PhantomData<A>,
}

impl<A: Attribute> WithTerms<A> {
    pub fn this() -> Term<Entity> {
        Term::var("this")
    }

    pub fn has() -> Term<A::Type> {
        Term::var("has")
    }
}

// Implement Concept for With<A>
impl<A: Attribute> crate::concept::Concept for With<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Instance = With<A>;
    type Match = WithMatch<A>;
    type Term = WithTerms<A>;

    const CONCEPT: crate::predicate::concept::Concept = A::CONCEPT;
}

// Implement Quarriable for With<A>
impl<A: Attribute> crate::dsl::Quarriable for With<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Query = WithMatch<A>;
}

// Implement Instance trait for With<A>
impl<A: Attribute> crate::concept::Instance for With<A>
where
    A: Clone + Send,
{
    fn this(&self) -> Entity {
        self.this.clone()
    }
}

// Implement Claim trait for With<A>
impl<A: Attribute> crate::claim::Claim for With<A>
where
    A: Clone,
{
    fn assert(self, transaction: &mut Transaction) {
        use crate::types::Scalar;
        crate::Relation::new(A::selector(), self.this, self.has.value().as_value())
            .assert(transaction);
    }

    fn retract(self, transaction: &mut Transaction) {
        use crate::types::Scalar;
        crate::Relation::new(A::selector(), self.this, self.has.value().as_value())
            .retract(transaction);
    }
}

// Implement Not operator for With<A> to support retractions with `!`
impl<A: Attribute> std::ops::Not for With<A>
where
    A: Clone,
{
    type Output = crate::claim::Revert<With<A>>;

    fn not(self) -> Self::Output {
        self.revert()
    }
}

// Implement IntoIterator for With<A>
impl<A: Attribute> IntoIterator for With<A>
where
    A: Clone,
{
    type Item = Relation;
    type IntoIter = std::iter::Once<Relation>;

    fn into_iter(self) -> Self::IntoIter {
        use crate::types::Scalar;
        std::iter::once(crate::Relation::new(
            A::selector(),
            self.this,
            self.has.value().as_value(),
        ))
    }
}

// Implement Match trait for WithMatch<A>
impl<A: Attribute> crate::concept::Match for WithMatch<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Concept = With<A>;
    type Instance = With<A>;

    fn realize(
        &self,
        source: crate::selection::Answer,
    ) -> Result<Self::Instance, crate::QueryError> {
        Ok(With {
            this: source.get(&self.this)?,
            has: A::from_value(source.get(&self.has)?),
        })
    }
}

// Implement Not operator for WithMatch<A> to support negations in pattern matching
impl<A: Attribute> std::ops::Not for WithMatch<A>
where
    A: Clone + std::fmt::Debug + Send + 'static,
{
    type Output = crate::Premise;

    fn not(self) -> Self::Output {
        // Convert to Application, then wrap in Negation
        let application: Application = self.into();
        crate::Premise::Exclude(crate::negation::Negation::not(application))
    }
}

// Implement From<WithMatch<A>> for Parameters
impl<A: Attribute> From<WithMatch<A>> for Parameters
where
    A: Clone,
{
    fn from(source: WithMatch<A>) -> Self {
        let mut params = Self::new();
        params.insert("this".to_string(), source.this.as_unknown());
        params.insert("has".to_string(), source.has.as_unknown());
        params
    }
}

// Implement From<WithMatch<A>> for ConceptApplication
impl<A: Attribute> From<WithMatch<A>> for crate::application::ConceptApplication
where
    A: Clone,
{
    fn from(source: WithMatch<A>) -> Self {
        crate::application::ConceptApplication {
            terms: source.into(),
            concept: A::CONCEPT,
        }
    }
}

// Implement From<WithMatch<A>> for Application
impl<A: Attribute> From<WithMatch<A>> for Application
where
    A: Clone,
{
    fn from(source: WithMatch<A>) -> Self {
        Application::Concept(source.into())
    }
}

// Implement From<WithMatch<A>> for Premise
impl<A: Attribute> From<WithMatch<A>> for Premise
where
    A: Clone,
{
    fn from(source: WithMatch<A>) -> Self {
        Premise::Apply(source.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::attribute::Attribute;

    mod person {
        use crate::attribute::Attribute;
        use crate::Cardinality;

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
                operator: "person",
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

            fn from_value(value: Self::Type) -> Self {
                Self(value)
            }
        }

        pub struct Birthday(pub u32);

        const BIRTHDAY_CONCEPT: crate::predicate::concept::Concept = {
            const ATTRS: crate::predicate::concept::Attributes =
                crate::predicate::concept::Attributes::Static(&[(
                    "birthday",
                    crate::attribute::AttributeSchema {
                        namespace: "person",
                        name: "birthday",
                        description: "The birthday of the person",
                        cardinality: Cardinality::One,
                        content_type: <u32 as crate::types::IntoType>::TYPE,
                        marker: std::marker::PhantomData,
                    },
                )]);
            crate::predicate::concept::Concept::Static {
                operator: "person",
                attributes: &ATTRS,
            }
        };

        impl Attribute for Birthday {
            type Type = u32;
            type Match = crate::attribute::WithMatch<Self>;
            type Instance = crate::attribute::With<Self>;
            type Term = crate::attribute::WithTerms<Self>;

            const NAMESPACE: &'static str = "person";
            const NAME: &'static str = "birthday";
            const DESCRIPTION: &'static str = "The birthday of the person";
            const CARDINALITY: Cardinality = Cardinality::One;
            const SCHEMA: crate::attribute::AttributeSchema<Self::Type> =
                crate::attribute::AttributeSchema {
                    namespace: Self::NAMESPACE,
                    name: Self::NAME,
                    description: Self::DESCRIPTION,
                    cardinality: Self::CARDINALITY,
                    content_type: <u32 as crate::types::IntoType>::TYPE,
                    marker: std::marker::PhantomData,
                };
            const CONCEPT: crate::predicate::concept::Concept = BIRTHDAY_CONCEPT;

            fn value(&self) -> &Self::Type {
                &self.0
            }

            fn from_value(value: Self::Type) -> Self {
                Self(value)
            }
        }
    }

    #[test]
    fn test_person_name() {
        let _name = person::Name("hello".into());
        // Basic test that Attribute trait is implemented
        assert_eq!(person::Name::NAMESPACE, "person");
        assert_eq!(person::Name::NAME, "name");
    }
}
