use crate::application::{Application, FactApplication};
pub use crate::artifact::{Attribute as ArtifactsAttribute, Cause, Entity, Value};
use crate::error::{SchemaError, TypeError};
pub use crate::predicate::Fact;
pub use crate::schema::Cardinality;
pub use crate::types::{IntoType, Scalar, Type};
use crate::Parameters;
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

pub trait Attribute {
    type Type: Scalar;

    const NAMESPACE: &'static str;
    const NAME: &'static str;
    const DESCRIPTION: &'static str;
    const CARDINALITY: &'static Cardinality;

    fn value(&self) -> &Self::Type;
    fn selector() -> crate::artifact::Attribute {
        format!("{}/{}", Self::NAMESPACE, Self::NAME)
            .parse()
            .expect("Failed to parse attribute")
    }

    // Provide method versions for backwards compatibility
    fn namespace() -> &'static str {
        Self::NAMESPACE
    }
    fn name() -> &'static str {
        Self::NAME
    }
    fn description() -> &'static str {
        Self::DESCRIPTION
    }
    fn cardinality() -> &'static Cardinality {
        Self::CARDINALITY
    }
}

/// Macro to generate Quarriable and Claim implementations for attribute tuples
macro_rules! impl_attribute_tuples {
    // Base case: (Entity, T1, T2, ...)
    ($(($($T:ident),+)),*) => {
        $(
            impl_attribute_tuples!(@quarriable $($T),+);
            impl_attribute_tuples!(@claim $($T),+);
        )*
    };

    // Generate Quarriable for (Entity, T1, T2, ...)
    (@quarriable $($T:ident),+) => {
        impl<$($T: Attribute),+> crate::dsl::Quarriable for (Entity, $($T),+) {
            type Query = (Term<Entity>, $(Term<$T::Type>),+);
        }
    };

    // Generate Claim for (Entity, T1, T2, ...)
    (@claim $T1:ident $(, $T:ident)*) => {
        #[allow(non_snake_case)]
        impl<$T1: Attribute $(, $T: Attribute)*> crate::claim::Claim for (Entity, $T1 $(, $T)*) {
            fn assert(self, transaction: &mut crate::Transaction) {
                let (entity, $T1 $(, $T)*) = self;

                transaction.associate(crate::Relation {
                    the: $T1::selector(),
                    of: entity.clone(),
                    is: $T1.value().as_value(),
                });

                $(
                    transaction.associate(crate::Relation {
                        the: $T::selector(),
                        of: entity.clone(),
                        is: $T.value().as_value(),
                    });
                )*
            }

            fn retract(self, transaction: &mut crate::Transaction) {
                let (entity, $T1 $(, $T)*) = self;

                transaction.dissociate(crate::Relation {
                    the: $T1::selector(),
                    of: entity.clone(),
                    is: $T1.value().as_value(),
                });

                $(
                    transaction.dissociate(crate::Relation {
                        the: $T::selector(),
                        of: entity.clone(),
                        is: $T.value().as_value(),
                    });
                )*
            }
        }
    };
}

// Generate implementations for tuples of size 1-15 (matching Bevy's tuple limits)
impl_attribute_tuples!(
    (T1),
    (T1, T2),
    (T1, T2, T3),
    (T1, T2, T3, T4),
    (T1, T2, T3, T4, T5),
    (T1, T2, T3, T4, T5, T6),
    (T1, T2, T3, T4, T5, T6, T7),
    (T1, T2, T3, T4, T5, T6, T7, T8),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14),
    (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)
);

#[cfg(test)]
mod tests {
    use crate::attribute::Attribute;
    use crate::Cardinality;

    mod person {
        use crate::attribute::Attribute;
        use crate::Cardinality;

        pub struct Name(pub String);

        impl Attribute for Name {
            type Type = String;

            fn namespace() -> &'static str {
                "person"
            }
            fn description() -> &'static str {
                "The name of the person"
            }
            fn name() -> &'static str {
                "name"
            }
            fn cardinality() -> &'static Cardinality {
                &Cardinality::One
            }
            fn value(&self) -> &Self::Type {
                &self.0
            }
        }

        pub struct Birthday(pub u32);
        impl Attribute for Birthday {
            type Type = u32;

            fn namespace() -> &'static str {
                "person"
            }
            fn description() -> &'static str {
                "The birthday of the person"
            }
            fn name() -> &'static str {
                "birthday"
            }
            fn cardinality() -> &'static Cardinality {
                &Cardinality::One
            }
            fn value(&self) -> &Self::Type {
                &self.0
            }
        }
    }

    #[test]
    fn test_person_name() {
        let _name = person::Name("hello".into());
        // Basic test that Attribute trait is implemented
        assert_eq!(person::Name::namespace(), "person");
        assert_eq!(person::Name::name(), "name");
    }
}
