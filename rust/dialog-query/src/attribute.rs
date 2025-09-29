use crate::application::FactApplication;
use crate::artifact::ValueDataType;
pub use crate::artifact::{Attribute as ArtifactsAttribute, Entity, Value};
use crate::error::{SchemaError, TypeError};
pub use crate::fact_selector::FactSelector;
pub use crate::term::Term;
pub use crate::types::{IntoValueDataType, Scalar};
use crate::Parameters;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub use std::marker::PhantomData;

/// Cardinality indicates whether an attribute can have one or many values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cardinality {
    One,
    Many,
}

/// A relation specific to the attribute module containing cardinality information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Relation {
    pub the: ArtifactsAttribute,
    pub is: Value,
    pub cardinality: Cardinality,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attribute<T: Scalar> {
    pub namespace: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub cardinality: Cardinality,
    pub data_type: ValueDataType,
    pub marker: PhantomData<T>,
}

impl<T: Scalar> Attribute<T> {
    pub fn new(
        namespace: &'static str,
        name: &'static str,
        description: &'static str,
        data_type: ValueDataType,
    ) -> Self {
        Self {
            namespace,
            name,
            description,
            cardinality: Cardinality::One,
            data_type,
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
    pub fn data_type(&self) -> Option<ValueDataType> {
        Some(self.data_type)
    }

    /// Type checks that provided term matches cells content type. If term
    pub fn check<T: Scalar>(&self, term: &Term<T>) -> Result<&Term<T>, TypeError> {
        let expected = self.data_type();
        // First we type check the input to ensure it matches cell's content type
        if let Some(actual) = term.data_type() {
            if (&actual != expected) {
                Err(TypeError::TypeMismatch {
                    expected: expected.into(),
                    actual: term.into(),
                })?;
            };
        };

        Ok(term)
    }

    pub fn conform<T: Scalar>(
        &self,
        term: Option<&Term<T>>,
    ) -> Result<Option<&Term<T>>, TypeError> {
        // We check that cell type matches term type.
        if let Some(term) = term {
            self.check(term)?;
        }

        Ok(term)
    }

    pub fn resolve(&self, value: Value) -> Result<Relation, TypeError> {
        if value.data_type() == self.data_type {
            Ok(Relation {
                the: self.the().parse().unwrap(),
                is: value.clone(),
                cardinality: self.cardinality,
            })
        } else {
            Err(TypeError::TypeMismatch {
                expected: self.data_type,
                actual: Term::Constant(value),
            })
        }
    }

    pub fn apply(&self, parameters: Parameters) -> Result<FactApplication, SchemaError> {
        // Check that type of the `is` parameter matches the attribute's data type
        self.conform(parameters.get("is"))?;

        // Check that if `this` parameter is provided, it has entity type.
        if let Some(this) = parameters.get("this") {
            if let Some(actual) = this.data_type() {
                if (&actual != Type::Entity) {
                    Err(TypeError::TypeMismatch {
                        expected: Type::Entity,
                        actual: this.into(),
                    })?;
                };
            };
        }?;

        let blank = Term::blank();

        Ok(FactApplication::new(
            self.the().parse().expect("Expected a valid attribute"),
            parameters.get("this").unwrap_or(&blank).into(),
            parameters.get("is").unwrap_or(&blank).into(),
            self.cardinality,
        ))
    }
}

impl<T: Scalar> Serialize for Attribute<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Attribute", 4)?;
        state.serialize_field("namespace", self.namespace)?;
        state.serialize_field("name", self.name)?;
        state.serialize_field("description", self.description)?;
        state.serialize_field("data_type", &self.data_type)?;
        state.end()
    }
}

impl<'de, T: Scalar> Deserialize<'de> for Attribute<T> {
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
            DataType,
        }

        struct AttributeVisitor<T>(PhantomData<T>);

        impl<'de, T: Scalar> Visitor<'de> for AttributeVisitor<T> {
            type Value = Attribute<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct Attribute")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Attribute<T>, V::Error>
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

                Ok(Attribute {
                    namespace,
                    name,
                    description,
                    cardinality: Cardinality::One,
                    data_type,
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

#[derive(Clone, Debug)]
pub struct Match<T: Scalar> {
    pub attribute: Attribute<T>,
    pub of: Term<Entity>,
}

impl<T: Scalar> Match<T> {
    pub fn new(
        namespace: &'static str,
        name: &'static str,
        description: &'static str,
        data_type: ValueDataType,
        of: Term<Entity>,
    ) -> Self {
        Self {
            attribute: Attribute::new(namespace, name, description, data_type),
            of,
        }
    }

    pub fn of(&self) -> Term<Entity> {
        self.of.clone()
    }
    pub fn the(&self) -> String {
        self.attribute.the()
    }

    pub fn is<Is: Into<Term<T>>>(self, term: Is) -> FactSelector<T> {
        FactSelector::new()
            .the(self.the())
            .of(self.of())
            .is(term.into())
    }
    pub fn not<Is: Into<Term<T>>>(self, term: Is) -> FactSelector<T> {
        FactSelector::new()
            .the(self.the())
            .of(self.of())
            .is(term.into())
    }
}
