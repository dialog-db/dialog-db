use crate::{Attribute, Entity, REFERENCE_MAX, REFERENCE_MIN, Reference, Value, XQueryError};

pub type EntityKeyPart = Reference;
pub type AttributeKeyPart = (Reference, Reference);
pub type ValueKeyPart = (u8, Reference);

pub const ZERO_ENTITY_KEY_PART: EntityKeyPart = REFERENCE_MIN;
pub const ZERO_ATTRIBUTE_KEY_PART: AttributeKeyPart = (REFERENCE_MIN, REFERENCE_MIN);
pub const ZERO_VALUE_KEY_PART: ValueKeyPart = (u8::MIN, REFERENCE_MIN);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum KeyPart {
    Entity(EntityKeyPart),
    Attribute(AttributeKeyPart),
    Value(ValueKeyPart),
}

impl KeyPart {
    pub fn as_entity_part(&self) -> Result<&EntityKeyPart, XQueryError> {
        match self {
            KeyPart::Entity(part) => Ok(part),
            _ => Err(XQueryError::InvalidReference(format!(
                "Expected fragment to be an entity reference"
            ))),
        }
    }

    pub fn as_attribute_part(&self) -> Result<&AttributeKeyPart, XQueryError> {
        match self {
            KeyPart::Attribute(part) => Ok(part),
            _ => Err(XQueryError::InvalidReference(format!(
                "Expected fragment to be an attribute reference"
            ))),
        }
    }

    pub fn as_value_part(&self) -> Result<&ValueKeyPart, XQueryError> {
        match self {
            KeyPart::Value(part) => Ok(part),
            _ => Err(XQueryError::InvalidReference(format!(
                "Expected fragment to be a value reference"
            ))),
        }
    }
}

impl From<Entity> for KeyPart {
    fn from(value: Entity) -> Self {
        KeyPart::Entity(value.into())
    }
}

impl From<Attribute> for KeyPart {
    fn from(value: Attribute) -> Self {
        KeyPart::Attribute(value.into())
    }
}

impl From<Value> for KeyPart {
    fn from(value: Value) -> Self {
        KeyPart::Value((value.data_type().into(), Reference::from(value)))
    }
}

impl From<&Value> for KeyPart {
    fn from(value: &Value) -> Self {
        KeyPart::Value((value.data_type().into(), Reference::from(value)))
    }
}

/// A trait that is implemented by every index key type in a [TripleStore]. When
/// constructing indexes, it is useful to be able to lay out the keys in ways
/// that are optimal for a given index. So, different key types exist that
/// effectively encode the same data but in a different order.
///
/// When implemented, this trait it adds several common helpers for quickly
/// creating keys for known dimensions of the key space, that are useful
/// regardless of the layout of the key.
pub trait IndexKey: Default {
    /// Construct a key of the given type for the given entity, attribute and value parts
    fn create(entity: EntityKeyPart, attribute: AttributeKeyPart, value: ValueKeyPart) -> Self;

    /// Get the entity part of the key
    fn get_entity_part(&self) -> &EntityKeyPart;

    fn get_entity_part_mut(&mut self) -> &mut EntityKeyPart;

    /// Get the attribute part of the key
    fn get_attribute_part(&self) -> &AttributeKeyPart;

    fn get_attribute_part_mut(&mut self) -> &mut AttributeKeyPart;

    /// Get the value part of the key
    fn get_value_part(&self) -> &ValueKeyPart;

    fn get_value_part_mut(&mut self) -> &mut ValueKeyPart;

    /// Get the key parts as a three-element array in entity-attribute-value
    /// order
    fn parts(&self) -> [KeyPart; 3] {
        [
            KeyPart::Entity(*self.get_entity_part()),
            KeyPart::Attribute(*self.get_attribute_part()),
            KeyPart::Value(*self.get_value_part()),
        ]
    }

    /// Construct a key of this type at the lowest bound (all bits are zeroes)
    fn min() -> Self {
        Self::create(
            REFERENCE_MIN,
            (REFERENCE_MIN, REFERENCE_MIN),
            (u8::MIN, REFERENCE_MIN),
        )
    }

    /// Construct a key of this type at the highest bound (all bits are ones)
    fn max() -> Self {
        Self::create(
            REFERENCE_MAX,
            (REFERENCE_MAX, REFERENCE_MAX),
            (u8::MAX, REFERENCE_MAX),
        )
    }

    /// Create a new key based on this one that is scoped to the specified [Entity]
    fn entity(&self, entity: &Entity) -> Self {
        self.entity_part(**entity)
    }

    fn entity_part(&self, fragment: EntityKeyPart) -> Self {
        Self::create(fragment, *self.get_attribute_part(), *self.get_value_part())
    }

    /// Create a new key based on this one that is scoped to the specified [Attribute]
    fn attribute(&self, namespace: &str, predicate: Option<&str>) -> Self {
        let mut attribute: AttributeKeyPart = Attribute {
            namespace: namespace.to_string(),
            predicate: predicate.unwrap_or_default().to_owned(),
        }
        .into();

        if predicate.is_none() {
            attribute.1 = self.get_attribute_part().1;
        };

        self.attribute_part(attribute)
    }

    fn attribute_part(&self, fragment: AttributeKeyPart) -> Self {
        Self::create(*self.get_entity_part(), fragment, *self.get_value_part())
    }

    /// Create a new key based on this one that is scoped to the specified [Value]
    fn value<V>(&self, value: &Value) -> Self {
        self.value_part((value.data_type().into(), Reference::from(value)))
    }

    fn value_part(&self, fragment: ValueKeyPart) -> Self {
        Self::create(
            *self.get_entity_part(),
            *self.get_attribute_part(),
            fragment,
        )
    }
}

/// Templatized implementation of [IndexKey] for any key-like struct
macro_rules! index_key {
    ($keylike:ty) => {
        impl IndexKey for $keylike {
            fn create(
                entity: EntityKeyPart,
                attribute: AttributeKeyPart,
                value: ValueKeyPart,
            ) -> Self {
                Self {
                    entity,
                    attribute,
                    value,
                }
            }

            fn get_entity_part(&self) -> &EntityKeyPart {
                &self.entity
            }

            fn get_entity_part_mut(&mut self) -> &mut EntityKeyPart {
                &mut self.entity
            }

            fn get_attribute_part(&self) -> &AttributeKeyPart {
                &self.attribute
            }

            fn get_attribute_part_mut(&mut self) -> &mut AttributeKeyPart {
                &mut self.attribute
            }

            fn get_value_part(&self) -> &ValueKeyPart {
                &self.value
            }

            fn get_value_part_mut(&mut self) -> &mut ValueKeyPart {
                &mut self.value
            }
        }

        impl<A, V> From<(Entity, A, V)> for $keylike
        where
            Attribute: From<A>,
            Value: From<V>,
        {
            fn from((entity, attribute, value): (Entity, A, V)) -> Self {
                let value = Value::from(value);
                Self {
                    entity: *entity,
                    attribute: Attribute::from(attribute).into(),
                    value: (value.data_type().into(), Reference::from(value)),
                }
            }
        }
    };
}

pub type PrimaryKey = EavKey;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct EavKey {
    pub entity: EntityKeyPart,
    pub attribute: AttributeKeyPart,
    pub value: ValueKeyPart,
}

index_key!(EavKey);

impl From<AevKey> for EavKey {
    fn from(value: AevKey) -> Self {
        Self {
            entity: value.entity,
            attribute: value.attribute,
            value: value.value,
        }
    }
}

impl From<VaeKey> for EavKey {
    fn from(value: VaeKey) -> Self {
        Self {
            entity: value.entity,
            attribute: value.attribute,
            value: value.value,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct AevKey {
    pub attribute: AttributeKeyPart,
    pub entity: EntityKeyPart,
    pub value: ValueKeyPart,
}

index_key!(AevKey);

impl From<EavKey> for AevKey {
    fn from(value: EavKey) -> Self {
        Self {
            entity: value.entity,
            attribute: value.attribute,
            value: value.value,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct VaeKey {
    pub value: ValueKeyPart,
    pub attribute: AttributeKeyPart,
    pub entity: EntityKeyPart,
}

index_key!(VaeKey);

impl From<EavKey> for VaeKey {
    fn from(value: EavKey) -> Self {
        Self {
            entity: value.entity,
            attribute: value.attribute,
            value: value.value,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::Entity;

    use super::{EavKey, IndexKey};

    #[test]
    fn it_can_iterate_over_a_map_using_a_key_range() {
        let min = <EavKey as IndexKey>::min();
        let max = <EavKey as IndexKey>::max();

        let mut map = BTreeMap::<EavKey, ()>::new();

        let mut entities = vec![];

        for _ in 0..100 {
            let entity = Entity::new();
            let base_key = EavKey::default().entity(&entity);

            let name_key = base_key.attribute("test", Some("name"));
            let color_key = base_key.attribute("test", Some("color"));

            map.insert(name_key, ());
            map.insert(color_key, ());

            entities.push(entity);
        }

        // Use a range to iterate over all keys
        let mut count = 0;

        for _ in map.range(min..max) {
            count += 1;
        }

        assert_eq!(count, 200);

        for entity in entities {
            // Use entity-scoped range to iterate all keys for each entity
            let entity_min = <EavKey as IndexKey>::min().entity(&entity);
            let entity_max = <EavKey as IndexKey>::max().entity(&entity);

            let mut count = 0;

            for (key, _) in map.range(entity_min..entity_max) {
                count += 1;
                assert_eq!(key.get_entity_part(), &*entity);
            }

            assert_eq!(count, 2);
        }
    }
}
