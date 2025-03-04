use super::{Attribute, Entity, REFERENCE_MAX, REFERENCE_MIN, Reference};

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
    fn create(entity: Reference, attribute: (Reference, Reference), value: Reference) -> Self;

    /// Get the entity part of the key
    fn get_entity_part(&self) -> &Reference;

    /// Get the attribute part of the key
    fn get_attribute_part(&self) -> &(Reference, Reference);

    /// Get the value part of the key
    fn get_value_part(&self) -> &Reference;

    /// Construct a key of this type at the lowest bound (all bits are zeroes)
    fn min() -> Self {
        Self::create(REFERENCE_MIN, (REFERENCE_MIN, REFERENCE_MIN), REFERENCE_MIN)
    }

    /// Construct a key of this type at the highest bound (all bits are ones)
    fn max() -> Self {
        Self::create(REFERENCE_MAX, (REFERENCE_MAX, REFERENCE_MAX), REFERENCE_MAX)
    }

    /// Create a new key based on this one that is scoped to the specified [Entity]
    fn entity(&self, entity: &Entity) -> Self {
        Self::create(**entity, *self.get_attribute_part(), *self.get_value_part())
    }

    /// Create a new key based on this one that is scoped to the specified [Attribute]
    fn attribute(&self, namespace: &str, predicate: Option<&str>) -> Self {
        let mut attribute: (Reference, Reference) = Attribute {
            namespace: namespace.to_string(),
            predicate: predicate.unwrap_or_default().to_owned(),
        }
        .into();

        if predicate.is_none() {
            attribute.1 = self.get_attribute_part().1;
        };

        Self::create(*self.get_entity_part(), attribute, *self.get_value_part())
    }

    /// Create a new key based on this one that is scoped to the specified [Value]
    fn value<V>(&self, value: V) -> Self
    where
        V: AsRef<[u8]>,
    {
        Self::create(
            *self.get_entity_part(),
            *self.get_attribute_part(),
            blake3::hash(value.as_ref()).as_bytes().to_owned(),
        )
    }
}

pub type PrimaryKey = EavKey;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct EavKey {
    pub entity: Reference,
    pub attribute: (Reference, Reference),
    pub value: Reference,
}

impl<A, V> From<(Entity, A, V)> for EavKey
where
    Attribute: From<A>,
    V: AsRef<[u8]>,
{
    fn from((entity, attribute, value): (Entity, A, V)) -> Self {
        EavKey {
            entity: *entity,
            attribute: Attribute::from(attribute).into(),
            value: blake3::hash(value.as_ref()).as_bytes().to_owned(),
        }
    }
}

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

impl IndexKey for EavKey {
    fn create(entity: Reference, attribute: (Reference, Reference), value: Reference) -> Self {
        Self {
            entity,
            attribute,
            value,
        }
    }

    fn get_entity_part(&self) -> &Reference {
        &self.entity
    }

    fn get_attribute_part(&self) -> &(Reference, Reference) {
        &self.attribute
    }

    fn get_value_part(&self) -> &Reference {
        &self.value
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct AevKey {
    pub attribute: (Reference, Reference),
    pub entity: Reference,
    pub value: Reference,
}

impl From<EavKey> for AevKey {
    fn from(value: EavKey) -> Self {
        Self {
            entity: value.entity,
            attribute: value.attribute,
            value: value.value,
        }
    }
}

impl<A, V> From<(Entity, A, V)> for AevKey
where
    Attribute: From<A>,
    V: AsRef<[u8]>,
{
    fn from((entity, attribute, value): (Entity, A, V)) -> Self {
        AevKey {
            entity: *entity,
            attribute: Attribute::from(attribute).into(),
            value: blake3::hash(value.as_ref()).as_bytes().to_owned(),
        }
    }
}

impl IndexKey for AevKey {
    fn create(entity: Reference, attribute: (Reference, Reference), value: Reference) -> Self {
        Self {
            entity,
            attribute,
            value,
        }
    }

    fn get_entity_part(&self) -> &Reference {
        &self.entity
    }

    fn get_attribute_part(&self) -> &(Reference, Reference) {
        &self.attribute
    }

    fn get_value_part(&self) -> &Reference {
        &self.value
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct VaeKey {
    pub value: Reference,
    pub attribute: (Reference, Reference),
    pub entity: Reference,
}

impl From<EavKey> for VaeKey {
    fn from(value: EavKey) -> Self {
        Self {
            entity: value.entity,
            attribute: value.attribute,
            value: value.value,
        }
    }
}

impl<A, V> From<(Entity, A, V)> for VaeKey
where
    Attribute: From<A>,
    V: AsRef<[u8]>,
{
    fn from((entity, attribute, value): (Entity, A, V)) -> Self {
        VaeKey {
            entity: *entity,
            attribute: Attribute::from(attribute).into(),
            value: blake3::hash(value.as_ref()).as_bytes().to_owned(),
        }
    }
}

impl IndexKey for VaeKey {
    fn create(entity: Reference, attribute: (Reference, Reference), value: Reference) -> Self {
        Self {
            entity,
            attribute,
            value,
        }
    }

    fn get_entity_part(&self) -> &Reference {
        &self.entity
    }

    fn get_attribute_part(&self) -> &(Reference, Reference) {
        &self.attribute
    }

    fn get_value_part(&self) -> &Reference {
        &self.value
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
                assert_eq!(key.get_entity_part(), entity.as_ref());
            }

            assert_eq!(count, 2);
        }
    }
}
