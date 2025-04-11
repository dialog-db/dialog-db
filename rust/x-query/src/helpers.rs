#[cfg(not(test))]
mod inner {}

#[cfg(test)]
mod inner {
    use std::str::FromStr;

    use crate::{Attribute, Entity, MemoryStore, PrimaryKey, TripleStoreMut, Value};
    use anyhow::Result;

    pub async fn make_store() -> Result<(MemoryStore, Vec<(PrimaryKey, Entity, Attribute, Value)>)>
    {
        let mut store = MemoryStore::default();

        let item_id_attribute = Attribute::from_str("item/id")?;
        let item_name_attribute = Attribute::from_str("item/name")?;
        let back_reference_attribute = Attribute::from_str("back/reference")?;
        let parent_attribute = Attribute::from_str("relationship/parentOf")?;

        let mut data = vec![];

        let mut last_entity = None;

        for i in 0..8u128 {
            let entity = Entity::new();

            data.push((
                entity.clone(),
                item_id_attribute.clone(),
                Value::UnsignedInt(i),
            ));

            data.push((
                entity.clone(),
                item_name_attribute.clone(),
                Value::String(format!("name{i}")),
            ));

            if let Some(parent_entity) = last_entity {
                data.push((
                    entity.clone(),
                    parent_attribute.clone(),
                    Value::Entity(parent_entity),
                ))
            }

            data.push((
                Entity::new(),
                back_reference_attribute.clone(),
                Value::Entity(entity.clone()),
            ));

            last_entity = Some(entity);
        }

        let mut entries: Vec<(crate::EavKey, Entity, Attribute, Value)> = vec![];
        for (entity, attribute, value) in data {
            let value = value;
            let key = store
                .assert(entity.clone(), attribute.clone(), value.clone())
                .await?;
            entries.push((key, entity, attribute, value));
        }

        Ok((store, entries))
    }
}

#[allow(unused)]
pub use inner::*;
