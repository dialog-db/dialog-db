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

        let item_id_attribute = Attribute::from_str("item:id")?;
        let item_name_attribute = Attribute::from_str("item:name")?;
        let back_reference_attribute = Attribute::from_str("back:reference")?;

        let mut data = vec![];

        for i in 0..8usize {
            let entity = Entity::new();

            data.push((
                entity.clone(),
                item_id_attribute.clone(),
                i.to_le_bytes().to_vec(),
            ));

            data.push((
                entity.clone(),
                item_name_attribute.clone(),
                format!("name{i}").as_bytes().to_vec(),
            ));

            data.push((
                Entity::new(),
                back_reference_attribute.clone(),
                entity.as_ref().to_vec(),
            ));
        }

        let mut entries = vec![];
        for (entity, attribute, value) in data {
            let key = store
                .assert(entity.clone(), attribute.clone(), &value)
                .await?;
            entries.push((key, entity, attribute, value));
        }

        Ok((store, entries))
    }
}

pub use inner::*;
