use std::str::FromStr;

use crate::{Artifact, Attribute, Entity, Value};
use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Generate deterministic test data consisting of facts that reference a
/// specified number of [`Entity`]s.
pub fn generate_data(entity_count: usize) -> Result<Vec<Artifact>> {
    let item_id_attribute = Attribute::from_str("item/id")?;
    let item_name_attribute = Attribute::from_str("item/name")?;
    let back_reference_attribute = Attribute::from_str("back/reference")?;
    let parent_attribute = Attribute::from_str("relationship/parentOf")?;

    let mut rng = ChaCha8Rng::from_seed([0u8; 32]);
    let mut data = vec![];
    let mut make_entity = || Entity::from(rng.random::<[u8; 32]>());
    let mut last_entity: Option<Entity> = None;

    for i in 0..entity_count {
        let entity = make_entity();

        data.push(Artifact {
            the: item_id_attribute.clone(),
            of: entity.clone(),
            is: Value::UnsignedInt(i as u128),
            cause: None,
        });

        data.push(Artifact {
            the: item_name_attribute.clone(),
            of: entity.clone(),
            is: Value::String(format!("name{i}")),
            cause: None,
        });

        if let Some(parent_entity) = last_entity {
            data.push(Artifact {
                the: parent_attribute.clone(),
                of: entity.clone(),
                is: Value::Entity(*parent_entity),
                cause: None,
            });
        }

        data.push(Artifact {
            the: back_reference_attribute.clone(),
            of: make_entity(),
            is: Value::Entity(*entity),
            cause: None,
        });

        last_entity = Some(entity);
    }

    Ok(data)
}
