use async_stream::try_stream;

use crate::{KeyPart, KeyStream, MatchableTerm, Pattern, TripleStorePull};

pub fn key_stream<T>(store: T, pattern: &Pattern) -> impl KeyStream
where
    T: TripleStorePull + 'static,
{
    let pattern = pattern.clone();

    try_stream! {
        if let MatchableTerm::Constant { key_part: entity @ KeyPart::Entity(_), .. } = pattern.entity() {
            for await item in store.attributes_of_entity(entity.clone()) {
                yield item?;
            }
        } else if let MatchableTerm::Constant { key_part: attribute @ KeyPart::Attribute(_), .. } = pattern.attribute() {
            for await item in store.entities_with_attribute(attribute.clone()) {
                yield item?;
            }
        } else if let MatchableTerm::Constant { key_part: value @ KeyPart::Value(_), .. } = pattern.value() {
            for await item in store.entities_with_value(value.clone()) {
                yield item?;
            }
        } else {
            for await item in store.keys() {
                yield item?;
            }
        }
    }
}
