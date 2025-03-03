use super::{Attribute, Entity, Value};

#[derive(Debug, Clone)]
pub enum Field {
    Entity(Entity),
    Attribute(Attribute),
    Value(Value),
}
