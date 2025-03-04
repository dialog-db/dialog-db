use crate::{Attribute, Entity, Value};

#[derive(Debug, Clone)]
pub enum Literal {
    Entity(Entity),
    Attribute(Attribute),
    Value(Value),
}
