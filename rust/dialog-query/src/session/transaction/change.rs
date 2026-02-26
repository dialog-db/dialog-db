use crate::artifact::{Attribute, Entity, Value};
use std::collections::HashMap;

/// Changes organized by entity -> attribute -> operations.
/// Each `(entity, attribute)` pair may have multiple changes — for example
/// asserting several values on a `Cardinality::Many` attribute in one
/// transaction.
pub type Changes = HashMap<Entity, HashMap<Attribute, Vec<Change>>>;

/// A single write operation on an `(entity, attribute)` pair inside a
/// [`Transaction`](super::Transaction).
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// Assert a value for an entity-attribute pair
    Assert(Value),
    /// Retract a value from an entity-attribute pair
    Retract(Value),
}
