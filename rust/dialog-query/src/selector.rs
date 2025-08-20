//! Selector patterns for matching against the database

use crate::term::Term;
use crate::types::IntoValueDataType;
use dialog_artifacts::{Attribute, Entity};
use serde::{Deserialize, Serialize};

/// Selector pattern for matching against the database (with variables/constants)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Selector<T>
where
    T: IntoValueDataType + Clone,
{
    /// The entity (subject)
    pub entity: Term<Entity>,
    /// The attribute (predicate)
    pub attribute: Term<Attribute>,
    /// The value (object)
    pub value: Term<T>,
}

impl<T> Selector<T>
where
    T: IntoValueDataType + Clone,
{
    /// Create a new selector with the given terms
    pub fn new(entity: Term<Entity>, attribute: Term<Attribute>, value: Term<T>) -> Self {
        Self {
            entity,
            attribute,
            value,
        }
    }
}
