//! Selector patterns for matching against the database

use crate::artifact::{Attribute, Entity};
use crate::term::Term;
use crate::types::IntoType;
use serde::{Deserialize, Serialize};

/// Selector pattern for matching against the database (with variables/constants)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(bound = "T: IntoType + Clone + Serialize + for<'a> Deserialize<'a> + 'static")]
pub struct Selector<T>
where
    T: IntoType + Clone + 'static,
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
    T: IntoType + Clone,
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
