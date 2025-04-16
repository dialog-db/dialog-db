use crate::{Attribute, Entity, Value};

#[cfg(doc)]
use crate::FactStore;

/// The basic query system for selecting [`Artifact`]s from a [`FactStore`] You can
/// assign its fields directly, but for convenience and ergonomics it is also
/// possible to construct it incrementally with the `the`, `of` and `is`
/// methods.
///
/// When a field is specified, all [`Artifact`]s that are selected will share the
/// same field value.
///
/// Note that when all fields of the [`FactSelector`] are `None`, it implies
/// that all [`Artifact`]s in the [`FactStore`] should be selected (this can be very
/// slow and is often not what you want). To avoid this, always be sure to
/// specify at least one field of the [`FactSelector`] before submitting a
/// query!
#[derive(Default, Clone)]
pub struct FactSelector {
    /// The [`Entity`] (or subject) that selected [`Artifact`]s should refer to
    pub entity: Option<Entity>,
    /// The [`Attribute`] (or predicate) used in any selected [`Artifact`]s
    pub attribute: Option<Attribute>,
    /// The [`Value`] (or object) that selected [`Artifact`]s should refer to.
    pub value: Option<Value>,
}

impl FactSelector {
    /// Set the [`Attribute`] field (the predicate) of the [`FactSelector`]
    pub fn the(mut self, attribute: Attribute) -> Self {
        self.attribute = Some(attribute);
        self
    }

    /// Set the [`Entity`] field (the subject) of the [`FactSelector`]
    pub fn of(mut self, entity: Entity) -> Self {
        self.entity = Some(entity);
        self
    }

    /// Set the [`Value`] field (the object) of the [`FactSelector`]
    pub fn is(mut self, value: Value) -> Self {
        self.value = Some(value);
        self
    }
}
