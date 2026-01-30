#![allow(private_bounds)]

//! Domain module for the [`ArtifactSelector`]

use std::marker::PhantomData;

use crate::{Attribute, Entity, Value};

#[cfg(doc)]
use crate::ArtifactStore;

use super::Blake3Hash;

/// A marker type that represents a totally open-ended [`ArtifactSelector`]
#[derive(Clone)]
pub struct Unconstrained;
impl ArtifactSelectorState for Unconstrained {}

/// A marker type that represents an [`ArtifactSelector`] that is constrained
/// by at least the attribute, entity or value part of a triple.
#[derive(Debug, Clone)]
pub struct Constrained;
impl ArtifactSelectorState for Constrained {}

trait ArtifactSelectorState {}

/// The basic query system for selecting [`Artifact`]s from a [`ArtifactStore`]
/// You can assign its fields directly, but for convenience and ergonomics it is
/// also possible to construct it incrementally with the `the`, `of` and `is`
/// methods.
///
/// When a field is specified, all [`Artifact`]s that are selected will share
/// the same field value.
///
/// Note that when all fields of the [`ArtifactSelector`] are `None`, it implies
/// that all [`Artifact`]s in the [`ArtifactStore`] should be selected (this can
/// be very slow and is often not what you want). To avoid this, always be sure
/// to specify at least one field of the [`ArtifactSelector`] before submitting
/// a query!
#[derive(Debug, Clone)]
pub struct ArtifactSelector<State>
where
    State: ArtifactSelectorState,
{
    entity: Option<Entity>,
    attribute: Option<Attribute>,
    value: Option<Value>,

    value_reference: Option<Blake3Hash>,
    state_type: PhantomData<State>,
}

impl Default for ArtifactSelector<Unconstrained> {
    fn default() -> Self {
        Self::new()
    }
}

impl ArtifactSelector<Unconstrained> {
    /// Construct a new, unconstrained [`ArtifactSelector`]. It will need to be
    /// constrained (by configuring at least an entity, attribute or value)
    /// before it can be used.
    pub fn new() -> Self {
        Self {
            entity: None,
            attribute: None,
            value: None,
            value_reference: None,
            state_type: PhantomData,
        }
    }
}

impl<State> ArtifactSelector<State>
where
    State: ArtifactSelectorState,
{
    /// The [`Entity`] (or subject) that selected [`Artifact`]s should refer to
    pub fn entity(&self) -> Option<&Entity> {
        self.entity.as_ref()
    }

    /// The [`Attribute`] (or predicate) used in any selected [`Artifact`]s
    pub fn attribute(&self) -> Option<&Attribute> {
        self.attribute.as_ref()
    }

    /// The [`Value`] (or object) that selected [`Artifact`]s should refer to.
    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// The [`Blake3Hash`] of the configured [`Value`], if any
    pub fn value_reference(&self) -> Option<&Blake3Hash> {
        self.value_reference.as_ref()
    }

    /// Set the [`Attribute`] field (the predicate) of the [`ArtifactSelector`]
    pub fn the(self, attribute: Attribute) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: Some(attribute),
            entity: self.entity,
            value_reference: self.value_reference,
            value: self.value,
            state_type: PhantomData,
        }
    }

    /// Set the [`Entity`] field (the subject) of the [`ArtifactSelector`]
    pub fn of(self, entity: Entity) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: Some(entity),
            value_reference: self.value_reference,
            value: self.value,
            state_type: PhantomData,
        }
    }

    /// Set the [`Value`] field (the object) of the [`ArtifactSelector`]
    pub fn is(self, value: Value) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: self.entity,
            value_reference: Some(value.to_reference()),
            value: Some(value),
            state_type: PhantomData,
        }
    }
}
