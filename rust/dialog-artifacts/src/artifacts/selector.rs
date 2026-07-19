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

    /// Prefix bound on the entity URI: selected [`Artifact`]s'
    /// entities must have URIs beginning with this string. The
    /// entity key stores the first 32 URI bytes raw (the rest is
    /// hashed), so scans range over the raw head and re-check
    /// longer prefixes against the stored datum.
    entity_prefix: Option<String>,
    /// Prefix bound on the attribute name: selected [`Artifact`]s'
    /// attributes must have names beginning with this string. The
    /// attribute key stores the full (64-byte-capped) name raw, so
    /// this bound is an exact key range.
    attribute_prefix: Option<String>,
    /// Prefix bound on the value: selected [`Artifact`]s' values must
    /// be textual (string/bytes) and begin with this string. The M3
    /// value-in-key format stores the value order-preservingly in the
    /// VAE index, so this is an exact key range over the value dimension
    /// (subject to per-entry re-checking for spilled values, whose key
    /// carries a reference rather than the inline prefix).
    value_prefix: Option<String>,
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
            entity_prefix: None,
            attribute_prefix: None,
            value_prefix: None,
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

    /// The prefix bound on entity URIs, if any
    pub fn entity_prefix(&self) -> Option<&str> {
        self.entity_prefix.as_deref()
    }

    /// The prefix bound on attribute names, if any
    pub fn attribute_prefix(&self) -> Option<&str> {
        self.attribute_prefix.as_deref()
    }

    /// The prefix bound on values, if any
    pub fn value_prefix(&self) -> Option<&str> {
        self.value_prefix.as_deref()
    }

    /// Set the [`Attribute`] field (the predicate) of the [`ArtifactSelector`]
    pub fn the(self, attribute: Attribute) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: Some(attribute),
            entity: self.entity,
            value_reference: self.value_reference,
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
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
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
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
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
            state_type: PhantomData,
        }
    }

    /// Constrain selected [`Artifact`]s to attributes whose name begins
    /// with `prefix`. A prefix is a constraint, so the resulting
    /// selector is [`Constrained`]; an exact attribute set via
    /// [`ArtifactSelector::the`] takes precedence during scans.
    pub fn the_starting_with(self, prefix: impl Into<String>) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: self.entity,
            value_reference: self.value_reference,
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: Some(prefix.into()),
            value_prefix: self.value_prefix,
            state_type: PhantomData,
        }
    }

    /// Constrain selected [`Artifact`]s to entities whose URI begins
    /// with `prefix`. A prefix is a constraint, so the resulting
    /// selector is [`Constrained`]; an exact entity set via
    /// [`ArtifactSelector::of`] takes precedence during scans.
    pub fn of_starting_with(self, prefix: impl Into<String>) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: self.entity,
            value_reference: self.value_reference,
            value: self.value,
            entity_prefix: Some(prefix.into()),
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
            state_type: PhantomData,
        }
    }

    /// Constrain selected [`Artifact`]s to textual values (string/bytes)
    /// beginning with `prefix`. A prefix is a constraint, so the resulting
    /// selector is [`Constrained`]; an exact value set via
    /// [`ArtifactSelector::is`] takes precedence during scans.
    ///
    /// The M3 value-in-key format stores the value order-preservingly in the
    /// VAE index, so this narrows the scan to the value sub-range whose keys
    /// begin with `prefix` (a spilled value, whose key carries a reference
    /// rather than the inline bytes, is re-checked per entry).
    pub fn is_starting_with(self, prefix: impl Into<String>) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: self.entity,
            value_reference: self.value_reference,
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: Some(prefix.into()),
            state_type: PhantomData,
        }
    }
}
