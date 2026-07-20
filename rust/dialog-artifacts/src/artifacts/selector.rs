#![allow(private_bounds)]

//! Domain module for the [`ArtifactSelector`]

use std::marker::PhantomData;

use crate::{Attribute, Entity, Value};

#[cfg(doc)]
use crate::ArtifactStore;

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

/// A one-sided bound on a [`Value`]: the bounding value and whether the bound
/// itself is included. Used for the value range constraints
/// ([`ArtifactSelector::is_at_least`] and friends).
#[derive(Debug, Clone)]
pub struct ValueBound {
    /// The bounding value.
    pub value: Value,
    /// Whether the bound is inclusive (`>=` / `<=`) rather than exclusive
    /// (`>` / `<`).
    pub inclusive: bool,
}

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

    /// Prefix bound on the entity URI: selected [`Artifact`]s'
    /// entities must have URIs beginning with this string. The
    /// entity key stores the full URI raw, so this bound is an
    /// exact key range.
    entity_prefix: Option<String>,
    /// Prefix bound on the attribute name: selected [`Artifact`]s'
    /// attributes must have names beginning with this string. The
    /// attribute key stores the full (64-byte-capped) name raw, so
    /// this bound is an exact key range.
    attribute_prefix: Option<String>,
    /// Prefix bound on the value: selected [`Artifact`]s' values must
    /// be strings beginning with this string. The M3 value-in-key
    /// format stores the value order-preservingly in the VAE index, so
    /// this is an exact key range over the value dimension. Spilled
    /// values participate through the leading bytes their key carries:
    /// a prefix within that in-key prefix decides from the key alone,
    /// and a longer one loads the value and post-filters. A prefix
    /// containing a NUL byte cannot match past the NUL (the inline
    /// payload escapes `0x00`).
    value_prefix: Option<String>,
    /// Lower bound on the value: selected [`Artifact`]s' values must be
    /// `>=` (or `>`, when not inclusive) this. The value sorts
    /// order-preservingly in the VAE index, so this is a key range bound;
    /// exclusivity is enforced by the per-entry re-check.
    value_lower: Option<ValueBound>,
    /// Upper bound on the value: selected [`Artifact`]s' values must be
    /// `<=` (or `<`, when not inclusive) this.
    value_upper: Option<ValueBound>,
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
            entity_prefix: None,
            attribute_prefix: None,
            value_prefix: None,
            value_lower: None,
            value_upper: None,
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

    /// The lower bound on values, if any
    pub fn value_lower(&self) -> Option<&ValueBound> {
        self.value_lower.as_ref()
    }

    /// The upper bound on values, if any
    pub fn value_upper(&self) -> Option<&ValueBound> {
        self.value_upper.as_ref()
    }

    /// Set the [`Attribute`] field (the predicate) of the [`ArtifactSelector`]
    pub fn the(self, attribute: Attribute) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: Some(attribute),
            entity: self.entity,
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
            value_lower: self.value_lower,
            value_upper: self.value_upper,
            state_type: PhantomData,
        }
    }

    /// Set the [`Entity`] field (the subject) of the [`ArtifactSelector`]
    pub fn of(self, entity: Entity) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: Some(entity),
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
            value_lower: self.value_lower,
            value_upper: self.value_upper,
            state_type: PhantomData,
        }
    }

    /// Set the [`Value`] field (the object) of the [`ArtifactSelector`]
    pub fn is(self, value: Value) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: self.entity,
            value: Some(value),
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
            value_lower: self.value_lower,
            value_upper: self.value_upper,
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
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: Some(prefix.into()),
            value_prefix: self.value_prefix,
            value_lower: self.value_lower,
            value_upper: self.value_upper,
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
            value: self.value,
            entity_prefix: Some(prefix.into()),
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
            value_lower: self.value_lower,
            value_upper: self.value_upper,
            state_type: PhantomData,
        }
    }

    /// Constrain selected [`Artifact`]s to string values beginning with
    /// `prefix`. A prefix is a constraint, so the resulting selector is
    /// [`Constrained`]; an exact value set via [`ArtifactSelector::is`] takes
    /// precedence during scans.
    ///
    /// The M3 value-in-key format stores the value order-preservingly in the
    /// VAE index, so this narrows the scan to the value sub-range whose keys
    /// begin with `prefix`. A spilled value participates through the leading
    /// bytes its key carries: a probe within that in-key prefix decides from
    /// the key alone, and a longer probe loads the value and post-filters.
    pub fn is_starting_with(self, prefix: impl Into<String>) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: self.entity,
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: Some(prefix.into()),
            value_lower: self.value_lower,
            value_upper: self.value_upper,
            state_type: PhantomData,
        }
    }

    /// Constrain selected [`Artifact`]s to values greater than or equal to
    /// `value` (`>= value`). The value sorts order-preservingly in the VAE
    /// index, so this bounds the scan's value sub-range from below.
    pub fn is_at_least(self, value: Value) -> ArtifactSelector<Constrained> {
        self.with_value_lower(ValueBound {
            value,
            inclusive: true,
        })
    }

    /// Constrain selected [`Artifact`]s to values strictly greater than
    /// `value` (`> value`).
    pub fn is_greater_than(self, value: Value) -> ArtifactSelector<Constrained> {
        self.with_value_lower(ValueBound {
            value,
            inclusive: false,
        })
    }

    /// Constrain selected [`Artifact`]s to values less than or equal to
    /// `value` (`<= value`). Bounds the scan's value sub-range from above.
    pub fn is_at_most(self, value: Value) -> ArtifactSelector<Constrained> {
        self.with_value_upper(ValueBound {
            value,
            inclusive: true,
        })
    }

    /// Constrain selected [`Artifact`]s to values strictly less than `value`
    /// (`< value`).
    pub fn is_less_than(self, value: Value) -> ArtifactSelector<Constrained> {
        self.with_value_upper(ValueBound {
            value,
            inclusive: false,
        })
    }

    /// Constrain selected [`Artifact`]s to values in the inclusive range
    /// `[lower, upper]`.
    pub fn is_between(self, lower: Value, upper: Value) -> ArtifactSelector<Constrained> {
        self.is_at_least(lower).is_at_most(upper)
    }

    fn with_value_lower(self, bound: ValueBound) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: self.entity,
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
            value_lower: Some(bound),
            value_upper: self.value_upper,
            state_type: PhantomData,
        }
    }

    fn with_value_upper(self, bound: ValueBound) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            attribute: self.attribute,
            entity: self.entity,
            value: self.value,
            entity_prefix: self.entity_prefix,
            attribute_prefix: self.attribute_prefix,
            value_prefix: self.value_prefix,
            value_lower: self.value_lower,
            value_upper: Some(bound),
            state_type: PhantomData,
        }
    }
}
