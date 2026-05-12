#![allow(private_bounds)]

//! Selector for querying artifacts.

use std::marker::PhantomData;

use crate::{DialogArtifactsError, Entity, Symbol, Value};

#[cfg(doc)]
use crate::ArtifactStore;

use super::Blake3Hash;

/// A marker type that represents a totally open-ended [`ArtifactSelector`]
#[derive(Clone)]
pub struct Unconstrained;
impl ArtifactSelectorState for Unconstrained {}

/// A marker type that represents an [`ArtifactSelector`] that is constrained
/// by at least one slot of a triple (domain, name, entity, or value).
#[derive(Debug, Clone)]
pub struct Constrained;
impl ArtifactSelectorState for Constrained {}

trait ArtifactSelectorState {}

/// The basic query system for selecting [`Artifact`]s from a [`ArtifactStore`]
/// You can assign its fields directly, but for convenience and ergonomics it is
/// also possible to construct it incrementally with the [`within`](Self::within),
/// [`named`](Self::named), [`of`](Self::of), and [`is`](Self::is) methods.
///
/// The attribute slot of an artifact is structurally two [`Symbol`]s: a
/// domain half and a name half. Constraining only [`domain`](Self::domain)
/// (via [`within`](Self::within)) selects every artifact whose domain
/// matches, enabling a contiguous prefix scan of the attribute index.
/// Constraining both yields a fully-bound attribute. Composite-attribute
/// handling lives at the layer above this one.
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
    domain: Option<Symbol>,
    name: Option<Symbol>,
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
    /// constrained (by configuring at least a domain, name, entity or value)
    /// before it can be used.
    pub fn new() -> Self {
        Self {
            entity: None,
            domain: None,
            name: None,
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

    /// The domain half of the attribute used in any selected
    /// [`Artifact`]s. When set without [`name`](Self::name) it implies a
    /// prefix scan over the attribute index.
    pub fn domain(&self) -> Option<&Symbol> {
        self.domain.as_ref()
    }

    /// The name half of the attribute used in any selected [`Artifact`]s.
    pub fn name(&self) -> Option<&Symbol> {
        self.name.as_ref()
    }

    /// The [`Value`] (or object) that selected [`Artifact`]s should refer to.
    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// The [`Blake3Hash`] of the configured [`Value`], if any
    pub fn value_reference(&self) -> Option<&Blake3Hash> {
        self.value_reference.as_ref()
    }

    /// Constrain the selector to artifacts whose attribute domain is the
    /// given [`Symbol`].
    ///
    /// Used alone, this enables a contiguous prefix scan over the attribute
    /// index. Combined with [`named`](Self::named) it pins the attribute
    /// fully.
    pub fn within(self, domain: Symbol) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            domain: Some(domain),
            name: self.name,
            entity: self.entity,
            value_reference: self.value_reference,
            value: self.value,
            state_type: PhantomData,
        }
    }

    /// Constrain the selector to artifacts whose attribute name matches.
    ///
    /// Note: a name alone does not constrain a contiguous range of the
    /// index, so this method preserves the current state (it does **not**
    /// mark the selector as `Constrained`). To constrain on name, pair it
    /// with another field: a domain (via [`within`](Self::within)), an
    /// entity, or a value.
    pub fn named(self, name: Symbol) -> ArtifactSelector<State> {
        ArtifactSelector {
            domain: self.domain,
            name: Some(name),
            entity: self.entity,
            value_reference: self.value_reference,
            value: self.value,
            state_type: PhantomData,
        }
    }

    /// String-accepting form of [`within`](Self::within): parse `domain`
    /// into a [`Symbol`] and constrain the selector to that domain.
    pub fn with_domain(
        self,
        domain: impl AsRef<str>,
    ) -> Result<ArtifactSelector<Constrained>, DialogArtifactsError> {
        Ok(self.within(domain.as_ref().parse()?))
    }

    /// String-accepting form of [`named`](Self::named): parse `name` into
    /// a [`Symbol`] and constrain the selector to that name.
    pub fn with_name(
        self,
        name: impl AsRef<str>,
    ) -> Result<ArtifactSelector<State>, DialogArtifactsError> {
        Ok(self.named(name.as_ref().parse()?))
    }

    /// Set the [`Entity`] field (the subject) of the [`ArtifactSelector`]
    pub fn of(self, entity: Entity) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            domain: self.domain,
            name: self.name,
            entity: Some(entity),
            value_reference: self.value_reference,
            value: self.value,
            state_type: PhantomData,
        }
    }

    /// Set the [`Value`] field (the object) of the [`ArtifactSelector`]
    pub fn is(self, value: Value) -> ArtifactSelector<Constrained> {
        ArtifactSelector::<Constrained> {
            domain: self.domain,
            name: self.name,
            entity: self.entity,
            value_reference: Some(value.to_reference()),
            value: Some(value),
            state_type: PhantomData,
        }
    }
}
