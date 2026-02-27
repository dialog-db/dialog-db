use std::collections::HashSet;

use crate::Relation;
use crate::artifact::Value;

use super::Factor;

/// The set of [`Factor`]s that support a single variable binding.
///
/// A variable may be bound by multiple independent sources — for example,
/// two different relation premises might both select the same variable. The
/// first factor added becomes the *primary* (used to retrieve the bound
/// value via [`content`](Factors::content)); subsequent factors are stored
/// as alternates and contribute to the provenance record without changing
/// the bound value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Factors {
    primary: Factor,
    alternates: HashSet<Factor>,
}

impl Factors {
    /// Create a new Factors with just a primary factor
    pub fn new(primary: Factor) -> Self {
        Self {
            primary,
            alternates: HashSet::new(),
        }
    }

    /// Get the value from the factors
    pub fn content(&self) -> Value {
        self.primary.content()
    }

    /// Add a factor to this binding.
    /// Returns true if a new factor was added, false if it was already present.
    pub fn add(&mut self, factor: Factor) -> bool {
        if self.primary == factor {
            false
        } else {
            self.alternates.insert(factor)
        }
    }

    /// Iterate over all factors (primary and alternates) that support this binding.
    /// This provides evidence for where this value came from.
    pub fn evidence(&self) -> impl Iterator<Item = &Factor> + '_ {
        std::iter::once(&self.primary).chain(self.alternates.iter())
    }
}

impl From<&Factors> for Value {
    fn from(factors: &Factors) -> Self {
        factors.content()
    }
}

impl From<&Factors> for Relation {
    /// Extract the relation from factors.
    /// Uses the first factor's source relation (primary or first alternate).
    fn from(factors: &Factors) -> Self {
        if let Some(factor) = factors.evidence().next() {
            if let Some(relation) = factor.fact() {
                relation.clone()
            } else {
                panic!("Cannot convert Derived factor to Relation")
            }
        } else {
            panic!("Cannot convert empty Factors to Relation")
        }
    }
}
