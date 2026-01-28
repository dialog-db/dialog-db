//! Constraint trait for deriving capability chain types.

use super::{Ability, Constrained, Policy, Subject};

/// Trait for deriving capability constrain chain type from an individual
/// constraints of the chain.
pub trait Constraint {
    /// The full capability chain type.
    type Capability: Ability;
}

/// For the Subject capabilty is the Subject itself.
impl Constraint for Subject {
    type Capability = Subject;
}

/// For any `Policy` or `Subject`, `Constraint::Capability` gives the full
/// `Constrained<...>` chain type, which implements the `Ability` trait.
impl<T: Policy> Constraint for T {
    type Capability = Constrained<T, <T::Of as Constraint>::Capability>;
}
