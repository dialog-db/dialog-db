/// Identifies which component of a fact contributed to a
/// [`Factor::Selected`](super::Factor::Selected) binding.
///
/// A stored fact is an `(attribute, entity, value, cause)` tuple.
/// When a relation premise matches a fact, each of these four components
/// may bind a different variable. The `Selector` records *which* component
/// was the source for a given binding.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Selector {
    /// The attribute component of a fact.
    The,
    /// The entity component of a fact.
    Of,
    /// The value component of a fact.
    Is,
    /// The cause (provenance hash) component of a fact.
    Cause,
}
