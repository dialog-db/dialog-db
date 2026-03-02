//! Relation query and descriptor types.

/// Relation descriptor for parameter signatures.
pub mod descriptor;
/// Relation application for queries.
pub mod query;

pub use descriptor::RelationDescriptor;
pub use query::RelationQuery;
