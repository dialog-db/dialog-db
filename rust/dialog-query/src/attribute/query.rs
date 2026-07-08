/// Base EAV scan query (Cardinality::Many).
pub mod all;
/// Type-erased attribute query dispatching between cardinality variants.
pub mod dynamic;
/// Winner-selecting attribute query (Cardinality::One).
pub mod only;
/// Sibling-resolution strategies for `Cardinality::One` reads.
pub mod resolution;
/// Typed attribute query wrapping a single `Attribute` type.
pub mod typed;

/// WS4 end-to-end integration: a real automerge-typed attribute folded,
/// edited, and converged through the shipped typed path.
#[cfg(test)]
mod integration;

pub use dynamic::DynamicAttributeQuery;
pub use dynamic::DynamicAttributeQuery as AttributeQuery;
pub use resolution::Resolution;
pub use typed::StaticAttributeQuery;
