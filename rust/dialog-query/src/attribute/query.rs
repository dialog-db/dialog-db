/// Base EAV scan query (Cardinality::Many).
pub mod all;
/// Type-erased attribute query dispatching between cardinality variants.
pub mod dynamic;
/// Winner-selecting attribute query (Cardinality::One).
pub mod only;
/// Typed attribute query wrapping a single `Attribute` type.
pub mod typed;

pub use dynamic::DynamicAttributeQuery;
pub use dynamic::DynamicAttributeQuery as AttributeQuery;
pub use typed::StaticAttributeQuery;
