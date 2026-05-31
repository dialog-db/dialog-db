/// Base EAV scan query (Cardinality::Many).
pub mod all;
/// Type-erased attribute query dispatching between cardinality variants.
pub mod dynamic;
/// Winner-selecting attribute query (Cardinality::One).
pub mod only;
/// Typed attribute query wrapping a single `Attribute` type.
pub mod typed;

use serde::{Deserialize, Serialize};

pub use dynamic::DynamicAttributeQuery;
pub use dynamic::DynamicAttributeQuery as AttributeQuery;
pub use typed::StaticAttributeQuery;

/// Resolution policy for an attribute query.
///
/// Distinguishes the two row-multiplicity semantics independent of
/// [`Cardinality`](crate::Cardinality), which controls how many
/// rows a fact lookup *can* produce. `Resolution` controls what
/// happens when a fact lookup *finds nothing*:
///
/// - [`Resolution::Required`] — the lookup yields zero rows on
///   miss. Standard EAV semantics: a query that demands a fact
///   filters out input rows that lack one. This is the default.
/// - [`Resolution::Optional`] — the lookup yields one fallback
///   row on miss, with the `is` slot (and the named `cause` slot,
///   if any) bound to [`Binding::Absent`](crate::Binding::Absent).
///   Used by concept projection for `maybe` fields and by any
///   premise that wants set-widening optionality at the row
///   layer.
///
/// Schema impact: `Resolution::Optional` widens the `is` slot's
/// `content_type` with the `Nothing` atom via
/// [`Type::optional`](crate::type_system::Type::optional), signaling
/// to the planner and unifier that this slot may bind to Absent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Resolution {
    /// Standard EAV: zero rows when no fact matches the lookup.
    #[default]
    Required,
    /// Yield one fallback row when no fact matches; the `is`
    /// (and named `cause`) slot bind to
    /// [`Binding::Absent`](crate::Binding::Absent).
    Optional,
}
