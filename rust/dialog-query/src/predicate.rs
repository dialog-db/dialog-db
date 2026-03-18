use crate::Entity;
use crate::query::Application;
use dialog_common::ConditionalSend;

/// Associates a domain type with its query, conclusion, and descriptor types.
///
/// Implemented by `#[derive(Concept)]` and `#[derive(Formula)]` structs,
/// `Predicate` is the trait-level glue that connects:
/// - `Application` — the query pattern struct (fields as [`Term`](crate::Term)s)
///   used to build premises.
/// - `Conclusion` — the concrete result struct whose fields are resolved values.
/// - `Descriptor` — an entity-like identifier for the predicate itself.
///
/// Use the [`Query`] type alias for ergonomic construction:
///
/// ```rs
/// let q = Query::<Person> {
///     this: Term::var("entity"),
///     name: Term::var("name"),
/// };
/// ```
pub trait Predicate {
    /// The materialized conclusion type produced by resolving a query.
    type Conclusion: ConditionalSend + 'static;
    /// The application type associated with this predicate
    type Application: Application<Conclusion = Self::Conclusion>;
    /// The descriptor type that identifies this predicate. Must convert to Entity.
    type Descriptor: Into<Entity>;
}

/// Type alias to construct type-safe formula / concept applications.
///
/// ```rs
/// // Given a Concept or Formula type T, Query<T> resolves to T::Application.
/// // For example, with a Person concept:
/// let query = Query::<Person> {
///     this: Term::var("entity"),
///     name: Term::from("John"),
///     address: Term::var("address"),
/// };
/// ```
#[allow(type_alias_bounds)]
pub type Query<T: Predicate> = T::Application;
