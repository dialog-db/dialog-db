pub use crate::concept::Concept;
pub use crate::predicate::formula::Formula;

/// Represents predicates that can be applied using an associated application
/// type. This is used by predicate definitions like `Concept` and `Formula`
/// to associate a type for describing application parameters. This works in
/// conjunction with the `Query` type alias that provides a universal way to
/// construct type-safe formula / concept applications as shown below.
///
/// ```rs
/// let person = Query::<Person> {
///     name: "John".to_string(),
///     address: Term::var("address"),
/// }
/// ```
pub trait Predicate {
    /// The application type associated with this predicate
    type Application;
}

/// Type alias to construct type-safe formula / concept applications.
///
/// ```rs
/// #[derive(Debug, Clone, Concept)]
/// pub struct Person {
///     this: Entity,
///     name: String,
///     address: Term,
/// }
///
/// let query = Query::<Person> {
///     name: "John".to_string(),
///     address: Term::var("address"),
/// }
/// ```
#[allow(type_alias_bounds)]
pub type Query<T: Predicate> = T::Application;

/// Convenience alias kept for backward compatibility during migration.
#[allow(type_alias_bounds)]
pub type Match<T: Predicate> = T::Application;

/// Type that can be used to reference input cells of the formula as shown below.
///
/// ```rs
/// #[derive(Debug, Clone, Formula)]
/// pub struct Echo {
///     input: String,
///     #[derived]
///     output: String,
/// }
///
/// impl Echo for Echo {
///     fn derive(cells: Input<Self>) -> Self {
///         Self {
///             output: format!("{}, {}", &cells.input, &cells.input),
///             input: cells.input,
///         }
///     }
/// }
/// ```
#[allow(type_alias_bounds)]
pub type Input<T: Formula> = T::Input;
