pub use crate::concept::Concept;
pub use crate::predicate::formula::Formula;

/// Represents predicates that can be applied using an associated `Query`
/// type. This is used by predicate definitions like `Concept` and `Formula`
/// to associate type for describing application parameters. This works in
/// conjunction with `Match` type that we have universal way to construct type
/// safe formula / concept applications as shown below.
///
/// ```rs
/// let person = Match::<Person> {
///     name: "John".to_string(),
///     address: Term::var("address"),
/// }
/// ```
pub trait Quarriable {
    /// The query/match pattern type associated with this predicate
    type Query;
}

/// Type that can be used to construct type safe formula / concept applications
/// as shown below.
///
/// ```rs
/// #[derive(Debug, Clone, Concept)]
/// pub struct Person {
///     this: Entity,
///     name: String,
///     address: Term,
/// }
///
/// let query = Match::<Person> {
///     name: "John".to_string(),
///     address: Term::var("address"),
/// }
/// ```
#[allow(type_alias_bounds)]
pub type Match<T: Quarriable> = T::Query;

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
