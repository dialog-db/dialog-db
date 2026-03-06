use super::Formula;

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
