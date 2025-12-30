//! Procedural macros for Dialog.

use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemTrait};

mod effect;
mod effectful;

/// Attribute macro that generates an algebraic effects system from a trait.
///
/// See the [`effect`] module documentation for details.
#[proc_macro_attribute]
pub fn effect(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let trait_def = parse_macro_input!(item as ItemTrait);
    effect::effect_impl(trait_def)
}

/// Attribute macro that transforms a function with `yield` expressions into
/// an effect-based computation returning a `Task`.
///
/// # Example
///
/// ```ignore
/// use dialog_macros::effectful;
///
/// #[effectful(BlockStore)]
/// pub fn copy(from: Vec<u8>, to: Vec<u8>) -> Result<(), Error> {
///     let content = yield BlockStore::get(from)?;
///     yield BlockStore::set(to, content.unwrap_or_default())
/// }
/// ```
///
/// This transforms `yield expr` into `expr.perform(&__co).await` and wraps
/// the body in a `Task::new(|__co| async move { ... })`.
///
/// See the [`effectful`] module documentation for details.
#[proc_macro_attribute]
pub fn effectful(attr: TokenStream, item: TokenStream) -> TokenStream {
    effectful::effectful_impl(attr, item)
}
