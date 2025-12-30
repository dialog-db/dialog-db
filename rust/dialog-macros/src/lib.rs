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

/// Attribute macro that transforms functions and methods with `perform!`
/// expressions into effect-based computations returning `Task`.
///
/// # On Functions
///
/// ```ignore
/// use dialog_macros::effectful;
///
/// #[effectful(BlockStore)]
/// pub fn copy(from: Vec<u8>, to: Vec<u8>) -> Result<(), Error> {
///     let content = perform!(BlockStore::get(from))?;
///     perform!(BlockStore::set(to, content.unwrap_or_default()))
/// }
/// ```
///
/// # On Methods (inherent or trait impl)
///
/// ```ignore
/// impl Cache {
///     #[effectful(BlockStore)]
///     fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
///         perform!(BlockStore::get(key))
///     }
/// }
/// ```
///
/// # On Trait Definitions
///
/// ```ignore
/// trait Storage {
///     #[effectful(BlockStore)]
///     fn load(&self, key: Vec<u8>) -> Option<Vec<u8>>;
/// }
///
/// impl Storage for MyStorage {
///     #[effectful(BlockStore)]
///     fn load(&self, key: Vec<u8>) -> Option<Vec<u8>> {
///         perform!(BlockStore::get(key))
///     }
/// }
/// ```
///
/// See the [`effectful`] module documentation for details.
#[proc_macro_attribute]
pub fn effectful(attr: TokenStream, item: TokenStream) -> TokenStream {
    effectful::effectful_impl(attr, item)
}
