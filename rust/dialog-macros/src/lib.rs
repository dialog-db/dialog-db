//! Procedural macros for Dialog.

use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemTrait};

mod effect;

/// Attribute macro that generates an algebraic effects system from a trait.
///
/// See the [`effect`] module documentation for details.
#[proc_macro_attribute]
pub fn effect(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let trait_def = parse_macro_input!(item as ItemTrait);
    effect::effect_impl(trait_def)
}
