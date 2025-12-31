//! Procedural macros for Dialog.

use proc_macro::TokenStream;

mod effect;
mod effectful;
mod provider;

/// Attribute macro that generates an algebraic effects system from a trait.
///
/// See the [`effect`] module documentation for details.
#[proc_macro_attribute]
pub fn effect(attr: TokenStream, item: TokenStream) -> TokenStream {
    effect::effect_impl(attr, item)
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

/// Attribute macro that generates a `Provider` implementation for a struct.
///
/// # Example
///
/// ```ignore
/// use dialog_macros::provider;
///
/// #[provider(BlobStore)]
/// struct MyBackend {
///     data: HashMap<Vec<u8>, Vec<u8>>,
/// }
///
/// impl BlobStore for MyBackend {
///     async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
///         self.data.get(&key).cloned()
///     }
///     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
///         self.data.insert(key, value);
///     }
/// }
/// ```
///
/// This generates a `Provider` impl that dispatches capability requests
/// to the trait methods.
#[proc_macro_attribute]
pub fn provider(attr: TokenStream, item: TokenStream) -> TokenStream {
    provider::provider_impl(attr, item)
}
