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

/// Attribute macro that generates a `Provider` implementation from an impl block.
///
/// # Example
///
/// ```ignore
/// use dialog_macros::provider;
///
/// #[provider(blob_store::Capability)]
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
/// This generates both the original trait impl and a `Provider` impl that
/// dispatches capability requests using `capability.perform(self)`.
///
/// # Complex Where Clauses
///
/// The macro preserves generic bounds from the impl block:
///
/// ```ignore
/// #[provider(env::Capability)]
/// impl<LS, LM, SC, MC> Env for Environment<Site<LS, LM, SC, MC>>
/// where
///     LS: StorageBackend + Clone,
///     LM: MemoryBackend + Clone,
///     SC: Connection<LS>,
///     MC: Connection<LM>,
/// {
/// }
/// ```
///
/// This generates both the original `impl Env` and an `impl Provider` with
/// the same generics and where clause.
#[proc_macro_attribute]
pub fn provider(attr: TokenStream, item: TokenStream) -> TokenStream {
    provider::provider_impl(attr, item)
}
