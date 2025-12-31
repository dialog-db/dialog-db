//! The `#[provider(Capability)]` macro generates a `Provider` implementation for an impl block.
//!
//! # Example
//!
//! ```ignore
//! use dialog_macros::provider;
//!
//! #[provider(blob_store::Capability)]
//! impl BlobStore for MyBackend {
//!     async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
//!         self.data.get(&key).cloned()
//!     }
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
//!         self.data.insert(key, value);
//!     }
//! }
//! ```
//!
//! This generates:
//! ```ignore
//! impl BlobStore for MyBackend { ... }  // the original impl
//!
//! impl dialog_common::fx::Provider for MyBackend {
//!     type Capability = blob_store::Capability;
//!
//!     async fn provide(&mut self, capability: blob_store::Capability) -> <blob_store::Capability as dialog_common::fx::Capability>::Output {
//!         capability.perform(self).await
//!     }
//! }
//! ```
//!
//! # Complex Where Clauses
//!
//! The macro preserves generic bounds from the impl block:
//!
//! ```ignore
//! #[provider(env::Capability)]
//! impl<LS, LM, SC, MC> Env for Environment<Site<LS, LM, SC, MC>>
//! where
//!     LS: StorageBackend + Clone,
//!     LM: MemoryBackend + Clone,
//!     SC: Connection<LS>,
//!     MC: Connection<LM>,
//! {
//! }
//! ```
//!
//! This generates both the original `impl Env` and an `impl Provider` with
//! the same generics and where clause.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, ItemImpl, Path,
};

/// Arguments to the provider macro: the Capability type path
struct ProviderArgs {
    /// The Capability type path (e.g., `env::Capability` or `blob_store::Capability`)
    capability_path: Path,
}

impl Parse for ProviderArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let capability_path = input.parse()?;
        Ok(ProviderArgs { capability_path })
    }
}

/// Generates a `Provider` implementation from an existing trait impl block.
///
/// # Arguments
///
/// * `Capability` - The path to the Capability type (e.g., `blob_store::Capability`)
///
/// # Example
///
/// ```ignore
/// #[provider(env::Capability)]
/// impl Env for MyBackend { ... }
/// ```
pub fn provider_impl(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as ProviderArgs);
    let input = parse_macro_input!(item as ItemImpl);

    let result = generate_provider_from_impl_block(&args, &input);

    match result {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate_provider_from_impl_block(
    args: &ProviderArgs,
    input: &ItemImpl,
) -> syn::Result<TokenStream2> {
    let capability_path = &args.capability_path;

    // Extract impl components
    let generics = &input.generics;
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let self_ty = &input.self_ty;

    Ok(quote! {
        #input

        impl #impl_generics dialog_common::fx::Provider for #self_ty #where_clause {
            type Capability = #capability_path;

            async fn provide(&mut self, capability: #capability_path) -> <#capability_path as dialog_common::fx::Capability>::Output {
                capability.perform(self).await
            }
        }
    })
}
