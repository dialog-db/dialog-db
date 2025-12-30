//! The `#[provider(EffectModule)]` macro generates a `Provider` implementation for a struct.
//!
//! # Example
//!
//! ```ignore
//! use dialog_macros::provider;
//!
//! #[provider(BlockStore)]
//! struct MyBackend {
//!     data: HashMap<Vec<u8>, Vec<u8>>,
//! }
//!
//! impl BlockStore::BlockStore for MyBackend {
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
//! impl dialog_common::fx::Provider for MyBackend {
//!     type Capability = BlockStore::Capability;
//!
//!     async fn provide(&mut self, capability: BlockStore::Capability) -> BlockStore::Output {
//!         BlockStore::dispatch(self, capability).await
//!     }
//! }
//! ```
//!
//! # Multiple Effect Modules
//!
//! You can derive from multiple effect modules:
//!
//! ```ignore
//! #[provider(CompositeEnv)]
//! struct MyBackend { ... }
//! ```
//!
//! Where `CompositeEnv` is defined as:
//! ```ignore
//! #[effect]
//! trait CompositeEnv: BlockStore + TransactionalMemory {}
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, DeriveInput, Path, Token,
};

/// Arguments to the provider macro: a single effect module path
struct ProviderArgs {
    effect_module: Path,
}

impl Parse for ProviderArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let effect_module = input.parse()?;
        // Allow optional trailing comma
        let _ = input.parse::<Option<Token![,]>>();
        Ok(ProviderArgs { effect_module })
    }
}

pub fn provider_impl(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as ProviderArgs);
    let input = parse_macro_input!(item as DeriveInput);

    let result = generate_provider_impl(&args, &input);

    match result {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate_provider_impl(args: &ProviderArgs, input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_name = &input.ident;
    let effect_module = &args.effect_module;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Reproduce the original struct definition
    let vis = &input.vis;
    let attrs = &input.attrs;
    let generics = &input.generics;

    let struct_def = match &input.data {
        syn::Data::Struct(data) => {
            let fields = &data.fields;
            match fields {
                syn::Fields::Named(fields) => quote! {
                    #(#attrs)*
                    #vis struct #struct_name #generics #where_clause #fields
                },
                syn::Fields::Unnamed(fields) => quote! {
                    #(#attrs)*
                    #vis struct #struct_name #generics #fields #where_clause;
                },
                syn::Fields::Unit => quote! {
                    #(#attrs)*
                    #vis struct #struct_name #generics #where_clause;
                },
            }
        }
        _ => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "#[provider] can only be applied to structs",
            ))
        }
    };

    Ok(quote! {
        #struct_def

        impl #impl_generics dialog_common::fx::Provider for #struct_name #ty_generics #where_clause {
            type Capability = #effect_module::Capability;

            async fn provide(&mut self, capability: #effect_module::Capability) -> #effect_module::Output {
                #effect_module::dispatch(self, capability).await
            }
        }
    })
}
