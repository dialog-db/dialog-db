//! The `#[provider(TraitName)]` macro generates a `Provider` implementation for a struct.
//!
//! # Example
//!
//! ```ignore
//! use dialog_macros::provider;
//!
//! #[provider(BlobStore)]
//! struct MyBackend {
//!     data: HashMap<Vec<u8>, Vec<u8>>,
//! }
//!
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
//! impl dialog_common::fx::Provider for MyBackend {
//!     type Capability = blob_store::Capability;
//!
//!     async fn provide(&mut self, capability: blob_store::Capability) -> blob_store::Output {
//!         blob_store::dispatch(self, capability).await
//!     }
//! }
//! ```
//!
//! # Composite Effects
//!
//! You can use composite effect traits:
//!
//! ```ignore
//! #[provider(CompositeEnv)]
//! struct MyBackend { ... }
//! ```
//!
//! Where `CompositeEnv` is defined as:
//! ```ignore
//! #[effect]
//! trait CompositeEnv: BlobStore + TransactionalMemory {}
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
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
    let trait_path = &args.effect_module;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Derive module name from trait name (e.g., BlobStore -> blob_store)
    let trait_name = &trait_path.segments.last().unwrap().ident;
    let module_name = format_ident!("{}", to_snake_case(&trait_name.to_string()));

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
            type Capability = #module_name::Capability;

            async fn provide(&mut self, capability: #module_name::Capability) -> #module_name::Output {
                #module_name::dispatch(self, capability).await
            }
        }
    })
}

/// Convert PascalCase to snake_case
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();

    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.extend(c.to_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}
