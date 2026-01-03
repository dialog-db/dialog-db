//! The `#[effect]` macro generates an algebraic effects system from a trait.
//!
//! ```ignore
//! use dialog_macros::effect;
//!
//! #[effect]
//! pub trait BlobStore {
//!     async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>>;
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>);
//! }
//! ```
//!
//! This generates:
//! - A module `blob_store` (snake_case) containing:
//!   - `Provider` trait (the original trait renamed, with ConditionalSync added)
//!   - `Get`, `Set` effect structs
//!   - Blanket `Effect` impls for any `P: Provider`
//!   - `Consumer` struct for method-style effect creation
//! - Re-export: `pub use blob_store::Provider as BlobStore`
//! - Const: `pub const BlobStore: blob_store::Consumer` for `BlobStore.get(key)` syntax
//!
//! You can specify a custom module name:
//! ```ignore
//! #[effect(store)]
//! pub trait BlobStore { ... }
//! // Generates module `store` instead of `blob_store`
//! ```
//!
//! For traits with supertraits (composition):
//! ```ignore
//! #[effect]
//! pub trait Env: BlobStore + TransactionalMemory {}
//! ```
//!
//! Usage:
//! ```ignore
//! // Implement the trait directly (not BlobStore::BlobStore)
//! impl BlobStore for MyBackend {
//!     async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> { ... }
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) { ... }
//! }
//!
//! // Create effects using either syntax:
//! blob_store::get(key).perform(&mut provider).await  // module function
//! BlobStore.get(key).perform(&mut provider).await    // const method
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    FnArg, Ident, ItemTrait, Pat, ReturnType, TraitItem, TraitItemFn, Type, TypeParamBound,
};

/// Optional argument to the effect macro: a custom module name
struct EffectArgs {
    module_name: Option<Ident>,
}

impl Parse for EffectArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            Ok(EffectArgs { module_name: None })
        } else {
            let name: Ident = input.parse()?;
            Ok(EffectArgs {
                module_name: Some(name),
            })
        }
    }
}

/// Attribute macro that generates an algebraic effects system from a trait.
///
/// See the [module-level documentation](self) for details.
pub fn effect_impl(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = syn::parse_macro_input!(args as EffectArgs);
    let item = syn::parse_macro_input!(item as ItemTrait);

    match generate_effect_system(&args, &item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate_effect_system(args: &EffectArgs, trait_def: &ItemTrait) -> syn::Result<TokenStream2> {
    let trait_name = &trait_def.ident;
    let trait_vis = &trait_def.vis;

    // Determine module name: custom or derived from trait name (snake_case)
    let module_name = args
        .module_name
        .clone()
        .unwrap_or_else(|| format_ident!("{}", to_snake_case(&trait_name.to_string())));

    // Collect supertraits (these are assumed to be effect capabilities)
    let supertraits: Vec<_> = trait_def
        .supertraits
        .iter()
        .filter_map(|bound| {
            if let TypeParamBound::Trait(trait_bound) = bound {
                // Get the trait path (e.g., BlobStore)
                Some(trait_bound.path.clone())
            } else {
                None
            }
        })
        .collect();

    // Collect method info from this trait's own methods
    let methods: Vec<MethodInfo> = trait_def
        .items
        .iter()
        .filter_map(|item| {
            if let TraitItem::Fn(method) = item {
                Some(parse_method_info(method))
            } else {
                None
            }
        })
        .collect::<syn::Result<Vec<_>>>()?;

    // Generate the module contents
    let provider_trait = generate_provider_trait(trait_def, &supertraits);
    let free_functions = methods.iter().map(generate_free_function);
    let effect_structs = methods.iter().map(generate_effect_struct);
    let consumer_struct = generate_consumer_struct(&methods);
    let effect_impls = methods.iter().map(generate_effect_impl);

    Ok(quote! {
        #[allow(async_fn_in_trait, missing_docs)]
        #trait_vis mod #module_name {
            use super::*;

            #provider_trait

            // Consumer struct for BlobStore.get(key) syntax
            #consumer_struct

            // Free functions that return effect structs
            #(#free_functions)*

            // Effect structs with blanket Effect impls
            #(#effect_structs)*

            // Blanket Effect impls for any P: Provider
            #(#effect_impls)*
        }

        // Re-export the trait at the parent scope
        #trait_vis use #module_name::Provider as #trait_name;

        // Const for BlobStore.get(key) syntax
        #[allow(non_upper_case_globals, missing_docs)]
        #trait_vis const #trait_name: #module_name::Consumer = #module_name::Consumer;
    })
}

struct MethodInfo {
    method_name: Ident,
    struct_name: Ident,
    params: Vec<(Ident, Type)>,
    output_type: Type,
    #[allow(dead_code)]
    is_mut: bool,
}

fn parse_method_info(method: &TraitItemFn) -> syn::Result<MethodInfo> {
    let method_name = method.sig.ident.clone();

    // Create struct name: MethodName in PascalCase (e.g., Get, Set)
    let struct_name = format_ident!("{}", to_pascal_case(&method_name.to_string()));

    // Extract parameters (skip self)
    let params: Vec<_> = method
        .sig
        .inputs
        .iter()
        .filter_map(|arg| {
            if let FnArg::Typed(pat_type) = arg {
                if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                    return Some((pat_ident.ident.clone(), (*pat_type.ty).clone()));
                }
            }
            None
        })
        .collect();

    // Extract return type
    let output_type = match &method.sig.output {
        ReturnType::Default => syn::parse_quote! { () },
        ReturnType::Type(_, ty) => (**ty).clone(),
    };

    // Check if &mut self
    let is_mut = method.sig.inputs.first().is_some_and(|arg| {
        if let FnArg::Receiver(receiver) = arg {
            receiver.mutability.is_some()
        } else {
            false
        }
    });

    Ok(MethodInfo {
        method_name,
        struct_name,
        params,
        output_type,
        is_mut,
    })
}

/// Generate the trait definition inside the module (named Provider)
fn generate_provider_trait(trait_def: &ItemTrait, supertraits: &[syn::Path]) -> TokenStream2 {
    let vis = &trait_def.vis;
    let attrs = &trait_def.attrs;
    let items = &trait_def.items;

    // Supertraits now reference re-exported traits directly (e.g., BlobStore, not blob_store::Provider)
    // The re-export makes the trait available at the parent scope with the original name
    let supertrait_bounds = supertraits.iter().map(|path| {
        quote! { #path }
    });

    // Add ConditionalSync as a supertrait for Send safety
    let supertraits_with_sync = if supertraits.is_empty() {
        quote! { dialog_common::ConditionalSync }
    } else {
        quote! { #(#supertrait_bounds +)* dialog_common::ConditionalSync }
    };

    quote! {
        #(#attrs)*
        #vis trait Provider: #supertraits_with_sync {
            #(#items)*
        }
    }
}

fn generate_free_function(method: &MethodInfo) -> TokenStream2 {
    let method_name = &method.method_name;
    let struct_name = &method.struct_name;

    let params = method.params.iter().map(|(name, ty)| {
        quote! { #name: #ty }
    });

    let field_names: Vec<_> = method.params.iter().map(|(name, _)| name).collect();

    quote! {
        /// Create an effect struct for the `#method_name` operation.
        pub fn #method_name(#(#params),*) -> #struct_name {
            #struct_name { #(#field_names),* }
        }
    }
}

fn generate_effect_struct(method: &MethodInfo) -> TokenStream2 {
    let struct_name = &method.struct_name;

    let fields = method.params.iter().map(|(name, ty)| {
        quote! { pub #name: #ty }
    });

    quote! {
        /// Effect struct representing a pending operation.
        #[derive(Clone)]
        pub struct #struct_name {
            #(#fields),*
        }
    }
}

/// Generate the Consumer struct that enables BlobStore.get(key) syntax
fn generate_consumer_struct(methods: &[MethodInfo]) -> TokenStream2 {
    let fx_methods = methods.iter().map(|m| {
        let method_name = &m.method_name;
        let struct_name = &m.struct_name;

        let params = m.params.iter().map(|(name, ty)| {
            quote! { #name: #ty }
        });

        let field_names: Vec<_> = m.params.iter().map(|(name, _)| name).collect();

        quote! {
            pub fn #method_name(self, #(#params),*) -> #struct_name {
                #struct_name { #(#field_names),* }
            }
        }
    });

    quote! {
        /// Unit struct that provides method-style effect creation.
        ///
        /// Used with the const of the same name as the trait to enable
        /// `BlobStore.get(key)` syntax.
        #[derive(Clone, Copy)]
        pub struct Consumer;

        impl Consumer {
            #(#fx_methods)*
        }
    }
}

/// Generate blanket Effect impl for an effect struct.
///
/// This generates `impl<P: Provider> Effect<Output, P> for EffectStruct`.
/// Any type implementing the Provider trait can be used as the provider.
fn generate_effect_impl(method: &MethodInfo) -> TokenStream2 {
    let struct_name = &method.struct_name;
    let method_name = &method.method_name;
    let output_type = &method.output_type;

    let field_names: Vec<_> = method.params.iter().map(|(name, _)| name).collect();
    let call_args = field_names.iter().map(|name| quote! { self.#name });

    quote! {
        impl<P: Provider> dialog_common::fx::Effect<#output_type, P> for #struct_name {
            async fn perform(self, provider: &mut P) -> #output_type {
                provider.#method_name(#(#call_args),*).await
            }
        }
    }
}

fn to_pascal_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;

    for c in s.chars() {
        if c == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.extend(c.to_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }

    result
}

/// Convert PascalCase to snake_case
/// e.g., "BlobStore" -> "blob_store"
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
