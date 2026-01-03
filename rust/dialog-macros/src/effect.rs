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
    visit_mut::VisitMut,
    FnArg, Generics, Ident, ItemTrait, Pat, ReturnType, TraitItem, TraitItemFn, TraitItemType,
    Type, TypeParamBound, TypePath,
};

/// Optional argument to the effect macro: a custom module name
struct EffectArgs {
    module_name: Option<Ident>,
}

/// Visitor that transforms `Self::AssociatedType` to `P::AssociatedType` in types.
/// This is needed because in the Effect impl, `Self` refers to the effect struct,
/// but we want to reference the provider's associated types.
struct SelfToProviderTransformer;

impl VisitMut for SelfToProviderTransformer {
    fn visit_type_path_mut(&mut self, type_path: &mut TypePath) {
        // Check if path starts with Self
        if type_path.qself.is_none() && !type_path.path.segments.is_empty() {
            let first_segment = &type_path.path.segments[0];
            if first_segment.ident == "Self" && type_path.path.segments.len() > 1 {
                // Replace "Self" with "P" in paths like Self::Item
                type_path.path.segments[0].ident = format_ident!("P");
            }
        }

        // Continue visiting nested types
        syn::visit_mut::visit_type_path_mut(self, type_path);
    }
}

/// Transform a type by replacing `Self::X` with `P::X`
fn transform_self_to_provider(ty: &Type) -> Type {
    let mut ty = ty.clone();
    let mut transformer = SelfToProviderTransformer;
    transformer.visit_type_mut(&mut ty);
    ty
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
    let trait_generics = &trait_def.generics;

    // Extract generic params and where clause
    let (impl_generics, ty_generics, where_clause) = trait_generics.split_for_impl();
    let has_generics = !trait_generics.params.is_empty();

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

    // Collect associated types from this trait
    let assoc_types: Vec<_> = trait_def
        .items
        .iter()
        .filter_map(|item| {
            if let TraitItem::Type(assoc_type) = item {
                Some(assoc_type.clone())
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
    let provider_trait = generate_provider_trait(trait_def, &supertraits, &assoc_types);
    let free_functions = methods
        .iter()
        .map(|m| generate_free_function(m, trait_generics));
    let effect_structs = methods
        .iter()
        .map(|m| generate_effect_struct(m, trait_generics));
    let consumer_struct = generate_consumer_struct(&methods, trait_generics);
    let effect_impls = methods
        .iter()
        .map(|m| generate_effect_impl(m, trait_generics));

    // For generic traits, we can't have a simple const Consumer
    // Instead, we provide a function or skip the const
    let const_or_fn = if has_generics {
        // For generic traits, provide a function that returns Consumer
        quote! {
            /// Create a consumer for method-style effect creation.
            #[allow(non_snake_case, missing_docs)]
            #trait_vis fn #trait_name #impl_generics() -> #module_name::Consumer #ty_generics #where_clause {
                #module_name::Consumer(::std::marker::PhantomData)
            }
        }
    } else {
        quote! {
            // Const for BlobStore.get(key) syntax
            #[allow(non_upper_case_globals, missing_docs)]
            #trait_vis const #trait_name: #module_name::Consumer = #module_name::Consumer;
        }
    };

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

        #const_or_fn
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
fn generate_provider_trait(
    trait_def: &ItemTrait,
    supertraits: &[syn::Path],
    _assoc_types: &[TraitItemType],
) -> TokenStream2 {
    let vis = &trait_def.vis;
    let attrs = &trait_def.attrs;
    let generics = &trait_def.generics;

    // Filter items to only include methods and associated types (exclude other items if any)
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
        #vis trait Provider #generics: #supertraits_with_sync {
            #(#items)*
        }
    }
}

fn generate_free_function(method: &MethodInfo, trait_generics: &Generics) -> TokenStream2 {
    let method_name = &method.method_name;
    let struct_name = &method.struct_name;

    let (impl_generics, ty_generics, where_clause) = trait_generics.split_for_impl();
    let has_generics = !trait_generics.params.is_empty();

    let params = method.params.iter().map(|(name, ty)| {
        quote! { #name: #ty }
    });

    let field_names: Vec<_> = method.params.iter().map(|(name, _)| name).collect();

    if has_generics {
        quote! {
            /// Create an effect struct for the `#method_name` operation.
            pub fn #method_name #impl_generics(#(#params),*) -> #struct_name #ty_generics #where_clause {
                #struct_name { #(#field_names,)* _marker: ::std::marker::PhantomData }
            }
        }
    } else {
        quote! {
            /// Create an effect struct for the `#method_name` operation.
            pub fn #method_name(#(#params),*) -> #struct_name {
                #struct_name { #(#field_names),* }
            }
        }
    }
}

fn generate_effect_struct(method: &MethodInfo, trait_generics: &Generics) -> TokenStream2 {
    let struct_name = &method.struct_name;
    let has_generics = !trait_generics.params.is_empty();

    let fields = method.params.iter().map(|(name, ty)| {
        quote! { pub #name: #ty }
    });

    if has_generics {
        // For generic traits, we need PhantomData to use the type parameters
        let type_params: Vec<_> = trait_generics
            .params
            .iter()
            .filter_map(|p| {
                if let syn::GenericParam::Type(tp) = p {
                    Some(&tp.ident)
                } else {
                    None
                }
            })
            .collect();

        let phantom_type = if type_params.len() == 1 {
            let tp = &type_params[0];
            quote! { #tp }
        } else {
            quote! { (#(#type_params),*) }
        };

        quote! {
            /// Effect struct representing a pending operation.
            #[derive(Clone)]
            pub struct #struct_name #trait_generics {
                #(#fields,)*
                #[doc(hidden)]
                pub _marker: ::std::marker::PhantomData<#phantom_type>,
            }
        }
    } else {
        quote! {
            /// Effect struct representing a pending operation.
            #[derive(Clone)]
            pub struct #struct_name {
                #(#fields),*
            }
        }
    }
}

/// Generate the Consumer struct that enables BlobStore.get(key) syntax
fn generate_consumer_struct(methods: &[MethodInfo], trait_generics: &Generics) -> TokenStream2 {
    let has_generics = !trait_generics.params.is_empty();
    let (impl_generics, ty_generics, where_clause) = trait_generics.split_for_impl();

    let fx_methods = methods.iter().map(|m| {
        let method_name = &m.method_name;
        let struct_name = &m.struct_name;

        let params = m.params.iter().map(|(name, ty)| {
            quote! { #name: #ty }
        });

        let field_names: Vec<_> = m.params.iter().map(|(name, _)| name).collect();

        if has_generics {
            quote! {
                pub fn #method_name(self, #(#params),*) -> #struct_name #ty_generics {
                    #struct_name { #(#field_names,)* _marker: ::std::marker::PhantomData }
                }
            }
        } else {
            quote! {
                pub fn #method_name(self, #(#params),*) -> #struct_name {
                    #struct_name { #(#field_names),* }
                }
            }
        }
    });

    if has_generics {
        // For generic traits, Consumer needs to carry the type parameters
        let type_params: Vec<_> = trait_generics
            .params
            .iter()
            .filter_map(|p| {
                if let syn::GenericParam::Type(tp) = p {
                    Some(&tp.ident)
                } else {
                    None
                }
            })
            .collect();

        let phantom_type = if type_params.len() == 1 {
            let tp = &type_params[0];
            quote! { #tp }
        } else {
            quote! { (#(#type_params),*) }
        };

        quote! {
            /// Unit struct that provides method-style effect creation.
            ///
            /// Used with the const of the same name as the trait to enable
            /// `BlobStore.get(key)` syntax.
            pub struct Consumer #trait_generics (pub ::std::marker::PhantomData<#phantom_type>);

            impl #impl_generics Clone for Consumer #ty_generics #where_clause {
                fn clone(&self) -> Self {
                    *self
                }
            }

            impl #impl_generics Copy for Consumer #ty_generics #where_clause {}

            impl #impl_generics Consumer #ty_generics #where_clause {
                #(#fx_methods)*
            }
        }
    } else {
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
}

/// Generate blanket Effect impl for an effect struct.
///
/// This generates `impl<P: Provider> Effect<P> for EffectStruct` with associated type Output.
/// Any type implementing the Provider trait can be used as the provider.
fn generate_effect_impl(method: &MethodInfo, trait_generics: &Generics) -> TokenStream2 {
    let struct_name = &method.struct_name;
    let method_name = &method.method_name;
    // Transform Self::X to P::X so that associated types reference the provider
    let output_type = transform_self_to_provider(&method.output_type);
    let has_generics = !trait_generics.params.is_empty();

    let field_names: Vec<_> = method.params.iter().map(|(name, _)| name).collect();
    let call_args = field_names.iter().map(|name| quote! { self.#name });

    if has_generics {
        let (_impl_generics, ty_generics, where_clause) = trait_generics.split_for_impl();

        // We need to combine the trait's generic params with P: Provider
        // Extract just the type param idents for the Provider bound
        let generic_params = &trait_generics.params;

        quote! {
            impl<#generic_params, P: Provider #ty_generics> dialog_common::fx::Effect<P> for #struct_name #ty_generics #where_clause {
                type Output = #output_type;

                async fn perform(self, provider: &mut P) -> Self::Output {
                    provider.#method_name(#(#call_args),*).await
                }
            }
        }
    } else {
        quote! {
            impl<P: Provider> dialog_common::fx::Effect<P> for #struct_name {
                type Output = #output_type;

                async fn perform(self, provider: &mut P) -> Self::Output {
                    provider.#method_name(#(#call_args),*).await
                }
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
