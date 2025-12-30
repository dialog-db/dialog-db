//! The `#[effect]` macro generates an algebraic effects system from a trait.
//!
//! ```ignore
//! use dialog_macros::effect;
//!
//! #[effect]
//! pub trait BlockStore {
//!     async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>>;
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>);
//! }
//! ```
//!
//! This generates a module with the trait name containing:
//! - The original trait (with ConditionalSync added)
//! - Free functions `get()`, `set()` that return effect structs
//! - `Get`, `Set` effect structs (implement `Effect` trait)
//! - `Capability` enum (the capability type for this effect module)
//! - `Output` enum (results of capability requests)
//! - `Env<T>` wrapper struct that implements `Provider` for any `T: Trait`
//! - `Effect` impls for each effect struct (using From/TryFrom for composition)
//! - `From<Capability>` impl for composing capabilities
//! - `TryFrom<Output>` for extracting outputs
//!
//! For traits with supertraits (composition):
//! ```ignore
//! #[effect]
//! pub trait Env: BlockStore + TransactionalMemory {}
//! ```
//! The macro combines Capability/Output enums from all supertraits and generates
//! dispatch functions for composite providers.
//!
//! Usage:
//! ```ignore
//! // Implement the trait
//! impl BlockStore::BlockStore for MyBackend {
//!     async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> { ... }
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) { ... }
//! }
//!
//! // Use effects with the generated Env wrapper
//! let provider = BlockStore::Env::new(my_backend);
//! BlockStore::get(key).perform(&provider).await
//! BlockStore::set(key, value).perform(&provider).await
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{FnArg, Ident, ItemTrait, Pat, ReturnType, TraitItem, TraitItemFn, Type, TypeParamBound};

/// Attribute macro that generates an algebraic effects system from a trait.
///
/// See the [module-level documentation](self) for details.
pub fn effect_impl(item: ItemTrait) -> TokenStream {
    match generate_effect_system(&item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate_effect_system(trait_def: &ItemTrait) -> syn::Result<TokenStream2> {
    let trait_name = &trait_def.ident;
    let trait_vis = &trait_def.vis;

    // Collect supertraits (these are assumed to be effect capabilities)
    let supertraits: Vec<_> = trait_def
        .supertraits
        .iter()
        .filter_map(|bound| {
            if let TypeParamBound::Trait(trait_bound) = bound {
                // Get the trait path (e.g., BlockStore)
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
    let provider_trait = generate_provider_trait(trait_def, trait_name, &supertraits);
    let free_functions = methods.iter().map(generate_free_function);
    let effect_structs = methods.iter().map(generate_effect_struct);
    let capability_enum = generate_capability_enum(&methods, &supertraits);
    let output_enum = generate_output_enum(&methods, &supertraits);
    let from_impls = generate_from_impls(&methods, &supertraits);
    let capability_trait_impl = generate_capability_trait_impl();
    let effect_impls = methods.iter().map(generate_effect_impl);
    let into_provider_impl = generate_into_provider_impl(&methods, trait_name, &supertraits);
    let dispatch_fn = generate_dispatch_fn(&methods, trait_name, &supertraits);

    Ok(quote! {
        #[allow(async_fn_in_trait)]
        #[allow(non_snake_case)]
        #trait_vis mod #trait_name {
            use super::*;

            #provider_trait

            // Free functions that return effect structs
            #(#free_functions)*

            // Effect structs
            #(#effect_structs)*

            // Capability enum
            #capability_enum

            // Output enum
            #output_enum

            // From/TryFrom impls for composition
            #from_impls

            // Capability trait impl (links Capability to Output)
            #capability_trait_impl

            // Effect impls
            #(#effect_impls)*

            // IntoProvider impl for &mut T
            #into_provider_impl

            // Dispatch function for implementing custom providers
            #dispatch_fn
        }
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
    let is_mut = method.sig.inputs.first().map_or(false, |arg| {
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

/// Generate the trait definition inside the module
fn generate_provider_trait(
    trait_def: &ItemTrait,
    trait_name: &Ident,
    supertraits: &[syn::Path],
) -> TokenStream2 {
    let vis = &trait_def.vis;
    let attrs = &trait_def.attrs;
    let items = &trait_def.items;

    // Prepend supertraits with their module path (e.g., BlockStore -> BlockStore::BlockStore)
    let supertrait_bounds = supertraits.iter().map(|path| {
        quote! { #path::#path }
    });

    // Add ConditionalSync as a supertrait for Send safety
    let supertraits_with_sync = if supertraits.is_empty() {
        quote! { dialog_common::ConditionalSync }
    } else {
        quote! { #(#supertrait_bounds +)* dialog_common::ConditionalSync }
    };

    quote! {
        #(#attrs)*
        #vis trait #trait_name: #supertraits_with_sync {
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
    let output_type = &method.output_type;

    let fields = method.params.iter().map(|(name, ty)| {
        quote! { pub #name: #ty }
    });

    quote! {
        /// Effect struct representing a pending operation.
        #[derive(Clone)]
        pub struct #struct_name {
            #(#fields),*
        }

        impl #struct_name {
            /// Wrap a result value in the Output enum.
            pub fn output(result: #output_type) -> Output {
                Output::#struct_name(result)
            }
        }

        impl From<#struct_name> for Capability {
            fn from(effect: #struct_name) -> Self {
                Capability::#struct_name(effect)
            }
        }
    }
}

fn generate_capability_enum(methods: &[MethodInfo], supertraits: &[syn::Path]) -> TokenStream2 {
    // Variants for own methods
    let method_variants = methods.iter().map(|m| {
        let struct_name = &m.struct_name;
        quote! { #struct_name(#struct_name) }
    });

    // Variants for supertraits (re-export their Capability)
    let supertrait_variants = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! { #variant_name(#path::Capability) }
    });

    quote! {
        /// Enum of all capability requests for this effect module.
        #[derive(Clone)]
        pub enum Capability {
            #(#method_variants,)*
            #(#supertrait_variants,)*
        }
    }
}

fn generate_output_enum(methods: &[MethodInfo], supertraits: &[syn::Path]) -> TokenStream2 {
    // Variants for own methods
    let method_variants = methods.iter().map(|m| {
        let struct_name = &m.struct_name;
        let output_type = &m.output_type;
        quote! { #struct_name(#output_type) }
    });

    // Variants for supertraits (re-export their Output)
    let supertrait_variants = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! { #variant_name(#path::Output) }
    });

    quote! {
        /// Enum of all outputs from capability requests.
        #[derive(Default)]
        pub enum Output {
            #[default]
            __Default,
            #(#method_variants,)*
            #(#supertrait_variants,)*
        }
    }
}

fn generate_from_impls(methods: &[MethodInfo], supertraits: &[syn::Path]) -> TokenStream2 {
    // Generate From<Supertrait::Capability> for Capability
    let from_capability_impls = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! {
            impl From<#path::Capability> for Capability {
                fn from(cap: #path::Capability) -> Self {
                    Capability::#variant_name(cap)
                }
            }
        }
    });

    // Generate From<Supertrait::Output> for Output
    let from_output_impls = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! {
            impl From<#path::Output> for Output {
                fn from(out: #path::Output) -> Self {
                    Output::#variant_name(out)
                }
            }
        }
    });

    // Generate TryFrom<Output> for Supertrait::Output
    let try_from_output_impls = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! {
            impl TryFrom<Output> for #path::Output {
                type Error = Output;
                fn try_from(out: Output) -> Result<Self, Self::Error> {
                    match out {
                        Output::#variant_name(inner) => Ok(inner),
                        other => Err(other),
                    }
                }
            }
        }
    });

    // Generate TryFrom<Output> for individual method output types
    let try_from_method_output_impls = methods.iter().map(|m| {
        let struct_name = &m.struct_name;
        let output_type = &m.output_type;
        quote! {
            impl TryFrom<Output> for #output_type {
                type Error = Output;
                fn try_from(out: Output) -> Result<Self, Self::Error> {
                    match out {
                        Output::#struct_name(result) => Ok(result),
                        other => Err(other),
                    }
                }
            }
        }
    });

    quote! {
        #(#from_capability_impls)*
        #(#from_output_impls)*
        #(#try_from_output_impls)*
        #(#try_from_method_output_impls)*
    }
}

fn generate_capability_trait_impl() -> TokenStream2 {
    quote! {
        impl dialog_common::fx::Capability for Capability {
            type Output = Output;
        }
    }
}

fn generate_effect_impl(method: &MethodInfo) -> TokenStream2 {
    let struct_name = &method.struct_name;
    let output_type = &method.output_type;

    // Generate Effect impl that works with any Capability type via From/TryFrom
    // The impl works in two ways:
    // 1. Direct: Cap = Capability (for the defining module's Capability type)
    // 2. Indirect: Cap: From<Capability> (for composite Capability types)
    quote! {
        impl<Cap> dialog_common::fx::Effect<#output_type, Cap> for #struct_name
        where
            Cap: dialog_common::fx::Capability + From<Capability>,
            Cap::Output: TryInto<Output>,
        {
            async fn perform<P>(self, provider: &mut P) -> #output_type
            where
                P: dialog_common::fx::Provider<Capability = Cap>,
            {
                // Convert through the module's Capability type
                let module_cap: Capability = self.into();
                let cap: Cap = module_cap.into();
                let out = provider.provide(cap).await;
                match out.try_into() {
                    Ok(Output::#struct_name(result)) => result,
                    _ => unreachable!("Provider returned wrong output variant"),
                }
            }
        }
    }
}

/// Previously generated IntoProvider impl - now users use #[provider] macro instead
fn generate_into_provider_impl(
    _methods: &[MethodInfo],
    _trait_name: &Ident,
    _supertraits: &[syn::Path],
) -> TokenStream2 {
    // No generated code needed - users will use #[provider(Trait)] macro instead
    quote! {}
}

/// Generate the dispatch function for custom Provider implementations
fn generate_dispatch_fn(
    methods: &[MethodInfo],
    trait_name: &Ident,
    supertraits: &[syn::Path],
) -> TokenStream2 {
    // If there are no methods and only supertraits, we still generate dispatch
    // for the composite case

    // Generate match arms for own methods
    let method_arms: Vec<_> = methods
        .iter()
        .map(|m| {
            let method_name = &m.method_name;
            let struct_name = &m.struct_name;

            let field_names: Vec<_> = m.params.iter().map(|(name, _)| name).collect();
            let call_args = field_names.iter().map(|name| quote! { effect.#name });

            quote! {
                Capability::#struct_name(effect) => {
                    #struct_name::output(backend.#method_name(#(#call_args),*).await)
                }
            }
        })
        .collect();

    // Generate match arms for supertrait capabilities
    let supertrait_arms: Vec<_> = supertraits
        .iter()
        .map(|path| {
            let variant_name = path.segments.last().unwrap().ident.clone();
            quote! {
                Capability::#variant_name(cap) => {
                    Output::#variant_name(#path::dispatch(backend, cap).await)
                }
            }
        })
        .collect();

    let all_arms = method_arms.iter().chain(supertrait_arms.iter());

    // Build supertrait bounds
    let supertrait_bounds = supertraits.iter().map(|path| {
        quote! { #path::#path }
    });

    let trait_bounds = if supertraits.is_empty() {
        quote! { #trait_name }
    } else {
        quote! { #trait_name + #(#supertrait_bounds)+* }
    };

    quote! {
        /// Dispatch a capability request to the backend.
        ///
        /// This function is useful for implementing custom Provider types.
        pub async fn dispatch<T>(backend: &mut T, capability: Capability) -> Output
        where
            T: #trait_bounds,
        {
            match capability {
                #(#all_arms)*
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
