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
//!   - Free functions `get()`, `set()` that return effect structs
//!   - `Get`, `Set` effect structs (implement `Effect` trait)
//!   - `Capability` enum (the capability type for this effect module)
//!   - `Output` enum (results of capability requests)
//!   - `Consumer` struct for method-style effect creation
//!   - `dispatch` function for implementing custom providers
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
    let capability_enum = generate_capability_enum(&methods, &supertraits, &module_name);
    let output_enum = generate_output_enum(&methods, &supertraits, &module_name);
    let from_impls = generate_from_impls(&methods, &supertraits, &module_name);
    let capability_trait_impl = generate_capability_trait_impl();
    let effect_impls = methods.iter().map(generate_effect_impl);
    let dispatch_fn = generate_dispatch_fn(&methods, &supertraits, &module_name);

    Ok(quote! {
        #[allow(async_fn_in_trait)]
        #trait_vis mod #module_name {
            use super::*;

            #provider_trait

            // Consumer struct for BlobStore.get(key) syntax
            #consumer_struct

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

            // Dispatch function for implementing custom providers
            #dispatch_fn
        }

        // Re-export the trait at the parent scope
        #trait_vis use #module_name::Provider as #trait_name;

        // Const for BlobStore.get(key) syntax
        #[allow(non_upper_case_globals)]
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

fn generate_capability_enum(
    methods: &[MethodInfo],
    supertraits: &[syn::Path],
    _module_name: &Ident,
) -> TokenStream2 {
    // Variants for own methods
    let method_variants = methods.iter().map(|m| {
        let struct_name = &m.struct_name;
        quote! { #struct_name(#struct_name) }
    });

    // Variants for supertraits - derive module name from trait name
    // e.g., BlobStore -> blob_store::Capability
    let supertrait_variants = supertraits.iter().map(|path| {
        let trait_name = &path.segments.last().unwrap().ident;
        let module_name = format_ident!("{}", to_snake_case(&trait_name.to_string()));
        quote! { #trait_name(#module_name::Capability) }
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

fn generate_output_enum(
    methods: &[MethodInfo],
    supertraits: &[syn::Path],
    _module_name: &Ident,
) -> TokenStream2 {
    // Variants for own methods
    let method_variants = methods.iter().map(|m| {
        let struct_name = &m.struct_name;
        let output_type = &m.output_type;
        quote! { #struct_name(#output_type) }
    });

    // Variants for supertraits - derive module name from trait name
    let supertrait_variants = supertraits.iter().map(|path| {
        let trait_name = &path.segments.last().unwrap().ident;
        let module_name = format_ident!("{}", to_snake_case(&trait_name.to_string()));
        quote! { #trait_name(#module_name::Output) }
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

fn generate_from_impls(
    methods: &[MethodInfo],
    supertraits: &[syn::Path],
    _module_name: &Ident,
) -> TokenStream2 {
    // Generate From<supertrait_module::Capability> for Capability
    let from_capability_impls = supertraits.iter().map(|path| {
        let trait_name = &path.segments.last().unwrap().ident;
        let supertrait_module = format_ident!("{}", to_snake_case(&trait_name.to_string()));
        quote! {
            impl From<#supertrait_module::Capability> for Capability {
                fn from(cap: #supertrait_module::Capability) -> Self {
                    Capability::#trait_name(cap)
                }
            }
        }
    });

    // Generate From<supertrait_module::Output> for Output
    let from_output_impls = supertraits.iter().map(|path| {
        let trait_name = &path.segments.last().unwrap().ident;
        let supertrait_module = format_ident!("{}", to_snake_case(&trait_name.to_string()));
        quote! {
            impl From<#supertrait_module::Output> for Output {
                fn from(out: #supertrait_module::Output) -> Self {
                    Output::#trait_name(out)
                }
            }
        }
    });

    // Generate TryFrom<Output> for supertrait_module::Output
    let try_from_output_impls = supertraits.iter().map(|path| {
        let trait_name = &path.segments.last().unwrap().ident;
        let supertrait_module = format_ident!("{}", to_snake_case(&trait_name.to_string()));
        quote! {
            impl TryFrom<Output> for #supertrait_module::Output {
                type Error = Output;
                fn try_from(out: Output) -> Result<Self, Self::Error> {
                    match out {
                        Output::#trait_name(inner) => Ok(inner),
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

/// Generate the dispatch function for custom Provider implementations
fn generate_dispatch_fn(
    methods: &[MethodInfo],
    supertraits: &[syn::Path],
    _module_name: &Ident,
) -> TokenStream2 {
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
            let trait_name = &path.segments.last().unwrap().ident;
            let supertrait_module = format_ident!("{}", to_snake_case(&trait_name.to_string()));
            quote! {
                Capability::#trait_name(cap) => {
                    Output::#trait_name(#supertrait_module::dispatch(backend, cap).await)
                }
            }
        })
        .collect();

    let all_arms = method_arms.iter().chain(supertrait_arms.iter());

    // Build supertrait bounds - now reference the re-exported trait names directly
    let supertrait_bounds = supertraits.iter().map(|path| {
        quote! { #path }
    });

    let trait_bounds = if supertraits.is_empty() {
        quote! { Provider }
    } else {
        quote! { Provider + #(#supertrait_bounds)+* }
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
