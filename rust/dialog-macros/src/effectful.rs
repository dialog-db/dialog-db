//! The `#[effectful]` macro transforms functions with `perform!` macro calls into
//! effect-based computations that return `Task`.
//!
//! # On Functions and Methods
//!
//! ```ignore
//! use dialog_macros::effectful;
//!
//! #[effectful(BlockStore, TransactionalMemory)]
//! pub fn copy(from: Vec<u8>, to: Vec<u8>) -> Result<(), Error> {
//!     let content = perform!(BlockStore::get(from))?;
//!     perform!(BlockStore::set(to, content.unwrap_or_default()))
//! }
//! ```
//!
//! This expands to:
//! ```ignore
//! pub fn copy<Cap>(from: Vec<u8>, to: Vec<u8>) -> dialog_common::fx::Task<Cap, impl std::future::Future<Output = Result<(), Error>>>
//! where
//!     Cap: dialog_common::fx::Capability
//!         + From<BlockStore::Capability>
//!         + From<TransactionalMemory::Capability>,
//!     Cap::Output: TryInto<BlockStore::Output> + TryInto<TransactionalMemory::Output>,
//! {
//!     dialog_common::fx::Task::new(|__co| async move {
//!         let content = BlockStore::get(from).perform(&__co).await?;
//!         BlockStore::set(to, content.unwrap_or_default()).perform(&__co).await
//!     })
//! }
//! ```
//!
//! # On Methods with &self
//!
//! For methods with `&self`, an explicit lifetime is added to capture the borrow:
//!
//! ```ignore
//! impl Cache {
//!     #[effectful(BlockStore)]
//!     fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
//!         perform!(BlockStore::get(key))
//!     }
//! }
//! ```
//!
//! Expands to:
//! ```ignore
//! impl Cache {
//!     fn get<'__self, __Cap>(&'__self self, key: Vec<u8>) -> dialog_common::fx::Task<__Cap, impl std::future::Future<Output = Option<Vec<u8>>> + '__self>
//!     where
//!         __Cap: dialog_common::fx::Capability + From<BlockStore::Capability>,
//!         __Cap::Output: TryInto<BlockStore::Output>,
//!     {
//!         dialog_common::fx::Task::new(|__co| async move {
//!             BlockStore::get(key).perform(&__co).await
//!         })
//!     }
//! }
//! ```
//!
//! # On Trait Methods
//!
//! Works the same way for both trait definitions and implementations:
//!
//! ```ignore
//! trait Storage {
//!     #[effectful(BlockStore)]
//!     fn load(&self, key: Vec<u8>) -> Option<Vec<u8>>;
//! }
//!
//! impl Storage for MyStorage {
//!     #[effectful(BlockStore)]
//!     fn load(&self, key: Vec<u8>) -> Option<Vec<u8>> {
//!         perform!(BlockStore::get(key))
//!     }
//! }
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse::{discouraged::Speculative, Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    visit_mut::VisitMut,
    Expr, ExprMacro, FnArg, ItemFn, Path, Receiver, ReturnType, Token, TraitItemFn,
};

/// Arguments to the effectful macro: a comma-separated list of capability paths
struct EffectfulArgs {
    capabilities: Punctuated<Path, Token![,]>,
}

impl Parse for EffectfulArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(EffectfulArgs {
            capabilities: Punctuated::parse_terminated(input)?,
        })
    }
}

/// Visitor that transforms `perform!(expr)` into `expr.perform(&__co).await`
struct PerformTransformer;

impl VisitMut for PerformTransformer {
    fn visit_expr_mut(&mut self, expr: &mut Expr) {
        // First, recurse into child expressions
        syn::visit_mut::visit_expr_mut(self, expr);

        // Then check if this is a perform! macro invocation
        if let Expr::Macro(ExprMacro { mac, .. }) = expr {
            // Check if the macro is named "perform"
            if mac.path.is_ident("perform") {
                // Parse the inner expression from the macro tokens
                let inner_tokens = &mac.tokens;
                let inner_expr: Expr = syn::parse2(inner_tokens.clone())
                    .expect("perform! macro should contain a valid expression");

                *expr = syn::parse_quote! {
                    #inner_expr.perform(&mut &__co).await
                };
            }
        }
    }
}

/// Either a function (ItemFn) or a trait method declaration (TraitItemFn)
enum EffectfulTarget {
    Fn(ItemFn),
    TraitMethod(TraitItemFn),
}

impl Parse for EffectfulTarget {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        // We need to fork to try parsers since they consume the input
        let fork = input.fork();

        // Try to parse as TraitItemFn (works for both with and without body)
        if let Ok(trait_method) = fork.parse::<TraitItemFn>() {
            input.advance_to(&fork);
            return Ok(EffectfulTarget::TraitMethod(trait_method));
        }

        // Fall back to ItemFn (for regular functions and impl methods)
        input.parse::<ItemFn>().map(EffectfulTarget::Fn)
    }
}

pub fn effectful_impl(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as EffectfulArgs);
    let item = parse_macro_input!(item as EffectfulTarget);

    let result = match item {
        EffectfulTarget::Fn(mut func) => generate_effectful_function(&args, &mut func),
        EffectfulTarget::TraitMethod(method) => {
            if method.default.is_none() {
                // Trait method declaration (no body)
                generate_effectful_trait_method(&args, &method)
            } else {
                // Method with body - could be trait default or impl method
                // Convert to ItemFn for processing
                let item_fn = ItemFn {
                    attrs: method.attrs,
                    vis: syn::Visibility::Inherited,
                    sig: method.sig,
                    block: Box::new(method.default.unwrap()),
                };
                generate_effectful_function(&args, &mut { item_fn })
            }
        }
    };

    match result {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate_effectful_function(args: &EffectfulArgs, func: &mut ItemFn) -> syn::Result<TokenStream2> {
    let capabilities = &args.capabilities;

    // Extract function parts
    let vis = &func.vis;
    let sig = &mut func.sig;
    let fn_name = &sig.ident;
    let output_type = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Check if this is a method with &self or &mut self receiver
    let has_ref_self = sig.inputs.first().map_or(false, |arg| {
        matches!(arg, FnArg::Receiver(r) if r.reference.is_some())
    });

    // Check for existing generics and preserve them
    let existing_generics = &sig.generics;
    let existing_params = &existing_generics.params;
    let existing_where = existing_generics.where_clause.as_ref();

    // Build capability bounds (collect into vectors so they can be used multiple times)
    let from_bounds: Vec<_> = capabilities.iter().map(|cap| {
        quote! { From<#cap::Capability> }
    }).collect();

    let try_into_bounds: Vec<_> = capabilities.iter().map(|cap| {
        quote! { TryInto<#cap::Output> }
    }).collect();

    // Transform the function body - replace perform! with .perform(&__co).await
    let mut body = func.block.as_ref().clone();
    let mut transformer = PerformTransformer;
    transformer.visit_block_mut(&mut body);

    let body_stmts = &body.stmts;

    // Handle async - if the function is marked async, we need to remove that
    if sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            sig.asyncness,
            "#[effectful] functions should not be marked async; the macro handles this",
        ));
    }

    // For methods with &self, add explicit lifetime to capture the borrow
    if has_ref_self {
        // Modify the receiver to have explicit lifetime
        if let Some(FnArg::Receiver(receiver)) = sig.inputs.first_mut() {
            let new_receiver = add_lifetime_to_receiver(receiver);
            *receiver = new_receiver;
        }

        // Generate with lifetime
        let generics_with_cap = if existing_params.is_empty() {
            quote! { <'__self, __Cap> }
        } else {
            quote! { <'__self, #existing_params, __Cap> }
        };

        // Build the where clause with lifetime bounds on __Cap
        let where_clause_with_lifetime = if let Some(existing) = existing_where {
            let existing_predicates = &existing.predicates;
            quote! {
                where
                    #existing_predicates,
                    __Cap: dialog_common::fx::Capability + #(#from_bounds)+* + '__self,
                    __Cap::Output: #(#try_into_bounds)+* + '__self,
            }
        } else {
            quote! {
                where
                    __Cap: dialog_common::fx::Capability + #(#from_bounds)+* + '__self,
                    __Cap::Output: #(#try_into_bounds)+* + '__self,
            }
        };

        let inputs = &sig.inputs;
        let return_type = quote! {
            dialog_common::fx::Task<__Cap, impl std::future::Future<Output = #output_type> + '__self>
        };

        Ok(quote! {
            #vis fn #fn_name #generics_with_cap (#inputs) -> #return_type
            #where_clause_with_lifetime
            {
                dialog_common::fx::Task::new(|__co| async move {
                    #(#body_stmts)*
                })
            }
        })
    } else {
        // No &self, no lifetime needed
        let generics_with_cap = if existing_params.is_empty() {
            quote! { <__Cap> }
        } else {
            quote! { <#existing_params, __Cap> }
        };

        // Build the where clause without lifetime bounds
        let where_clause = if let Some(existing) = existing_where {
            let existing_predicates = &existing.predicates;
            quote! {
                where
                    #existing_predicates,
                    __Cap: dialog_common::fx::Capability + #(#from_bounds)+*,
                    __Cap::Output: #(#try_into_bounds)+*,
            }
        } else {
            quote! {
                where
                    __Cap: dialog_common::fx::Capability + #(#from_bounds)+*,
                    __Cap::Output: #(#try_into_bounds)+*,
            }
        };

        let inputs = &sig.inputs;
        let return_type = quote! {
            dialog_common::fx::Task<__Cap, impl std::future::Future<Output = #output_type>>
        };

        Ok(quote! {
            #vis fn #fn_name #generics_with_cap (#inputs) -> #return_type
            #where_clause
            {
                dialog_common::fx::Task::new(|__co| async move {
                    #(#body_stmts)*
                })
            }
        })
    }
}

/// Add '__self lifetime to a receiver (e.g., &self -> &'__self self)
fn add_lifetime_to_receiver(receiver: &Receiver) -> Receiver {
    let mut new_receiver = receiver.clone();
    if let Some((and_token, lifetime)) = &mut new_receiver.reference {
        // Add the '__self lifetime
        *lifetime = Some(syn::Lifetime::new("'__self", and_token.span));
    }
    new_receiver
}

/// Generate an effectful trait method declaration (no body, just signature with bounds)
fn generate_effectful_trait_method(args: &EffectfulArgs, method: &TraitItemFn) -> syn::Result<TokenStream2> {
    let capabilities = &args.capabilities;

    let sig = &method.sig;
    let fn_name = &sig.ident;
    let attrs = &method.attrs;
    let output_type = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Check if this is a method with &self or &mut self receiver
    let has_ref_self = sig.inputs.first().map_or(false, |arg| {
        matches!(arg, FnArg::Receiver(r) if r.reference.is_some())
    });

    // Check for existing generics and preserve them
    let existing_generics = &sig.generics;
    let existing_params = &existing_generics.params;
    let existing_where = existing_generics.where_clause.as_ref();

    // Build capability bounds (collect into vectors so they can be used multiple times)
    let from_bounds: Vec<_> = capabilities.iter().map(|cap| {
        quote! { From<#cap::Capability> }
    }).collect();

    let try_into_bounds: Vec<_> = capabilities.iter().map(|cap| {
        quote! { TryInto<#cap::Output> }
    }).collect();

    // Handle async - if the method is marked async, we need to reject that
    if sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            sig.asyncness,
            "#[effectful] methods should not be marked async; the macro handles this",
        ));
    }

    // For methods with &self, add explicit lifetime
    if has_ref_self {
        // Create modified inputs with lifetime on receiver
        let mut inputs = sig.inputs.clone();
        if let Some(FnArg::Receiver(receiver)) = inputs.first_mut() {
            *receiver = add_lifetime_to_receiver(receiver);
        }

        let generics_with_cap = if existing_params.is_empty() {
            quote! { <'__self, __Cap> }
        } else {
            quote! { <'__self, #existing_params, __Cap> }
        };

        // Build the where clause with lifetime bounds on __Cap
        let where_clause = if let Some(existing) = existing_where {
            let existing_predicates = &existing.predicates;
            quote! {
                where
                    #existing_predicates,
                    __Cap: dialog_common::fx::Capability + #(#from_bounds)+* + '__self,
                    __Cap::Output: #(#try_into_bounds)+* + '__self,
            }
        } else {
            quote! {
                where
                    __Cap: dialog_common::fx::Capability + #(#from_bounds)+* + '__self,
                    __Cap::Output: #(#try_into_bounds)+* + '__self,
            }
        };

        let return_type = quote! {
            dialog_common::fx::Task<__Cap, impl std::future::Future<Output = #output_type> + '__self>
        };

        Ok(quote! {
            #(#attrs)*
            fn #fn_name #generics_with_cap (#inputs) -> #return_type
            #where_clause;
        })
    } else {
        let inputs = &sig.inputs;
        let generics_with_cap = if existing_params.is_empty() {
            quote! { <__Cap> }
        } else {
            quote! { <#existing_params, __Cap> }
        };

        // Build the where clause without lifetime bounds
        let where_clause = if let Some(existing) = existing_where {
            let existing_predicates = &existing.predicates;
            quote! {
                where
                    #existing_predicates,
                    __Cap: dialog_common::fx::Capability + #(#from_bounds)+*,
                    __Cap::Output: #(#try_into_bounds)+*,
            }
        } else {
            quote! {
                where
                    __Cap: dialog_common::fx::Capability + #(#from_bounds)+*,
                    __Cap::Output: #(#try_into_bounds)+*,
            }
        };

        let return_type = quote! {
            dialog_common::fx::Task<__Cap, impl std::future::Future<Output = #output_type>>
        };

        Ok(quote! {
            #(#attrs)*
            fn #fn_name #generics_with_cap (#inputs) -> #return_type
            #where_clause;
        })
    }
}
