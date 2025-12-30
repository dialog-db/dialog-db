//! The `#[effectful]` macro transforms functions with `perform!` macro calls into
//! effect-based computations that return `Task`.
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

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    visit_mut::VisitMut,
    Expr, ExprMacro, FnArg, ItemFn, Path, ReturnType, Token,
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
                    #inner_expr.perform(&__co).await
                };
            }
        }
    }
}

pub fn effectful_impl(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as EffectfulArgs);
    let mut func = parse_macro_input!(item as ItemFn);

    match generate_effectful_function(&args, &mut func) {
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
    let inputs = &sig.inputs;
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

    // Build capability bounds
    let from_bounds = capabilities.iter().map(|cap| {
        quote! { From<#cap::Capability> }
    });

    let try_into_bounds = capabilities.iter().map(|cap| {
        quote! { TryInto<#cap::Output> }
    });

    // Transform the function body - replace perform! with .perform(&__co).await
    let mut body = func.block.as_ref().clone();
    let mut transformer = PerformTransformer;
    transformer.visit_block_mut(&mut body);

    let body_stmts = &body.stmts;

    // Build the where clause
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

    // Generate the function with Cap generic parameter
    let generics_with_cap = if existing_params.is_empty() {
        quote! { <__Cap> }
    } else {
        quote! { <#existing_params, __Cap> }
    };

    // Handle async - if the function is marked async, we need to remove that
    let is_async = sig.asyncness.is_some();
    if is_async {
        return Err(syn::Error::new_spanned(
            sig.asyncness,
            "#[effectful] functions should not be marked async; the macro handles this",
        ));
    }

    // For methods with &self, we need to add + use<'_, __Cap> to capture the lifetime
    // This uses the Rust 2024 precise capturing syntax
    let return_type = if has_ref_self {
        quote! {
            dialog_common::fx::Task<__Cap, impl std::future::Future<Output = #output_type> + use<'_, __Cap>>
        }
    } else {
        quote! {
            dialog_common::fx::Task<__Cap, impl std::future::Future<Output = #output_type>>
        }
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
