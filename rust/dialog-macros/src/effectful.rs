//! The `#[effectful]` macro transforms functions with `perform!(expr)` calls into
//! effect-based computations using `Task`-wrapped async closures.
//!
//! # On Functions and Methods
//!
//! ```ignore
//! use dialog_macros::effectful;
//!
//! #[effectful(BlockStore + TransactionalMemory)]
//! pub fn copy(from: Vec<u8>, to: Vec<u8>) -> Result<(), Error> {
//!     let content = perform!(BlockStore.get(from))?;
//!     perform!(BlockStore.set(to, content.unwrap_or_default()))
//! }
//! ```
//!
//! The macro parses trait bounds using `+` syntax (e.g., `Store + Logger`).
//!
//! This expands to:
//! ```ignore
//! pub fn copy<__P: BlockStore + TransactionalMemory>(from: Vec<u8>, to: Vec<u8>) -> impl Effect<Result<(), Error>, __P> {
//!     Task(async move |__provider: &mut __P| {
//!         let content = BlockStore.get(from).perform(__provider).await?;
//!         BlockStore.set(to, content.unwrap_or_default()).perform(__provider).await
//!     })
//! }
//! ```
//!
//! # On Methods with &self
//!
//! For methods with `&self`, the closure captures `self` and returns an effect with lifetime:
//!
//! ```ignore
//! impl Cache {
//!     #[effectful(BlockStore)]
//!     fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
//!         perform!(BlockStore.get(format!("{}{}", self.prefix, key)))
//!     }
//! }
//! ```
//!
//! Expands to:
//! ```ignore
//! impl Cache {
//!     fn get<__P: BlockStore>(&self, key: Vec<u8>) -> impl Effect<Option<Vec<u8>>, __P> + '_ {
//!         Task(async move |__provider: &mut __P| {
//!             BlockStore.get(format!("{}{}", self.prefix, key)).perform(__provider).await
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
//!         perform!(BlockStore.get(key))
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
    Expr, ExprMacro, FnArg, ItemFn, Path, ReturnType, Token, TraitItemFn,
};

/// Arguments to the effectful macro: trait bounds separated by `+`
struct EffectfulArgs {
    bounds: Punctuated<Path, Token![+]>,
}

impl Parse for EffectfulArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(EffectfulArgs {
            bounds: Punctuated::parse_separated_nonempty(input)?,
        })
    }
}

/// Visitor that transforms function body for effectful methods:
/// 1. Replaces `perform!(expr)` with `expr.perform(__provider).await`
///
/// With the closure-based approach, `self` references work naturally since
/// the closure captures `self` from the outer scope.
struct PerformTransformer;

impl PerformTransformer {
    fn new(_has_self: bool) -> Self {
        Self
    }

    /// Transform a perform! macro invocation into .perform(__provider).await
    fn transform_macro(&self, mac: &syn::Macro) -> Option<Expr> {
        if mac.path.is_ident("perform") {
            let inner_tokens = &mac.tokens;
            let inner_expr: Expr = syn::parse2(inner_tokens.clone())
                .expect("perform! macro should contain a valid expression");

            Some(syn::parse_quote! {
                #inner_expr.perform(__provider).await
            })
        } else {
            None
        }
    }
}

impl VisitMut for PerformTransformer {
    fn visit_expr_mut(&mut self, expr: &mut Expr) {
        // Handle perform! macro invocations BEFORE recursing
        if let Expr::Macro(ExprMacro { mac, .. }) = expr {
            if let Some(transformed) = self.transform_macro(mac) {
                *expr = transformed;
                return;
            }
        }

        // Recurse into child expressions
        syn::visit_mut::visit_expr_mut(self, expr);
    }

    fn visit_stmt_mut(&mut self, stmt: &mut syn::Stmt) {
        // Handle macro statements BEFORE recursing: `perform!(...);`
        if let syn::Stmt::Macro(stmt_macro) = stmt {
            if let Some(transformed) = self.transform_macro(&stmt_macro.mac) {
                // Replace the macro statement with an expression statement
                *stmt = syn::Stmt::Expr(transformed, stmt_macro.semi_token);
                return;
            }
        }

        // Recurse into child statements
        syn::visit_mut::visit_stmt_mut(self, stmt);
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
        EffectfulTarget::Fn(func) => generate_effectful_function(&args, &func),
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
                generate_effectful_function(&args, &item_fn)
            }
        }
    };

    match result {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate_effectful_function(
    args: &EffectfulArgs,
    func: &ItemFn,
) -> syn::Result<TokenStream2> {
    let bounds = &args.bounds;

    // Extract function parts
    let vis = &func.vis;
    let sig = &func.sig;
    let fn_name = &sig.ident;
    let output_type = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Check if this is a method with &self or &mut self receiver
    let has_ref_self = sig.inputs.first().is_some_and(|arg| {
        matches!(arg, FnArg::Receiver(r) if r.reference.is_some())
    });

    // Handle async - if the function is marked async, we need to reject that
    if sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            sig.asyncness,
            "#[effectful] functions should not be marked async; the macro handles this",
        ));
    }

    // Check for existing generics and preserve them
    let existing_generics = &sig.generics;
    let existing_params = &existing_generics.params;
    let existing_where = existing_generics.where_clause.as_ref();

    // Build trait bounds
    let trait_bounds = bounds.iter().collect::<Vec<_>>();
    let bounds_tokens = quote! { #(#trait_bounds)+* };

    // Transform the function body - transform perform! macros and self references
    let mut body = func.block.as_ref().clone();
    let mut transformer = PerformTransformer::new(has_ref_self);
    transformer.visit_block_mut(&mut body);
    let body_stmts = &body.stmts;

    if has_ref_self {
        // Method with &self - use closure-based approach to capture self
        let generics = if existing_params.is_empty() {
            quote! { <__P: #bounds_tokens> }
        } else {
            quote! { <#existing_params, __P: #bounds_tokens> }
        };

        let where_clause = if let Some(existing) = existing_where {
            let existing_predicates = &existing.predicates;
            quote! { where #existing_predicates }
        } else {
            quote! {}
        };

        let inputs = &sig.inputs;

        // For methods with &self, we use Task with an async closure that captures self
        // The async closure can access self directly since it's captured in the closure environment
        Ok(quote! {
            #vis fn #fn_name #generics (#inputs) -> impl dialog_common::fx::Effect<#output_type, __P> + '_
            #where_clause
            {
                dialog_common::fx::Task(async move |__provider: &mut __P| {
                    #(#body_stmts)*
                })
            }
        })
    } else {
        // Free function or method without &self - use closure approach for consistency
        let generics = if existing_params.is_empty() {
            quote! { <__P: #bounds_tokens> }
        } else {
            quote! { <#existing_params, __P: #bounds_tokens> }
        };

        let where_clause = if let Some(existing) = existing_where {
            let existing_predicates = &existing.predicates;
            quote! { where #existing_predicates }
        } else {
            quote! {}
        };

        let inputs = &sig.inputs;

        // Use Task with an async closure that captures parameters
        Ok(quote! {
            #vis fn #fn_name #generics (#inputs) -> impl dialog_common::fx::Effect<#output_type, __P>
            #where_clause
            {
                dialog_common::fx::Task(async move |__provider: &mut __P| {
                    #(#body_stmts)*
                })
            }
        })
    }
}

/// Generate an effectful trait method declaration (no body, just signature with bounds)
fn generate_effectful_trait_method(
    args: &EffectfulArgs,
    method: &TraitItemFn,
) -> syn::Result<TokenStream2> {
    let bounds = &args.bounds;

    let sig = &method.sig;
    let fn_name = &sig.ident;
    let attrs = &method.attrs;
    let inputs = &sig.inputs;
    let output_type = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Check if this is a method with &self or &mut self receiver
    let has_ref_self = sig.inputs.first().is_some_and(|arg| {
        matches!(arg, FnArg::Receiver(r) if r.reference.is_some())
    });

    // Check for existing generics and preserve them
    let existing_generics = &sig.generics;
    let existing_params = &existing_generics.params;
    let existing_where = existing_generics.where_clause.as_ref();

    // Handle async - if the method is marked async, we need to reject that
    if sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            sig.asyncness,
            "#[effectful] methods should not be marked async; the macro handles this",
        ));
    }

    // Build trait bounds
    let trait_bounds = bounds.iter().collect::<Vec<_>>();
    let bounds_tokens = quote! { #(#trait_bounds)+* };

    let generics = if existing_params.is_empty() {
        quote! { <__P: #bounds_tokens> }
    } else {
        quote! { <#existing_params, __P: #bounds_tokens> }
    };

    let where_clause = if let Some(existing) = existing_where {
        let existing_predicates = &existing.predicates;
        quote! { where #existing_predicates }
    } else {
        quote! {}
    };

    // For methods with &self, add lifetime bound; otherwise just return the effect
    let return_type = if has_ref_self {
        quote! { impl dialog_common::fx::Effect<#output_type, __P> + '_ }
    } else {
        quote! { impl dialog_common::fx::Effect<#output_type, __P> }
    };

    Ok(quote! {
        #(#attrs)*
        fn #fn_name #generics (#inputs) -> #return_type
        #where_clause;
    })
}
