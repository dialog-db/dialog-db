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
//! - Free functions `get()`, `set()` that return request structs
//! - `Get`, `Set` request structs
//! - `Request` enum (request types)
//! - `Response` enum (response types)
//! - `Command` struct implementing `Capability`
//! - `Effect` impls for each request struct (using From/TryFrom for composition)
//! - `From<Request>` impl for the module's Request type
//! - `TryFrom<Response>` for extracting responses
//! - Blanket `Provider` impl for `Arc<Mutex<T>>` where `T: TraitName`
//!
//! For traits with supertraits (composition):
//! ```ignore
//! #[effect]
//! pub trait Env: BlockStore + TransactionalMemory {}
//! ```
//! The macro combines Request/Response enums from all supertraits and generates
//! tuple Provider impls.
//!
//! Usage:
//! ```ignore
//! // Implement the trait
//! impl BlockStore for MyBackend {
//!     async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> { ... }
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) { ... }
//! }
//!
//! // Use effects
//! BlockStore::get(key).perform(&co).await
//! BlockStore::set(key, value).perform(&co).await
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
    let request_structs = methods.iter().map(generate_request_struct);
    let request_enum = generate_request_enum(&methods, &supertraits);
    let response_enum = generate_response_enum(&methods, &supertraits);
    let from_impls = generate_from_impls(&methods, &supertraits);
    let command_struct = generate_command_struct();
    let effect_impls = methods.iter().map(generate_effect_impl);
    let provider_impl = generate_provider_impl(&methods, trait_name, &supertraits);
    let tuple_provider_impls = generate_tuple_provider_impls(&supertraits);

    Ok(quote! {
        #[allow(async_fn_in_trait)]
        #[allow(non_snake_case)]
        #trait_vis mod #trait_name {
            use super::*;

            #provider_trait

            // Free functions that return request structs
            #(#free_functions)*

            // Request structs
            #(#request_structs)*

            // Request enum
            #request_enum

            // Response enum
            #response_enum

            // From/TryFrom impls for composition
            #from_impls

            // Command struct (implements Capability)
            #command_struct

            // Effect impls
            #(#effect_impls)*

            // Blanket Provider impl
            #provider_impl

            // Tuple Provider impls for composition
            #tuple_provider_impls
        }
    })
}

struct MethodInfo {
    method_name: Ident,
    struct_name: Ident,
    params: Vec<(Ident, Type)>,
    output_type: Type,
    is_mut: bool,
}

fn parse_method_info(method: &TraitItemFn) -> syn::Result<MethodInfo> {
    let method_name = method.sig.ident.clone();

    // Create struct name: MethodName in PascalCase (e.g., Get, Set)
    let struct_name = format_ident!("{}", to_pascal_case(&method_name.to_string()));

    // Parse parameters (skip self)
    let mut params = Vec::new();
    let mut is_mut = false;

    for arg in &method.sig.inputs {
        match arg {
            FnArg::Receiver(r) => {
                is_mut = r.mutability.is_some();
            }
            FnArg::Typed(pat_type) => {
                if let Pat::Ident(pat_ident) = &*pat_type.pat {
                    params.push((pat_ident.ident.clone(), (*pat_type.ty).clone()));
                }
            }
        }
    }

    // Parse return type
    let output_type = match &method.sig.output {
        ReturnType::Default => syn::parse_quote!(()),
        ReturnType::Type(_, ty) => (**ty).clone(),
    };

    Ok(MethodInfo {
        method_name,
        struct_name,
        params,
        output_type,
        is_mut,
    })
}

fn generate_provider_trait(
    trait_def: &ItemTrait,
    trait_name: &Ident,
    supertraits: &[syn::Path],
) -> TokenStream2 {
    let trait_vis = &trait_def.vis;
    let generics = &trait_def.generics;
    let items = &trait_def.items;

    // Build supertraits: all effect supertraits (referencing their inner trait)
    let supertrait_refs = supertraits.iter().map(|path| {
        quote! { #path::#path }
    });

    if supertraits.is_empty() {
        quote! {
            #trait_vis trait #trait_name #generics {
                #(#items)*
            }
        }
    } else {
        quote! {
            #trait_vis trait #trait_name #generics : #(#supertrait_refs)+* {
                #(#items)*
            }
        }
    }
}

fn generate_free_function(method: &MethodInfo) -> TokenStream2 {
    let method_name = &method.method_name;
    let struct_name = &method.struct_name;

    let params = method.params.iter().map(|(name, ty)| {
        quote! { #name: #ty }
    });

    let field_inits = method.params.iter().map(|(name, _)| {
        quote! { #name }
    });

    quote! {
        pub fn #method_name(#(#params),*) -> #struct_name {
            #struct_name { #(#field_inits),* }
        }
    }
}

fn generate_request_struct(method: &MethodInfo) -> TokenStream2 {
    let struct_name = &method.struct_name;
    let output_type = &method.output_type;

    let fields = method.params.iter().map(|(name, ty)| {
        quote! { pub #name: #ty }
    });

    quote! {
        #[derive(Debug, Clone)]
        pub struct #struct_name {
            #(#fields),*
        }

        impl #struct_name {
            /// Wrap the result in the appropriate Response variant.
            pub fn respond(result: #output_type) -> Response {
                Response::#struct_name(result)
            }
        }
    }
}

fn generate_request_enum(methods: &[MethodInfo], supertraits: &[syn::Path]) -> TokenStream2 {
    // Variants for this trait's own methods
    let method_variants = methods.iter().map(|m| {
        let struct_name = &m.struct_name;
        quote! { #struct_name(#struct_name) }
    });

    // Variants for supertrait requests
    let supertrait_variants = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! { #variant_name(#path::Request) }
    });

    quote! {
        #[derive(Debug, Clone)]
        pub enum Request {
            #(#method_variants,)*
            #(#supertrait_variants),*
        }
    }
}

fn generate_response_enum(methods: &[MethodInfo], supertraits: &[syn::Path]) -> TokenStream2 {
    // Variants for this trait's own methods
    let method_variants = methods.iter().map(|m| {
        let struct_name = &m.struct_name;
        let output_ty = &m.output_type;
        quote! { #struct_name(#output_ty) }
    });

    // Variants for supertrait responses
    let supertrait_variants = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! { #variant_name(#path::Response) }
    });

    // Default impl - use Init if no supertraits, otherwise delegate to first supertrait
    let default_impl = if supertraits.is_empty() {
        quote! {
            impl Default for Response {
                fn default() -> Self {
                    Response::Init
                }
            }
        }
    } else {
        let first_supertrait = &supertraits[0];
        let first_variant = first_supertrait.segments.last().unwrap().ident.clone();
        quote! {
            impl Default for Response {
                fn default() -> Self {
                    Response::#first_variant(#first_supertrait::Response::default())
                }
            }
        }
    };

    // Only include Init variant if there are no supertraits
    let init_variant = if supertraits.is_empty() {
        quote! { Init, }
    } else {
        quote! {}
    };

    quote! {
        #[derive(Debug, Clone)]
        pub enum Response {
            #init_variant
            #(#method_variants,)*
            #(#supertrait_variants),*
        }

        #default_impl
    }
}

fn generate_from_impls(methods: &[MethodInfo], supertraits: &[syn::Path]) -> TokenStream2 {
    // From<OwnRequest> for Request (for own methods)
    let own_request_from_impls = methods.iter().map(|m| {
        let struct_name = &m.struct_name;
        quote! {
            impl From<#struct_name> for Request {
                fn from(req: #struct_name) -> Self {
                    Request::#struct_name(req)
                }
            }
        }
    });

    // From<Supertrait::Request> for Request
    let supertrait_request_from_impls = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! {
            impl From<#path::Request> for Request {
                fn from(req: #path::Request) -> Self {
                    Request::#variant_name(req)
                }
            }
        }
    });

    // TryFrom<Response> for Supertrait::Response
    let supertrait_response_try_from_impls = supertraits.iter().map(|path| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        quote! {
            impl TryFrom<Response> for #path::Response {
                type Error = Response;
                fn try_from(resp: Response) -> Result<Self, Self::Error> {
                    match resp {
                        Response::#variant_name(inner) => Ok(inner),
                        other => Err(other),
                    }
                }
            }
        }
    });

    quote! {
        #(#own_request_from_impls)*
        #(#supertrait_request_from_impls)*
        #(#supertrait_response_try_from_impls)*
    }
}

fn generate_command_struct() -> TokenStream2 {
    quote! {
        pub struct Command;

        impl dialog_common::fx::Capability for Command {
            type Request = Request;
            type Response = Response;
        }
    }
}

fn generate_effect_impl(method: &MethodInfo) -> TokenStream2 {
    let struct_name = &method.struct_name;
    let output_type = &method.output_type;

    // Generate Effect impl that works with any Request/Response types via From/TryFrom
    // The impl works in two ways:
    // 1. Direct: Req: From<StructName> (for the defining module's Request type)
    // 2. Indirect: Req: From<Request> where Request: From<StructName> (for composite modules)
    quote! {
        impl<Req, Resp> dialog_common::fx::Effect<#output_type, Req, Resp> for #struct_name
        where
            Req: From<Request>,
            Resp: TryInto<Response>,
        {
            async fn perform<P>(self, provider: &P) -> #output_type
            where
                P: dialog_common::fx::Provider<Request = Req, Response = Resp>,
            {
                // Convert through the module's Request type
                let module_req: Request = self.into();
                let req: Req = module_req.into();
                let resp = provider.provide(req).await;
                match resp.try_into() {
                    Ok(Response::#struct_name(result)) => result,
                    _ => unreachable!("Provider returned wrong response variant"),
                }
            }
        }
    }
}

fn generate_provider_impl(
    methods: &[MethodInfo],
    trait_name: &Ident,
    _supertraits: &[syn::Path],
) -> TokenStream2 {
    // If there are no methods and only supertraits, don't generate a dispatch function
    if methods.is_empty() {
        return quote! {};
    }

    // Generate match arms for own methods
    let method_arms = methods.iter().map(|m| {
        let method_name = &m.method_name;
        let struct_name = &m.struct_name;

        let field_names: Vec<_> = m.params.iter().map(|(name, _)| name).collect();
        let call_args = field_names.iter().map(|name| quote! { req.#name });

        if m.is_mut {
            quote! {
                Request::#struct_name(req) => {
                    #struct_name::respond(backend.#method_name(#(#call_args),*).await)
                }
            }
        } else {
            quote! {
                Request::#struct_name(req) => {
                    #struct_name::respond(backend.#method_name(#(#call_args),*).await)
                }
            }
        }
    });

    // Generate a dispatch function that users can call to implement their own Provider
    quote! {
        /// Dispatch a request to the backend. Used to implement Provider.
        pub async fn dispatch<T>(backend: &mut T, request: Request) -> Response
        where
            T: #trait_name,
        {
            match request {
                #(#method_arms)*
            }
        }
    }
}

fn generate_tuple_provider_impls(supertraits: &[syn::Path]) -> TokenStream2 {
    if supertraits.is_empty() {
        return quote! {};
    }

    // Generate a dispatch function for composite providers instead of implementing Provider for tuples
    // (implementing Provider for tuples would violate the orphan rule)
    let len = supertraits.len();

    // Type parameters: P0, P1, P2, ...
    let type_params: Vec<_> = (0..len).map(|i| format_ident!("P{}", i)).collect();

    // Where clauses: P0: Provider<Request = S0::Request, Response = S0::Response>
    let where_clauses = supertraits.iter().zip(type_params.iter()).map(|(path, param)| {
        quote! {
            #param: dialog_common::fx::Provider<Request = #path::Request, Response = #path::Response>
        }
    });

    // Match arms for each supertrait
    let match_arms = supertraits.iter().enumerate().map(|(i, path)| {
        let variant_name = path.segments.last().unwrap().ident.clone();
        let idx = syn::Index::from(i);
        quote! {
            Request::#variant_name(req) => {
                Response::#variant_name(providers.#idx.provide(req).await)
            }
        }
    });

    let tuple_type = quote! { (#(#type_params),*) };

    quote! {
        /// Dispatch a composite request to the appropriate provider.
        /// Users should wrap this in their own Provider implementation.
        pub async fn dispatch_composite<#(#type_params),*>(
            providers: &#tuple_type,
            request: Request,
        ) -> Response
        where
            #(#where_clauses),*
        {
            match request {
                #(#match_arms)*
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
