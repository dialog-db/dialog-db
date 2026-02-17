//! Router derive macro implementation.
//!
//! Generates `Provider<RemoteInvocation<Fx, Address>>` implementations for
//! composite structs whose fields each route to a different address type.
//!
//! For each field, the macro uses the [`ProviderRoute`] trait to determine
//! the address type via `<FieldType as ProviderRoute>::Address`. Fields whose
//! types don't implement `ProviderRoute` are silently skipped (the where clause
//! is unsatisfied).
//!
//! # Example
//!
//! ```rust,ignore
//! #[derive(Router)]
//! pub struct Network<Issuer> {
//!     issuer: Issuer,
//!     #[cfg(feature = "s3")]
//!     s3: Router<s3::Address, s3::Connection<Issuer>>,
//!     #[cfg(feature = "ucan")]
//!     ucan: Router<ucan::Address, ucan::Connection<Issuer>>,
//! }
//! ```
//!
//! Generates (for each field):
//!
//! ```rust,ignore
//! #[cfg(feature = "s3")]
//! impl<Fx, Issuer> Provider<RemoteInvocation<Fx, <Router<s3::Address, s3::Connection<Issuer>> as ProviderRoute>::Address>>
//!     for Network<Issuer>
//! where
//!     Router<s3::Address, s3::Connection<Issuer>>: ProviderRoute
//!         + Provider<RemoteInvocation<Fx, <Router<s3::Address, s3::Connection<Issuer>> as ProviderRoute>::Address>>,
//! {
//!     async fn execute(&mut self, input: RemoteInvocation<Fx, ...>) -> Fx::Output {
//!         self.s3.execute(input).await
//!     }
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

/// Implementation used by `#[derive(Router)]`.
pub fn generate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match generate_router(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn generate_router(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;
    let (_impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let generic_params = &input.generics.params;

    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "#[derive(Router)] requires a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "#[derive(Router)] can only be used on structs",
            ));
        }
    };

    let existing_predicates = where_clause
        .map(|wc| {
            let predicates = &wc.predicates;
            quote! { #predicates, }
        })
        .unwrap_or_default();

    let mut impls = Vec::new();

    for field in fields {
        // Skip fields annotated with #[route(skip)]
        let has_skip = field.attrs.iter().any(|attr| {
            if attr.path().is_ident("route")
                && let Ok(meta) = attr.parse_args::<syn::Ident>()
            {
                return meta == "skip";
            }
            false
        });
        if has_skip {
            continue;
        }

        let field_name = field.ident.as_ref().unwrap();
        let field_ty = &field.ty;

        // Collect #[cfg(...)] attributes from the field
        let cfg_attrs: Vec<_> = field
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("cfg"))
            .collect();

        // The address type is derived from the ProviderRoute trait
        let address_projection =
            quote! { <#field_ty as ::dialog_capability::ProviderRoute>::Address };

        let impl_block = quote! {
            #(#cfg_attrs)*
            #[cfg_attr(not(target_arch = "wasm32"), ::async_trait::async_trait)]
            #[cfg_attr(target_arch = "wasm32", ::async_trait::async_trait(?Send))]
            impl<__Fx, #generic_params>
                ::dialog_capability::Provider<
                    ::dialog_effects::remote::RemoteInvocation<__Fx, #address_projection>
                >
                for #struct_name #ty_generics
            where
                #existing_predicates
                __Fx: ::dialog_capability::Effect + ::dialog_common::ConditionalSend + 'static,
                <__Fx as ::dialog_capability::Constraint>::Capability: ::dialog_common::ConditionalSend + 'static,
                #address_projection: ::dialog_common::ConditionalSend + 'static,
                #field_ty: ::dialog_capability::ProviderRoute
                    + ::dialog_capability::Provider<
                        ::dialog_effects::remote::RemoteInvocation<__Fx, #address_projection>
                    > + ::dialog_common::ConditionalSend,
                Self: ::dialog_common::ConditionalSend,
            {
                async fn execute(
                    &mut self,
                    input: ::dialog_effects::remote::RemoteInvocation<__Fx, #address_projection>,
                ) -> __Fx::Output {
                    ::dialog_capability::Provider::execute(&mut self.#field_name, input).await
                }
            }
        };

        impls.push(impl_block);
    }

    let router_impl = quote! {
        impl<#generic_params> ::dialog_capability::Router for #struct_name #ty_generics
        where
            #existing_predicates
        {}
    };

    Ok(quote! {
        #router_impl
        #(#impls)*
    })
}
