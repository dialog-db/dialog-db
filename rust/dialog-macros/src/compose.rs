//! `#[derive(Provider)]` macro implementation.
//!
//! Generates `Provider<Fx>` impls for composite structs where each field
//! is annotated with `#[provide(...)]` listing the effects it handles.
//!
//! # Example
//!
//! ```rust,ignore
//! #[derive(Provider)]
//! pub struct Env {
//!     #[provide(archive::Get, archive::Put)]
//!     local: FileSystem,
//!     #[provide(credential::Identify, credential::Sign)]
//!     credentials: KeyStore,
//! }
//! ```
//!
//! Generates:
//! ```rust,ignore
//! impl Provider<archive::Get> for Env
//! where FileSystem: Provider<archive::Get> { ... }
//!
//! impl Provider<archive::Put> for Env
//! where FileSystem: Provider<archive::Put> { ... }
//!
//! impl Provider<credential::Identify> for Env
//! where KeyStore: Provider<credential::Identify> { ... }
//!
//! impl Provider<credential::Sign> for Env
//! where KeyStore: Provider<credential::Sign> { ... }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

pub fn generate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match generate_compose(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct ProvideField<'a> {
    field_name: &'a syn::Ident,
    field_ty: &'a syn::Type,
    effects: Vec<syn::Path>,
    cfg_attrs: Vec<&'a syn::Attribute>,
}

fn generate_compose(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "#[derive(Provider)] requires a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "#[derive(Provider)] can only be used on structs",
            ));
        }
    };

    let provide_fields: Vec<ProvideField> = fields
        .iter()
        .filter_map(|field| {
            let field_name = field.ident.as_ref().unwrap();
            let field_ty = &field.ty;
            let cfg_attrs: Vec<_> = field
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("cfg"))
                .collect();

            // Look for #[provide(...)] attribute
            let mut effects = Vec::new();
            for attr in &field.attrs {
                if attr.path().is_ident("provide")
                    && let Ok(paths) =
                        attr.parse_args_with(|input: syn::parse::ParseStream| {
                            let paths = syn::punctuated::Punctuated::<
                                syn::Path,
                                syn::Token![,],
                            >::parse_terminated(input)?;
                            Ok(paths.into_iter().collect::<Vec<_>>())
                        })
                {
                    effects.extend(paths);
                }
            }

            if effects.is_empty() {
                return None;
            }

            Some(ProvideField {
                field_name,
                field_ty,
                effects,
                cfg_attrs,
            })
        })
        .collect();

    let existing_predicates = where_clause
        .map(|wc| {
            let predicates = &wc.predicates;
            quote! { #predicates, }
        })
        .unwrap_or_default();

    // Generate one Provider<Effect> impl per effect per field
    let mut provider_impls = Vec::new();
    for pf in &provide_fields {
        let field_name = pf.field_name;
        let field_ty = pf.field_ty;
        let cfg_attrs = &pf.cfg_attrs;

        for effect in &pf.effects {
            provider_impls.push(quote! {
                #(#cfg_attrs)*
                #[allow(clippy::absolute_paths)]
                #[cfg_attr(not(target_arch = "wasm32"), ::async_trait::async_trait)]
                #[cfg_attr(target_arch = "wasm32", ::async_trait::async_trait(?Send))]
                impl #impl_generics ::dialog_capability::Provider<#effect>
                    for #struct_name #ty_generics
                where
                    #existing_predicates
                    #effect: ::dialog_capability::Effect,
                    <#effect as ::dialog_capability::Effect>::Of: ::dialog_capability::Constraint,
                    ::dialog_capability::Capability<#effect>: ::dialog_common::ConditionalSend,
                    #field_ty: ::dialog_capability::Provider<#effect>
                        + ::dialog_common::ConditionalSync,
                    Self: ::dialog_common::ConditionalSend + ::dialog_common::ConditionalSync,
                {
                    async fn execute(
                        &self,
                        input: ::dialog_capability::Capability<#effect>,
                    ) -> <#effect as ::dialog_capability::Effect>::Output {
                        <#field_ty as ::dialog_capability::Provider<#effect>>::execute(
                            &self.#field_name, input
                        ).await
                    }
                }
            });
        }
    }

    Ok(quote! {
        #(#provider_impls)*
    })
}
