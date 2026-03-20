//! `#[derive(Provider)]` macro implementation.
//!
//! Generates `Provider<C>` impls for composite structs where each field
//! is annotated with `#[provide(...)]` listing the commands it handles.
//!
//! Works with any `Command` type — both `Effect` types (which implement
//! `Command` via blanket impl) and explicit `Command` types like
//! `S3Invocation<archive::Get>` or `Authorize<Fx, S3Access>`.
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
//!     #[provide(S3Invocation<archive::Get>, S3Invocation<archive::Put>)]
//!     remote: S3,
//! }
//! ```
//!
//! Generates:
//! ```rust,ignore
//! impl Provider<archive::Get> for Env
//! where FileSystem: Provider<archive::Get> { ... }
//!
//! impl Provider<S3Invocation<archive::Get>> for Env
//! where S3: Provider<S3Invocation<archive::Get>> { ... }
//! // etc.
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
    commands: Vec<syn::Type>,
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

            let mut commands = Vec::new();
            for attr in &field.attrs {
                if attr.path().is_ident("provide")
                    && let Ok(types) = attr.parse_args_with(|input: syn::parse::ParseStream| {
                        let types = syn::punctuated::Punctuated::<
                                syn::Type,
                                syn::Token![,],
                            >::parse_terminated(input)?;
                        Ok(types.into_iter().collect::<Vec<_>>())
                    })
                {
                    commands.extend(types);
                }
            }

            if commands.is_empty() {
                return None;
            }

            Some(ProvideField {
                field_name,
                field_ty,
                commands,
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

    // Generate one Provider<C> impl per command per field
    let mut provider_impls = Vec::new();
    for pf in &provide_fields {
        let field_name = pf.field_name;
        let field_ty = pf.field_ty;
        let cfg_attrs = &pf.cfg_attrs;

        for command in &pf.commands {
            provider_impls.push(quote! {
                #(#cfg_attrs)*
                #[allow(clippy::absolute_paths)]
                #[cfg_attr(not(target_arch = "wasm32"), ::async_trait::async_trait)]
                #[cfg_attr(target_arch = "wasm32", ::async_trait::async_trait(?Send))]
                impl #impl_generics ::dialog_capability::Provider<#command>
                    for #struct_name #ty_generics
                where
                    #existing_predicates
                    #command: ::dialog_capability::Command,
                    <#command as ::dialog_capability::Command>::Input: ::dialog_common::ConditionalSend,
                    #field_ty: ::dialog_capability::Provider<#command>
                        + ::dialog_common::ConditionalSync,
                    Self: ::dialog_common::ConditionalSend + ::dialog_common::ConditionalSync,
                {
                    async fn execute(
                        &self,
                        input: <#command as ::dialog_capability::Command>::Input,
                    ) -> <#command as ::dialog_capability::Command>::Output {
                        <#field_ty as ::dialog_capability::Provider<#command>>::execute(
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
