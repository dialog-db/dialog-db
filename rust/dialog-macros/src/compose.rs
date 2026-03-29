//! `#[derive(Provider)]` macro implementation.
//!
//! Generates `Provider<C>` impls for structs and enums.
//!
//! For **structs**, annotate fields with `#[provide(...)]`:
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
//! For **enums**, annotate the enum with `#[provide(...)]`.
//! Each variant must be a single-field tuple variant. The generated
//! impl matches on variants and delegates. `#[cfg(...)]` on variants
//! is preserved.
//!
//! ```rust,ignore
//! #[derive(Provider)]
//! #[provide(archive::Get, archive::Put, memory::Resolve)]
//! pub enum Store {
//!     #[cfg(not(target_arch = "wasm32"))]
//!     FileSystem(FileStore),
//!     Volatile(Volatile),
//! }
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

struct EnumVariant<'a> {
    variant_name: &'a syn::Ident,
    inner_ty: &'a syn::Type,
    cfg_attrs: Vec<&'a syn::Attribute>,
}

fn generate_compose(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    match &input.data {
        syn::Data::Struct(_) => generate_struct(input),
        syn::Data::Enum(_) => generate_enum(input),
        _ => Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Provider)] can only be used on structs or enums",
        )),
    }
}

fn generate_struct(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
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
        _ => unreachable!(),
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

fn parse_provide_attr(attrs: &[syn::Attribute]) -> Vec<syn::Type> {
    let mut commands = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("provide")
            && let Ok(types) = attr.parse_args_with(|input: syn::parse::ParseStream| {
                let types =
                    syn::punctuated::Punctuated::<syn::Type, syn::Token![,]>::parse_terminated(
                        input,
                    )?;
                Ok(types.into_iter().collect::<Vec<_>>())
            })
        {
            commands.extend(types);
        }
    }
    commands
}

fn generate_enum(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let enum_name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let commands = parse_provide_attr(&input.attrs);
    if commands.is_empty() {
        return Err(syn::Error::new_spanned(
            enum_name,
            "#[derive(Provider)] on enums requires #[provide(...)] on the enum",
        ));
    }

    let variants: Vec<EnumVariant> = match &input.data {
        syn::Data::Enum(data) => data
            .variants
            .iter()
            .map(|v| {
                let variant_name = &v.ident;
                let cfg_attrs: Vec<_> = v
                    .attrs
                    .iter()
                    .filter(|attr| attr.path().is_ident("cfg"))
                    .collect();

                let inner_ty = match &v.fields {
                    syn::Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                        &fields.unnamed.first().unwrap().ty
                    }
                    _ => {
                        return Err(syn::Error::new_spanned(
                            v,
                            "#[derive(Provider)] enum variants must be single-field tuples",
                        ));
                    }
                };

                Ok(EnumVariant {
                    variant_name,
                    inner_ty,
                    cfg_attrs,
                })
            })
            .collect::<Result<_, _>>()?,
        _ => unreachable!(),
    };

    let existing_predicates = where_clause
        .map(|wc| {
            let predicates = &wc.predicates;
            quote! { #predicates, }
        })
        .unwrap_or_default();

    let mut provider_impls = Vec::new();
    for command in &commands {
        let match_arms: Vec<_> = variants
            .iter()
            .map(|v| {
                let variant_name = v.variant_name;
                let inner_ty = v.inner_ty;
                let cfg_attrs = &v.cfg_attrs;
                quote! {
                    #(#cfg_attrs)*
                    Self::#variant_name(inner) => {
                        <#inner_ty as ::dialog_capability::Provider<#command>>::execute(
                            inner, input
                        ).await
                    }
                }
            })
            .collect();

        provider_impls.push(quote! {
            #[allow(clippy::absolute_paths)]
            #[cfg_attr(not(target_arch = "wasm32"), ::async_trait::async_trait)]
            #[cfg_attr(target_arch = "wasm32", ::async_trait::async_trait(?Send))]
            impl #impl_generics ::dialog_capability::Provider<#command>
                for #enum_name #ty_generics
            where
                #existing_predicates
                #command: ::dialog_capability::Command,
                <#command as ::dialog_capability::Command>::Input: ::dialog_common::ConditionalSend,
                Self: ::dialog_common::ConditionalSend + ::dialog_common::ConditionalSync,
            {
                async fn execute(
                    &self,
                    input: <#command as ::dialog_capability::Command>::Input,
                ) -> <#command as ::dialog_capability::Command>::Output {
                    match self {
                        #(#match_arms)*
                    }
                }
            }
        });
    }

    Ok(quote! {
        #(#provider_impls)*
    })
}
