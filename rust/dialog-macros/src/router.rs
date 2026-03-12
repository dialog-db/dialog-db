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
//! Additionally generates a unified `{StructName}Address` enum that combines
//! all routable field addresses into a single type, along with a
//! `ProviderRoute` impl on the struct and a unified `Provider` dispatch impl.
//!
//! # Example
//!
//! ```rust,ignore
//! #[derive(Router)]
//! pub struct Network<Issuer: Clone> {
//!     #[cfg(feature = "s3")]
//!     s3: Route<Issuer, s3::Credentials, s3::Connection<Issuer>>,
//!     #[cfg(feature = "ucan")]
//!     ucan: Route<Issuer, ucan::Credentials, ucan::Connection<Issuer>>,
//! }
//! ```
//!
//! Generates per-field `Provider<RemoteInvocation<Fx, FieldAddress>>` impls,
//! plus a unified `NetworkAddress` enum and dispatch impl.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, parse_macro_input};

/// Implementation used by `#[derive(Router)]`.
pub fn generate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match generate_router(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// A routable field extracted from the struct.
struct RoutableField<'a> {
    field_name: &'a syn::Ident,
    field_ty: &'a syn::Type,
    cfg_attrs: Vec<&'a syn::Attribute>,
    variant_name: syn::Ident,
}

/// Convert a snake_case identifier to PascalCase.
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|segment| {
            let mut chars = segment.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => {
                    let upper: String = c.to_uppercase().collect();
                    upper + chars.as_str()
                }
            }
        })
        .collect()
}

/// Extract bare generic param identifiers (without bounds) for use in
/// type positions like `PhantomData<(Issuer,)>`. `Issuer: Clone` → `Issuer`.
fn bare_generics(
    generics: &syn::Generics,
) -> (Vec<proc_macro2::TokenStream>, proc_macro2::TokenStream) {
    let bare: Vec<_> = generics
        .params
        .iter()
        .map(|p| match p {
            syn::GenericParam::Type(tp) => {
                let ident = &tp.ident;
                quote! { #ident }
            }
            syn::GenericParam::Lifetime(lp) => {
                let lt = &lp.lifetime;
                quote! { #lt }
            }
            syn::GenericParam::Const(cp) => {
                let ident = &cp.ident;
                quote! { #ident }
            }
        })
        .collect();

    let bare_ty_generics = if bare.is_empty() {
        quote! {}
    } else {
        quote! { < #(#bare),* > }
    };

    (bare, bare_ty_generics)
}

fn generate_router(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;
    let vis = &input.vis;
    let (_impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let generic_params = &input.generics.params;
    let (bare_params, bare_ty_generics) = bare_generics(&input.generics);
    let has_generics = !generic_params.is_empty();

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

    // Collect routable fields (non-skipped)
    let routable_fields: Vec<RoutableField> = fields
        .iter()
        .filter_map(|field| {
            let has_skip = field.attrs.iter().any(|attr| {
                if attr.path().is_ident("route")
                    && let Ok(meta) = attr.parse_args::<syn::Ident>()
                {
                    return meta == "skip";
                }
                false
            });
            if has_skip {
                return None;
            }

            let field_name = field.ident.as_ref().unwrap();
            let field_ty = &field.ty;
            let cfg_attrs: Vec<_> = field
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("cfg"))
                .collect();
            let variant_name = format_ident!("{}", to_pascal_case(&field_name.to_string()));

            Some(RoutableField {
                field_name,
                field_ty,
                cfg_attrs,
                variant_name,
            })
        })
        .collect();

    // Per-field Provider impls (existing behavior)
    let per_field_impls: Vec<_> = routable_fields
        .iter()
        .map(|rf| {
            let field_name = rf.field_name;
            let field_ty = rf.field_ty;
            let cfg_attrs = &rf.cfg_attrs;
            let address_projection =
                quote! { <#field_ty as ::dialog_capability::ProviderRoute>::Address };

            quote! {
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
                    Self: ::dialog_common::ConditionalSend + ::dialog_common::ConditionalSync,
                {
                    async fn execute(
                        &self,
                        input: ::dialog_effects::remote::RemoteInvocation<__Fx, #address_projection>,
                    ) -> __Fx::Output {
                        ::dialog_capability::Provider::execute(&self.#field_name, input).await
                    }
                }
            }
        })
        .collect();

    // Unified address enum name
    let address_enum_name = format_ident!("{}Address", struct_name);

    // Router trait impl with unified Address type
    let router_impl = quote! {
        impl<#generic_params> ::dialog_capability::Router for #struct_name #ty_generics
        where
            #existing_predicates
        {
            type Address = #address_enum_name #bare_ty_generics;
        }
    };

    let enum_variants: Vec<_> = routable_fields
        .iter()
        .map(|rf| {
            let variant_name = &rf.variant_name;
            let field_ty = rf.field_ty;
            let cfg_attrs = &rf.cfg_attrs;
            let address_projection =
                quote! { <#field_ty as ::dialog_capability::ProviderRoute>::Address };

            quote! {
                #(#cfg_attrs)*
                #variant_name(#address_projection)
            }
        })
        .collect();

    // PhantomData variant to suppress unused type parameter warnings
    // when all real variants are cfg'd out.
    let phantom_variant = if has_generics {
        quote! {
            #[doc(hidden)]
            #[serde(skip)]
            __Phantom(::core::marker::PhantomData<(#(#bare_params),*)>),
        }
    } else {
        quote! {}
    };

    let phantom_match_arm = if has_generics {
        quote! {
            #address_enum_name::__Phantom(_) => unreachable!(),
        }
    } else {
        quote! {}
    };

    // The enum definition uses bare generics (no bounds).
    // Where clause from the struct propagates for associated type resolution.
    let enum_where = if existing_predicates.is_empty() {
        quote! {}
    } else {
        quote! { where #existing_predicates }
    };

    let serde_bound_attr = if has_generics {
        quote! { #[serde(bound = "")] }
    } else {
        quote! {}
    };

    let address_enum = quote! {
        #[derive(
            Debug, Clone, PartialEq, Eq, Hash,
            ::serde::Serialize, ::serde::Deserialize,
        )]
        #serde_bound_attr
        #[allow(non_camel_case_types, missing_docs)]
        #vis enum #address_enum_name #bare_ty_generics #enum_where {
            #(#enum_variants,)*
            #phantom_variant
        }
    };

    // From impls for each variant
    let from_impls: Vec<_> = routable_fields
        .iter()
        .map(|rf| {
            let variant_name = &rf.variant_name;
            let field_ty = rf.field_ty;
            let cfg_attrs = &rf.cfg_attrs;
            let address_projection =
                quote! { <#field_ty as ::dialog_capability::ProviderRoute>::Address };

            quote! {
                #(#cfg_attrs)*
                impl<#generic_params> From<#address_projection>
                    for #address_enum_name #bare_ty_generics
                where
                    #existing_predicates
                    #field_ty: ::dialog_capability::ProviderRoute,
                {
                    fn from(address: #address_projection) -> Self {
                        Self::#variant_name(address)
                    }
                }
            }
        })
        .collect();

    // Unified Provider dispatch impl
    let per_field_where_clauses: Vec<_> = routable_fields
        .iter()
        .map(|rf| {
            let field_ty = rf.field_ty;
            let address_projection =
                quote! { <#field_ty as ::dialog_capability::ProviderRoute>::Address };

            quote! {
                #field_ty: ::dialog_capability::ProviderRoute
                    + ::dialog_capability::Provider<
                        ::dialog_effects::remote::RemoteInvocation<__Fx, #address_projection>
                    > + ::dialog_common::ConditionalSend,
                #address_projection: ::dialog_common::ConditionalSend + 'static,
            }
        })
        .collect();

    let match_arms: Vec<_> = routable_fields
        .iter()
        .map(|rf| {
            let variant_name = &rf.variant_name;
            let field_name = rf.field_name;
            let cfg_attrs = &rf.cfg_attrs;

            quote! {
                #(#cfg_attrs)*
                #address_enum_name::#variant_name(inner) => {
                    ::dialog_capability::Provider::execute(
                        &self.#field_name,
                        ::dialog_effects::remote::RemoteInvocation::new(capability, inner),
                    ).await
                }
            }
        })
        .collect();

    let unified_provider_impl = quote! {
        #[cfg_attr(not(target_arch = "wasm32"), ::async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", ::async_trait::async_trait(?Send))]
        impl<__Fx, #generic_params>
            ::dialog_capability::Provider<
                ::dialog_effects::remote::RemoteInvocation<__Fx, #address_enum_name #bare_ty_generics>
            >
            for #struct_name #ty_generics
        where
            #existing_predicates
            __Fx: ::dialog_capability::Effect + ::dialog_common::ConditionalSend + 'static,
            <__Fx as ::dialog_capability::Constraint>::Capability: ::dialog_common::ConditionalSend + 'static,
            #(#per_field_where_clauses)*
            #address_enum_name #bare_ty_generics: ::dialog_common::ConditionalSend + 'static,
            Self: ::dialog_common::ConditionalSend + ::dialog_common::ConditionalSync,
        {
            async fn execute(
                &self,
                input: ::dialog_effects::remote::RemoteInvocation<__Fx, #address_enum_name #bare_ty_generics>,
            ) -> __Fx::Output {
                let (capability, address) = input.into_parts();
                match address {
                    #(#match_arms)*
                    #phantom_match_arm
                }
            }
        }
    };

    Ok(quote! {
        #router_impl
        #(#per_field_impls)*
        #address_enum
        #(#from_impls)*
        #unified_provider_impl
    })
}

#[cfg(test)]
mod tests {
    use super::to_pascal_case;

    #[test]
    fn pascal_case_simple() {
        assert_eq!(to_pascal_case("s3"), "S3");
        assert_eq!(to_pascal_case("ucan"), "Ucan");
        assert_eq!(to_pascal_case("my_field"), "MyField");
        assert_eq!(to_pascal_case("a_b_c"), "ABC");
        assert_eq!(to_pascal_case("hello_world"), "HelloWorld");
    }
}
