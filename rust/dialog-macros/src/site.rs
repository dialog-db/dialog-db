//! `#[derive(Site)]` macro implementation.
//!
//! Generates composite [`Site`](dialog_capability::Site) types from a struct
//! of site fields. Each field must be a type that implements
//! `dialog_capability::Site`. The macro derives address, authorization, and
//! claim enums from the associated types, plus all dispatch boilerplate.
//!
//! # Generated types and impls
//!
//! Given:
//! ```rust,ignore
//! #[derive(Router)]
//! pub struct Network {
//!     s3: S3,
//!     ucan: UcanSite,
//! }
//! ```
//!
//! The macro generates:
//! - `NetworkAddress` enum (variants from field names, types from `<Site>::Address`)
//! - `NetworkAuthorization` enum (from `<Site>::Authorization`)
//! - `NetworkClaim<Fx>` enum (from `<Site>::Claim<Fx>`)
//! - `impl Site for Network`
//! - `impl SiteAddress for NetworkAddress`
//! - `From<VariantAddr>` impls via a `FromSiteAddress` helper trait (a bare
//!   `impl From<<S as Site>::Address> for Address` would hit cross-crate
//!   coherence issues with associated-type projections)
//! - `From<NetworkAddress> for SiteId`
//! - `From<(Capability<Fx>, SiteIssuer, NetworkAddress)> for NetworkClaim<Fx>`
//! - `Acquire<Env> for NetworkClaim<Fx>`
//! - `Provider<ForkInvocation<Network, Fx>> for Network`
//!
//! # Limitations
//!
//! `#[cfg]`-gated fields are not currently supported because the generated
//! `Acquire` and `Provider` impls need per-variant bounds in their `where`
//! clauses, and attributes in where clauses are still unstable on Rust
//! (rust#115590).

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, parse_macro_input};

pub fn generate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match generate_site(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

struct Field<'a> {
    field_name: &'a syn::Ident,
    field_ty: &'a syn::Type,
    variant_name: syn::Ident,
    cfg_attrs: Vec<&'a syn::Attribute>,
}

fn generate_site(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let struct_name = &input.ident;
    let vis = &input.vis;

    let data = match &input.data {
        syn::Data::Struct(data) => data,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "Site can only be derived on structs",
            ));
        }
    };

    let named_fields = match &data.fields {
        syn::Fields::Named(named) => &named.named,
        _ => {
            return Err(syn::Error::new_spanned(input, "Site requires named fields"));
        }
    };

    let fields: Vec<Field> = named_fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap();
            let variant_name = format_ident!("{}", field_name.to_string().to_case(Case::Pascal));
            let cfg_attrs: Vec<_> = f
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("cfg"))
                .collect();
            // We currently can't support `#[cfg]`-gated fields because the
            // generated `Acquire` and `Provider` impls need per-variant
            // bounds in their `where` clauses, and attributes inside `where`
            // clauses are still unstable (rust#115590).
            if !cfg_attrs.is_empty() {
                return Err(syn::Error::new_spanned(
                    f,
                    "`#[derive(Site)]` does not yet support `#[cfg]`-gated \
                     fields. See rust#115590.",
                ));
            }
            Ok(Field {
                field_name,
                field_ty: &f.ty,
                variant_name,
                cfg_attrs,
            })
        })
        .collect::<syn::Result<_>>()?;

    let address_enum_name = format_ident!("{}Address", struct_name);
    let auth_enum_name = format_ident!("{}Authorization", struct_name);
    let claim_enum_name = format_ident!("{}Claim", struct_name);
    // Helper trait used to implement `From<A> for {Struct}Address` without
    // running into coherence errors on associated type projections.
    let from_site_address_trait_name = format_ident!("__{}FromSiteAddress", struct_name);

    // Per-field type projections
    let per_field: Vec<_> = fields
        .iter()
        .map(|f| {
            let site_ty = &f.field_ty;
            let addr_ty = quote! { <#site_ty as ::dialog_capability::Site>::Address };
            let auth_ty = quote! { <#site_ty as ::dialog_capability::Site>::Authorization };
            (f, addr_ty, auth_ty)
        })
        .collect();

    // Site type aliases (avoids associated type projections in async_trait)
    let site_alias_names: Vec<_> = fields
        .iter()
        .map(|f| format_ident!("__{}SiteAlias", f.variant_name))
        .collect();

    let site_alias_defs: Vec<_> = site_alias_names
        .iter()
        .zip(fields.iter())
        .map(|(alias, f)| {
            let cfgs = &f.cfg_attrs;
            let site_ty = f.field_ty;
            quote! {
                #(#cfgs)*
                type #alias = #site_ty;
            }
        })
        .collect();

    // Address enum variants
    let addr_variants: Vec<_> = per_field
        .iter()
        .map(|(f, addr_ty, _)| {
            let cfgs = &f.cfg_attrs;
            let vname = &f.variant_name;
            quote! {
                #(#cfgs)*
                #[doc = concat!(stringify!(#vname), " address.")]
                #vname(#addr_ty),
            }
        })
        .collect();

    // Authorization enum variants
    let auth_variants: Vec<_> = per_field
        .iter()
        .map(|(f, _, auth_ty)| {
            let cfgs = &f.cfg_attrs;
            let vname = &f.variant_name;
            quote! {
                #(#cfgs)*
                #[doc = concat!(stringify!(#vname), " authorization.")]
                #vname(#auth_ty),
            }
        })
        .collect();

    // Claim enum variants (uses aliases to avoid GATs in async_trait)
    let claim_alias_names: Vec<_> = fields
        .iter()
        .map(|f| format_ident!("__{}ClaimAlias", f.variant_name))
        .collect();

    let claim_alias_defs: Vec<_> = claim_alias_names
        .iter()
        .zip(site_alias_names.iter())
        .zip(fields.iter())
        .map(|((alias, site_alias), f)| {
            let cfgs = &f.cfg_attrs;
            quote! {
                #(#cfgs)*
                type #alias<Fx> = <#site_alias as ::dialog_capability::Site>::Claim<Fx>;
            }
        })
        .collect();

    let claim_variants: Vec<_> = claim_alias_names
        .iter()
        .zip(fields.iter())
        .map(|(alias, f)| {
            let cfgs = &f.cfg_attrs;
            let vname = &f.variant_name;
            quote! {
                #(#cfgs)*
                #[doc = concat!(stringify!(#vname), " claim.")]
                #vname(#alias<Fx>),
            }
        })
        .collect();

    // FromSiteAddress<S> impls per variant. Each impl has a distinct header
    // (parameterized by the concrete site type), so they don't conflict with
    // each other or with the blanket From<T> for T. This works around a
    // coherence limitation: direct `impl From<<S as Site>::Address> for Address`
    // would fail E0119 because the compiler can't prove projections across
    // crates don't overlap.
    let from_site_addr_impls: Vec<_> = per_field
        .iter()
        .map(|(f, _, _)| {
            let cfgs = &f.cfg_attrs;
            let vname = &f.variant_name;
            let site_ty = f.field_ty;
            quote! {
                #(#cfgs)*
                impl #from_site_address_trait_name<#site_ty> for #address_enum_name {
                    fn from_site_address(
                        address: <#site_ty as ::dialog_capability::Site>::Address,
                    ) -> #address_enum_name {
                        #address_enum_name::#vname(address)
                    }
                }
            }
        })
        .collect();

    // From<Address> for SiteId
    let site_id_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let cfgs = &f.cfg_attrs;
            let vname = &f.variant_name;
            quote! {
                #(#cfgs)*
                #address_enum_name::#vname(addr) => addr.into(),
            }
        })
        .collect();

    // From<(Capability, SiteIssuer, Address)> for Claim
    let claim_from_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let cfgs = &f.cfg_attrs;
            let vname = &f.variant_name;
            quote! {
                #(#cfgs)*
                #address_enum_name::#vname(addr) => {
                    Self::#vname((capability, issuer, addr).into())
                }
            }
        })
        .collect();

    // Acquire where-clause bounds, one per variant.
    //
    // Note: this does NOT currently support `#[cfg]`-gated fields because
    // attributes inside where clauses are unstable on Rust (rust#115590).
    // If you add a cfg-gated field to a `#[derive(Site)]` struct, expect a
    // build error on stable. A future refactor can route these bounds
    // through a marker trait whose impls are cfg-gated at item level.
    let acquire_bound_trait_defs: Vec<proc_macro2::TokenStream> = Vec::new();
    let acquire_bounds: Vec<_> = claim_alias_names
        .iter()
        .zip(site_alias_names.iter())
        .map(|(alias, site_alias)| {
            quote! {
                #alias<Fx>: ::dialog_capability::fork::Acquire<
                    Env, Site = #site_alias, Effect = Fx,
                > + ::dialog_common::ConditionalSend,
            }
        })
        .collect();

    let acquire_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let cfgs = &f.cfg_attrs;
            let vname = &f.variant_name;
            quote! {
                #(#cfgs)*
                Self::#vname(claim) => {
                    let invocation = claim.perform(env).await?;
                    Ok(::dialog_capability::ForkInvocation::new(
                        invocation.capability,
                        #address_enum_name::#vname(invocation.address),
                        #auth_enum_name::#vname(invocation.authorization),
                    ))
                }
            }
        })
        .collect();

    // Dispatch (`Provider<ForkInvocation>`) where-clause bounds.
    // Same cfg limitation as `acquire_bounds` above.
    let dispatch_bound_trait_defs: Vec<proc_macro2::TokenStream> = Vec::new();
    let dispatch_bounds: Vec<_> = site_alias_names
        .iter()
        .map(|site_alias| {
            quote! {
                ::dialog_capability::ForkInvocation<#site_alias, Fx>:
                    ::dialog_common::ConditionalSend,
                #site_alias: ::dialog_capability::Provider<
                    ::dialog_capability::ForkInvocation<#site_alias, Fx>,
                >,
            }
        })
        .collect();

    let dispatch_arms: Vec<_> = fields
        .iter()
        .map(|f| {
            let cfgs = &f.cfg_attrs;
            let vname = &f.variant_name;
            let fname = f.field_name;
            quote! {
                #(#cfgs)*
                (#auth_enum_name::#vname(auth), #address_enum_name::#vname(addr)) => {
                    ::dialog_capability::ForkInvocation::new(capability, addr, auth)
                        .perform(&self.#fname)
                        .await
                }
            }
        })
        .collect();

    Ok(quote! {
        /// Composite address for this site's transports.
        ///
        /// Generated by `#[derive(Site)]` from the fields of
        #[doc = concat!("[`", stringify!(#struct_name), "`].")]
        #[derive(
            Debug, Clone, Hash,
            ::serde::Serialize, ::serde::Deserialize,
            PartialEq, Eq,
        )]
        #vis enum #address_enum_name {
            #(#addr_variants)*
        }

        // Helper trait: one impl per variant with a distinct header
        // (parameterized by the concrete site type). See `from_site_addr_impls`.
        #[doc(hidden)]
        trait #from_site_address_trait_name<S: ::dialog_capability::Site> {
            fn from_site_address(
                address: <S as ::dialog_capability::Site>::Address,
            ) -> #address_enum_name;
        }

        #(#from_site_addr_impls)*

        // Single blanket `From<A>` that dispatches through the helper trait.
        // The `A::Site` discriminator picks the right variant impl.
        impl<__A> ::core::convert::From<__A> for #address_enum_name
        where
            __A: ::dialog_capability::SiteAddress,
            #address_enum_name: #from_site_address_trait_name<<__A as ::dialog_capability::SiteAddress>::Site>,
        {
            fn from(address: __A) -> #address_enum_name {
                <#address_enum_name as #from_site_address_trait_name<
                    <__A as ::dialog_capability::SiteAddress>::Site,
                >>::from_site_address(address)
            }
        }

        impl ::core::convert::From<#address_enum_name> for ::dialog_capability::SiteId {
            fn from(address: #address_enum_name) -> Self {
                match address {
                    #(#site_id_arms)*
                }
            }
        }

        impl ::dialog_capability::SiteAddress for #address_enum_name {
            type Site = #struct_name;
        }

        /// Composite authorization material.
        #[derive(Debug, Clone)]
        #vis enum #auth_enum_name {
            #(#auth_variants)*
        }

        /// Composite claim for authorization.
        #vis enum #claim_enum_name<Fx: ::dialog_capability::Effect> {
            #(#claim_variants)*
        }

        impl<Fx: ::dialog_capability::Effect> ::core::convert::From<(
            ::dialog_capability::Capability<Fx>,
            ::dialog_capability::SiteIssuer,
            #address_enum_name,
        )> for #claim_enum_name<Fx> {
            fn from(
                (capability, issuer, address): (
                    ::dialog_capability::Capability<Fx>,
                    ::dialog_capability::SiteIssuer,
                    #address_enum_name,
                ),
            ) -> Self {
                match address {
                    #(#claim_from_arms)*
                }
            }
        }

        impl ::dialog_capability::Site for #struct_name {
            type Authorization = #auth_enum_name;
            type Address = #address_enum_name;
            type Claim<Fx: ::dialog_capability::Effect> = #claim_enum_name<Fx>;
        }

        #(#site_alias_defs)*
        #(#claim_alias_defs)*

        #(#acquire_bound_trait_defs)*

        #[cfg_attr(not(target_arch = "wasm32"), ::async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", ::async_trait::async_trait(?Send))]
        impl<Fx, Env> ::dialog_capability::fork::Acquire<Env> for #claim_enum_name<Fx>
        where
            Fx: ::dialog_capability::Effect
                + Clone
                + ::dialog_common::ConditionalSend
                + ::dialog_common::ConditionalSync
                + 'static,
            <Fx as ::dialog_capability::Effect>::Of: ::dialog_capability::Constraint<
                Capability: ::dialog_common::ConditionalSend
                    + ::dialog_common::ConditionalSync,
            >,
            ::dialog_capability::Capability<Fx>: ::dialog_capability::Ability
                + ::dialog_common::ConditionalSend
                + ::dialog_common::ConditionalSync,
            #(#acquire_bounds)*
            Env: ::dialog_common::ConditionalSync,
        {
            type Site = #struct_name;
            type Effect = Fx;

            async fn perform(
                self,
                env: &Env,
            ) -> ::core::result::Result<
                ::dialog_capability::ForkInvocation<#struct_name, Fx>,
                ::dialog_capability::AuthorizeError,
            > {
                match self {
                    #(#acquire_arms)*
                }
            }
        }

        #(#dispatch_bound_trait_defs)*

        #[cfg_attr(not(target_arch = "wasm32"), ::async_trait::async_trait)]
        #[cfg_attr(target_arch = "wasm32", ::async_trait::async_trait(?Send))]
        impl<Fx> ::dialog_capability::Provider<
            ::dialog_capability::ForkInvocation<#struct_name, Fx>,
        > for #struct_name
        where
            Fx: ::dialog_capability::Effect
                + ::dialog_common::ConditionalSend
                + ::dialog_common::ConditionalSync
                + 'static,
            <Fx as ::dialog_capability::Effect>::Of: ::dialog_capability::Constraint<
                Capability: ::dialog_common::ConditionalSend
                    + ::dialog_common::ConditionalSync,
            >,
            #(#dispatch_bounds)*
            Self: ::dialog_common::ConditionalSend + ::dialog_common::ConditionalSync,
        {
            async fn execute(
                &self,
                input: ::dialog_capability::ForkInvocation<#struct_name, Fx>,
            ) -> <Fx as ::dialog_capability::Effect>::Output {
                let ::dialog_capability::ForkInvocation {
                    capability,
                    address,
                    authorization,
                } = input;
                match (authorization, address) {
                    #(#dispatch_arms)*
                    _ => unreachable!("authorization/address type mismatch"),
                }
            }
        }
    })
}
