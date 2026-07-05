//! `#[derive(Attenuate)]` macro implementation.
//!
//! Generates an `Attenuate` trait impl for effect types. Fields annotated
//! with `#[attenuate(into = TargetType)]` are projected via `From`
//! conversion; fields with `#[attenuate(into = Type, with = path)]` use
//! a custom function.
//!
//! Fields can also be renamed with `rename = name`.
//!
//! For types with no `#[attenuate(...)]` annotations,
//! `Attenuate::Attenuation = Self`.
//!
//! For types with annotations, a parallel `{Name}Attenuation` struct is
//! generated.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident, Type, parse_macro_input};

pub fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match generate(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// How a field is converted in the generated Attenuation struct.
enum Conversion {
    /// `<Type>::from(self.field)` — uses the `From` trait.
    From(Type),
    /// `path(self.field)` — calls a custom function.
    With(syn::ExprPath),
}

struct AttenuateField<'a> {
    ident: &'a syn::Ident,
    ty: &'a Type,
    /// The target type for this field in the Attenuation struct (if projected).
    into_ty: Option<Type>,
    /// How to convert the source field to the target type.
    conversion: Option<Conversion>,
    rename: Option<Ident>,
    attrs: Vec<&'a syn::Attribute>,
    /// A `#[serde(with = "...")]` path to attach to the projected field.
    serde_with: Option<syn::LitStr>,
    /// Whether the conversion must clone the source (a field with several
    /// projections is consumed only by its last one).
    clone_source: bool,
}

struct AttenuateAttr {
    into_ty: Type,
    with_fn: Option<syn::ExprPath>,
    rename: Option<Ident>,
    serde_with: Option<syn::LitStr>,
}

fn parse_attenuate_attr(attr: &syn::Attribute) -> syn::Result<AttenuateAttr> {
    attr.parse_args_with(|input: syn::parse::ParseStream| {
        let mut into_ty = None;
        let mut with_fn = None;
        let mut rename = None;
        let mut serde_with = None;

        loop {
            let key: syn::Ident = input.parse()?;
            let _: syn::Token![=] = input.parse()?;

            if key == "into" {
                into_ty = Some(input.parse::<Type>()?);
            } else if key == "with" {
                with_fn = Some(input.parse::<syn::ExprPath>()?);
            } else if key == "rename" {
                rename = Some(input.parse::<Ident>()?);
            } else if key == "serde_with" {
                serde_with = Some(input.parse::<syn::LitStr>()?);
            } else {
                return Err(syn::Error::new_spanned(
                    &key,
                    "expected `into`, `with`, `rename`, or `serde_with`",
                ));
            }

            if input.peek(syn::Token![,]) {
                let _: syn::Token![,] = input.parse()?;
            } else {
                break;
            }
        }

        let into_ty = into_ty.ok_or_else(|| {
            syn::Error::new(proc_macro2::Span::call_site(), "missing `into = Type`")
        })?;

        Ok(AttenuateAttr {
            into_ty,
            with_fn,
            rename,
            serde_with,
        })
    })
}

fn generate(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let vis = &input.vis;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(named) => generate_for_named_struct(
                name,
                vis,
                &impl_generics,
                &ty_generics,
                where_clause,
                named,
            ),
            syn::Fields::Unit => {
                generate_for_unit(name, &impl_generics, &ty_generics, where_clause)
            }
            syn::Fields::Unnamed(_) => {
                generate_for_unit(name, &impl_generics, &ty_generics, where_clause)
            }
        },
        _ => Err(syn::Error::new_spanned(
            name,
            "#[derive(Attenuate)] can only be used on structs",
        )),
    }
}

/// Build a where clause that includes existing predicates plus
/// `Self: Serialize + DeserializeOwned` (needed when `Attenuation = Self`).
fn identity_where_clause(
    name: &syn::Ident,
    ty_generics: &syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
) -> proc_macro2::TokenStream {
    let existing = where_clause.map(|wc| {
        let predicates = &wc.predicates;
        quote! { #predicates, }
    });
    quote! {
        where #existing #name #ty_generics: ::serde::Serialize + ::serde::de::DeserializeOwned
    }
}

fn generate_for_unit(
    name: &syn::Ident,
    impl_generics: &syn::ImplGenerics,
    ty_generics: &syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
) -> syn::Result<proc_macro2::TokenStream> {
    let wc = identity_where_clause(name, ty_generics, where_clause);
    Ok(quote! {
        impl #impl_generics ::dialog_capability::Attenuate for #name #ty_generics
        #wc
        {
            type Attenuation = Self;
            fn into_attenuation(self) -> Self { self }
        }
    })
}

fn generate_for_named_struct(
    name: &syn::Ident,
    vis: &syn::Visibility,
    impl_generics: &syn::ImplGenerics,
    ty_generics: &syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
    named: &syn::FieldsNamed,
) -> syn::Result<proc_macro2::TokenStream> {
    let mut fields = Vec::new();
    let mut has_projections = false;

    for field in &named.named {
        let ident = field.ident.as_ref().unwrap();
        let ty = &field.ty;

        let mut attenuate_attrs = Vec::new();
        let mut other_attrs = Vec::new();

        for attr in &field.attrs {
            if attr.path().is_ident("attenuate") {
                attenuate_attrs.push(parse_attenuate_attr(attr)?);
                has_projections = true;
            } else {
                other_attrs.push(attr);
            }
        }

        if attenuate_attrs.is_empty() {
            fields.push(AttenuateField {
                ident,
                ty,
                into_ty: None,
                conversion: None,
                rename: None,
                attrs: other_attrs,
                serde_with: None,
                clone_source: false,
            });
            continue;
        }

        // A field may carry several projections (e.g. a payload projected
        // to both a digest and a checksum); each then needs a distinct
        // `rename`, and every conversion but the last clones the source.
        if attenuate_attrs.len() > 1 && attenuate_attrs.iter().any(|a| a.rename.is_none()) {
            return Err(syn::Error::new_spanned(
                ident,
                "each of multiple #[attenuate(...)] projections of one field requires `rename`",
            ));
        }

        let last = attenuate_attrs.len() - 1;
        for (index, a) in attenuate_attrs.into_iter().enumerate() {
            let conv = match a.with_fn {
                Some(path) => Conversion::With(path),
                None => Conversion::From(a.into_ty.clone()),
            };
            // Projected fields get a fresh type — don't carry original attrs
            // (e.g. #[serde(with = "serde_bytes")] doesn't apply to Checksum);
            // `serde_with` re-attaches one explicitly when needed.
            fields.push(AttenuateField {
                ident,
                ty,
                into_ty: Some(a.into_ty),
                conversion: Some(conv),
                rename: a.rename,
                attrs: Vec::new(),
                serde_with: a.serde_with,
                clone_source: index < last,
            });
        }
    }

    if !has_projections {
        let wc = identity_where_clause(name, ty_generics, where_clause);
        return Ok(quote! {
            impl #impl_generics ::dialog_capability::Attenuate for #name #ty_generics
            #wc
            {
                type Attenuation = Self;
                fn into_attenuation(self) -> Self { self }
            }
        });
    }

    let attenuation_name = format_ident!("{}Attenuation", name);

    let attenuation_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let field_ident = f.rename.as_ref().unwrap_or(f.ident);
            let ty = f.into_ty.as_ref().unwrap_or(f.ty);
            let attrs = &f.attrs;
            let serde_attr = f
                .serde_with
                .as_ref()
                .map(|with| quote! { #[serde(with = #with)] });
            quote! { #(#attrs)* #serde_attr pub #field_ident: #ty }
        })
        .collect();

    let attenuation_conversions: Vec<_> = fields
        .iter()
        .map(|f| {
            let source_ident = f.ident;
            let field_ident = f.rename.as_ref().unwrap_or(f.ident);
            let source = if f.clone_source {
                quote! { self.#source_ident.clone() }
            } else {
                quote! { self.#source_ident }
            };
            match &f.conversion {
                Some(Conversion::With(path)) => {
                    quote! { #field_ident: #path(#source) }
                }
                Some(Conversion::From(into_ty)) => {
                    quote! { #field_ident: <#into_ty>::from(#source) }
                }
                None => {
                    quote! { #field_ident: #source }
                }
            }
        })
        .collect();

    Ok(quote! {
        #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize)]
        #[allow(missing_docs)]
        #vis struct #attenuation_name {
            #(#attenuation_fields,)*
        }

        impl ::dialog_capability::Attenuate for #attenuation_name {
            type Attenuation = Self;
            fn into_attenuation(self) -> Self { self }
        }

        // The generated projection struct mirrors the source's position in
        // the capability chain: same `Of` and same ability-path segment.
        // The source must be an `Attenuation` — which `Effect` implies via
        // blanket impl.
        impl ::dialog_capability::Attenuation for #attenuation_name {
            type Of = <#name as ::dialog_capability::Attenuation>::Of;
            fn attenuation() -> &'static str {
                <#name as ::dialog_capability::Attenuation>::attenuation()
            }
        }

        impl #impl_generics ::dialog_capability::Attenuate for #name #ty_generics
        #where_clause
        {
            type Attenuation = #attenuation_name;
            fn into_attenuation(self) -> #attenuation_name {
                #attenuation_name {
                    #(#attenuation_conversions,)*
                }
            }
        }
    })
}
