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
}

struct AttenuateAttr {
    into_ty: Type,
    with_fn: Option<syn::ExprPath>,
    rename: Option<Ident>,
}

fn parse_attenuate_attr(attr: &syn::Attribute) -> syn::Result<AttenuateAttr> {
    attr.parse_args_with(|input: syn::parse::ParseStream| {
        let mut into_ty = None;
        let mut with_fn = None;
        let mut rename = None;

        loop {
            let key: syn::Ident = input.parse()?;
            let _: syn::Token![=] = input.parse()?;

            if key == "into" {
                into_ty = Some(input.parse::<Type>()?);
            } else if key == "with" {
                with_fn = Some(input.parse::<syn::ExprPath>()?);
            } else if key == "rename" {
                rename = Some(input.parse::<Ident>()?);
            } else {
                return Err(syn::Error::new_spanned(
                    &key,
                    "expected `into`, `with`, or `rename`",
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
            fn attenuate(self) -> Self { self }
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

        let mut attenuate_attr = None;
        let mut other_attrs = Vec::new();

        for attr in &field.attrs {
            if attr.path().is_ident("attenuate") {
                attenuate_attr = Some(parse_attenuate_attr(attr)?);
                has_projections = true;
            } else {
                other_attrs.push(attr);
            }
        }

        let (into_ty, conversion, rename, field_attrs) = match attenuate_attr {
            Some(a) => {
                let conv = match a.with_fn {
                    Some(path) => Conversion::With(path),
                    None => Conversion::From(a.into_ty.clone()),
                };
                // Projected fields get a fresh type — don't carry original attrs
                // (e.g. #[serde(with = "serde_bytes")] doesn't apply to Checksum)
                (Some(a.into_ty), Some(conv), a.rename, Vec::new())
            }
            None => (None, None, None, other_attrs),
        };

        fields.push(AttenuateField {
            ident,
            ty,
            into_ty,
            conversion,
            rename,
            attrs: field_attrs,
        });
    }

    if !has_projections {
        let wc = identity_where_clause(name, ty_generics, where_clause);
        return Ok(quote! {
            impl #impl_generics ::dialog_capability::Attenuate for #name #ty_generics
            #wc
            {
                type Attenuation = Self;
                fn attenuate(self) -> Self { self }
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
            quote! { #(#attrs)* pub #field_ident: #ty }
        })
        .collect();

    let attenuation_conversions: Vec<_> = fields
        .iter()
        .map(|f| {
            let source_ident = f.ident;
            let field_ident = f.rename.as_ref().unwrap_or(f.ident);
            match &f.conversion {
                Some(Conversion::With(path)) => {
                    quote! { #field_ident: #path(self.#source_ident) }
                }
                Some(Conversion::From(into_ty)) => {
                    quote! { #field_ident: <#into_ty>::from(self.#source_ident) }
                }
                None => {
                    quote! { #field_ident: self.#source_ident }
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
            fn attenuate(self) -> Self { self }
        }

        impl #impl_generics ::dialog_capability::Attenuate for #name #ty_generics
        #where_clause
        {
            type Attenuation = #attenuation_name;
            fn attenuate(self) -> #attenuation_name {
                #attenuation_name {
                    #(#attenuation_conversions,)*
                }
            }
        }
    })
}
