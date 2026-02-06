//! Attribute derive macro implementation

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use super::helpers::{
    extract_doc_comments, parse_cardinality_attribute, parse_namespace_attribute, to_kebab_case,
};

pub fn derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;

    // Parse tuple struct with single field
    let wrapped_type = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                &fields.unnamed.first().unwrap().ty
            }
            Fields::Unnamed(_) => {
                return syn::Error::new_spanned(
                    &input,
                    "Attribute can only be derived for tuple structs with exactly one field",
                )
                .to_compile_error()
                .into();
            }
            _ => {
                return syn::Error::new_spanned(
                    &input,
                    "Attribute can only be derived for tuple structs (e.g., struct Name(String))",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &input,
                "Attribute can only be derived for tuple structs",
            )
            .to_compile_error()
            .into();
        }
    };

    // Check if namespace is explicitly specified
    let explicit_namespace = parse_namespace_attribute(&input.attrs);

    // Extract attribute name (convert PascalCase/snake_case to kebab-case)
    let attr_name = to_kebab_case(&struct_name.to_string());
    let attr_name_lit = syn::LitStr::new(&attr_name, proc_macro2::Span::call_site());

    // Extract doc comments
    let description = extract_doc_comments(&input.attrs);
    let description_lit = syn::LitStr::new(&description, proc_macro2::Span::call_site());

    // Parse cardinality
    let cardinality = parse_cardinality_attribute(&input.attrs);

    // Generate namespace static names (unique per struct)
    let compute_len_name = syn::Ident::new(
        &format!(
            "__compute_{}_namespace_len",
            struct_name.to_string().to_lowercase()
        ),
        struct_name.span(),
    );
    let compute_bytes_name = syn::Ident::new(
        &format!(
            "__compute_{}_namespace_bytes",
            struct_name.to_string().to_lowercase()
        ),
        struct_name.span(),
    );
    let namespace_len_name = syn::Ident::new(
        &format!("__{}_NAMESPACE_LEN", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let namespace_bytes_name = syn::Ident::new(
        &format!("{}_NAMESPACE_BYTES", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let namespace_name = syn::Ident::new(
        &format!("{}_NAMESPACE", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Generate additional const name for module path
    let module_path_const_name = syn::Ident::new(
        &format!("__{}_MODULE_PATH", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Generate namespace - explicit or derived
    let (namespace_static_decl, namespace_expr) = if let Some(ref ns) = explicit_namespace {
        let ns_lit = syn::LitStr::new(ns, proc_macro2::Span::call_site());
        (quote! {}, quote! { #ns_lit })
    } else {
        // For derived namespaces: use const fn with const-compatible str construction
        (
            quote! {
                // Capture module_path!() in a const to avoid temporary value issues
                const #module_path_const_name: &str = module_path!();

                const fn #compute_len_name(path: &str) -> usize {
                    let bytes = path.as_bytes();

                    // Find the last segment (after the last ::)
                    let mut last_sep_pos = 0;
                    let mut i = 0;
                    while i < bytes.len() {
                        if i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
                            last_sep_pos = i + 2;
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }

                    // Count length from last separator to end
                    bytes.len() - last_sep_pos
                }

                const fn #compute_bytes_name<const N: usize>(path: &str) -> [u8; N] {
                    let mut result = [0u8; N];
                    let bytes = path.as_bytes();

                    // Find the last segment (after the last ::)
                    let mut last_sep_pos = 0;
                    let mut i = 0;
                    while i < bytes.len() {
                        if i + 1 < bytes.len() && bytes[i] == b':' && bytes[i + 1] == b':' {
                            last_sep_pos = i + 2;
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }

                    // Copy last segment, converting underscore to hyphen
                    let mut out = 0;
                    i = last_sep_pos;
                    while i < bytes.len() && out < N {
                        let byte = if bytes[i] == b'_' { b'-' } else { bytes[i] };
                        result[out] = byte;
                        out += 1;
                        i += 1;
                    }

                    result
                }

                #[allow(non_snake_case)]
                const fn #namespace_name<const N: usize>(bytes: &[u8; N]) -> &str {
                    // SAFETY: We only insert valid UTF-8 bytes (ASCII letters, hyphens)
                    // in compute_bytes_name, so this is guaranteed to be valid UTF-8
                    unsafe { std::str::from_utf8_unchecked(bytes) }
                }

                const #namespace_len_name: usize = #compute_len_name(#module_path_const_name);
                const #namespace_bytes_name: [u8; #namespace_len_name] = #compute_bytes_name(#module_path_const_name);
            },
            quote! { #namespace_name(&#namespace_bytes_name) },
        )
    };

    // Generate concept const name
    let concept_const_name = syn::Ident::new(
        &format!("{}_CONCEPT", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    let expanded = quote! {
        #namespace_static_decl

        // Generate the CONCEPT constant
        const #concept_const_name: dialog_query::predicate::concept::Concept = {
            const ATTRS: dialog_query::predicate::concept::Attributes =
                dialog_query::predicate::concept::Attributes::Static(&[(
                    "has",
                    dialog_query::attribute::AttributeSchema {
                        namespace: #namespace_expr,
                        name: #attr_name_lit,
                        description: #description_lit,
                        cardinality: #cardinality,
                        content_type: <#wrapped_type as dialog_query::types::IntoType>::TYPE,
                        marker: std::marker::PhantomData,
                    },
                )]);

            dialog_query::predicate::concept::Concept::Static {
                description: #description_lit,
                attributes: &ATTRS,
            }
        };

        impl dialog_query::attribute::Attribute for #struct_name {
            type Type = #wrapped_type;

            type Match = dialog_query::attribute::WithMatch<Self>;
            type Instance = dialog_query::attribute::With<Self>;
            type Term = dialog_query::attribute::WithTerms<Self>;

            const NAMESPACE: &'static str = #namespace_expr;
            const NAME: &'static str = #attr_name_lit;
            const DESCRIPTION: &'static str = #description_lit;
            const CARDINALITY: dialog_query::attribute::Cardinality = #cardinality;
            const SCHEMA: dialog_query::attribute::AttributeSchema<Self::Type> = dialog_query::attribute::AttributeSchema {
                namespace: Self::NAMESPACE,
                name: Self::NAME,
                description: Self::DESCRIPTION,
                cardinality: Self::CARDINALITY,
                content_type: <#wrapped_type as dialog_query::types::IntoType>::TYPE,
                marker: std::marker::PhantomData,
            };
            const CONCEPT: dialog_query::predicate::concept::Concept = #concept_const_name;

            fn value(&self) -> &Self::Type {
                &self.0
            }

            fn new(value: Self::Type) -> Self {
                Self(value)
            }
        }

        // Debug implementation showing attribute metadata
        impl std::fmt::Debug for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!(#struct_name))
                    .field("namespace", &<Self as dialog_query::attribute::Attribute>::NAMESPACE)
                    .field("name", &<Self as dialog_query::attribute::Attribute>::NAME)
                    .field("value", &self.0)
                    .finish()
            }
        }

        // Display implementation showing selector and value
        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}/{}: {:?}",
                    <Self as dialog_query::attribute::Attribute>::NAMESPACE,
                    <Self as dialog_query::attribute::Attribute>::NAME,
                    self.0
                )
            }
        }

        // Generic From implementation for any type that can convert into the wrapped type
        impl<U: ::std::convert::Into<#wrapped_type>> ::std::convert::From<U> for #struct_name {
            fn from(value: U) -> Self {
                <Self as dialog_query::attribute::Attribute>::new(value.into())
            }
        }
    };

    TokenStream::from(expanded)
}
