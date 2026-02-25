//! Attribute derive macro implementation
//!
//! Generates an `Attribute` trait impl for a newtype struct, turning it into
//! a typed, self-describing ECS-style attribute that can be used in queries.
//!
//! # Example input
//!
//! ```rust,ignore
//! /// A person's full name
//! #[derive(Attribute)]
//! #[cardinality(one)]
//! struct FullName(String);
//! ```
//!
//! # Generated output (simplified)
//!
//! ```rust,ignore
//! // -- Namespace derivation (when no explicit #[namespace("...")] is given) --
//! // Extracts the last segment of module_path!() at compile time.
//! // e.g. module_path!() = "my_crate::model" → namespace = "model"
//! const __FULLNAME_MODULE_PATH: &str = module_path!();
//! const __FULLNAME_NAMESPACE_LEN: usize = __compute_fullname_namespace_len(__FULLNAME_MODULE_PATH);
//! const FULLNAME_NAMESPACE_BYTES: [u8; __FULLNAME_NAMESPACE_LEN] = __compute_fullname_namespace_bytes(__FULLNAME_MODULE_PATH);
//! // FULLNAME_NAMESPACE(...) converts the byte array back to &str
//!
//! // -- Concept constant --
//! const FULLNAME_CONCEPT: Concept = Concept::Static {
//!     description: "A person's full name",
//!     attributes: &Attributes::Static(&[("has", AttributeDescriptor::Static {
//!         namespace: /* derived or explicit */,
//!         name: "full-name",         // PascalCase → kebab-case
//!         description: "A person's full name",
//!         cardinality: Cardinality::One,
//!         content_type: <String as IntoType>::TYPE,
//!     })]),
//! };
//!
//! // -- Attribute trait impl --
//! impl Attribute for FullName {
//!     type Type = String;
//!     type Query = MatchQuery<Self>;
//!     type Proof = With<Self>;
//!     type Term = WithTerms<Self>;
//!
//!     const DESCRIPTOR: AttributeDescriptor = /* ... */;
//!     const CONCEPT: Concept = FULLNAME_CONCEPT;
//!
//!     fn value(&self) -> &String { &self.0 }
//!     fn new(value: String) -> Self { Self(value) }
//! }
//!
//! // -- Debug, Display, and From impls --
//! // Debug:   FullName { namespace: "model", name: "full-name", value: "Alice" }
//! // Display: model/full-name: "Alice"
//! // From<U>: any U: Into<String> can be converted into FullName
//! ```

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

    // If the user wrote #[namespace("my-ns")], use that; otherwise we'll derive it
    // from module_path!() at compile time (see namespace_static_decl below).
    let explicit_namespace = parse_namespace_attribute(&input.attrs);

    // Convert struct name to kebab-case for the attribute name.
    // e.g. FullName → "full-name", created_at → "created-at"
    let attr_name = to_kebab_case(&struct_name.to_string());
    let attr_name_lit = syn::LitStr::new(&attr_name, proc_macro2::Span::call_site());

    // Extract doc comments
    let description = extract_doc_comments(&input.attrs);
    let description_lit = syn::LitStr::new(&description, proc_macro2::Span::call_site());

    // Parse cardinality
    let cardinality = parse_cardinality_attribute(&input.attrs);

    // Generate unique identifiers for the const-fn namespace machinery.
    // Each struct gets its own set to avoid name collisions when multiple
    // Attribute derives exist in the same module.
    // e.g. for FullName: __compute_fullname_namespace_len, FULLNAME_NAMESPACE_BYTES, etc.
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

    // Build the namespace value. Two cases:
    // 1. Explicit: #[namespace("my-ns")] → just use the string literal directly.
    // 2. Derived: extract last segment of module_path!() at compile time using
    //    const fns (because module_path!() is only available as a macro, not in
    //    const contexts, we capture it in a const and process it with const fns).
    //    e.g. "my_crate::models::person" → "person"
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

    // Assemble the final generated code: namespace consts, Attribute impl,
    // and the Debug/Display/From trait impls.
    let expanded = quote! {
        #namespace_static_decl

        impl dialog_query::dsl::Predicate for #struct_name {
            type Proof = dialog_query::concept::With<Self>;
            type Application = dialog_query::concept::WithQuery<Self>;
            type Descriptor = dialog_query::attribute::AttributeDescriptor;
        }

        impl dialog_query::attribute::Attribute for #struct_name {
            type Type = #wrapped_type;

            type Query = dialog_query::concept::WithQuery<Self>;
            type Proof = dialog_query::concept::With<Self>;
            type Term = dialog_query::concept::WithTerms<Self>;

            fn descriptor() -> dialog_query::attribute::AttributeDescriptor {
                let the = format!("{}/{}", #namespace_expr, #attr_name_lit)
                    .parse::<dialog_query::attribute::The>()
                    .expect("attribute selector must be valid");
                dialog_query::attribute::AttributeDescriptor::new(
                    the,
                    #description_lit,
                    #cardinality,
                    <#wrapped_type as dialog_query::types::IntoType>::TYPE,
                )
            }

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
                    .field("namespace", &<Self as dialog_query::attribute::Attribute>::descriptor().namespace())
                    .field("name", &<Self as dialog_query::attribute::Attribute>::descriptor().name())
                    .field("value", &self.0)
                    .finish()
            }
        }

        // Display implementation showing selector and value
        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}/{}: {:?}",
                    <Self as dialog_query::attribute::Attribute>::descriptor().namespace(),
                    <Self as dialog_query::attribute::Attribute>::descriptor().name(),
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
