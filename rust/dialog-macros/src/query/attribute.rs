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
//! // -- Domain derivation (when no explicit #[domain("...")] is given) --
//! // Extracts the last segment of module_path!() at compile time.
//! // e.g. module_path!() = "my_crate::model" → domain = "model"
//! const __FULLNAME_MODULE_PATH: &str = module_path!();
//! const __FULLNAME_DOMAIN_LEN: usize = __compute_fullname_domain_len(__FULLNAME_MODULE_PATH);
//! const FULLNAME_DOMAIN_BYTES: [u8; __FULLNAME_DOMAIN_LEN] = __compute_fullname_domain_bytes(__FULLNAME_MODULE_PATH);
//! // FULLNAME_DOMAIN(...) converts the byte array back to &str
//!
//! // -- Attribute trait impl --
//! impl Attribute for FullName {
//!     type Type = String;
//!
//!     fn descriptor() -> AttributeDescriptor {
//!         // Builds descriptor from domain/name with description, cardinality, type
//!         AttributeDescriptor::new(/* domain/full-name */, "A person's full name",
//!             Cardinality::One, <String as IntoType>::TYPE)
//!     }
//!
//!     fn value(&self) -> &String { &self.0 }
//! }
//!
//! // -- Debug, Display, and From impls --
//! // Debug:   FullName { domain: "model", name: "full-name", value: "Alice" }
//! // Display: model/full-name: "Alice"
//! // From<U>: any U: Into<String> can be converted into FullName
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

use super::helpers::{
    extract_doc_comments, parse_cardinality_attribute, parse_domain_attribute, to_kebab_case,
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

    // If the user wrote #[domain("my-ns")], use that; otherwise we'll derive it
    // from module_path!() at compile time (see domain_static_decl below).
    let explicit_domain = parse_domain_attribute(&input.attrs);

    // Convert struct name to kebab-case for the attribute name.
    // e.g. FullName → "full-name", created_at → "created-at"
    let attr_name = to_kebab_case(&struct_name.to_string());
    let attr_name_lit = syn::LitStr::new(&attr_name, proc_macro2::Span::call_site());

    // Extract doc comments
    let description = extract_doc_comments(&input.attrs);
    let description_lit = syn::LitStr::new(&description, proc_macro2::Span::call_site());

    // Parse cardinality
    let cardinality = parse_cardinality_attribute(&input.attrs);

    // Generate unique identifiers for the const-fn domain machinery.
    // Each struct gets its own set to avoid name collisions when multiple
    // Attribute derives exist in the same module.
    // e.g. for FullName: __compute_fullname_domain_len, FULLNAME_DOMAIN_BYTES, etc.
    let compute_len_name = syn::Ident::new(
        &format!(
            "__compute_{}_domain_len",
            struct_name.to_string().to_lowercase()
        ),
        struct_name.span(),
    );
    let compute_bytes_name = syn::Ident::new(
        &format!(
            "__compute_{}_domain_bytes",
            struct_name.to_string().to_lowercase()
        ),
        struct_name.span(),
    );
    let domain_len_name = syn::Ident::new(
        &format!("__{}_DOMAIN_LEN", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let domain_bytes_name = syn::Ident::new(
        &format!("{}_DOMAIN_BYTES", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );
    let domain_name = syn::Ident::new(
        &format!("{}_DOMAIN", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Generate additional const name for module path
    let module_path_const_name = syn::Ident::new(
        &format!("__{}_MODULE_PATH", struct_name.to_string().to_uppercase()),
        struct_name.span(),
    );

    // Build the domain value. Two cases:
    // 1. Explicit: #[domain("my-ns")] → just use the string literal directly.
    // 2. Derived: extract last segment of module_path!() at compile time using
    //    const fns (because module_path!() is only available as a macro, not in
    //    const contexts, we capture it in a const and process it with const fns).
    //    e.g. "my_crate::models::person" → "person"
    let (domain_static_decl, domain_expr) = if let Some(ref ns) = explicit_domain {
        let ns_lit = syn::LitStr::new(ns, proc_macro2::Span::call_site());
        (quote! {}, quote! { #ns_lit })
    } else {
        // For derived domains: use const fn with const-compatible str construction
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
                const fn #domain_name<const N: usize>(bytes: &[u8; N]) -> &str {
                    // SAFETY: We only insert valid UTF-8 bytes (ASCII letters, hyphens)
                    // in compute_bytes_name, so this is guaranteed to be valid UTF-8
                    unsafe { std::str::from_utf8_unchecked(bytes) }
                }

                const #domain_len_name: usize = #compute_len_name(#module_path_const_name);
                const #domain_bytes_name: [u8; #domain_len_name] = #compute_bytes_name(#module_path_const_name);
            },
            quote! { #domain_name(&#domain_bytes_name) },
        )
    };

    // Assemble the final generated code: domain consts, Attribute impl,
    // and the Debug/Display/From trait impls.
    let expanded = quote! {
        #domain_static_decl

        impl dialog_query::Predicate for #struct_name {
            type Conclusion = dialog_query::With<Self>;
            type Application = dialog_query::WithQuery<Self>;
            type Descriptor = dialog_query::AttributeDescriptor;
        }

        impl dialog_query::Attribute for #struct_name {
            type Type = #wrapped_type;

            fn descriptor() -> dialog_query::AttributeDescriptor {
                let the = format!("{}/{}", #domain_expr, #attr_name_lit)
                    .parse::<dialog_query::The>()
                    .expect("attribute selector must be valid");
                dialog_query::AttributeDescriptor::new(
                    the,
                    #description_lit,
                    #cardinality,
                    <#wrapped_type as dialog_query::IntoType>::TYPE,
                )
            }

            fn value(&self) -> &Self::Type {
                &self.0
            }
        }

        // Debug implementation showing attribute metadata
        impl std::fmt::Debug for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!(#struct_name))
                    .field("domain", &<Self as dialog_query::Attribute>::descriptor().domain())
                    .field("name", &<Self as dialog_query::Attribute>::descriptor().name())
                    .field("value", &self.0)
                    .finish()
            }
        }

        // Display implementation showing selector and value
        impl std::fmt::Display for #struct_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}/{}: {:?}",
                    <Self as dialog_query::Attribute>::descriptor().domain(),
                    <Self as dialog_query::Attribute>::descriptor().name(),
                    self.0
                )
            }
        }

        // Generic From implementation for any type that can convert into the wrapped type
        impl<U: ::std::convert::Into<#wrapped_type>> ::std::convert::From<U> for #struct_name {
            fn from(value: U) -> Self {
                Self(value.into())
            }
        }
    };

    TokenStream::from(expanded)
}
