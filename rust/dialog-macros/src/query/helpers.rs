//! Shared helper functions for query derive macros

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Expr, Lit, Meta, Type};

/// Generate the Type value for a field type in static attribute declarations.
///
/// This uses the IntoType trait's const TYPE associated constant to determine
/// the type at compile time. This works because IntoType::TYPE is a const,
/// allowing proper type detection without string matching.
///
/// Returns Option<Type> directly from the trait's TYPE constant:
/// - Some(Type::String) for String types
/// - None for Value types (accepts any type)
pub fn type_to_value_data_type(ty: &Type) -> TokenStream {
    quote! {
        <#ty as dialog_query::types::IntoType>::TYPE
    }
}

/// Extract doc comments from attributes
pub fn extract_doc_comments(attrs: &[Attribute]) -> String {
    let mut docs = Vec::new();

    for attr in attrs {
        match &attr.meta {
            Meta::NameValue(nv) if nv.path.is_ident("doc") => {
                if let Expr::Lit(expr_lit) = &nv.value
                    && let Lit::Str(lit) = &expr_lit.lit
                {
                    // Trim leading space that rustdoc adds
                    let doc = lit.value();
                    let trimmed = doc.trim_start_matches(' ');
                    docs.push(trimmed.to_string());
                }
            }
            _ => {}
        }
    }

    // Join multiple doc comment lines with spaces and trim
    docs.join(" ").trim().to_string()
}

pub fn to_snake_case(s: &str) -> String {
    let mut result = String::new();

    for ch in s.chars() {
        if ch.is_uppercase() {
            if !result.is_empty() {
                result.push('.');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }

    result
}

/// Convert PascalCase or snake_case to kebab-case at compile time
/// Examples:
/// - UserName -> user-name
/// - HTTPRequest -> http-request
/// - account_name -> account-name
pub fn to_kebab_case(s: &str) -> String {
    let mut result = String::new();
    let mut prev_is_lower = false;
    let mut prev_is_upper = false;

    for (i, ch) in s.chars().enumerate() {
        if ch == '_' {
            result.push('-');
            prev_is_lower = false;
            prev_is_upper = false;
        } else if ch.is_uppercase() {
            // Add hyphen before uppercase if:
            // 1. Not at start
            // 2. Previous was lowercase (camelCase boundary)
            // 3. Next is lowercase and previous was uppercase (HTTPRequest -> http-request)
            if i > 0
                && (prev_is_lower
                    || (prev_is_upper && s.chars().nth(i + 1).is_some_and(|c| c.is_lowercase())))
            {
                result.push('-');
            }
            result.push(ch.to_lowercase().next().unwrap());
            prev_is_lower = false;
            prev_is_upper = true;
        } else {
            result.push(ch);
            prev_is_lower = true;
            prev_is_upper = false;
        }
    }

    result
}

/// Parse the `#[derived]` or `#[derived(cost = N)]` attribute
/// Returns `Some(cost)` if the field is derived, `None` otherwise
/// Default cost is 1 if not specified
pub fn parse_derived_attribute(attrs: &[Attribute]) -> Option<usize> {
    for attr in attrs {
        if attr.path().is_ident("derived") {
            // Check if there are any nested meta items
            let mut cost = Some(1); // Default cost is 1

            // Try to parse nested meta (cost = N)
            let result = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("cost") {
                    let value = meta.value()?;
                    let lit: Lit = value.parse()?;
                    if let Lit::Int(lit_int) = lit {
                        cost = Some(lit_int.base10_parse::<usize>()?);
                        Ok(())
                    } else {
                        Err(meta.error("cost must be an integer"))
                    }
                } else {
                    Err(meta.error("unknown derived attribute parameter"))
                }
            });

            // If parsing succeeds or there's no nested content, return the cost
            // If parsing fails, it's an error in the attribute syntax
            match result {
                Ok(()) => return cost,
                Err(_) if matches!(attr.meta, syn::Meta::Path(_)) => {
                    // Just #[derived] with no parameters - use default cost
                    return Some(1);
                }
                Err(e) => {
                    // Syntax error in attribute
                    panic!("Error parsing derived attribute: {}", e);
                }
            }
        }
    }
    None
}

/// Parse the `#[namespace(...)]` attribute
///
/// Supports:
/// - `#[namespace(custom)]` - simple identifier syntax
/// - `#[namespace("io.gozala")]` - string literal syntax (for dotted namespaces)
/// - `#[namespace = "legacy"]` - legacy syntax (still supported)
///
/// Returns `Some(namespace)` if specified, `None` to use default
pub fn parse_namespace_attribute(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("namespace") {
            if let Meta::List(list) = &attr.meta {
                let tokens = list.tokens.clone();

                // First try parsing as a string literal: #[namespace("custom")]
                if let Ok(lit) = syn::parse2::<syn::LitStr>(tokens.clone()) {
                    return Some(lit.value());
                }

                // Then try parsing as a simple identifier: #[namespace(custom)]
                if let Ok(ident) = syn::parse2::<syn::Ident>(tokens) {
                    return Some(ident.to_string());
                }
            }

            // Support legacy #[namespace = "..."] syntax
            if let Meta::NameValue(nv) = &attr.meta
                && let Expr::Lit(expr_lit) = &nv.value
                && let Lit::Str(lit) = &expr_lit.lit
            {
                return Some(lit.value());
            }
        }
    }
    None
}

/// Parse the `#[cardinality(many)]` attribute
/// Returns the appropriate Cardinality token stream
pub fn parse_cardinality_attribute(attrs: &[Attribute]) -> TokenStream {
    for attr in attrs {
        if attr.path().is_ident("cardinality") {
            let result = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("many") || meta.path.is_ident("one") {
                    Ok(())
                } else {
                    Err(meta.error("cardinality must be 'one' or 'many'"))
                }
            });

            match result {
                Ok(()) => {
                    // Check which one it was
                    if let Meta::List(list) = &attr.meta {
                        let tokens_str = list.tokens.to_string();
                        if tokens_str.contains("many") {
                            return quote! { dialog_query::attribute::Cardinality::Many };
                        }
                    }
                }
                Err(e) => {
                    panic!("Error parsing cardinality attribute: {}", e);
                }
            }
        }
    }

    // Default to One
    quote! { dialog_query::attribute::Cardinality::One }
}
