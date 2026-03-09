//! Shared helper functions for query derive macros

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, Expr, Lit, Meta, Type};

/// Generate the Type value for a field type in static attribute declarations.
///
/// This resolves through the Typed → TypeDescriptor chain:
/// `<<T as Typed>::Descriptor as TypeDescriptor>::TYPE`
///
/// Returns Option<Type> directly from the descriptor's TYPE constant:
/// - Some(Type::String) for String types
/// - Some(Type::Entity) for Entity types
/// - etc.
pub fn type_to_value_data_type(ty: &Type) -> TokenStream {
    quote! {
        <<#ty as dialog_query::Typed>::Descriptor as dialog_query::TypeDescriptor>::TYPE
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

/// Parse the `#[derived]` or `#[derived(cost = N)]` attribute.
/// Returns `Ok(Some(cost))` if the field is derived, `Ok(None)` otherwise.
/// Default cost is 1 if not specified.
pub fn parse_derived_attribute(attrs: &[Attribute]) -> Result<Option<usize>, syn::Error> {
    for attr in attrs {
        if attr.path().is_ident("derived") {
            // Just #[derived] with no parameters
            if matches!(attr.meta, syn::Meta::Path(_)) {
                return Ok(Some(1));
            }

            let mut cost = 1usize;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("cost") {
                    let value = meta.value()?;
                    let lit: Lit = value.parse()?;
                    if let Lit::Int(lit_int) = lit {
                        cost = lit_int.base10_parse::<usize>()?;
                        Ok(())
                    } else {
                        Err(meta.error("cost must be an integer"))
                    }
                } else {
                    Err(meta.error("unknown derived attribute parameter"))
                }
            })?;

            return Ok(Some(cost));
        }
    }
    Ok(None)
}

/// Parse the `#[domain(...)]` attribute to extract the domain value.
///
/// Supports:
/// - `#[domain(custom)]` - simple identifier syntax
/// - `#[domain("io.gozala")]` - string literal syntax (for dotted domains)
/// - `#[domain = "value"]` - name-value syntax
/// - `#[namespace(...)]` - legacy alias (same syntax variants)
///
/// Returns `Some(domain)` if specified, `None` to use default
pub fn parse_domain_attribute(attrs: &[Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("domain") || attr.path().is_ident("namespace") {
            if let Meta::List(list) = &attr.meta {
                let tokens = list.tokens.clone();

                // First try parsing as a string literal: #[domain("custom")]
                if let Ok(lit) = syn::parse2::<syn::LitStr>(tokens.clone()) {
                    return Some(lit.value());
                }

                // Then try parsing as a simple identifier: #[domain(custom)]
                if let Ok(ident) = syn::parse2::<syn::Ident>(tokens) {
                    return Some(ident.to_string());
                }
            }

            // Support #[domain = "..."] syntax
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

/// Parse the `#[cardinality(one)]` or `#[cardinality(many)]` attribute.
/// Returns the appropriate Cardinality token stream, defaulting to `One`.
pub fn parse_cardinality_attribute(attrs: &[Attribute]) -> Result<TokenStream, syn::Error> {
    for attr in attrs {
        if attr.path().is_ident("cardinality") {
            let mut is_many = false;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("many") {
                    is_many = true;
                    Ok(())
                } else if meta.path.is_ident("one") {
                    Ok(())
                } else {
                    Err(meta.error("cardinality must be 'one' or 'many'"))
                }
            })?;

            return if is_many {
                Ok(quote! { dialog_query::Cardinality::Many })
            } else {
                Ok(quote! { dialog_query::Cardinality::One })
            };
        }
    }

    Ok(quote! { dialog_query::Cardinality::One })
}
