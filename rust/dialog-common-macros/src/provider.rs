//! Provider macro implementation for dialog-db testing.
//!
//! This module provides the `#[provider]` attribute macro that generates
//! the `Provisionable` trait implementation for address types.
//!
//! The macro transforms an async function returning `Service<Address, Provider>`
//! into a provisionable address that works with the `#[dialog_common::test]` macro.

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, ReturnType, Type, parse_macro_input};

/// Implementation used by `dialog_common::provider` macro.
pub fn generate(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Ensure no attributes were passed
    if !attr.is_empty() {
        return syn::Error::new_spanned(
            proc_macro2::TokenStream::from(attr),
            "#[provider] does not accept any arguments",
        )
        .to_compile_error()
        .into();
    }

    let input = parse_macro_input!(item as ItemFn);
    generate_provider(&input)
}

/// Generate provider implementation from the annotated function.
///
/// Given a function like:
/// ```rs
/// use serde::{Deserialize, Serialize};
///
/// #[dialog_common::provider]
/// async fn tcp_server(settings: Settings) -> anyhow::Result<Service<Host, (std::net::TcpListener,)>> {
///     let listener = std::net::TcpListener::bind(format!("127.0.0.1:{}", settings.port))?;
///     let addr = listener.local_addr()?;
///     Ok(Service::new(Host { url: format!("http://{}", addr) }, (listener,)))
/// }
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct Host {
///     pub url: String,
/// }
///
/// #[derive(Debug, Clone, Default)]
/// pub struct Settings {
///     pub port: u16
/// }
/// ```
///
/// This generates a `Provisionable` trait implementation on `Host`:
///
/// ```rs
/// use dialog_common::provider::Provisionable;
/// #[cfg(not(target_arch = "wasm32"))]
/// impl Provisionable for Host {
///     type Settings = Settings;
///     type Provider = (std::net::TcpListener,);
///
///     async fn start(settings: Self::Settings) -> anyhow::Result<Service<Self, Self::Provider>> {
///         tcp_server(settings).await
///     }
/// }
/// ```
fn generate_provider(input: &ItemFn) -> TokenStream {
    let fn_name = &input.sig.ident;
    let vis = &input.vis;
    let attrs = &input.attrs;
    let body = &input.block;

    // Extract the settings type and pattern from the first parameter
    let (settings_pat, settings_type) = match extract_settings_param(input) {
        Ok(result) => result,
        Err(e) => return e.to_compile_error().into(),
    };

    // Extract Address and Provider types from the return type
    let (address_type, provider_type) = match extract_service_types(input) {
        Ok(types) => types,
        Err(e) => return e.to_compile_error().into(),
    };

    let expanded = quote! {
        // Provider function and Provisionable impl are native-only
        #[cfg(not(target_arch = "wasm32"))]
        #(#attrs)*
        #vis async fn #fn_name(#settings_pat: #settings_type) -> ::anyhow::Result<::dialog_common::helpers::Service<#address_type, #provider_type>>
            #body

        #[cfg(not(target_arch = "wasm32"))]
        #[::async_trait::async_trait]
        impl ::dialog_common::helpers::Provisionable for #address_type {
            type Settings = #settings_type;
            type Provider = #provider_type;

            async fn start(settings: Self::Settings) -> ::anyhow::Result<::dialog_common::helpers::Service<Self, Self::Provider>> {
                #fn_name(settings).await
            }
        }
    };

    TokenStream::from(expanded)
}

/// Extract the settings pattern and type from the function's first parameter.
fn extract_settings_param(func: &ItemFn) -> syn::Result<(syn::Pat, Type)> {
    let inputs = &func.sig.inputs;

    if inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[provider] function must have a settings parameter",
        ));
    }

    if inputs.len() > 1 {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[provider] function must have exactly one settings parameter",
        ));
    }

    let arg = inputs.first().unwrap();

    match arg {
        syn::FnArg::Typed(pat_type) => Ok(((*pat_type.pat).clone(), (*pat_type.ty).clone())),
        syn::FnArg::Receiver(_) => Err(syn::Error::new_spanned(
            arg,
            "#[provider] function cannot have self parameter",
        )),
    }
}

/// Extract Address and Provider types from the return type.
///
/// Expected return type: `anyhow::Result<Service<Address, Provider>>`
fn extract_service_types(func: &ItemFn) -> syn::Result<(Type, Type)> {
    let ReturnType::Type(_, return_type) = &func.sig.output else {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "#[provider] function must return anyhow::Result<Service<Address, Provider>>",
        ));
    };

    // Navigate through Result<Service<A, P>> to extract A and P
    let result_args = extract_generic_args(return_type, "Result")?;
    if result_args.is_empty() {
        return Err(syn::Error::new_spanned(
            return_type,
            "Expected Result<Service<...>>",
        ));
    }

    let service_type = &result_args[0];
    let service_args = extract_generic_args(service_type, "Service")?;

    if service_args.len() < 2 {
        return Err(syn::Error::new_spanned(
            service_type,
            "Expected Service<Address, Provider>",
        ));
    }

    Ok((service_args[0].clone(), service_args[1].clone()))
}

/// Extract generic arguments from a type like `Foo<A, B>`.
fn extract_generic_args(ty: &Type, expected_name: &str) -> syn::Result<Vec<Type>> {
    match ty {
        Type::Path(type_path) => {
            let segment = type_path.path.segments.last().ok_or_else(|| {
                syn::Error::new_spanned(ty, format!("Expected {}<...>", expected_name))
            })?;

            // Check if the type name matches (last segment)
            if segment.ident != expected_name {
                return Err(syn::Error::new_spanned(
                    ty,
                    format!("Expected {}, found {}", expected_name, segment.ident),
                ));
            }

            match &segment.arguments {
                syn::PathArguments::AngleBracketed(args) => {
                    let types: Vec<Type> = args
                        .args
                        .iter()
                        .filter_map(|arg| {
                            if let syn::GenericArgument::Type(t) = arg {
                                Some(t.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    Ok(types)
                }
                _ => Err(syn::Error::new_spanned(
                    ty,
                    format!("Expected {}<...>", expected_name),
                )),
            }
        }
        _ => Err(syn::Error::new_spanned(
            ty,
            format!("Expected {}<...>", expected_name),
        )),
    }
}
