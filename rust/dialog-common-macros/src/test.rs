//! Test macro implementation for dialog-db testing.
//!
//! This module provides the implementation for cross-platform test macros
//! that handle resource provisioning and multi-target testing.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Expr, FnArg, Ident, ItemFn, Pat, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

/// Combine user attributes from the function with extra framework attributes.
fn combine_attrs(input: &ItemFn, extra_attrs: TokenStream2) -> TokenStream2 {
    let user_attrs = &input.attrs;
    quote! { #(#user_attrs)* #extra_attrs }
}

/// Check if an attribute is a test framework attribute that should only apply to functions.
fn is_test_framework_attr(attr: &syn::Attribute) -> bool {
    let path = attr.path();
    // Check for tokio::test, wasm_bindgen_test::wasm_bindgen_test, etc.
    if path.segments.len() >= 2 {
        let first = path.segments.first().map(|s| s.ident.to_string());
        let last = path.segments.last().map(|s| s.ident.to_string());
        matches!(
            (first.as_deref(), last.as_deref()),
            (Some("tokio"), Some("test")) | (Some("wasm_bindgen_test"), Some("wasm_bindgen_test"))
        )
    } else if path.segments.len() == 1 {
        // Check for bare #[test]
        path.segments
            .first()
            .map(|s| s.ident == "test")
            .unwrap_or(false)
    } else {
        false
    }
}

/// Filter out test framework attributes from a list of attributes.
fn filter_non_test_attrs(attrs: &[syn::Attribute]) -> Vec<&syn::Attribute> {
    attrs
        .iter()
        .filter(|a| !is_test_framework_attr(a))
        .collect()
}

/// Read feature names from the crate's Cargo.toml.
fn read_crate_features() -> Vec<String> {
    let Ok(path) = std::env::var("CARGO_MANIFEST_PATH") else {
        return Vec::new();
    };
    let Ok(content) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };

    let mut features = Vec::new();
    let mut in_features = false;
    for line in content.lines() {
        let t = line.trim();
        if t.starts_with('[') {
            in_features = t == "[features]";
        } else if in_features && !t.is_empty() && !t.starts_with('#') {
            if let Some(i) = t.find('=') {
                let name = t[..i].trim();
                if name != "default" {
                    features.push(name.to_string());
                }
            }
        }
    }
    features
}

/// Assignments for Resource::Settings, e.g.:
/// `#[dialog_common::test(bucket = "custom-bucket")]`
pub struct ProviderSettings(pub Vec<(Ident, Expr)>);

impl Parse for ProviderSettings {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self(Vec::new()));
        }

        let pairs = Punctuated::<Setting, Token![,]>::parse_terminated(input)?;
        Ok(Self(pairs.into_iter().map(|p| (p.name, p.value)).collect()))
    }
}

struct Setting {
    name: Ident,
    value: Expr,
}

impl Parse for Setting {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let value: Expr = input.parse()?;
        Ok(Self { name, value })
    }
}

/// Implementation used by `dialog_common::test` macro.
pub fn generate(attr: TokenStream, item: TokenStream) -> TokenStream {
    let settings = parse_macro_input!(attr as ProviderSettings);
    let input = parse_macro_input!(item as ItemFn);
    let attrs = combine_attrs(&input, default_test_attrs());

    generate_test(&input, attrs, &settings)
}

/// Implementation used by `dialog_common::test::custom` macro.
pub fn generate_custom(attr: TokenStream, item: TokenStream) -> TokenStream {
    let settings = parse_macro_input!(attr as ProviderSettings);
    let input = parse_macro_input!(item as ItemFn);
    let attrs = combine_attrs(&input, quote! {});

    generate_test(&input, attrs, &settings)
}

/// Shared implementation for both macros.
fn generate_test(input: &ItemFn, attrs: TokenStream2, settings: &ProviderSettings) -> TokenStream {
    // Check if function has parameters (provisioned test) or not (simple test)
    if input.sig.inputs.is_empty() {
        // Simple test - no provisioning needed
        generate_basic_test(input, &attrs)
    } else {
        // Provisioned test - provider/test setup
        generate_provisioned_test(input, &attrs, settings)
    }
}

/// Generate default test framework attributes.
fn default_test_attrs() -> TokenStream2 {
    quote! {
        #[cfg_attr(
            all(test, not(target_arch = "wasm32")),
            tokio::test
        )]
        #[cfg_attr(
            all(test, target_arch = "wasm32"),
            wasm_bindgen_test::wasm_bindgen_test
        )]
    }
}

/// Generate a basic test without provisioning.
fn generate_basic_test(input: &ItemFn, attrs: &TokenStream2) -> TokenStream {
    let vis = &input.vis;
    let name = &input.sig.ident;
    let output = &input.sig.output;
    let body = &input.block;

    let expanded = quote! {
        #attrs
        #vis async fn #name() #output
            #body
    };

    TokenStream::from(expanded)
}

/// Generate a provisioned test with provider/test setup for cross-target testing.
///
/// The provider module (native only):
/// 1. Starts the provider
/// 2. Serializes resource to env var
/// 3. Spawns the test via cargo (with same features)
/// 4. Stops the provider
///
/// The test function (any target):
/// 1. Reads resource from env var
/// 2. Runs test body
fn generate_provisioned_test(
    input: &ItemFn,
    attrs: &TokenStream2,
    settings: &ProviderSettings,
) -> TokenStream {
    let name = &input.sig.ident;
    let vis = &input.vis;
    let body = &input.block;
    let output = &input.sig.output;
    // Provider module only gets non-test attrs (doc comments, cfg, etc.)
    // Test framework attrs (tokio::test, etc.) only apply to the inner test function
    let module_attrs = filter_non_test_attrs(&input.attrs);

    // Extract the resource type from the function parameter
    let (param_name, resource_type) = match extract_resource_param(input) {
        Ok(result) => result,
        Err(e) => return e.to_compile_error().into(),
    };

    let provider = generate_provider(name, vis, &module_attrs, &resource_type, settings);

    // Generate the test function - skips if env var not set (not invoked by provider)
    let test = quote! {
        #attrs
        #vis async fn #name() #output {
            // Skip if not invoked by provider (env var not set)
            let Ok(json) = ::std::env::var(::dialog_common::helpers::PROVISIONED_ENV_VAR) else {
                println!("skipped (run via provider)");
                return Ok(());
            };
            let #param_name: #resource_type = ::serde_json::from_str(&json)
                .expect("Failed to deserialize resource");

            #body
        }
    };

    let expanded = quote! {
        #provider
        #test
    };

    TokenStream::from(expanded)
}

/// Generate the provider module that starts resources and invokes the test.
///
/// The provider is a module with the same name as the test, gated to native-only.
/// It contains a test function that starts the resource provider, serializes
/// the resource state, and invokes the actual test in a subprocess.
fn generate_provider(
    name: &Ident,
    vis: &syn::Visibility,
    module_attrs: &[&syn::Attribute],
    resource_type: &Type,
    settings: &ProviderSettings,
) -> TokenStream2 {
    let name_str = name.to_string();

    // Build settings field assignments
    let field_names: Vec<_> = settings.0.iter().map(|(name, _)| name).collect();
    let field_values: Vec<_> = settings.0.iter().map(|(_, value)| value).collect();

    let settings_setup = quote! {
        let mut settings = <#resource_type as ::dialog_common::helpers::Resource>::Settings::default();
        #(settings.#field_names = (#field_values).into();)*
    };

    // Generate cfg!() checks for each feature in Cargo.toml
    let checks: Vec<_> = read_crate_features()
        .iter()
        .map(|f| {
            let name = f.as_str();
            quote! { if cfg!(feature = #name) { f.push(#name); } }
        })
        .collect();

    quote! {
        #(#module_attrs)*
        #[cfg(all(test, not(target_arch = "wasm32")))]
        #vis mod #name {
            use super::*;

            #[tokio::test]
            pub async fn test() -> ::anyhow::Result<()> {
                use ::dialog_common::helpers::{Resource, Provider, PROVISIONED_ENV_VAR};
                use ::std::process::Command;

                #settings_setup

                // Start the provider and get the resource
                let provider = <#resource_type as Resource>::start(settings)
                    .await
                    .expect("Failed to start provider");
                let resource: #resource_type = provider.provide();

                // Serialize resource for test
                let state_json = ::serde_json::to_string(&resource)
                    .expect("Failed to serialize resource");

                // Check if --nocapture was passed
                let show_output = ::std::env::var("RUST_TEST_NOCAPTURE").is_ok()
                    || ::std::env::args().any(|arg| arg == "--nocapture");

                // Check if --color=always was passed
                let color_always = ::std::env::args().any(|arg| arg == "--color=always");

                // Capture combined stdout+stderr to preserve natural output ordering
                use ::std::process::Stdio;

                // Add package (CARGO_PKG_NAME is set during compilation)
                let pkg_name = env!("CARGO_PKG_NAME");

                // Build feature list using cfg!() checks
                let mut f: Vec<&str> = Vec::new();
                #(#checks)*
                let features_str = f.join(",");

                // Build the cargo command string
                // Use --exact with full module path to match only the inner test,
                // not this provider module's __run__ function
                let color_flag = if color_always { "--color=always" } else { "" };
                // module_path!() gives us "crate_name::path::to::test_name" (provider module).
                // Test harness uses "path::to::test_name" (without crate, same as provider module).
                // We strip "crate_name::" prefix to get the test filter.
                let test_path = &module_path!()[pkg_name.len() + 2..];
                let test_cmd = if features_str.is_empty() {
                    format!(
                        "cargo test {} -p {} --lib -- --exact {} --nocapture",
                        color_flag, pkg_name, test_path
                    )
                } else {
                    format!(
                        "cargo test {} -p {} --features {} --lib -- --exact {} --nocapture",
                        color_flag, pkg_name, features_str, test_path
                    )
                };

                // Use sh -c to run the command with stderr merged into stdout (2>&1)
                let mut cmd = Command::new("sh");
                cmd.arg("-c")
                    .arg(format!("{} 2>&1", test_cmd))
                    .env(PROVISIONED_ENV_VAR, &state_json)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                // Spawn the subprocess and wait for it in a blocking thread
                // This allows the tokio runtime to continue processing (e.g., the server)
                let output = ::tokio::task::spawn_blocking(move || {
                    cmd.output().expect("Failed to execute cargo test")
                }).await.expect("Failed to join blocking task");

                // stdout now contains both stdout and stderr in natural order
                let combined = String::from_utf8_lossy(&output.stdout);

                // Show output on failure or when --nocapture was passed
                if (show_output || !output.status.success()) && !combined.is_empty() {
                    use ::std::fmt::Write;
                    use ::std::io::Write as IoWrite;
                    let mut buf = String::new();
                    let _ = writeln!(buf, "\n  ┌─ {}", test_cmd);
                    for line in combined.lines() {
                        let _ = writeln!(buf, "  │ {}", line);
                    }
                    let _ = writeln!(buf, "  └─");
                    // Write atomically to stderr
                    let stderr = ::std::io::stderr();
                    let mut handle = stderr.lock();
                    let _ = handle.write_all(buf.as_bytes());
                    let _ = handle.flush();
                }

                if !output.status.success() {
                    panic!("Test '{}' failed", #name_str);
                }

                // Verify the test actually ran (not "running 0 tests")
                // This catches cases where the test wasn't compiled (e.g., cfg mismatch)
                if combined.contains("running 0 tests") {
                    panic!(
                        "Test '{}' was not found. This usually means the test \
                         wasn't compiled (check cfg attributes match).",
                        #name_str
                    );
                }

                Ok(())
            }
        }
    }
}

/// Extract the parameter name and resource type from the function signature.
fn extract_resource_param(func: &ItemFn) -> syn::Result<(Ident, Type)> {
    let inputs = &func.sig.inputs;

    if inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            &func.sig,
            "provisioned test must have exactly one resource parameter",
        ));
    }

    let arg = inputs.first().unwrap();

    match arg {
        FnArg::Typed(pat_type) => {
            let param_name = match pat_type.pat.as_ref() {
                Pat::Ident(pat_ident) => pat_ident.ident.clone(),
                _ => {
                    return Err(syn::Error::new_spanned(
                        &pat_type.pat,
                        "Expected a simple identifier for the parameter",
                    ));
                }
            };
            let param_type = (*pat_type.ty).clone();
            Ok((param_name, param_type))
        }
        FnArg::Receiver(_) => Err(syn::Error::new_spanned(
            arg,
            "test function cannot have self parameter",
        )),
    }
}
