//! Test macro implementation for dialog-db testing.
//!
//! This module provides the implementation for cross-platform test macros
//! that handle service provisioning and multi-target testing.
//!
//! # CI Test Matrix
//!
//! The macro is designed to support this CI workflow:
//!
//! 1. `cargo test` - Run all tests natively (unit tests + integration tests inline)
//! 2. `cargo test --target wasm32-unknown-unknown` - Run unit tests in wasm
//! 3. `RUSTFLAGS="--cfg dialog_test_wasm" cargo test` - Run integration tests in wasm
//!    (native provider spawns wasm inner tests)
//!
//! # Generated Code
//!
//! For unit tests (no parameters):
//! - Gated with `not(dialog_test_wasm)` so they don't run during wasm integration runs
//! - Uses `tokio::test` on native, `wasm_bindgen_test` on wasm
//!
//! For integration tests (with address parameter):
//! - Tests that require external services (S3, databases, etc.) that need provisioning
//! - Native inline provider: starts service, runs test via tokio::spawn, stops service
//! - Native wasm-spawn provider: starts service, spawns `cargo test --target wasm32...`
//! - Wasm inner: deserializes address from env var, runs test

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Expr, FnArg, Ident, ItemFn, Pat, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

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

/// Generate a blake3 hash from the function source so it can be
/// uniqueily identified.
fn source_hash(func: &ItemFn) -> String {
    let source = quote::quote!(#func).to_string();
    let hash = blake3::hash(source.as_bytes());
    hash.to_hex().to_string()
}

/// Assignments for Provisionable::Settings, e.g.:
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

    if input.sig.inputs.is_empty() {
        generate_unit_test(&input)
    } else {
        generate_integration_test(&input, &settings)
    }
}

/// Given a `source` function it generates a unit test
/// by adding attributes for both wasm and native runtimes.
///
/// ```rs
/// #[dialog_common::test]
/// fn it_works() {
///     assert_eq!(2 + 2, 4);
/// }
///
/// #[dialog_common::test]
/// async fn it_works_async() {
///     assert_eq!(2 + 2, 4);
/// }
/// ```
///
/// Generates:
/// ```rs
/// // Compile during wasm integration test but as a dead code as it is not
/// // an integration test and we want to supress import warnings
/// #[cfg_attr(dialog_test_wasm, allow(dead_code))]
/// // Compile as test on native, except for wasm integration
/// #[cfg_attr(all(not(dialog_test_wasm), not(target_arch = "wasm32")), test)]
/// // Compile as bindgen test on wasm, expcept for wasm integration
/// #[cfg_attr(all(not(dialog_test_wasm), target_arch = "wasm32"), wasm_bindgen_test::wasm_bindgen_test)]
/// fn it_works() {
///     assert_eq!(2 + 2, 4);
/// }
///
/// // Compile during wasm integration test but as a dead code as it is not
/// // an integration test and we want to supress import warnings
/// #[cfg_attr(dialog_test_wasm, allow(dead_code))]
/// // Compile as test on native, except for wasm integration
/// #[cfg_attr(all(not(dialog_test_wasm), not(target_arch = "wasm32")), tokio::test)]
/// // Compile as bindgen test on wasm, expcept for wasm integration
/// async fn it_works_async() {
///     assert_eq!(2 + 2, 4);
/// }
/// ```
fn generate_unit_test(source: &ItemFn) -> TokenStream {
    let vis = &source.vis;
    let name = &source.sig.ident;
    let asyncness = &source.sig.asyncness;
    let output = &source.sig.output;
    let body = &source.block;
    let user_attrs = &source.attrs;

    // Choose the right test attribute for native based on async vs sync
    let native_test_attr = if source.sig.asyncness.is_some() {
        quote! { tokio::test }
    } else {
        quote! { test }
    };

    let expanded = quote! {
        // Compile during wasm integration test but as a dead code as it is not
        // an integration test and we want to supress import warnings
        #[cfg_attr(dialog_test_wasm, allow(dead_code))]
        // Compile as test on native, except for wasm integration
        #[cfg_attr(all(not(dialog_test_wasm), not(target_arch = "wasm32")), #native_test_attr)]
        // Compile as bindgen test on wasm, expcept for wasm integration
        #[cfg_attr(all(not(dialog_test_wasm), target_arch = "wasm32"), wasm_bindgen_test::wasm_bindgen_test)]
        #(#user_attrs)*
        #vis #asyncness fn #name() #output
            #body
    };

    TokenStream::from(expanded)
}

/// Generate an integration test with service provisioning.
///
/// Given:
/// ```rs
/// #[dialog_common::test]
/// async fn it_connects(server: ServerAddress) -> anyhow::Result<()> {
///     assert!(!server.endpoint.is_empty());
///     Ok(())
/// }
/// ```
///
/// Generates (simplified, with hash `abc123`):
/// ```rs
/// // 1. Integration logic - the actual test body (allow dead_code when dialog_test_wasm)
/// #[cfg_attr(dialog_test_wasm, allow(dead_code))]
/// async fn it_connects_logic_abc123(server: ServerAddress) -> anyhow::Result<()> {
///     assert!(!server.endpoint.is_empty());
///     Ok(())
/// }
///
/// // 2. Native test - runs when `cargo test` (no flags)
/// #[cfg(all(not(dialog_test_wasm), not(target_arch = "wasm32")))]
/// #[tokio::test]
/// async fn it_connects() -> anyhow::Result<()> {
///     let service = ServerAddress::start(Default::default()).await?;
///     let result = tokio::spawn(it_connects_logic_abc123(service.address.clone())).await;
///     service.stop().await?;
///     result?;
///     Ok(())
/// }
///
/// // 3. Wasm integration test - runs when `RUSTFLAGS="--cfg dialog_test_wasm" cargo test`
/// #[cfg(all(dialog_test_wasm, not(target_arch = "wasm32")))]
/// #[tokio::test]
/// async fn it_connects() -> anyhow::Result<()> {
///     let service = ServerAddress::start(Default::default()).await?;
///     let json = serde_json::to_string(&service.address)?;
///     // Spawns: RUSTFLAGS="--cfg dialog_test_wasm_inner" \
///     //         PROVISIONED_SERVICE_ADDRESS='...' \
///     //         cargo test --target wasm32-unknown-unknown it_connects_abc123
///     // ... spawn cargo test for wasm target ...
///     service.stop().await?;
/// }
///
/// // 4. Wasm test - compiled into wasm, receives address via compile-time env var
/// #[cfg(all(dialog_test_wasm_inner, target_arch = "wasm32", target_os = "unknown"))]
/// #[wasm_bindgen_test::wasm_bindgen_test]
/// async fn it_connects_abc123() -> Result<(), wasm_bindgen::JsValue> {
///     let json = option_env!("PROVISIONED_SERVICE_ADDRESS").unwrap();
///     let server: ServerAddress = serde_json::from_str(json)?;
///     it_connects_logic_abc123(server).await?;
/// }
/// ```
///
/// The blake3 hash ensures unique test names so the wasm provider can target
/// exactly the intended test when spawning `cargo test`.
fn generate_integration_test(source: &ItemFn, settings: &ProviderSettings) -> TokenStream {
    let test = match IntegrationTest::new(source, settings) {
        Ok(test) => test,
        Err(e) => return e.to_compile_error().into(),
    };

    test.generate()
}

/// Context shared by all integration test generator functions.
struct IntegrationTest<'a> {
    /// Visibility of the test function
    vis: &'a syn::Visibility,
    /// Original test function identifier
    ident: &'a Ident,
    /// Test name as string
    name: String,
    /// Function body
    body: &'a syn::Block,
    /// Return type
    output: &'a syn::ReturnType,
    /// User-defined attributes
    user_attrs: &'a [syn::Attribute],
    /// Parameter name for the address
    param_name: Ident,
    /// Address type
    address_type: Type,
    /// Identifier for the integration logic function (e.g., `test_logic_abc123`)
    integration_ident: Ident,
    /// Identifier for the wasm test (e.g., `test_abc123`)
    wasm_test_ident: Ident,
    /// Name of the wasm test as string (for cargo test targeting)
    wasm_test_name: String,
    /// Token stream for setting up provider settings
    settings_setup: proc_macro2::TokenStream,
    /// Token stream for feature checks (used by wasm integration test)
    feature_checks: Vec<proc_macro2::TokenStream>,
}

impl<'a> IntegrationTest<'a> {
    fn new(source: &'a ItemFn, settings: &ProviderSettings) -> syn::Result<Self> {
        let ident = &source.sig.ident;
        let name = ident.to_string();
        let hash = source_hash(source);

        let (param_name, address_type) = extract_address_param(source)?;

        let field_names: Vec<_> = settings.0.iter().map(|(name, _)| name).collect();
        let field_values: Vec<_> = settings.0.iter().map(|(_, value)| value).collect();

        let settings_setup = quote! {
            let mut settings = <#address_type as ::dialog_common::helpers::Provisionable>::Settings::default();
            #(settings.#field_names = (#field_values).into();)*
        };

        let feature_checks: Vec<_> = read_crate_features()
            .iter()
            .map(|f| {
                let name = f.as_str();
                quote! { if cfg!(feature = #name) { features.push(#name); } }
            })
            .collect();

        Ok(Self {
            vis: &source.vis,
            ident,
            name: name.clone(),
            body: &source.block,
            output: &source.sig.output,
            user_attrs: &source.attrs,
            param_name,
            address_type,
            integration_ident: Ident::new(&format!("{}_logic_{}", name, hash), ident.span()),
            wasm_test_ident: Ident::new(&format!("{}_{}", name, hash), ident.span()),
            wasm_test_name: format!("{}_{}", name, hash),
            settings_setup,
            feature_checks,
        })
    }

    fn generate(&self) -> TokenStream {
        let logic = self.integration_logic();
        let native = self.native_test();
        let wasm_integration = self.wasm_integration_test();
        let wasm = self.wasm_test();

        TokenStream::from(quote! {
            #logic
            #native
            #wasm_integration
            #wasm
        })
    }

    /// Generate the integration logic function containing the actual test body.
    ///
    /// This function is called by the native test or wasm test.
    /// When `dialog_test_wasm` is set, the wasm integration test spawns a subprocess
    /// instead of calling this directly, so we add `allow(dead_code)`.
    fn integration_logic(&self) -> proc_macro2::TokenStream {
        let IntegrationTest {
            vis,
            user_attrs,
            integration_ident,
            param_name,
            address_type,
            output,
            body,
            ..
        } = self;

        quote! {
            // Integration logic - called by native test or wasm test.
            // When dialog_test_wasm is set, wasm integration test spawns a subprocess
            // instead of calling this directly, so allow dead_code to silence warnings.
            #[cfg_attr(dialog_test_wasm, allow(dead_code))]
            #(#user_attrs)*
            #vis async fn #integration_ident(#param_name: #address_type) #output
                #body
        }
    }

    /// Generate the native test.
    ///
    /// Runs with `cargo test` (no flags). Starts service, runs test, stops service
    /// all in the same process.
    fn native_test(&self) -> proc_macro2::TokenStream {
        let IntegrationTest {
            vis,
            ident,
            address_type,
            integration_ident,
            settings_setup,
            ..
        } = self;

        quote! {
            // Native test: runs with `cargo test` (no flags)
            // Starts service, runs test, stops service - all in same process
            #[cfg(all(not(dialog_test_wasm), not(target_arch = "wasm32")))]
            #[tokio::test]
            #vis async fn #ident() -> ::anyhow::Result<()> {
                use ::dialog_common::helpers::Provisionable;

                #settings_setup

                // Start the service
                let service = <#address_type as Provisionable>::start(settings)
                    .await
                    .expect("Failed to start service");
                let address: #address_type = service.address.clone();

                // Run the test in a spawned task so panics don't prevent cleanup
                let result = ::tokio::spawn(#integration_ident(address)).await;

                // Always stop the service
                service.stop().await?;

                // Propagate the result
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(e) => {
                        if e.is_panic() {
                            ::std::panic::resume_unwind(e.into_panic());
                        }
                        Err(::anyhow::anyhow!("Task failed: {}", e))
                    }
                }
            }
        }
    }

    /// Generate the wasm integration test.
    ///
    /// Runs with `RUSTFLAGS="--cfg dialog_test_wasm" cargo test`. Starts service on native,
    /// spawns wasm subprocess for test, stops service.
    fn wasm_integration_test(&self) -> proc_macro2::TokenStream {
        let IntegrationTest {
            vis,
            ident,
            name,
            address_type,
            settings_setup,
            wasm_test_name,
            feature_checks,
            ..
        } = self;

        quote! {
            // Wasm integration test: runs with `RUSTFLAGS="--cfg dialog_test_wasm" cargo test`
            // Starts service on native, spawns wasm subprocess for test, stops service
            #[cfg(all(dialog_test_wasm, not(target_arch = "wasm32")))]
            #[tokio::test]
            #vis async fn #ident() -> ::anyhow::Result<()> {
                use ::dialog_common::helpers::{Provisionable, PROVISIONED_SERVICE_ADDRESS};
                use ::std::process::Stdio;

                #settings_setup

                // Start the service
                let service = <#address_type as Provisionable>::start(settings)
                    .await
                    .expect("Failed to start service");

                // Serialize address for the wasm test
                let address = ::serde_json::to_string(&service.address)
                    .expect("Failed to serialize address");

                // Check if --nocapture was passed
                let show_output = ::std::env::var("RUST_TEST_NOCAPTURE").is_ok()
                    || ::std::env::args().any(|arg| arg == "--nocapture");

                // Check if --color=always was passed
                let color_always = ::std::env::args().any(|arg| arg == "--color=always");

                let pkg_name = env!("CARGO_PKG_NAME");

                // Build feature list using cfg!() to forward same features in the subprocess
                let mut features: Vec<&str> = Vec::new();
                #(#feature_checks)*
                let features_str = features.join(",");

                // Build RUSTFLAGS with dialog_test_wasm_inner cfg so that only wasm
                // integration tests will run.
                let existing_rustflags = ::std::env::var("RUSTFLAGS").unwrap_or_default();
                let rustflags = format!("{} --cfg dialog_test_wasm_inner", existing_rustflags);

                // Build cargo command args
                let mut args = vec![
                    "test".to_string(),
                    "-p".to_string(), pkg_name.to_string(),
                    "--target".to_string(), "wasm32-unknown-unknown".to_string(),
                    "--lib".to_string(),
                ];
                if color_always {
                    args.push("--color=always".to_string());
                }
                if !features_str.is_empty() {
                    args.push("--features".to_string());
                    args.push(features_str);
                }
                args.push(#wasm_test_name.to_string());
                args.push("--".to_string());
                args.push("--nocapture".to_string());

                // Spawn cargo test directly with env vars
                let mut cmd = ::std::process::Command::new("cargo");
                cmd.args(&args)
                    .env("RUSTFLAGS", &rustflags)
                    .env(PROVISIONED_SERVICE_ADDRESS, &address)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());

                let output = ::tokio::task::spawn_blocking(move || {
                    cmd.output().expect("Failed to execute cargo test")
                }).await.expect("Failed to join blocking task");

                // Combine stdout and stderr for display
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);

                // Show output on failure or when --nocapture was passed
                if show_output || !output.status.success() {
                    use ::std::fmt::Write;
                    use ::std::io::Write as IoWrite;
                    let mut buf = String::new();
                    let _ = writeln!(buf, "\n  ┌─ cargo {}", args.join(" "));
                    for line in stderr.lines().chain(stdout.lines()) {
                        let _ = writeln!(buf, "  │ {}", line);
                    }
                    let _ = writeln!(buf, "  └─");
                    let out = ::std::io::stderr();
                    let mut handle = out.lock();
                    let _ = handle.write_all(buf.as_bytes());
                    let _ = handle.flush();
                }

                // Stop the service
                service.stop().await?;

                if !output.status.success() {
                    panic!("Test '{}' failed in wasm", #name);
                }

                // Check if tests were actually run (not skipped)
                let combined = format!("{}{}", stdout, stderr);
                if combined.contains("no tests to run") ||
                   combined.contains("0 passed; 0 failed") {
                    panic!(
                        "Test '{}' was skipped in wasm. \
                         This usually means the test wasn't compiled with the right cfg flags.",
                        #name
                    );
                }

                Ok(())
            }
        }
    }

    /// Generate the wasm test.
    ///
    /// Compiled into wasm, invoked by the wasm integration test. Uses `option_env!`
    /// (compile-time) instead of `std::env::var` (runtime) because wasm32-unknown-unknown
    /// has no access to host environment variables at runtime.
    fn wasm_test(&self) -> proc_macro2::TokenStream {
        let IntegrationTest {
            vis,
            address_type,
            integration_ident,
            wasm_test_ident,
            ..
        } = self;

        quote! {
            // Wasm test: compiled into wasm, invoked by wasm integration test
            // Address is received via compile-time env var (option_env!) since wasm has no runtime env
            #[cfg(all(dialog_test_wasm_inner, target_arch = "wasm32", target_os = "unknown"))]
            #[wasm_bindgen_test::wasm_bindgen_test]
            #vis async fn #wasm_test_ident() -> Result<(), ::wasm_bindgen::JsValue> {
                // option_env! captures the env var at compile time and embeds it in the binary
                let source = ::std::option_env!("PROVISIONED_SERVICE_ADDRESS")
                    .ok_or_else(|| ::wasm_bindgen::JsValue::from_str(
                        "Missing compile-time env var PROVISIONED_SERVICE_ADDRESS. \
                         This test must be invoked via the wasm integration test."
                    ))?;
                let address: #address_type = ::serde_json::from_str(source)
                    .map_err(|e| ::wasm_bindgen::JsValue::from_str(&format!("Failed to deserialize: {}", e)))?;

                #integration_ident(address).await
                    .map_err(|e| ::wasm_bindgen::JsValue::from_str(&format!("{}", e)))
            }
        }
    }
}

/// Extract the parameter name and address type from an integration test
/// function so that associated service can be provisioned and test could
/// be executed with the address.
///
/// Currently we only support integration tests with a sole parameter to
/// represent a required service address.
///
/// This function extracts parameter name and its type identifier.
///
/// # Example
///
/// Given:
/// ```rs
/// async fn it_connects(server: ServerAddress) -> anyhow::Result<()> { ... }
/// ```
///
/// Returns: `(Ident("server"), Type(ServerAddress))`
///
/// Errors if source function does not have exactly one paramater.
fn extract_address_param(source: &ItemFn) -> syn::Result<(Ident, Type)> {
    let inputs = &source.sig.inputs;

    if inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            &source.sig,
            "Integration test must have exactly one parameter",
        ));
    }

    let parameter = inputs.first().unwrap();

    match parameter {
        FnArg::Typed(address) => {
            let name = match address.pat.as_ref() {
                Pat::Ident(pat) => pat.ident.clone(),
                _ => {
                    return Err(syn::Error::new_spanned(
                        &address.pat,
                        "Expected a simple identifier for the parameter",
                    ));
                }
            };
            let address_type = (*address.ty).clone();
            Ok((name, address_type))
        }
        FnArg::Receiver(_) => Err(syn::Error::new_spanned(
            parameter,
            "Integration test must not take self parameter",
        )),
    }
}
