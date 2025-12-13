#![warn(missing_docs)]

//! Procedural macros for the dialog project.
//!
//! This crate provides procedural macro implementations for testing and service
//! provisioning across dialog crates. Macros are re-exported through `dialog_common`
//! (with the `helpers` feature) for convenient access.
//!
//! Procedural macros must be defined in their own crate, which is why these live
//! here rather than in the crates that use them.

use proc_macro::TokenStream;
mod provider;
mod test;

/// A cross-platform test macro with automatic service provisioning.
///
/// This macro is re-exported as [`dialog_common::test`] (with the `helpers` feature).
/// See that documentation for usage examples.
///
/// # CI Test Matrix
///
/// The macro generates code that supports these CI configurations:
///
/// 1. `cargo test` - Unit tests run natively
/// 2. `cargo test --target wasm32-unknown-unknown` - Unit tests run in wasm
/// 3. `cargo test --features integration-tests` - Unit tests + integration tests run natively
/// 4. `cargo test --features web-integration-tests` - Integration tests run in wasm
///    (unit tests skipped, native provider spawns wasm inner tests)
///
/// # Generated Code
///
/// For **unit tests** (no parameters): Uses `#[test]` on native, `#[wasm_bindgen_test]` on wasm.
/// Gated with `#[cfg(not(feature = "web-integration-tests"))]` to skip during wasm integration runs.
///
/// For **integration tests** (with address parameter):
/// - **Native** (`integration-tests` feature): Starts service, runs test, stops service
/// - **Web** (`web-integration-tests` feature): Starts service, spawns wasm subprocess, stops service
/// - **Wasm inner** (`dialog_test_wasm_integration` cfg): Deserializes address from env var, runs test
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::generate(attr, item)
}

/// Mark a function as a service provider for integration tests.
///
/// This macro is re-exported as [`dialog_common::provider`] (with the `helpers` feature).
/// See that documentation for usage examples.
///
/// Transforms an async function returning `Service<Address, Provider>` into a
/// `Provisionable` implementation that works with the `#[dialog_common::test]` macro.
///
/// # Generated Code
///
/// The macro generates:
/// 1. The original provider function (native-only via `#[cfg(not(target_arch = "wasm32"))]`)
/// 2. A `Provisionable` trait implementation on the address type
///
/// This allows the address type to be used with `#[dialog_common::test]`.
#[proc_macro_attribute]
pub fn provider(attr: TokenStream, item: TokenStream) -> TokenStream {
    provider::generate(attr, item)
}
