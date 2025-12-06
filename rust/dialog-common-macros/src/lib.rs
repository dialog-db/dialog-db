#![warn(missing_docs)]

//! Procedural macros for dialog-db testing.
//!
//! This crate provides macros for writing tests that can run across different
//! targets (native, WASM, etc.) with optional provisioned resources.
//!
//! # Macros
//!
//! - [`test`] - Adds default test framework attributes (`tokio::test` / `wasm_bindgen_test`)
//! - [`test_custom`] - Just provisioning, no test attributes (bring your own)
//!
//! These are re-exported from `dialog_common` as:
//! - `#[dialog_common::test]` - Default macro with test attributes
//! - `#[dialog_common::test::custom]` - Custom macro without test attributes

use proc_macro::TokenStream;
mod test;

// disabling because we don't want to add crate dependencies just for this
#[cfg(not(doctest))]
/// A cross-platform test macro with default test framework attributes.
///
/// This macro always adds the default test framework attributes:
/// - `#[tokio::test]` for native targets
/// - `#[wasm_bindgen_test]` for WASM targets
///
/// If you need custom test framework attributes, use `#[dialog_common::test::custom]` instead.
///
/// # Usage
///
/// ## Simple test (no provisioning)
///
/// For tests that don't require external resources:
///
/// ```rs
/// #[dialog_common::test]
/// async fn it_works() {
///     assert_eq!(2 + 2, 4);
/// }
/// ```
///
/// ## With provisioned resources
///
/// For tests that need external infrastructure (servers, databases, etc.), you can
/// define a `Resource` type that describes what the test needs and a `Provider` that
/// sets it up.
///
/// ### Step 1: Define the resource (what the test receives)
///
/// The resource is serializable so it can be passed to inner tests via environment:
///
/// ```rs
/// use serde::{Deserialize, Serialize};
///
/// /// Connection info for a test server.
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct TestServer {
///     pub endpoint: String,
///     pub port: u16,
/// }
/// ```
///
/// ### Step 2: Define the provider (native only)
///
/// The provider runs on native only and handles setup/teardown. Use `#[cfg]` to
/// exclude it from WASM builds:
///
/// ```rs
/// use serde::{Deserialize, Serialize};
/// use dialog_common::helpers::{Provider, Resource};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct TestServer {
///     pub endpoint: String,
///     pub port: u16,
/// }
///
/// // Provider is native-only (starts actual servers)
/// #[cfg(not(target_arch = "wasm32"))]
/// pub struct LocalServer {
///     handle: tokio::task::JoinHandle<()>,
///     port: u16,
/// }
///
/// #[cfg(not(target_arch = "wasm32"))]
/// impl Provider for LocalServer {
///     type Resource = TestServer;
///     type Settings = ();
///
///     async fn start(_settings: Self::Settings) -> anyhow::Result<Self> {
///         // Start server on random port...
///         let port = 8080; // simplified
///         Ok(Self { handle: todo!(), port })
///     }
///
///     fn resource(&self) -> Self::Resource {
///         TestServer {
///             endpoint: format!("http://127.0.0.1:{}", self.port),
///             port: self.port,
///         }
///     }
///
///     async fn stop(self) -> anyhow::Result<()> {
///         self.handle.abort();
///         Ok(())
///     }
/// }
///
/// // Resource impl is also native-only (references the Provider type)
/// #[cfg(not(target_arch = "wasm32"))]
/// impl Resource for TestServer {
///     type Provider = LocalServer;
/// }
/// ```
///
/// ### Step 3: Write the test
///
/// When a function takes a `Resource` parameter, the macro generates:
///
/// 1. **Outer test (native only)**: Starts the provider, serializes the resource
///    to an environment variable, invokes the inner test as a subprocess, then
///    stops the provider.
///
/// 2. **Inner test (any target)**: Deserializes the resource from the environment
///    variable and runs the test logic.
///
/// ```rs
/// #[dialog_common::test]
/// async fn it_connects_to_server(server: TestServer) -> anyhow::Result<()> {
///     // Use server.endpoint to connect...
///     Ok(())
/// }
/// ```
///
/// ### With custom settings
///
/// Provider settings can be customized via macro attributes. Settings must have
/// `pub` fields for macro access:
///
/// ```rs
/// #[derive(Default)]
/// pub struct ServerSettings {
///     pub port: u16,
///     pub tls: bool,
/// }
/// ```
///
/// Override specific fields in the test:
///
/// ```rs
/// #[dialog_common::test(port = 9000u16, tls = true)]
/// async fn it_uses_custom_port(server: TestServer) -> anyhow::Result<()> {
///     assert_eq!(server.port, 9000);
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::generate(attr, item)
}

/// A cross-platform test macro without default test framework attributes.
///
/// Use this when you want to provide your own test framework attributes.
/// This macro only handles resource provisioning (outer/inner test setup).
///
/// Re-exported as `#[dialog_common::test::custom]`.
///
/// # Usage
///
/// With custom tokio configuration:
///
/// ```rs
/// #[dialog_common::test::custom]
/// #[tokio::test(flavor = "multi_thread")]
/// async fn it_needs_multi_thread(server: TestServer) -> anyhow::Result<()> {
///     // Test logic here...
///     Ok(())
/// }
/// ```
///
/// With settings:
///
/// ```rs
/// #[dialog_common::test::custom(port = 9000u16)]
/// #[tokio::test]
/// async fn it_uses_custom_port(server: TestServer) -> anyhow::Result<()> {
///     // Test logic here...
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn test_custom(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::generate_custom(attr, item)
}
