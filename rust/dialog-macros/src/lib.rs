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

// disabling because we don't want to add crate dependencies just for this
#[cfg(not(doctest))]
/// A cross-platform test macro with automatic service provisioning.
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
/// # Usage
///
/// ## Unit tests
///
/// Tests that do not require external services:
///
/// ```rs
/// // Sync test
/// #[dialog_common::test]
/// fn it_works() {
///     assert_eq!(2 + 2, 4);
/// }
///
/// // Async test
/// #[dialog_common::test]
/// async fn it_works_async() -> anyhow::Result<()> {
///     assert_eq!(2 + 2, 4);
///     Ok(())
/// }
/// ```
///
/// Unit tests are gated with `#[cfg(not(feature = "web-integration-tests"))]` so they don't
/// run during web integration test runs (case 4 above).
///
/// ## Integration tests
///
/// Tests that need external services (S3, databases, servers, etc.):
///
/// ### Step 1: Define the address (what the test receives)
///
/// The address is serializable so it can be passed to wasm tests via environment:
///
/// ```rs
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct ServerAddress {
///     pub endpoint: String,
///     pub port: u16,
/// }
/// ```
///
/// ### Step 2: Define the provider (native only)
///
/// Use `#[dialog_common::provider]` to create a function that starts the service
/// and returns its address:
///
/// ```rs
/// use dialog_common::helpers::{Provider, Service};
///
/// pub struct LocalServer { /* ... */ }
///
/// impl Provider for LocalServer {
///     async fn stop(self) -> anyhow::Result<()> {
///         Ok(())
///     }
/// }
///
/// #[dialog_common::provider]
/// async fn server(_settings: ()) -> anyhow::Result<Service<ServerAddress, LocalServer>> {
///     let server = LocalServer { /* start server */ };
///     Ok(Service::new(
///         ServerAddress { endpoint: "...".into(), port: 8080 },
///         server
///     ))
/// }
/// ```
///
/// ### Step 3: Write the test
///
/// When a function takes an address parameter, the macro generates:
///
/// - **Native integration test** (`integration-tests` feature): Starts service, runs test, stops service
/// - **Web integration test** (`web-integration-tests` feature): Starts service, spawns wasm test, stops service
/// - **Wasm inner** (`dialog_test_wasm_integration` cfg): Deserializes address from env var, runs test
///
/// ```rs
/// #[dialog_common::test]
/// async fn it_connects_to_server(server: ServerAddress) -> anyhow::Result<()> {
///     // Use server.endpoint to connect...
///     Ok(())
/// }
/// ```
///
/// ### With custom settings
///
/// Provider settings can be customized via macro attributes:
///
/// ```rs
/// #[dialog_common::test(port = 9000, tls = true)]
/// async fn it_uses_custom_port(server: ServerAddress) -> anyhow::Result<()> {
///     assert_eq!(server.port, 9000);
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::generate(attr, item)
}

// disabling because we don't want to add crate dependencies just for this
#[cfg(not(doctest))]
/// Mark a function as a service provider for integration tests.
///
/// This macro transforms an async function returning `Service<Address, Provider>`
/// into a `Provisionable` implementation that works with the `#[dialog_common::test]` macro.
///
/// # Usage
///
/// ```rust
/// use dialog_common::helpers::Service;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct Host {
///     pub url: String,
/// }
///
/// #[derive(Debug, Clone, Default)]
/// pub struct Settings {
///     pub port: u16,   // 0 = random available port
/// }
///
/// #[dialog_common::provider]
/// async fn tcp_server(settings: Settings) -> anyhow::Result<Service<Host, (std::net::TcpListener,)>> {
///     let listener = std::net::TcpListener::bind(format!("127.0.0.1:{}", settings.port))?;
///     let addr = listener.local_addr()?;
///     Ok(Service::new(Host { url: format!("http://{}", addr) }, (listener,)))
/// }
/// ```
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
