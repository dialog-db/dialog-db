#![warn(missing_docs)]

//! This crate constitutes a library of light weight helpers that are shared
//! across multiple other crates. Their chief quality is that they have
//! virtually zero dependencies.

// Allow the crate to refer to itself as `dialog_common`
extern crate self as dialog_common;

/// Cross-platform logging macro that uses `console.log` on web and `println!` on native.
///
/// # Examples
///
/// ```
/// use dialog_common::log;
///
/// log!("Hello, world!");
/// log!("Value: {}", 42);
/// log!("Multiple values: {} and {}", "foo", "bar");
/// ```
#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {{
        #[cfg(target_arch = "wasm32")]
        {
            web_sys::console::log_1(&format!($($arg)*).into());
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            println!($($arg)*);
        }
    }};
}

mod sync;
pub use sync::*;

pub mod bytes;
pub use bytes::Bytes;

mod hash;
pub use hash::*;

/// Async utilities for cross-platform task management.
pub mod r#async;
pub use r#async::*;

#[cfg(feature = "helpers")]
pub mod helpers;

/// Test macro for cross-platform testing with optional service provisioning.
///
/// # Unit Tests
///
/// Tests **without parameters** are unit tests. They run on both native and wasm targets.
///
/// ```rust
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
/// Run unit tests with:
/// - `cargo test` - native
/// - `cargo test --target wasm32-unknown-unknown` - wasm
///
/// # Integration Tests
///
/// Tests **with a parameter** are integration tests. The parameter type determines
/// which service is provisioned, and it **must** implement [`helpers::Provisionable`].
/// Use [`#[dialog_common::provider]`](macro@provider) to generate this implementation.
///
/// Service providers (databases, servers, etc.) run on native, so integration tests
/// require the `integration-tests` or `web-integration-tests` feature flag. With
/// `web-integration-tests`, the provider runs natively while the test code itself
/// executes in wasm, allowing you to test browser-specific behavior against real services.
///
/// ```rust
/// use dialog_common::helpers::Service;
/// use serde::{Deserialize, Serialize};
///
/// // 1. Define the address type (passed to tests, must be serializable)
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct Host {
///     pub url: String,
/// }
///
/// // 2. Define settings for the provider (must impl Default)
/// #[derive(Debug, Clone, Default)]
/// pub struct Settings {
///     pub port: u16,
/// }
///
/// // 3. Define the provider using #[dialog_common::provider]
/// //    This generates `impl Provisionable for Host`
/// #[dialog_common::provider]
/// async fn tcp_server(settings: Settings) -> anyhow::Result<Service<Host, (std::net::TcpListener,)>> {
///     let listener = std::net::TcpListener::bind(format!("127.0.0.1:{}", settings.port))?;
///     let addr = listener.local_addr()?;
///     Ok(Service::new(Host { url: format!("http://{}", addr) }, (listener,)))
/// }
///
/// // 4. Write integration tests - the parameter makes it an integration test
/// #[dialog_common::test]
/// async fn it_starts_server(host: Host) -> anyhow::Result<()> {
///     assert!(host.url.starts_with("http://127.0.0.1:"));
///     Ok(())
/// }
///
/// // Provider settings can be customized via macro attributes
/// #[dialog_common::test(port = 8080)]
/// async fn it_uses_custom_port(host: Host) -> anyhow::Result<()> {
///     assert!(host.url.contains(":8080"));
///     Ok(())
/// }
/// ```
///
/// Run integration tests with:
/// - `cargo test --features integration-tests` - native only
/// - `cargo test --features web-integration-tests` - spawns wasm subprocess for each test
#[cfg(feature = "helpers")]
pub use dialog_macros::test;

/// Provider macro for defining service providers for integration tests.
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
/// See the macro documentation for details.
#[cfg(feature = "helpers")]
pub use dialog_macros::provider;
