#![warn(missing_docs)]

//! This crate constitutes a library of light weight helpers that are shared
//! across multiple other crates. Their chief quality is that they have
//! virtually zero dependencies.

// Allow the crate to refer to itself as `dialog_common`
extern crate self as dialog_common;

mod sync;
pub use sync::*;

mod hash;
pub use hash::*;

#[cfg(feature = "helpers")]
pub mod helpers;

/// Test macro for cross-platform testing with resource provisioning.
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
/// #[dialog_common::test]
/// async fn it_starts_server(host: Host) -> anyhow::Result<()> {
///     assert!(host.url.starts_with("http://127.0.0.1:"));
///     Ok(())
/// }
///
/// // Test with custom settings
/// #[dialog_common::test(port = 8080)]
/// async fn it_uses_custom_port(host: Host) -> anyhow::Result<()> {
///   assert!(host.url.contains(":8080"));
///   Ok(())
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
/// See the macro documentation for the full CI test matrix.
#[cfg(feature = "helpers")]
pub use dialog_common_macros::test;

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
pub use dialog_common_macros::provider;
