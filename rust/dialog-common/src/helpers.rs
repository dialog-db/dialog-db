//! Testing utilities for cross-target integration tests with provisioned services.
//!
//! This module enables writing integration tests where:
//! - A **native** outer test starts a service (e.g., TCP server, database)
//! - The test body runs on **any target** (native or wasm) using the service
//!
//! # Example
//!
//! ```no_run
//! use dialog_common::helpers::Service;
//! use serde::{Deserialize, Serialize};
//!
//! // Address passed to tests (must be serializable)
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! pub struct ServerAddress {
//!     pub url: String,
//! }
//!
//! // Settings to configure the provider (must impl Default)
//! #[derive(Debug, Clone, Default)]
//! pub struct ServerSettings {
//!     pub port: u16,   // 0 = random available port
//!     pub host: String,
//! }
//!
//! #[dialog_common::provider]
//! async fn tcp_server(settings: ServerSettings) -> anyhow::Result<Service<ServerAddress, (std::net::TcpListener,)>> {
//!     let host = if settings.host.is_empty() { "127.0.0.1" } else { &settings.host };
//!     let listener = std::net::TcpListener::bind(format!("{}:{}", host, settings.port))?;
//!     let addr = listener.local_addr()?;
//!     Ok(Service::new(ServerAddress { url: format!("http://{}", addr) }, (listener,)))
//! }
//!
//! // Test with default settings
//! #[dialog_common::test]
//! async fn it_starts_server(addr: ServerAddress) -> anyhow::Result<()> {
//!     assert!(addr.url.starts_with("http://127.0.0.1:"));
//!     Ok(())
//! }
//!
//! // Test with custom settings
//! #[dialog_common::test(port = 8080u16)]
//! async fn it_uses_custom_port(addr: ServerAddress) -> anyhow::Result<()> {
//!     assert!(addr.url.contains(":8080"));
//!     Ok(())
//! }
//! # fn main() {}
//! ```
//!
//! # How It Works
//!
//! - **Native mode** (`--features integration-tests`): Provider starts, test runs, provider stops
//! - **Wasm mode** (`--features web-integration-tests`): Native outer test starts provider,
//!   serializes address to env var, spawns wasm inner test which deserializes and runs

use async_trait::async_trait;
use dialog_common::ConditionalSend;
use serde::{Serialize, de::DeserializeOwned};

/// Environment variable name used to pass serialized address to inner tests.
pub const PROVISIONED_SERVICE_ADDRESS: &str = "PROVISIONED_SERVICE_ADDRESS";

/// A provider that manages the lifecycle of a service.
///
/// Implement this trait on types that hold service state and need cleanup.
/// The default `stop` implementation simply drops self, which works for
/// types that clean up in their `Drop` implementation.
///
/// For async cleanup, override the `stop` method.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Provider: Sized + ConditionalSend {
    /// Stop the provider and clean up resources.
    ///
    /// The default implementation simply drops self.
    async fn stop(self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Unit type is a valid provider (no cleanup needed).
impl Provider for () {}

/// Any type wrapped in a tuple is a valid provider (cleanup via drop).
impl<T: ConditionalSend> Provider for (T,) {}

/// A running service with an address and a provider.
///
/// - `Address`: Serializable data passed to tests (endpoint, port, credentials, etc.)
/// - `P`: The provider that manages the service lifecycle
///
/// When the service is stopped, the provider's `stop` method is called.
pub struct Service<Address, P: Provider = ()> {
    /// The address of the service, passed to tests.
    pub address: Address,
    provider: P,
}

impl<A, P: Provider> Service<A, P> {
    /// Create a new service with the given address and provider.
    pub fn new(address: A, provider: P) -> Self {
        Service { address, provider }
    }
}

impl<A, P: Provider> Service<A, P> {
    /// Stop the service by stopping its provider.
    pub async fn stop(self) -> anyhow::Result<()> {
        self.provider.stop().await
    }
}

impl<A: Serialize, P: Provider> Service<A, P> {
    /// Serialize the address to JSON.
    pub fn address_json(&self) -> anyhow::Result<String> {
        serde_json::to_string(&self.address)
            .map_err(|e| anyhow::anyhow!("Failed to serialize address: {}", e))
    }
}

/// A type that can be provisioned for testing.
///
/// This trait is implemented on the "address" type that gets serialized and passed
/// to the inner test. The `#[dialog_common::provider]` macro generates this impl.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Provisionable: Sized + Serialize + DeserializeOwned + Clone {
    /// Settings to configure the provider. Must implement Default.
    type Settings: Default;
    /// The provider type that manages the service.
    type Provider: Provider;

    /// Start a service with the given settings.
    async fn start(settings: Self::Settings) -> anyhow::Result<Service<Self, Self::Provider>>;
}

/// Deserialize an address from the PROVISIONED_SERVICE_ADDRESS env var.
///
/// This is called by the inner test to retrieve the address set by the outer test.
pub fn address<A: DeserializeOwned>() -> anyhow::Result<A> {
    let json = std::env::var(PROVISIONED_SERVICE_ADDRESS).map_err(|_| {
        anyhow::anyhow!(
            "Missing {} environment variable",
            PROVISIONED_SERVICE_ADDRESS
        )
    })?;

    serde_json::from_str(&json).map_err(|e| anyhow::anyhow!("Failed to deserialize address: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    // Simple test resource (no real provider)

    /// Minimal test address for macro testing
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TestAddress {
        pub value: String,
    }

    /// Minimal provider for macro testing
    pub struct TestServer;

    impl Provider for TestServer {}

    /// Provider function for TestAddress
    #[dialog_common::provider]
    async fn test_service(_settings: ()) -> anyhow::Result<Service<TestAddress, TestServer>> {
        Ok(Service::new(
            TestAddress {
                value: "test".to_string(),
            },
            TestServer,
        ))
    }

    // Resource with Settings

    /// Address that uses configurable settings
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ConfiguredAddress {
        pub endpoint: String,
        pub bucket: String,
    }

    /// Settings for ConfiguredAddress (native-only, used by provider)
    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Debug, Clone, Default)]
    pub struct ConfiguredSettings {
        pub endpoint: String,
        pub bucket: String,
    }

    /// Provider that uses settings (empty struct since this is just a demo)
    #[cfg(not(target_arch = "wasm32"))]
    pub struct ConfiguredServer;
    #[cfg(not(target_arch = "wasm32"))]
    impl Provider for ConfiguredServer {}

    /// Provider function for ConfiguredAddress
    #[dialog_common::provider]
    async fn configured_service(
        settings: ConfiguredSettings,
    ) -> anyhow::Result<Service<ConfiguredAddress, ConfiguredServer>> {
        let endpoint = if settings.endpoint.is_empty() {
            "http://default:9000".to_string()
        } else {
            settings.endpoint
        };
        let bucket = if settings.bucket.is_empty() {
            "default-bucket".to_string()
        } else {
            settings.bucket
        };
        Ok(Service::new(
            ConfiguredAddress { endpoint, bucket },
            ConfiguredServer,
        ))
    }

    // Resource with actual native provider

    /// Address that represents a running TCP server
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ServerAddress {
        pub port: u16,
    }

    /// Settings for the server
    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Debug, Clone, Default)]
    pub struct ServerSettings {
        pub port: u16,
    }

    /// Provider function for ServerAddress
    #[dialog_common::provider]
    async fn tcp_server(
        settings: ServerSettings,
    ) -> anyhow::Result<Service<ServerAddress, (std::net::TcpListener,)>> {
        let addr = format!("127.0.0.1:{}", settings.port);
        let listener = std::net::TcpListener::bind(&addr)?;
        let port = listener.local_addr()?.port();
        Ok(Service::new(ServerAddress { port }, (listener,)))
    }

    /// Tests that the macro generates working outer/inner test pairs.
    #[dialog_common::test]
    async fn it_runs_provisioned_test(addr: TestAddress) -> anyhow::Result<()> {
        assert_eq!(addr.value, "test");
        Ok(())
    }

    /// Tests a simple (non-provisioned) test with default attributes.
    #[dialog_common::test]
    async fn it_runs_simple_test() {
        assert_eq!(2 + 2, 4);
    }

    /// Tests provisioned test with default settings
    #[dialog_common::test]
    async fn it_uses_default_settings(addr: ConfiguredAddress) -> anyhow::Result<()> {
        assert_eq!(addr.endpoint, "http://default:9000");
        assert_eq!(addr.bucket, "default-bucket");
        Ok(())
    }

    /// Tests provisioned test with custom settings
    #[dialog_common::test(endpoint = "http://custom:8080", bucket = "my-bucket")]
    async fn it_uses_custom_settings(addr: ConfiguredAddress) -> anyhow::Result<()> {
        assert_eq!(addr.endpoint, "http://custom:8080");
        assert_eq!(addr.bucket, "my-bucket");
        Ok(())
    }

    /// Tests provisioned test with partial settings (only bucket)
    #[dialog_common::test(bucket = "partial-bucket")]
    async fn it_uses_partial_settings(addr: ConfiguredAddress) -> anyhow::Result<()> {
        assert_eq!(addr.endpoint, "http://default:9000");
        assert_eq!(addr.bucket, "partial-bucket");
        Ok(())
    }

    /// Tests provisioned test with struct destructuring in the parameter
    #[dialog_common::test]
    async fn it_supports_destructuring(
        ConfiguredAddress { endpoint, bucket }: ConfiguredAddress,
    ) -> anyhow::Result<()> {
        assert_eq!(endpoint, "http://default:9000");
        assert_eq!(bucket, "default-bucket");
        Ok(())
    }

    /// Tests unit test with lifetime parameter (Rust's #[test] doesn't support
    /// type generics, but does support lifetime parameters)
    #[dialog_common::test]
    fn it_supports_lifetimes<'a>() {
        let value: &'a str = "test";
        assert_eq!(value, "test");
    }

    // --- Tests with actual native provider ---
    // These tests are native-only because TcpServer can't run in wasm.
    // They're also skipped in web integration mode (web-integration-tests feature) since
    // there's no wasm inner test to spawn.

    /// Tests a real TCP server provider (native only)
    #[cfg(all(not(target_arch = "wasm32"), not(feature = "web-integration-tests")))]
    #[dialog_common::test]
    async fn it_starts_tcp_server(addr: ServerAddress) -> anyhow::Result<()> {
        assert!(addr.port > 0);
        let addr_str = format!("127.0.0.1:{}", addr.port);
        std::net::TcpStream::connect(&addr_str)?;
        Ok(())
    }

    /// Tests TCP server with specific port setting
    #[cfg(all(not(target_arch = "wasm32"), not(feature = "web-integration-tests")))]
    #[dialog_common::test(port = 0u16)]
    async fn it_starts_tcp_server_on_random_port(addr: ServerAddress) -> anyhow::Result<()> {
        assert!(addr.port > 0);
        Ok(())
    }
}
