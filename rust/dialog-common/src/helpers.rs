//! Testing utilities for integration tests with provisioned resources.
//!
//! This module provides traits and helpers for writing integration tests that require
//! external resources (like S3 servers, databases, etc.) where the resource is started
//! in an "outer" test and the actual test logic runs in an "inner" test that receives
//! serialized resource state.
//!
//! # Example
//!
//! ```no_run
//! use dialog_common::helpers::{Resource, Provider};
//! use serde::{Deserialize, Serialize};
//!
//! // The resource passed to inner tests (serialized)
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! pub struct TestResource {
//!     pub endpoint: String,
//! }
//!
//! // The provider that creates resources
//! pub struct TestServer {
//!     endpoint: String,
//! }
//!
//! impl Resource for TestResource {
//!     type Settings = ();
//!     type Provider = TestServer;
//!
//!     async fn start(_settings: Self::Settings) -> anyhow::Result<Self::Provider> {
//!         Ok(TestServer { endpoint: "http://localhost:8080".to_string() })
//!     }
//! }
//!
//! impl Provider for TestServer {
//!     type Resource = TestResource;
//!
//!     fn provide(&self) -> TestResource {
//!         TestResource { endpoint: self.endpoint.clone() }
//!     }
//! }
//!
//! // Then in tests:
//! #[dialog_common::test]
//! async fn my_test(env: TestResource) -> anyhow::Result<()> {
//!     assert!(!env.endpoint.is_empty());
//!     Ok(())
//! }
//! ```

use serde::{Serialize, de::DeserializeOwned};

/// Environment variable name used to pass serialized resource to inner tests.
pub const PROVISIONED_ENV_VAR: &str = "PROVISIONED_ENV";

/// A type representing the resource passed to inner tests.
///
/// This trait is implemented on the "resource" type that gets serialized and passed
/// to the inner test. It declares which [`Provider`] can create it.
pub trait Resource: Sized + Serialize + DeserializeOwned {
    /// Settings to configure the provider. Must implement Default.
    type Settings: Default;
    /// The provider type that can create this resource.
    type Provider: Provider<Resource = Self>;

    /// Start a provider with the given settings.
    fn start(
        settings: Self::Settings,
    ) -> impl std::future::Future<Output = anyhow::Result<Self::Provider>> + Send;
}

/// A provider that can start and stop resources for testing.
///
/// This trait is implemented on the "server" or "resource" type that stays alive
/// during the inner test execution.
pub trait Provider: Sized {
    /// The resource type this provider produces.
    type Resource: Resource<Provider = Self>;

    /// Provide the resource to pass to the inner test.
    ///
    /// This is called after `start()` to extract the serializable state
    /// that will be passed to the inner test via environment variables.
    fn provide(&self) -> Self::Resource;

    /// Stop the provider and clean up resources.
    ///
    /// This is called after the inner test completes (success or failure).
    /// The default implementation simply drops self.
    fn stop(self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
        async { Ok(()) }
    }
}

/// Deserialize resource from the PROVISIONED_ENV env var.
///
/// This is called by the inner test to retrieve the resource set by the outer test.
pub fn resource<R: Resource>() -> anyhow::Result<R> {
    let json = std::env::var(PROVISIONED_ENV_VAR)
        .map_err(|_| anyhow::anyhow!("Missing {} environment variable", PROVISIONED_ENV_VAR))?;

    serde_json::from_str(&json)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize resource: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    /// Minimal test resource for macro testing
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TestResource {
        pub value: String,
    }

    /// Minimal provider for macro testing
    pub struct TestProvider;

    impl Resource for TestResource {
        type Settings = ();
        type Provider = TestProvider;

        async fn start(_settings: Self::Settings) -> anyhow::Result<Self::Provider> {
            Ok(TestProvider)
        }
    }

    impl Provider for TestProvider {
        type Resource = TestResource;

        fn provide(&self) -> TestResource {
            TestResource {
                value: "test".to_string(),
            }
        }
    }

    // --- Resource with Settings ---

    /// Resource that uses configurable settings
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ConfiguredResource {
        pub endpoint: String,
        pub bucket: String,
    }

    /// Settings for ConfiguredResource
    #[derive(Debug, Clone, Default)]
    pub struct ConfiguredSettings {
        pub endpoint: String,
        pub bucket: String,
    }

    /// Provider that uses settings to configure the resource
    pub struct ConfiguredProvider {
        endpoint: String,
        bucket: String,
    }

    impl Resource for ConfiguredResource {
        type Settings = ConfiguredSettings;
        type Provider = ConfiguredProvider;

        async fn start(settings: Self::Settings) -> anyhow::Result<Self::Provider> {
            Ok(ConfiguredProvider {
                endpoint: if settings.endpoint.is_empty() {
                    "http://default:9000".to_string()
                } else {
                    settings.endpoint
                },
                bucket: if settings.bucket.is_empty() {
                    "default-bucket".to_string()
                } else {
                    settings.bucket
                },
            })
        }
    }

    impl Provider for ConfiguredProvider {
        type Resource = ConfiguredResource;

        fn provide(&self) -> ConfiguredResource {
            ConfiguredResource {
                endpoint: self.endpoint.clone(),
                bucket: self.bucket.clone(),
            }
        }
    }

    // --- Resource with actual native provider ---

    /// Resource that represents a running TCP server
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ServerResource {
        pub port: u16,
    }

    /// Settings for the server
    #[derive(Debug, Clone, Default)]
    pub struct ServerSettings {
        pub port: u16,
    }

    /// A simple TCP listener provider (native only)
    #[cfg(not(target_arch = "wasm32"))]
    pub struct TcpServerProvider {
        listener: std::net::TcpListener,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl Resource for ServerResource {
        type Settings = ServerSettings;
        type Provider = TcpServerProvider;

        async fn start(settings: Self::Settings) -> anyhow::Result<Self::Provider> {
            // Bind to localhost with specified port (0 = random available port)
            let addr = format!("127.0.0.1:{}", settings.port);
            let listener = std::net::TcpListener::bind(&addr)?;
            Ok(TcpServerProvider { listener })
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl Provider for TcpServerProvider {
        type Resource = ServerResource;

        fn provide(&self) -> ServerResource {
            ServerResource {
                port: self.listener.local_addr().unwrap().port(),
            }
        }
    }

    /// Tests that the macro generates working outer/inner test pairs.
    /// This test verifies that doc comments don't break test attribute generation.
    #[dialog_common::test]
    async fn it_runs_provisioned_test(env: TestResource) -> anyhow::Result<()> {
        assert_eq!(env.value, "test");
        Ok(())
    }

    /// Tests the custom macro variant with user-provided test attributes.
    #[dialog_common::test::custom]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    async fn it_runs_custom_provisioned_test(env: TestResource) -> anyhow::Result<()> {
        assert_eq!(env.value, "test");
        Ok(())
    }

    /// Tests a simple (non-provisioned) test with default attributes.
    #[dialog_common::test]
    async fn it_runs_simple_test() {
        assert_eq!(2 + 2, 4);
    }

    /// Tests a simple test with custom attributes.
    #[dialog_common::test::custom]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
    async fn it_runs_custom_simple_test() {
        assert_eq!(3 + 3, 6);
    }

    /// Test that only runs in native
    #[dialog_common::test::custom]
    #[tokio::test]
    async fn it_runs_only_in_native() {
        assert_eq!("native", "native");
    }

    /// Test that only runs in wasm
    #[dialog_common::test::custom]
    #[wasm_bindgen_test::wasm_bindgen_test]
    async fn it_runs_only_in_wasm() {
        assert_eq!("wasm32", "wasm32");
    }

    // --- Tests with settings ---

    /// Tests provisioned test with default settings
    #[dialog_common::test]
    async fn it_uses_default_settings(env: ConfiguredResource) -> anyhow::Result<()> {
        assert_eq!(env.endpoint, "http://default:9000");
        assert_eq!(env.bucket, "default-bucket");
        Ok(())
    }

    /// Tests provisioned test with custom settings
    #[dialog_common::test(endpoint = "http://custom:8080", bucket = "my-bucket")]
    async fn it_uses_custom_settings(env: ConfiguredResource) -> anyhow::Result<()> {
        assert_eq!(env.endpoint, "http://custom:8080");
        assert_eq!(env.bucket, "my-bucket");
        Ok(())
    }

    /// Tests provisioned test with partial settings (only bucket)
    #[dialog_common::test(bucket = "partial-bucket")]
    async fn it_uses_partial_settings(env: ConfiguredResource) -> anyhow::Result<()> {
        assert_eq!(env.endpoint, "http://default:9000");
        assert_eq!(env.bucket, "partial-bucket");
        Ok(())
    }

    // --- Tests with actual native provider ---

    /// Tests a real TCP server provider (native only)
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_starts_tcp_server(env: ServerResource) -> anyhow::Result<()> {
        // The provider started a real TCP listener, we got the port
        assert!(env.port > 0);
        // Try to connect to verify it's actually listening
        let addr = format!("127.0.0.1:{}", env.port);
        std::net::TcpStream::connect(&addr)?;
        Ok(())
    }

    /// Tests TCP server with specific port setting
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test(port = 0u16)]
    async fn it_starts_tcp_server_on_random_port(env: ServerResource) -> anyhow::Result<()> {
        // port = 0 means OS picks a random available port
        assert!(env.port > 0);
        Ok(())
    }
}
