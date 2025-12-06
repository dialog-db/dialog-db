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
/// ```no_run
/// #[dialog_common::test]
/// async fn it_works() {
///     assert_eq!(2 + 2, 4);
/// }
/// ```
///
/// ## With provisioned resources
///
/// When a function takes a `Resource` parameter, the macro generates:
///
/// 1. **Outer test (native)**: Starts the provider, serializes environment,
///    invokes inner test, then stops the provider.
///
/// 2. **Inner test**: Deserializes environment and runs test logic.
///
/// ```no_run
/// use dialog_common::helpers::Resource;
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// pub struct S3Resource {
///     pub endpoint: String,
///     pub bucket: String,
/// }
///
/// impl Resource for S3Resource {
///     type Provider = S3Server;
///     // ...
/// }
///
/// #[dialog_common::test]
/// async fn it_stores_and_retrieves(env: S3Resource) -> anyhow::Result<()> {
///     let backend = S3::open(&env.endpoint, &env.bucket, Session::Public);
///     // ...
///     Ok(())
/// }
///
/// // With custom settings (fields must be pub on Settings type):
/// #[dialog_common::test(bucket = "custom-bucket")]
/// async fn it_uses_custom_bucket(env: S3Resource) -> anyhow::Result<()> {
///     // ...
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
/// ```no_run
/// // With custom tokio configuration
/// #[dialog_common::test::custom]
/// #[tokio::test(flavor = "multi_thread")]
/// async fn it_needs_multi_thread(env: S3Resource) -> anyhow::Result<()> {
///     // ...
///     Ok(())
/// }
///
/// // With settings
/// #[dialog_common::test::custom(bucket = "custom")]
/// #[tokio::test]
/// async fn it_uses_custom_bucket(env: S3Resource) -> anyhow::Result<()> {
///     // ...
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn test_custom(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::generate_custom(attr, item)
}
