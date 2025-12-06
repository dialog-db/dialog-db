#![warn(missing_docs)]

//! This crate constitutes a library of light weight helpers that are shared
//! across multiple other crates. Their chief quality is that they have
//! virtually zero dependencies.

// Allow the crate to refer to itself as `dialog_common` in generated macro code.
// This is needed because our test macros expand to `::dialog_common::helpers::...`.
#[cfg(feature = "helpers")]
extern crate self as dialog_common;

mod sync;
pub use sync::*;

mod hash;
pub use hash::*;

#[cfg(feature = "helpers")]
pub mod helpers;

/// Test utilities and macros for provisioned testing.
///
/// Use `#[dialog_common::test]` for default test attributes, or
/// `#[dialog_common::test::custom]` when you need custom test framework configuration.
#[cfg(feature = "helpers")]
pub use dialog_common_macros::test;

/// Additional test macros for custom configurations.
#[cfg(feature = "helpers")]
pub mod test {
    /// A test macro without default test framework attributes.
    ///
    /// Use this when you want to provide your own `#[tokio::test]` or other
    /// test framework attributes with custom configuration.
    pub use dialog_common_macros::test_custom as custom;
}
