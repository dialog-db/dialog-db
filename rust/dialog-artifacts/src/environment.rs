//! Environment composition and builder.
//!
//! Use [`Builder`] to configure and open an environment:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use dialog_artifacts::environment::Builder;
//!
//! let env = Builder::default().build().await?;
//! # Ok(())
//! # }
//! ```

mod builder;
mod error;
pub mod grant;
mod provider;

pub use builder::Builder;
pub use error::OpenError;
#[cfg(feature = "ucan")]
pub use grant::ucan::Ucan;
pub use provider::Environment;

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod web;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use web::*;

#[cfg(any(test, feature = "helpers"))]
mod test;
#[cfg(any(test, feature = "helpers"))]
pub use test::*;

/// The platform-specific environment type.
///
/// On native: `Environment<Credentials, FileSystem, Remote>`
/// On web: `Environment<Credentials, IndexedDb, Remote>`
#[cfg(not(target_arch = "wasm32"))]
pub type DialogEnvironment = NativeEnvironment;

/// The platform-specific environment type.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type DialogEnvironment = WebEnvironment;
