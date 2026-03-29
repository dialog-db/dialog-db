//! Environment composition and builder.
//!
//! Use [`Builder`] to configure and open an environment:
//!
//! ```no_run
//! # #[cfg(not(target_arch = "wasm32"))]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use dialog_artifacts::environment::Builder;
//! use dialog_artifacts::Operator;
//!
//! // Defaults — system storage, default profile, unique operator
//! let env = Builder::default().build().await?;
//!
//! // With derived operator
//! let env = Builder::default()
//!     .operator(b"alice")
//!     .build()
//!     .await?;
//!
//! // With UCAN delegation grant
//! # #[cfg(feature = "ucan")]
//! # {
//! use dialog_artifacts::environment::Ucan;
//! let env = Builder::default()
//!     .operator(b"alice")
//!     .grant(Ucan::delegate(&dialog_capability::Subject::any()))
//!     .build()
//!     .await?;
//! # }
//! # Ok(())
//! # }
//! ```

mod builder;
mod error;
pub mod grant;
mod provider;

pub use builder::Builder;
#[cfg(feature = "ucan")]
pub use dialog_capability::ucan::Ucan;
pub use error::OpenError;
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

/// The platform-specific environment type.
///
/// On native: `Environment<Credentials, FileSystem, Remote>`
/// On web: `Environment<Credentials, IndexedDb, Remote>`
#[cfg(not(target_arch = "wasm32"))]
pub type DialogEnvironment = NativeEnvironment;

/// The platform-specific environment type.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type DialogEnvironment = WebEnvironment;
