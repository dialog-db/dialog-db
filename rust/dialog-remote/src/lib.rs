#![warn(missing_docs)]

pub mod s3;

mod error;
pub use error::*;

#[cfg(feature = "cloudflare")]
mod cloudflare;
#[cfg(feature = "cloudflare")]
pub use cloudflare::*;
