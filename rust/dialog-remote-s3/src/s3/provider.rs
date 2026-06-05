//! Provider implementations for S3 sites.
//!
//! Each module provides two layers:
//! - `Provider<S3Invocation<Fx>>` — HTTP execution of authorized requests
//! - `Provider<ForkInvocation<S3, Fx>>` — authorization via S3Authorization, then delegation

pub mod archive;
pub mod memory;
