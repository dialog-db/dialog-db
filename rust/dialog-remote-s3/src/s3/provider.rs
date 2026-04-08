//! Provider implementations for S3 sites.
//!
//! Each module provides two layers:
//! - `Provider<Authorized<Fx>>` on `Http` — shared HTTP execution (used by S3 and UCAN)
//! - `Provider<Fork<S3, Fx>>` on `S3` — SigV4 authorization, then delegates to `Http`

pub mod archive;
pub mod memory;
