//! S3 site configuration.
//!
//! The `S3Site` type and `S3Invocation` have been removed. S3 site
//! configuration is now provided by:
//! - `S3` unit struct in `dialog-storage` (implements `Site`)
//! - `Address` in this crate (carries endpoint/region/bucket/path_style)
//! - `ForkInvocation<S3, Fx>` replaces `S3Invocation<Fx>`
