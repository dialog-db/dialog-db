//! `Provider<Fork<UcanSite, Fx>>` implementations.
//!
//! Each impl authorizes via the UCAN access service (`UcanAddress::authorize`),
//! then delegates to `Provider<Authorized<Fx>>` on `S3` for shared HTTP execution.

pub mod archive;
pub mod memory;
