//! Signer trait for authorizing S3 requests.
//!
//! This trait allows different credential types to sign/authorize S3 request claims.

use super::{AuthorizationError, Claim, RequestDescriptor};
use dialog_common::capability::Did;

/// Trait for types that can authorize S3 request claims.
///
/// Different credential implementations (public, private, UCAN) implement
/// this trait to produce authorized request descriptors.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Signer {
    /// Get the subject DID that this signer is authorized for.
    ///
    /// This is the resource owner whose data can be accessed with these credentials.
    fn subject(&self) -> &Did;

    /// Authorize a claim and produce a request descriptor.
    ///
    /// The request descriptor contains the presigned URL and headers
    /// needed to make the actual HTTP request.
    ///
    /// The `'static` bound is required for UCAN credentials which use
    /// runtime type dispatch via `std::any::Any`.
    async fn sign<C: Claim + Send + Sync + 'static>(
        &self,
        claim: &C,
    ) -> Result<RequestDescriptor, AuthorizationError>;
}
