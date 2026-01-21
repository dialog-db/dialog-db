use crate::access::{AuthorizationError, AuthorizedRequest, S3Request};
use async_trait::async_trait;

/// Trait describing credentials that can autorize S3Requests
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Credentials
where
    Self: Sized,
{
    /// Takes S3Request and issues authorization in form of presigned
    /// S3 URL and associated headers.
    async fn authorize<R: S3Request>(
        &self,
        request: &R,
    ) -> Result<AuthorizedRequest, AuthorizationError>;
}
