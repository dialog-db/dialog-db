//! Pre-authorized HTTP request for S3 operations.
//!
//! A [`Permit`] is the result of authorization — either SigV4 signing (direct S3)
//! or a UCAN access service response. It carries everything needed to make the
//! actual HTTP request: presigned URL, method, and headers.

use serde::{Deserialize, Serialize};
use url::Url;

/// A pre-authorized HTTP request — presigned URL + method + headers.
///
/// Produced by SigV4 signing (direct S3) or by a UCAN access service.
/// Fed into [`S3Invocation<Fx>`](crate::S3Invocation) for typed execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permit {
    /// The presigned URL to use.
    pub url: Url,
    /// HTTP method (GET, PUT, DELETE).
    pub method: String,
    /// Headers to include in the request.
    pub headers: Vec<(String, String)>,
}
