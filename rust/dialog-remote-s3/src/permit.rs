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
///
/// Converts to [`reqwest::RequestBuilder`] via `From`, or use
/// [`send`](Permit::send) to execute the request directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permit {
    /// The presigned URL to use.
    pub url: Url,
    /// HTTP method (GET, PUT, DELETE).
    pub method: String,
    /// Headers to include in the request.
    pub headers: Vec<(String, String)>,
}

impl Permit {
    /// Send this permit as an HTTP request.
    pub async fn send(self) -> Result<reqwest::Response, reqwest::Error> {
        reqwest::RequestBuilder::from(self).send().await
    }
}

impl From<Permit> for reqwest::RequestBuilder {
    fn from(permit: Permit) -> reqwest::RequestBuilder {
        let client = reqwest::Client::new();
        let mut builder = match permit.method.as_str() {
            "GET" => client.get(permit.url),
            "PUT" => client.put(permit.url),
            "DELETE" => client.delete(permit.url),
            _ => client.request(
                reqwest::Method::from_bytes(permit.method.as_bytes()).unwrap(),
                permit.url,
            ),
        };

        for (key, value) in permit.headers {
            builder = builder.header(key, value);
        }

        builder
    }
}
