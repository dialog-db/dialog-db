//! UCAN access service test server.
//!
//! This module provides a local UCAN access service for integration testing.
//! It receives UCAN invocation containers, verifies them using `UcanAuthorizer`,
//! and returns pre-signed S3 request descriptors.

use super::{LocalS3, UcanS3Address};
use dialog_common::helpers::{Provider, Service};
use dialog_s3_credentials::Address;
use dialog_s3_credentials::s3::Credentials;
use dialog_s3_credentials::ucan::UcanAuthorizer;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

/// A running UCAN access service test server instance.
pub struct UcanAccessServer {
    /// The endpoint URL where the access service is listening
    pub endpoint: String,
    /// The backing S3 server
    pub s3_server: LocalS3,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl UcanAccessServer {
    /// Start a UCAN access service backed by a local S3 server.
    ///
    /// # Arguments
    ///
    /// * `s3_server` - A running LocalS3 server instance
    /// * `bucket` - The bucket name to use
    /// * `access_key` - AWS access key ID for S3 authentication
    /// * `secret_key` - AWS secret access key for S3 authentication
    pub async fn start(
        s3_server: LocalS3,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
    ) -> anyhow::Result<Self> {
        // Create S3 credentials for the authorizer
        // Note: The subject here is a placeholder since UcanAuthorizer uses
        // the subject from the UCAN invocation's capability chain
        let address = Address::new(&s3_server.endpoint, "us-east-1", bucket);
        let s3_credentials =
            Credentials::private(address, access_key, secret_key)?.with_path_style(true);

        // Use UcanAuthorizer from ucan/provider.rs
        let authorizer = Arc::new(RwLock::new(UcanAuthorizer::new(s3_credentials)));

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let endpoint = format!("http://{}", addr);

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let authorizer_clone = authorizer.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    result = listener.accept() => {
                        if let Ok((stream, _)) = result {
                            let authorizer = authorizer_clone.clone();
                            tokio::spawn(async move {
                                let service = hyper::service::service_fn(move |req| {
                                    let authorizer = authorizer.clone();
                                    async move {
                                        handle_request(req, authorizer).await
                                    }
                                });
                                let _ = http1::Builder::new()
                                    .serve_connection(TokioIo::new(stream), service)
                                    .await;
                            });
                        }
                    }
                }
            }
        });

        Ok(UcanAccessServer {
            endpoint,
            s3_server,
            shutdown_tx,
        })
    }
}

/// Add CORS headers to a response builder.
fn add_cors_headers(
    builder: hyper::http::response::Builder,
) -> hyper::http::response::Builder {
    builder
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "POST, OPTIONS")
        .header("Access-Control-Allow-Headers", "Content-Type")
        .header("Access-Control-Max-Age", "86400")
}

/// Handle an incoming UCAN access service request.
async fn handle_request(
    req: Request<Incoming>,
    authorizer: Arc<RwLock<UcanAuthorizer>>,
) -> Result<Response<http_body_util::Full<bytes::Bytes>>, Infallible> {
    use bytes::Bytes;
    use http_body_util::Full;

    // Handle CORS preflight requests
    if req.method() == Method::OPTIONS {
        return Ok(add_cors_headers(Response::builder())
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap());
    }

    // Only accept POST requests
    if req.method() != Method::POST {
        return Ok(add_cors_headers(Response::builder())
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::from("Method not allowed")))
            .unwrap());
    }

    // Read request body
    use http_body_util::BodyExt;
    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return Ok(add_cors_headers(Response::builder())
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from(format!(
                    "Failed to read body: {}",
                    e
                ))))
                .unwrap());
        }
    };

    // Authorize the UCAN container using UcanAuthorizer
    let authorizer = authorizer.read().await;
    match authorizer.authorize(&body_bytes).await {
        Ok(descriptor) => {
            // Serialize the RequestDescriptor as CBOR
            match serde_ipld_dagcbor::to_vec(&descriptor) {
                Ok(cbor_bytes) => {
                    // Return raw CBOR bytes
                    Ok(add_cors_headers(Response::builder())
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/cbor")
                        .body(Full::new(Bytes::from(cbor_bytes)))
                        .unwrap())
                }
                Err(e) => Ok(add_cors_headers(Response::builder())
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Full::new(Bytes::from(format!(
                        "Failed to encode response: {}",
                        e
                    ))))
                    .unwrap()),
            }
        }
        Err(e) => Ok(add_cors_headers(Response::builder())
            .status(StatusCode::FORBIDDEN)
            .body(Full::new(Bytes::from(format!(
                "Authorization failed: {}",
                e
            ))))
            .unwrap()),
    }
}

#[async_trait::async_trait]
impl Provider for UcanAccessServer {
    async fn stop(self) -> anyhow::Result<()> {
        let _ = self.shutdown_tx.send(());
        self.s3_server.stop().await
    }
}

/// Settings for configuring the UCAN access service test server.
#[derive(Debug, Clone)]
pub struct UcanS3Settings {
    /// The bucket name to create. Defaults to "test-bucket".
    pub bucket: String,
    /// AWS access key ID. Defaults to "test-access-key".
    pub access_key_id: String,
    /// AWS secret access key. Defaults to "test-secret-key".
    pub secret_access_key: String,
}

impl Default for UcanS3Settings {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
        }
    }
}

/// Provider function for UcanS3Address.
///
/// Starts both an S3 server and a UCAN access service.
#[dialog_common::provider]
pub async fn ucan_s3(
    settings: UcanS3Settings,
) -> anyhow::Result<Service<UcanS3Address, UcanAccessServer>> {
    let bucket = if settings.bucket.is_empty() {
        "test-bucket"
    } else {
        &settings.bucket
    };

    // Start the S3 server
    let s3_server = LocalS3::start_with_auth(
        &settings.access_key_id,
        &settings.secret_access_key,
        &[bucket],
    )
    .await?;

    let s3_endpoint = s3_server.endpoint.clone();

    // Start the UCAN access service
    let ucan_server = UcanAccessServer::start(
        s3_server,
        bucket,
        &settings.access_key_id,
        &settings.secret_access_key,
    )
    .await?;

    let address = UcanS3Address {
        access_service_url: ucan_server.endpoint.clone(),
        s3_endpoint,
        bucket: bucket.to_string(),
        access_key_id: settings.access_key_id,
        secret_access_key: settings.secret_access_key,
    };

    Ok(Service::new(address, ucan_server))
}
