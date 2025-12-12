//! In-memory S3-compatible test server for integration testing.
//!
//! This module provides a local S3-compatible server that runs in-memory,
//! enabling integration tests without requiring external S3 services.
//!
//! # Features
//!
//! - **In-memory storage**: All data is stored in memory for fast test execution
//! - **S3 API compatibility**: Implements GET, PUT, DELETE, HEAD, and ListObjectsV2
//! - **Optional authentication**: Supports both public and authenticated access modes
//! - **CORS support**: Includes permissive CORS headers for browser-based testing
//!
//! # Provider Integration
//!
//! This module includes `#[dialog_common::provider]` implementations that integrate
//! with the `#[dialog_common::test]` macro for automatic server lifecycle management.

use super::{PublicS3Address, PublicS3Settings, S3Address, S3Settings};
use async_trait::async_trait;
use bytes::Bytes;
use dialog_common::helpers::{Provider, Service};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use hyper_util::service::TowerToHyperService;
use s3s::dto::{
    DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectInput, GetObjectOutput, HeadObjectInput,
    HeadObjectOutput, ListObjectsV2Input, ListObjectsV2Output, Object, PutObjectInput,
    PutObjectOutput, StreamingBlob, Timestamp,
};
use s3s::service::S3ServiceBuilder;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

/// Simple in-memory S3 backend for testing.
///
/// Stores objects in memory organized by bucket name and key.
/// All operations are thread-safe via `RwLock`.
#[derive(Clone, Default)]
pub struct InMemoryS3 {
    buckets: Arc<RwLock<HashMap<String, HashMap<String, StoredObject>>>>,
}

/// A running local S3 test server instance.
///
/// Holds the server endpoint and shutdown channel.
/// When dropped or stopped, the server is shut down gracefully.
pub struct LocalS3 {
    /// The endpoint URL where the server is listening (e.g., "http://127.0.0.1:9000")
    pub endpoint: String,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl LocalS3 {
    /// Start a local S3-compatible test server with pre-created buckets.
    ///
    /// The server listens on a random available port on localhost.
    /// Returns a handle that can be used to get the endpoint URL and stop the server.
    pub async fn start(buckets: &[&str]) -> anyhow::Result<Self> {
        Self::start_internal(None, buckets).await
    }

    /// Start a test server with authentication and pre-created buckets.
    ///
    /// Requests must be signed with the provided credentials to succeed.
    pub async fn start_with_auth(
        access_key: &str,
        secret_key: &str,
        buckets: &[&str],
    ) -> anyhow::Result<LocalS3> {
        let auth = s3s::auth::SimpleAuth::from_single(access_key, secret_key);
        Self::start_internal(Some(auth), buckets).await
    }

    async fn start_internal(
        auth: Option<s3s::auth::SimpleAuth>,
        buckets: &[&str],
    ) -> anyhow::Result<LocalS3> {
        let storage = InMemoryS3::default();

        // Pre-create buckets
        for bucket in buckets {
            storage.create_bucket(bucket).await;
        }

        let mut builder = S3ServiceBuilder::new(storage.clone());
        if let Some(auth) = auth {
            builder.set_auth(auth);
        }
        let s3_service = builder.build();

        // Wrap the S3 service with CORS layer for browser-based testing
        // Expose headers needed for S3 operations (ETag, x-amz-* headers)
        let service = ServiceBuilder::new()
            .layer(CorsLayer::very_permissive().expose_headers([
                hyper::header::ETAG,
                hyper::header::CONTENT_LENGTH,
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderName::from_static("x-amz-checksum-sha256"),
                hyper::header::HeaderName::from_static("x-amz-request-id"),
            ]))
            .service(s3_service);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let endpoint = format!("http://{}", addr);

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    result = listener.accept() => {
                        if let Ok((stream, _)) = result {
                            let hyper_service = TowerToHyperService::new(service.clone());
                            tokio::spawn(async move {
                                let _ = http1::Builder::new()
                                    .serve_connection(TokioIo::new(stream), hyper_service)
                                    .await;
                            });
                        }
                    }
                }
            }
        });

        Ok(LocalS3 {
            endpoint,
            shutdown_tx,
        })
    }
}

#[derive(Clone)]
struct StoredObject {
    data: Vec<u8>,
    content_type: Option<String>,
    e_tag: String,
    last_modified: Timestamp,
}

impl InMemoryS3 {
    /// Create a bucket if it doesn't exist.
    pub async fn create_bucket(&self, bucket: &str) {
        let mut buckets = self.buckets.write().await;
        if !buckets.contains_key(bucket) {
            buckets.insert(bucket.to_string(), HashMap::new());
        }
    }

    async fn get_or_create_bucket(
        &self,
        bucket: &str,
    ) -> tokio::sync::RwLockWriteGuard<'_, HashMap<String, HashMap<String, StoredObject>>> {
        let mut buckets = self.buckets.write().await;
        if !buckets.contains_key(bucket) {
            buckets.insert(bucket.to_string(), HashMap::new());
        }
        buckets
    }
}

#[async_trait]
impl S3 for InMemoryS3 {
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let bucket = &req.input.bucket;
        let key = &req.input.key;

        let buckets = self.buckets.read().await;
        if let Some(bucket_contents) = buckets.get(bucket) {
            if let Some(obj) = bucket_contents.get(key) {
                let body = s3s::Body::from(Bytes::from(obj.data.clone()));
                let output = GetObjectOutput {
                    body: Some(StreamingBlob::from(body)),
                    content_length: Some(obj.data.len() as i64),
                    content_type: obj.content_type.clone(),
                    e_tag: Some(ETag::Strong(obj.e_tag.clone())),
                    last_modified: Some(obj.last_modified.clone()),
                    ..Default::default()
                };
                return Ok(S3Response::new(output));
            }
        }
        Err(s3_error!(NoSuchKey))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let content_type = req.input.content_type.clone();

        let data = if let Some(mut body) = req.input.body {
            // Collect stream data chunk by chunk using Stream trait
            use futures_util::StreamExt;
            let mut chunks = Vec::new();
            while let Some(result) = body.next().await {
                if let Ok(bytes) = result {
                    chunks.extend_from_slice(&bytes);
                }
            }
            chunks
        } else {
            Vec::new()
        };

        // Calculate MD5 for ETag
        let e_tag = format!("{:x}", md5::compute(&data));

        let stored = StoredObject {
            data,
            content_type,
            e_tag: e_tag.clone(),
            last_modified: Timestamp::from(SystemTime::now()),
        };

        let mut buckets = self.get_or_create_bucket(&bucket).await;
        if let Some(bucket_contents) = buckets.get_mut(&bucket) {
            bucket_contents.insert(key, stored);
        }

        let output = PutObjectOutput {
            e_tag: Some(ETag::Strong(e_tag)),
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let bucket = &req.input.bucket;
        let key = &req.input.key;

        let mut buckets = self.buckets.write().await;
        if let Some(bucket_contents) = buckets.get_mut(bucket) {
            bucket_contents.remove(key);
        }

        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let bucket = &req.input.bucket;
        let key = &req.input.key;

        let buckets = self.buckets.read().await;
        if let Some(bucket_contents) = buckets.get(bucket) {
            if let Some(obj) = bucket_contents.get(key) {
                let output = HeadObjectOutput {
                    content_length: Some(obj.data.len() as i64),
                    content_type: obj.content_type.clone(),
                    e_tag: Some(ETag::Strong(obj.e_tag.clone())),
                    last_modified: Some(obj.last_modified.clone()),
                    ..Default::default()
                };
                return Ok(S3Response::new(output));
            }
        }
        Err(s3_error!(NoSuchKey))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let bucket = &req.input.bucket;
        let prefix = req.input.prefix.as_deref().unwrap_or("");

        let buckets = self.buckets.read().await;

        // Return NoSuchBucket error if bucket doesn't exist (matches real S3 behavior)
        // See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_Errors
        let bucket_contents = buckets.get(bucket).ok_or_else(|| s3_error!(NoSuchBucket))?;

        let mut contents = Vec::new();
        for (key, obj) in bucket_contents.iter() {
            // Filter by prefix
            if key.starts_with(prefix) {
                contents.push(Object {
                    key: Some(key.clone()),
                    size: Some(obj.data.len() as i64),
                    e_tag: Some(ETag::Strong(obj.e_tag.clone())),
                    last_modified: Some(obj.last_modified.clone()),
                    ..Default::default()
                });
            }
        }

        // Sort by key for consistent ordering
        contents.sort_by(|a, b| a.key.cmp(&b.key));

        let output = ListObjectsV2Output {
            contents: Some(contents),
            is_truncated: Some(false),
            key_count: None,
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }
}

#[async_trait::async_trait]
impl Provider for LocalS3 {
    async fn stop(self) -> anyhow::Result<()> {
        let _ = self.shutdown_tx.send(());
        Ok(())
    }
}

/// Provider function for S3Address (authenticated server)
#[dialog_common::provider]
pub async fn local_s3(settings: S3Settings) -> anyhow::Result<Service<S3Address, LocalS3>> {
    let bucket = if settings.bucket.is_empty() {
        "test-bucket"
    } else {
        &settings.bucket
    };
    let server = LocalS3::start_with_auth(
        &settings.access_key_id,
        &settings.secret_access_key,
        &[bucket],
    )
    .await?;
    let address = S3Address {
        endpoint: server.endpoint.clone(),
        bucket: bucket.to_string(),
        access_key_id: settings.access_key_id,
        secret_access_key: settings.secret_access_key,
    };
    Ok(Service::new(address, server))
}

/// Provider function for PublicS3Address (public server, no auth)
#[dialog_common::provider]
pub async fn public_local_s3(
    settings: PublicS3Settings,
) -> anyhow::Result<Service<PublicS3Address, LocalS3>> {
    let bucket = if settings.bucket.is_empty() {
        "test-bucket"
    } else {
        &settings.bucket
    };
    let server = LocalS3::start(&[bucket]).await?;
    let address = PublicS3Address {
        endpoint: server.endpoint.clone(),
        bucket: bucket.to_string(),
    };
    Ok(Service::new(address, server))
}
