//! UCAN access service test server.
//!
//! Provides a local UCAN access service for integration testing.
//! Receives UCAN invocation containers, verifies them using [`UcanAuthorizer`],
//! and returns presigned S3 request descriptors.

use super::UcanS3Address;
use crate::{FromUcanArgs, UcanAuthorizer};
use dialog_capability::access::AuthorizeError;
use dialog_common::helpers::{Provider, Service};
use dialog_remote_s3::helpers::LocalS3;
use dialog_remote_s3::{Address, S3Credential, S3Error};
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
    /// The endpoint URL where the access service is listening.
    pub endpoint: String,
    /// The backing S3 server.
    pub s3_server: LocalS3,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

/// What the service holds across requests: the authorizer, whether it
/// performs operations itself, and how it has answered so far.
struct ServerState {
    authorizer: RwLock<UcanAuthorizer>,
    /// Perform an operation in the request that proved it when the
    /// invocation arrives in `Authorization` and the client accepts
    /// the outcome. Off, the service verifies such an invocation and
    /// answers with a permit, as a service over storage it cannot reach
    /// itself does.
    direct: bool,
    /// Never read `Authorization`: every request's body is read as a
    /// container, as a service that predates the direct exchange does.
    legacy: bool,
    /// The largest body the service reads; larger ones are turned away
    /// with a 413, as the real service turns away a body that outgrew
    /// what it expected.
    max_body_bytes: u64,
    stats: Stats,
}

/// How the service answered, for a test to read back at `GET /stats`:
/// whether a request was answered with a permit or performed outright.
#[derive(Default)]
struct Stats {
    requests: std::sync::atomic::AtomicUsize,
    redeemed: std::sync::atomic::AtomicUsize,
    performed: std::sync::atomic::AtomicUsize,
}

/// The counts `GET /stats` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServerStats {
    /// Requests received at the invocation endpoint.
    pub requests: usize,
    /// Invocations answered with a permit.
    pub redeemed: usize,
    /// Invocations performed in the request that proved them.
    pub performed: usize,
}

impl UcanAccessServer {
    /// Start a UCAN access service backed by a local S3 server.
    pub async fn start(
        s3_server: LocalS3,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
    ) -> anyhow::Result<Self> {
        Self::start_with(
            s3_server,
            bucket,
            access_key,
            secret_key,
            true,
            false,
            u64::MAX,
        )
        .await
    }

    /// Start the service, performing operations directly when `direct`
    /// and answering every invocation with a permit otherwise, reading
    /// no `Authorization` header at all when `legacy`, and turning away
    /// bodies over `max_body_bytes`.
    pub async fn start_with(
        s3_server: LocalS3,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        direct: bool,
        legacy: bool,
        max_body_bytes: u64,
    ) -> anyhow::Result<Self> {
        let address = Address::builder(&s3_server.endpoint)
            .region("us-east-1")
            .bucket(bucket)
            .path_style(true)
            .build()?;

        let credential = S3Credential::new(access_key, secret_key);

        let authorizer = Arc::new(ServerState {
            authorizer: RwLock::new(UcanAuthorizer::new(address, Some(credential))),
            direct,
            legacy,
            max_body_bytes,
            stats: Stats::default(),
        });

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

fn add_cors_headers(builder: hyper::http::response::Builder) -> hyper::http::response::Builder {
    builder
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        .header(
            "Access-Control-Allow-Headers",
            "Authorization, Content-Type, Accept",
        )
        .header("Access-Control-Expose-Headers", "ETag")
        .header("Access-Control-Max-Age", "86400")
        .header("Cache-Control", "no-store")
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<Response<http_body_util::Full<bytes::Bytes>>, Infallible> {
    use bytes::Bytes;
    use http_body_util::Full;
    use std::sync::atomic::Ordering;

    if req.method() == Method::OPTIONS {
        return Ok(add_cors_headers(Response::builder())
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap());
    }

    if req.method() == Method::GET && req.uri().path() == "/stats" {
        let stats = ServerStats {
            requests: state.stats.requests.load(Ordering::SeqCst),
            redeemed: state.stats.redeemed.load(Ordering::SeqCst),
            performed: state.stats.performed.load(Ordering::SeqCst),
        };
        return Ok(add_cors_headers(Response::builder())
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                serde_json::to_vec(&stats).expect("stats serialize"),
            )))
            .unwrap());
    }

    if req.method() != Method::POST {
        return Ok(add_cors_headers(Response::builder())
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::from("Method not allowed")))
            .unwrap());
    }

    let declared = req
        .headers()
        .get("content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|declared| declared > state.max_body_bytes) {
        return Ok(too_large(state.max_body_bytes));
    }

    state.stats.requests.fetch_add(1, Ordering::SeqCst);

    // An invocation under the UCAN scheme in `Authorization` asks for
    // the operation's outcome, or a permit to perform it, as `Accept`
    // says; its body is the bytes the operation stores. A request that
    // carries none is the permit flow: a container in the body,
    // answered with a permit. A legacy service never looks at the
    // header, and reads every request's body as a container.
    let credential = req
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .filter(|_| !state.legacy)
        .and_then(|value| value.split_once(char::is_whitespace))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("UCAN"))
        .map(|(_, rest)| rest.trim_start().to_owned());
    let wants_outcome = req
        .headers()
        .get("accept")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains("application/octet-stream"));

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
    if body_bytes.len() as u64 > state.max_body_bytes {
        return Ok(too_large(state.max_body_bytes));
    }

    let container = match &credential {
        Some(credential) => match dialog_ucan_core::Container::decode(credential.as_bytes())
            .and_then(|container| container.to_bytes())
        {
            Ok(bytes) => bytes,
            Err(error) => {
                return Ok(refused(&AuthorizeError::Malformed {
                    detail: format!("the credential does not carry a container: {error}"),
                }));
            }
        },
        None => body_bytes.to_vec(),
    };
    let perform_here = credential.is_some() && wants_outcome && state.direct;

    let authorizer = state.authorizer.read().await;
    match authorizer.authorize(&container).await {
        Ok(descriptor) if perform_here => {
            state.stats.performed.fetch_add(1, Ordering::SeqCst);
            let range = read_range(&container);
            Ok(perform(descriptor, body_bytes, range).await)
        }
        Ok(descriptor) => match serde_ipld_dagcbor::to_vec(&descriptor) {
            Ok(cbor_bytes) => {
                state.stats.redeemed.fetch_add(1, Ordering::SeqCst);
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
        },
        Err(S3Error::Authorization(reason)) => Ok(refused(&reason)),
        Err(e) => Ok(add_cors_headers(Response::builder())
            .status(StatusCode::FORBIDDEN)
            .body(Full::new(Bytes::from(format!(
                "Authorization failed: {}",
                e
            ))))
            .unwrap()),
    }
}

/// The range a blob read asks for, from the invocation's arguments, so
/// the object request carries it the way the client's own would have.
fn read_range(container: &[u8]) -> Option<String> {
    use dialog_effects::blob::prelude::BlobReadExt as _;

    let container = dialog_ucan_core::Container::from_bytes(container).ok()?;
    let chain = dialog_ucan_core::InvocationChain::try_from(container).ok()?;
    let segments: Vec<&str> = chain.command().0.iter().map(String::as_str).collect();
    if segments != ["use", "get", "archive", "blob"] {
        return None;
    }
    let capability = <dialog_effects::blob::Read as FromUcanArgs>::capability_from_args(
        chain.subject(),
        chain.arguments(),
    )
    .ok()?;
    let range = capability.range()?;
    Some(match range.length {
        Some(length) => format!(
            "bytes={}-{}",
            range.offset,
            range.offset + length.max(1) - 1
        ),
        None => format!("bytes={}-", range.offset),
    })
}

/// The answer to an invocation that did not verify, in the shape the
/// real service gives: the reason as JSON, under the status it earns.
/// A container that could not be read is a 400, which is also what a
/// service that predates the direct exchange answers a request whose
/// body is not a container.
fn refused(reason: &AuthorizeError) -> Response<http_body_util::Full<bytes::Bytes>> {
    use bytes::Bytes;
    use http_body_util::Full;

    let status = match reason {
        AuthorizeError::InvalidSignature { .. }
        | AuthorizeError::InvalidAudience { .. }
        | AuthorizeError::Expired { .. }
        | AuthorizeError::NotValidBefore { .. } => StatusCode::UNAUTHORIZED,
        AuthorizeError::Malformed { .. } | AuthorizeError::UnavailableProof { .. } => {
            StatusCode::BAD_REQUEST
        }
        AuthorizeError::Unavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::FORBIDDEN,
    };
    add_cors_headers(Response::builder())
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(reason).unwrap_or_default(),
        )))
        .unwrap()
}

/// The answer to a body over the limit, in the shape the real service
/// gives: a JSON error the client reads as "turned away for size".
fn too_large(limit: u64) -> Response<http_body_util::Full<bytes::Bytes>> {
    use bytes::Bytes;
    use http_body_util::Full;

    add_cors_headers(Response::builder())
        .status(StatusCode::PAYLOAD_TOO_LARGE)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(format!(
            r#"{{"error":{{"code":"PAYLOAD_TOO_LARGE","message":"request body exceeds the {limit}-byte limit"}}}}"#
        ))))
        .unwrap()
}

/// Perform the operation the permit authorizes against the local S3,
/// the way the real service performs it against its bucket, and answer
/// with the outcome the object route would have given: the status, the
/// `ETag`, and the body.
///
/// A write's bytes are the request body; a write with an empty body is
/// answered as the object route answers a body of the wrong length.
async fn perform(
    mut permit: dialog_remote_s3::Permit,
    body_bytes: bytes::Bytes,
    range: Option<String>,
) -> Response<http_body_util::Full<bytes::Bytes>> {
    use bytes::Bytes;
    use http_body_util::Full;

    let outcome = match permit.method.as_str() {
        "PUT" => {
            if body_bytes.is_empty() {
                return add_cors_headers(Response::builder())
                    .status(StatusCode::LENGTH_REQUIRED)
                    .body(Full::new(Bytes::from(
                        "the request carries no bytes to store",
                    )))
                    .unwrap();
            }
            permit.upload(body_bytes.to_vec()).await
        }
        _ => {
            if let Some(range) = range {
                permit.headers.push(("range".to_string(), range));
            }
            permit.send().await
        }
    };
    match outcome {
        Ok(response) => {
            let status = response.status().as_u16();
            let etag = response
                .headers()
                .get("etag")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let bytes = response.bytes().await.unwrap_or_default();
            let mut builder = add_cors_headers(Response::builder())
                .status(status)
                .header("Content-Type", "application/octet-stream");
            if let Some(etag) = etag {
                builder = builder.header("ETag", etag);
            }
            builder
                .body(Full::new(Bytes::from(bytes.to_vec())))
                .unwrap()
        }
        Err(error) => add_cors_headers(Response::builder())
            .status(StatusCode::BAD_GATEWAY)
            .body(Full::new(Bytes::from(format!("storage failed: {error}"))))
            .unwrap(),
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
    /// Whether the service performs operations in the request that
    /// proved them. Off, it verifies the invocation and answers with a
    /// permit, standing in for a service over storage it cannot reach
    /// itself. Defaults to on.
    pub direct: bool,
    /// Whether the service ignores `Authorization` altogether and reads
    /// every body as a container, standing in for a service that
    /// predates the direct exchange. Defaults to off.
    pub legacy: bool,
    /// The largest request body the service reads. Unbounded by default;
    /// a test sets it to stand in for a service whose limit a write's
    /// bytes exceed.
    pub max_body_bytes: u64,
}

impl Default for UcanS3Settings {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            access_key_id: "test-access-key".to_string(),
            secret_access_key: "test-secret-key".to_string(),
            direct: true,
            legacy: false,
            max_body_bytes: u64::MAX,
        }
    }
}

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

    let s3_server = LocalS3::start_with_auth(
        &settings.access_key_id,
        &settings.secret_access_key,
        &[bucket],
    )
    .await?;

    let s3_endpoint = s3_server.endpoint.clone();

    let ucan_server = UcanAccessServer::start_with(
        s3_server,
        bucket,
        &settings.access_key_id,
        &settings.secret_access_key,
        settings.direct,
        settings.legacy,
        settings.max_body_bytes,
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
